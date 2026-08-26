package server

import (
	"context"
	"fmt"
	"unicode/utf8"

	"connectrpc.com/connect"
	enums "go.temporal.io/api/enums/v1"
	historypb "go.temporal.io/api/history/v1"
	"google.golang.org/protobuf/proto"

	v1 "github.com/picatz/flowstate/pkg/flowstate/v1"
)

// Reading a run's own account of itself.
//
// Every other verb about an existing run reports its *state*. This reports what
// it did, which is a different question and the one asked at the moment there is
// no state left to report: a run that has already failed has no now to describe.
//
// # What is read, and what is refused
//
// History events, and the summary the interpreter wrote onto each command it
// issued (see `engine/summary.go`). Nothing here decodes an activity's input or
// result payload. Those hold the resolved task — an author's inputs, and
// references a task resolves inside itself — and decoding them to label a row
// would put that material on the read path, where the caller is whoever asked
// and the answer travels. A step is therefore named by its label or not at all.
//
// The one payload-shaped thing reported is a failure's *message*, and only the
// outermost one, which is the identical decision [FlowstateServer.pendingActivities]
// already made for [v1.PendingActivity.LastFailure]: the chain repeats what the
// attempt count says, and Temporal's failure converter writes every level of an
// unwrapped error into what it persists, so the chain is the shape most likely
// to carry what a scrubbed outer message deliberately dropped.
//
// # Two bounds, and a completeness check that trusts neither
//
// A caller asks for entries; what a history holds is *events*, most of them
// bookkeeping about workflow tasks and workers that never become an entry. So
// the answer only grows when reportable events come back, and how many come
// back is Temporal's choice rather than ours — the shape `list.go` describes,
// where a bound measured in what you collect is no bound at all against a peer
// that decides the ratio. Events examined and entries reported are therefore
// bounded separately.
//
// Neither bound would catch a read that simply *stopped*. The SDK's history
// iterator ends its walk when a page comes back empty, whether or not more
// remains, so a walk can finish short and look finished — "not short, but
// claiming to be the whole of it", which is the exact defect CLAUDE.md records
// for `List`. The check is therefore made against the data: a closed run's
// history ends with an event saying how it ended, so an account of a closed run
// that never reaches one is short however it came to be short.
const (
	// defaultTimelineEntries is how many entries come back when a caller does
	// not say.
	defaultTimelineEntries = 500

	// maxTimelineEntries bounds what a caller may ask for. Mirrors the schema's
	// own ceiling on [v1.GetTimelineRequest.MaxEntries]; the schema refuses a
	// larger ask and this clamps one that arrives another way.
	maxTimelineEntries = 5000

	// maxTimelineScan bounds how many history events one request may examine,
	// whatever it finds among them.
	//
	// Far above the entry bound, and not merely because a history holds several
	// events per reportable one. Resumption re-walks: a request carrying
	// [v1.GetTimelineRequest.AfterEventId] reads the history from the start and
	// skips what the caller already has, so the budget has to cover a whole
	// run's history rather than one answer's worth of it — a budget that ran
	// out before reaching the cursor would make the tail of a long run
	// unreachable, which is the dead end this resumption exists to remove.
	//
	// Sized against the number rather than against a feeling: Temporal
	// force-terminates an execution at 51,200 history events, which is the
	// figure [v1.MaxAtomicBlockActivities] is itself derived from. So this
	// covers any history that can exist under those defaults several times
	// over, and the ordinary case is that the walk always reaches the end.
	//
	// A deployment that raised that cap far enough gets a truncated answer
	// rather than a wrong one, and an empty truncated answer says so plainly —
	// see [v1.GetTimelineResponse.Truncated].
	maxTimelineScan = 200_000

	// maxTimelineFailureBytes bounds one failure message.
	//
	// A failure's message is the one thing reported here whose length is chosen
	// by the workload rather than by this repository: a task can fail with
	// whatever string it likes, and a run started by an outside party is not
	// ours to assume anything about. Bounded generously, because the outermost
	// sentence is usually the whole diagnosis and a cap that cuts a real one is
	// worse than no timeline.
	maxTimelineFailureBytes = 1024

	// maxTimelineBytes bounds the whole answer, which the message cap does not.
	//
	// Bounding one resource does not bound another the peer controls the ratio
	// to: a message cap times the entry ceiling is still several megabytes, and
	// the entry ceiling exists to bound *entries*. So the serialized size is
	// counted as the answer is assembled and the read stops against it, saying
	// that it stopped (Codex, #1119).
	maxTimelineBytes = 4 << 20
)

// GetTimeline reports what a run did, event by event.
func (s *FlowstateServer) GetTimeline(
	ctx context.Context, req *connect.Request[v1.GetTimelineRequest],
) (*connect.Response[v1.GetTimelineResponse], error) {
	if err := v1.Validate(req.Msg); err != nil {
		return nil, connect.NewError(connect.CodeInvalidArgument, err)
	}

	// Authorized before anything is read, and through the client the check was
	// made with — see [FlowstateServer.authorizeRun]. A history is the whole
	// account of a workload, so a timeline readable by whoever guessed an id
	// would be a larger disclosure than Get's rather than a smaller one.
	temporal, described, err := s.authorizeRun(ctx, req.Msg.GetWorkflowId(), req.Msg.GetRunId())
	if err != nil {
		return nil, err
	}

	limit := int(req.Msg.GetMaxEntries())
	switch {
	case limit <= 0:
		limit = defaultTimelineEntries
	case limit > maxTimelineEntries:
		limit = maxTimelineEntries
	}

	// The run the check was made against, not the one the caller named: an
	// empty run id means "the latest", and resolving it here is what keeps the
	// history read and the authorization on the same execution.
	execution := described.GetWorkflowExecutionInfo().GetExecution()

	out := &v1.GetTimelineResponse{}

	// What the walk carries about work still in flight, keyed by the event that
	// scheduled it: the label written onto that scheduling, and the attempt the
	// latest start reported. Temporal writes each once and the events that say
	// how the work ended carry only a reference back, so this is the join.
	//
	// Both are dropped when that work ends, which is what bounds them: an
	// entry lives from a scheduling to its own ending, so what is held is the
	// work in flight rather than everything the run has ever done — and the
	// engine already bounds that, at [v1.MaxAtomicBlockActivities], since work
	// in flight at once is exactly what a suspension-opaque block's ceiling
	// counts. Without the drop, a walk over a long history would instead
	// accumulate a row per activity the run has *ever* scheduled, on the read
	// path, charged to whoever asked.
	inFlight := map[int64]*activityInFlight{}

	history := temporal.GetWorkflowHistory(ctx, execution.GetWorkflowId(), execution.GetRunId(),
		// Never a long poll. Waiting for a new event would turn a read into a
		// held connection, on the one verb meant to be safe to point an agent
		// at unattended.
		false, enums.HISTORY_EVENT_FILTER_TYPE_ALL_EVENT)

	after := req.Msg.GetAfterEventId()
	scanned := 0
	assembled := 0
	ended := false

	for history.HasNext() {
		if scanned >= maxTimelineScan || len(out.Entries) >= limit {
			out.Truncated = true
			break
		}

		event, err := history.Next()
		if err != nil {
			return nil, connect.NewError(connect.CodeInternal, fmt.Errorf("reading run history: %w", err))
		}
		scanned++

		// Where this run sits in the chain, off the history's own first event.
		// Both directions, because forward alone is a trap: omitting a run id
		// resolves the latest segment, whose successor is by definition empty.
		if started := event.GetWorkflowExecutionStartedEventAttributes(); started != nil {
			out.PreviousRunId = started.GetContinuedExecutionRunId()
			out.FirstRunId = started.GetFirstExecutionRunId()
		}
		if next := event.GetWorkflowExecutionContinuedAsNewEventAttributes().GetNewExecutionRunId(); next != "" {
			out.NextRunId = next
		}

		entry := s.timelineEntry(event, inFlight)
		if entry == nil {
			continue
		}
		// The run reached its own ending, which is the claim [v1.GetTimelineResponse.Truncated]
		// is checked against. Recorded from the walk rather than from what was
		// reported, so a resumption that skips past the ending still knows the
		// account is whole.
		if entry.GetKind() == v1.TimelineEntry_KIND_RUN_ENDED ||
			entry.GetKind() == v1.TimelineEntry_KIND_RUN_CONTINUED {
			ended = true
		}

		// Skipped after the joins are made, never before: the scheduling a
		// resumption walks past is what names the rows it will report.
		if entry.GetEventId() <= after {
			continue
		}

		// Counted as the answer is assembled rather than measured afterwards,
		// because a response refused for being too large is a question nobody
		// gets an answer to. Stopping short is a truncation, which this already
		// has a way to report — and never on the first entry, or a single
		// oversized row would make a run unreadable rather than clipped.
		size := proto.Size(entry)
		if !timelineFits(assembled, size, len(out.Entries)) {
			out.Truncated = true

			break
		}
		assembled += size

		out.Entries = append(out.Entries, entry)
	}

	// A closed run's history ends with an event saying how it ended. Reaching
	// none means this account stopped short — because a bound was hit, or
	// because the walk gave up on an empty page and reported nothing about it.
	// The caller is told either way rather than handed a prefix that reads
	// whole.
	if !ended && segmentClosed(described.GetWorkflowExecutionInfo().GetStatus()) {
		out.Truncated = true
	}

	return connect.NewResponse(out), nil
}

// segmentClosed reports whether this execution has finished, which is what
// makes "the history must reach an ending event" a checkable claim.
//
// Asked of Temporal's own status rather than of [runStatus]'s answer, and that
// is the whole point of the function existing. [runStatus] maps
// CONTINUED_AS_NEW to RUNNING deliberately and correctly: callers address
// *workloads*, and a workload that continued as new is still going, so
// reporting a segment as ended would answer a question about the workload with
// a fact about its bookkeeping.
//
// A timeline asks the other question. It is per segment — that is what
// [v1.GetTimelineResponse.NextRunId] and PreviousRunId are for — and a segment
// that continued as new is finished, and must end with the event saying so.
// Borrowing the workload-level answer here made the completeness check silently
// inapplicable to exactly the segments the predecessor pointers had just made
// reachable: an earlier segment read by run id, where a walk that stopped short
// would come back looking whole (Codex, #1119).
//
// Anything that is not running is closed, rather than a list of the statuses
// that are: a status Temporal adds later that means "finished" then reads as
// finished, and the failure direction is a spurious truncation rather than a
// prefix presented as an account.
func segmentClosed(status enums.WorkflowExecutionStatus) bool {
	return status != enums.WORKFLOW_EXECUTION_STATUS_UNSPECIFIED &&
		status != enums.WORKFLOW_EXECUTION_STATUS_RUNNING
}

// activityInFlight is what a walk knows about one scheduled activity until it
// ends: the label Temporal recorded on the scheduling, and the attempt the
// latest start reported.
//
// The attempt is here rather than read off the ending because Temporal does not
// put it there. `ActivityTaskFailed`, `…TimedOut`, `…Completed` and `…Canceled`
// carry a reference to the scheduling and to the start, and no attempt number
// at all — so a failed row left to itself cannot say which try failed, which is
// exactly what [v1.TimelineEntry.Attempt] promises and exactly what makes a
// stuck run legible (Codex, #1119).
type activityInFlight struct {
	label   string
	attempt int32
}

// timelineEntry maps one history event to an entry, or to nil where the event
// is not something the workload did.
//
// The mapping is deliberately narrow. A history carries dozens of event types,
// most of them bookkeeping about workflow tasks and about the worker; a caller
// made to filter those is a caller reimplementing this, and one that did not
// would read a run's own scheduling as things the workload did.
func (s *FlowstateServer) timelineEntry(
	event *historypb.HistoryEvent, inFlight map[int64]*activityInFlight,
) *v1.TimelineEntry {
	entry := &v1.TimelineEntry{
		EventId: event.GetEventId(),
		Time:    event.GetEventTime(),
	}

	// ended closes out an activity: it names the row from what the walk
	// collected at the scheduling and at the latest start, then forgets it,
	// which is what keeps the map to work in flight.
	ended := func(scheduled int64) {
		entry.ScheduledEventId = scheduled
		if work, ok := inFlight[scheduled]; ok {
			entry.Step = work.label
			entry.Attempt = work.attempt
			delete(inFlight, scheduled)
		}
	}

	switch event.GetEventType() {
	case enums.EVENT_TYPE_ACTIVITY_TASK_SCHEDULED:
		entry.Kind = v1.TimelineEntry_KIND_STEP_SCHEDULED
		entry.Step = s.summaryText(event)
		entry.ScheduledEventId = event.GetEventId()
		// Attempt one, said rather than left at zero. This is the only row a
		// normally executed activity gets for its first try — the start that
		// would otherwise carry the number is not reported, because a started
		// row beside every scheduled row is noise — so a zero here would make
		// a machine reader see the first attempt as unspecified while the
		// schema says attempt-capable entries begin at one (Codex, #1119).
		entry.Attempt = 1
		// Recorded for the events that report how this work ended, which carry
		// a reference here and nothing else about it.
		inFlight[event.GetEventId()] = &activityInFlight{label: entry.Step, attempt: 1}

	case enums.EVENT_TYPE_ACTIVITY_TASK_COMPLETED:
		entry.Kind = v1.TimelineEntry_KIND_STEP_COMPLETED
		ended(event.GetActivityTaskCompletedEventAttributes().GetScheduledEventId())

	case enums.EVENT_TYPE_ACTIVITY_TASK_FAILED:
		attrs := event.GetActivityTaskFailedEventAttributes()
		entry.Kind = v1.TimelineEntry_KIND_STEP_FAILED
		ended(attrs.GetScheduledEventId())
		entry.Failure = boundedFailure(attrs.GetFailure().GetMessage())

	case enums.EVENT_TYPE_ACTIVITY_TASK_TIMED_OUT:
		attrs := event.GetActivityTaskTimedOutEventAttributes()
		entry.Kind = v1.TimelineEntry_KIND_STEP_TIMED_OUT
		ended(attrs.GetScheduledEventId())
		entry.Failure = boundedFailure(attrs.GetFailure().GetMessage())

	case enums.EVENT_TYPE_ACTIVITY_TASK_CANCELED:
		entry.Kind = v1.TimelineEntry_KIND_STEP_CANCELED
		ended(event.GetActivityTaskCanceledEventAttributes().GetScheduledEventId())

	case enums.EVENT_TYPE_ACTIVITY_TASK_STARTED:
		// Temporal schedules an activity once and starts it once per attempt,
		// so this is the only event that says which try is running — and the
		// only place an *ending* can learn it from either, which is why the
		// number is kept whether or not this event is worth a row.
		attrs := event.GetActivityTaskStartedEventAttributes()
		scheduled := attrs.GetScheduledEventId()

		work, known := inFlight[scheduled]
		if known {
			entry.Step = work.label
			work.attempt = attrs.GetAttempt()
		}
		entry.ScheduledEventId = scheduled

		// The first attempt is already reported by the scheduling itself, which
		// carries its number too. A row here as well would make an ordinary run
		// twice as long to read for a fact it already states.
		if attrs.GetAttempt() <= 1 {
			return nil
		}

		// A retry means the try before it did not succeed, and Temporal records
		// that failure *here* rather than as an event of its own: only a final,
		// retries-exhausted failure gets an `ActivityTaskFailed`. Reported as
		// the failure it is rather than as detail on a scheduling, because a
		// consumer filtering on KIND_STEP_FAILED would otherwise miss every
		// non-terminal failure — which is to say every failure a *retrying* run
		// has, the case this whole feature exists for, and the case the schema
		// already promised one row per attempt of (Codex, #1119).
		//
		// The row is about the attempt that ended, not the one starting now.
		entry.Attempt = attrs.GetAttempt() - 1
		entry.Failure = boundedFailure(attrs.GetLastFailure().GetMessage())

		// A timeout and an error are different diagnoses and read identically
		// in a message-only report, so the failure's own shape decides the
		// kind, exactly as it does for a terminal one.
		if attrs.GetLastFailure().GetTimeoutFailureInfo() != nil {
			entry.Kind = v1.TimelineEntry_KIND_STEP_TIMED_OUT
		} else {
			entry.Kind = v1.TimelineEntry_KIND_STEP_FAILED
		}

	case enums.EVENT_TYPE_TIMER_STARTED:
		entry.Kind = v1.TimelineEntry_KIND_TIMER_STARTED
		entry.Step = s.summaryText(event)
		// Recorded under this event's own id, which is what a TimerFired
		// refers back to — the same join an activity's ending makes.
		if entry.Step != "" {
			inFlight[event.GetEventId()] = &activityInFlight{label: entry.Step}
		}

	case enums.EVENT_TYPE_TIMER_FIRED:
		entry.Kind = v1.TimelineEntry_KIND_TIMER_FIRED
		if work, ok := inFlight[event.GetTimerFiredEventAttributes().GetStartedEventId()]; ok {
			entry.Step = work.label
			delete(inFlight, event.GetTimerFiredEventAttributes().GetStartedEventId())
		}

	case enums.EVENT_TYPE_TIMER_CANCELED:
		// Not a row: a cancelled timer is a wait that ended because the thing
		// it was bounding happened, and the account already says that — the
		// signal that answered a gate is right there. Forgotten, though, or a
		// run parked and released many times would carry every lapsed timer.
		delete(inFlight, event.GetTimerCanceledEventAttributes().GetStartedEventId())

		return nil

	case enums.EVENT_TYPE_WORKFLOW_EXECUTION_SIGNALED:
		// Named, never carrying its payload: a signal's payload is somebody's
		// decision, and it is exactly the kind of thing a read surface must not
		// spread. The name is the fact a reader needs — which gate was
		// answered, and when.
		entry.Kind = v1.TimelineEntry_KIND_SIGNAL_RECEIVED
		entry.Step = event.GetWorkflowExecutionSignaledEventAttributes().GetSignalName()

	case enums.EVENT_TYPE_WORKFLOW_EXECUTION_CONTINUED_AS_NEW:
		entry.Kind = v1.TimelineEntry_KIND_RUN_CONTINUED

	case enums.EVENT_TYPE_WORKFLOW_EXECUTION_COMPLETED,
		enums.EVENT_TYPE_WORKFLOW_EXECUTION_CANCELED,
		enums.EVENT_TYPE_WORKFLOW_EXECUTION_TERMINATED,
		enums.EVENT_TYPE_WORKFLOW_EXECUTION_TIMED_OUT:
		entry.Kind = v1.TimelineEntry_KIND_RUN_ENDED

	case enums.EVENT_TYPE_WORKFLOW_EXECUTION_FAILED:
		entry.Kind = v1.TimelineEntry_KIND_RUN_ENDED
		entry.Failure = boundedFailure(event.GetWorkflowExecutionFailedEventAttributes().GetFailure().GetMessage())

	default:
		return nil
	}

	return entry
}

// timelineFits reports whether one more entry of this size may join an answer
// that has already assembled this many bytes.
//
// A function rather than an expression inline, so the bound is something a test
// can reach: a bound nothing reaches is a bound nothing tests, and 4 MiB of
// entries is not a thing a run in a test produces on demand.
//
// The first entry always fits. A single oversized row would otherwise come back
// as an empty truncated answer, which is this API's spelling for "nothing past
// here is readable" — a much worse thing to say than "here is the row, and
// there is more".
func timelineFits(assembled, size, entries int) bool {
	return entries == 0 || assembled+size <= maxTimelineBytes
}

// boundedFailure is a failure's message, cut to [maxTimelineFailureBytes].
//
// At a rune boundary rather than a byte offset, for the reason
// `flowtest.truncateRuneSafe` gives: a byte cut through a multi-byte sequence
// produces invalid UTF-8, which protojson refuses to encode as a string at all
// — so one overlong message would fail the whole answer's marshalling rather
// than shorten its own row, on the surface (`-o json`, and MCP) where that
// matters most.
//
// The cut is stated in the text. A message silently shortened is a diagnosis a
// reader may act on believing they have all of it.
func boundedFailure(message string) string {
	if len(message) <= maxTimelineFailureBytes {
		return message
	}

	cut := maxTimelineFailureBytes
	for cut > 0 && !utf8.RuneStart(message[cut]) {
		cut--
	}

	return message[:cut] + "…(truncated)"
}

// summaryText reads the label the interpreter wrote onto a command.
//
// A summary travels as a payload, so this decodes one — the one payload on this
// path that is safe to read, because its content is chosen by the interpreter
// from step ids the schema constrains rather than by anything an author put in
// a task. See `engine/summary.go`.
//
// Through the *configured* converter, which is what makes a labelled timeline
// work on a deployment running a payload codec: the labels are encrypted with
// everything else, and this server holds the codec that reads them back. A
// second default converter built here would decode nothing there and the
// account would come back with every row unnamed — the guard in
// converter_guard_internal_test.go exists because that failure is silent.
//
// A decode failure is silence rather than an error, on
// [FlowstateServer.heartbeatPhase]'s reasoning: this is the caption on a row,
// and refusing to answer the whole question because one caption could not be
// read would be withholding the account over its labels.
func (s *FlowstateServer) summaryText(event *historypb.HistoryEvent) string {
	payload := event.GetUserMetadata().GetSummary()
	if payload == nil {
		return ""
	}

	var summary string
	if err := s.dataConverter.FromPayload(payload, &summary); err != nil {
		return ""
	}

	return summary
}
