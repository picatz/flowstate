package server

import (
	"context"
	"errors"
	"fmt"
	"strings"

	"connectrpc.com/connect"
	enums "go.temporal.io/api/enums/v1"
	failurepb "go.temporal.io/api/failure/v1"
	historypb "go.temporal.io/api/history/v1"
	"google.golang.org/protobuf/proto"

	"github.com/picatz/flowstate/internal/textbound"
	v1 "github.com/picatz/flowstate/pkg/flowstate/v1"
	"github.com/picatz/flowstate/pkg/flowstate/v1/engine"
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
	// covers any history that can exist under those defaults with room to
	// spare, and the ordinary case is that the walk always reaches the end.
	//
	// It is also, transitively, the bound on *round trips*, and that is why it
	// is this number rather than a comfortable multiple of it. The SDK's
	// history iterator fetches at most one page per HasNext, and the loop calls
	// HasNext once per event consumed, so pages fetched never exceeds events
	// scanned. A peer answering with one event per page therefore costs one
	// request per event — bounded, but only by this — so every unit of headroom
	// here is a unit of request amplification a single authorized call can
	// spend (Codex, #1119).
	//
	// A tighter, *independent* request budget is what `list.go` has and what
	// this wants. It is still a follow-up, and no longer for the reason this
	// comment used to give — that reason has been overtaken and saying so is
	// the point of rewriting it, because a stale blocker sends the next reader
	// to build something that already exists. The Temporal namespace a raw
	// history request needs *is* available now:
	// [FlowstateServer.clientAndTemporalNamespaceFor] landed for exactly this
	// ask and is still waiting for its first caller.
	//
	// What blocks it is that a request budget is only safe where running out
	// leaves a *cursor*. `list.go` can do that because the SDK's public client
	// offers ListWorkflow as a raw request carrying NextPageToken, which
	// `list.go` hands straight back to the caller — so exhaustion there is a
	// short page and the work still gets done across calls. History is offered
	// through no such method. The only public constructor of a
	// [client.HistoryEventIterator] is GetWorkflowHistory
	// (`sdk@v1.47.0/client/client.go:1243`), which takes no token; the concrete
	// iterator and its `nexttoken` field are unexported
	// (`internal/internal_workflow_client.go:194-206`), and its one
	// construction site always begins at the first page (`:446`). An iterator
	// therefore cannot be resumed, and because resumption re-walks, a budget
	// spent here would make everything past requests × page-size *permanently*
	// unreachable rather than delayed — precisely the dead end the paragraph
	// above refuses, arriving through the fix for a different problem.
	//
	// Reaching the token means calling Temporal's raw
	// GetWorkflowExecutionHistory through [client.Client.WorkflowService], and
	// that gives up three things the SDK supplies from behind `internal/`,
	// where nothing here can import them: RawHistory blobs are deserialized by
	// `internal/common/serializer`, without which a deployment whose frontend
	// answers raw reads as an *empty* history; the per-attempt RPC timeout; and
	// the retry configuration, which the SDK passes as a context value under an
	// internal key — its interceptor is disabled by default
	// (`grpc_dialer.go:151-153`), so a raw call made without it is not retried
	// at all. Re-implementing those is a copy of SDK internals that fails
	// silently when the SDK moves.
	//
	// #1135 priced that trade and decided against it (2026-08-30): the
	// transitive bound above IS the round-trip bound, documented here where
	// the independent budget would have gone. What it concedes, sized rather
	// than felt: a peer answering one event per page costs one request per
	// event, so the worst case is maxTimelineScan round trips — and the peer
	// who could choose that shape is the Temporal substrate, which the threat
	// model already trusts with every payload it stores (THREAT_MODEL.md's
	// substrate-access paragraph). A conforming server's round trips are
	// events over page size. The standing ask is upstream: token access on
	// the SDK's history iterator, which would make a real budget one field
	// and one loop bound. Revisit there, not here.
	//
	// A deployment that raised Temporal's own cap far enough gets a truncated
	// answer rather than a wrong one, and an empty truncated answer says so
	// plainly — see [v1.GetTimelineResponse.Truncated].
	maxTimelineScan = 60_000

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

// getTimeline reports what a run did, event by event; [FlowstateServer.GetTimeline]
// decides what of its failure text the caller may read.
func (s *FlowstateServer) getTimeline(
	ctx context.Context, req *connect.Request[v1.GetTimelineRequest],
) (*connect.Response[v1.GetTimelineResponse], error) {
	if err := v1.Validate(req.Msg); err != nil {
		return nil, connect.NewError(connect.CodeInvalidArgument, err)
	}

	// Authorized before anything is read, and through the client the check was
	// made with — see [FlowstateServer.authorizeRun]. A history is the whole
	// account of a workload, so a timeline readable by whoever guessed an id
	// would be a larger disclosure than Get's rather than a smaller one.
	// Refused before the run is even addressed, because it is a property of the
	// request rather than of anything the server would find. Event ids restart
	// at 1 in every segment, so a cursor counts within one and says nothing
	// without it — and an empty run id resolves to *the latest*, which is a
	// different segment the moment the workload continues as new between two
	// calls. The old cursor applied to the new segment silently skips its
	// beginning or mixes two segments into one account (Codex, #1119).
	if req.Msg.GetAfterEventId() > 0 && req.Msg.GetRunId() == "" {
		return nil, connect.NewError(connect.CodeInvalidArgument, errors.New(
			"after_event_id counts within one run, and event ids restart in every "+
				"segment of a workload — so name the segment with run_id. The answer "+
				"you are continuing reports it as run_id; resuming without it would "+
				"read the latest segment, which may not be the one you were reading"))
	}

	temporal, described, err := s.authorizeRun(ctx, "GetTimeline", req.Msg.GetWorkflowId(), req.Msg.GetRunId())
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

	// Always the resolved run rather than the one the caller named, which is
	// the point: an empty run id means "the latest", and a caller who asked
	// that way needs to be told what it resolved to before they can resume.
	out := &v1.GetTimelineResponse{RunId: execution.GetRunId()}

	history := temporal.GetWorkflowHistory(ctx, execution.GetWorkflowId(), execution.GetRunId(),
		// Never a long poll. Waiting for a new event would turn a read into a
		// held connection, on the one verb meant to be safe to point an agent
		// at unattended.
		false, enums.HISTORY_EVENT_FILTER_TYPE_ALL_EVENT)

	scanned, ended, err := s.walkTimeline(history, out, limit, req.Msg.GetAfterEventId(), maxTimelineBytes)
	if err != nil {
		return nil, err
	}

	// Whether the walk reached the end of what was there, checked two ways
	// because neither covers the other.
	//
	// The count is the general one and the only one a *running* segment has:
	// Describe reported how many events the history held a moment ago, and a
	// walk that read fewer than that — without stopping for a bound of its own
	// — gave up early. The SDK's history iterator does exactly that on an empty
	// page, silently (Codex, #1119). A running segment's history grows after
	// Describe, so reading *more* than it reported is ordinary and only reading
	// fewer is evidence.
	//
	// The ending event is the second, and it catches what a count cannot: a
	// history of the right length whose last event is not the one a finished
	// segment must end with. It applies only to a closed segment, since a
	// running one has no ending to reach yet.
	if !out.Truncated {
		info := described.GetWorkflowExecutionInfo()

		switch {
		case info.GetHistoryLength() > 0 && int64(scanned) < info.GetHistoryLength():
			out.Truncated = true
		case !ended && segmentClosed(info.GetStatus()):
			out.Truncated = true
		}
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
//
// occurrence is the label's ordinal among the schedulings the walk has seen,
// carried so an ending reports the same number as its scheduling; zero when
// the walk was not counting or had reached [maxTimelineLabels]. lease is set on
// a timer that is a debug lease, so its ending is reported as the lease ending.
type activityInFlight struct {
	label      string
	attempt    int32
	occurrence int32
	lease      *debugLeaseTimer
}

// debugLeaseTimer is what a lease timer's summary names, kept until the timer
// closes so the closing row carries it too.
type debugLeaseTimer struct {
	session string
	actor   string
}

const (
	// maxTimelineLabels bounds the distinct step labels one walk counts
	// occurrences for. The walk is already bounded by [maxTimelineScan] events;
	// this keeps the counting map to a fixed size regardless of how many
	// distinct labels a history holds. Labels past it are reported with
	// occurrence zero rather than guessed.
	maxTimelineLabels = 4096

	// maxTimelineActorBytes and maxTimelineSessionBytes bound the lease text
	// lifted out of a timer summary. The writer already bounds the holder; a
	// history can also come from another writer, so the read bounds again.
	maxTimelineActorBytes   = 320
	maxTimelineSessionBytes = 128
)

// boundedLeaseText cleans and cuts one value lifted from a lease summary.
func boundedLeaseText(s string, limit int) string {
	return textbound.Cut(strings.ToValidUTF8(s, ""), limit)
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
	return s.timelineEntryCounted(event, inFlight, nil)
}

// timelineEntryCounted is [FlowstateServer.timelineEntry] that also numbers
// each step's schedulings by label in occurrences, a map owned by the walk. A
// nil map counts nothing and leaves every occurrence zero.
func (s *FlowstateServer) timelineEntryCounted(
	event *historypb.HistoryEvent, inFlight map[int64]*activityInFlight, occurrences map[string]int32,
) *v1.TimelineEntry {
	entry := &v1.TimelineEntry{
		EventId: event.GetEventId(),
		Time:    event.GetEventTime(),
	}

	// ended closes out an activity: it names the row from what the walk
	// collected at the scheduling and at the latest start, then forgets it,
	// which is what keeps the map to work in flight.
	//
	// attempted says whether this ending is about a *run* of the activity. It
	// is, for everything that can only happen to something that started — a
	// completion, a terminal failure — and the number is the latest start's,
	// which is the one that was running, because Temporal runs one attempt at a
	// time.
	ended := func(scheduled int64, attempted bool) {
		entry.ScheduledEventId = scheduled
		if work, ok := inFlight[scheduled]; ok {
			entry.Step = work.label
			entry.Occurrence = work.occurrence
			if attempted {
				entry.Attempt = work.attempt
			}
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
		work := &activityInFlight{label: entry.Step, attempt: 1}
		if occurrences != nil {
			if _, seen := occurrences[entry.Step]; seen || len(occurrences) < maxTimelineLabels {
				occurrences[entry.Step]++
				work.occurrence = occurrences[entry.Step]
			}
		}
		entry.Occurrence = work.occurrence
		inFlight[event.GetEventId()] = work

	case enums.EVENT_TYPE_ACTIVITY_TASK_COMPLETED:
		entry.Kind = v1.TimelineEntry_KIND_STEP_COMPLETED
		ended(event.GetActivityTaskCompletedEventAttributes().GetScheduledEventId(), true)

	case enums.EVENT_TYPE_ACTIVITY_TASK_FAILED:
		attrs := event.GetActivityTaskFailedEventAttributes()
		entry.Kind = v1.TimelineEntry_KIND_STEP_FAILED
		ended(attrs.GetScheduledEventId(), true)
		entry.Failure = s.failureMessage(attrs.GetFailure())

	case enums.EVENT_TYPE_ACTIVITY_TASK_TIMED_OUT:
		attrs := event.GetActivityTaskTimedOutEventAttributes()
		entry.Kind = v1.TimelineEntry_KIND_STEP_TIMED_OUT
		// True here on the reading that a timeout with no start is a
		// schedule-to-start timeout, where the attempt that timed out is the
		// one being *scheduled* — a number the walk does not hold, since it
		// only learns an attempt from a start. Reporting the last started
		// attempt is wrong there and so is reporting none, so this keeps the
		// existing answer rather than trading one wrong number for another;
		// see the follow-up noted on #1119.
		ended(attrs.GetScheduledEventId(), true)
		entry.Failure = s.failureMessage(attrs.GetFailure())

	case enums.EVENT_TYPE_ACTIVITY_TASK_CANCELED:
		attrs := event.GetActivityTaskCanceledEventAttributes()
		entry.Kind = v1.TimelineEntry_KIND_STEP_CANCELED
		// The one ending that can happen to an activity which is not running.
		// A cancellation delivered while a step sits in retry backoff ends the
		// *activity*, and the last attempt to have started ended some time
		// earlier and ended by failing — so copying that number onto this row
		// says attempt 3 was cancelled when attempt 3 had already failed.
		//
		// Asked of the event rather than inferred from the walk:
		// `started_event_id` is "the id of the ACTIVITY_TASK_STARTED event this
		// cancel confirmation corresponds to", so an unset one is the event
		// itself saying it corresponds to no run of the activity. Then no
		// attempt is claimed, which the schema already spells as zero — the
		// same "this row is not about an attempt" a run-level failure uses
		// (#1119).
		ended(attrs.GetScheduledEventId(), attrs.GetStartedEventId() != 0)

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
			entry.Occurrence = work.occurrence
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
		entry.Failure = s.failureMessage(attrs.GetLastFailure())

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
			work := &activityInFlight{label: entry.Step}
			// A debug lease is told from every other timer by its summary
			// alone, parsed with the engine's own spelling of it. A summary
			// that does not parse stays an ordinary timer.
			if session, holder, ok := engine.ParseDebugLeaseSummary(entry.Step); ok {
				work.lease = &debugLeaseTimer{
					session: boundedLeaseText(session, maxTimelineSessionBytes),
					actor:   boundedLeaseText(holder, maxTimelineActorBytes),
				}
				entry.Kind = v1.TimelineEntry_KIND_DEBUG_PAUSED
				entry.SessionId = work.lease.session
				entry.Actor = work.lease.actor
			}
			inFlight[event.GetEventId()] = work
		}

	case enums.EVENT_TYPE_TIMER_FIRED:
		entry.Kind = v1.TimelineEntry_KIND_TIMER_FIRED
		if work, ok := inFlight[event.GetTimerFiredEventAttributes().GetStartedEventId()]; ok {
			entry.Step = work.label
			if work.lease != nil {
				entry.Kind = v1.TimelineEntry_KIND_DEBUG_RESUMED
				entry.SessionId, entry.Actor, entry.EndReason = work.lease.session, work.lease.actor, endReasonLapsed
			}
			delete(inFlight, event.GetTimerFiredEventAttributes().GetStartedEventId())
		}

	case enums.EVENT_TYPE_TIMER_CANCELED:
		// A row, because the wait it closes was one: a reader folding rows by
		// label sees `<step> · wait timeout` begin and, without this, never end
		// when the signal wins, so a gate that was answered reads as waiting
		// forever on a run that succeeded. The signal row cannot stand in for it,
		// as it is named for the signal rather than the step. Labelled from the
		// start it closes, then forgotten, or a run parked and released many
		// times would carry every lapsed timer.
		entry.Kind = v1.TimelineEntry_KIND_TIMER_CANCELED
		started := event.GetTimerCanceledEventAttributes().GetStartedEventId()
		if work, ok := inFlight[started]; ok {
			entry.Step = work.label
			if work.lease != nil {
				entry.Kind = v1.TimelineEntry_KIND_DEBUG_RESUMED
				entry.SessionId, entry.Actor, entry.EndReason = work.lease.session, work.lease.actor, endReasonReleased
			}
			delete(inFlight, started)
		}

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
		entry.Failure = s.failureMessage(event.GetWorkflowExecutionFailedEventAttributes().GetFailure())

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
	return timelineFitsWithin(maxTimelineBytes, assembled, size, entries)
}

// timelineFitsWithin is [timelineFits] against a given budget, so a test can
// reach the bound with a few rows.
func timelineFitsWithin(budget, assembled, size, entries int) bool {
	return entries == 0 || assembled+size <= budget
}

// refitTimeline reports how many of entries fit the answer's byte bound, by
// the rule the assembly applied. Redaction runs after assembly and can
// lengthen a failure, so what fit then is measured again; the entries past the
// bound are cut, which the caller reports as a truncation and a resumption
// reads again.
func refitTimeline(entries []*v1.TimelineEntry) int {
	assembled := 0
	for i, entry := range entries {
		size := proto.Size(entry)
		if !timelineFits(assembled, size, i) {
			return i
		}
		assembled += size
	}
	return len(entries)
}

// failureMessage is what a failure says, read through the deployment's own
// converter and cut to [maxTimelineFailureBytes].
//
// The decode is not an optimisation. When a payload codec is configured,
// Flowstate turns on the SDK's failure encoding with it (see
// `payloadcodec.Config.FailureConverter`), which is what keeps a rejected
// value out of history in the clear — and it does that by moving the real
// message into `encoded_attributes` and writing the literal string "Encoded
// failure" in its place. So reading `GetMessage()` on such a deployment
// reports "Encoded failure" for every failure in the account: the timeline
// would be structurally perfect and diagnostically empty on exactly the
// deployments that care most about what their runs did (Codex, #1119).
//
// Only when something was encoded, so a deployment without a codec takes the
// path it always took. And through [converter.DecodeCommonFailureAttributes]
// rather than the failure converter's FailureToError, because that builds the
// whole error chain and its Error() would then carry every level's text — the
// one thing [v1.TimelineEntry.Failure] promises not to report.
//
// On a copy, because the failure belongs to a history event this walk is
// reading rather than owning.
func (s *FlowstateServer) failureMessage(failure *failurepb.Failure) string {
	return boundedFailure(s.decodedFailureMessage(failure))
}

// decodedFailureMessage is the decode without the bound, for the reader that
// needs the first and must not have the second.
//
// [FlowstateServer.pendingActivities] reports a handful of retrying steps on a
// response about one run, not thousands of entries assembled against a byte
// budget — so the aggregate the timeline's cap exists for does not arise there,
// and quietly shortening a message [v1.PendingActivity.LastFailure] has always
// returned whole would be changing that RPC's contract as a side effect of
// fixing a different defect. Whether it should be bounded too is a real
// question and a separate one.
func (s *FlowstateServer) decodedFailureMessage(failure *failurepb.Failure) string {
	if failure == nil {
		return ""
	}
	if failure.GetEncodedAttributes() == nil {
		return failure.GetMessage()
	}

	// Decoded here rather than through [converter.DecodeCommonFailureAttributes],
	// which is the awkward part of this seam and worth being explicit about.
	// That helper returns nothing and swallows its own decode error, leaving
	// the message exactly as it found it — the SDK's sentinel, "Encoded
	// failure" — so a failed decode is silent, and reporting what it left
	// behind hands a reader a placeholder that reads like a diagnosis. Now
	// that almost every failure decodes, the few that do not are the ones a
	// reader would trust (Codex, #1119).
	//
	// The decode fails for real reasons: a key rotated since the run, a codec
	// the operator has since reconfigured, a payload written by a deployment
	// this one is not, an older record whose shape has moved on.
	//
	// Three checks were tried and two do not work. Comparing the message
	// before and after is a heuristic that mis-fires on a workload whose
	// failure message is literally the sentinel. Asking whether the decode
	// cleared `encoded_attributes` fails against this SDK, which never clears
	// them, so every encoded failure would read as unreadable. And decoding
	// into a target that accepts any JSON proves only that the bytes
	// *decrypted*, not that they hold what the SDK writes — a record whose
	// `message` is a number passes that and then decodes to nothing.
	//
	// So the shape is named and the decode that answers is the decode that is
	// reported. The cost is a copy of two field names the SDK keeps
	// unexported; what makes that safe is that
	// TestAnEncodedFailureStillSaysWhatWentWrong round-trips through the SDK's
	// own encoder, so a release that renamed either one fails there rather
	// than quietly reporting every failure as empty.
	var attributes struct {
		Message    *string `json:"message"`
		StackTrace string  `json:"stack_trace"`
	}
	if err := s.dataConverter.FromPayload(failure.GetEncodedAttributes(), &attributes); err != nil {
		return unreadableFailure
	}

	// A pointer, because a plain string cannot tell an absent field from an
	// empty one. `{}` and `{"message": null}` decode without error and leave
	// it zero, so a corrupted or version-skewed record would read as an
	// ordinary failure that happened to carry no message — which is a fact
	// about the workload rather than about this deployment's ability to read
	// it, and the wrong one (Codex, #1119).
	if attributes.Message == nil {
		return unreadableFailure
	}

	return *attributes.Message
}

// unreadableFailure is what a failure says when this deployment cannot read it.
//
// A sentence rather than silence, because a reader has to be able to tell "this
// failure carried no message" from "there is a message and it is not readable
// here" — the second names something an operator can go and fix, and the first
// does not.
const unreadableFailure = "(failure message unavailable: encoded with a codec this server cannot read)"

// boundedFailure is a failure's message, cut to [maxTimelineFailureBytes] by
// [textbound.Cut], so the cut cannot leave invalid UTF-8 for `-o json` and MCP
// to refuse.
//
// The cut is stated in the text rather than with textbound's bare "...": a
// message silently shortened is a diagnosis a reader may act on believing they
// have all of it.
func boundedFailure(message string) string {
	if len(message) <= maxTimelineFailureBytes {
		return message
	}

	return textbound.Cut(message, maxTimelineFailureBytes) + v1.TruncatedSuffix
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

// maxLeaseJoinBuffer bounds the rows held behind a withheld lease cancel. Past
// it the cancel is reported as a real end, which is the safe direction.
const maxLeaseJoinBuffer = 16

// leaseJoin folds the lease timer's re-arming out of the account.
//
// The engine re-arms the lease timer on every wake (see the loop in
// engine/debuglease.go), so a renewal, a refused ask or a resume from a
// non-holder is recorded as a TimerCanceled followed at once by a new lease
// TimerStarted for the same session. Reported as they are, those read as the
// run resuming and being paused again when it never moved. The join holds a
// cancel back until a later row decides it: a re-arm for the same session
// makes the cancel and the new start disappear into one continuous pause, and
// anything else makes the held row a real end.
//
// Rows must reach the caller in event-id order, because a client resumes with
// `after` set to the last id it read and the server skips everything at or
// below it. So the debug signals and same-session pacing rows that arrive while
// a cancel is held are buffered behind it, and are released after it (or, on a
// re-arm, alone) in the order they were read. Nothing jumps ahead of an earlier
// row, and none is lost or repeated.
//
// Bounded: at most one withheld row and [maxLeaseJoinBuffer] buffered rows.
type leaseJoin struct {
	held     string
	pending  *v1.TimelineEntry
	buffered []*v1.TimelineEntry
}

// flush releases the withheld row, then what was buffered behind it, in order.
// The withheld row is a real end.
func (j *leaseJoin) flush() []*v1.TimelineEntry {
	if j.pending == nil {
		return nil
	}

	out := append([]*v1.TimelineEntry{j.pending}, j.buffered...)
	j.pending, j.buffered, j.held = nil, nil, ""

	return out
}

// continues reports whether a row may sit inside an unbroken pause: a debug
// ask arriving, or a timer pacing the held session's backlog.
func (j *leaseJoin) continues(entry *v1.TimelineEntry) bool {
	switch entry.GetKind() {
	case v1.TimelineEntry_KIND_SIGNAL_RECEIVED:
		return entry.GetStep() == v1.DebugSignal
	case v1.TimelineEntry_KIND_TIMER_STARTED, v1.TimelineEntry_KIND_TIMER_FIRED, v1.TimelineEntry_KIND_TIMER_CANCELED:
		session, ok := engine.ParseDebugBacklogSummary(entry.GetStep())

		return ok && session == j.held
	default:
		return false
	}
}

// push takes the next row and returns the rows to report, in order.
func (j *leaseJoin) push(entry *v1.TimelineEntry) []*v1.TimelineEntry {
	switch {
	case entry.GetKind() == v1.TimelineEntry_KIND_DEBUG_RESUMED && entry.GetEndReason() == endReasonReleased:
		out := j.flush()
		j.pending = entry
		j.held = entry.GetSessionId()

		return out

	case entry.GetKind() == v1.TimelineEntry_KIND_DEBUG_RESUMED:
		out := append(j.flush(), entry)
		j.held = ""

		return out

	case entry.GetKind() == v1.TimelineEntry_KIND_DEBUG_PAUSED:
		if j.pending != nil && entry.GetSessionId() == j.held {
			// The same hold, re-armed: the cancel and this start are dropped
			// and what arrived between them is reported.
			out := j.buffered
			j.pending, j.buffered = nil, nil

			return out
		}
		out := append(j.flush(), entry)
		j.held = entry.GetSessionId()

		return out

	case j.pending != nil && j.continues(entry):
		if len(j.buffered) < maxLeaseJoinBuffer {
			j.buffered = append(j.buffered, entry)

			return nil
		}

		// Too much to hold behind one cancel: it is reported as the end.
		return append(j.flush(), entry)

	case j.pending == nil && j.continues(entry):
		return []*v1.TimelineEntry{entry}

	default:
		// Any other row means the run moved on.
		out := append(j.flush(), entry)
		j.held = ""

		return out
	}
}

// The two spellings of [v1.TimelineEntry.EndReason].
const (
	endReasonReleased = "released"
	endReasonLapsed   = "lapsed"
)

// historyIterator is the part of Temporal's history iterator the walk reads.
type historyIterator interface {
	HasNext() bool
	Next() (*historypb.HistoryEvent, error)
}

// walkTimeline reads a history into out, resuming past `after` and stopping at
// `limit` entries or the byte budget, and reports how many events it examined
// and whether it saw the run's own ending.
func (s *FlowstateServer) walkTimeline(
	history historyIterator, out *v1.GetTimelineResponse, limit int, after int64, budget int,
) (scanned int, ended bool, err error) {
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

	// Schedulings seen per step label, for [v1.TimelineEntry.Occurrence]. Held
	// for the whole walk since an ordinal counts every earlier scheduling, and
	// bounded by [maxTimelineLabels].
	occurrences := map[string]int32{}

	assembled := 0

	// emit reports one row, or false when the answer is full.
	emit := func(entry *v1.TimelineEntry) bool {
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
			return true
		}

		// Counted as the answer is assembled rather than measured afterwards,
		// because a response refused for being too large is a question nobody
		// gets an answer to. Stopping short is a truncation, which this already
		// has a way to report — and never on the first entry, or a single
		// oversized row would make a run unreadable rather than clipped.
		size := proto.Size(entry)
		if !timelineFitsWithin(budget, assembled, size, len(out.Entries)) || len(out.Entries) >= limit {
			out.Truncated = true

			return false
		}
		assembled += size

		out.Entries = append(out.Entries, entry)

		return true
	}

	var join leaseJoin

	for history.HasNext() {
		if scanned >= maxTimelineScan || len(out.Entries) >= limit {
			out.Truncated = true
			break
		}

		event, err := history.Next()
		if err != nil {
			return 0, false, connect.NewError(connect.CodeInternal, fmt.Errorf("reading run history: %w", err))
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

		entry := s.timelineEntryCounted(event, inFlight, occurrences)
		if entry == nil {
			continue
		}

		// Through the lease join, which may hold a row back or drop one, so
		// what is reported can be more or fewer rows than the walk read.
		stop := false
		for _, row := range join.push(entry) {
			if !emit(row) {
				stop = true

				break
			}
		}
		if stop {
			break
		}
	}

	// A release still held back at the end of the history had no re-arm after
	// it: it is a real end. Not flushed after a truncation, because the rows
	// past the cut are unread and a resumption derives this one again.
	if !out.Truncated {
		for _, row := range join.flush() {
			if !emit(row) {
				break
			}
		}
	}

	return scanned, ended, nil
}
