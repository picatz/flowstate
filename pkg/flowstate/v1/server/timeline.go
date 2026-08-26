package server

import (
	"context"
	"fmt"

	"connectrpc.com/connect"
	enumspb "go.temporal.io/api/enums/v1"
	historypb "go.temporal.io/api/history/v1"

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
	// Higher than the entry bound by an order of magnitude on purpose: a run's
	// history holds several events per reportable one — a workflow task
	// scheduled, started and completed around each — so a budget equal to the
	// entry bound would answer a perfectly ordinary run with a truncated
	// account.
	maxTimelineScan = 50_000
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

	// Labels resolved as the walk goes. A terminal event carries only a
	// reference back to the scheduling that was labelled, so this is the join
	// while both are in reach; the reference itself is reported too, for a
	// reader whose answer was truncated between them.
	labels := map[int64]string{}

	history := temporal.GetWorkflowHistory(ctx, execution.GetWorkflowId(), execution.GetRunId(),
		// Never a long poll. Waiting for a new event would turn a read into a
		// held connection, on the one verb meant to be safe to point an agent
		// at unattended.
		false, enumspb.HISTORY_EVENT_FILTER_TYPE_ALL_EVENT)

	scanned := 0
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

		if next := event.GetWorkflowExecutionContinuedAsNewEventAttributes().GetNewExecutionRunId(); next != "" {
			out.NextRunId = next
		}

		entry := s.timelineEntry(event, labels)
		if entry == nil {
			continue
		}
		if entry.GetKind() == v1.TimelineEntry_KIND_RUN_ENDED ||
			entry.GetKind() == v1.TimelineEntry_KIND_RUN_CONTINUED {
			ended = true
		}
		out.Entries = append(out.Entries, entry)
	}

	// A closed run's history ends with an event saying how it ended. Reaching
	// none means this account stopped short — because a bound was hit, or
	// because the walk gave up on an empty page and reported nothing about it.
	// The caller is told either way rather than handed a prefix that reads
	// whole.
	if !ended && isClosed(getWorkflowExecutionStatus(described)) {
		out.Truncated = true
	}

	return connect.NewResponse(out), nil
}

// isClosed reports whether a run has finished, which is what makes "the history
// must reach an ending event" a checkable claim.
func isClosed(status v1.RunResponse_Status) bool {
	return status != v1.RunResponse_STATUS_UNSPECIFIED && status != v1.RunResponse_STATUS_RUNNING
}

// timelineEntry maps one history event to an entry, or to nil where the event
// is not something the workload did.
//
// The mapping is deliberately narrow. A history carries dozens of event types,
// most of them bookkeeping about workflow tasks and about the worker; a caller
// made to filter those is a caller reimplementing this, and one that did not
// would read a run's own scheduling as things the workload did.
func (s *FlowstateServer) timelineEntry(event *historypb.HistoryEvent, labels map[int64]string) *v1.TimelineEntry {
	entry := &v1.TimelineEntry{
		EventId: event.GetEventId(),
		Time:    event.GetEventTime(),
	}

	switch event.GetEventType() {
	case enumspb.EVENT_TYPE_ACTIVITY_TASK_SCHEDULED:
		entry.Kind = v1.TimelineEntry_KIND_STEP_SCHEDULED
		entry.Step = s.summaryText(event)
		entry.ScheduledEventId = event.GetEventId()
		// Recorded for the events that report how this work ended, which carry
		// a reference here and no label of their own.
		if entry.Step != "" {
			labels[event.GetEventId()] = entry.Step
		}

	case enumspb.EVENT_TYPE_ACTIVITY_TASK_COMPLETED:
		attrs := event.GetActivityTaskCompletedEventAttributes()
		entry.Kind = v1.TimelineEntry_KIND_STEP_COMPLETED
		entry.ScheduledEventId = attrs.GetScheduledEventId()
		entry.Step = labels[attrs.GetScheduledEventId()]

	case enumspb.EVENT_TYPE_ACTIVITY_TASK_FAILED:
		attrs := event.GetActivityTaskFailedEventAttributes()
		entry.Kind = v1.TimelineEntry_KIND_STEP_FAILED
		entry.ScheduledEventId = attrs.GetScheduledEventId()
		entry.Step = labels[attrs.GetScheduledEventId()]
		entry.Failure = attrs.GetFailure().GetMessage()

	case enumspb.EVENT_TYPE_ACTIVITY_TASK_TIMED_OUT:
		attrs := event.GetActivityTaskTimedOutEventAttributes()
		entry.Kind = v1.TimelineEntry_KIND_STEP_TIMED_OUT
		entry.ScheduledEventId = attrs.GetScheduledEventId()
		entry.Step = labels[attrs.GetScheduledEventId()]
		entry.Failure = attrs.GetFailure().GetMessage()

	case enumspb.EVENT_TYPE_ACTIVITY_TASK_CANCELED:
		attrs := event.GetActivityTaskCanceledEventAttributes()
		entry.Kind = v1.TimelineEntry_KIND_STEP_CANCELED
		entry.ScheduledEventId = attrs.GetScheduledEventId()
		entry.Step = labels[attrs.GetScheduledEventId()]

	case enumspb.EVENT_TYPE_ACTIVITY_TASK_STARTED:
		// The attempt lives here rather than on the scheduling: Temporal
		// schedules an activity once and starts it once per attempt, so this is
		// the only event that says which try is running. Reported as a
		// scheduled entry rather than a kind of its own, because "handed to a
		// worker, attempt 3" is the fact a reader wants and a separate started
		// row beside every scheduled row is noise.
		attrs := event.GetActivityTaskStartedEventAttributes()
		entry.Kind = v1.TimelineEntry_KIND_STEP_SCHEDULED
		entry.ScheduledEventId = attrs.GetScheduledEventId()
		entry.Step = labels[attrs.GetScheduledEventId()]
		entry.Attempt = attrs.GetAttempt()
		entry.Failure = attrs.GetLastFailure().GetMessage()

		// The first attempt is already reported by the scheduling itself, so
		// only a retry earns a row. This is what makes a stuck run legible
		// without making an ordinary one twice as long.
		if attrs.GetAttempt() <= 1 {
			return nil
		}

	case enumspb.EVENT_TYPE_TIMER_STARTED:
		entry.Kind = v1.TimelineEntry_KIND_TIMER_STARTED
		entry.Step = s.summaryText(event)
		// Recorded under this event's own id, which is what a TimerFired
		// refers back to — the same join an activity's terminal events make.
		if entry.Step != "" {
			labels[event.GetEventId()] = entry.Step
		}

	case enumspb.EVENT_TYPE_TIMER_FIRED:
		entry.Kind = v1.TimelineEntry_KIND_TIMER_FIRED
		entry.Step = labels[event.GetTimerFiredEventAttributes().GetStartedEventId()]

	case enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_SIGNALED:
		// Named, never carrying its payload: a signal's payload is somebody's
		// decision, and it is exactly the kind of thing a read surface must not
		// spread. The name is the fact a reader needs — which gate was
		// answered, and when.
		entry.Kind = v1.TimelineEntry_KIND_SIGNAL_RECEIVED
		entry.Step = event.GetWorkflowExecutionSignaledEventAttributes().GetSignalName()

	case enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_CONTINUED_AS_NEW:
		entry.Kind = v1.TimelineEntry_KIND_RUN_CONTINUED

	case enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_COMPLETED,
		enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_CANCELED,
		enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_TERMINATED,
		enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_TIMED_OUT:
		entry.Kind = v1.TimelineEntry_KIND_RUN_ENDED

	case enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_FAILED:
		entry.Kind = v1.TimelineEntry_KIND_RUN_ENDED
		entry.Failure = event.GetWorkflowExecutionFailedEventAttributes().GetFailure().GetMessage()

	default:
		return nil
	}

	return entry
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
