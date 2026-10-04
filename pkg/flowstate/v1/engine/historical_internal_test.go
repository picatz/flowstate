package engine

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	enumspb "go.temporal.io/api/enums/v1"
	historypb "go.temporal.io/api/history/v1"
	"google.golang.org/protobuf/types/known/timestamppb"
)

func taskEvent(id int64, kind enumspb.EventType, at time.Time) *historypb.HistoryEvent {
	return &historypb.HistoryEvent{EventId: id, EventType: kind, EventTime: timestamppb.New(at)}
}

func taskCompleted(id, started int64, at time.Time) *historypb.HistoryEvent {
	event := taskEvent(id, enumspb.EVENT_TYPE_WORKFLOW_TASK_COMPLETED, at)
	event.Attributes = &historypb.HistoryEvent_WorkflowTaskCompletedEventAttributes{
		WorkflowTaskCompletedEventAttributes: &historypb.WorkflowTaskCompletedEventAttributes{StartedEventId: started},
	}

	return event
}

// TestTheClockOfThePointsTaskSkipsTasksTheReplayDoesNotRun: a task that was
// started and then timed out, and a start the prefix ends at, are never run, so
// neither names the clock the point's own task reads.
func TestTheClockOfThePointsTaskSkipsTasksTheReplayDoesNotRun(t *testing.T) {
	t.Parallel()

	at := func(s int) time.Time { return time.Unix(1000+int64(s), 0) }
	const (
		started   = enumspb.EVENT_TYPE_WORKFLOW_TASK_STARTED
		timedOut  = enumspb.EVENT_TYPE_WORKFLOW_TASK_TIMED_OUT
		scheduled = enumspb.EVENT_TYPE_WORKFLOW_TASK_SCHEDULED
	)
	history := []*historypb.HistoryEvent{
		taskEvent(2, scheduled, at(1)),
		taskEvent(3, started, at(2)),
		taskCompleted(4, 3, at(3)),
		taskEvent(5, scheduled, at(4)),
		taskEvent(6, started, at(5)), // times out
		taskEvent(7, timedOut, at(6)),
		taskEvent(8, scheduled, at(7)),
		taskEvent(9, started, at(8)), // the prefix ends here
	}

	assert.True(t, lastTaskStart(history).Equal(at(2)),
		"the timed-out task and the unrun final start are not the point's task")
	assert.True(t, lastTaskStart(history[:3]).Equal(at(2)))
	assert.True(t, lastTaskStart(history[:2]).IsZero(), "no task has run yet")

	history = append(history, taskCompleted(10, 9, at(9)))
	assert.True(t, lastTaskStart(history).Equal(at(8)), "once the task completed it is the point's task")
}
