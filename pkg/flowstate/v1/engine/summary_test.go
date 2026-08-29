package engine_test

import (
	"net/http"
	"sort"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	enumspb "go.temporal.io/api/enums/v1"
	"go.temporal.io/sdk/client"
	"go.temporal.io/sdk/converter"

	v1 "github.com/picatz/flowstate/pkg/flowstate/v1"
	"github.com/picatz/flowstate/pkg/flowstate/v1/engine"
	"github.com/picatz/flowstate/pkg/flowstate/v1/internal/conformance"
)

// TestEveryCommandInHistoryNamesItsStep runs a workflow that reaches every kind
// of command this engine writes and reads its own history back.
//
// The claim is the blanket one, and it is deliberately blanket: *no* activity
// scheduled and *no* timer started by this run is unlabelled. A test that
// listed the labels it expected to find would be a claim about the list — it
// would pass just as happily with a seventh dispatch site quietly writing
// nothing — and the whole value of the label is that an operator reading a
// history can assume every row has one.
//
// The exact labels are checked too, because "has a label" and "has the right
// label" are different failures and only the second is visible to a reader: a
// compensation labelled as the step it undoes reads as the step running twice.
func TestEveryCommandInHistoryNamesItsStep(t *testing.T) {
	t.Parallel()

	baseURL := conformance.NewHTTPServer(t)

	temporal := newTemporalNamespace(t)
	startWorker(t, temporal)

	echo := func(body string) *v1.Task {
		return &v1.Task{
			Name: "http",
			Inputs: map[string]*v1.Value{
				"method": v1.NewLiteral(http.MethodPost),
				"url":    v1.NewLiteral(baseURL + "/echo"),
				"body":   v1.NewLiteral(body),
			},
		}
	}

	// The failure is what makes the compensation run, and it has to fail once
	// rather than five times: the point of the run is its history, and four
	// extra attempts are four extra minutes of backoff for no extra event
	// shape.
	fails := &v1.Node{
		Id:     "boom",
		Policy: &v1.StepPolicy{Retry: &v1.RetryPolicy{MaxAttempts: 1}},
		Kind: &v1.Node_Task{Task: &v1.Task{
			Name: "http",
			Inputs: map[string]*v1.Value{
				"method": v1.NewLiteral(http.MethodGet),
				"url":    v1.NewLiteral(baseURL + "/status/500"),
			},
		}},
	}

	wf := &v1.Workflow{
		Name: "history_names_its_steps",

		// Present so the run's own `vars:` activity is scheduled: it is one of
		// the dispatches that is not a step's work, and the blanket claim
		// covers it too.
		Vars: map[string]*v1.Value{"greeting": v1.NewLiteral("hi")},

		Steps: []*v1.Node{
			{
				Id:   "forward",
				Kind: &v1.Node_Task{Task: echo("forward")},
				Undo: &v1.Compensation{Task: echo("undo-forward")},
			},

			// Two sibling loops whose body steps share an id, which the
			// language permits — a body's outputs do not escape, so `page` is
			// unique within each loop's visibility domain rather than within
			// the file (`refScope`, flowfile/validate.go). Labelling by id
			// alone leaves these two indistinguishable in history, which is
			// exactly as useful as no label (Codex, #1118). One iteration each,
			// because what is under test is telling the *loops* apart and a
			// second iteration only repeats a label.
			pages("first", echo("first-page")),
			pages("second", echo("second-page")),

			sleepStep("nap", time.Millisecond),
			signalStep("gate", "go", 30*time.Second),
			fails,
		},
	}

	run, err := temporal.ExecuteWorkflow(t.Context(),
		client.StartWorkflowOptions{
			ID:        "history-names-its-steps",
			TaskQueue: engine.RunTaskQueueName,
		},
		engine.Run, &v1.RunState{Workflow: wf})
	require.NoError(t, err)

	// Answered rather than left to lapse: a lapsed gate fails the run before
	// `boom` is reached and the compensation never runs. The timer bounding the
	// gate is started either way — that is the event under test — and answering
	// it means this test does not spend thirty seconds waiting for one.
	//
	// Only once the wait has been *reached*, which is why this counts timers
	// rather than sleeping first. A signal delivered before the step that
	// consumes it is ordinary and supported (the run drains it from its channel
	// and carries it), and it is exactly the wrong thing here: a wait satisfied
	// from a pending signal never creates the timer, so an earlier signal would
	// have this test assert the absence of the label it exists to check. The
	// gate's timer is the second this run starts — `nap` writes the first.
	require.Eventually(t, func() bool {
		return timersStarted(t, temporal, run.GetID()) >= 2
	}, 60*time.Second, 100*time.Millisecond, "the run never reached the gate")

	require.NoError(t, temporal.SignalWorkflow(t.Context(), run.GetID(), "", "go",
		&v1.SignalDelivery{Payload: &v1.Node_Outputs{}}))

	// The run fails: that is what `boom` is for, and the failure is what makes
	// the compensation run.
	require.Error(t, run.Get(t.Context(), nil), "the run was expected to fail so that its compensation runs")

	labelled, unlabelled := historyLabels(t, temporal, run.GetID(), run.GetRunID())

	assert.Empty(t, unlabelled,
		"every activity scheduled and every timer started has to say which step it came from; "+
			"these did not, so a run holding one of them reads in Temporal Web as an unnamed row")

	sort.Strings(labelled)
	assert.Equal(t, []string{
		"`boom`",
		"`first` > `page`",
		"`forward`",
		"`forward` · undo",
		"`gate` · wait timeout",
		"`nap` · sleep",
		"`second` > `page`",
		"run vars",
	}, labelled)
}

// pages is a loop of one iteration whose body step is called `page`, twice over
// with different loop ids — the sibling-visibility case in one helper so the two
// differ in nothing but the enclosing step.
func pages(id string, body *v1.Task) *v1.Node {
	return &v1.Node{
		Id: id,
		Kind: &v1.Node_ForEach{ForEach: &v1.ForEach{
			Items:    v1.NewLiteralList("only"),
			Iterator: "item",
			Body:     []*v1.Node{{Id: "page", Kind: &v1.Node_Task{Task: body}}},
		}},
	}
}

// timersStarted counts the timers a run has started so far, which is how this
// test knows a wait has been reached: a timer appears in history the moment the
// workflow task that created it is committed, and nothing else about a run
// waiting on a signal does.
func timersStarted(t *testing.T, temporal client.Client, workflowID string) int {
	t.Helper()

	history := temporal.GetWorkflowHistory(t.Context(), workflowID, "", false,
		enumspb.HISTORY_EVENT_FILTER_TYPE_ALL_EVENT)

	var started int
	for history.HasNext() {
		event, err := history.Next()
		if err != nil {
			return started
		}
		if event.GetEventType() == enumspb.EVENT_TYPE_TIMER_STARTED {
			started++
		}
	}

	return started
}

// historyLabels reads a finished run's history and splits the commands that
// carry a summary from the ones that do not.
//
// Only the two event types the engine's own commands produce are read.
// Everything else in a history — the workflow's own start, task scheduling, the
// signal — is written by the server or by a caller rather than by the
// interpreter, and holding those to a rule the interpreter enforces would make
// this test fail for something nobody here decides.
func historyLabels(t *testing.T, temporal client.Client, workflowID, runID string) (labelled, unlabelled []string) {
	t.Helper()

	history := temporal.GetWorkflowHistory(t.Context(), workflowID, runID, false,
		enumspb.HISTORY_EVENT_FILTER_TYPE_ALL_EVENT)

	for history.HasNext() {
		event, err := history.Next()
		require.NoError(t, err)

		var what string
		switch event.GetEventType() {
		case enumspb.EVENT_TYPE_ACTIVITY_TASK_SCHEDULED:
			what = "activity " + event.GetActivityTaskScheduledEventAttributes().GetActivityType().GetName()
		case enumspb.EVENT_TYPE_TIMER_STARTED:
			what = "timer " + event.GetTimerStartedEventAttributes().GetTimerId()
		default:
			continue
		}

		payload := event.GetUserMetadata().GetSummary()
		if payload == nil {
			unlabelled = append(unlabelled, what)
			continue
		}

		// The default converter, because this test's worker is built with it.
		// A deployment running a payload codec reads these back through its
		// codec server exactly as it reads every other payload — see
		// summary.go.
		var summary string
		require.NoError(t, converter.GetDefaultDataConverter().FromPayload(payload, &summary),
			"decoding the summary on %s", what)

		if summary == "" {
			unlabelled = append(unlabelled, what)
			continue
		}
		labelled = append(labelled, summary)
	}

	return labelled, unlabelled
}
