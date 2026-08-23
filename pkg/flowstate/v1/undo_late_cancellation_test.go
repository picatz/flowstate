package flowstatev1_test

import (
	"context"
	"log/slog"
	"testing"

	v1 "github.com/picatz/flowstate/pkg/flowstate/v1"
	"github.com/picatz/flowstate/pkg/flowstate/v1/internal/conformance"
	"github.com/stretchr/testify/require"
)

// The stop that arrives while the last step is succeeding, and why it is tested
// here rather than in `internal/conformance`.
//
// Every other cancellation case in this repository parks the run on an hour-long
// wait so the stop lands at a known point, and both drivers run all of them
// ([conformance.UndoCancellationCases]). This one cannot be written that way,
// because the whole of it is the *absence* of a park: the run has to be finishing
// its last step at the moment it is told to stop, so that the step succeeds, its
// scope reports nil, and nothing on the way out ever looks at the context again.
//
// The stop is delivered by the `log:` step itself, through the logger the run is
// given. That is deterministic where a timer would be raced: the step is running
// when the handler fires, and the `log:` task is the one task in the library that
// does not consult its context — so it returns success on a cancelled one, which
// is exactly the activity behaviour `engine/policy.go` sets `WaitForCancellation`
// to preserve. [conformance.UndoCancellationCase]'s doc says this window needs the
// driver to supply a third timing, and that is right about the *shared* corpus: it
// is the durable half that needs the new hook, not this one.
//
// The durable driver has the same window and the same guard, and Temporal's test
// environment cannot stage it. A cancellation it delivers lands in one of two
// places and neither is the window: during an activity, where the activity is
// cancelled, the step fails, and the run leaves through the failure path that
// already compensated before this change; or after the workflow has closed, where
// there is nothing left to decide. Both were tried against the tree this landed
// on, and both behave identically with the guard and without it. So this file
// covers one driver on purpose rather than by omission, and the durable half is a
// named follow-up — it needs an instrument the SDK's test environment does not
// expose today — rather than a case faked into the shared corpus, where an arm
// asserting a different outcome per driver would be worse than an absent one.

// cancelOnMessage stops a run the moment a `log:` step emits a chosen message.
//
// Keyed on the message rather than firing on any record, because a run logs for
// reasons of its own and a hook that stopped it on the first of those would be
// cancelling somewhere nobody chose.
type cancelOnMessage struct {
	message string
	cancel  context.CancelFunc
}

func (h cancelOnMessage) Enabled(context.Context, slog.Level) bool { return true }

func (h cancelOnMessage) Handle(_ context.Context, record slog.Record) error {
	if record.Message == h.message {
		h.cancel()
	}

	return nil
}

func (h cancelOnMessage) WithAttrs([]slog.Attr) slog.Handler { return h }

func (h cancelOnMessage) WithGroup(string) slog.Handler { return h }

// lateCancellationWorkflow is a run whose last step is a `log:` step, so that the
// stop can be landed inside it.
//
// `undo` chooses whether the step before it registers a compensation. Both cases
// end the same way — a stop is a stop — and what differs is whether there was
// anything to take back, which is the half the summary reports.
func lateCancellationWorkflow(name, base, message string, undo bool) *v1.Workflow {
	first := &v1.Node{
		Id: "first",
		Kind: &v1.Node_Task{Task: &v1.Task{
			Name: "http",
			Inputs: map[string]*v1.Value{
				"url":     v1.NewLiteral(base + "/do/a"),
				"outputs": v1.NewExpr(`{"said": response.body}`),
			},
		}},
	}

	if undo {
		first.Undo = &v1.Compensation{Task: &v1.Task{
			Name:   "http",
			Inputs: map[string]*v1.Value{"url": v1.NewExpr(`"` + base + `/do/undo-" + steps.first.said`)},
		}}
	}

	return &v1.Workflow{
		Name:    name,
		Profile: v1.CurrentProfile,
		// Declared so that the outputs claim below is a claim about something: a
		// run with no declared outputs makes "outputs were not evaluated"
		// vacuously true. This one resolves trivially on a completed run, so its
		// absence from a cancelled run's transcript can only be the guard.
		DeclaredOutputs: []*v1.OutputDeclaration{
			{Name: "said", Value: v1.NewExpr("steps.first.said")},
		},
		Steps: []*v1.Node{
			first,
			{
				Id: "stop",
				Kind: &v1.Node_Task{Task: &v1.Task{
					Name:   "log",
					Inputs: map[string]*v1.Value{"message": v1.NewLiteral(message)},
				}},
			},
		},
	}
}

// TestRunWorkflowUndoOnLateCancellation is the case the whole guard exists for.
//
// Before it, a run told to stop while its final step was succeeding reported
// COMPLETED and dropped every compensation those steps had registered — the
// resources stayed allocated and the run said the work was done, which is the one
// outcome compensation exists to prevent and the one sentence that stops anybody
// looking for what is still held.
//
// Asserted in the negative direction, which is the direction that fails when the
// guard is deleted: not "a cancelled run compensates" — the parked cases already
// say that, and they stayed green through the defect — but that this run does not
// report success, and that the undo actually reached the world. `Recorded` is the
// half a summary assertion cannot make: it is what the recording server saw.
func TestRunWorkflowUndoOnLateCancellation(t *testing.T) {
	const message = "the stop lands here"

	for _, test := range []struct {
		name     string
		undo     bool
		summary  string
		recorded []string
	}{
		{
			// The defect: `first` provisioned something and registered how to take
			// it back, the stop arrived while `stop` was succeeding, and the run
			// closed COMPLETED over a resource nobody was coming back for.
			name:     "a stop landing as the last step succeeds still takes the run back",
			undo:     true,
			summary:  `; compensation ran in reverse order: undid "first"`,
			recorded: []string{"a", "undo-a"},
		},
		{
			// The blast radius, asserted rather than left to be discovered. The
			// guard turns on the cancellation alone, so a run with no `undo:`
			// anywhere also stops reporting COMPLETED on this race: it takes
			// nothing back and invents no summary — there is nothing registered —
			// but it does close CANCELED and does not evaluate its declared
			// outputs. That is a status change for every workflow that predates
			// compensation, which is why it is a case here and a sentence in
			// docs/DSL.md rather than something met first in production.
			name:     "a stop landing as the last step succeeds cancels a run with nothing to take back",
			undo:     false,
			recorded: []string{"a"},
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			base, recorded := conformance.NewUndoServer(t)

			ctx, cancel := context.WithCancel(t.Context())
			defer cancel()

			ctx = v1.ContextWithLogger(ctx, slog.New(cancelOnMessage{message: message, cancel: cancel}))

			transcript, err := v1.Run(ctx, lateCancellationWorkflow("undo-late-cancel", base, message, test.undo))

			require.Error(t, err,
				"a run stopped while its last step succeeded reported success, with its compensations never run")

			// Still a stopped run rather than a failed one: compensating changes
			// what happened in the world, not what the run was.
			require.ErrorIs(t, err, context.Canceled,
				"a stopped run stopped reading as cancelled once it compensated: %v", err)

			if test.summary == "" {
				require.NotContains(t, err.Error(), "compensation ran",
					"a run with nothing registered reported compensating anyway")
			} else {
				require.Contains(t, err.Error(), test.summary,
					"the cancellation does not carry the account of what was compensated")
			}

			// The workflow declares an output that every completed run resolves,
			// so an empty RunOutputs here is the guard's doing and nothing
			// else's: a regression that evaluated declared outputs before
			// honouring the stop would populate it and fail this line.
			require.Nil(t, transcript.GetRunOutputs(),
				"a cancelled run's transcript carried run outputs, so the declared outputs were evaluated after the stop")

			require.Equal(t, test.recorded, recorded(),
				"the effects that happened, and their order, are not what stopping this run should have produced")
		})
	}
}
