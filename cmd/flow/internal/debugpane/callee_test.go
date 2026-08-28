package debugpane_test

import (
	"strings"
	"testing"

	"github.com/charmbracelet/colorprofile"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/picatz/flowstate/cmd/flow/internal/debugpane"
	"github.com/picatz/flowstate/cmd/flow/internal/ui"
	v1 "github.com/picatz/flowstate/pkg/flowstate/v1"
	"github.com/picatz/flowstate/pkg/flowstate/v1/auth"
	"github.com/picatz/flowstate/pkg/flowstate/v1/flowdebug"
)

// A step id is not an identity across a `call:`, and the pane has to know it.
//
// A callee's step ids belong to the callee and not to its caller — `runCall`
// moves the run's position across that boundary for exactly this reason, "so a
// consumer of the runtime position [cannot confuse] equal step ids in two
// different workflow files" (`eval.go:1804-1812`) — while `WalkWorkflow`'s
// inventory flattens caller and callee into one list. Before this was carried
// through, a run held at the callee's `build` drew *both* `build` rows with the
// held marker, pointed the position at the caller's, and painted the caller's
// finished outcome onto the callee's row.
//
// Pointing at a step the run is not at is the one failure a debugger must never
// have, so all three are asserted, in the frame where the run is inside the
// callee.

// callingWorkflow is a caller and a callee that both declare `build`.
func callingWorkflow() *v1.Workflow {
	return &v1.Workflow{Name: "outer", Steps: []*v1.Node{
		markStep("build"),
		{Id: "nested", Kind: &v1.Node_Call{Call: &v1.Call{
			Workflow: &v1.Workflow{Name: "inner", Steps: []*v1.Node{markStep("build")}},
		}}},
	}}
}

// callingInventory is what `stepList` produces for it: the flattened walk, each
// entry against the workflow that declares it.
func callingInventory() []flowdebug.Step {
	return []flowdebug.Step{
		{Workflow: "outer", ID: "build"},
		{Workflow: "outer", ID: "nested"},
		{Workflow: "inner", ID: "build"},
	}
}

// insideTheCallee runs that workflow to the boundary inside the callee and
// returns the frame drawn there, plus its rendering.
//
// withRuntimePosition decides whether the run carries one at all: `flow run
// local` and `flow test` both install a [v1.TaskRuntime]
// (`cmd/flow/secrets.go:607`, `flowtest/run.go:615`), and an embedder driving
// [v1.Run] directly does not — which is the case the pane must degrade for
// rather than guess through.
func insideTheCallee(t *testing.T, withRuntimePosition bool) (debugpane.Frame, string) {
	t.Helper()

	caps := paneCapabilities(80, 24, colorprofile.NoTTY, true)
	layout := debugpane.Layout{Width: caps.Width, Height: caps.Height}
	theme := ui.NewTheme(true, caps)

	workflow := callingWorkflow()

	var (
		last  debugpane.Frame
		text  string
		found bool
	)

	var session *flowdebug.Session

	session, err := flowdebug.New(flowdebug.Options{
		// Three boundaries: the caller's build, the call itself, the callee's
		// build. The last is the one this is about.
		In:    strings.NewReader("step\nstep\nstep\ncontinue\n"),
		Out:   &strings.Builder{},
		Steps: callingInventory(),
		Emit: func(_ string, tone flowdebug.Tone) {
			if tone != flowdebug.ToneBreak {
				return
			}
			frame, paused := debugpane.Snapshot(t.Context(), session, layout)
			if !paused {
				return
			}
			last, found = frame, true
			text = debugpane.Render(frame, theme, caps.Symbols(), layout)
		},
	})
	require.NoError(t, err)
	t.Cleanup(func() { _ = session.Close() })

	ctx := v1.NewContextWithRegistry(t.Context(), paneRegistry(t))
	ctx = v1.NewContextWithDebugger(ctx, session)
	ctx = v1.NewContextWithRunObserver(ctx, session)
	if withRuntimePosition {
		ctx = v1.ContextWithTaskRuntime(ctx, v1.TaskRuntime{
			Step: auth.StepRef{Workflow: workflow.GetName(), Run: "run-1"},
		})
	}

	_, runErr := v1.Run(ctx, workflow)
	require.NoError(t, runErr)
	require.True(t, found, "the run never stopped, so there is no frame to assert about")

	return last, text
}

// TestThePaneHoldsTheCalleesStepNotTheCallers is the fix, in the frame where it
// matters.
func TestThePaneHoldsTheCalleesStepNotTheCallers(t *testing.T) {
	t.Parallel()

	frame, text := insideTheCallee(t, true)

	// The engine's own answer for where the run is, carried through to the
	// position rather than reconstructed here.
	assert.Equal(t, "build", frame.At.Step)
	assert.Equal(t, "inner", frame.At.Workflow,
		"the run is inside the callee and the position does not say so")

	// Exactly one row is held, and it is the callee's — index 2 of the
	// inventory, not index 0.
	require.Len(t, frame.Steps, 3, "the whole list fits, so the window is all of it")
	require.Equal(t, 2, frame.Held, "the pane points at the caller's step, not the one the run is in")
	assert.Equal(t, "inner", frame.Steps[frame.Held].Workflow)

	// And one row, not two: the marker is a property of the position, and the
	// position is one step.
	assert.Equal(t, 1, strings.Count(text, ui.Capabilities{Unicode: true}.Symbols().Arrow),
		"more than one row was marked as the step the run is held at")

	// The two rows sharing an id are drawn against the workflow that declares
	// them, so a reader can tell which is which.
	assert.Contains(t, text, "inner.build")
	assert.Contains(t, text, "outer.build")

	// `nested` is declared once, so it carries no qualifier: the id is the
	// name, and qualifying every row would be noise on the ordinary workflow.
	assert.NotContains(t, text, "outer.nested",
		"an unambiguous row was qualified, which makes the qualifier mean nothing")
}

// TestAnOutcomeNothingCanAttributeIsNotAttributed is the third symptom, and the
// one the fix answers by refusing rather than by resolving.
//
// [v1.RunObserver] hands over bare ids, so `StepFinished("build")` after the
// caller's step names both rows and belongs to one. The session cannot tell
// which, so it says so instead of painting the outcome onto the callee's row —
// fail closed, because under-claiming is a gap a reader can see and mis-claiming
// is a debugger reporting a step that never ran as finished.
func TestAnOutcomeNothingCanAttributeIsNotAttributed(t *testing.T) {
	t.Parallel()

	frame, text := insideTheCallee(t, true)

	require.Len(t, frame.Steps, 3)

	// The caller's build genuinely finished two boundaries ago. It still reads
	// pending, because nothing can say that the outcome was its and not the
	// callee's.
	assert.Equal(t, flowdebug.StepPending, frame.Steps[0].State,
		"an outcome was attributed to a row that shares its id across a call")

	// The held row is the exception: the position names it exactly.
	assert.Equal(t, flowdebug.StepRunning, frame.Steps[2].State)

	// `nested` is unambiguous, so its own state is reported as usual — the
	// refusal is scoped to the ids it is about, not to the list.
	assert.Equal(t, flowdebug.StepRunning, frame.Steps[1].State,
		"an unambiguous row lost its state to a rule about a different one")

	assert.Equal(t, 2, frame.StepsUnattributed)
	assert.Contains(t, text, "outcomes not attributed",
		"rows read pending for a reason the pane never gave")
}

// TestAPositionWithNoWorkflowPointsAtNothingRatherThanAtTheWrongStep is the
// degradation, and the reason the fix does not depend on a runtime position
// being installed.
//
// An embedder driving [v1.Run] with no [v1.TaskRuntime] gives the boundary no
// workflow to report. The list still holds two rows called `build`, and the
// honest answer is that the pane cannot say which — not the first one.
func TestAPositionWithNoWorkflowPointsAtNothingRatherThanAtTheWrongStep(t *testing.T) {
	t.Parallel()

	frame, text := insideTheCallee(t, false)

	assert.Empty(t, frame.At.Workflow, "a run with no runtime position reported one anyway")
	assert.Equal(t, -1, frame.Held,
		"with nothing to disambiguate them the pane picked a row anyway, which is the defect")

	assert.NotContains(t, text, ui.Capabilities{Unicode: true}.Symbols().Arrow,
		"a row was marked held on a position that cannot be placed")

	// The rows are still drawn, and still say what they are — a pane that
	// cannot point is not a pane that reports nothing.
	assert.Contains(t, text, "inner.build")
	assert.Contains(t, text, "outer.build")
	assert.Contains(t, text, "3 step(s)")
}

// TestAnOrdinaryWorkflowIsUnaffected is the positive control the three tests
// above need.
//
// Every claim they make is about a list containing two workflows. If the rules
// they pin also fired on a workflow with no `call:` in it, the pane would have
// stopped attributing outcomes and stopped qualifying nothing — which is a
// regression the tests above cannot see.
func TestAnOrdinaryWorkflowIsUnaffected(t *testing.T) {
	t.Parallel()

	caps := paneCapabilities(80, 24, colorprofile.NoTTY, true)
	layout := debugpane.Layout{Width: caps.Width, Height: caps.Height}

	frame := lastFrameOf(t, paneWorkflow(), paneScript, caps, flowdebug.Options{
		Steps: declared("release", "checkout", "build", "flaky", "gated", "deploy", "notify"),
	})

	assert.Zero(t, frame.StepsUnattributed,
		"a workflow with no call had rows treated as ambiguous")
	require.GreaterOrEqual(t, frame.Held, 0, "the pane could not place a position it should have")

	text := debugpane.Render(frame, ui.NewTheme(true, caps), caps.Symbols(), layout)
	assert.NotContains(t, text, "release.",
		"an unambiguous list was qualified, which is noise on every ordinary workflow")
	assert.Contains(t, text, "checkout ok", "an ordinary row stopped reporting its outcome")
}
