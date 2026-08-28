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
// withSecrets decides whether a [v1.TaskRuntime] is installed, and the answer
// must not matter. It used to be the only way the workflow reached the
// boundary, which made the whole qualification inert on an ordinary `flow run
// local --debug` — `cmd/flow/secrets.go` hands back the context untouched when
// neither secrets nor an identity broker is configured — and on every `flow
// test --debug`, whose runtime carries an empty `Step` (Codex, #1186). The
// engine stamps it now, so both values of this flag are the same run.
func insideTheCallee(t *testing.T, withSecrets bool) (debugpane.Frame, string) {
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
	if withSecrets {
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

// TestTheDefaultPathsAllReportTheWorkflow is the reachability claim, and it is
// the one the previous head failed.
//
// The qualification is worth nothing if it only works where secrets happen to
// be configured. `flow run local --debug` with no secret providers and no
// identity broker gets its context back untouched from `cmd/flow/secrets.go`,
// and `flow test --debug` gets a runtime whose `Step` is empty — so on both
// paths the boundary was told no workflow, `positionIn` read the shared id as
// ambiguous, and the pane marked no row at all. Fail-closed, and useless
// exactly where people are.
//
// Both values of the flag are asserted because the point is that it stopped
// mattering: the engine stamps the workflow, so a run with secrets and a run
// without are the same run as far as this is concerned.
func TestTheDefaultPathsAllReportTheWorkflow(t *testing.T) {
	t.Parallel()

	for _, withSecrets := range []bool{false, true} {
		frame, text := insideTheCallee(t, withSecrets)

		assert.Equal(t, "inner", frame.At.Workflow,
			"withSecrets=%v: the boundary reported no workflow, so nothing can be qualified", withSecrets)
		require.Equal(t, 2, frame.Held,
			"withSecrets=%v: the pane marked no row, which is what an unreported workflow looks like",
			withSecrets)
		assert.Contains(t, text, ui.Capabilities{Unicode: true}.Symbols().Arrow,
			"withSecrets=%v: no row carries the held marker", withSecrets)
	}
}

// TestASessionTheEngineNeverDrovePointsAtNothing is the degradation that
// remains, and it is now the only one.
//
// The two absences are told apart by who is running the workflow. A run the
// engine interprets always carries the workflow, whatever a deployment
// configured — that is the whole of the fix above. What is left is a
// [v1.Debugger] an embedder drives itself, through [flowdebug.Session.Control]
// or by calling BeforeStep directly: no engine ran, so no position was stamped,
// and the honest answer for a shared id is that the pane cannot say which row.
func TestASessionTheEngineNeverDrovePointsAtNothing(t *testing.T) {
	t.Parallel()

	caps := paneCapabilities(80, 24, colorprofile.NoTTY, true)
	layout := debugpane.Layout{Width: caps.Width, Height: caps.Height}

	session, err := flowdebug.New(flowdebug.Options{
		Controlled: true,
		Out:        &strings.Builder{},
		Steps:      callingInventory(),
	})
	require.NoError(t, err)
	t.Cleanup(func() { _ = session.Close() })

	scope := v1.NewScope(v1.CurrentProfile, &v1.Workflow_StepOutputs{
		StepValues: map[string]*v1.Node_Outputs{},
	})

	// Driven by hand, on a context the engine never touched — which is exactly
	// what an embedder holding the seam has.
	finished := make(chan error, 1)
	go func() { finished <- session.BeforeStep(t.Context(), markStep("build"), scope) }()

	_, err = session.WaitForPause(t.Context())
	require.NoError(t, err)

	frame, paused := debugpane.Snapshot(t.Context(), session, layout)
	require.True(t, paused)

	assert.Empty(t, frame.At.Workflow, "a context the engine never stamped reported a workflow anyway")
	assert.Equal(t, -1, frame.Held,
		"with nothing to disambiguate them the pane picked a row anyway, which is the defect")

	text := debugpane.Render(frame, ui.NewTheme(true, caps), caps.Symbols(), layout)
	assert.NotContains(t, text, ui.Capabilities{Unicode: true}.Symbols().Arrow,
		"a row was marked held on a position that cannot be placed")

	// The rows are still drawn, and still say what they are — a pane that
	// cannot point is not a pane that reports nothing.
	assert.Contains(t, text, "inner.build")
	assert.Contains(t, text, "outer.build")
	assert.Contains(t, text, "3 step(s)")

	require.NoError(t, session.Control(t.Context(), "continue"))
	require.NoError(t, <-finished)
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

// One callee, two call sites — the same defect one level deeper.
//
// A workflow *name* is not a declaration. Invoking one callee from two `call:`
// steps puts two rows in the inventory under one name, and grouping by name
// says the session can attribute an outcome that names one of two invocations
// (Codex, #1186). The identity is the walk's own descent, which is the engine's
// structure read statically: `runNodes` descends into a callee once per `call:`
// node (`eval.go:1734`).

// twiceCalledWorkflow invokes one callee from two call sites.
func twiceCalledWorkflow() *v1.Workflow {
	callee := func() *v1.Workflow {
		return &v1.Workflow{Name: "inner", Steps: []*v1.Node{markStep("build")}}
	}

	return &v1.Workflow{Name: "outer", Steps: []*v1.Node{
		{Id: "first_call", Kind: &v1.Node_Call{Call: &v1.Call{Workflow: callee()}}},
		{Id: "second_call", Kind: &v1.Node_Call{Call: &v1.Call{Workflow: callee()}}},
	}}
}

// twiceCalledInventory is what `stepList` produces for it: four rows, two of
// them `inner.build`, each against its own declaration.
func twiceCalledInventory() []flowdebug.Step {
	return []flowdebug.Step{
		{Workflow: "outer", Declaration: 0, ID: "first_call"},
		{Workflow: "inner", Declaration: 1, Via: "first_call", ID: "build"},
		{Workflow: "outer", Declaration: 0, ID: "second_call"},
		{Workflow: "inner", Declaration: 2, Via: "second_call", ID: "build"},
	}
}

// TestOneCalleeCalledTwiceIsTwoDeclarations is the fix.
//
// Grouped by name the two `build` rows are one declaration, so the observer's
// bare-id outcome lands on both and `Unattributed` reads zero — the session
// claiming an attribution it cannot make. Grouped by declaration they are two,
// which is what they are.
func TestOneCalleeCalledTwiceIsTwoDeclarations(t *testing.T) {
	t.Parallel()

	caps := paneCapabilities(80, 24, colorprofile.NoTTY, true)
	layout := debugpane.Layout{Width: caps.Width, Height: caps.Height}

	var (
		last  debugpane.Frame
		found bool
	)

	var session *flowdebug.Session

	session, err := flowdebug.New(flowdebug.Options{
		// Four boundaries; the last is the second callee's `build`.
		In:    strings.NewReader("step\nstep\nstep\nstep\ncontinue\n"),
		Out:   &strings.Builder{},
		Steps: twiceCalledInventory(),
		Emit: func(_ string, tone flowdebug.Tone) {
			if tone != flowdebug.ToneBreak {
				return
			}
			if frame, paused := debugpane.Snapshot(t.Context(), session, layout); paused {
				last, found = frame, true
			}
		},
	})
	require.NoError(t, err)
	t.Cleanup(func() { _ = session.Close() })

	ctx := v1.NewContextWithRegistry(t.Context(), paneRegistry(t))
	ctx = v1.NewContextWithDebugger(ctx, session)
	ctx = v1.NewContextWithRunObserver(ctx, session)
	_, runErr := v1.Run(ctx, twiceCalledWorkflow())
	require.NoError(t, runErr)
	require.True(t, found)

	// The first callee's `build` genuinely finished before the run reached the
	// second. Neither row may claim that outcome, because `StepFinished` named
	// only `build` and both rows answer to it.
	assert.Equal(t, 2, last.StepsUnattributed,
		"two invocations of one callee were grouped as one declaration, so an "+
			"outcome naming neither was attributed to both")

	require.Len(t, last.Steps, 4)
	for _, i := range []int{1, 3} {
		assert.Equal(t, flowdebug.StepPending, last.Steps[i].State,
			"row %d claimed an outcome that names two invocations", i)
	}

	// And the position cannot be placed: the boundary is told the callee's
	// *name* and nothing about which invocation is running, so the pane marks
	// no row rather than the wrong one.
	assert.Equal(t, "inner", last.At.Workflow)
	assert.Equal(t, -1, last.Held,
		"the pane pointed at one of two indistinguishable invocations")

	// The rows are still told apart on screen, by the call step an author
	// wrote — the one thing that differs when the name does not.
	text := debugpane.Render(last, ui.NewTheme(true, caps), caps.Symbols(), layout)
	assert.Contains(t, text, "first_call.build")
	assert.Contains(t, text, "second_call.build")
	assert.NotContains(t, text, "outer.first_call",
		"a row whose id nothing else carries was qualified anyway")
}

// TestTwoEmbeddedWorkflowsSharingANameAreTwoDeclarations is the other shape of
// the same defect, and it is the one a name can never fix: two genuinely
// different callees that happen to declare the same `name:`.
func TestTwoEmbeddedWorkflowsSharingANameAreTwoDeclarations(t *testing.T) {
	t.Parallel()

	session, err := flowdebug.New(flowdebug.Options{
		Out: &strings.Builder{},
		Steps: []flowdebug.Step{
			{Workflow: "shared", Declaration: 1, Via: "a", ID: "build"},
			{Workflow: "shared", Declaration: 2, Via: "b", ID: "build"},
		},
	})
	require.NoError(t, err)
	t.Cleanup(func() { _ = session.Close() })

	assert.Equal(t, 2, session.Steps(0, -1).Unattributed,
		"two workflows sharing a display name were read as one declaration")
}
