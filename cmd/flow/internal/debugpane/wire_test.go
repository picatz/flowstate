package debugpane_test

import (
	"strings"
	"testing"

	"github.com/charmbracelet/colorprofile"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/picatz/flowstate/cmd/flow/internal/debugpane"
	v1 "github.com/picatz/flowstate/pkg/flowstate/v1"
	"github.com/picatz/flowstate/pkg/flowstate/v1/flowdebug"
)

// One session, two renderings of it, held to saying the same thing.
//
// This is the conformance shape the drivers already live by, applied one layer
// out: the local pane and the wire messages are two fronts over one session
// core, and a front that disagrees with another about what the run is doing is
// the two-drivers problem wearing different clothes (#928's binding constraint,
// and the reason `debug.proto` is a bridge rather than a second model).
//
// It is deliberately not a golden file. What is pinned is that the two agree,
// so a change to the session's answers moves both and this stays green, while a
// change to one of the two fronts alone fails here — which is the only failure
// worth catching.

// wireAgreement is what the two fronts said at one stop.
type wireAgreement struct {
	frame    debugpane.Frame
	position *v1.DebugPosition
	window   *v1.DebugStepWindow
	scope    *v1.DebugScope
}

// bothFronts drives a workflow under a scripted session and returns, for every
// stop, the pane's frame and the messages built from the same held run.
//
// Both are taken inside one [flowdebug.ToneBreak], so they describe the same
// pause rather than two pauses that happen to look alike. The window the
// messages are asked for is the *frame's own* — a comparison against a window
// of this test's choosing would be comparing two different questions.
func bothFronts(t *testing.T, workflow *v1.Workflow, inputs map[string]*v1.Value, script string, opts flowdebug.Options, layout debugpane.Layout) []wireAgreement {
	t.Helper()

	var (
		session *flowdebug.Session
		stops   []wireAgreement
	)

	opts.In = strings.NewReader(script)
	opts.Out = &strings.Builder{}
	opts.Emit = func(_ string, tone flowdebug.Tone) {
		if tone != flowdebug.ToneBreak {
			return
		}

		frame, paused := debugpane.Snapshot(t.Context(), session, layout)
		if !paused {
			return
		}

		position, held := session.PositionProto()
		require.True(t, held, "the pane read a pause the messages did not")

		scope, err := session.ScopeProto(t.Context(), debugpane.MaxScopeEvaluations)
		require.NoError(t, err)

		stops = append(stops, wireAgreement{
			frame:    frame,
			position: position,
			window:   session.StepWindowProto(frame.StepsBefore, len(frame.Steps)),
			scope:    scope,
		})
	}

	var err error
	session, err = flowdebug.New(opts)
	require.NoError(t, err)
	t.Cleanup(func() { _ = session.Close() })

	ctx := v1.NewContextWithRegistry(t.Context(), paneRegistry(t))
	ctx = v1.NewContextWithDebugger(ctx, session)
	ctx = v1.NewContextWithRunObserver(ctx, session)

	_, _ = v1.RunWithInputs(ctx, workflow, inputs)

	require.NotEmpty(t, stops, "the run never stopped, so nothing was compared")

	return stops
}

// agrees reports whether the pane's rendering of a value is the wire's, allowing
// for the pane's own display cut.
//
// The two caps are deliberately different and both are right: the wire's is
// `flowdebug.MaxInspectRunes`, which is how much text an answer may be, and the
// pane's is [debugpane.MaxValueRunes], which is how wide a row may be. So a long
// value legitimately differs by the pane having cut it, and what must hold is
// that the pane's is a prefix of the wire's rather than a different value.
func agrees(pane, wire string) bool {
	if pane == wire {
		return true
	}

	cut, wasCut := strings.CutSuffix(pane, " (cut)")

	return wasCut && strings.HasPrefix(wire, cut)
}

// paneValueOf is what the pane would show for one wire binding.
func paneValueOf(binding *v1.DebugBinding) string {
	if err := binding.GetError(); err != "" {
		// A name whose value could not be produced is drawn as its reason
		// rather than dropped, so the pane cannot come to list fewer names than
		// the scope holds.
		return "(" + err + ")"
	}

	return binding.GetRendered()
}

// TestTheWireMessagesSayWhatThePaneShows is the agreement, over every stop of a
// run that reaches every row shape.
func TestTheWireMessagesSayWhatThePaneShows(t *testing.T) {
	t.Parallel()

	caps := paneCapabilities(80, 24, colorprofile.NoTTY, true)
	layout := debugpane.Layout{Width: caps.Width, Height: caps.Height}

	stops := bothFronts(t, paneWorkflow(), map[string]*v1.Value{"version": v1.NewLiteral("2026.9.0")},
		"step\nstep\nstep\nstep\ncontinue\n", flowdebug.Options{
			Steps: declared("release", "checkout", "build", "flaky", "gated", "deploy", "notify"),
		}, layout)
	require.GreaterOrEqual(t, len(stops), 2, "one stop cannot show a disagreement that appears as a run moves")

	checked := 0
	for i, stop := range stops {
		// The position.
		assert.Equal(t, stop.frame.At.Step, stop.position.GetStepId(), "stop %d: the two fronts name different steps", i)
		assert.Equal(t, stop.frame.At.Kind, stop.position.GetKind(), "stop %d: the two fronts describe the step differently", i)
		assert.Equal(t, stop.frame.At.Workflow, stop.position.GetWorkflow(), "stop %d: the two fronts name different workflows", i)
		assert.Equal(t, stop.frame.At.Autopsy, stop.position.GetAutopsy(), "stop %d: the two fronts disagree about which prompt is holding", i)

		// The step window's own facts.
		assert.Equal(t, stop.frame.StepsTotal, int(stop.window.GetTotal()), "stop %d: different list lengths", i)
		assert.Equal(t, stop.frame.StepsBefore, int(stop.window.GetOffset()), "stop %d: different offsets", i)
		assert.Equal(t, stop.frame.StepsUnattributed, int(stop.window.GetUnattributed()),
			"stop %d: the two fronts disagree about how much cannot be attributed", i)
		assert.Equal(t, stop.frame.StepsTruncated, stop.window.GetTruncated(), "stop %d: different truncation", i)

		// The rows, one for one.
		require.Len(t, stop.window.GetSteps(), len(stop.frame.Steps), "stop %d: different numbers of rows", i)
		for j, row := range stop.frame.Steps {
			wire := stop.window.GetSteps()[j]
			assert.Equal(t, row.ID, wire.GetStepId(), "stop %d row %d: different ids", i, j)
			assert.Equal(t, row.Workflow, wire.GetWorkflow(), "stop %d row %d: different workflows", i, j)
			assert.Equal(t, row.Via, wire.GetVia(), "stop %d row %d: different call steps", i, j)
			assert.Equal(t, row.Declaration, int(wire.GetDeclaration()), "stop %d row %d: different declarations", i, j)
			assert.Equal(t, row.State.String(), wireStateName(wire.GetState()),
				"stop %d row %d: the two fronts say the step did different things", i, j)
			checked++
		}

		// The held row. The pane's -1 and the message's absence are the same
		// fact said two ways, and reading one as the other would mark a row
		// nothing is holding.
		if stop.frame.Held < 0 {
			assert.Nil(t, stop.window.Held, "stop %d: the wire marked a row the pane could not place", i)
		} else {
			require.NotNil(t, stop.window.Held, "stop %d: the wire left unmarked a row the pane is holding", i)
			assert.Equal(t, stop.frame.Held, int(stop.window.GetHeld()), "stop %d: the two fronts mark different rows", i)
		}

		// The scope.
		assert.Equal(t, stop.frame.BindingsTotal, int(stop.scope.GetTotal()),
			"stop %d: the two fronts disagree about how many names the run can reach", i)

		var resolved []*v1.DebugBinding
		for _, group := range stop.scope.GetGroups() {
			for _, binding := range group.GetBindings() {
				if binding.GetAnswer() != nil {
					resolved = append(resolved, binding)
				}
			}
		}
		require.Len(t, resolved, len(stop.frame.Bindings), "stop %d: different numbers of resolved bindings", i)

		for j, binding := range stop.frame.Bindings {
			wire := resolved[j]
			assert.Equal(t, binding.Expression, wire.GetExpression(),
				"stop %d binding %d: the two fronts would ask for this value with different expressions", i, j)
			assert.True(t, agrees(binding.Value, paneValueOf(wire)),
				"stop %d binding %d (%s): the pane shows %q and the wire carries %q",
				i, j, binding.Expression, binding.Value, paneValueOf(wire))
			checked++
		}
	}

	assert.Positive(t, checked, "no row and no binding was compared, so this test asserts nothing")
}

// wireStateName is the schema's outcome in the words the prompt uses, so the
// comparison above is between two vocabularies rather than between one and a
// number.
//
// Written out rather than derived from the enum name, for the reason the
// producing table is: the enum name is a wire fact and the prompt's word is a
// rendering, and text-deriving one from the other would make renaming either a
// silent change to the other.
func wireStateName(state v1.DebugStepState) string {
	switch state {
	case v1.DebugStepState_DEBUG_STEP_STATE_RUNNING:
		return "running"
	case v1.DebugStepState_DEBUG_STEP_STATE_DONE:
		return "ok"
	case v1.DebugStepState_DEBUG_STEP_STATE_TOLERATED:
		return "tolerated"
	case v1.DebugStepState_DEBUG_STEP_STATE_FAILED:
		return "failed"
	case v1.DebugStepState_DEBUG_STEP_STATE_SKIPPED:
		return "skipped"
	case v1.DebugStepState_DEBUG_STEP_STATE_PENDING:
		return "pending"
	default:
		// Never the answer for a row this producer built, and named rather
		// than folded into "pending": a session that watched nothing and a
		// producer that said nothing are different facts, and a comparison
		// that quietly equated them would pass on a bridge that had stopped
		// filling the field.
		return "unspecified"
	}
}

// TestTheWireSeesTheCalleeThePaneCannotPlace is the same agreement at the one
// stop where both fronts have to refuse.
//
// The pane marks no row and the messages carry no declaration and no mark. What
// this pins is that they refuse *together*: a wire message that resolved the
// position here would be pointing a remote client at one of two invocations
// while the local pane, over the same session, declined to.
func TestTheWireSeesTheCalleeThePaneCannotPlace(t *testing.T) {
	t.Parallel()

	caps := paneCapabilities(80, 24, colorprofile.NoTTY, true)
	layout := debugpane.Layout{Width: caps.Width, Height: caps.Height}

	stops := bothFronts(t, twiceCalledWorkflow(), nil, "step\nstep\nstep\nstep\ncontinue\n", flowdebug.Options{
		Steps: twiceCalledInventory(),
	}, layout)
	require.Len(t, stops, 4, "the run should have offered four boundaries")

	last := stops[3]
	require.Equal(t, "inner", last.position.GetWorkflow())
	assert.Equal(t, -1, last.frame.Held, "the pane placed a position it cannot place")
	assert.Nil(t, last.window.Held, "the wire marked a row the pane refused to mark")
	assert.Nil(t, last.position.Declaration,
		"the wire named one of two indistinguishable invocations while the pane named none")
	assert.Equal(t, last.frame.StepsUnattributed, int(last.window.GetUnattributed()))
	assert.Equal(t, 2, int(last.window.GetUnattributed()),
		"the rows an outcome cannot be attributed to stopped being counted")
}
