package debugpane_test

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"testing"

	"github.com/charmbracelet/colorprofile"
	golden "github.com/charmbracelet/x/exp/golden"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/picatz/flowstate/cmd/flow/internal/debugpane"
	"github.com/picatz/flowstate/cmd/flow/internal/ui"
	v1 "github.com/picatz/flowstate/pkg/flowstate/v1"
	"github.com/picatz/flowstate/pkg/flowstate/v1/flowdebug"
)

// The panes, driven the way a recorded session is driven.
//
// Every frame here comes from a real local run held by a real
// [flowdebug.Session] whose command stream is a script — which is `flow debug
// replay`'s own shape (`cmd/flow/debugreplay.go`: "a pre-flight over the
// script, a reader handed to the shared path"), in one process. That is what
// makes a golden frame a test rather than a photograph: the same script over
// the same workflow reaches the same stops in the same order, the frame reads
// no clock, and the layout is stated rather than measured.
//
// The replay *verb* cannot itself paint one, and that is the seam holding
// rather than a gap: `flow debug replay` redirects its own stdin to the script,
// so no console attaches and no panes are drawn. See
// `cmd/flow/debugpanes_test.go` for the test of that, in both directions.

// paneCapabilities is a stream of a stated size, so a frame's width and height
// are the test's rather than the machine's.
func paneCapabilities(width, height int, profile colorprofile.Profile, unicode bool) ui.Capabilities {
	return ui.Capabilities{Profile: profile, TTY: true, Width: width, Height: height, Unicode: unicode}
}

// paneRegistry answers the two tasks the fixtures use: `mark`, which succeeds
// with its own id, and `boom`, which fails.
func paneRegistry(t *testing.T) *v1.Registry {
	t.Helper()

	registry := v1.NewRegistry()
	require.NoError(t, registry.Register(v1.TaskDef{Name: "mark", Fn: func(_ context.Context, inputs map[string]*v1.Value, _ *v1.Scope) (*v1.Node_Outputs, error) {
		return &v1.Node_Outputs{NamedValues: map[string]*v1.Value{
			"artifact": v1.NewLiteral(inputs["id"].GetLiteral().GetStringValue() + ".tar.gz"),
		}}, nil
	}}))
	require.NoError(t, registry.Register(v1.TaskDef{Name: "boom", Fn: func(context.Context, map[string]*v1.Value, *v1.Scope) (*v1.Node_Outputs, error) {
		return nil, errors.New("deliberate failure")
	}}))

	return registry
}

// markStep is one succeeding step.
func markStep(id string) *v1.Node {
	return &v1.Node{Id: id, Kind: &v1.Node_Task{Task: &v1.Task{
		Name:   "mark",
		Inputs: map[string]*v1.Value{"id": v1.NewLiteral(id)},
	}}}
}

// paneWorkflow is the fixture the goldens are drawn from: one step of every
// outcome a step list has a mark for, and two the run has not reached, so a
// frame exercises every row shape rather than the two a happy path produces.
func paneWorkflow() *v1.Workflow {
	return &v1.Workflow{
		Name:           "release",
		DeclaredInputs: []*v1.InputDeclaration{{Name: "version", Type: v1.InputDeclaration_TYPE_STRING}},
		Vars:           map[string]*v1.Value{"region": v1.NewLiteral("eu-west-1")},
		Steps: []*v1.Node{
			markStep("checkout"),
			markStep("build"),
			{Id: "flaky", Kind: &v1.Node_Task{Task: &v1.Task{Name: "boom"}},
				Policy: &v1.StepPolicy{ContinueOnError: true, Retry: &v1.RetryPolicy{MaxAttempts: 1}}},
			{Id: "gated", Condition: v1.NewExpr("false"), Kind: &v1.Node_Value{Value: v1.NewExpr("1")}},
			markStep("deploy"),
			markStep("notify"),
		},
	}
}

// replayed drives a workflow under a scripted session and returns the frame
// rendered at every stop, joined.
//
// The frames are taken where a console would paint one — at
// [flowdebug.ToneBreak], the session's own heading for "the run has stopped
// somewhere new" — so what is pinned is what a reader would have seen and not a
// second arrangement of the same facts. Everything else the session emits is
// dropped: the transcript around the panes is `flow test`'s pin, not this
// package's.
func replayed(t *testing.T, workflow *v1.Workflow, script string, caps ui.Capabilities, opts flowdebug.Options) string {
	t.Helper()

	var frames strings.Builder
	var session *flowdebug.Session

	theme := ui.NewTheme(true, caps)
	layout := debugpane.Layout{Width: caps.Width, Height: caps.Height}

	opts.In = strings.NewReader(script)
	opts.Out = &strings.Builder{}
	opts.Emit = func(_ string, tone flowdebug.Tone) {
		if tone != flowdebug.ToneBreak {
			return
		}

		frame, paused := debugpane.Snapshot(t.Context(), session)
		if !paused {
			return
		}
		frames.WriteString(debugpane.Render(frame, theme, caps.Symbols(), layout))
		frames.WriteString("\n")
	}

	var err error
	session, err = flowdebug.New(opts)
	require.NoError(t, err)
	t.Cleanup(func() { _ = session.Close() })

	ctx := v1.NewContextWithRegistry(t.Context(), paneRegistry(t))
	ctx = v1.NewContextWithDebugger(ctx, session)
	ctx = v1.NewContextWithRunObserver(ctx, session)

	_, _ = v1.RunWithInputs(ctx, workflow, map[string]*v1.Value{"version": v1.NewLiteral("2026.9.0")})

	require.NotEmpty(t, frames.String(), "the run never stopped, so no frame was drawn")

	return frames.String()
}

// paneScript walks the run to the fourth boundary and then lets it finish, so
// the last frame drawn holds every state at once: two done, one tolerated, one
// skipped, one held, one pending.
const paneScript = "step\nstep\nstep\ncontinue\n"

// TestPaneFramesGolden pins the frames a styled 80x24 terminal draws.
//
// A golden rather than substrings, for `flow watch`'s reason one package over:
// a substring proves a fragment survived, and a golden proves the reviewer who
// approved this diff is the one still looking at it. Update with
// `go test ./cmd/flow/internal/debugpane -run TestPaneFrames -update` after
// reading what changed.
func TestPaneFramesGolden(t *testing.T) {
	caps := paneCapabilities(80, 24, colorprofile.TrueColor, true)

	golden.RequireEqual(t, []byte(replayed(t, paneWorkflow(), paneScript, caps, flowdebug.Options{
		Steps: []string{"checkout", "build", "flaky", "gated", "deploy", "notify"},
	})))
}

// TestPaneFramesGoldenASCII varies exactly one axis: the symbol set.
//
// [ui.SymbolSet] promises that every mark which can appear in a column is one
// column wide in both sets, "so switching sets never changes a layout". These
// two goldens beside each other are that promise, pinned.
func TestPaneFramesGoldenASCII(t *testing.T) {
	caps := paneCapabilities(80, 24, colorprofile.TrueColor, false)

	golden.RequireEqual(t, []byte(replayed(t, paneWorkflow(), paneScript, caps, flowdebug.Options{
		Steps: []string{"checkout", "build", "flaky", "gated", "deploy", "notify"},
	})))
}

// TestPaneFramesGoldenPlain varies the colour depth: below ANSI every role
// collapses and the panes carry text alone.
//
// The axis worth having a cell for, because it is what a pipe, a CI log and a
// screen reader receive — and because the whole palette is declared to lose
// emphasis and no information when it goes.
func TestPaneFramesGoldenPlain(t *testing.T) {
	caps := paneCapabilities(80, 24, colorprofile.NoTTY, true)

	golden.RequireEqual(t, []byte(replayed(t, paneWorkflow(), paneScript, caps, flowdebug.Options{
		Steps: []string{"checkout", "build", "flaky", "gated", "deploy", "notify"},
	})))
}

// TestTheSameScriptDrawsTheSameFrames is the property the goldens rest on,
// asserted rather than assumed.
//
// A golden that happened to be stable on the machine it was written on is not a
// deterministic frame; it is one nobody has run twice. Two runs of the same
// script over the same workflow must be byte-identical, and the thing that
// would break it is exactly what this package refuses to do — read a clock, a
// terminal, or a map in map order.
func TestTheSameScriptDrawsTheSameFrames(t *testing.T) {
	caps := paneCapabilities(80, 24, colorprofile.TrueColor, true)
	opts := flowdebug.Options{Steps: []string{"checkout", "build", "flaky", "gated", "deploy", "notify"}}

	first := replayed(t, paneWorkflow(), paneScript, caps, opts)
	second := replayed(t, paneWorkflow(), paneScript, caps, opts)

	assert.Equal(t, first, second, "two replays of one script drew different frames")
}

// TestNoPauseDrawsNothing is the frame's own precondition.
func TestNoPauseDrawsNothing(t *testing.T) {
	t.Parallel()

	caps := paneCapabilities(80, 24, colorprofile.TrueColor, true)

	assert.Empty(t, debugpane.Render(debugpane.Frame{}, ui.NewTheme(true, caps), caps.Symbols(),
		debugpane.Layout{Width: 80, Height: 24}),
		"a frame of a session holding no run drew something anyway")
}

// The redaction direction, which is why the scope pane resolves its values
// through [flowdebug.Session.Evaluate] and through nothing else.
//
// A pane is a new front on a paused session, and #1120's finding was that a
// front reaching values by any other door reopens the hole the seam exists to
// close. Both directions are asserted here, because an absence assertion is
// worth nothing until the thing could have been present (CLAUDE.md).

// theSecret is what must not reach a pane.
const theSecret = "hunter2-swordfish"

// redactedRun draws the frames of a run whose scope holds theSecret, with the
// session's redactors installed or not.
func redactedRun(t *testing.T, redacting bool) string {
	t.Helper()

	caps := paneCapabilities(80, 24, colorprofile.NoTTY, true)

	// The middle step composes the secret into a longer string, which is the
	// case #1120 found and the only one that tells the two redactors apart: a
	// value redactor matches by equality, so `"Bearer " + inputs.credential` is
	// not the secret and passes it through whole. Only the text redactor — the
	// substring backstop over the whole rendering — withholds it.
	//
	// Without it every row is a bare name whose value *is* the secret, which
	// the equality half catches on its own; a test built from those alone stays
	// green with the text half deleted.
	workflow := &v1.Workflow{
		Name:           "carries-a-secret",
		DeclaredInputs: []*v1.InputDeclaration{{Name: "credential", Type: v1.InputDeclaration_TYPE_STRING}},
		Steps: []*v1.Node{
			markStep("first"),
			{Id: "composed", Kind: &v1.Node_Value{Value: v1.NewExpr(`"Bearer " + inputs.credential`)}},
			markStep("second"),
		},
	}

	var frames strings.Builder
	var session *flowdebug.Session

	theme := ui.NewTheme(true, caps)

	session, err := flowdebug.New(flowdebug.Options{
		In:    strings.NewReader("step\nstep\ncontinue\n"),
		Out:   &strings.Builder{},
		Steps: []string{"first", "composed", "second"},
		Emit: func(_ string, tone flowdebug.Tone) {
			if tone != flowdebug.ToneBreak {
				return
			}
			frame, paused := debugpane.Snapshot(t.Context(), session)
			if !paused {
				return
			}
			frames.WriteString(debugpane.Render(frame, theme, caps.Symbols(),
				debugpane.Layout{Width: caps.Width, Height: caps.Height}))
		},
	})
	require.NoError(t, err)
	t.Cleanup(func() { _ = session.Close() })

	if redacting {
		// The two seams [flowdebug.Session.SetValueRedactor] describes, which
		// `flow test` installs together: one asks whether a value *is* the
		// secret, the other whether a string *contains* it.
		session.SetRedactor(func(text string) string {
			return strings.ReplaceAll(text, theSecret, "[redacted]")
		})
		session.SetValueRedactor(func(value any) any {
			if text, ok := value.(string); ok && text == theSecret {
				return "[redacted]"
			}

			return value
		})
	}

	ctx := v1.NewContextWithRegistry(t.Context(), paneRegistry(t))
	ctx = v1.NewContextWithDebugger(ctx, session)
	ctx = v1.NewContextWithRunObserver(ctx, session)

	_, runErr := v1.RunWithInputs(ctx, workflow, map[string]*v1.Value{"credential": v1.NewLiteral(theSecret)})
	require.NoError(t, runErr)

	return frames.String()
}

// TestTheScopePaneWouldRenderTheValue is the positive direction, and it runs
// first for a reason: without it the test below passes on a pane that renders
// nothing at all.
func TestTheScopePaneWouldRenderTheValue(t *testing.T) {
	assert.Contains(t, redactedRun(t, false), theSecret,
		"the value never reached the pane even unredacted, so the refusal below proves nothing")
}

// TestTheScopePaneWithholdsWhatThePromptWithholds is the direction that
// matters.
func TestTheScopePaneWithholdsWhatThePromptWithholds(t *testing.T) {
	frames := redactedRun(t, true)

	assert.NotContains(t, frames, theSecret,
		"a redacted session's secret was rendered into a pane")
	assert.Contains(t, frames, "[redacted]",
		"the row vanished rather than being withheld, which hides that there is a name there at all")
}

// The bounds. An author's file decides both of these numbers, so each is
// asserted at a size no terminal could draw.

// TestAHugeScopeTruncatesLegibly is the scope pane's bound, at ten thousand
// names.
//
// Two things are being asserted, and the second is the one a count alone would
// miss: the pane draws a bounded number of rows, *and* it says how many it did
// not — a list silently cut tells a reader their run can name eleven things.
func TestAHugeScopeTruncatesLegibly(t *testing.T) {
	t.Parallel()

	const names = 10_000

	caps := paneCapabilities(80, 24, colorprofile.NoTTY, true)

	vars := make(map[string]*v1.Value, names)
	for i := range names {
		vars[fmt.Sprintf("v%05d", i)] = v1.NewLiteral(i)
	}

	scope := v1.NewScope(v1.CurrentProfile, &v1.Workflow_StepOutputs{StepValues: map[string]*v1.Node_Outputs{}})
	scope.AmbientVars = vars

	frame := frameAtAStop(t, scope, "deploy")

	require.GreaterOrEqual(t, frame.BindingsTotal, names,
		"the frame did not see the names it is supposed to be bounding")
	assert.LessOrEqual(t, len(frame.Bindings), debugpane.MaxScopeEvaluations,
		"the snapshot evaluated past its own bound")

	text := debugpane.Render(frame, ui.NewTheme(true, caps), caps.Symbols(),
		debugpane.Layout{Width: caps.Width, Height: caps.Height})

	assert.Less(t, strings.Count(text, "\n"), 40,
		"ten thousand names filled the screen rather than a pane")
	assert.Contains(t, text, fmt.Sprintf("more of %d", frame.BindingsTotal),
		"the pane cut the list without saying how much of it was left")
}

// TestAHugeStepListTruncatesLegibly is the step pane's bound, at five thousand
// steps.
//
// The window is the bound, and the counts either side of it are what make it
// legible: a reader has to be able to tell "there are more steps" from "the run
// has six".
func TestAHugeStepListTruncatesLegibly(t *testing.T) {
	t.Parallel()

	const count = 5_000

	caps := paneCapabilities(80, 24, colorprofile.NoTTY, true)

	ids := make([]string, 0, count)
	for i := range count {
		ids = append(ids, fmt.Sprintf("s%05d", i))
	}

	frame := debugpane.Frame{
		Paused: true,
		At:     flowdebug.Position{Step: ids[count/2], Kind: `task "mark"`},
		Steps:  make([]flowdebug.Step, 0, count),
	}
	for _, id := range ids {
		frame.Steps = append(frame.Steps, flowdebug.Step{ID: id})
	}

	text := debugpane.Render(frame, ui.NewTheme(true, caps), caps.Symbols(),
		debugpane.Layout{Width: caps.Width, Height: caps.Height})

	lines := strings.Count(text, "\n")
	assert.Less(t, lines, 40, "five thousand steps filled the screen rather than a pane")

	assert.Contains(t, text, "earlier", "the pane cut the front of the list without saying so")
	assert.Contains(t, text, "later", "the pane cut the tail of the list without saying so")
	assert.Contains(t, text, "5000 step(s)", "the pane drew a window and did not say what it is a window on")

	// And the window is centred on where the run is held, which is the whole
	// reason it is a window rather than a head or a tail.
	assert.Contains(t, text, ids[count/2], "the paused step was not in the window drawn around it")
}

// TestAnUnmeasurableStreamStillDrawsBoundedPanes covers the layout a pipe
// reports: [ui.Capabilities.Height] is zero where there are no rows to count,
// and a pane that read that as "no rows" would draw nothing.
func TestAnUnmeasurableStreamStillDrawsBoundedPanes(t *testing.T) {
	t.Parallel()

	caps := paneCapabilities(0, 0, colorprofile.NoTTY, true)

	frame := debugpane.Frame{
		Paused: true,
		At:     flowdebug.Position{Step: "b", Kind: `task "mark"`},
		Steps: []flowdebug.Step{
			{ID: "a", State: flowdebug.StepDone},
			{ID: "b", State: flowdebug.StepRunning},
		},
	}

	text := debugpane.Render(frame, ui.NewTheme(true, caps), caps.Symbols(),
		debugpane.Layout{Width: caps.Width, Height: caps.Height})

	require.NotEmpty(t, text, "a stream with no measured height drew no panes at all")
	assert.Contains(t, text, "a")
	assert.Contains(t, text, "b")

	// Bounded by the fallback width rather than by nothing, which is what
	// [ui.ClampWidth] answers a stream it cannot measure.
	for line := range strings.SplitSeq(strings.TrimRight(text, "\n"), "\n") {
		assert.LessOrEqual(t, len([]rune(line)), 80, "a line ran past the fallback width: %q", line)
	}
}

// TestATerminalNarrowerThanAPaneHeadingStillDraws is the width the layout has
// to survive rather than the one it is designed for.
//
// [ui.ClampWidth] floors a *measured* width at the fallback, so this is not a
// terminal anybody has — it is a caller passing a layout directly, which slice
// 3 will be doing over a wire. The heading's rule has to give way rather than
// be repeated a negative number of times.
func TestATerminalNarrowerThanAPaneHeadingStillDraws(t *testing.T) {
	t.Parallel()

	caps := paneCapabilities(80, 24, colorprofile.NoTTY, true)

	frame := debugpane.Frame{
		Paused: true,
		At:     flowdebug.Position{Step: "a", Kind: `task "mark"`},
		Steps:  []flowdebug.Step{{ID: "a", State: flowdebug.StepRunning}},
	}

	for _, width := range []int{1, 3, 6, 7, 8} {
		text := debugpane.Render(frame, ui.NewTheme(true, caps), caps.Symbols(),
			debugpane.Layout{Width: width, Height: 24})

		require.NotEmpty(t, text, "width %d drew nothing", width)

		for line := range strings.SplitSeq(strings.TrimRight(text, "\n"), "\n") {
			assert.LessOrEqual(t, len([]rune(line)), width,
				"width %d produced a line past its own margin: %q", width, line)
		}
	}

	// And at the first width the label itself fits in, it is there whole rather
	// than cut to make room for a rule that has nowhere to go.
	assert.Contains(t,
		debugpane.Render(frame, ui.NewTheme(true, caps), caps.Symbols(),
			debugpane.Layout{Width: 6, Height: 24}),
		"steps", "the pane gave up its own label before it gave up its rule")
}

// frameAtAStop holds a run at one step and snapshots the frame there.
func frameAtAStop(t *testing.T, scope *v1.Scope, step string) debugpane.Frame {
	t.Helper()

	session, err := flowdebug.New(flowdebug.Options{Controlled: true, Out: &strings.Builder{}})
	require.NoError(t, err)
	t.Cleanup(func() { _ = session.Close() })

	finished := make(chan error, 1)
	go func() { finished <- session.BeforeStep(t.Context(), markStep(step), scope) }()

	_, err = session.WaitForPause(t.Context())
	require.NoError(t, err)

	frame, paused := debugpane.Snapshot(t.Context(), session)
	require.True(t, paused)

	require.NoError(t, session.Control(t.Context(), "continue"))
	require.NoError(t, <-finished)

	return frame
}
