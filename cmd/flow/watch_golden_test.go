package main

import (
	"strings"
	"testing"
	"time"

	tea "charm.land/bubbletea/v2"
	"github.com/charmbracelet/colorprofile"
	golden "github.com/charmbracelet/x/exp/golden"
	"github.com/charmbracelet/x/exp/teatest/v2"
	"github.com/stretchr/testify/require"

	"github.com/picatz/flowstate/cmd/flow/internal/ui"
	"github.com/picatz/flowstate/cmd/flow/internal/watch"
	v1 "github.com/picatz/flowstate/pkg/flowstate/v1"
)

// TestWatchViewGoldenCompletedRun and TestWatchViewGoldenRetryingRun are the
// pattern #402 asked for: a real [tea.Program], driven by [teatest], with the
// screen it settles on pinned by a golden file instead of scraped for a
// substring. Where the tests above assert that a fact reached the screen
// *somewhere* in the transcript — because ordering within a frame and across
// a differential repaint is not a property this package makes any promise
// about — these assert what the settled screen *is*, in full, once nothing
// is left mid-flight to make that comparison flaky.
//
// A substring proves a fragment survived; a golden proves the reviewer who
// approved this diff is the one who is still looking at it. Update a golden
// with `go test ./cmd/flow -run TestWatchViewGolden -update` after
// confirming the new shape by eye — see golden.RequireEqual. Scoped to this
// package rather than `./cmd/flow/...`: golden's `-update` flag is not
// registered in sibling packages' test binaries, which fail with "flag
// provided but not defined" when it is passed down to them too.
//
// This covers two cells of the matrix #402 names (styled 80x24, a completed
// run and a run stuck retrying). The rest of the axes #402 named — the
// 100-column clamp and the 80-column floor, NO_COLOR, FLOWSTATE_SYMBOLS=ascii,
// and TTY vs plain-line mode — are pinned below by #774, one representative
// state per axis rather than the full cross-product (see the comment above
// that group for why crossing them would explode combinatorially, and
// docs/CI's no-silent-caps rule for why that sampling is written down rather
// than left implicit). #774 also finishes the "remaining states" axis: every
// terminal status the live view can reach and had no golden yet.
//
// Converting the fold()-based substring tests above to the same pin is still
// open, tracked separately in #774's body as follow-up rather than required
// by its acceptance criteria.
//
// Built with [colorprofile.TrueColor] rather than the NoTTY surface the
// fold()-based tests above use: those assert on substrings, so styling was
// noise to avoid, but a golden's whole point is that the pin is what a
// reviewer looks at — a golden built from a profile with no escape sequences
// could not catch a color or emphasis regression, only a text one, leaving
// the "styled" half of what these two cells claim to cover unpinned.
//
// The golden lines are narrower than 80 columns, and that is not a resize
// race: [ui.Trim] renders through lipgloss's block layout, which pads every
// line to match the *widest line in the content*, not to the MaxWidth it
// caps at — a block with room on the right does not fill it, the same way
// `flow watch` does not paint over a wider terminal than its content needs.
// Here the run id line is 40 columns wide, so that is what the rest of the
// block is padded to.
func TestWatchViewGoldenCompletedRun(t *testing.T) {
	surface, _, _ := terminalSurface(80, 24, colorprofile.TrueColor)

	tm := teatest.NewTestModel(t,
		newWatchModel(t.Context(), surface, &scriptedPoller{
			answers: []pollAnswer{finishedPoll("checkout", "build", "deploy")},
		}, 5*time.Millisecond, "flowstate-workflow-3f7c", nil),
		teatest.WithInitialTermSize(80, 24))

	tm.WaitFinished(t, teatest.WithFinalTimeout(20*time.Second))

	final, ok := tm.FinalModel(t).(watch.Model)
	if !ok {
		t.Fatalf("final model was a %T", tm.FinalModel(t))
	}

	golden.RequireEqual(t, final.View().Content)
}

func TestWatchViewGoldenRetryingRun(t *testing.T) {
	surface, _, _ := terminalSurface(80, 24, colorprofile.TrueColor)

	model := newWatchModel(t.Context(), surface, &scriptedPoller{}, time.Second, "flowstate-workflow-3f7c", nil)
	model = fold(t, model, tea.WindowSizeMsg{Width: 80, Height: 24},
		watch.StateMsg{At: observed, Response: retryingAt("deploy", 4, "connection refused").response})

	golden.RequireEqual(t, model.View().Content)
}

// The matrix below, for #774.
//
// #402 named five axes: the 100-column clamp and the 80-column floor,
// NO_COLOR, FLOWSTATE_SYMBOLS=ascii vs the unicode default, TTY vs
// plain-line mode, and the run states the live view had not yet pinned. Five
// axes crossed against each other is a combinatorial explosion nothing here
// needs: the clamp and NO_COLOR and the symbol set are each a property of
// [ui.Capabilities] that the view resolves independently of what state the
// run is in, so a defect in any one of them shows up against *any* state —
// it does not take the full product to catch one. Each of the first four
// tests below therefore holds every other axis at the styled-80x24-unicode
// baseline the two tests above already use, and varies exactly the one
// dimension it is named for. The "remaining states" axis is the one
// exception: a run's status *is* the thing under test there, so it is
// covered directly rather than sampled.
//
// Two states recur across the width/colour/symbol tests rather than a fresh
// one per test: the retrying state above (a warning mark, on a pending
// note) with a completed step folded in (a success mark, in the step list).
// One frame that already exercises both of the marks a symbol set defines
// is more informative under a colour or symbol change than a bare running
// frame would be, and reusing it means a reviewer comparing two golden
// diffs is looking at the same content lit two different ways.

// styledRetryWithAStep is the shared content for the width, colour, and
// symbol axes: a run retrying one step with a warning mark, and another
// already done with a success mark — see the matrix comment above.
func styledRetryWithAStep() *v1.GetResponse {
	answer := retryingAt("deploy", 4, "connection refused")
	answer.response.Kind = &v1.GetResponse_Outputs{Outputs: &v1.Workflow_StepOutputs{
		StepValues: map[string]*v1.Node_Outputs{
			"checkout": {NamedValues: map[string]*v1.Value{"result": v1.NewLiteral("ok")}},
		},
	}}

	return answer.response
}

// TestWatchViewGoldenWidthClamp pins ui.ClampWidth's cap: a terminal reporting
// more than maxWidth is drawn at maxWidth rather than at its own word, the
// same rule every printing surface obeys (see [ui.ClampWidth]). Only the
// width differs from TestWatchViewGoldenRetryingRun's baseline plus the one
// extra step above — a reviewer diffing the two goldens is looking at the
// clamp and nothing else.
func TestWatchViewGoldenWidthClamp(t *testing.T) {
	const reported = 300

	surface, _, _ := terminalSurface(reported, 24, colorprofile.TrueColor)
	model := newWatchModel(t.Context(), surface, &scriptedPoller{}, time.Second, "flowstate-workflow-3f7c", nil)

	folded := fold(t, model,
		tea.WindowSizeMsg{Width: reported, Height: 24},
		watch.StateMsg{At: observed, Response: styledRetryWithAStep()})

	require.Equal(t, ui.ClampWidth(reported), folded.ViewWidth(),
		"a terminal wider than maxWidth was taken at its own word instead of being clamped")
	require.Less(t, folded.ViewWidth(), reported)

	golden.RequireEqual(t, folded.View().Content)
}

// TestWatchViewGoldenWidthFloor pins the other half of [ui.ClampWidth]: a
// terminal that cannot be measured — width 0, what a pipe or a resize event
// bubbletea has not yet reported reads as — falls back to fallbackWidth (80)
// rather than laying the view out against no columns at all. Distinct from
// every 80-column golden already in this file: those pass 80 explicitly, so
// none of them can tell a rule that floors an unmeasured terminal at 80 from
// one that was simply handed 80 to begin with.
func TestWatchViewGoldenWidthFloor(t *testing.T) {
	surface, _, _ := terminalSurface(0, 24, colorprofile.TrueColor)
	model := newWatchModel(t.Context(), surface, &scriptedPoller{}, time.Second, "flowstate-workflow-3f7c", nil)

	folded := fold(t, model, watch.StateMsg{At: observed, Response: styledRetryWithAStep()})

	require.Equal(t, 80, folded.ViewWidth(), "an unmeasured terminal did not fall back to the 80-column floor")

	golden.RequireEqual(t, folded.View().Content)
}

// TestWatchViewGoldenNoColor pins the NO_COLOR axis.
//
// [ui.NewTheme] treats any profile below colorprofile.ANSI as "plain" and
// suppresses every style outright — not a dimmer palette, no styling at all,
// because a bold style still emits SGR on a profile with no colour and
// styleIf's whole point is that a plain-text surface emits none of it. What
// colorprofile.Detect resolves NO_COLOR to on a real terminal is exactly
// that profile, ASCII, while TTY stays true — a live view still draws, it
// just draws with no escape sequences.
//
// Unicode is left on here on purpose, holding it apart from the ascii-symbol
// axis below: FLOWSTATE_SYMBOLS overrides wantsUnicode's own profile check
// (see [ui.SymbolsEnv]), so `NO_COLOR=1 FLOWSTATE_SYMBOLS=unicode` is a
// combination the environment can actually produce, and it is what proves
// colour and marks are two independent knobs rather than one.
func TestWatchViewGoldenNoColor(t *testing.T) {
	var out, errOut strings.Builder

	caps := ui.Capabilities{
		Profile: colorprofile.ASCII,
		TTY:     true,
		Dark:    true,
		Width:   80,
		Height:  24,
		Unicode: true,
	}
	surface := ui.ForCapabilities(&out, &errOut, caps, caps)

	model := newWatchModel(t.Context(), surface, &scriptedPoller{}, time.Second, "flowstate-workflow-3f7c", nil)
	folded := fold(t, model,
		tea.WindowSizeMsg{Width: 80, Height: 24},
		watch.StateMsg{At: observed, Response: styledRetryWithAStep()})

	drawn := folded.View().Content
	require.NotContains(t, drawn, "\x1b[", "NO_COLOR left an escape sequence in the view")
	require.Contains(t, drawn, "✓", "NO_COLOR also silenced the unicode marks, which is a different setting")

	golden.RequireEqual(t, drawn)
}

// TestWatchViewGoldenAsciiSymbols pins the FLOWSTATE_SYMBOLS=ascii axis, the
// mirror image of the NO_COLOR test above: styling stays on — this terminal
// is a full TrueColor one — and only the mark set changes, the same way a
// person who finds the geometric marks illegible on their font can ask for
// ASCII ones without giving up colour.
func TestWatchViewGoldenAsciiSymbols(t *testing.T) {
	var out, errOut strings.Builder

	caps := ui.Capabilities{
		Profile: colorprofile.TrueColor,
		TTY:     true,
		Dark:    true,
		Width:   80,
		Height:  24,
		Unicode: false,
	}
	surface := ui.ForCapabilities(&out, &errOut, caps, caps)

	model := newWatchModel(t.Context(), surface, &scriptedPoller{}, time.Second, "flowstate-workflow-3f7c", nil)
	folded := fold(t, model,
		tea.WindowSizeMsg{Width: 80, Height: 24},
		watch.StateMsg{At: observed, Response: styledRetryWithAStep()})

	drawn := folded.View().Content
	require.Contains(t, drawn, "\x1b[", "the styled surface emitted no styling, so the colour axis is not isolated here")
	require.NotContains(t, drawn, "✓", "a unicode mark was drawn despite Unicode being false")
	require.NotContains(t, drawn, "△", "a unicode mark was drawn despite Unicode being false")

	golden.RequireEqual(t, drawn)
}

// TestWatchViewGoldenPlainLine pins the TTY-vs-plain-line axis: the shape a
// script, a CI job, or any other non-terminal stdout receives instead of the
// live view (see followPlainly). One line per change rather than a redrawn
// screen, through [watch.State.Line] rather than [watch.Model.View] — a
// different rendering path start to finish, so it earns its own golden
// rather than a colour variant of the ones above.
//
// The script below is also where "waiting for signal" gets covered, rather
// than as a state of its own in the live-view matrix further down: the live
// view folds pending *activities* into its screen but never draws
// [watch.State.Waits] — see [watch.Model.View] — so a run parked on a signal
// would render there exactly like a bare running one, pinning nothing new.
// The plain shape does render it, in [watch.State.Line], which is the one
// place this golden can actually show what a held gate looks like. That gap
// in the live view is left alone rather than papered over with a matching
// golden: this package is test-only, and #402's own ordering note says a
// golden of a surface nobody has decided is right yet is drift protection
// for a defect, not coverage.
//
// The held gate carries no deadline, deliberately: [pendingWaitLines] renders
// a countdown against whatever moment it is given, followPlainly reads that
// moment from the real clock, and a golden pinned against a duration that
// keeps shrinking between the run and the read is a golden that cannot pass
// twice. Every other line below is built the same deterministic way the
// fold()-based tests above are — no unset countdown, no unset
// next-attempt-scheduled time — for the identical reason.
func TestWatchViewGoldenPlainLine(t *testing.T) {
	waitingForSignal := func() pollAnswer {
		return pollAnswer{response: &v1.GetResponse{
			WorkflowId: "flowstate-workflow-3f7c",
			RunId:      "0198f1e2-0000-7000-8000-000000000000",
			Status:     v1.RunResponse_STATUS_RUNNING,
			Progress: &v1.RunProgress{
				StepId: "approve",
				PendingWaits: []*v1.PendingWait{{
					StepId:     "approve_gate",
					SignalName: "go-ahead",
					Policed:    true,
				}},
			},
		}}
	}

	poller := &scriptedPoller{answers: []pollAnswer{
		runningAt("checkout"),
		waitingForSignal(),
		retryingAt("deploy", 2, "connection reset by peer"),
		finishedPoll("checkout", "deploy"),
	}}
	surface, _, errOut := plainSurface()

	require.NoError(t, followPlainly(t.Context(), surface, renderingOf(FormatText), poller, time.Millisecond,
		"flowstate-workflow-3f7c", nil))

	golden.RequireEqual(t, errOut.String())
}

// The remaining-states axis: every terminal status the live view can reach
// and had no golden pin before #774. TestWatchViewGoldenCompletedRun already
// covers STATUS_COMPLETED and TestWatchViewGoldenRetryingRun covers
// STATUS_RUNNING with an activity in trouble, so what is left is the other
// four terminal statuses plus the one non-terminal warning shape the running
// state can be in besides retrying: the server itself going quiet.
//
// STATUS_UNSPECIFIED is deliberately not one of these. [watch.State.Absorb]
// refuses it before folding anything in — see TerminalStatus's doc and
// TestWatchStopsOnEveryTerminalStatusAndKeepsGoingOtherwise's own case for
// it — so it is a poll answer this package rejects, not a run state the view
// was ever asked to draw. Pinning a frame for it would snapshot a code path
// whose whole job is to never reach [watch.Model.View] with that status
// still held.
//
// "Compensating" is likewise not a cell of its own. It is not a status the
// schema defines — a compensated run reports STATUS_FAILED once the undo
// steps finish (see docs/DSL.md, "A compensated run reports FAILED") — so
// what the view draws while compensation is running is STATUS_RUNNING with
// an activity retrying or a position naming the undo step, which is the
// same shape TestWatchViewGoldenRetryingRun and the width/colour/symbol
// group above already pin. A second golden of that shape under a different
// name would not exercise a line of code the first one does not.

// TestWatchViewGoldenOutageWarning pins the shape [watch.State.Absorb] draws
// when a poll is refused rather than answered: the run stays on screen as it
// was last known, with a warning note saying the server has gone quiet
// rather than the screen simply freezing with no explanation (see
// TestWatchViewSaysWhyNothingIsMoving for the substring version of this same
// claim).
func TestWatchViewGoldenOutageWarning(t *testing.T) {
	surface, _, _ := terminalSurface(80, 24, colorprofile.TrueColor)
	model := newWatchModel(t.Context(), surface, &scriptedPoller{}, time.Second, "flowstate-workflow-3f7c", nil)

	folded := fold(t, model,
		tea.WindowSizeMsg{Width: 80, Height: 24},
		watch.StateMsg{At: observed, Response: response(v1.RunResponse_STATUS_RUNNING, "checkout")},
		watch.StateMsg{At: observed.Add(time.Second), Err: transientRefusal()},
	)

	golden.RequireEqual(t, folded.View().Content)
}

// TestWatchViewGoldenFailedRun pins STATUS_FAILED: the Danger pill, and the
// failure message in the danger-marked block below the step list.
func TestWatchViewGoldenFailedRun(t *testing.T) {
	surface, _, _ := terminalSurface(80, 24, colorprofile.TrueColor)
	model := newWatchModel(t.Context(), surface, &scriptedPoller{}, time.Second, "flowstate-workflow-3f7c", nil)

	folded := fold(t, model, tea.WindowSizeMsg{Width: 80, Height: 24},
		watch.StateMsg{At: observed, Response: failedResponse(
			v1.RunResponse_STATUS_FAILED, `step "deploy" could not reach the registry`)})

	golden.RequireEqual(t, folded.View().Content)
}

// TestWatchViewGoldenCanceledRun pins STATUS_CANCELED: the Neutral pill —
// deliberately not a fault colour, since cancelling is a run being told to
// stop rather than a run going wrong (see [statusTone]) — and, distinct
// from the failed and terminated goldens, no danger block at all: a
// deliberate stop with nothing further to explain carries no error message.
func TestWatchViewGoldenCanceledRun(t *testing.T) {
	surface, _, _ := terminalSurface(80, 24, colorprofile.TrueColor)
	model := newWatchModel(t.Context(), surface, &scriptedPoller{}, time.Second, "flowstate-workflow-3f7c", nil)

	folded := fold(t, model, tea.WindowSizeMsg{Width: 80, Height: 24},
		watch.StateMsg{At: observed, Response: response(v1.RunResponse_STATUS_CANCELED, "checkout")})

	golden.RequireEqual(t, folded.View().Content)
}

// TestWatchViewGoldenTerminatedRun pins STATUS_TERMINATED: a Danger pill like
// STATUS_FAILED's, for a run taken away mid-flight rather than one that ran
// its own course badly — and, unlike the canceled golden above, a message in
// the danger block, which is what proves that block is keyed on the response
// carrying a failure rather than on the status spelling "FAILED".
func TestWatchViewGoldenTerminatedRun(t *testing.T) {
	surface, _, _ := terminalSurface(80, 24, colorprofile.TrueColor)
	model := newWatchModel(t.Context(), surface, &scriptedPoller{}, time.Second, "flowstate-workflow-3f7c", nil)

	folded := fold(t, model, tea.WindowSizeMsg{Width: 80, Height: 24},
		watch.StateMsg{At: observed, Response: failedResponse(
			v1.RunResponse_STATUS_TERMINATED, "terminated by operator")})

	golden.RequireEqual(t, folded.View().Content)
}

// TestWatchViewGoldenTimedOutRun pins STATUS_TIMED_OUT: the Warning pill —
// its own tone, distinct from both the danger statuses above and the neutral
// cancellation, because "the deadline passed" is a different fact to go and
// look at than either of those (see [outcomeError]'s word for it) — carrying
// steps it completed before the deadline and no failure message, the honest
// answer when the server has none to give.
func TestWatchViewGoldenTimedOutRun(t *testing.T) {
	surface, _, _ := terminalSurface(80, 24, colorprofile.TrueColor)
	model := newWatchModel(t.Context(), surface, &scriptedPoller{}, time.Second, "flowstate-workflow-3f7c", nil)

	folded := fold(t, model, tea.WindowSizeMsg{Width: 80, Height: 24},
		watch.StateMsg{At: observed, Response: response(v1.RunResponse_STATUS_TIMED_OUT, "checkout", "build")})

	golden.RequireEqual(t, folded.View().Content)
}
