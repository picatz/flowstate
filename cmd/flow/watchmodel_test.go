package main

import (
	"context"
	"io"
	"strings"
	"testing"
	"time"

	tea "charm.land/bubbletea/v2"
	"github.com/charmbracelet/colorprofile"
	"github.com/charmbracelet/x/exp/teatest/v2"
	"github.com/stretchr/testify/require"

	"github.com/picatz/flowstate/cmd/flow/internal/ui"
	v1 "github.com/picatz/flowstate/pkg/flowstate/v1"
)

// terminalSurface describes a terminal this test does not have.
//
// Built through ui.ForCapabilities rather than by hand, so it is the same
// construction a real terminal gets: a struct assembled here would keep compiling
// after the surface grew a field, and the view would then be tested against a
// palette nothing produces.
func terminalSurface(width, height int, profile colorprofile.Profile) (*ui.UI, *strings.Builder, *strings.Builder) {
	var out, errOut strings.Builder

	caps := ui.Capabilities{
		Profile: profile,
		TTY:     true,
		Dark:    true,
		Width:   width,
		Height:  height,
		Unicode: true,
	}

	return ui.ForCapabilities(&out, &errOut, caps, caps), &out, &errOut
}

// fold applies a sequence of messages, returning the model that results.
//
// The model is a value, so Update returns a new one each time and the sequence has
// to be threaded — which is exactly the property that makes a bubbletea model
// testable without a program, and exactly the one a helper like this is for.
func fold(t *testing.T, model watchModel, msgs ...tea.Msg) watchModel {
	t.Helper()

	current := tea.Model(model)
	for _, msg := range msgs {
		current, _ = current.Update(msg)
	}

	folded, ok := current.(watchModel)
	require.True(t, ok, "Update returned a %T", current)

	return folded
}

// viewOf renders a model's screen as a string.
func viewOf(model watchModel) string {
	return model.View().Content
}

// TestWatchViewShowsProgressAsItArrives drives the real program, which is the only
// way to establish that the poll loop, the renderer, and the terminal state all
// agree.
//
// Everything below this test folds messages by hand, which proves the model and says
// nothing about whether a program built from it ever draws or ever exits.
//
// Asserted as a *progression* rather than on the last frame. A view whose final
// screen is right is also produced by a watch that drew nothing until the run
// finished, which is a `flow get` with extra steps: each condition below has to
// become true in order, and the reader only moves forwards.
func TestWatchViewShowsProgressAsItArrives(t *testing.T) {
	surface, _, _ := terminalSurface(80, 24, colorprofile.NoTTY)
	poller := &scriptedPoller{answers: []pollAnswer{
		runningPoll(),
		runningPoll("checkout"),
		runningPoll("build", "checkout"),
		finishedPoll("build", "checkout", "deploy"),
	}}

	// Slower than the poll loop needs to be, so each answer gets a frame of its own:
	// bubbletea coalesces redraws, and at a millisecond a run this short reaches its
	// end inside one frame — which would leave the progression untestable rather than
	// absent.
	tm := teatest.NewTestModel(t,
		newWatchModel(t.Context(), surface, poller, 20*time.Millisecond, "flowstate-workflow-3f7c", nil),
		teatest.WithInitialTermSize(80, 24))

	// No key is pressed: a run reaching a terminal status has to stop the program by
	// itself, or `flow watch` in a CI job never returns. The timeout is what
	// distinguishes "quit on its own" from "was still drawing when the test ended".
	tm.WaitFinished(t, teatest.WithFinalTimeout(20*time.Second))

	// Every frame the program wrote, in the order it wrote them. Read once at the
	// end rather than with a series of teatest.WaitFor calls: each of those keeps
	// only what it read itself, so a frame carrying two of the strings below
	// satisfies the first wait and is gone before the second looks.
	drawn := readAll(t, tm.FinalOutput(t))

	// Ordering rather than presence, which is the whole claim. A final screen saying
	// COMPLETED with three steps on it is also what a watch that drew nothing until
	// the run finished produces — a `flow get` with extra machinery.
	//
	// "checkout" before "COMPLETED" is what rules that out, and it is worth being
	// precise about why: within any one frame the status is drawn *above* the step
	// list, so a step name preceding the terminal status can only have come from an
	// earlier frame — one drawn while the run was still going. "deploy" then comes
	// after, because it finished in the same answer that finished the run.
	requireInOrder(t, drawn,
		"flowstate-workflow-3f7c",
		"RUNNING",
		"checkout",
		"COMPLETED",
		"deploy",
	)
	require.Contains(t, drawn, "q stops watching")

	final, ok := tm.FinalModel(t).(watchModel)
	require.True(t, ok)
	require.Equal(t, v1.RunResponse_STATUS_COMPLETED, final.state.status)
	require.False(t, final.quit, "the run finishing was recorded as the person quitting")
}

// requireInOrder asserts that each string first appears after the one before it.
func requireInOrder(t *testing.T, drawn string, want ...string) {
	t.Helper()

	at := 0
	for i, s := range want {
		found := strings.Index(drawn[at:], s)
		if i == 0 {
			require.GreaterOrEqual(t, found, 0, "%q was never drawn", s)
		} else {
			require.GreaterOrEqual(t, found, 0, "%q was not drawn after %q", s, want[i-1])
		}
		at += found + len(s)
	}
}

// readAll drains a reader into a string.
func readAll(t *testing.T, r io.Reader) string {
	t.Helper()

	b, err := io.ReadAll(r)
	require.NoError(t, err)

	return string(b)
}

// TestWatchViewStopsOnEveryKeyThatMeansStop covers the three spellings.
//
// A watch has no state to navigate, so the keyboard is exactly these and nothing
// else: `q` because that is what every full-screen terminal program uses, `esc`
// because it is what people who do not know that press, and `ctrl+c` because a
// program that ignores it is a program people have to kill.
func TestWatchViewStopsOnEveryKeyThatMeansStop(t *testing.T) {
	for name, key := range map[string]tea.KeyPressMsg{
		"q":      {Code: 'q', Text: "q"},
		"esc":    {Code: tea.KeyEscape},
		"ctrl+c": {Code: 'c', Mod: tea.ModCtrl},
	} {
		t.Run(name, func(t *testing.T) {
			surface, _, _ := terminalSurface(80, 24, colorprofile.NoTTY)
			poller := &scriptedPoller{answers: []pollAnswer{runningPoll()}}

			tm := teatest.NewTestModel(t,
				newWatchModel(t.Context(), surface, poller, time.Millisecond, "flowstate-workflow-3f7c", nil),
				teatest.WithInitialTermSize(80, 24))

			teatest.WaitFor(t, tm.Output(), func(b []byte) bool {
				return strings.Contains(string(b), "RUNNING")
			}, teatest.WithDuration(10*time.Second), teatest.WithCheckInterval(time.Millisecond))

			tm.Send(key)
			tm.WaitFinished(t, teatest.WithFinalTimeout(10*time.Second))

			final, ok := tm.FinalModel(t).(watchModel)
			require.True(t, ok)
			require.True(t, final.quit, "%s did not stop the watch", name)
			require.Equal(t, v1.RunResponse_STATUS_RUNNING, final.state.status)
		})
	}
}

// TestWatchViewIgnoresKeysThatMeanNothing writes the negative direction of the test
// above.
//
// Asserting only that three keys quit is satisfied by a model that quits on
// *anything*, which would make a stray keystroke — or a paste — stop a watch
// somebody wanted to keep.
func TestWatchViewIgnoresKeysThatMeanNothing(t *testing.T) {
	surface, _, _ := terminalSurface(80, 24, colorprofile.NoTTY)
	model := newWatchModel(t.Context(), surface, &scriptedPoller{}, time.Second, "w", nil)

	for _, key := range []tea.KeyPressMsg{
		{Code: 'x', Text: "x"},
		{Code: 'Q', Text: "Q"},
		{Code: ' ', Text: " "},
		{Code: tea.KeyEnter},
		{Code: 'd', Mod: tea.ModCtrl},
	} {
		updated, cmd := model.key(key)

		folded, ok := updated.(watchModel)
		require.True(t, ok)
		require.False(t, folded.quit, "%q stopped the watch", key.String())
		require.Nil(t, cmd, "%q produced a command", key.String())
	}
}

// TestWatchViewMeasuresElapsedFromItsOwnMessages is what makes a still screen
// legible.
//
// A watch showing nothing but a status is indistinguishable from a watch that has
// frozen, which is the complaint that makes `flow get` in a loop unpleasant. The
// number is measured from the poll messages rather than from a clock, so a test can
// state exactly what the screen should say.
func TestWatchViewMeasuresElapsedFromItsOwnMessages(t *testing.T) {
	surface, _, _ := terminalSurface(80, 24, colorprofile.NoTTY)
	model := newWatchModel(t.Context(), surface, &scriptedPoller{}, time.Second, "flowstate-workflow-3f7c", nil)

	// Before any poll there is nothing to measure, and claiming "0s" would be a
	// measurement rather than the absence of one.
	require.Contains(t, viewOf(model), "watching")
	require.NotContains(t, viewOf(model), "watching for")

	start := time.Date(2026, 7, 30, 9, 0, 0, 0, time.UTC)
	folded := fold(t, model,
		watchPollMsg{at: start},
		watchStateMsg{response: response(v1.RunResponse_STATUS_RUNNING)},
		watchPollMsg{at: start.Add(42 * time.Second)},
	)

	require.Contains(t, viewOf(folded), "watching for 42s")
}

// TestWatchViewSaysWhyNothingIsMoving checks that an outage is announced rather than
// left to be inferred from a screen that has stopped changing.
func TestWatchViewSaysWhyNothingIsMoving(t *testing.T) {
	surface, _, _ := terminalSurface(80, 24, colorprofile.NoTTY)
	model := newWatchModel(t.Context(), surface, &scriptedPoller{}, time.Second, "flowstate-workflow-3f7c", nil)

	folded := fold(t, model,
		tea.WindowSizeMsg{Width: 80, Height: 24},
		watchPollMsg{at: time.Date(2026, 7, 30, 9, 0, 0, 0, time.UTC)},
		watchStateMsg{response: response(v1.RunResponse_STATUS_RUNNING, "checkout")},
		watchStateMsg{err: transientRefusal()},
	)

	drawn := viewOf(folded)
	require.Contains(t, drawn, "connection refused",
		"the outage was announced without saying what went wrong")
	require.Contains(t, drawn, "--address",
		"the advice about where to point this was lost to the right margin")
	// What was already known is still on screen: an outage is a note beside the run,
	// not a reason to stop reporting it.
	require.Contains(t, drawn, "RUNNING")
	require.Contains(t, drawn, "checkout")
}

// TestWatchViewShowsAFailureMessage checks that the reason a run failed is on the
// screen and not only in the exit status.
func TestWatchViewShowsAFailureMessage(t *testing.T) {
	surface, _, _ := terminalSurface(80, 24, colorprofile.NoTTY)
	model := newWatchModel(t.Context(), surface, &scriptedPoller{}, time.Second, "flowstate-workflow-3f7c", nil)

	folded := fold(t, model, watchStateMsg{
		response: failedResponse(v1.RunResponse_STATUS_FAILED, `step "deploy" could not reach the registry`),
	})

	drawn := viewOf(folded)
	require.Contains(t, drawn, "FAILED")
	require.Contains(t, drawn, "could not reach the registry")
}

// TestWatchViewCapsTheStepListAndSaysHowMany is the no-silent-caps rule applied to a
// screen.
//
// A list that quietly shows a window reads as the whole list, so a reader counting
// steps would be counting the wrong thing. Both numbers are asserted: how many are
// hidden, and how many there are.
func TestWatchViewCapsTheStepListAndSaysHowMany(t *testing.T) {
	steps := make([]string, 0, 30)
	for i := range 30 {
		steps = append(steps, string(rune('a'+i/26))+string(rune('a'+i%26)))
	}

	surface, _, _ := terminalSurface(80, 24, colorprofile.NoTTY)
	model := newWatchModel(t.Context(), surface, &scriptedPoller{}, time.Second, "flowstate-workflow-3f7c", nil)

	folded := fold(t, model,
		tea.WindowSizeMsg{Width: 80, Height: 24},
		watchStateMsg{response: response(v1.RunResponse_STATUS_RUNNING, steps...)},
	)

	visible := folded.visibleSteps()
	require.Less(t, visible, len(steps), "the terminal was tall enough not to cap, so nothing is under test")

	drawn := viewOf(folded)
	require.Contains(t, drawn, "30 step(s) done", "the whole count was not reported")
	require.Contains(t, drawn, "earlier steps", "steps were dropped without saying so")

	// The tail, because the useful question is what just finished.
	require.Contains(t, drawn, steps[len(steps)-1])
	require.NotContains(t, drawn, steps[0]+"\n")
}

// TestWatchViewFitsAShortTerminal checks the floor on the step list.
//
// A terminal too short to hold the header and the list is better served by a short
// list that scrolls the header off than by a view that silently stops reporting
// progress at all.
func TestWatchViewFitsAShortTerminal(t *testing.T) {
	surface, _, _ := terminalSurface(80, 24, colorprofile.NoTTY)
	model := newWatchModel(t.Context(), surface, &scriptedPoller{}, time.Second, "flowstate-workflow-3f7c", nil)

	folded := fold(t, model,
		tea.WindowSizeMsg{Width: 80, Height: 4},
		watchStateMsg{response: response(v1.RunResponse_STATUS_RUNNING, "a", "b", "c", "d", "e")},
	)

	require.GreaterOrEqual(t, folded.visibleSteps(), 3)
	require.Contains(t, viewOf(folded), "5 step(s) done")
}

// TestWatchViewFitsTheTerminal checks that no line runs past the right margin,
// whichever way it was made to fit.
func TestWatchViewFitsTheTerminal(t *testing.T) {
	const width = 40

	long := strings.Repeat("very-long-workflow-id-", 6)
	surface, _, _ := terminalSurface(width, 24, colorprofile.NoTTY)
	model := newWatchModel(t.Context(), surface, &scriptedPoller{}, time.Second, long, nil)

	folded := fold(t, model,
		tea.WindowSizeMsg{Width: width, Height: 24},
		watchStateMsg{response: failedResponse(v1.RunResponse_STATUS_FAILED, strings.Repeat("cause ", 40))},
	)

	for _, line := range strings.Split(viewOf(folded), "\n") {
		require.LessOrEqual(t, len([]rune(line)), width, "a line ran past the terminal: %q", line)
	}
}

// TestWatchViewFollowsTheTerminalItIsToldAbout checks that a resize is acted on.
//
// Every other test here builds a model whose surface already reports the size it then
// sends, so the resize handler could be deleted and none of them would notice. What
// notices is a size that *disagrees* with the one the surface was built with, in both
// directions — which is also the case bubbletea produces at startup, since it reports
// the real terminal and Detect reports a clamped one.
func TestWatchViewFollowsTheTerminalItIsToldAbout(t *testing.T) {
	surface, _, _ := terminalSurface(80, 24, colorprofile.NoTTY)
	model := newWatchModel(t.Context(), surface, &scriptedPoller{}, time.Second,
		strings.Repeat("long-workflow-id-", 6), nil)

	narrowed := fold(t, model,
		tea.WindowSizeMsg{Width: 30, Height: 24},
		watchStateMsg{response: response(v1.RunResponse_STATUS_RUNNING, "checkout")})

	require.Equal(t, 30, narrowed.viewWidth(), "a resize narrower than the surface was ignored")
	for _, line := range strings.Split(viewOf(narrowed), "\n") {
		require.LessOrEqual(t, len([]rune(line)), 30, "a line ran past the resized terminal: %q", line)
	}

	// Wider, and clamped — the terminal reports its real width, and this view is
	// bounded to a readable measure like every surface that prints.
	widened := fold(t, model,
		tea.WindowSizeMsg{Width: 300, Height: 24},
		watchStateMsg{response: response(v1.RunResponse_STATUS_RUNNING, "checkout")})

	require.Equal(t, ui.ClampWidth(300), widened.viewWidth(),
		"a very wide terminal was taken at its word, so this view is wider than every other")
	require.Less(t, widened.viewWidth(), 300)

	// A terminal that could not be measured at all still gets a width to lay out
	// against, so nothing here divides by nothing or draws one line per word.
	unmeasured := fold(t, model, tea.WindowSizeMsg{Width: 0, Height: 0})
	require.Positive(t, unmeasured.viewWidth(), "an unmeasurable terminal left the view with no width")
	require.Equal(t, ui.ClampWidth(0), unmeasured.viewWidth())

	// And the height, which decides how many steps fit.
	tall := fold(t, model, tea.WindowSizeMsg{Width: 80, Height: 200})
	require.Equal(t, maxVisibleSteps, tall.visibleSteps())

	short := fold(t, model, tea.WindowSizeMsg{Width: 80, Height: 10})
	require.Less(t, short.visibleSteps(), maxVisibleSteps,
		"a short terminal was told nothing about its height")
}

// TestWatchViewTrimsIdentifiersAndWrapsProse is the split, in both directions.
//
// Trimming everything is the easy uniform rule and it loses the thing a reader most
// needs: a truncated failure message ends mid-sentence, and the sentence is why they
// are looking at the screen. Wrapping everything is the other easy rule and it makes
// a long id reflow every line under it on each redraw.
func TestWatchViewTrimsIdentifiersAndWrapsProse(t *testing.T) {
	const width = 40

	id := strings.Repeat("long-workflow-id-", 5)
	surface, _, _ := terminalSurface(width, 40, colorprofile.NoTTY)

	folded := fold(t, newWatchModel(t.Context(), surface, &scriptedPoller{}, time.Second, id, nil),
		tea.WindowSizeMsg{Width: width, Height: 40},
		watchStateMsg{response: failedResponse(v1.RunResponse_STATUS_FAILED,
			"the registry refused the push because the tag already exists")},
	)

	drawn := viewOf(folded)

	// The id is cut, because it is looked up rather than read.
	require.NotContains(t, drawn, id, "an id longer than the terminal was not trimmed")
	require.Contains(t, drawn, "long-workflow-id-long", "the id was dropped rather than trimmed")

	// The message is whole, because it is the reason somebody is looking.
	for _, word := range strings.Fields("the registry refused the push because the tag already exists") {
		require.Contains(t, drawn, word, "the failure message lost %q to the right margin", word)
	}
}

// TestWatchViewSurvivesItsOwnStyling is the property the ui package is built around,
// checked on the one surface that draws rather than prints.
//
// Meaning is carried by the words and the layout; colour and weight only make the
// meaning faster to find. That is what a log file, a screen reader, and a
// colour-blind reader all receive.
func TestWatchViewSurvivesItsOwnStyling(t *testing.T) {
	msgs := []tea.Msg{
		tea.WindowSizeMsg{Width: 100, Height: 40},
		watchPollMsg{at: time.Date(2026, 7, 30, 9, 0, 0, 0, time.UTC)},
		watchStateMsg{response: response(v1.RunResponse_STATUS_RUNNING, "checkout", "build")},
	}

	plainSurf, _, _ := terminalSurface(100, 40, colorprofile.NoTTY)
	plainDrawn := viewOf(fold(t, newWatchModel(t.Context(), plainSurf, &scriptedPoller{}, time.Second, "w", nil), msgs...))

	styledSurf, _, _ := terminalSurface(100, 40, colorprofile.TrueColor)
	styledDrawn := viewOf(fold(t, newWatchModel(t.Context(), styledSurf, &scriptedPoller{}, time.Second, "w", nil), msgs...))

	require.Contains(t, styledDrawn, "\x1b[", "the styled surface emitted no styling, so nothing is under test")
	require.NotContains(t, plainDrawn, "\x1b[", "the plain surface emitted styling")

	// Every word the unstyled screen says, the styled screen says too. Styling wraps
	// text; it never replaces it.
	for _, word := range strings.Fields(plainDrawn) {
		require.Contains(t, styledDrawn, word, "styling dropped %q", word)
	}
}

// TestWatchViewIsStyledForTheStreamItDrawsOn is what lets one invocation show a live
// view and pipe its answer.
//
// The view goes to stderr and the outputs to stdout, so `flow watch x | jq` has a
// piped answer stream and a terminal account stream. Styled from the answer stream's
// palette the view would be drawn plain on a perfectly capable terminal, for the sake
// of a pipe that never receives a byte of it.
func TestWatchViewIsStyledForTheStreamItDrawsOn(t *testing.T) {
	var out, errOut strings.Builder

	piped := ui.Capabilities{Profile: colorprofile.NoTTY, Width: 80}
	terminal := ui.Capabilities{
		Profile: colorprofile.TrueColor, TTY: true, Dark: true, Width: 80, Height: 24, Unicode: true,
	}
	surface := ui.ForCapabilities(&out, &errOut, piped, terminal)

	folded := fold(t, newWatchModel(t.Context(), surface, &scriptedPoller{}, time.Second, "w", nil),
		watchStateMsg{response: response(v1.RunResponse_STATUS_RUNNING, "checkout")})

	require.Contains(t, viewOf(folded), "\x1b[",
		"the view was drawn plain because stdout is a pipe, on a terminal that is not")
	require.Contains(t, viewOf(folded), "✓",
		"the marks were chosen for the piped stream rather than the terminal drawing them")
}

// TestWatchViewUsesASCIIWhereMarksAreNotSafe checks that the marks come from the
// stream's own symbol set rather than being written into the view.
//
// A mark that does not render is a replacement glyph in the middle of a status line,
// and one that renders at an unexpected width breaks the alignment of every column
// after it.
func TestWatchViewUsesASCIIWhereMarksAreNotSafe(t *testing.T) {
	var out, errOut strings.Builder

	ascii := ui.Capabilities{TTY: true, Dark: true, Width: 80, Height: 24, Unicode: false}
	surface := ui.ForCapabilities(&out, &errOut, ascii, ascii)

	folded := fold(t, newWatchModel(t.Context(), surface, &scriptedPoller{}, time.Second, "w", nil),
		watchStateMsg{response: response(v1.RunResponse_STATUS_RUNNING, "checkout")})

	drawn := viewOf(folded)
	require.Contains(t, drawn, ascii.Symbols().Success)
	require.NotContains(t, drawn, "✓", "a mark was written into the view instead of taken from the symbol set")
	require.Contains(t, drawn, "checkout", "the step is named, not only marked")
}

// TestWatchViewTimesEveryPollItPerforms is the live shape's half of the outage
// allowance.
//
// The allowance is enforced against the clock, and the state machine reads that clock
// from the message rather than for itself — which means an answer arriving with no
// observation time silently makes the allowance unreachable: every elapsed span
// computes as zero and the live view waits on an unreachable server forever, exactly
// the bug the allowance exists to prevent. Nothing else here notices, because every
// other test states the time itself.
func TestWatchViewTimesEveryPollItPerforms(t *testing.T) {
	surface, _, _ := terminalSurface(80, 24, colorprofile.NoTTY)
	model := newWatchModel(t.Context(), surface,
		&scriptedPoller{answers: []pollAnswer{{err: transientRefusal()}}},
		time.Second, "flowstate-workflow-3f7c", nil)

	msg, ok := model.fetch()().(watchStateMsg)
	require.True(t, ok, "a poll produced a %T", msg)
	require.False(t, msg.at.IsZero(),
		"a poll result carried no observation time, so the outage allowance can never advance")

	// And it reaches the state machine, rather than being carried and dropped.
	folded := fold(t, model,
		watchStateMsg{at: observed, err: transientRefusal()},
		watchStateMsg{at: observed.Add(outageAllowance), err: transientRefusal()})

	require.True(t, folded.state.gaveUp,
		"the live view did not give up after the whole allowance had passed")
	require.ErrorContains(t, watchEnding(surface, folded), "gave up")
}

// TestWatchViewTreatsAnInterruptedPollAsStopping is the live shape's half of the same
// rule the plain shape holds.
//
// A poll cut short by ctrl+c fails with a cancelled context, which connect reports as
// a refusal — so a model that folded it in would record the server as having stopped
// answering and exit non-zero on a run that is fine.
func TestWatchViewTreatsAnInterruptedPollAsStopping(t *testing.T) {
	surface, _, _ := terminalSurface(80, 24, colorprofile.NoTTY)

	ctx, cancel := context.WithCancel(t.Context())
	model := newWatchModel(ctx, surface, &scriptedPoller{}, time.Second, "flowstate-workflow-3f7c", nil)
	model = fold(t, model, watchStateMsg{response: response(v1.RunResponse_STATUS_RUNNING, "checkout")})

	cancel()
	folded := fold(t, model, watchStateMsg{err: transientRefusal()})

	require.True(t, folded.quit, "an interrupted poll was not recognised as the watcher stopping")
	require.False(t, folded.state.gaveUp,
		"an interrupted watch recorded the server as having stopped answering")
	require.NoError(t, watchEnding(surface, folded))
}

// TestWatchEndingReportsTheRunUnlessTheWatcherStopped is the join between the live
// shape and the ending both shapes share.
//
// A view that quits because somebody pressed q must not report the run as anything,
// and a view that quits because the run finished must report exactly what a pipe
// would — outputs on stdout and the run's status as the exit code.
func TestWatchEndingReportsTheRunUnlessTheWatcherStopped(t *testing.T) {
	t.Run("the watcher stopped", func(t *testing.T) {
		surface, out, _ := plainSurface()
		model := newWatchModel(t.Context(), surface, &scriptedPoller{}, time.Second, "flowstate-workflow-3f7c", nil)
		folded := fold(t, model, watchStateMsg{response: response(v1.RunResponse_STATUS_RUNNING, "checkout")})
		folded.quit = true

		require.NoError(t, watchEnding(surface, folded),
			"an interrupted watch was reported as a failed run")
		require.Empty(t, out.String(), "a run still going wrote outputs it does not have")
	})

	t.Run("the run finished", func(t *testing.T) {
		surface, out, _ := plainSurface()
		model := newWatchModel(t.Context(), surface, &scriptedPoller{}, time.Second, "flowstate-workflow-3f7c", nil)
		folded := fold(t, model, watchStateMsg{response: response(v1.RunResponse_STATUS_COMPLETED, "greet")})

		require.NoError(t, watchEnding(surface, folded))
		require.Contains(t, out.String(), "stepValues",
			"the live shape did not write the outputs a pipe would have received")
	})

	t.Run("the run failed", func(t *testing.T) {
		surface, out, _ := plainSurface()
		model := newWatchModel(t.Context(), surface, &scriptedPoller{}, time.Second, "flowstate-workflow-3f7c", nil)
		folded := fold(t, model, watchStateMsg{
			response: failedResponse(v1.RunResponse_STATUS_TIMED_OUT, "the deploy never returned"),
		})

		err := watchEnding(surface, folded)
		require.ErrorContains(t, err, "timed out")
		require.ErrorContains(t, err, "never returned")
		require.Empty(t, out.String())
	})
}
