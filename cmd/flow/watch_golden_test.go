package main

import (
	"testing"
	"time"

	tea "charm.land/bubbletea/v2"
	"github.com/charmbracelet/colorprofile"
	golden "github.com/charmbracelet/x/exp/golden"
	"github.com/charmbracelet/x/exp/teatest/v2"

	"github.com/picatz/flowstate/cmd/flow/internal/watch"
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
// with `go test ./cmd/flow/... -run TestWatchViewGolden -update` after
// confirming the new shape by eye — see golden.RequireEqual.
//
// This covers two cells of the matrix #402 names (styled 80x24, a completed
// run and a run stuck retrying). The rest of the matrix — the 100-column
// clamp, NO_COLOR, FLOWSTATE_SYMBOLS=ascii — and converting the fold()-based
// tests above to the same pin are follow-up work, tracked so the convention
// does not stop at two cases: see #774.
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
