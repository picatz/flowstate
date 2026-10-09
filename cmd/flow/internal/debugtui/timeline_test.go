package debugtui

import (
	"fmt"
	"strings"
	"testing"

	"charm.land/lipgloss/v2"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/picatz/flowstate/cmd/flow/internal/pane"
	"github.com/picatz/flowstate/cmd/flow/internal/tui"
	"github.com/picatz/flowstate/cmd/flow/internal/tui/tuitest"
	v1 "github.com/picatz/flowstate/pkg/flowstate/v1"
)

// timelined is a fake held at its fourth step, whose snapshot carries a point
// for each step it has reached.
func timelined() *fakeTarget {
	f := newFake()
	f.timelined, f.at = true, 3

	return f
}

// crowd is a frame whose timeline has n points, the run at current, every other
// point reachable: more than a strip can hold.
func crowd(f *fakeTarget, n, current int, dropped uint32) *v1.DebugTimeline {
	timeline := &v1.DebugTimeline{Current: int32(current), Dropped: dropped}
	for i := range n {
		timeline.Points = append(timeline.Points, &v1.DebugTimelinePoint{
			Reachable: i != current, Occurrence: &v1.DebugOccurrence{Address: fmt.Sprint("step-", i)},
		})
	}

	return timeline
}

// TestTheTimelineStripGolden pins the strip in each state it has, in the three
// variants every pane is: the point the run is at bracketed, an unreachable one
// muted, a diverged one marked, and what does not fit counted.
func TestTheTimelineStripGolden(t *testing.T) {
	for _, v := range styles {
		t.Run(v.name, func(t *testing.T) {
			var b strings.Builder
			show := func(name string, s Screen, width, replayTo int) {
				row := pane.Rect{W: width, H: 1}
				text := TimelineView(s.Frame, true, s.Diverged, replayTo, row, &pane.Hits{}, opts(v.style, width, 1, false))
				b.WriteString("=== " + name + "\n" + text + "\n")
				assert.LessOrEqual(t, lipgloss.Width(text), width, name)
			}

			s := screenOf(t, timelined(), true, tui.Size{W: 100, H: 30})
			show("at the fourth step", s, 60, -1)

			s.Diverged = map[uint64]bool{1: true}
			show("point 1 diverged", s, 60, -1)

			s.Diverged = nil
			show("replaying to point 1", s, 60, 1)

			dropped := newFake()
			dropped.timelined, dropped.at, dropped.dropped = true, 3, 3
			show("three dropped", screenOf(t, dropped, true, tui.Size{W: 100, H: 30}), 60, -1)

			many := screenOf(t, newFake(), true, tui.Size{W: 100, H: 30})
			many.Frame.Snapshot.Timeline = crowd(newFake(), 60, 30, 0)
			show("sixty points, at the thirtieth, too narrow", many, 60, -1)

			history := screenOf(t, newFake(), true, tui.Size{W: 100, H: 30})
			history.Frame.Snapshot.Timeline = crowd(newFake(), 12, 2, 0)
			show("a history: points ahead of the run", history, 60, -1)

			tuitest.Golden(t, b.String())
		})
	}
}

func TestTheStripIsDrawnBetweenTheBodyAndTheConsoleOnlyWithATimeline(t *testing.T) {
	t.Parallel()

	without, _ := screenOf(t, newFake(), true, tui.Size{W: 100, H: 30}).Draw(plain)
	assert.NotContains(t, without, "timeline", "a target with no account of its stops draws no strip")

	s := screenOf(t, timelined(), true, tui.Size{W: 100, H: 30})
	text, hits := s.Draw(plain)
	rows := lines(text)
	strip := -1
	for i, row := range rows {
		if strings.HasPrefix(row, "timeline") {
			strip = i
		}
	}
	require.NotEqual(t, -1, strip, text)
	assert.Contains(t, rows[strip], "[3]")
	assert.Contains(t, rows[strip+1], "console", "the console follows the strip")
	tuitest.Fits(t, text, s.Size)

	// Every point is a hit, and nothing else on that row is.
	for i := range 4 {
		x, y := find(t, modelOf(s), fmt.Sprint(pointPrefix, i))
		assert.Equal(t, strip, y)
		hit, ok := hits.At(x, y)
		require.True(t, ok)
		assert.Equal(t, pane.KindPoint, hit.Kind)
	}
}

// modelOf is a model showing the screen, for the helpers that look at its hits.
func modelOf(s Screen) Model { return Model{screen: s} }

func TestTheStripFitsEverySizeAndEveryState(t *testing.T) {
	t.Parallel()

	for _, size := range tuitest.Sizes {
		for _, v := range styles {
			t.Run(size.String()+"/"+v.name, func(t *testing.T) {
				s := screenOf(t, newFake().withBig(40), true, size)
				s.Frame.Snapshot.Timeline = crowd(newFake(), 1024, 600, 90)
				s.Diverged = map[uint64]bool{90 + 599: true}
				text, hits := s.Draw(v.style)
				tuitest.Fits(t, text, size)
				assert.Len(t, lines(text), size.H)
				if size.W >= MinWidth && size.H >= MinHeight {
					assert.Contains(t, text, "timeline")
					assert.Contains(t, text, "earlier")
					assert.Contains(t, text, "later")
				}
				for row := range size.H {
					for col := range size.W {
						if hit, ok := hits.At(col, row); ok && hit.Kind == pane.KindPoint {
							assert.True(t, strings.HasPrefix(hit.ID, pointPrefix), hit.ID)
						}
					}
				}
			})
		}
	}
}

func TestAClickOnAPointTravelsThereAndTheTranscriptSaysSo(t *testing.T) {
	t.Parallel()

	fake := timelined()
	m := started(t, fake)
	require.Contains(t, view(m), "[3]")

	x, y := find(t, m, pointPrefix+"1")
	m = send(m, tuitest.Click(x, y))
	assert.Equal(t, []int32{1}, fake.travels, "the click was not a goto")

	assert.Equal(t, 1, fake.at, "the target did not move")
	assert.Contains(t, view(m), "[1]", "the strip's current did not follow the receipt")
	said := strings.Join(m.screen.Console.Lines(), "\n")
	assert.Contains(t, said, Prompt+"goto 1")
	assert.Contains(t, said, "← back to build", "the record reads as what happened")
}

func TestAClickOffEveryPointTravelsNowhere(t *testing.T) {
	t.Parallel()

	fake := timelined()
	m := started(t, fake)
	x, y := find(t, m, pointPrefix+"3")

	// The row to the right of the last point, and the row above the strip.
	for _, at := range [][2]int{{m.screen.Size.W - 1, y}, {x, y - 1}} {
		m = send(m, tuitest.Click(at[0], at[1]))
	}
	m = send(m, tuitest.RightClick(x-2, y))
	assert.Empty(t, fake.travels, "a click that was on no point sent a goto")
	assert.Equal(t, 3, fake.at)
}

func TestADivergedTravelMarksThePointAndMovesNothing(t *testing.T) {
	t.Parallel()

	fake := timelined()
	fake.diverge = map[int32]bool{0: true}
	m := started(t, fake)

	x, y := find(t, m, pointPrefix+"0")
	m = send(m, tuitest.Click(x, y))
	assert.Equal(t, []int32{0}, fake.travels)
	assert.Equal(t, 3, fake.at, "a diverged travel moved the target")
	assert.True(t, m.screen.Diverged[0])
	assert.Contains(t, m.screen.Toast.Text(), "not deterministic")
	assert.Contains(t, m.screen.Toast.Text(), "point 0 is not reachable")

	text := view(m)
	assert.Contains(t, text, "✗0", "the point is not marked")
	assert.Contains(t, text, "[3]", "the run is where it was")
	assert.NotContains(t, strings.Join(m.screen.Console.Lines(), "\n"), "back to", "nothing was travelled")
}

func TestGPromptsForAPointInTheConsole(t *testing.T) {
	t.Parallel()

	fake := timelined()
	m := started(t, fake)

	m = send(m, tuitest.Key("g"))
	assert.Equal(t, paneConsole, m.screen.Focus)
	assert.Equal(t, "goto ", m.screen.Console.Text, "the verb is written and the point is left to type")
	assert.Empty(t, fake.travels, "g travelled without a point")

	m = send(m, tuitest.Key("2"), tuitest.Key("enter"))
	assert.Equal(t, []int32{2}, fake.travels)
	assert.Equal(t, 2, fake.at)
}

func TestTheStatusBarSaysReplayingWhileATravelRuns(t *testing.T) {
	t.Parallel()

	s := screenOf(t, timelined(), true, tui.Size{W: 100, H: 30})
	for busy, want := range map[string]string{"goto 1": "replaying: goto 1", "back": "replaying: back", "step": "working: step"} {
		s.Busy = busy
		text, _ := s.Draw(plain)
		assert.Contains(t, text, want)
	}

	// The strip mutes what lies beyond the point being replayed to.
	s.Busy = "goto 1"
	row := lines(func() string { text, _ := s.Draw(styled); return text }())
	assert.Contains(t, strings.Join(row, "\n"), "timeline")
}

func TestATravelNoteEscapesWhatTheTargetNamed(t *testing.T) {
	t.Parallel()

	snapshot := &v1.DebugSnapshot{Revision: 7, Occurrence: &v1.DebugOccurrence{Address: "pay\x1b]0;owned\x07ments"}}
	note := travelNote(snapshot, 3, 1, pane.Options{Symbols: plain.Symbols})
	assert.NotContains(t, note, "\x1b")
	assert.NotContains(t, note, "\x07")
	assert.Contains(t, note, "(rev 7)")
	assert.True(t, strings.HasPrefix(note, "←"))

	onward := travelNote(snapshot, 1, 3, pane.Options{Symbols: plain.Symbols})
	assert.True(t, strings.HasPrefix(onward, "→ on to"))
	assert.True(t, strings.HasPrefix(travelNote(snapshot, 3, 1, pane.Options{Symbols: ascii.Symbols}), "<-"), "the mark degrades with the symbol set")
}
