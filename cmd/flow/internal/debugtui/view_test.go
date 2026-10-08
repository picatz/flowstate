package debugtui

import (
	"strings"
	"testing"

	"charm.land/lipgloss/v2"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/picatz/flowstate/cmd/flow/internal/pane"
	"github.com/picatz/flowstate/cmd/flow/internal/tui"
	"github.com/picatz/flowstate/cmd/flow/internal/tui/tuitest"
	"github.com/picatz/flowstate/cmd/flow/internal/ui"
)

func opts(st Style, w, h int, focused bool) pane.Options {
	return pane.Options{Width: w, Height: h, Theme: st.Theme, Symbols: st.Symbols, Focused: focused}
}

// opened is a screen whose scope has two groups open and a row selected, so a
// frame shows what a person is looking at and not the collapsed first view.
func opened(t *testing.T, f *fakeTarget, program bool, size tui.Size) Screen {
	t.Helper()

	s := screenOf(t, f, program, size)
	s.Tree.Toggle("g:inputs")
	s.Tree.Toggle("g:steps")
	s.Tree.Select("steps.checkout")
	s.Focus, s.Pane = paneScope, paneScope

	return s
}

// Every pane is pinned in the three variants debugpane's frames are: styled,
// ASCII marks, and plain.

func TestThePaneViewsGolden(t *testing.T) {
	for _, v := range styles {
		t.Run(v.name, func(t *testing.T) {
			s := opened(t, newFake(), true, tui.Size{W: 100, H: 30})
			var b strings.Builder
			for _, part := range []struct{ name, text string }{
				{"header", HeaderView(s.Frame, true, 80, v.style)},
				{"steps", StepsView(s.Frame, true, 0, opts(v.style, 40, 9, false))},
				{"scope", ScopeView(s.Tree, s.Frame, true, "", opts(v.style, 44, 9, true))},
				{"inspector", InspectorView(s.Tree, s.Frame, opts(v.style, 40, 8, false))},
				{"console", ConsoleView(consoleWith("inspect inputs.region", `"eu-west-1"`), "", opts(v.style, 60, 5, true))},
				{"help", HelpView(s.Keys, s.Verbs, opts(v.style, 80, 60, false), 0)},
			} {
				b.WriteString("=== " + part.name + "\n")
				b.WriteString(part.text + "\n")
				for _, line := range lines(part.text) {
					assert.LessOrEqual(t, lipgloss.Width(line), 100, part.name)
				}
			}
			tuitest.Golden(t, b.String())
		})
	}
}

func consoleWith(typed string, said ...string) Console {
	c := NewConsole()
	for _, line := range said {
		c.Say(line)
	}
	c.Insert(typed)

	return c
}

func TestEveryFrameShapeIsDrawnAndSaysWhatIsMissing(t *testing.T) {
	cases := map[string]struct {
		build func(*fakeTarget) *fakeTarget
		// program is whether the program's step list is given.
		program bool
		want    []string
		not     []string
	}{
		"held with the program": {build: func(f *fakeTarget) *fakeTarget { return f }, program: true,
			want: []string{"checkout", "build", "6 step(s)", "inputs"}},
		"no program": {build: func(f *fakeTarget) *fakeTarget { return f },
			want: []string{debugpaneNote, "inputs"}, not: []string{"6 step(s)"}},
		"inspect refused": {build: func(f *fakeTarget) *fakeTarget { f.denyInspect = true; return f }, program: true,
			want: []string{"inspect is not permitted", "build"}, not: []string{"eu-west-1"}},
		"the run is over": {build: func(f *fakeTarget) *fakeTarget { f.at = len(f.program); return f }, program: true,
			want: []string{"not held", "completed"}, not: []string{"6 step(s)"}},
	}
	for name, test := range cases {
		t.Run(name, func(t *testing.T) {
			s := screenOf(t, test.build(newFake()), test.program, tui.Size{W: 120, H: 36})
			text, _ := s.Draw(plain)
			for _, want := range test.want {
				assert.Contains(t, text, want)
			}
			for _, not := range test.not {
				assert.NotContains(t, text, not)
			}
			tuitest.Fits(t, text, s.Size)
		})
	}
}

// debugpaneNote is the sentence the step pane gives a target with no program.
const debugpaneNote = "no step inventory; pass --program"

func TestAPartialFrameSaysEarlierStepsAreNotShown(t *testing.T) {
	t.Parallel()

	f := newFake()
	s := screenOf(t, f, true, tui.Size{W: 120, H: 36})
	s.Frame.Partial = true
	text, _ := s.Draw(plain)
	assert.Contains(t, text, "earlier steps not shown")
}

// TestTheScreenGolden pins one rich frame at each width the layout folds at,
// and under the floor.
func TestTheScreenGolden(t *testing.T) {
	for _, size := range []tui.Size{{W: 40, H: 12}, {W: 60, H: 16}, {W: 80, H: 24}, {W: 100, H: 30}, {W: 120, H: 36}} {
		t.Run(size.String(), func(t *testing.T) {
			s := opened(t, newFake(), true, size)
			s.Console = consoleWith("step", "debug> inspect inputs.region", `"eu-west-1"`)
			text, _ := s.Draw(styled)
			tuitest.Golden(t, text)
			tuitest.Fits(t, text, size)
		})
	}
}

func TestTheScreenGoldenInTheOtherVariants(t *testing.T) {
	for _, v := range styles[1:] {
		for _, size := range []tui.Size{{W: 80, H: 24}, {W: 120, H: 36}} {
			t.Run(v.name+"/"+size.String(), func(t *testing.T) {
				s := opened(t, newFake(), true, size)
				text, _ := s.Draw(v.style)
				tuitest.Golden(t, text)
				tuitest.Fits(t, text, size)
			})
		}
	}
}

func TestTheHelpAndTabbedScreensGolden(t *testing.T) {
	s := opened(t, newFake(), true, tui.Size{W: 100, H: 30})
	s.Help = true
	text, _ := s.Draw(styled)
	t.Run("help", func(t *testing.T) { tuitest.Golden(t, text) })

	s = opened(t, newFake(), true, tui.Size{W: 70, H: 20})
	s.Pane, s.Focus = paneScope, paneScope
	s.Toast = s.Toast.Show(ui.ToneWarning, "the command was not applied: the run is not at a boundary")
	text, _ = s.Draw(styled)
	t.Run("tabbed scope with a toast", func(t *testing.T) { tuitest.Golden(t, text) })
}

// ---- the size matrix ----

func TestTheScreenFitsEverySizeInEveryState(t *testing.T) {
	t.Parallel()

	long := strings.Repeat("a very long answer ", 30)
	states := map[string]func(*Screen){
		"collapsed":    func(*Screen) {},
		"opened":       func(s *Screen) { s.Tree.Toggle("g:inputs"); s.Tree.Toggle("g:steps") },
		"help":         func(s *Screen) { s.Help = true },
		"toast":        func(s *Screen) { s.Toast = s.Toast.Show(ui.ToneWarning, long) },
		"busy":         func(s *Screen) { s.Busy = long },
		"long console": func(s *Screen) { s.Console = consoleWith(long, long, long, long, long, long, long, long) },
		"console":      func(s *Screen) { s.Focus = paneConsole },
		"scope tab":    func(s *Screen) { s.Pane = paneScope },
		"unloaded":     func(s *Screen) { s.Loaded = false },
		"problem":      func(s *Screen) { s.Frame.Scope, s.Problem = nil, long },
	}

	for name, mod := range states {
		for _, size := range tuitest.Sizes {
			for _, v := range styles {
				t.Run(name+"/"+size.String()+"/"+v.name, func(t *testing.T) {
					s := screenOf(t, newFake().withBig(40), true, size)
					mod(&s)
					text, hits := s.Draw(v.style)
					tuitest.Fits(t, text, size)
					assert.Len(t, lines(text), size.H, "the screen is not exactly the terminal's height")
					require.NotNil(t, hits)
					if size.W < MinWidth || size.H < MinHeight {
						assert.Zero(t, hits.Len(), "a sentence has nothing to click")
					}
				})
			}
		}
	}
}
