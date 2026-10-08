package tui_test

import (
	"strings"
	"testing"

	tea "charm.land/bubbletea/v2"
	"charm.land/lipgloss/v2"
	"github.com/charmbracelet/colorprofile"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/picatz/flowstate/cmd/flow/internal/pane"
	"github.com/picatz/flowstate/cmd/flow/internal/tui"
	"github.com/picatz/flowstate/cmd/flow/internal/tui/tuitest"
	"github.com/picatz/flowstate/cmd/flow/internal/ui"
)

func theme(profile colorprofile.Profile) (ui.Theme, ui.SymbolSet) {
	caps := ui.Capabilities{Profile: profile, TTY: true, Width: 80, Height: 24, Unicode: true}

	return ui.NewTheme(true, caps), caps.Symbols()
}

func TestAGridFoldsByWidthAndRefusesWhatIsTooSmall(t *testing.T) {
	t.Parallel()

	grid := tui.Grid{
		Min: tui.Size{W: 20, H: 4},
		Rules: []tui.Rule{
			{MinWidth: 100, Root: tui.Divide(pane.Split{Gap: 1}, tui.Leaf("a"), tui.Divide(pane.Split{Gap: 1}, tui.Leaf("b"), tui.Leaf("c")))},
			{MinWidth: 50, Root: tui.Divide(pane.Split{Orientation: pane.Rows}, tui.Leaf("a"), tui.Leaf("b"))},
			{MinWidth: 20, Tabs: []string{"a", "b"}},
		},
	}

	wide, err := grid.Resolve(pane.Rect{W: 100, H: 10}, "a")
	require.NoError(t, err)
	assert.Equal(t, 0, wide.Rule)
	assert.True(t, wide.Shown("c"))
	covered := 0
	for _, c := range wide.Cells {
		covered += c.Rect.W
	}
	assert.Equal(t, 100-2, covered, "two one-cell gaps and every other cell in a pane")

	mid, err := grid.Resolve(pane.Rect{W: 99, H: 10}, "a")
	require.NoError(t, err)
	assert.Equal(t, 1, mid.Rule)
	assert.False(t, mid.Shown("c"), "a pane the rule does not name was drawn")

	narrow, err := grid.Resolve(pane.Rect{X: 3, Y: 2, W: 49, H: 10}, "b")
	require.NoError(t, err)
	assert.Equal(t, []string{"a", "b"}, narrow.Tabs)
	require.Len(t, narrow.Cells, 1)
	assert.Equal(t, "b", narrow.Cells[0].Pane, "the focused tab is the one shown")
	assert.Equal(t, pane.Rect{X: 3, Y: 3, W: 49, H: 9}, narrow.Cells[0].Rect, "the tab row is above the pane")
	assert.Equal(t, pane.Rect{X: 3, Y: 2, W: 49, H: 1}, narrow.TabRow)

	other, err := grid.Resolve(pane.Rect{W: 49, H: 10}, "not-a-tab")
	require.NoError(t, err)
	assert.Equal(t, "a", other.Cells[0].Pane, "an unknown focus shows the first tab")

	for _, area := range []pane.Rect{{W: 19, H: 10}, {W: 80, H: 3}, {}} {
		_, err := grid.Resolve(area, "a")
		var small tui.ErrTooSmall
		require.ErrorAs(t, err, &small, "%+v", area)
		assert.Contains(t, err.Error(), "at least 20x4")
	}
}

func TestTheFocusRingWrapsBothWaysAndIgnoresStrangers(t *testing.T) {
	t.Parallel()

	ring := tui.NewRing("a", "b", "c")
	assert.Equal(t, "a", ring.Current())
	assert.Equal(t, "b", ring.Next().Current())
	assert.Equal(t, "c", ring.Prev().Current(), "prev from the first wraps to the last")
	assert.Equal(t, "a", ring.Next().Next().Next().Current())
	assert.Equal(t, "c", ring.Set("c").Current())
	assert.Equal(t, "a", ring.Set("nope").Current(), "a name that is not a member moved focus")
	assert.True(t, ring.Is("a"))

	empty := tui.NewRing()
	assert.Equal(t, "", empty.Next().Current())
	assert.Equal(t, "", empty.Prev().Current())
}

func TestAKeymapMatchesAndTeachesTheSameKeys(t *testing.T) {
	t.Parallel()

	keys, err := tui.NewKeymap(
		tui.Binding{Name: "verb:step", Keys: []string{"s", "space"}, Help: "run the next step", Group: "Run", Hint: true, Short: "step"},
		tui.Binding{Name: "verb:next", Keys: []string{"n"}, Help: "run this step whole", Group: "Run"},
		tui.Binding{Name: "help", Keys: []string{"?"}, Help: "show help", Group: "Screen", Hint: true},
	)
	require.NoError(t, err)

	got, ok := keys.Match("space")
	require.True(t, ok)
	assert.Equal(t, "verb:step", got.Name)
	_, ok = keys.Match("x")
	assert.False(t, ok)
	_, ok = keys.Named("help")
	assert.True(t, ok)

	th, _ := theme(colorprofile.NoTTY)
	help := strings.Join(keys.Help(60, th), "\n")
	for _, binding := range keys.Bindings() {
		assert.Contains(t, help, binding.Help, "the help omits a bound key")
		assert.Contains(t, help, binding.Keys[0])
	}
	assert.Less(t, strings.Index(help, "Run"), strings.Index(help, "Screen"), "groups lose their order")

	hints := keys.Hints(80, th)
	assert.Equal(t, "s step  ? help", hints, "only hinted bindings, by their short word, else their name")
	assert.Equal(t, "s step", keys.Hints(10, th), "a hint that does not fit is not cut in half")
	assert.Equal(t, "", keys.Hints(2, th))
}

func TestABarAndAToastAreExactlyAsWideAsAsked(t *testing.T) {
	t.Parallel()

	th, sym := theme(colorprofile.TrueColor)
	for _, width := range []int{1, 5, 20, 80} {
		for _, bar := range []tui.Bar{{}, {Left: "left"}, {Left: "left", Right: "right"}, {Left: strings.Repeat("x", 100), Right: "r"}} {
			assert.Equal(t, width, lipgloss.Width(bar.View(width)), "%+v at %d", bar, width)
		}
		assert.Equal(t, width, lipgloss.Width(tui.Toast{}.View(width, th, sym)))
		shown := tui.Toast{}.Show(ui.ToneDanger, strings.Repeat("long ", 50))
		assert.Equal(t, width, lipgloss.Width(shown.View(width, th, sym)))
	}
	assert.Equal(t, "", tui.Bar{Left: "x"}.View(0))

	// The right side gives way before the left is cut.
	assert.Equal(t, "leftside  ", tui.Bar{Left: "leftside", Right: "right"}.View(10))
	assert.True(t, strings.HasSuffix(tui.Bar{Left: "l", Right: "right"}.View(10), "right"))
}

func TestAToastEscapesWhatItIsToldAndIsClearedNotExpired(t *testing.T) {
	t.Parallel()

	th, sym := theme(colorprofile.NoTTY)
	toast := tui.Toast{}.Show(ui.ToneWarning, "bad \x1b[2J news")
	assert.True(t, toast.Active())
	assert.NotContains(t, toast.View(40, th, sym), "\x1b")
	assert.Contains(t, toast.View(40, th, sym), `\x1b[2J`)
	assert.Contains(t, toast.View(40, th, sym), sym.Warning, "the tone has a mark that outlives the colour")

	assert.False(t, toast.Clear().Active())
	assert.Equal(t, strings.Repeat(" ", 10), toast.Clear().View(10, th, sym))
}

func TestKeyNamesAreWhatAKeymapMatches(t *testing.T) {
	t.Parallel()

	for _, name := range []string{"s", "S", "?", ":", "/", "space", "enter", "esc", "tab", "shift+tab", "ctrl+c", "ctrl+d", "up", "pgdown", "backspace"} {
		assert.Equal(t, name, tui.KeyName(tuitest.Key(name)), "%q does not survive being pressed")
	}

	// A terminal that reports shift on a capital letter is the same key as one that does not.
	shifted := tea.KeyPressMsg{Code: 'b', ShiftedCode: 'B', Text: "B", Mod: tea.ModShift}
	assert.Equal(t, "B", tui.KeyName(shifted))
}

func TestFoldDropsCommandsAndRunExecutesThem(t *testing.T) {
	t.Parallel()

	m := counter{}
	folded := tuitest.Fold(m, "go", "go").(counter)
	assert.Equal(t, 2, folded.n, "the messages were applied")
	assert.Equal(t, 0, folded.echoed, "Fold ran a command")

	ran := tuitest.Run(m, "go").(counter)
	assert.Equal(t, 1, ran.n)
	assert.Equal(t, 1, ran.echoed, "Run did not feed a command's message back")

	// A model that asks for more forever ends the test instead of hanging it.
	loop := tuitest.Run(looper{}, "go").(looper)
	assert.Equal(t, tuitest.MaxDrain, loop.n)

	// Quitting stops delivery: what the same batch asked for after it is not sent.
	quit := tuitest.Run(quitter{}, "go").(quitter)
	assert.Equal(t, 1, quit.n)
}

func TestSizesIncludeTheFloorAndEachFold(t *testing.T) {
	t.Parallel()

	var widths []int
	for _, s := range tuitest.Sizes {
		widths = append(widths, s.W)
	}
	assert.Equal(t, []int{40, 60, 80, 100, 120, 200}, widths)
}

// counter answers "go" by counting it and asking for an echo.
type counter struct{ n, echoed int }

func (c counter) Init() tea.Cmd  { return nil }
func (c counter) View() tea.View { return tea.NewView("") }
func (c counter) Update(msg tea.Msg) (tea.Model, tea.Cmd) {
	switch msg {
	case "go":
		c.n++

		return c, func() tea.Msg { return "echo" }
	case "echo":
		c.echoed++
	}

	return c, nil
}

type looper struct{ n int }

func (l looper) Init() tea.Cmd  { return nil }
func (l looper) View() tea.View { return tea.NewView("") }
func (l looper) Update(tea.Msg) (tea.Model, tea.Cmd) {
	l.n++

	return l, func() tea.Msg { return "again" }
}

type quitter struct{ n int }

func (q quitter) Init() tea.Cmd  { return nil }
func (q quitter) View() tea.View { return tea.NewView("") }
func (q quitter) Update(tea.Msg) (tea.Model, tea.Cmd) {
	q.n++

	return q, tea.Batch(tea.Quit, func() tea.Msg { return "after" })
}
