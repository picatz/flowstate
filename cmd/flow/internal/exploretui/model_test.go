package exploretui

import (
	"go/parser"
	"go/token"
	"os"
	"path/filepath"
	"strings"
	"testing"

	tea "charm.land/bubbletea/v2"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/picatz/flowstate/cmd/flow/internal/tui"
	"github.com/picatz/flowstate/cmd/flow/internal/tui/tuitest"
	v1 "github.com/picatz/flowstate/pkg/flowstate/v1"
)

func press(m Model, names ...string) Model {
	for _, n := range names {
		m = send(m, tuitest.Key(n))
	}

	return m
}

func TestTheFirstReadFillsTheScreen(t *testing.T) {
	l := &loader{graph: fleet()}
	m := modelFor(t, l)
	assert.Contains(t, view(m), "reading", "before the read finishes the screen says so")

	m = tuitest.Start(m).(Model)
	out := view(m)
	for _, want := range []string{"flow explore", "fixtures", "3 workflows", "audit", "charge", "checkout", "1 FAILED, 2 RUNNING"} {
		assert.Contains(t, out, want)
	}
	assert.Equal(t, 1, l.calls)
	assert.False(t, m.Screen().Loading)
}

func TestKeysOpenAndCloseRowsAndShowTheirDetails(t *testing.T) {
	m, _ := started(t, fleet())
	assert.Contains(t, view(m), "audit", "the first row is selected")

	m = press(m, "j", "j", "enter")
	assert.Equal(t, []string{
		"audit|1 COMPLETED", "charge|", "checkout|1 FAILED, 2 RUNNING",
		"charge|calls", "approved|waits for", "http|uses x2",
	}, rowsOf(m))
	assert.Contains(t, view(m), "called by", "the details are of the selected row")

	m = press(m, "j", "l")
	assert.Equal(t, []string{"audit|calls  1 COMPLETED", "http|uses"}, rowsOf(m)[4:6], "right opens charge under checkout")

	// left closes an open row, and on a closed one goes to the parent.
	m = press(m, "h")
	assert.Equal(t, "charge", selectedLabel(m))
	m = press(m, "h")
	assert.Equal(t, "checkout", selectedLabel(m))
	m = press(m, "h")
	assert.Equal(t, []string{"audit|1 COMPLETED", "charge|", "checkout|1 FAILED, 2 RUNNING"}, rowsOf(m))
}

// rowsOf is the visible rows as "label|value".
func rowsOf(m Model) []string {
	var out []string
	for _, r := range m.Screen().Tree.Rows() {
		out = append(out, r.Label+"|"+r.Value)
	}

	return out
}

func selectedLabel(m Model) string {
	for _, r := range m.Screen().Tree.Rows() {
		if r.ID == m.Screen().Tree.Selected() {
			return r.Label
		}
	}

	return ""
}

func TestARefreshKeepsWhatWasOpenAndTheSelection(t *testing.T) {
	m, l := started(t, fleet())
	m = press(m, "j", "j", "enter", "j", "l", "j")
	before := m.Screen().Tree.Selected()
	require.NotEmpty(t, before)

	// The system moved on: audit now has a failed run too.
	l.set(withRun(fleet(), "audit", v1.RunResponse_STATUS_FAILED), nil)
	m = press(m, "r")

	assert.Equal(t, 2, l.calls)
	assert.Equal(t, before, m.Screen().Tree.Selected(), "the selection is the same row")
	assert.Contains(t, view(m), "1 COMPLETED, 1 FAILED", "the new counts are shown")
	assert.Len(t, rowsOf(m), 8, "checkout and charge are still open")
}

func TestAFailedRefreshKeepsTheGraphAndSaysWhy(t *testing.T) {
	m, l := started(t, fleet())
	l.set(nil, errRefused)
	m = press(m, "r")

	out := view(m)
	assert.Contains(t, out, "checkout", "what was known stays on screen")
	assert.Contains(t, out, "cannot read the graph: the server refused")
	assert.Contains(t, m.Screen().Problem, "refused")

	m = press(m, "j")
	assert.NotContains(t, view(m), "cannot read", "a toast lasts to the next key")
}

func TestAFirstReadThatFailsShowsTheProblemInTheTree(t *testing.T) {
	l := &loader{err: errRefused}
	m := tuitest.Start(modelFor(t, l)).(Model)

	assert.Contains(t, view(m), "the server refused")
	assert.Nil(t, m.Screen().Index)
	// r reads again.
	l.set(fleet(), nil)
	m = press(m, "r")
	assert.Contains(t, view(m), "checkout")
	assert.Empty(t, m.Screen().Problem)
}

func TestARefreshDuringARefreshIsRefusedAndAStaleAnswerIsDropped(t *testing.T) {
	l := &loader{graph: fleet()}
	m := modelFor(t, l)

	// Init has not been answered: the screen is reading.
	m, _ = pressNoRun(m, "r")
	assert.Contains(t, m.Screen().Toast.Text(), "still reading")

	// An answer for a read that is no longer the latest changes nothing.
	next, _ := m.Update(graphMsg{seq: m.seq - 1, graph: fleet()})
	assert.Nil(t, next.(Model).Screen().Index)
	next, _ = m.Update(graphMsg{seq: m.seq, graph: fleet()})
	assert.NotNil(t, next.(Model).Screen().Index)
}

func pressNoRun(m Model, name string) (Model, tea.Cmd) {
	next, cmd := m.Update(tuitest.Key(name))

	return next.(Model), cmd
}

func TestClickingARowOpensItAndAClickElsewhereDoesNothing(t *testing.T) {
	m, _ := started(t, fleet())
	x, y := find(t, m, "checkout")

	m = send(m, tuitest.Click(x, y))
	assert.Len(t, rowsOf(m), 6)
	assert.Equal(t, "checkout", selectedLabel(m))

	m = send(m, tuitest.Click(x, y))
	assert.Len(t, rowsOf(m), 3, "a second click closes it")

	before := view(m)
	m = send(m, tuitest.Click(0, 0), tuitest.RightClick(x, y))
	assert.Equal(t, before, view(m))
}

func TestTheWheelScrollsTheTree(t *testing.T) {
	g := &v1.Graph{}
	for i := range 60 {
		g.Nodes = append(g.Nodes, node(wf, "wf"+string(rune('a'+i/26))+string(rune('a'+i%26))))
	}
	m, _ := started(t, g)
	x, y := find(t, m, "wfaa")

	m = send(m, tuitest.Wheel(x, y, false), tuitest.Wheel(x, y, false))
	assert.Positive(t, m.Screen().Tree.Top())
	m = send(m, tuitest.Wheel(x, y+1, true), tuitest.Wheel(x, y+1, true))
	assert.Zero(t, m.Screen().Tree.Top())
}

func TestTheSelectionIsKeptOnScreen(t *testing.T) {
	g := &v1.Graph{}
	for i := range 60 {
		g.Nodes = append(g.Nodes, node(wf, "wf"+string(rune('a'+i/26))+string(rune('a'+i%26))))
	}
	m, _ := started(t, g, func(c *Config) { c.Size = tui.Size{W: 80, H: 12} })

	m = press(m, "G")
	assert.Contains(t, view(m), "wfch", "the last row is shown")
	m = press(m, "home")
	assert.Contains(t, view(m), "wfaa")
	m = press(m, "pgdown")
	assert.NotEqual(t, "wfaa", selectedLabel(m))
}

func TestHelpListsTheKeysAndLeavesWithEscOrQuestionMark(t *testing.T) {
	m, _ := started(t, fleet())
	m = press(m, "?")
	out := view(m)
	for _, want := range []string{"help", "open or close the row", "read the files and the server again"} {
		assert.Contains(t, out, want)
	}
	m = press(m, "j", "esc")
	assert.False(t, m.Screen().Help)
	assert.Contains(t, view(m), "checkout")

	m = press(m, "?", "?")
	assert.False(t, m.Screen().Help)
}

func TestTheScreenLeavesOnQuitAndInterrupt(t *testing.T) {
	for _, name := range []string{"q", "ctrl+c", "ctrl+d"} {
		m, _ := started(t, fleet())
		assert.True(t, press(m, name).Done(), name)

		m, _ = started(t, fleet())
		m = press(m, "?")
		assert.True(t, press(m, name).Done(), "%s in the help", name)
	}
}

func TestATerminalTooSmallIsSaidOnceAndKeysDoNothing(t *testing.T) {
	m, _ := started(t, fleet(), func(c *Config) { c.Size = tui.Size{W: 30, H: 8} })
	assert.Contains(t, view(m), "needs at least 40x10")

	before := m.Screen().Tree.Selected()
	m = press(m, "j", "enter")
	assert.Equal(t, before, m.Screen().Tree.Selected())
	assert.True(t, press(m, "q").Done(), "q still leaves")

	m = send(m, tuitest.Resize(tui.Size{W: 100, H: 30}))
	assert.Contains(t, view(m), "checkout")
}

func TestALabelWithControlCharactersIsNeverWrittenRaw(t *testing.T) {
	g := &v1.Graph{
		Nodes: []*v1.GraphNode{node(wf, "evil\x1b[2Jname\x07"), node(task, "t\x1b]0;x\x07")},
		Edges: []*v1.GraphEdge{edge(uses, wf, "evil\x1b[2Jname\x07", task, "t\x1b]0;x\x07", 1)},
	}
	m, _ := started(t, g)
	m = press(m, "enter")

	out := view(m)
	assert.NotContains(t, out, "\x1b[2J")
	assert.NotContains(t, out, "\x1b]0;")
	assert.NotContains(t, out, "\x07")
}

func TestEverySizeFitsItsTerminal(t *testing.T) {
	for _, size := range tuitest.Sizes {
		t.Run(size.String(), func(t *testing.T) {
			m, _ := started(t, fleet(), func(c *Config) { c.Size = size })
			m = press(m, "j", "j", "enter", "j", "l")
			tuitest.Fits(t, view(m), size)

			m = press(m, "?")
			tuitest.Fits(t, view(m), size)
		})
	}
}

// ---- hygiene ----

// The screen is a function of the messages it was sent: no clocks.
func TestNoViewCodeReadsAClock(t *testing.T) {
	t.Parallel()

	files, err := filepath.Glob("*.go")
	require.NoError(t, err)
	for _, file := range files {
		if strings.HasSuffix(file, "_test.go") {
			continue
		}
		source, err := os.ReadFile(file)
		require.NoError(t, err)
		for _, banned := range []string{"time.Now(", "time.Tick(", "time.After(", "time.Since(", "time.NewTimer(", "time.NewTicker(", "tea.Tick(", "tea.Every("} {
			assert.NotContains(t, string(source), banned, "%s reads or waits on a clock", file)
		}
	}
}

// The explorer is one client of the shared parts and imports neither the
// debugger nor anything that talks to a server.
func TestTheExplorerImportsNoOtherClientAndNoTransport(t *testing.T) {
	t.Parallel()

	files, err := filepath.Glob("*.go")
	require.NoError(t, err)
	for _, file := range files {
		if strings.HasSuffix(file, "_test.go") {
			continue
		}
		parsed, err := parser.ParseFile(token.NewFileSet(), file, nil, parser.ImportsOnly)
		require.NoError(t, err)
		for _, spec := range parsed.Imports {
			for _, banned := range []string{"internal/debugtui", "internal/debugpane", "connectrpc", "flowdebug", "net/http"} {
				assert.NotContains(t, spec.Path.Value, banned, "%s imports %s", file, banned)
			}
		}
	}
}

func TestFilteringNarrowsTheWorkflowsAsYouTypeAndEscClearsIt(t *testing.T) {
	m, _ := started(t, fleet())

	m = press(m, "f", "c", "h")
	assert.True(t, m.Screen().Filtering)
	assert.Equal(t, []string{"charge|", "checkout|1 FAILED, 2 RUNNING"}, rowsOf(m))
	assert.Contains(t, view(m), `2 of 3 workflows match "ch"`)
	assert.Contains(t, view(m), "filter: ch", "the prompt is on the status line")

	m = press(m, "e", "c")
	assert.Equal(t, []string{"checkout|1 FAILED, 2 RUNNING"}, rowsOf(m), "only checkout matches chec")
	assert.Equal(t, "checkout", selectedLabel(m), "the selection moves to a row that is still there")

	m = press(m, "backspace", "backspace", "enter")
	assert.False(t, m.Screen().Filtering, "enter returns the keys to the tree")
	assert.Equal(t, "ch", m.Screen().Filter, "and keeps the filter")
	assert.Len(t, rowsOf(m), 2)

	m = press(m, "esc")
	assert.Empty(t, m.Screen().Filter)
	assert.Len(t, rowsOf(m), 3)
}

func TestAFilterMatchesTheWorkflowNameAndNotItsKind(t *testing.T) {
	m, _ := started(t, fleet())

	m = press(m, "f", "W", "O", "R", "K")
	assert.Empty(t, rowsOf(m), "the node id carries the kind but the name is what people type")
	assert.Contains(t, view(m), "0 of 3 workflows match")

	m = press(m, "ctrl+u", "A", "U", "D")
	assert.Equal(t, []string{"audit|1 COMPLETED"}, rowsOf(m), "case is ignored")
}

func TestKeysTypedIntoTheFilterAreNotCommands(t *testing.T) {
	m, l := started(t, fleet())

	m = press(m, "f", "r", "q", "?")
	assert.Equal(t, 1, l.calls, "r did not refresh")
	assert.False(t, m.Done(), "q did not quit")
	assert.False(t, m.Screen().Help, "? did not open help")
	assert.Equal(t, "rq?", m.Screen().Filter)

	m = press(m, "ctrl+c")
	assert.True(t, m.Done(), "ctrl+c still leaves")
}

func TestAFilterKeepsWhatIsOpenAndIsBounded(t *testing.T) {
	m, _ := started(t, fleet())
	m = press(m, "j", "j", "enter")
	opened := len(rowsOf(m))
	require.Greater(t, opened, 3)

	m = press(m, "f", "c", "enter")
	assert.Len(t, rowsOf(m), opened-1, "charge and checkout match, audit goes, and checkout stays open")

	m = press(m, "f")
	for range 2 * maxFilter {
		m = press(m, "x")
	}
	assert.Len(t, []rune(m.Screen().Filter), maxFilter)
}
