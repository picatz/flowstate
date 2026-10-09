package debugtui

import (
	"fmt"
	"strconv"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/picatz/flowstate/cmd/flow/internal/pane"
	"github.com/picatz/flowstate/cmd/flow/internal/tui"
	"github.com/picatz/flowstate/cmd/flow/internal/tui/tuitest"
	v1 "github.com/picatz/flowstate/pkg/flowstate/v1"
	"github.com/picatz/flowstate/pkg/flowstate/v1/flowdebug"
)

// typed is a line typed at the console and submitted, from wherever focus is.
func typed(m Model, line string) Model {
	if m.screen.Focus != paneConsole {
		m = send(m, tuitest.Key(":"))
	}

	return send(m, append(tuitest.Keys(line), tuitest.Key("enter"))...)
}

// consoleLines are the rows the console is drawn in.
func consoleLines(t *testing.T, m Model) []string {
	t.Helper()

	g, err := m.screen.geometry()
	require.NoError(t, err)

	return lines(view(m))[g.console.Y : g.console.Y+g.console.H]
}

func transcript(m Model) string { return strings.Join(m.screen.Console.Lines(), "\n") }

// ---- expand ----

// TestExpandPagesFromTheConsole: a value typed at `inspect` is a tree in the
// scope pane, and enter on a collapsed row, or on its "... more" row, is the
// console's own `expand`, run through the driver a page at a time.
func TestExpandPagesFromTheConsole(t *testing.T) {
	t.Parallel()

	fake := newFake().withList(250)
	m := started(t, fake)
	m = typed(m, "inspect steps.list")

	root := resultPrefix + "steps.list"
	node, ok := m.screen.Tree.Node(root)
	require.True(t, ok, "the inspection is not a row in the scope pane")
	assert.Equal(t, 250, node.Total)
	assert.Empty(t, node.Children, "the children are asked for when the row is opened, not before")
	assert.Equal(t, root, m.screen.Tree.Selected())
	assert.Contains(t, transcript(m), "[250 items]", "the answer is also in the transcript")

	// enter on the collapsed row is `expand`.
	m = send(m, tuitest.Key("esc"), tuitest.Key("tab"))
	require.Equal(t, paneScope, m.screen.Focus)
	asked := len(fake.inspects)
	m = send(m, tuitest.Key("enter"))
	last := fake.inspects[len(fake.inspects)-1]
	assert.Greater(t, len(fake.inspects), asked)
	assert.Equal(t, "steps.list", last.GetExpression())
	assert.True(t, last.GetChildren(), "the row was not expanded through the children listing")
	assert.Zero(t, last.GetOffset())
	assert.Contains(t, transcript(m), "debug> expand steps.list\n", "the expand is the console's, echoed like a typed one")
	node, _ = m.screen.Tree.Node(root)
	assert.Len(t, node.Children, flowdebug.DefaultInspectLimit, "one page, not the whole list")
	assert.True(t, m.screen.Tree.Open(root))

	// The "... 150 more" row is the next page: `expand steps.list from 100`.
	m = send(m, tuitest.Key("end"))
	require.Equal(t, "more:"+root, m.screen.Tree.Selected())
	m = send(m, tuitest.Key("enter"))
	last = fake.inspects[len(fake.inspects)-1]
	assert.EqualValues(t, flowdebug.DefaultInspectLimit, last.GetOffset())
	assert.Contains(t, transcript(m), "debug> expand steps.list from 100")
	node, _ = m.screen.Tree.Node(root)
	assert.Len(t, node.Children, 200)

	m = send(m, tuitest.Key("end"), tuitest.Key("enter"))
	node, _ = m.screen.Tree.Node(root)
	require.Len(t, node.Children, 250, "the last page did not complete the list")
	for _, row := range m.screen.Tree.Rows() {
		assert.NotEqual(t, pane.RowMore, row.Kind, "a complete list still offers more")
	}
	assert.Equal(t, "steps.list.[0]", node.Children[0].ID[strings.Index(node.Children[0].ID, idSep)+1:])

	// Typed, `expand ... from N` pages the same inspection; past the end it
	// changes nothing and says so.
	before := len(node.Children)
	m = typed(m, "expand steps.list from 250")
	assert.Contains(t, transcript(m), "steps.list has 250 children; none from 250")
	node, _ = m.screen.Tree.Node(root)
	assert.Len(t, node.Children, before, "a page past the end added rows")

	m = typed(m, "expand steps.list")
	node, _ = m.screen.Tree.Node(root)
	assert.Len(t, node.Children, flowdebug.DefaultInspectLimit, "expand from the start did not restart the listing")
}

// TestAnExpandAnsweredAfterTheRunMovedJoinsNothing: a page is of the stop it was
// asked at; the tree a later stop built is not given its children.
func TestAnExpandAnsweredAfterTheRunMovedJoinsNothing(t *testing.T) {
	t.Parallel()

	fake := newFake().withList(250)
	m := started(t, fake)
	m = typed(m, "inspect steps.list")
	root := resultPrefix + "steps.list"
	m = send(m, tuitest.Key("esc"), tuitest.Key("tab"))

	cmd := m.load(pane.Request{Parent: root})
	require.NotNil(t, cmd)
	msg := cmd().(doneMsg)
	msg.fillRev = m.frameRev - 1
	m = send(m, msg)
	node, _ := m.screen.Tree.Node(root)
	assert.Empty(t, node.Children, "a page of an earlier stop was appended")

	// A hostile name is not typed as a command: it is read by the screen.
	hostile := pane.Request{Parent: "steps.\x1b[2Jx"}
	m.screen.Busy = ""
	require.NotNil(t, m.load(hostile))
	assert.Empty(t, m.screen.Busy, "a name with a control character became a command line")
}

// ---- watches ----

func watchTarget() *fakeTarget {
	fake := newFake()
	fake.timelined = true
	fake.eval = func(expression string, at int) (string, string, bool) {
		if expression == "stop" {
			return strconv.Itoa(at), "", true
		}

		return "", "", false
	}

	return fake
}

func revisionsOf(fake *fakeTarget, expression string) []uint64 {
	var out []uint64
	for _, req := range fake.inspects {
		if req.GetExpression() == expression && !req.GetChildren() {
			out = append(out, req.GetRevision())
		}
	}

	return out
}

func watchValue(m Model, expr string) string {
	for _, w := range m.screen.Watches {
		if w.Expr == expr {
			return w.Value.GetRendered()
		}
	}

	return "<no such watch>"
}

// TestAWatchFollowsEveryStopAndTravel: a watch is read through Target.Inspect at
// the stop each frame read is of, so a step and a goto each show the value of
// the stop they land on, and nothing between them asks.
func TestAWatchFollowsEveryStopAndTravel(t *testing.T) {
	t.Parallel()

	fake := watchTarget()
	m := started(t, fake)
	m = typed(m, "watch stop")
	require.Len(t, m.screen.Watches, 1)
	assert.Equal(t, "1", watchValue(m, "stop"))
	assert.Contains(t, transcript(m), "debug> watch stop")
	assert.Empty(t, fake.resumes, "a watch is the screen's and moves nothing")

	rows := strings.Join(paneLines(t, m, paneScope), "\n")
	assert.Regexp(t, `watches\s+\{1\}`, rows)
	assert.Regexp(t, `stop\s+1`, rows, "the watch is not drawn with its value")

	m = send(m, tuitest.Key("esc"), tuitest.Key("s"))
	assert.Equal(t, "2", watchValue(m, "stop"), "a step did not refresh the watch")

	m = typed(m, "goto 0")
	require.Equal(t, []int32{0}, fake.travels)
	assert.Equal(t, "0", watchValue(m, "stop"), "a travel did not refresh the watch")
	assert.Regexp(t, `stop\s+0`, strings.Join(paneLines(t, m, paneScope), "\n"))

	// One inspection per stop, each at the revision of the stop it is of.
	assert.Equal(t, []uint64{2, 3, 1}, revisionsOf(fake, "stop"))

	// The run ending is not a failure of the watch: it is not held, which the row
	// says, and nothing is asked.
	m = send(m, tuitest.Key("esc"), tuitest.Key("c"))
	assert.Equal(t, []uint64{2, 3, 1}, revisionsOf(fake, "stop"), "a watch was evaluated with no held run")
	assert.True(t, m.screen.Watches[0].NotHeld)
	assert.Regexp(t, `stop\s+\(not held\)`, strings.Join(paneLines(t, m, paneScope), "\n"))
}

// TestTheWKeyWatchesTheSelectedRow: the key is the verb for the row under the
// cursor, and a group has no expression to watch.
func TestTheWKeyWatchesTheSelectedRow(t *testing.T) {
	t.Parallel()

	m := started(t, newFake())
	m = send(m, tuitest.Key("tab"), tuitest.Key("w"))
	assert.Empty(t, m.screen.Watches)
	assert.Contains(t, m.screen.Toast.Text(), "select a name")

	m = send(m, tuitest.Key("enter"), tuitest.Key("j"), tuitest.Key("w"))
	require.Len(t, m.screen.Watches, 1)
	assert.Equal(t, "inputs.version", m.screen.Watches[0].Expr)
	assert.Equal(t, `"2026.9.0"`, watchValue(m, "inputs.version"))
	assert.Equal(t, watchPrefix+"inputs.version", m.screen.Tree.Selected(), "the new watch is not the selected row")

	// A watch row is watched by its expression, so `i` and `w` on it mean the
	// same name; watching it again is refused, not duplicated.
	m = send(m, tuitest.Key("w"))
	assert.Len(t, m.screen.Watches, 1)
	assert.Contains(t, m.screen.Toast.Text(), "already watching")
	m = send(m, tuitest.Key("i"))
	assert.Equal(t, "inspect inputs.version", m.screen.Console.Text)
}

// TestTheSeventeenthWatchIsRefused: the bound is on the work a stop costs, so
// the seventeenth is refused in one line and never asked about.
func TestTheSeventeenthWatchIsRefused(t *testing.T) {
	t.Parallel()

	fake := newFake()
	m := started(t, fake)
	for i := range MaxWatches {
		m = typed(m, fmt.Sprintf("watch inputs.version == %q + %d", "x", i))
	}
	require.Len(t, m.screen.Watches, MaxWatches, "the sixteenth watch was refused")
	assert.Empty(t, m.screen.Toast.Text())

	asked := len(fake.inspects)
	m = typed(m, "watch inputs.region")
	assert.Len(t, m.screen.Watches, MaxWatches)
	assert.Equal(t, "at most 16 watches; unwatch one first", m.screen.Toast.Text())
	assert.NotContains(t, strings.Join(watchExprs(m.screen.Watches), "\n"), "inputs.region")
	assert.Contains(t, transcript(m), "at most 16 watches")
	for _, req := range fake.inspects[asked:] {
		assert.NotEqual(t, "inputs.region", req.GetExpression(), "a refused watch was evaluated")
	}

	// Removing one makes room, by position or by spelling.
	m = typed(m, "unwatch 1")
	require.Len(t, m.screen.Watches, MaxWatches-1)
	m = typed(m, "watch inputs.region")
	assert.Len(t, m.screen.Watches, MaxWatches)
	m = typed(m, "unwatch inputs.region")
	assert.Len(t, m.screen.Watches, MaxWatches-1)

	// Nothing else is removed by a number out of range or a name nobody watches.
	m = typed(m, "unwatch 99")
	assert.Contains(t, m.screen.Toast.Text(), "no such watch")
	m = typed(m, "unwatch nothing.here")
	assert.Contains(t, m.screen.Toast.Text(), "no such watch")
	assert.Len(t, m.screen.Watches, MaxWatches-1)

	// An unwatch of 0 is no position at all.
	m = typed(m, "unwatch 0")
	assert.Len(t, m.screen.Watches, MaxWatches-1)
}

// TestAWatchIsBoundedByTheLengthOfACommand: an expression a command line could
// not carry is not kept to be evaluated at every stop.
func TestAWatchIsBoundedByTheLengthOfACommand(t *testing.T) {
	t.Parallel()

	m := started(t, newFake())

	exact := strings.Repeat("a", flowdebug.MaxCommandBytes)
	m.watch(exact)
	assert.Len(t, m.screen.Watches, 1, "an expression of exactly the longest command was refused")

	m.watch(strings.Repeat("a", flowdebug.MaxCommandBytes+1))
	assert.Len(t, m.screen.Watches, 1)
	assert.Contains(t, m.screen.Toast.Text(), "longer than")

	m.watch("one\ntwo")
	assert.Len(t, m.screen.Watches, 1, "a watch is one line")
	m.watch("   ")
	assert.Len(t, m.screen.Watches, 1)
	assert.Contains(t, m.screen.Toast.Text(), "watch needs an expression")
}

// TestAWatchThatCannotEvaluateSaysSoOnce: the console says why when the watch
// starts failing, the row says it at every stop, and an answer clears it
// without a word.
func TestAWatchThatCannotEvaluateSaysSoOnce(t *testing.T) {
	t.Parallel()

	fake := newFake()
	broken := true
	fake.eval = func(expression string, at int) (string, string, bool) {
		if expression != "flaky" {
			return "", "", false
		}
		if broken {
			return "", "undefined here", true
		}

		return strconv.Itoa(at), "", true
	}
	said := func(m Model) int { return strings.Count(transcript(m), "watch flaky: undefined here") }

	m := started(t, fake)
	m = typed(m, "watch flaky")
	assert.Equal(t, 1, said(m))
	assert.Regexp(t, `flaky\s+\(undefined here\)`, strings.Join(paneLines(t, m, paneScope), "\n"), "the row does not say it")

	// Failing at every later stop is not said again.
	m = send(m, tuitest.Key("esc"), tuitest.Key("s"))
	m = send(m, tuitest.Key("s"))
	assert.Equal(t, 1, said(m), "the failure was repeated at a later stop")
	assert.Equal(t, "undefined here", m.screen.Watches[0].Err)

	// It evaluates again: the row has the value, and the console is silent.
	fake.mu.Lock()
	broken = false
	fake.mu.Unlock()
	before := len(m.screen.Console.Lines())
	m = send(m, tuitest.Key("s"))
	assert.Empty(t, m.screen.Watches[0].Err)
	assert.Equal(t, "4", watchValue(m, "flaky"))
	assert.Equal(t, 1, said(m))
	assert.NotContains(t, strings.Join(m.screen.Console.Lines()[before:], "\n"), "flaky", "the recovery was announced")

	// A name the target has no answer for fails the same way, with its own words.
	m = typed(m, "watch nosuch.name")
	assert.Contains(t, transcript(m), "watch nosuch.name: no such name: nosuch.name")
}

// TestAWatchSurvivesTheTargetRefusingInspect: a front that denies inspect gives
// the watch a reason, once, rather than an empty row.
func TestAWatchSurvivesTheTargetRefusingInspect(t *testing.T) {
	t.Parallel()

	fake := newFake()
	m := started(t, fake)
	fake.mu.Lock()
	fake.denyInspect = true
	fake.mu.Unlock()
	m = typed(m, "watch inputs.version")
	m = send(m, tuitest.Key("esc"), tuitest.Key("s"))

	assert.Equal(t, 1, strings.Count(transcript(m), "watch inputs.version: "))
	assert.Contains(t, m.screen.Watches[0].Err, "workload.debug_inspect")
}

func TestAFrontThatDoesNotInspectHasNoWatch(t *testing.T) {
	t.Parallel()

	var verbs []flowdebug.Verb
	for _, verb := range flowdebug.DriverVerbs() {
		if verb.Name != "inspect" {
			verbs = append(verbs, verb)
		}
	}
	m := started(t, newFake(), func(c *Config) { c.Verbs = verbs })
	_, bound := m.keys.Match("w")
	assert.False(t, bound, "a key for a verb the front does not answer")
	m = typed(m, "watch inputs.version")
	assert.Empty(t, m.screen.Watches)
	assert.Contains(t, m.screen.Toast.Text(), "does not answer inspect")
}

// ---- the completion menu ----

func TestTheCompletionMenuOffersAndAcceptsByKeyboard(t *testing.T) {
	t.Parallel()

	m := started(t, newFake())
	m = send(m, tuitest.Key(":"))
	m = send(m, tuitest.Keys("co")...)
	m = send(m, tuitest.Key("tab"))

	menu, open := m.screen.Console.Menu()
	require.True(t, open, "several offers opened no menu")
	var offered []string
	for _, c := range menu.Candidates {
		offered = append(offered, c.Text)
	}
	assert.Equal(t, []string{"continue", "complete "}, offered)
	assert.Equal(t, "co", m.screen.Console.Text, "the line changed before an offer was chosen")
	assert.Empty(t, m.screen.Busy)

	shown := strings.Join(consoleLines(t, m), "\n")
	assert.Contains(t, shown, "continue")
	assert.Contains(t, shown, "complete")

	// tab and shift+tab move through the offers and wrap.
	m = send(m, tuitest.Key("tab"))
	menu, _ = m.screen.Console.Menu()
	assert.Equal(t, 1, menu.Selected)
	m = send(m, tuitest.Key("tab"))
	menu, _ = m.screen.Console.Menu()
	assert.Zero(t, menu.Selected, "tab did not wrap")
	m = send(m, tuitest.Key("shift+tab"))
	menu, _ = m.screen.Console.Menu()
	assert.Equal(t, 1, menu.Selected, "shift+tab did not wrap")
	assert.Equal(t, "console", m.screen.Focus, "shift+tab left the console while a menu was open")

	// enter accepts: the line is the offer, and nothing was run.
	m = send(m, tuitest.Key("enter"))
	_, open = m.screen.Console.Menu()
	assert.False(t, open)
	assert.Equal(t, "complete ", m.screen.Console.Text)
	assert.Empty(t, m.cfg.Target.(*fakeTarget).resumes)
	assert.NotContains(t, transcript(m), "debug> complete", "accepting an offer ran the line")

	// esc closes the menu first and leaves the console second.
	m.screen.Console.Clear()
	m = send(m, tuitest.Keys("co")...)
	m = send(m, tuitest.Key("tab"))
	m = send(m, tuitest.Key("esc"))
	_, open = m.screen.Console.Menu()
	assert.False(t, open)
	assert.Equal(t, "console", m.screen.Focus)
	assert.Equal(t, "co", m.screen.Console.Text)
	m = send(m, tuitest.Key("esc"))
	assert.NotEqual(t, "console", m.screen.Focus)

	// Typing closes it: the offers were for a word that has changed.
	m = send(m, tuitest.Key(":"))
	m.screen.Console.Clear()
	m = send(m, tuitest.Keys("co")...)
	m = send(m, tuitest.Key("tab"))
	m = send(m, tuitest.Key("n"))
	_, open = m.screen.Console.Menu()
	assert.False(t, open)
	assert.Equal(t, "con", m.screen.Console.Text)
}

func TestTheCompletionMenuAcceptsAClick(t *testing.T) {
	t.Parallel()

	m := started(t, newFake())
	m = send(m, tuitest.Key(":"))
	m = send(m, tuitest.Keys("co")...)
	m = send(m, tuitest.Key("tab"))

	x, y := find(t, m, menuPrefix+"1")
	m = send(m, tuitest.Click(x, y))
	assert.Equal(t, "complete ", m.screen.Console.Text, "a click on the second offer did not take it")
	_, open := m.screen.Console.Menu()
	assert.False(t, open)
	assert.Equal(t, "console", m.screen.Focus)

	// A click on the console's other cells is not an offer.
	m.screen.Console.Clear()
	m = send(m, tuitest.Keys("co")...)
	m = send(m, tuitest.Key("tab"))
	_, y = find(t, m, paneConsole)
	m = send(m, tuitest.Click(0, y))
	_, open = m.screen.Console.Menu()
	assert.True(t, open, "a click outside the offers closed the menu")
	assert.Equal(t, "co", m.screen.Console.Text)
}

func TestTheMenuForANameContinuesTheReference(t *testing.T) {
	t.Parallel()

	m := started(t, newFake())
	m = send(m, tuitest.Key(":"))
	m = typed(m, "inspect inputs.version")
	m = send(m, tuitest.Keys("watch ")...)
	m = send(m, tuitest.Key("tab"))
	menu, open := m.screen.Console.Menu()
	require.True(t, open, "watch offers the names inspect does")
	var offered []string
	for _, c := range menu.Candidates {
		offered = append(offered, c.Text)
	}
	assert.Contains(t, offered, "inputs.")
	assert.Contains(t, offered, "steps.")
	for i, c := range menu.Candidates {
		if c.Text == "steps." {
			require.True(t, m.screen.Console.AcceptMenu(i))
		}
	}
	assert.Equal(t, "watch steps.", m.screen.Console.Text, "a name that continues was followed by a space")

	m.screen.Console.Clear()
	m = send(m, tuitest.Keys("wa")...)
	m = send(m, tuitest.Key("tab"))
	assert.Equal(t, "watch ", m.screen.Console.Text, "the screen's own verbs are not completed")
}

func TestUnwatchCompletesTheWatches(t *testing.T) {
	t.Parallel()

	m := started(t, newFake())
	m = typed(m, "watch inputs.version")
	m = typed(m, "watch inputs.region")
	m = send(m, tuitest.Keys("unwatch inputs.v")...)
	m = send(m, tuitest.Key("tab"))
	assert.Equal(t, "unwatch inputs.version ", m.screen.Console.Text)
}

func TestTheMenuIsBoundedAndNeverHoldsAnUnsafeOffer(t *testing.T) {
	t.Parallel()

	c := NewConsole()
	var offers []flowdebug.Candidate
	for i := range maxMenuCandidates + 20 {
		offers = append(offers, flowdebug.Candidate{Text: fmt.Sprintf("name%03d", i)})
	}
	offers = append(offers,
		flowdebug.Candidate{Text: "bad\x1b[2J"},
		flowdebug.Candidate{Text: strings.Repeat("x", flowdebug.MaxCommandBytes)})
	c.OpenMenu("inspect ", offers, false)

	menu, open := c.Menu()
	require.True(t, open)
	assert.Len(t, menu.Candidates, maxMenuCandidates)
	assert.True(t, menu.Truncated, "the menu cut offers without saying so")
	for _, candidate := range menu.Candidates {
		assert.NotContains(t, candidate.Text, "\x1b")
	}

	// One offer, or none after the unsafe ones are dropped, is not a choice.
	c.OpenMenu("", []flowdebug.Candidate{{Text: "only"}, {Text: "a\nb"}}, false)
	_, open = c.Menu()
	assert.False(t, open)
	assert.False(t, c.AcceptMenu(0))

	// Wrapping with no menu is a no-op, not a panic.
	c.MoveMenu(-1)
	assert.False(t, c.AcceptMenu(-1))
}

func TestTheMenuLeavesWhenFocusDoes(t *testing.T) {
	t.Parallel()

	m := started(t, newFake())
	m = send(m, tuitest.Key(":"))
	m = send(m, tuitest.Keys("co")...)
	m = send(m, tuitest.Key("tab"))
	x, y := find(t, m, panePrefix+paneScope)
	m = send(m, tuitest.Click(x, y))
	_, open := m.screen.Console.Menu()
	assert.False(t, open, "a menu stayed open in a console nobody is typing in")
}

// ---- the screen ----

func goldenScreen(t *testing.T, st Style, size tui.Size, build func(*testing.T) Model) {
	t.Helper()

	m := build(t)
	m = send(m, tuitest.Resize(size))
	m.cfg.Style = st
	text, _ := m.screen.Draw(st)
	tuitest.Golden(t, text)
	tuitest.Fits(t, text, size)
}

func withWatchesAndAResult(t *testing.T) Model {
	fake := watchTarget().withList(30)
	m := started(t, fake)
	m = typed(m, "watch stop")
	m = typed(m, "watch inputs.region")
	m = typed(m, "watch nosuch.name")
	m = typed(m, "inspect steps.list")
	m = send(m, tuitest.Key("esc"), tuitest.Key("tab"), tuitest.Key("enter"))
	m.screen.Tree.Select(resultPrefix + "steps.list")

	return m
}

func withTheMenuOpen(t *testing.T) Model {
	m := started(t, newFake())
	m = send(m, tuitest.Key(":"))
	m = send(m, tuitest.Keys("co")...)
	m = send(m, tuitest.Key("tab"))
	m = send(m, tuitest.Key("tab"))

	return m
}

func TestTheScopeWithWatchesAndAResultGolden(t *testing.T) {
	for _, v := range styles {
		for _, size := range []tui.Size{{W: 100, H: 30}, {W: 120, H: 36}} {
			t.Run(v.name+"/"+size.String(), func(t *testing.T) {
				goldenScreen(t, v.style, size, withWatchesAndAResult)
			})
		}
	}
}

func TestTheConsoleWithTheMenuOpenGolden(t *testing.T) {
	for _, v := range styles {
		for _, size := range []tui.Size{{W: 80, H: 24}, {W: 120, H: 36}} {
			t.Run(v.name+"/"+size.String(), func(t *testing.T) {
				goldenScreen(t, v.style, size, withTheMenuOpen)
			})
		}
	}
}

// ---- the contract with the target ----

// TestAResultIsOfTheStopItWasAskedAt: the run moving replaces the inspection's
// values, which would otherwise be drawn as the new stop's.
func TestAResultIsOfTheStopItWasAskedAt(t *testing.T) {
	t.Parallel()

	m := started(t, newFake())
	m = typed(m, "inspect inputs.version")
	_, ok := m.screen.Tree.Node(resultPrefix + "inputs.version")
	require.True(t, ok)

	m = send(m, tuitest.Key("esc"), tuitest.Key("s"))
	_, ok = m.screen.Tree.Node(resultPrefix + "inputs.version")
	assert.False(t, ok, "an inspection of an earlier stop outlived it")
	assert.Nil(t, m.screen.Result)
}

// TestAnErroredInspectIsNotATree: the transcript has the target's words and the
// scope pane gains no row for a value that does not exist.
func TestAnErroredInspectIsNotATree(t *testing.T) {
	t.Parallel()

	m := started(t, newFake())
	m = typed(m, "inspect nosuch.name")
	assert.Contains(t, transcript(m), "no such name")
	assert.Nil(t, m.screen.Result)
	_, ok := m.screen.Tree.Node(groupResult)
	assert.False(t, ok)
}

// TestAWatchedValueOpensWithoutTouchingTheScopeRowOfTheSameName: the watch, the
// inspection and the scope row of `steps.list` are three rows with three open
// states, and the children one of them is given are not given to the others.
func TestAWatchedValueOpensWithoutTouchingTheScopeRowOfTheSameName(t *testing.T) {
	t.Parallel()

	fake := newFake().withList(150)
	m := started(t, fake)
	m = typed(m, "watch steps.list")
	m = typed(m, "inspect steps.list")
	m = send(m, tuitest.Key("esc"), tuitest.Key("tab"))

	m.screen.Tree.Select(watchPrefix + "steps.list")
	m = send(m, tuitest.Key("enter"))

	watch, ok := m.screen.Tree.Node(watchPrefix + "steps.list")
	require.True(t, ok)
	require.Len(t, watch.Children, flowdebug.DefaultInspectLimit)
	assert.True(t, strings.HasPrefix(watch.Children[0].ID, watchPrefix+"steps.list"+idSep), "a child of a watch has the watch's id as its root")
	result, _ := m.screen.Tree.Node(resultPrefix + "steps.list")
	assert.Empty(t, result.Children, "the page was given to the inspection of the same name")
	scope, _ := m.screen.Tree.Node("steps.list")
	assert.Empty(t, scope.Children, "the page was given to the scope row of the same name")

	// A child row is a name a person can inspect or watch: its expression, not its id.
	m.screen.Tree.Select(watch.Children[3].ID)
	assert.Equal(t, "steps.list.[3]", SelectedExpression(m.screen.Tree))
}

// TestAWatchNeverShowsAnEarlierStopsAnswer: when the run let go of the hold, a
// watch that had been failing says "(not held)", not the old error; and when
// the run moved under a read, the watch is pending, not the previous stop's
// value beside the new stop.
func TestAWatchNeverShowsAnEarlierStopsAnswer(t *testing.T) {
	t.Parallel()

	m := started(t, newFake())
	m.screen.Watches = []Watch{
		{Expr: "failing", Err: "undefined here", Said: true},
		{Expr: "moved", Value: &v1.DebugValue{Rendered: "7"}},
	}

	m.applyWatches([]watchResult{{expr: "failing", outcome: outcomeNotHeld}})
	assert.Equal(t, "(not held)", m.screen.Watches[0].text())
	assert.False(t, m.screen.Watches[0].Said, "a hold that ended resets what the console said")

	m.applyWatches([]watchResult{{expr: "moved", outcome: outcomeSkipped}})
	assert.Nil(t, m.screen.Watches[1].Value, "the value of the stop before the move was kept")
	assert.Equal(t, "(reading)", m.screen.Watches[1].text())

	// A failing watch the run moved under stays unsaid-twice: Said survives.
	m.screen.Watches[0] = Watch{Expr: "failing", Err: "undefined here", Said: true}
	m.applyWatches([]watchResult{{expr: "failing", outcome: outcomeSkipped}})
	assert.True(t, m.screen.Watches[0].Said)
	assert.Equal(t, "(reading)", m.screen.Watches[0].text())
}
