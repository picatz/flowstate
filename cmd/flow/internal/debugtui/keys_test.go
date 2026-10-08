package debugtui

import (
	"slices"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/picatz/flowstate/cmd/flow/internal/pane"
	"github.com/picatz/flowstate/cmd/flow/internal/tui"
	"github.com/picatz/flowstate/cmd/flow/internal/tui/tuitest"
	"github.com/picatz/flowstate/pkg/flowstate/v1/flowdebug"
)

// TestKeymapIsTheCommandTable: every key that runs a verb runs one the driver
// front answers, and every verb the table gives that front is either bound or
// named as left to the console. A verb added to the table without a decision
// fails here; a stale name in either list does too.
func TestKeymapIsTheCommandTable(t *testing.T) {
	t.Parallel()

	verbs := flowdebug.DriverVerbs()
	keys, err := NewKeymap(verbs)
	require.NoError(t, err)

	offered := map[string]bool{}
	for _, verb := range verbs {
		offered[verb.Name] = true
	}

	bound := map[string]bool{}
	for _, binding := range keys.Bindings() {
		if verb, ok := strings.CutPrefix(binding.Name, verbBindPrefix); ok {
			assert.True(t, offered[verb], "%v is bound to %q, which the driver front does not answer", binding.Keys, verb)
			bound[verb] = true
		}
	}
	for _, key := range verbKeys {
		assert.True(t, offered[key.verb], "verbKeys names %q, which is not in the table for this front", key.verb)
	}
	for _, name := range consoleOnly {
		assert.True(t, offered[name], "consoleOnly names %q, which is not in the table for this front", name)
		assert.False(t, bound[name], "%q is both bound and console-only", name)
	}
	for _, verb := range verbs {
		assert.True(t, bound[verb.Name] || slices.Contains(consoleOnly, verb.Name),
			"the table's %q has no key and is not named as console-only", verb.Name)
	}

	// Every movement the table lists as one that rewinds or resumes is reachable by a key
	// or by name in the console, never silently absent.
	for _, verb := range verbs {
		if verb.Moves {
			assert.True(t, bound[verb.Name] || verb.Name == "until" || verb.Name == "detach", "%s moves the run and has no key", verb.Name)
		}
	}
}

func TestAVerbAFrontDoesNotOfferHasNoKey(t *testing.T) {
	t.Parallel()

	keys, err := NewKeymap([]flowdebug.Verb{{Name: "step"}, {Name: "continue"}})
	require.NoError(t, err)

	for _, key := range []string{"s", "space", "c"} {
		_, ok := keys.Match(key)
		assert.True(t, ok, key)
	}
	for _, key := range []string{"n", "f", "b", "r", "p", "q", "i"} {
		binding, ok := keys.Match(key)
		assert.False(t, ok, "%q is bound to %s on a front that offers neither", key, binding.Name)
	}

	// The keys the screen owns are there whatever the front offers.
	for _, key := range []string{"tab", "?", ":", "ctrl+c", "ctrl+d", "enter"} {
		_, ok := keys.Match(key)
		assert.True(t, ok, key)
	}
}

func TestEveryKeyIsBoundOnce(t *testing.T) {
	t.Parallel()

	_, err := tui.NewKeymap(tui.Binding{Name: "a", Keys: []string{"x"}}, tui.Binding{Name: "b", Keys: []string{"y", "x"}})
	require.ErrorContains(t, err, `"x" is bound to both a and b`)

	_, err = NewKeymap(flowdebug.DriverVerbs())
	require.NoError(t, err)
}

func TestTheHintBarShowsKeysThatDoSomething(t *testing.T) {
	t.Parallel()

	m := started(t, newFake())
	text := view(m)
	bar := lines(text)[len(lines(text))-1]
	for _, want := range []string{"s step", "n next", "c continue", "? help", "q quit"} {
		assert.Contains(t, bar, want)
	}
	for _, hint := range m.keys.Bindings() {
		if hint.Hint {
			for _, key := range hint.Keys[:1] {
				_, ok := m.keys.Match(key)
				assert.True(t, ok, "the hint bar shows %q, which does nothing", key)
			}
		}
	}

	// On a narrow bar the hints that fit are shown and the line is not cut mid-key.
	narrow := tuitest.Fold(modelFor(t, newFake(), func(c *Config) { c.Size = tui.Size{W: 60, H: 16} }))
	assert.LessOrEqual(t, len(strings.TrimSpace(lines(view(narrow.(Model)))[15])), 60)
}

// ---- console ----

func TestTheConsoleEditsALineAndRemembersWhatWasRun(t *testing.T) {
	t.Parallel()

	c := NewConsole()
	c.Insert("inspect x\x1b[31m\n")
	assert.Equal(t, "inspect x[31m", c.Text, "control bytes were typed into the line")

	c.Clear()
	c.Insert("break build if x")
	c.DeleteWord()
	assert.Equal(t, "break build if ", c.Text)
	c.Backspace()
	assert.Equal(t, "break build if", c.Text)

	c.Clear()
	c.Insert("héllo")
	c.Backspace()
	assert.Equal(t, "héll", c.Text, "backspace cut a multibyte rune in half")

	c.Clear()
	for _, line := range []string{"step", "next", "next", "continue"} {
		c.Insert(line)
		assert.Equal(t, line, c.Submit())
	}
	assert.Empty(t, c.Text)
	assert.Equal(t, "", c.Submit(), "an empty line is not a command")

	c.Older()
	assert.Equal(t, "continue", c.Text)
	c.Older()
	assert.Equal(t, "next", c.Text, "a repeated line is remembered once")
	c.Older()
	c.Older()
	c.Older()
	assert.Equal(t, "step", c.Text)
	c.Newer()
	c.Newer()
	assert.Equal(t, "continue", c.Text)
	c.Newer()
	assert.Empty(t, c.Text, "down past the newest is an empty line")
}

func TestTheConsoleIsBounded(t *testing.T) {
	t.Parallel()

	c := NewConsole()
	c.Insert(strings.Repeat("x", flowdebug.MaxCommandBytes))
	c.Insert("y")
	assert.Len(t, c.Text, flowdebug.MaxCommandBytes, "a line longer than a command may be was accepted")

	for i := range maxHistory + 10 {
		c.Clear()
		c.Insert(strings.Repeat("a", i+1))
		c.Submit()
	}
	assert.Len(t, c.history, maxHistory)

	for range maxTranscriptLines + 50 {
		c.Say(strings.Repeat("z", maxLineRunes*3))
	}
	assert.Len(t, c.Lines(), maxTranscriptLines)
	assert.LessOrEqual(t, len([]rune(c.Lines()[0])), maxLineRunes+1)
}

func TestTheTranscriptShowsAnAnswerAsDataAndNotAsControl(t *testing.T) {
	t.Parallel()

	c := NewConsole()
	c.Say("ok \x1b]0;pwned\x07 done\nsecond\tline")
	text := strings.Join(c.Lines(), "\n")
	assert.NotContains(t, text, "\x1b")
	assert.NotContains(t, text, "\x07")
	assert.Contains(t, text, `\x1b]0;pwned\a`)
	assert.Len(t, c.Lines(), 2)

	// And through the whole screen, from a target that answers with it.
	fake := newFake()
	fake.groups[0].names[0].rendered = "\x1b[2J\x1b[31mgone"
	fake.groups[0].names[0].name = "bad\x1bname"
	s := screenOf(t, fake, true, tui.Size{W: 120, H: 36})
	s.Tree.Toggle("g:inputs")
	s.Tree.Select("inputs.bad\x1bname")
	s.Frame.Snapshot.Session.Run.WorkflowId = "wf\x1b[1m"
	out, _ := s.Draw(plain)
	assert.NotContains(t, out, "\x1b", "a remote's string reached the terminal as control")
	assert.Contains(t, out, `\x1b[2J`)
}

func TestScopeNodesAreTheFramesScope(t *testing.T) {
	t.Parallel()

	frame := frameOf(t, newFake(), true)
	nodes := ScopeNodes(frame)
	require.Len(t, nodes, 3)
	assert.Equal(t, "g:inputs", nodes[0].ID)
	assert.Equal(t, "{2}", nodes[0].Value)
	require.Len(t, nodes[0].Children, 2)
	assert.Equal(t, "inputs.version", nodes[0].Children[0].ID, "a row is named by the expression that reads it")
	assert.Equal(t, 2, nodes[1].Children[0].Total, "a value with children is a branch")

	assert.Empty(t, ScopeNodes(flowdebug.Frame{}), "no scope is no nodes")

	tree := pane.NewTree(nodes)
	assert.Equal(t, "", SelectedExpression(nil))
	assert.Equal(t, "", SelectedExpression(tree), "a group is not an expression")
	tree.Toggle("g:inputs")
	tree.Select("inputs.region")
	assert.Equal(t, "inputs.region", SelectedExpression(tree))
}

// TestAHostileNameIsNotPutOnTheInputLine: the scope tree's names are the
// target's text. `i` copies the selected one into the console, so one with a
// control character is refused, and the console draws what it holds escaped.
func TestAHostileNameIsNotPutOnTheInputLine(t *testing.T) {
	t.Parallel()

	const hostile = "steps.x[\"\x1b[2J\x1b]0;owned\x07\"]"

	m := started(t, newFake())
	m.screen.Tree = pane.NewTree([]pane.Node{{ID: "g:steps", Label: "steps", Children: []pane.Node{{ID: hostile, Label: "x"}}}})
	m.screen.Tree.Toggle("g:steps")
	m.screen.Tree.Select(hostile)
	require.Equal(t, hostile, SelectedExpression(m.screen.Tree), "the fixture did not select the hostile name")

	m = send(m, tuitest.Key("i"))
	assert.Empty(t, m.screen.Console.Text, "a name with a control character reached the input line")
	assert.Contains(t, m.screen.Toast.Text(), "control character")

	// Whatever else puts text there, the draw escapes it.
	c := Console{Text: "inspect " + hostile}
	drawn := ConsoleView(c, "", pane.Options{Width: 80, Height: 4})
	assert.NotContains(t, drawn, "\x1b")
	assert.NotContains(t, drawn, "\x07")
	assert.Contains(t, drawn, "steps.x")

	// A typed or pasted control character is dropped, C1 included.
	c = Console{}
	c.Insert("a\u009bb\x1bc")
	assert.Equal(t, "abc", c.Text)
}

// TestOneReadRunsAtATimeHoweverFastTheRunMoves: revisions that arrive while a
// read is in flight cost one more read afterwards, not one each.
func TestOneReadRunsAtATimeHoweverFastTheRunMoves(t *testing.T) {
	t.Parallel()

	m := modelFor(t, newFake()) // its first read is in flight
	require.True(t, m.reading)

	for range 50 {
		assert.Nil(t, m.reread(), "a read was started while another was in flight")
	}
	require.True(t, m.dirty)

	seq := m.readSeq
	next, cmd := m.framed(frameMsg{seq: seq, frame: frameOf(t, newFake(), true)})
	m = next.(Model)
	assert.NotNil(t, cmd, "the revisions that arrived meanwhile were forgotten")
	assert.Greater(t, m.readSeq, seq)
	assert.True(t, m.reading)
	assert.False(t, m.dirty)

	// With nothing arriving meanwhile, a landed read starts nothing.
	next, _ = m.framed(frameMsg{seq: m.readSeq, frame: frameOf(t, newFake(), true)})
	m = next.(Model)
	assert.False(t, m.reading)
	seq = m.readSeq
	_, cmd = m.framed(frameMsg{seq: seq, frame: frameOf(t, newFake(), true)})
	assert.Equal(t, seq, m.readSeq)
	_ = cmd
}

// TestACandidateWithAControlCharacterIsNotPutOnTheLine: completions are the
// target's names, and Enter would submit what is on the line.
func TestACandidateWithAControlCharacterIsNotPutOnTheLine(t *testing.T) {
	t.Parallel()

	m := started(t, newFake())
	m.screen.Console.Text = "inspect st"
	next, _ := m.completed(completeMsg{line: "inspect st", answer: flowdebug.Completion{Prefix: "st", Candidates: []flowdebug.Candidate{{Text: "steps\x1b[2J"}}}})
	assert.Equal(t, "inspect st", next.(Model).screen.Console.Text)

	m.screen.Console.Text = "inspect st"
	next, _ = m.completed(completeMsg{line: "inspect st", answer: flowdebug.Completion{Prefix: "st", Candidates: []flowdebug.Candidate{{Text: "steps"}}}})
	assert.Equal(t, "inspect steps ", next.(Model).screen.Console.Text, "an ordinary candidate was refused")
}

// TestKeysDoNothingWhereOnlyTheTooSmallMessageShows: a key that acted would act
// on a screen nobody can see.
func TestKeysDoNothingWhereOnlyTheTooSmallMessageShows(t *testing.T) {
	t.Parallel()

	m := started(t, newFake(), func(c *Config) { c.Size = tui.Size{W: 40, H: 8} })
	before := m.screen.Console.Text
	for _, k := range []string{"c", "n", "q", "i", ":", "?"} {
		next, cmd := m.key(tuitest.Key(k))
		assert.Nil(t, cmd, k)
		assert.Equal(t, before, next.(Model).screen.Console.Text, k)
		assert.False(t, next.(Model).screen.Help, k)
	}

	_, cmd := m.key(tuitest.Key("ctrl+c"))
	assert.NotNil(t, cmd, "ctrl+c must still leave a screen that cannot be drawn")
}

// TestAPageOfAnEarlierStopIsNotAppended: the run moved between asking for a
// page and its arrival.
func TestAPageOfAnEarlierStopIsNotAppended(t *testing.T) {
	t.Parallel()

	m := started(t, newFake())
	m.frameRev = 9
	parent := "g:inputs"
	next, _ := m.paged(pageMsg{rev: 8, req: pane.Request{Parent: parent, Offset: 2}, nodes: []pane.Node{{ID: "inputs.stale"}}, total: 3})
	for _, row := range next.(Model).screen.Tree.Rows() {
		assert.NotEqual(t, "inputs.stale", row.ID)
	}
}
