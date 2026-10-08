package debugtui

import (
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"unicode"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/picatz/flowstate/cmd/flow/internal/pane"
	"github.com/picatz/flowstate/cmd/flow/internal/tui"
	"github.com/picatz/flowstate/cmd/flow/internal/tui/tuitest"
	"github.com/picatz/flowstate/cmd/flow/internal/ui"
	v1 "github.com/picatz/flowstate/pkg/flowstate/v1"
	"github.com/picatz/flowstate/pkg/flowstate/v1/flowdebug"
	"github.com/picatz/flowstate/pkg/flowstate/v1/flowfile"
)

// ---- a program written in two files, and the map the compiler makes of it ----

const (
	mainText = `edition: v2026.4
name: main
# fetch, charge, then hand over to the child
steps:
  - id: fetch
    log:
      message: fetching
  - id: charge
    log:
      message: ${"charging " + string(1)}
  - id: nested
    call: ./child.yaml
`
	childText = `edition: v2026.4
name: child
steps:
  - id: greet
    log:
      message: hi
`
)

// sourceFixture is a program compiled from files on disk, with the source map
// the CLI builds of it and the documents it hands the screen.
type sourceFixture struct {
	root, child string
	workflow    *v1.Workflow
	sm          *v1.DebugSourceMap
	docs        []Document
}

func newSourceFixture(t *testing.T) sourceFixture {
	t.Helper()

	dir := t.TempDir()
	fx := sourceFixture{root: filepath.Join(dir, "main.yaml"), child: filepath.Join(dir, "child.yaml")}
	require.NoError(t, os.WriteFile(fx.root, []byte(mainText), 0o600))
	require.NoError(t, os.WriteFile(fx.child, []byte(childText), 0o600))

	var positions *flowfile.Positions
	var err error
	fx.workflow, positions, err = flowfile.ParseFile(fx.root)
	require.NoError(t, err)
	fx.sm = flowfile.SourceMap(fx.root, []byte(mainText), fx.workflow, positions)
	require.Len(t, fx.sm.GetDocuments(), 2)
	fx.docs = []Document{{URI: fx.root, Text: []byte(mainText)}, {URI: fx.child, Text: []byte(childText)}}

	return fx
}

// lineOf is the 1-based line of text that holds needle.
func lineOf(t *testing.T, text, needle string) int {
	t.Helper()

	for i, line := range strings.Split(text, "\n") {
		if strings.Contains(line, needle) {
			return i + 1
		}
	}
	require.Failf(t, "no such line", "%q is not in the text", needle)

	return 0
}

// heldAt makes the run held before the step at path of workflow.
func heldAt(f *fakeTarget, workflow string, path ...string) {
	f.occurrence = &v1.DebugOccurrence{
		Address: strings.Join(path, "/"),
		Site:    &v1.DebugSite{Workflow: workflow, Path: path, Kind: "log"},
	}
}

// sourceModel is a screen over a run of the fixture's program held at charge.
func sourceModel(t *testing.T, fx sourceFixture, f *fakeTarget, mods ...func(*Config)) Model {
	t.Helper()

	f.program = []string{"fetch", "charge", "nested", "d", "e", "f"}
	f.at = 1
	f.irDigest = fx.sm.GetIrDigest()
	heldAt(f, "main", "charge")

	return started(t, f, append([]func(*Config){func(c *Config) {
		c.Frame.Program, c.Frame.SourceMap, c.Documents = fx.workflow, fx.sm, fx.docs
	}}, mods...)...)
}

// sourceText is the source pane as drawn, one string.
func sourceText(t *testing.T, m Model) string { return strings.Join(paneLines(t, m, paneSource), "\n") }

// ---- following the held frame ----

func TestTheSourcePaneFollowsTheHeldFrame(t *testing.T) {
	t.Parallel()

	fx := newSourceFixture(t)
	f := newFake()
	m := sourceModel(t, fx, f)

	charge := lineOf(t, mainText, "id: charge")
	text := sourceText(t, m)
	assert.Contains(t, text, fmt.Sprintf("main.yaml:%d", charge), "the heading names the held line")
	assert.Contains(t, text, "name: main")
	var marked []string
	for _, l := range strings.Split(text, "\n") {
		if strings.HasPrefix(l, "▶") {
			marked = append(marked, l)
		}
	}
	require.Len(t, marked, 1, "exactly one line is the current one")
	assert.Contains(t, marked[0], "id: charge")

	// The step's whole range is marked: its lines after the first carry the rail.
	rails := 0
	for _, l := range strings.Split(text, "\n") {
		if strings.HasPrefix(l, "│") {
			rails++
		}
	}
	assert.Equal(t, lineOf(t, mainText, "message: ${")-charge, rails, "the lines of the held step are marked as one range")

	// The run steps into the called file: the pane shows that document and says which.
	heldAt(f, "child", "greet")
	f.advance()
	m = send(m, frameFrom(t, m, f))
	text = sourceText(t, m)
	assert.Contains(t, text, fmt.Sprintf("child.yaml:%d", lineOf(t, childText, "id: greet")), "the heading names the callee's file")
	assert.Contains(t, text, "name: child")
	assert.NotContains(t, text, "name: main")
	for _, l := range strings.Split(text, "\n") {
		if strings.HasPrefix(l, "▶") {
			assert.Contains(t, l, "id: greet")
		}
	}

	// And back out again.
	heldAt(f, "main", "nested")
	f.advance()
	m = send(m, frameFrom(t, m, f))
	text = sourceText(t, m)
	assert.Contains(t, text, "name: main")
	assert.Contains(t, text, fmt.Sprintf("main.yaml:%d", lineOf(t, mainText, "id: nested")))
}

func TestACalleeTheMapDoesNotNameSaysWhereTheRunIs(t *testing.T) {
	t.Parallel()

	fx := newSourceFixture(t)
	f := newFake()
	m := sourceModel(t, fx, f, func(c *Config) { c.Size = tui.Size{W: 200, H: 40} })

	heldAt(f, "elsewhere", "step")
	f.advance()
	m = send(m, frameFrom(t, m, f))
	text := sourceText(t, m)
	assert.Contains(t, text, "name: main", "the last document stays up")
	assert.Contains(t, text, "elsewhere:step", "it says which workflow the run is in")
	assert.NotContains(t, text, "▶", "no line is marked as the current one")
}

// ---- gutter, clicks and keys ----

func TestAGutterClickSetsALineBreakpoint(t *testing.T) {
	t.Parallel()

	fx := newSourceFixture(t)
	f := newFake()
	var recorded []string
	m := sourceModel(t, fx, f, func(c *Config) { c.Accepted = func(line string) { recorded = append(recorded, line) } })

	fetch := lineOf(t, mainText, "id: fetch")
	x, y := find(t, m, gutterPrefix+strconv.Itoa(fetch))
	m = send(m, tuitest.Click(x, y))

	require.Len(t, f.replaced, 1, "the click did not send a breakpoint set")
	set := f.replaced[0].GetBreakpoints()
	require.Len(t, set, 1)
	assert.Equal(t, fx.root, set[0].GetLine().GetUri(), "the line is named by the map's own document")
	assert.Equal(t, uint32(fetch), set[0].GetLine().GetLine())
	assert.Empty(t, set[0].GetStep(), "a line breakpoint names a line, not a step")
	assert.Equal(t, flowdebug.LineBreakpointID(fx.root, uint32(fetch)), set[0].GetId())
	assert.Contains(t, strings.Join(m.screen.Console.Lines(), "\n"), Prompt+fmt.Sprintf("break main.yaml:%d", fetch))
	assert.Equal(t, paneSource, m.screen.Focus)

	// The armed line carries the mark.
	armed := ""
	for _, l := range paneLines(t, m, paneSource) {
		if strings.Contains(l, "id: fetch") {
			armed = l
		}
	}
	assert.Contains(t, armed, "•")

	// A second click on the same gutter takes it off again: the set goes empty.
	x, y = find(t, m, gutterPrefix+strconv.Itoa(fetch))
	m = send(m, tuitest.Click(x, y))
	require.Len(t, f.replaced, 2)
	assert.Empty(t, f.replaced[1].GetBreakpoints())
	assert.Contains(t, strings.Join(m.screen.Console.Lines(), "\n"), Prompt+"delete "+flowdebug.LineBreakpointID(fx.root, uint32(fetch)))
	for _, l := range paneLines(t, m, paneSource) {
		assert.NotContains(t, l, "•")
	}

	// Neither has a spelling a script could replay, so neither is recorded.
	assert.Empty(t, recorded)
	m = send(m, tuitest.Key("s"))
	assert.Equal(t, []string{"step"}, recorded, "a line that is a command still is")
}

func TestTheBKeyArmsTheSelectedLineOfAFocusedSource(t *testing.T) {
	t.Parallel()

	fx := newSourceFixture(t)
	f := newFake()
	m := sourceModel(t, fx, f)

	m = send(m, tuitest.Click(find(t, m, sourcePrefix+strconv.Itoa(lineOf(t, mainText, "id: nested")))))
	require.Empty(t, f.replaced, "a click on the text only selects")
	assert.Equal(t, lineOf(t, mainText, "id: nested"), m.screen.Source.Selected)
	assert.Equal(t, paneSource, m.screen.Focus)

	m = send(m, tuitest.Key("B"))
	require.Len(t, f.replaced, 1)
	assert.Equal(t, uint32(lineOf(t, mainText, "id: nested")), f.replaced[0].GetBreakpoints()[0].GetLine().GetLine())
	_ = m
}

func TestALineClickWithoutSourceBreakpointsToasts(t *testing.T) {
	t.Parallel()

	fx := newSourceFixture(t)

	t.Run("a front that does not answer break", func(t *testing.T) {
		t.Parallel()

		f := newFake()
		var verbs []flowdebug.Verb
		for _, v := range flowdebug.DriverVerbs() {
			if v.Name != "break" {
				verbs = append(verbs, v)
			}
		}
		m := sourceModel(t, fx, f, func(c *Config) { c.Verbs = verbs })
		before := strings.Join(m.screen.Console.Lines(), "\n")

		m = send(m, tuitest.Click(find(t, m, gutterPrefix+strconv.Itoa(lineOf(t, mainText, "id: fetch")))))
		assert.Contains(t, m.screen.Toast.Text(), "does not answer break")
		assert.Empty(t, f.replaced, "a breakpoint was sent to a front that cannot take it")
		assert.Equal(t, before, strings.Join(m.screen.Console.Lines(), "\n"), "something was sent to the console")
	})

	t.Run("a line no step is written on", func(t *testing.T) {
		t.Parallel()

		f := newFake()
		m := sourceModel(t, fx, f)
		m = send(m, tuitest.Click(find(t, m, gutterPrefix+"1")))
		assert.Contains(t, m.screen.Toast.Text(), "no step is written on line 1")
		assert.Empty(t, f.replaced)
	})

	t.Run("a source whose map is not verified", func(t *testing.T) {
		t.Parallel()

		f := newFake()
		m := sourceModel(t, fx, f, func(c *Config) { c.Frame.SourceMap = nil })
		m = send(m, tuitest.Key("tab"))
		require.Equal(t, paneSource, m.screen.Focus)

		m = send(m, tuitest.Key("B"))
		assert.Contains(t, m.screen.Toast.Text(), "digest mismatch")
		assert.Empty(t, f.replaced)
	})
}

func TestAClickOutsideAnySourceLineIsIgnored(t *testing.T) {
	t.Parallel()

	fx := newSourceFixture(t)
	f := newFake()
	m := sourceModel(t, fx, f)
	before := m.screen.Source.Selected

	// Under the last line of the document the pane is empty: there is no line there.
	rect, ok := m.cell(paneSource)
	require.True(t, ok)
	m = send(m, tuitest.Click(rect.X+rect.W/2, rect.Y+rect.H-1))
	assert.Equal(t, before, m.screen.Source.Selected)
	assert.Empty(t, f.replaced)

	// And off every pane at all.
	m = send(m, tuitest.Click(0, 0))
	assert.Equal(t, before, m.screen.Source.Selected)
	assert.Empty(t, f.replaced)
}

// ---- addresses, not lines ----

func TestAnUnverifiedMapShowsAddressesNotLines(t *testing.T) {
	t.Parallel()

	fx := newSourceFixture(t)

	check := func(t *testing.T, m Model, why string, numbers ...bool) {
		t.Helper()

		text := sourceText(t, m)
		assert.Contains(t, text, "held at charge", "the step's address stands in for its line")
		assert.Contains(t, strings.Join(strings.Fields(text), " "), why)
		assert.Contains(t, text, "addresses only")
		for _, line := range strings.Split(text, "\n") {
			if len(numbers) == 0 { // the wrapped sentence may start a row with the size it names
				assert.NotRegexp(t, `^\s*[▶│]?\s*\d+ `, line, "a line number was drawn: %q", line)
			}
		}
		assert.NotContains(t, text, "id: charge", "text of the file was drawn")
		assert.NotContains(t, text, "edition")
		_, hits := m.screen.Draw(plain)
		for y := range m.screen.Size.H {
			for x := range m.screen.Size.W {
				if hit, ok := hits.At(x, y); ok {
					assert.False(t, strings.HasPrefix(hit.ID, sourcePrefix) || strings.HasPrefix(hit.ID, gutterPrefix), "a line was clickable at %d,%d", x, y)
				}
			}
		}
	}

	t.Run("the run reports another program", func(t *testing.T) {
		t.Parallel()

		f := newFake()
		m := sourceModel(t, fx, f, func(c *Config) { c.Frame.Program = nil })
		f.irDigest = "sha256:somebody-elses"
		m = send(m, m.reread()())
		check(t, m, "does not match the program this run executes (digest mismatch)")
		assert.Nil(t, m.screen.Frame.SourceMap, "the map reached the screen")
	})

	t.Run("the caller offered a file and no map", func(t *testing.T) {
		t.Parallel()

		m := sourceModel(t, fx, newFake(), func(c *Config) { c.Frame.SourceMap = nil })
		check(t, m, "main.yaml does not match the program this run executes")
	})

	t.Run("no file was offered at all", func(t *testing.T) {
		t.Parallel()

		m := sourceModel(t, fx, newFake(), func(c *Config) { c.Frame.SourceMap, c.Documents = nil, nil })
		check(t, m, "pass --program")
	})

	t.Run("a file whose bytes are not the ones the map was made from", func(t *testing.T) {
		t.Parallel()

		moved := []Document{{URI: fx.root, Text: []byte("\n" + mainText)}, fx.docs[1]}
		m := sourceModel(t, fx, newFake(), func(c *Config) { c.Documents = moved })
		check(t, m, "main.yaml is not the bytes its source map was made from")
	})

	t.Run("a document the screen was not given", func(t *testing.T) {
		t.Parallel()

		m := sourceModel(t, fx, newFake(), func(c *Config) { c.Documents = fx.docs[1:] })
		check(t, m, "the text of main.yaml was not given to the screen")
	})

	t.Run("a document past the byte bound", func(t *testing.T) {
		t.Parallel()

		big := []Document{{URI: fx.root, Text: []byte(strings.Repeat("x", MaxSourceBytes+1))}, fx.docs[1]}
		m := sourceModel(t, fx, newFake(), func(c *Config) { c.Documents = big })
		check(t, m, fmt.Sprintf("is %d bytes, past the %d this pane reads", MaxSourceBytes+1, MaxSourceBytes), false)
	})
}

func TestAnAddressThatIsWithheldOrHasControlCharactersIsDrawnSafely(t *testing.T) {
	t.Parallel()

	fx := newSourceFixture(t)
	f := newFake()
	m := sourceModel(t, fx, f, func(c *Config) { c.Frame.SourceMap = nil })
	f.occurrence = &v1.DebugOccurrence{Address: "ste\x1b[31mp\x07", Site: &v1.DebugSite{Workflow: "main", Path: []string{"x"}}}
	f.advance()
	m = send(m, frameFrom(t, m, f))

	text := sourceText(t, m)
	assert.Contains(t, text, `ste\x1b[31mp\a`)
	assert.NotContains(t, text, "\x1b")
	assert.NotContains(t, text, "\x07")
}

// ---- scrolling ----

// longFixture is a hand-made map over a document of n lines, a step on every
// third, so a view can be scrolled and a held line can be far from the top.
func longFixture(n int) (*v1.DebugSourceMap, []Document) {
	var text strings.Builder
	sm := &v1.DebugSourceMap{IrDigest: "sha256:long"}
	for line := 1; line <= n; line++ {
		fmt.Fprintf(&text, "line %03d of the document\n", line)
		if line%3 == 1 {
			sm.Entries = append(sm.Entries, &v1.DebugSourceEntry{
				Site: &v1.DebugSite{Workflow: "long", Path: []string{fmt.Sprintf("s%03d", line)}},
				Location: &v1.DebugSourceLocation{Range: &v1.SourceRange{
					StartLine: uint32(line), StartColumn: 1, EndLine: uint32(line), EndColumn: 10,
				}},
			})
		}
	}
	sm.Documents = []*v1.DebugSourceDocument{{Uri: "/work/long.yaml", Digest: v1.ContentDigest([]byte(text.String())), Language: "flowfile"}}

	return sm, []Document{{URI: "/work/long.yaml", Text: []byte(text.String())}}
}

func longModel(t *testing.T, f *fakeTarget, n int) (Model, *v1.DebugSourceMap) {
	t.Helper()

	sm, docs := longFixture(n)
	f.irDigest = sm.GetIrDigest()
	f.program = []string{"s001", "s004", "s007", "s100", "s250"}
	f.at = 0
	heldAt(f, "long", "s001")
	m := started(t, f, func(c *Config) {
		c.Frame.SourceMap, c.Documents, c.Frame.Inventory = sm, docs, nil
		c.Size = tui.Size{W: 120, H: 36}
	})

	return m, sm
}

func TestTheHeldLineIsKeptInViewUntilTheViewIsScrolled(t *testing.T) {
	t.Parallel()

	f := newFake()
	m, _ := longModel(t, f, 300)
	require.Contains(t, sourceText(t, m), "line 001 of the document")

	// The held line moves a long way and the pane follows it.
	move := func(m Model, step string) Model {
		heldAt(f, "long", step)
		f.advance()

		return send(m, frameFrom(t, m, f))
	}
	m = move(m, "s250")
	assert.Contains(t, sourceText(t, m), "line 250 of the document")
	assert.Contains(t, sourceText(t, m), "long.yaml:250")
	assert.False(t, m.screen.Source.Scrolled)
	assert.Equal(t, 250, m.screen.Source.Selected, "the selection follows the held line")

	// The wheel takes the view from the run: a later stop does not pull it back.
	rect, _ := m.cell(paneSource)
	for range 8 {
		m = send(m, tuitest.Wheel(rect.X+rect.W/2, rect.Y+rect.H/2, true))
	}
	require.True(t, m.screen.Source.Scrolled)
	scrolled := sourceText(t, m)
	assert.NotContains(t, scrolled, "line 250 of the document")

	m = move(m, "s100")
	body := func(m Model) string { return strings.SplitN(sourceText(t, m), "\n", 2)[1] }
	assert.Equal(t, strings.SplitN(scrolled, "\n", 2)[1], body(m), "a new stop fought the person's scroll")

	// Asking the run to move gives the view back to it.
	m = send(m, tuitest.Key("s"))
	assert.False(t, m.screen.Source.Scrolled)

	// The wheel stays inside the document at both ends.
	for range 200 {
		m = send(m, tuitest.Wheel(rect.X+rect.W/2, rect.Y+rect.H/2, true))
	}
	assert.Equal(t, 0, m.screen.Source.Top)
	assert.Contains(t, sourceText(t, m), "line 001 of the document")
	for range 400 {
		m = send(m, tuitest.Wheel(rect.X+rect.W/2, rect.Y+rect.H/2, false))
	}
	assert.Contains(t, sourceText(t, m), "line 300 of the document")
}

func TestTheKeysMoveTheSelectedLineOfAFocusedSource(t *testing.T) {
	t.Parallel()

	f := newFake()
	m, _ := longModel(t, f, 300)
	m = send(m, tuitest.Key("tab"), tuitest.Key("tab"), tuitest.Key("tab"), tuitest.Key("tab"))
	for m.screen.Focus != paneSource {
		m = send(m, tuitest.Key("tab"))
	}
	require.Equal(t, 1, m.screen.Source.Selected, "the held line is selected")

	m = send(m, tuitest.Key("down"), tuitest.Key("down"), tuitest.Key("j"))
	assert.Equal(t, 4, m.screen.Source.Selected)
	assert.True(t, m.screen.Source.Scrolled, "moving the selection takes the view")
	m = send(m, tuitest.Key("up"))
	assert.Equal(t, 3, m.screen.Source.Selected)

	rows := m.sourceRows()
	m = send(m, tuitest.Key("pgdown"))
	assert.Equal(t, 3+rows, m.screen.Source.Selected)
	assert.Contains(t, sourceText(t, m), fmt.Sprintf("line %03d of the document", 3+rows), "the selected line scrolled into view")
	m = send(m, tuitest.Key("end"))
	assert.Equal(t, 300, m.screen.Source.Selected)
	assert.Contains(t, sourceText(t, m), "line 300 of the document")
	m = send(m, tuitest.Key("pgdown"))
	assert.Equal(t, 300, m.screen.Source.Selected, "a key past the end stays on it")
	m = send(m, tuitest.Key("home"))
	assert.Equal(t, 1, m.screen.Source.Selected)
	m = send(m, tuitest.Key("pgup"))
	assert.Equal(t, 1, m.screen.Source.Selected, "a key past the start stays on it")
	assert.Contains(t, sourceText(t, m), "line 001 of the document")
}

// ---- bounds and hostile text ----

func TestTheSourceIsBounded(t *testing.T) {
	t.Parallel()

	t.Run("a document with more lines than the pane holds", func(t *testing.T) {
		t.Parallel()

		text := strings.Repeat("a\n", MaxSourceLines+500)
		src := NewSource([]Document{{URI: "x.yaml", Text: []byte(text)}})
		require.Len(t, src.docs, 1)
		assert.Len(t, src.docs[0].lines, MaxSourceLines)
		assert.Equal(t, 500, src.docs[0].more)
		assert.Contains(t, footerNotes(sourceFace{doc: &src.docs[0]}), "500 more lines not shown")
	})

	t.Run("a line far longer than the pane", func(t *testing.T) {
		t.Parallel()

		src := NewSource([]Document{{URI: "x.yaml", Text: []byte(strings.Repeat("y", MaxSourceBytes-10) + "\nshort\n")}})
		doc := src.docs[0]
		require.Len(t, doc.lines, 2)
		assert.Len(t, []rune(doc.lines[0]), MaxSourceLineRunes)
		assert.True(t, doc.cut[0])
		assert.False(t, doc.cut[1])

		// A line wider than the pane ends in the mark, and no drawn row is wider
		// than the pane.
		clipped := clipLine(doc.lines[0], 30, "…", true)
		assert.True(t, strings.HasSuffix(clipped, "…"))
		assert.LessOrEqual(t, len([]rune(clipped)), 30)
		assert.Equal(t, "short", clipLine("short", 30, "…", false))
		assert.True(t, strings.HasSuffix(clipLine("short", 30, "…", true), "…"), "a line cut at the bound says so even where it fits")
		assert.Equal(t, "", clipLine("short", 0, "…", false))
	})

	t.Run("a document past the byte bound is not read", func(t *testing.T) {
		t.Parallel()

		src := NewSource([]Document{{URI: "x.yaml", Text: make([]byte, MaxSourceBytes+1)}})
		assert.Empty(t, src.docs[0].lines)
		assert.Equal(t, MaxSourceBytes+1, src.docs[0].big)
		assert.Empty(t, src.docs[0].digest, "a document that is not read is not hashed")
	})

	t.Run("more documents than the pane holds", func(t *testing.T) {
		t.Parallel()

		var docs []Document
		for i := range MaxSourceDocuments + 10 {
			docs = append(docs, Document{URI: fmt.Sprintf("f%d.yaml", i), Text: []byte("x\n")})
		}
		assert.Len(t, NewSource(docs).docs, MaxSourceDocuments)
	})

	t.Run("a map of many sites is indexed once", func(t *testing.T) {
		t.Parallel()

		f := newFake()
		m, sm := longModel(t, f, 3000)
		_ = send(m, tuitest.Key("s"))
		assert.Same(t, sm, m.screen.Source.indexFor)
		assert.Len(t, m.screen.Source.index, len(sm.GetEntries()))
	})
}

func TestControlCharactersInSourceNeverReachTheScreen(t *testing.T) {
	t.Parallel()

	hostile := "name: \x1b[2J\x1b]0;owned\x07 x\r\n" + // CSI clear, OSC title, bell, CRLF
		"a:\tb\x00c\x9b31m\x7f\n" + // tab, NUL, C1 CSI, DEL
		"\u202eevil\u2066 \u061c\n" + // bidi overrides
		"ok \u00e9 \u65e5\u672c\n"
	text := []byte(hostile)
	sm := &v1.DebugSourceMap{
		IrDigest: "sha256:h",
		Documents: []*v1.DebugSourceDocument{{
			Uri: "/work/h.yaml", Digest: v1.ContentDigest(text), Language: "flowfile",
		}},
		Entries: []*v1.DebugSourceEntry{{
			Site:     &v1.DebugSite{Workflow: "h", Path: []string{"x"}},
			Location: &v1.DebugSourceLocation{Range: &v1.SourceRange{StartLine: 1, StartColumn: 1, EndLine: 2, EndColumn: 3}},
		}},
	}
	f := newFake()
	f.irDigest = sm.GetIrDigest()
	heldAt(f, "h", "x")
	m := started(t, f, func(c *Config) {
		c.Frame.SourceMap, c.Documents = sm, []Document{{URI: "/work/h.yaml", Text: text}}
	})

	// In the plain variant nothing styles the text, so any escape byte in the
	// screen is the source's.
	screen := view(m)
	for _, r := range strings.ReplaceAll(screen, "\n", "") {
		assert.False(t, unicode.IsControl(r), "control character %U reached the screen", r)
		assert.False(t, reorders(r), "reordering character %U reached the screen", r)
	}
	pane := sourceText(t, m)
	assert.Contains(t, pane, `\x1b[2J`, "the escape is spelled, not sent")
	assert.Contains(t, pane, `\x00`)
	assert.Contains(t, pane, `\u202e`)
	assert.Contains(t, pane, "日本", "ordinary text is untouched")
	assert.NotContains(t, pane, `\r`, "the CR of a CRLF ending is the ending")

	// Every rune an author can write comes out either as itself or spelled out.
	for r := rune(0); r < 0x3000; r++ {
		got, _ := sanitizeLine("a" + string(r) + "b")
		for _, c := range got {
			require.False(t, unicode.IsControl(c) || reorders(c), "%U left a control character in %q", r, got)
		}
	}

	// The styled screen carries only the theme's own escapes: every escape is an
	// SGR sequence.
	styledScreen := func() string {
		m.cfg.Style = styled
		text, _ := m.screen.Draw(styled)

		return text
	}()
	assert.NotContains(t, styledScreen, "\x1b[2J")
	assert.NotContains(t, styledScreen, "\x1b]")
	assert.NotContains(t, styledScreen, "\x07")
}

func TestTabsAreExpandedToAFixedWidth(t *testing.T) {
	t.Parallel()

	got, _ := sanitizeLine("a\tb\t\tc")
	assert.Equal(t, "a   b       c", got)
	assert.Equal(t, strings.Repeat(" ", sourceTabWidth), func() string { s, _ := sanitizeLine("\t"); return s }())
}

func TestTheLightTouchHighlightsCommentsAndExpressions(t *testing.T) {
	t.Parallel()

	o := pane.Options{Theme: styled.Theme, Symbols: styled.Symbols}

	assert.Equal(t, o.Theme.Muted.Render("  # a comment ${not.an.expression}"), highlight("  # a comment ${not.an.expression}", o))
	assert.Equal(t, "plain: text", highlight("plain: text", o))
	assert.Equal(t, "m: "+o.Theme.Accent.Render("${a.b}")+" end", highlight("m: ${a.b} end", o))
	assert.Equal(t, o.Theme.Accent.Render("${ {a: 1}.a }")+" and "+o.Theme.Accent.Render("${b}"), highlight("${ {a: 1}.a } and ${b}", o), "braces nest")
	assert.Equal(t, "x "+o.Theme.Accent.Render("${unclosed"), highlight("x ${unclosed", o), "an expression cut by the pane's edge")

	// With no styling the text is exactly the text.
	for _, text := range []string{"# c", "m: ${a} ${b}", "${", "${}", "}${"} {
		assert.Equal(t, text, highlight(text, pane.Options{Theme: plain.Theme, Symbols: plain.Symbols}))
	}
}

// ---- layout, hits and goldens ----

func TestTheSourcePaneFitsEverySizeAndItsHitsLieOnDrawnCells(t *testing.T) {
	t.Parallel()

	fx := newSourceFixture(t)
	for _, size := range tuitest.Sizes {
		t.Run(size.String(), func(t *testing.T) {
			t.Parallel()

			m := sourceModel(t, fx, newFake(), func(c *Config) { c.Size = size })
			text, hits := m.screen.Draw(plain)
			if size.W < MinWidth || size.H < MinHeight {
				assert.Zero(t, hits.Len(), "something was clickable on a screen too small to draw")

				return
			}
			tuitest.Fits(t, text, size)

			rows := lines(text)
			rect, drawn := m.cell(paneSource)
			if !drawn {
				// Folded into a tab: the tab names it, and a click on the tab brings it up.
				x, y := find(t, m, paneSource)
				m = send(m, tuitest.Click(x, y))
				rect, drawn = m.cell(paneSource)
				require.True(t, drawn, "the source tab did not bring the pane up")
				text, hits = m.screen.Draw(plain)
				rows = lines(text)
			}

			seen := 0
			for y := range size.H {
				for x := range size.W {
					hit, ok := hits.At(x, y)
					if !ok {
						continue
					}
					id, gutter := strings.CutPrefix(hit.ID, gutterPrefix)
					if rest, line := strings.CutPrefix(hit.ID, sourcePrefix); line {
						id = rest
					} else if !gutter {
						continue
					}
					seen++
					assert.True(t, rect.Contains(x, y), "%s hit outside the pane at %d,%d", hit.ID, x, y)
					assert.True(t, hit.Rect.Contains(x, y))
					// The row is a row of the document: its number is on it, in the gutter.
					cells := []rune(rows[y])
					assert.Contains(t, strings.TrimSpace(string(cells[rect.X:min(len(cells), rect.X+rect.W)])), id, "%s is not drawn on its own row", hit.ID)
					if gutter {
						assert.Contains(t, string(cells[hit.Rect.X:hit.Rect.X+hit.Rect.W]), id, "the gutter hit of line %s lies off its number", id)
					}
				}
			}
			assert.Positive(t, seen, "nothing in the source could be clicked at %v", size)
		})
	}
}

func TestTheSourcePaneGolden(t *testing.T) {
	fx := newSourceFixture(t)
	for _, v := range styles {
		t.Run(v.name, func(t *testing.T) {
			var b strings.Builder
			show := func(name string, s *Source, frame flowdebug.Frame, w, h int, focused bool) {
				b.WriteString("=== " + name + "\n")
				b.WriteString(SourceView(s, frame, true, opts(v.style, w, h, focused)) + "\n")
			}

			f := newFake()
			f.irDigest = fx.sm.GetIrDigest()
			heldAt(f, "main", "charge")
			f.breakpoints = []*v1.DebugBreakpointState{{
				Id: "bp", Verified: true,
				Definition: &v1.DebugBreakpoint{Id: "bp", Line: &v1.DebugSourceLine{Uri: fx.root, Line: uint32(lineOf(t, mainText, "id: fetch"))}},
			}}
			frame := frameOf(t, f, true)
			frame.SourceMap = fx.sm
			src := NewSource(fx.docs)
			src.Apply(frame)
			show("held in the main file", src, frame, 46, 14, true)

			f.breakpoints = nil
			heldAt(f, "child", "greet")
			frame = frameOf(t, f, true)
			frame.SourceMap = fx.sm
			src.Apply(frame)
			show("held in a called file", src, frame, 40, 9, false)

			show("a narrow pane clips with a mark", src, frame, 22, 8, false)

			unverified := frameOf(t, f, true)
			show("unverified", NewSource(fx.docs), unverified, 46, 8, false)
			show("no source offered", NewSource(nil), unverified, 46, 8, false)

			tuitest.Golden(t, b.String())
		})
	}
}

func TestTheScreenWithASourceGolden(t *testing.T) {
	fx := newSourceFixture(t)
	for _, size := range []tui.Size{{W: 70, H: 20}, {W: 80, H: 24}, {W: 100, H: 30}, {W: 120, H: 36}} {
		t.Run(size.String(), func(t *testing.T) {
			f := newFake()
			m := sourceModel(t, fx, f, func(c *Config) { c.Size = size })
			if size.W < 80 {
				m = send(m, tuitest.Click(find(t, m, paneSource)))
			}
			text, _ := m.screen.Draw(plain)
			tuitest.Golden(t, text)
			tuitest.Fits(t, text, size)
		})
	}
}

func TestNothingInTheSourceReadsAClock(t *testing.T) {
	t.Parallel()

	source, err := os.ReadFile("source.go")
	require.NoError(t, err)
	for _, banned := range []string{"time.Now(", "time.Since(", "time.Tick(", "time.After("} {
		assert.NotContains(t, string(source), banned)
	}
}

func TestEscapeControlAgreesWithTheSanitizer(t *testing.T) {
	t.Parallel()

	// The sanitizer spells a control character the way the shared helper does.
	got, _ := sanitizeLine("a\x1b[0m\x07")
	assert.Equal(t, ui.EscapeControl("a\x1b[0m\x07"), got)
}
