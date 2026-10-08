package pane_test

import (
	"errors"
	"fmt"
	"go/parser"
	"go/token"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"charm.land/lipgloss/v2"
	"github.com/charmbracelet/colorprofile"
	golden "github.com/charmbracelet/x/exp/golden"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/picatz/flowstate/cmd/flow/internal/pane"
	"github.com/picatz/flowstate/cmd/flow/internal/ui"
)

// options is a stated drawing environment, so a view's bytes are the test's
// and not the machine's: the three variants are the ones debugpane pins.
func options(w, h int, profile colorprofile.Profile, unicode bool) pane.Options {
	caps := ui.Capabilities{Profile: profile, TTY: true, Width: w, Height: h, Unicode: unicode}

	return pane.Options{Width: w, Height: h, Theme: ui.NewTheme(true, caps), Symbols: caps.Symbols(), Focused: true}
}

var variants = []struct {
	name    string
	profile colorprofile.Profile
	unicode bool
}{
	{"styled", colorprofile.TrueColor, true},
	{"ascii", colorprofile.TrueColor, false},
	{"plain", colorprofile.NoTTY, true},
}

func sample() []pane.Node {
	return []pane.Node{
		{ID: "g:inputs", Label: "inputs", Value: "{2}", Children: []pane.Node{
			{ID: "inputs.version", Label: "version", Value: `"2026.9.0"`},
			{ID: "inputs.region", Label: "region", Value: `"eu-west-1"`},
		}},
		{ID: "g:steps", Label: "steps", Value: "{40}", Total: 40, Children: []pane.Node{
			{ID: "steps.build", Label: "build", Value: "{artifact: build.tar.gz}", Total: 1},
			{ID: "steps.test", Label: "test", Value: "{ok: true}"},
		}},
		{ID: "g:run", Label: "run", Value: "{}"},
	}
}

func TestHitsResolveWhatWasDrawnAndNothingElse(t *testing.T) {
	t.Parallel()

	var hits pane.Hits
	hits.Add(pane.Rect{X: 0, Y: 0, W: 10, H: 5}, "pane", pane.KindPane)
	hits.Add(pane.Rect{X: 0, Y: 1, W: 10, H: 1}, "row", pane.KindRow)
	hits.Add(pane.Rect{X: 3, Y: 3, W: 0, H: 1}, "empty", pane.KindRow)

	got, ok := hits.At(4, 1)
	require.True(t, ok)
	assert.Equal(t, "row", got.ID, "the later registration is on top")

	got, ok = hits.At(4, 2)
	require.True(t, ok)
	assert.Equal(t, "pane", got.ID)

	for _, outside := range [][2]int{{10, 1}, {-1, 0}, {0, 5}} {
		_, ok = hits.At(outside[0], outside[1])
		assert.False(t, ok, "%v is outside everything drawn", outside)
	}
	assert.Equal(t, 2, hits.Len(), "an empty rectangle was registered")

	var none *pane.Hits
	none.Add(pane.Rect{W: 1, H: 1}, "x", pane.KindPane)
	_, ok = none.At(0, 0)
	assert.False(t, ok, "a nil registry answered")
}

func TestHitsAreBounded(t *testing.T) {
	t.Parallel()

	var hits pane.Hits
	for i := range pane.MaxHits + 100 {
		hits.Add(pane.Rect{X: i, W: 1, H: 1}, "r", pane.KindRow)
	}
	assert.Equal(t, pane.MaxHits, hits.Len())
}

func TestATreeOpensAndClosesAndKeepsItsPlaceWhenReplaced(t *testing.T) {
	t.Parallel()

	tree := pane.NewTree(sample())
	require.Len(t, tree.Rows(), 3, "nothing starts open")
	assert.Equal(t, "g:inputs", tree.Selected())

	_, load := tree.Activate("g:inputs")
	assert.False(t, load, "a node whose children are all held has nothing to load")
	require.Len(t, tree.Rows(), 5)

	tree.Move(2)
	assert.Equal(t, "inputs.region", tree.Selected())
	tree.Collapse("g:inputs")
	assert.Equal(t, "g:inputs", tree.Selected(), "the selection was left inside a closed node")

	// A new read replaces the nodes; what was open stays open and the
	// selection stays on its node.
	tree.Toggle("g:inputs")
	tree.Select("inputs.version")
	tree.SetRoots(sample())
	assert.True(t, tree.Open("g:inputs"))
	assert.Equal(t, "inputs.version", tree.Selected())

	// A node that is gone takes its state with it.
	tree.SetRoots(sample()[1:])
	assert.False(t, tree.Open("g:inputs"))
	assert.Equal(t, "g:steps", tree.Selected())
}

func TestATreePagesChildrenItDoesNotHold(t *testing.T) {
	t.Parallel()

	tree := pane.NewTree(sample())
	request, load := tree.Activate("g:steps")
	assert.False(t, load, "two of forty are held, so opening needs no first page")

	rows := tree.Rows()
	more := rows[len(rows)-2]
	require.Equal(t, pane.RowMore, more.Kind, "the elision row is missing: %+v", rows)
	assert.Equal(t, 38, more.Remaining)

	request, load = tree.Activate(more.ID)
	require.True(t, load)
	assert.Equal(t, pane.Request{Parent: "g:steps", Offset: 2}, request)

	loader := func(r pane.Request) ([]pane.Node, int, error) {
		var out []pane.Node
		for i := r.Offset; i < r.Offset+20; i++ {
			out = append(out, pane.Node{ID: fmt.Sprintf("steps.s%d", i), Label: fmt.Sprintf("s%d", i)})
		}

		return out, 40, nil
	}
	require.NoError(t, tree.Load(loader, request))
	node, _ := tree.Node("g:steps")
	assert.Len(t, node.Children, 22)

	// A page for an offset the tree is not at is an answer to an old question.
	assert.False(t, tree.Fill("g:steps", 2, []pane.Node{{ID: "late"}}, 40), "a stale page was appended")
	_, found := tree.Node("late")
	assert.False(t, found)

	// A loader's refusal changes nothing.
	err := tree.Load(func(pane.Request) ([]pane.Node, int, error) { return nil, 0, errors.New("denied") }, request)
	require.EqualError(t, err, "denied")
	node, _ = tree.Node("g:steps")
	assert.Len(t, node.Children, 22)
}

func TestALazyBranchAsksForItsFirstPageWhenOpened(t *testing.T) {
	t.Parallel()

	tree := pane.NewTree(sample())
	tree.Toggle("g:steps")
	request, load := tree.Activate("steps.build")
	require.True(t, load, "a branch with nothing held did not ask")
	assert.Equal(t, pane.Request{Parent: "steps.build"}, request)

	_, load = tree.Activate("steps.test")
	assert.False(t, load, "a leaf asked for children")
	_, load = tree.Activate("no-such-row")
	assert.False(t, load)
	assert.Equal(t, "steps.test", tree.Selected(), "an unknown id moved the selection")
}

func TestATreeIsBoundedPerParent(t *testing.T) {
	t.Parallel()

	tree := pane.NewTree([]pane.Node{{ID: "p", Label: "p", Total: 100000}})
	var page []pane.Node
	for i := range pane.MaxChildren + 10 {
		page = append(page, pane.Node{ID: fmt.Sprint(i)})
	}
	tree.Fill("p", 0, page, 100000)
	node, _ := tree.Node("p")
	assert.Len(t, node.Children, pane.MaxChildren)
	assert.Equal(t, pane.MaxChildren, node.Total, "the count claims children the tree will not hold")
}

func TestATreeScrollsAndRevealsTheSelection(t *testing.T) {
	t.Parallel()

	var roots []pane.Node
	for i := range 50 {
		roots = append(roots, pane.Node{ID: fmt.Sprint("n", i), Label: fmt.Sprint("n", i)})
	}
	tree := pane.NewTree(roots)

	tree.Scroll(1000, 10)
	assert.Equal(t, 40, tree.Top(), "scrolled past the last page")
	tree.Scroll(-1000, 10)
	assert.Zero(t, tree.Top())

	tree.End()
	tree.Reveal(10)
	assert.Equal(t, 40, tree.Top())
	tree.Home()
	tree.Reveal(10)
	assert.Zero(t, tree.Top())
}

func TestATreeViewGolden(t *testing.T) {
	for _, v := range variants {
		t.Run(v.name, func(t *testing.T) {
			tree := pane.NewTree(sample())
			tree.Toggle("g:inputs")
			tree.Toggle("g:steps")
			tree.Select("steps.build")

			var hits pane.Hits
			o := options(40, 8, v.profile, v.unicode)
			o.Hits, o.Prefix, o.Origin = &hits, "scope/", pane.Rect{X: 2, Y: 3}
			view := tree.View(o, "nothing")
			golden.RequireEqual(t, []byte(view+"\n"))

			for line := range strings.SplitSeq(view, "\n") {
				assert.LessOrEqual(t, lipgloss.Width(line), 40)
			}
			hit, ok := hits.At(5, 3+4)
			require.True(t, ok)
			assert.Equal(t, "scope/steps.build", hit.ID, "the row drawn at that line is the one a click there names")
		})
	}
}

func TestATreeDrawsDataAsDataNotAsControl(t *testing.T) {
	t.Parallel()

	tree := pane.NewTree([]pane.Node{{ID: "x", Label: "na\x1b[31mme", Value: "line\none\x07"}})
	view := tree.View(options(60, 3, colorprofile.NoTTY, true), "")
	assert.NotContains(t, view, "\x1b")
	assert.NotContains(t, view, "\x07")
	assert.Contains(t, view, `na\x1b[31mme`)
	assert.Equal(t, 1, strings.Count(view, "\n")+1, "a newline in a value made a second row")
}

func TestAnEmptyTreeSaysWhy(t *testing.T) {
	t.Parallel()

	view := pane.NewTree(nil).View(options(40, 3, colorprofile.NoTTY, true), "inspect is not permitted")
	assert.Equal(t, "inspect is not permitted", view)
}

func TestTheInspectorWrapsAValueUnderItsKey(t *testing.T) {
	for _, v := range variants {
		t.Run(v.name, func(t *testing.T) {
			in := pane.Inspector{
				Fields: []pane.Field{
					{Key: "expression", Value: "steps.build.artifact"},
					{Key: "type", Value: "string"},
					{Key: "value", Value: strings.Repeat("0123456789", 5)},
				},
				Note: "cut at 240 characters",
			}
			view := in.View(options(30, 10, v.profile, v.unicode), "")
			golden.RequireEqual(t, []byte(view+"\n"))
			for line := range strings.SplitSeq(view, "\n") {
				assert.LessOrEqual(t, lipgloss.Width(line), 30)
			}
		})
	}

	empty := pane.Inspector{}.View(options(30, 3, colorprofile.NoTTY, true), "select a row")
	assert.Equal(t, "select a row", empty)

	// Past the height it says it stopped.
	tall := pane.Inspector{Fields: []pane.Field{{"k", strings.Repeat("x", 300)}}}
	lines := strings.Split(tall.View(options(20, 4, colorprofile.NoTTY, false), ""), "\n")
	assert.Len(t, lines, 4)
	assert.Equal(t, "...", lines[3])
}

func TestASplitDividesWithoutLosingACell(t *testing.T) {
	t.Parallel()

	for _, w := range []int{3, 10, 61, 120} {
		for _, split := range []pane.Split{{}, {Percent: 30, Gap: 1}, {Percent: 90, Gap: 2}} {
			a, b := split.Rects(pane.Rect{W: w, H: 7})
			assert.Positive(t, a.W)
			assert.Positive(t, b.W, "w=%d %+v left the second pane nothing", w, split)
			assert.Equal(t, w, a.W+min(split.Gap, max(0, w-2))+b.W)
		}
	}

	a, b := pane.Split{Orientation: pane.Rows, Percent: 25}.Rects(pane.Rect{X: 1, Y: 2, W: 9, H: 8})
	assert.Equal(t, pane.Rect{X: 1, Y: 2, W: 9, H: 2}, a)
	assert.Equal(t, pane.Rect{X: 1, Y: 4, W: 9, H: 6}, b)

	// A width of nothing is a split of the fallback, not a division by it.
	a, b = pane.Split{}.Rects(pane.Rect{})
	assert.Positive(t, a.W+b.W)
}

func TestStitchFillsTheScreenExactly(t *testing.T) {
	t.Parallel()

	out := pane.Split{Gap: 1}.View(21, 4, "left\nsecond line that is far too long to fit", "right")
	lines := strings.Split(out, "\n")
	require.Len(t, lines, 4)
	for _, line := range lines {
		assert.Equal(t, 21, lipgloss.Width(line), "%q", line)
	}
	assert.True(t, strings.HasPrefix(lines[0], "left"))
	assert.Contains(t, lines[0], "right")

	// A pane starting inside another is not drawn over it, and a pane off the
	// screen is not drawn at all.
	out = pane.Stitch(10, 2,
		pane.Placed{Rect: pane.Rect{W: 6, H: 1}, Text: "aaaaaa"},
		pane.Placed{Rect: pane.Rect{X: 3, W: 6, H: 1}, Text: "bbbbbb"},
		pane.Placed{Rect: pane.Rect{X: 12, W: 3, H: 1}, Text: "ccc"})
	assert.Equal(t, "aaaaaa    \n          ", out)
}

// TestThePanePackageHasNoShell is the line the package is built on: it imports
// neither bubbletea nor the shell that wraps it, so a pane can only be a
// function of its inputs.
func TestThePanePackageHasNoShell(t *testing.T) {
	t.Parallel()

	files, err := filepath.Glob("*.go")
	require.NoError(t, err)
	require.NotEmpty(t, files)

	for _, file := range files {
		parsed, err := parser.ParseFile(token.NewFileSet(), file, nil, parser.ImportsOnly)
		require.NoError(t, err)
		for _, spec := range parsed.Imports {
			path := strings.Trim(spec.Path.Value, `"`)
			assert.NotContains(t, path, "bubbletea", "%s imports the event loop", file)
			assert.NotContains(t, path, "flowstate/cmd/flow/internal/tui", "%s imports the shell", file)
		}
	}
}

// TestThePanePackageReadsNoClock: a pane is a function of its inputs.
func TestThePanePackageReadsNoClock(t *testing.T) {
	t.Parallel()

	files, err := filepath.Glob("*.go")
	require.NoError(t, err)
	for _, file := range files {
		if strings.HasSuffix(file, "_test.go") {
			continue
		}
		source, err := os.ReadFile(file)
		require.NoError(t, err)
		for _, banned := range []string{"time.Now(", "time.Tick(", "time.Since(", "os.Stdout", "term.GetSize"} {
			assert.NotContains(t, string(source), banned, "%s", file)
		}
	}
}
