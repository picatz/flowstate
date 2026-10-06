package strictyaml_test

import (
	"errors"
	"math/rand/v2"
	"runtime"
	"strings"
	"testing"

	"github.com/goccy/go-yaml/ast"
	"github.com/goccy/go-yaml/parser"
	"github.com/stretchr/testify/require"

	"github.com/picatz/flowstate/internal/strictyaml"
)

func TestParseBytesRefusesFlowNestingPastTheBoundWithAPosition(t *testing.T) {
	t.Parallel()

	for name, opener := range map[string]string{"sequences": "[", "mappings": "{a: "} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()

			doc := "a: 1\nb: " + strings.Repeat(opener, strictyaml.MaxFlowDepth+1) + "\n"
			_, err := strictyaml.ParseBytes([]byte(doc), 0)

			nesting, ok := errors.AsType[*strictyaml.NestingError](err)
			require.True(t, ok, "%v", err)
			require.Equal(t, 2, nesting.Line)
			require.NotContains(t, err.Error(), "a: 1")
		})
	}
}

func TestParseBytesAcceptsNestingAtTheBound(t *testing.T) {
	t.Parallel()

	doc := "b: " + strings.Repeat("[", strictyaml.MaxFlowDepth) + strings.Repeat("]", strictyaml.MaxFlowDepth) + "\n"
	_, err := strictyaml.ParseBytes([]byte(doc), 0)
	require.NoError(t, err)
}

// The depth is the lexer's, which is the parser's: a bracket in a quoted
// string, a comment or a block scalar opens nothing, so prose full of them is
// not refused, and a closer in a string does not give depth back.
func TestParseBytesCountsTheBracketsTheParserOpens(t *testing.T) {
	t.Parallel()

	many := strings.Repeat("[", 10*strictyaml.MaxFlowDepth)

	for name, doc := range map[string]string{
		"double quoted": "a: \"" + many + "\"\n",
		"single quoted": "a: '" + many + "'\n",
		"comment":       "a: 1 # " + many + "\n",
		"block scalar":  "a: |\n  " + many + "\n",
	} {
		_, err := strictyaml.ParseBytes([]byte(doc), 0)
		require.NoError(t, err, name)
	}

	// Closers in strings do not pay down real depth.
	doc := "a: " + strings.Repeat(`[ "]", `, strictyaml.MaxFlowDepth+1) + "\n"
	_, err := strictyaml.ParseBytes([]byte(doc), parser.ParseComments)
	require.Error(t, err)
}

// Forty thousand levels took 2.5 GB in the parser (#2338). The refusal reads
// the tokens and nothing more, so it stays far under what one parse of the
// document cost.
func TestParseBytesBoundsWhatAHostileDocumentCosts(t *testing.T) {
	// Serial: reads process-wide allocation counters.
	doc := []byte("a: " + strings.Repeat("[", 40000) + "\n")

	var before, after runtime.MemStats
	runtime.ReadMemStats(&before)
	_, err := strictyaml.ParseBytes(doc, 0)
	runtime.ReadMemStats(&after)

	require.Error(t, err)
	require.Less(t, after.TotalAlloc-before.TotalAlloc, uint64(32<<20))
}

// Unmarshal reads the same tokens as ParseBytes and refuses the same depth, so
// a caller that decodes a document after parsing it (or instead of parsing it)
// is as bounded as one that only parses.
func TestUnmarshalRefusesFlowNestingPastTheBound(t *testing.T) {
	t.Parallel()

	var into map[string]any

	err := strictyaml.UnmarshalStrict([]byte("a: "+strings.Repeat("[", 40000)+"\n"), &into)
	_, ok := errors.AsType[*strictyaml.NestingError](err)
	require.True(t, ok, "%v", err)

	require.NoError(t, strictyaml.UnmarshalStrict([]byte("a: [[1]]\n"), &into))
}

// Block nesting costs the parser what flow nesting does, and `- - - x` needs no
// bracket: a chain of sequence entries on one line, or mappings indented ever
// further, must be refused on the columns the lexer reports, while documents
// that are long rather than deep must not be.
func TestParseBytesRefusesBlockNestingPastTheBound(t *testing.T) {
	t.Parallel()

	var nested strings.Builder
	for i := range strictyaml.MaxFlowDepth + 1 {
		nested.WriteString(strings.Repeat(" ", i) + "k:\n")
	}

	for name, doc := range map[string]string{
		"a chain of sequence entries": strings.Repeat("- ", 40000) + "x\n",
		"indented mappings":           nested.String() + strings.Repeat(" ", strictyaml.MaxFlowDepth+1) + "v\n",
	} {
		_, err := strictyaml.ParseBytes([]byte(doc), 0)
		nesting, ok := errors.AsType[*strictyaml.NestingError](err)
		require.True(t, ok, "%s: %v", name, err)
		require.Contains(t, nesting.Reason, "block collections", name)
	}
}

func TestParseBytesDoesNotCountSiblingsOrFlowEntriesAsBlockDepth(t *testing.T) {
	t.Parallel()

	var long strings.Builder
	for range 5000 {
		long.WriteString("- a: 1\n  b: {c: [1, 2], d: 3}\n- - x\n  - y\n")
	}

	_, err := strictyaml.ParseBytes([]byte(long.String()), 0)
	require.NoError(t, err)

	// A chain exactly at the bound is read.
	_, err = strictyaml.ParseBytes([]byte(strings.Repeat("- ", strictyaml.MaxFlowDepth)+"x\n"), 0)
	require.NoError(t, err)
}

func TestParseBytesBoundsABlockChainsCost(t *testing.T) {
	// Serial: reads process-wide allocation counters.
	doc := []byte(strings.Repeat("- ", 40000) + "x\n")

	var before, after runtime.MemStats
	runtime.ReadMemStats(&before)
	_, err := strictyaml.ParseBytes(doc, 0)
	runtime.ReadMemStats(&after)

	require.Error(t, err)
	require.Less(t, after.TotalAlloc-before.TotalAlloc, uint64(32<<20))
}

// The column a mapping value's `:` sits at moves with the length of its key, so
// a stack keyed on it can be held flat by indenting each level one space more
// and its key two characters shorter: the parser still nests a level per line.
// The stack is keyed on where the entry starts.
func TestParseBytesCountsMappingDepthByWhereTheEntryStartsNotItsColon(t *testing.T) {
	t.Parallel()

	const levels = 3 * strictyaml.MaxFlowDepth / 2

	var doc strings.Builder
	for i := range levels {
		doc.WriteString(strings.Repeat(" ", i) + strings.Repeat("k", 2*(levels-i)) + ":\n")
	}

	doc.WriteString(strings.Repeat(" ", levels) + "v\n")

	_, err := strictyaml.ParseBytes([]byte(doc.String()), 0)
	nesting, ok := errors.AsType[*strictyaml.NestingError](err)
	require.True(t, ok, "%v", err)
	require.Contains(t, nesting.Reason, "block collections")
}

// Properties before a key start the entry too: an anchor or tag cannot be used
// to move where an entry is counted from.
func TestParseBytesCountsAnchoredAndTaggedEntriesFromTheirProperties(t *testing.T) {
	t.Parallel()

	const levels = 3 * strictyaml.MaxFlowDepth / 2

	for name, prop := range map[string]func(n int) string{
		"anchor": func(n int) string { return "&" + strings.Repeat("a", n) + " " },
		"tag":    func(n int) string { return "!" + strings.Repeat("t", n) + " " },
	} {
		var doc strings.Builder
		for i := range levels {
			doc.WriteString(strings.Repeat(" ", i) + prop(2*(levels-i)) + "k:\n")
		}

		doc.WriteString(strings.Repeat(" ", levels) + "v\n")

		_, err := strictyaml.ParseBytes([]byte(doc.String()), 0)
		_, ok := errors.AsType[*strictyaml.NestingError](err)
		require.True(t, ok, "%s: %v", name, err)
	}
}

// depthVisitor measures how deeply the parser nested a tree, which is what its
// cost follows, whatever the lexer's columns suggested.
type depthVisitor struct {
	depth int
	max   *int
}

func (v depthVisitor) Visit(ast.Node) ast.Visitor {
	*v.max = max(*v.max, v.depth)

	return depthVisitor{depth: v.depth + 1, max: v.max}
}

// A column-based check can be fooled by a shape its author did not think of
// (the anchor alone on a `- &a` line, then its key at the same column, nests in
// the parser and is flat in the columns). So the property is checked against
// the parser rather than reasoned about: whatever a tiled template is, a
// document the check accepts must stay within a few multiples of the bound in
// the parser's own tree (the flow, block and value-below counters each take up
// to the bound, so the sum is the ceiling that matters, and it is a constant). The templates are a fixed seed's worth of one to three line
// combinations of the tokens that open a level.
func TestParseBytesAcceptsNothingTheParserNestsFarPastTheBound(t *testing.T) {
	t.Parallel()

	pieces := []string{
		"- ", "- - ", "k:", "k: ", "? k", "? ", ": ", "&a", "!t", "&a k:", "!t k:",
		"- &a", "- !t", "- k:", "k: &a", "k: !t", "- ? k", "- &a k:", "[", "{a: ",
		"# c", "- # c", "k: # c", "- &a # c", "k: !t # c",
	}
	const tiles = 3 * strictyaml.MaxFlowDepth

	// Shapes reviewers found that a column alone could not count, then the
	// seeded draws.
	templates := [][]string{{"- &a", "k:"}, {"- !t", "? k"}, {"- &a", "? k"}, {"- &a", "# c", "k:"}, {"- &a # c", "      # deeper", "k:"}}

	rng := rand.New(rand.NewPCG(2338, 1))
	for range 400 {
		lines := make([]string, 1+rng.IntN(3))
		for i := range lines {
			lines[i] = pieces[rng.IntN(len(pieces))]
		}

		templates = append(templates, lines)
	}

	for _, lines := range templates {
		step := rng.IntN(3)

		var doc strings.Builder
		for level := range tiles {
			for i, line := range lines {
				// Each level indents `step` further; a later line of a template
				// may sit at the level's own column or one deeper.
				doc.WriteString(strings.Repeat(" ", level*step+(i%2)*rng.IntN(2)) + line + "\n")
			}
		}

		tree, err := strictyaml.ParseBytes([]byte(doc.String()), 0)
		if err != nil {
			continue
		}

		deepest := 0
		ast.Walk(depthVisitor{max: &deepest}, tree.Docs[0].Body)
		require.LessOrEqual(t, deepest, 4*strictyaml.MaxFlowDepth,
			"the check accepted a document the parser nests %d deep: lines %q, step %d", deepest, lines, step)
	}
}
