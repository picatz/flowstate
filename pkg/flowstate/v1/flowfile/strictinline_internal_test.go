package flowfile

import (
	"strconv"
	"strings"
	"testing"

	"github.com/goccy/go-yaml/parser"
	"github.com/stretchr/testify/require"
)

// The inliner's bound, driven from both sides.
//
// An inliner is an expander, and CLAUDE.md's rule for an expander is that the
// resource the attacker controls is bounded — here the total nodes the rewrite
// would add, in [maxNodes], the same spelling the compiler's own expansion uses.
// #841's third item is the reason this is asserted directly rather than left to
// the shape of the code: a bound nothing reaches is a bound nothing tests, and
// this rewriter is exactly the path that put one of these bounds back in front
// of input an outside party chooses.
//
// Both directions, because `sites <= maxNodes` is also satisfied by a rewriter
// that gave up at ten: the bound has to be *reached* and not exceeded.

// inlinableDocumentWithAliases builds a Flowfile whose single anchored scalar is
// referenced by n aliases, each in a whole-value position.
func inlinableDocumentWithAliases(n int) []byte {
	var b strings.Builder
	b.WriteString("edition: " + CurrentEdition + "\nname: many\nvars:\n  v: &v 1\n  many:\n")
	for range n {
		b.WriteString("    - *v\n")
	}
	b.WriteString("steps:\n  - id: a\n    log:\n      message: hi\n")
	return []byte(b.String())
}

func TestInlineStrictYAMLBoundsTheNodesItWouldAdd(t *testing.T) {
	t.Parallel()

	tests := map[string]struct {
		aliases int
		inlined bool
	}{
		"at the bound":   {aliases: maxNodes, inlined: true},
		"past the bound": {aliases: maxNodes + 1, inlined: false},
	}

	for name, test := range tests {
		t.Run(name, func(t *testing.T) {
			t.Parallel()

			src := inlinableDocumentWithAliases(test.aliases)
			require.Less(t, len(src), maxBytes,
				"premise: the document is one flow fix would read, so the byte bound is not what refuses it")

			file, err := parser.ParseBytes(src, parser.ParseComments)
			require.NoError(t, err)

			out, changes, ok := inlineStrictYAML(src, file)
			require.Equal(t, test.inlined, ok)
			if !test.inlined {
				require.Nil(t, out, "a refused rewrite writes nothing")
				require.Nil(t, changes)
				return
			}
			// Reached, not merely not exceeded: every site was spliced.
			require.Len(t, changes, test.aliases+1, "one change per alias, plus the anchor's own")
			require.NotContains(t, string(out), "*v")
			require.NotContains(t, string(out), "&v")
		})
	}
}

// TestInlineStrictYAMLCannotBeGivenANestedExpansion records why the bound above
// is the second answer rather than the first.
//
// A billion-laughs document is aliases *inside anchored values*, so that each
// level multiplies the last. This rewriter never sees such a level: an anchor
// whose value is anything but a single-line scalar is refused, so the deepest
// expansion it can be asked for is one scalar copied once per site. The bound
// exists anyway — a rewriter's safety should not rest on a second rule holding
// somewhere else — but this is the argument that the multiplying shape is
// unreachable, written where a change that reopened it would fail.
func TestInlineStrictYAMLCannotBeGivenANestedExpansion(t *testing.T) {
	t.Parallel()

	var b strings.Builder
	b.WriteString("edition: " + CurrentEdition + "\nname: boom\nvars:\n  l0: &l0 \"lol\"\n")
	for i := 1; i <= 9; i++ {
		b.WriteString("  l" + strconv.Itoa(i) + ": &l" + strconv.Itoa(i) + "\n")
		for range 9 {
			b.WriteString("    - *l" + strconv.Itoa(i-1) + "\n")
		}
	}
	b.WriteString("steps:\n  - id: s\n    log:\n      message: hi\n")

	src := []byte(b.String())
	file, err := parser.ParseBytes(src, parser.ParseComments)
	require.NoError(t, err)

	out, _, ok := inlineStrictYAML(src, file)
	require.False(t, ok, "an anchor over a sequence is refused, so no level of this can be inlined")
	require.Nil(t, out)
}
