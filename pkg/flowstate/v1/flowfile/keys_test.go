package flowfile

import (
	"fmt"
	"testing"

	"github.com/goccy/go-yaml/ast"
	"github.com/goccy/go-yaml/parser"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// What a mapping key may be is a question two passes ask, and they must give one
// answer: [compiler.entries] decides which names a mapping claims for itself
// using keyNameOf, and then reads the same keys again through compiler.keyName.
// A name the first pass fails to see is a name a merged mapping silently
// overrides — a wrong value with no diagnostic, which is the worst shape a bug
// can take in a config language.
//
// keyName is keyNameOf plus a diagnostic for that reason. This test pins the
// vocabulary both of them work over, because the divergence they used to have was
// a branch for a node type the parser never produces: unreachable, and so
// invisible to every test that went through the parser.
func TestKeyNodeVocabulary(t *testing.T) {
	t.Parallel()

	// Every way a key can be spelled, and the node the parser makes of it. A
	// change here is a change in what the DSL accepts, which is why the mapping is
	// written out rather than derived.
	tests := []struct {
		name   string
		src    string
		want   string // the AST node type the parser produces for the key
		spells string // the name keyNameOf reads from it, empty when it reads none
	}{
		{name: "plain", src: "a: 1\n", want: "*ast.StringNode", spells: "a"},
		{name: "double quoted", src: "\"a\": 1\n", want: "*ast.StringNode", spells: "a"},
		{name: "single quoted", src: "'a': 1\n", want: "*ast.StringNode", spells: "a"},
		{name: "number", src: "1: 1\n", want: "*ast.IntegerNode"},
		{name: "boolean", src: "true: 1\n", want: "*ast.BoolNode"},
		{name: "null", src: "~: 1\n", want: "*ast.NullNode"},
		{name: "merge", src: "<<: *x\n", want: "*ast.MergeKeyNode"},
		{name: "anchored", src: "&k a: 1\n", want: "*ast.AnchorNode"},
		{name: "aliased", src: "*k : 1\n", want: "*ast.AliasNode"},

		// Every explicit key arrives wrapped, whatever is inside it. This is the
		// row that matters: a block scalar cannot open a mapping key, so a key is
		// never a *ast.LiteralNode, and a keyNameOf that handled one would be
		// handling a shape that cannot occur.
		{name: "explicit plain", src: "? a\n: 1\n", want: "*ast.MappingKeyNode"},
		{name: "explicit block scalar", src: "? |-\n  a\n: 1\n", want: "*ast.MappingKeyNode"},
		{name: "explicit folded scalar", src: "? >-\n  a\n: 1\n", want: "*ast.MappingKeyNode"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			file, err := parser.ParseBytes([]byte(tt.src), 0)
			require.NoError(t, err, "this key spelling should parse as YAML")
			require.NotEmpty(t, file.Docs)

			var key ast.Node
			switch body := file.Docs[0].Body.(type) {
			case *ast.MappingNode:
				require.Len(t, body.Values, 1)
				key = body.Values[0].Key
			case *ast.MappingValueNode:
				key = body.Key
			default:
				t.Fatalf("document body is %T, want a mapping", body)
			}

			assert.Equal(t, tt.want, fmt.Sprintf("%T", key))

			name, ok := keyNameOf(key)
			if tt.spells == "" {
				assert.False(t, ok, "keyNameOf must not read a name from %s", tt.want)
				return
			}
			assert.True(t, ok)
			assert.Equal(t, tt.spells, name)
		})
	}
}

// TestKeyNamePlusDiagnosticMatchesKeyNameOf is the property the two functions
// have to hold: keyName accepts exactly what keyNameOf accepts, and reports
// everything else.
//
// Asserted over the vocabulary above rather than over the two implementations,
// because two switches agreeing today is what the previous version also looked
// like.
func TestKeyNamePlusDiagnosticMatchesKeyNameOf(t *testing.T) {
	t.Parallel()

	srcs := []string{
		"a: 1\n", "\"a\": 1\n", "'a': 1\n", "1: 1\n", "true: 1\n", "~: 1\n",
		"&k a: 1\n", "? a\n: 1\n", "? |-\n  a\n: 1\n",
	}
	for _, src := range srcs {
		file, err := parser.ParseBytes([]byte(src), 0)
		require.NoError(t, err)
		var key ast.Node
		switch body := file.Docs[0].Body.(type) {
		case *ast.MappingNode:
			key = body.Values[0].Key
		case *ast.MappingValueNode:
			key = body.Key
		}

		quiet, quietOK := keyNameOf(key)

		c := &compiler{pos: newPositions()}
		loud, loudOK := c.keyName(key, ref{})

		assert.Equal(t, quietOK, loudOK, "the two disagree about whether %q has a name", src)
		assert.Equal(t, quiet, loud, "the two read different names from %q", src)
		if !loudOK {
			assert.NotEmpty(t, c.diags, "a key with no name must be reported: %q", src)
		}
	}
}

// TestExplicitKeyIsReportedInTheAuthorsWords covers the message itself. "a
// mappingkey was written here" is the parser's word for it, and it says keys must
// be strings about a key that is one.
func TestExplicitKeyIsReportedInTheAuthorsWords(t *testing.T) {
	t.Parallel()

	_, _, err := Parse([]byte("name: t\nsteps:\n  - id: a\n    ? echo\n    : {}\n"))
	require.Error(t, err)

	var ds Diagnostics
	require.ErrorAs(t, err, &ds)
	assert.Contains(t, ds.Error(), "an explicit key (`? key` on its own line) is not written here; use `key: value`")
	assert.NotContains(t, ds.Error(), "mappingkey")
}
