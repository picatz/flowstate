package flowfile_test

import (
	"fmt"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	expr "google.golang.org/genproto/googleapis/api/expr/v1alpha1"

	v1 "github.com/picatz/flowstate/pkg/flowstate/v1"
	"github.com/picatz/flowstate/pkg/flowstate/v1/flowfile"
)

// One depth bound for an authored literal, in every position an author can
// write a mapping (#1765). An input's `default:` refused a literal 33 levels
// deep with a sentence naming why; the same literal under `vars:` or a step
// `value:` compiled to a CEL map literal, which no structure bound saw, and
// ran at 63 — refused only at 64 by the YAML parser's own depth, with a message
// about what a Flowfile is "meant" to be.

// nestedMapping writes `{k: {k: … leaf}}` depth levels deep as flow-style YAML.
func nestedMapping(depth int) string {
	value := `"leaf"`
	for range depth {
		value = "{k: " + value + "}"
	}
	return value
}

// positions are the three places the issue's table measured.
var positions = []struct {
	name   string
	source func(literal string) string
}{
	{"an input's default", func(literal string) string {
		return "edition: v2026.3\nname: t\ninputs:\n  doc:\n    type: struct\n    default: " + literal +
			"\nsteps:\n  - id: a\n    log:\n      message: hi\n"
	}},
	{"a vars entry", func(literal string) string {
		return "edition: v2026.3\nname: t\nvars:\n  d: " + literal + "\nsteps:\n  - id: a\n    log:\n      message: hi\n"
	}},
	{"a step value", func(literal string) string {
		return "edition: v2026.3\nname: t\nsteps:\n  - id: a\n    value: " + literal + "\n"
	}},
}

// TestALiteralPastTheDepthBoundIsRefusedInEveryPosition: depth 33 is refused
// wherever it is written, with the one sentence, at a position.
func TestALiteralPastTheDepthBoundIsRefusedInEveryPosition(t *testing.T) {
	t.Parallel()

	sentence := fmt.Sprintf("nests %d levels deep, over the %d levels this server can walk cheaply while "+
		"evaluating an expression over it", v1.MaxStructureDepth+1, v1.MaxStructureDepth)

	for _, position := range positions {
		t.Run(position.name, func(t *testing.T) {
			t.Parallel()

			diagnostics, err := flowfile.ValidateSource([]byte(position.source(nestedMapping(v1.MaxStructureDepth + 1))))
			var refused string
			switch {
			case err != nil:
				refused = err.Error()
			case len(diagnostics) > 0:
				refused = diagnostics.Error()
			}
			require.NotEmpty(t, refused, "a literal nested %d deep was admitted under %s", v1.MaxStructureDepth+1, position.name)
			assert.Contains(t, refused, sentence, "the refusal under %s does not read like the input default's", position.name)
			assert.NotContains(t, refused, "meant to go", "the YAML parser's depth answered where the value bound should have")
		})
	}
}

// TestALiteralAtTheDepthBoundCompilesInEveryPosition: depth 32 compiles
// wherever it is written, and the compiled specification's deepest literal is
// exactly 32.
func TestALiteralAtTheDepthBoundCompilesInEveryPosition(t *testing.T) {
	t.Parallel()

	for _, position := range positions {
		t.Run(position.name, func(t *testing.T) {
			t.Parallel()

			source := position.source(nestedMapping(v1.MaxStructureDepth))
			workflow, _, err := flowfile.Parse([]byte(source))
			require.NoError(t, err, "a literal at the bound was refused under %s", position.name)

			diagnostics, err := flowfile.ValidateSource([]byte(source))
			require.NoError(t, err)
			require.Empty(t, diagnostics, "a literal at the bound drew a diagnostic under %s", position.name)

			deepest := 0
			v1.WalkWorkflow(workflow, v1.Walk{Value: func(site v1.ValueSite) {
				if d := literalDepth(site.Value.GetLiteral(), 0); d > deepest {
					deepest = d
				}
			}})
			assert.Equal(t, v1.MaxStructureDepth, deepest, "the compiled literal is not %d deep under %s", v1.MaxStructureDepth, position.name)
		})
	}
}

// literalDepth counts map and list levels in a CEL literal.
func literalDepth(v *expr.Value, depth int) int {
	switch kind := v.GetKind().(type) {
	case *expr.Value_MapValue:
		deepest := depth + 1
		for _, entry := range kind.MapValue.GetEntries() {
			if d := literalDepth(entry.GetValue(), depth+1); d > deepest {
				deepest = d
			}
		}
		return deepest
	case *expr.Value_ListValue:
		deepest := depth + 1
		for _, element := range kind.ListValue.GetValues() {
			if d := literalDepth(element, depth+1); d > deepest {
				deepest = d
			}
		}
		return deepest
	}
	return depth
}

// TestTheParsersOwnDepthNoLongerSpeaksForTheFlowfile: past the YAML parser's
// own bound the message says what it bounds — the document this parser reads —
// rather than what a Flowfile is meant to be, which the value bound above now
// answers on its own terms.
func TestTheParsersOwnDepthNoLongerSpeaksForTheFlowfile(t *testing.T) {
	t.Parallel()

	_, _, err := flowfile.Parse([]byte("edition: v2026.3\nname: t\nvars:\n  d: " + nestedMapping(70) +
		"\nsteps:\n  - id: a\n    log:\n      message: hi\n"))
	require.Error(t, err)
	assert.Contains(t, err.Error(), "levels of YAML deep")
	assert.False(t, strings.Contains(err.Error(), "meant to go"))
}
