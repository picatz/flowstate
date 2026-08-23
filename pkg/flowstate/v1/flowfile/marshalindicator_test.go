package flowfile_test

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/picatz/flowstate/pkg/flowstate/v1/flowfile"
)

// A scalar that opens with a YAML indicator is the shape #728 was: Marshal
// handed the emitter a bare Go string, the emitter wrote `message: ? 0000000`
// plain, and `? ` reads back as YAML's explicit-key indicator — so the document
// Marshal had just produced did not parse.
//
// That is the failure CLAUDE.md calls the worst thing here, a rewriter
// corrupting a valid file, and it reached a literal value because literals were
// the one scalar position not asked the question every other position asks:
// does the emitter's rendering of this string read back as this string?
//
// The fuzzer found it, which is the second thing worth pinning. FuzzMarshalRoundTrip
// only reaches it when a seed happens to generate a leading indicator, so
// fuzz-smoke failed on some runs of an unchanged tree and passed on others —
// indistinguishable from flake until someone reproduced it. Named cases run
// every time.

// yamlIndicatorScalars open with, or consist of, characters YAML reads as
// structure rather than text. `?` and `-` introduce an explicit key and a block
// sequence entry when followed by a space; `,`, `[`, `]`, `{` and `}` separate
// or open collections in flow context; the rest are reserved or special enough
// that an emitter has to decide about them.
var yamlIndicatorScalars = []string{
	"? 0000000",
	"?",
	"? ",
	"- not a sequence entry",
	"-",
	"- ",
	", not a flow separator",
	",",
	": not a mapping value",
	"# not a comment",
	"& not an anchor",
	"* not an alias",
	"! not a tag",
	"| not a block scalar",
	"> not a folded scalar",
	"% not a directive",
	"@ reserved",
	"` reserved",
	"[ not a sequence",
	"{ not a mapping",
	"--- not a document marker",
	"... not a document end",
}

// TestMarshalledLiteralsWithYAMLIndicatorsParseBack is the regression for #728.
//
// A string literal in a task input is the position that was wrong. It asserts
// the round trip rather than the rendering: whether the emitter quotes, folds or
// escapes is its business, and pinning the bytes would fail on an emitter
// upgrade that is not a bug. What must hold is that reading the output back
// produces the value that went in.
func TestMarshalledLiteralsWithYAMLIndicatorsParseBack(t *testing.T) {
	t.Parallel()

	for _, scalar := range yamlIndicatorScalars {
		t.Run(fmt.Sprintf("%q", scalar), func(t *testing.T) {
			t.Parallel()

			source := "edition: v2026.3\nname: indicators\nsteps:\n" +
				"  - id: shown\n    log:\n      message: " + quoteForYAML(scalar) + "\n"

			workflow, _, err := flowfile.Parse([]byte(source))
			require.NoError(t, err, "the test's own document must parse, or it asserts nothing")

			marshalled, err := flowfile.Marshal(workflow)
			require.NoError(t, err)

			// The property. Before #728 this failed to parse at all for `? `.
			back, _, err := flowfile.Parse(marshalled)
			require.NoError(t, err, "Marshal produced a document that does not parse:\n%s", marshalled)

			require.Equal(t,
				workflow.GetSteps()[0].GetTask().GetInputs()["message"].String(),
				back.GetSteps()[0].GetTask().GetInputs()["message"].String(),
				"the value changed across a round trip:\n%s", marshalled)
		})
	}
}

// quoteForYAML writes a scalar as a double-quoted YAML string, which is how the
// test's *input* document spells it. Marshal's output is the thing under test;
// the input only has to be unambiguous.
func quoteForYAML(s string) string {
	out := []rune{'"'}
	for _, r := range s {
		switch r {
		case '"', '\\':
			out = append(out, '\\', r)
		default:
			out = append(out, r)
		}
	}
	return string(append(out, '"'))
}
