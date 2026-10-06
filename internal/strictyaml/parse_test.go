package strictyaml_test

import (
	"errors"
	"runtime"
	"strings"
	"testing"

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
