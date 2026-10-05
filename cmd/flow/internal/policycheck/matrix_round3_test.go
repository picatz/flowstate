package policycheck_test

import (
	"runtime"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/picatz/flowstate/cmd/flow/internal/policycheck"
)

func nestedBlock(levels int) string {
	var b strings.Builder
	b.WriteString("identities:\n  - name: a\n    inputs:\n")
	for i := range levels {
		b.WriteString(strings.Repeat(" ", 6+i))
		b.WriteString("k:\n")
	}

	return b.String()
}

// Depth is refused on the bytes, before a parser builds a tree: goccy's parser
// recurses per level and a few hundred kilobytes of `[` exhaust memory before any
// bound on the tree could run.
func TestParseMatrixRefusesDeepNestingBeforeParsing(t *testing.T) {
	// Serial: the allocation bound reads process-wide counters.
	tests := map[string]string{
		"deep flow sequences": "identities:\n  - name: a\n    inputs: {a: " + strings.Repeat("[", 40000) + "}\n",
		"deep flow mappings":  "identities:\n  - name: a\n    inputs: " + strings.Repeat("{a: ", 40000) + "\n",
		"deep block mappings": nestedBlock(policycheck.MaxNesting + 8),
		"deep block sequences": "identities:\n  - name: a\n    inputs:\n      k:\n        " +
			strings.Repeat("- ", policycheck.MaxNesting+8) + "x\n",
	}

	for name, doc := range tests {
		t.Run(name, func(t *testing.T) {
			var before, after runtime.MemStats
			runtime.ReadMemStats(&before)

			_, err := policycheck.ParseMatrix([]byte(doc))

			runtime.ReadMemStats(&after)

			require.ErrorContains(t, err, "nests more than")
			require.Regexp(t, `line \d+, column \d+`, err.Error())
			require.Less(t, after.TotalAlloc-before.TotalAlloc, uint64(8<<20),
				"the refusal allocated like parsing the document")
		})
	}
}

// What the scan must not take for nesting: brackets in quoted text and comments,
// and a document that nests as deep as the limit.
func TestParseMatrixDepthScanIgnoresQuotedAndCommentedBrackets(t *testing.T) {
	t.Parallel()

	brackets := strings.Repeat("[", 200)

	for name, doc := range map[string]string{
		"double quoted":    "identities:\n  - name: a\n    claims: {team: \"" + brackets + "\"}\n",
		"single quoted":    "identities:\n  - name: a\n    claims: {team: '" + brackets + "'}\n",
		"a comment":        "identities:\n  - name: a # " + brackets + "\n",
		"an escaped quote": "identities:\n  - name: a\n    claims: {team: \"\\\"" + brackets + "\"}\n",
		"balanced flow":    "identities:\n  - name: a\n    inputs: {k: [[[[[[[[1]]]]]]]]}\n",
	} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()

			_, err := policycheck.ParseMatrix([]byte(doc))
			require.NoError(t, err)
		})
	}
}

// A whole number the schema's double would round is refused, with a sentence
// that does not quote it.
func TestParseMatrixRefusesIntegersItCannotCarryExactly(t *testing.T) {
	t.Parallel()

	for name, value := range map[string]string{
		"just past 2^53": "9007199254740993",
		"2^53 itself":    "9007199254740992",
		"negative":       "-9007199254740993",
		"nested":         "[1, {x: 9007199254740993}]",
	} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()

			_, err := policycheck.ParseMatrix([]byte("identities:\n  - name: a\n    inputs: {n: " + value + "}\n"))
			require.ErrorContains(t, err, "2^53")
			require.NotContains(t, err.Error(), "9007199254740")
		})
	}

	for _, value := range []string{"9007199254740991", "1.5", "-9007199254740991", "0", "1e3"} {
		_, err := policycheck.ParseMatrix([]byte("identities:\n  - name: a\n    inputs: {n: " + value + "}\n"))
		require.NoError(t, err, value)
	}
}

func TestParseMatrixIsOneDocument(t *testing.T) {
	t.Parallel()

	_, err := policycheck.ParseMatrix([]byte("identities:\n  - name: x\n---\nidentities:\n  - name: y\n"))
	require.ErrorContains(t, err, "one document")

	_, err = policycheck.ParseMatrix([]byte("identities:\n  - name: x\n"))
	require.NoError(t, err)
}

func TestParseMatrixRefusesFormatCharactersInANameButNotLetters(t *testing.T) {
	t.Parallel()

	for name, row := range map[string]string{
		"a bidi override":   "\"a\\u202Eb\"",
		"a zero-width join": "\"a\\u200Db\"",
		"a C1 control":      "\"a\\u009Bb\"",
		"an escape":         "\"a\\u001Bb\"",
	} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()

			_, err := policycheck.ParseMatrix([]byte("identities:\n  - name: " + row + "\n"))
			require.ErrorContains(t, err, "identities[0].name")
		})
	}

	_, err := policycheck.ParseMatrix([]byte("identities:\n  - name: \"Zoë-sre\"\n"))
	require.NoError(t, err)
}
