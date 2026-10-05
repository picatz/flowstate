package policycheck_test

import (
	"fmt"
	"runtime"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/picatz/flowstate/cmd/flow/internal/policycheck"
)

// Depth is refused on the bytes, before a parser builds a tree: goccy's parser
// recurses per level and a few hundred kilobytes of `[` exhaust memory before any
// bound on the tree could run. The sizes here are the smallest that cross a cap,
// so a regression that hands the document to the parser costs the parser little.
func TestParseMatrixRefusesDeepNestingBeforeParsing(t *testing.T) {
	// Serial: the allocation bound reads process-wide counters.
	tests := map[string]string{
		"flow sequences": "identities:\n  - name: a\n    inputs: {a: " + strings.Repeat("[", policycheck.MaxFlowOpeners+1) + "}\n",
		"flow mappings":  "identities:\n  - name: a\n    inputs: " + strings.Repeat("{a: ", policycheck.MaxFlowOpeners+1) + "\n",
		"block mappings": "identities:\n  - name: a\n    inputs:\n      k: " + strings.Repeat("a: ", policycheck.MaxBlockTokens+1) + "\n",
		"block sequences": "identities:\n  - name: a\n    inputs:\n      k:\n        " +
			strings.Repeat("- ", policycheck.MaxBlockTokens+1) + "x\n",
		"indentation": "identities:\n  - name: a\n" + strings.Repeat(" ", policycheck.MaxBlockIndent+1) + "x: 1\n",
	}

	for name, doc := range tests {
		t.Run(name, func(t *testing.T) {
			var before, after runtime.MemStats
			runtime.ReadMemStats(&before)

			_, err := policycheck.ParseMatrix([]byte(doc))

			runtime.ReadMemStats(&after)

			require.ErrorContains(t, err, "needs none of that nesting")
			require.Regexp(t, `line \d+, column \d+`, err.Error())
			require.Less(t, after.TotalAlloc-before.TotalAlloc, uint64(8<<20),
				"the refusal allocated like parsing the document")
		})
	}
}

// Brackets count wherever they are, so a comment or a quote cannot hide them,
// and a few of them are nothing.
func TestParseMatrixCountsBracketsInCommentsAndQuotes(t *testing.T) {
	t.Parallel()

	many := strings.Repeat("[", policycheck.MaxFlowOpeners+1)
	few := strings.Repeat("[", 200)

	for name, doc := range map[string]string{
		"a comment":      "identities:\n  - name: a # %s\n",
		"double quoted":  "identities:\n  - name: a\n    claims: {team: \"%s\"}\n",
		"single quoted":  "identities:\n  - name: a\n    claims: {team: '%s'}\n",
		"a block scalar": "identities:\n  - name: a\n    claims:\n      team: |\n        %s\n",
	} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()

			_, err := policycheck.ParseMatrix([]byte(strings.ReplaceAll(doc, "%s", many)))
			require.ErrorContains(t, err, "too many flow collections")

			_, err = policycheck.ParseMatrix([]byte(strings.ReplaceAll(doc, "%s", few)))
			require.NoError(t, err)
		})
	}

	_, err := policycheck.ParseMatrix([]byte("identities:\n  - name: a\n    inputs: {k: [[[[[[[[1]]]]]]]]}\n"))
	require.NoError(t, err)
}

// A matrix at the realistic maximum, with nested inputs, claims, a starter and
// per-gate expectations in every row, must pass: the caps refuse nesting no table
// needs, not tables.
func TestParseMatrixAcceptsRealisticTablesAtTheLimits(t *testing.T) {
	t.Parallel()

	for _, rows := range []int{20, policycheck.MaxMatrixRows} {
		var b strings.Builder
		b.WriteString("identities:\n")

		for i := range rows {
			fmt.Fprintf(&b, `  - name: row-%d
    subject: "user:%d"
    issuer: https://issuer.example
    namespace: ops
    claims: {team: sre, role: admin, org: platform}
    starter: {subject: "user:%d", issuer: https://issuer.example, claims: {team: sre}}
    inputs:
      env: prod
      targets: [{name: a, ports: [80, 443]}, {name: b, ports: [8080]}]
      limits: {cpu: 1.5, mem: {max: 4096}}
    expect_by_gate: {debug: admitted, "signals.approve": refused}
`, i, i, i)
		}

		m, err := policycheck.ParseMatrix([]byte(b.String()))
		require.NoError(t, err, "%d rows", rows)
		require.Len(t, m.Identities, rows)
	}
}

// Nesting that is only block structure, at a depth no cap refuses, still parses.
func TestParseMatrixAcceptsModestBlockNesting(t *testing.T) {
	t.Parallel()

	doc := "identities:\n  - name: a\n    inputs:\n      a:\n        b:\n          c:\n            - d: 1\n              e: [1, {f: 2}]\n"
	_, err := policycheck.ParseMatrix([]byte(doc))
	require.NoError(t, err)
}

// A depth the scan refuses stays refused end to end at a size the parser could
// survive even if the scan failed, so a regression here costs little.
func TestParseMatrixRefusesModerateFlowDepthEndToEnd(t *testing.T) {
	// Serial: bounded by allocation counters.
	doc := "identities:\n  - name: a\n    inputs: {a: " + strings.Repeat("[", 5000) + "}\n"

	_, err := policycheck.ParseMatrix([]byte(doc))
	require.ErrorContains(t, err, "too many flow collections")
}

// A number the schema's double would round is refused, whole or not, with a
// sentence that does not quote it.
func TestParseMatrixRefusesNumbersItCannotCarryExactly(t *testing.T) {
	t.Parallel()

	for name, value := range map[string]string{
		"just past 2^53":            "9007199254740993",
		"2^53 itself":               "9007199254740992",
		"negative":                  "-9007199254740993",
		"nested":                    "[1, {x: 9007199254740993}]",
		"a float of 1.0e16":         "1.0e16",
		"a huge float":              "1.0e300",
		"a negative float":          "-1.0e16",
		"a float spelled with dots": "10000000000000000.0",
		"a float spelled past 2^53": "9.007199254740993e15",
	} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()

			_, err := policycheck.ParseMatrix([]byte("identities:\n  - name: a\n    inputs: {n: " + value + "}\n"))
			require.ErrorContains(t, err, "2^53")
			require.NotContains(t, err.Error(), "9007199254740")
			require.NotContains(t, err.Error(), "e16")
			require.NotContains(t, err.Error(), "e300")
			require.NotContains(t, err.Error(), "0000000")
		})
	}

	for _, value := range []string{"9007199254740991", "9.0e15", "9e15", "1.5", "-9007199254740991", "0", "1e3"} {
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

// The YAML reader gives `1e16`, with no dot, as text, and the engine reads an
// input the same way, so the check holds the value the engine would: a string,
// which carries no rounding to refuse.
func TestParseMatrixReadsExponentWithoutDotAsText(t *testing.T) {
	t.Parallel()

	for _, value := range []string{"1e16", "1e300", "-1e16"} {
		m, err := policycheck.ParseMatrix([]byte("identities:\n  - name: a\n    inputs: {n: " + value + "}\n"))
		require.NoError(t, err, value)
		require.Equal(t, value, m.Identities[0].Inputs["n"], value)
	}
}
