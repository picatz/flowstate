package policycheck_test

import (
	"fmt"
	"runtime"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/picatz/flowstate/cmd/flow/internal/policycheck"
)

// aliasBomb is nested aliases, ten references per level over nine levels: a few
// hundred bytes that expand to ten to the ninth values if anything follows them.
func aliasBomb() string {
	var b strings.Builder
	b.WriteString("identities:\n  - name: bomb\n    inputs:\n      k0: &a0 [x]\n")
	for i := 1; i < 9; i++ {
		refs := strings.TrimSuffix(strings.Repeat(fmt.Sprintf("*a%d, ", i-1), 10), ", ")
		fmt.Fprintf(&b, "      k%d: &a%d [%s]\n", i, i, refs)
	}

	return b.String()
}

// Anchors, aliases and merge keys are refused on their presence, before a
// decoder follows one, so an alias bomb costs the size of the document.
func TestParseMatrixRefusesAnchorsAliasesAndMergeKeys(t *testing.T) {
	bomb := aliasBomb()
	require.Less(t, len(bomb), 1024, "the bomb is small, which is the point")

	tests := []struct {
		name string
		doc  string
	}{
		{"an alias bomb", bomb},
		{"an anchor", "identities:\n  - name: a\n    principal: {claims: &c {team: dev}}\n"},
		{"an alias", "x: &c {team: dev}\nidentities:\n  - name: a\n    principal: {claims: *c}\n"},
		{"a merge key", "base: &b {team: dev}\nidentities:\n  - name: a\n    principal:\n      claims:\n        <<: *b\n"},
	}

	// Serial: the allocation bound reads process-wide counters.
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var before, after runtime.MemStats
			runtime.ReadMemStats(&before)

			_, err := policycheck.ParseMatrix([]byte(tt.doc))

			runtime.ReadMemStats(&after)

			require.ErrorContains(t, err, "anchors (&), aliases (*) and merge keys (<<) are not accepted")
			require.Regexp(t, `line \d+, column \d+`, err.Error())

			// By construction rather than by waiting: a decoder that followed
			// the aliases would allocate gigabytes before it returned.
			require.Less(t, after.TotalAlloc-before.TotalAlloc, uint64(64<<20),
				"refusing the construct allocated like decoding it")
		})
	}
}

func TestParseMatrixBoundsWhatARowsInputsHold(t *testing.T) {
	t.Parallel()

	items := strings.Repeat("1, ", policycheck.MaxRowInputNodes)
	_, err := policycheck.ParseMatrix([]byte("identities:\n  - name: a\n    inputs: {k: [" + items + "1]}\n"))
	require.ErrorContains(t, err, "more than")

	deep := strings.Repeat("[", 40) + strings.Repeat("]", 40)
	_, err = policycheck.ParseMatrix([]byte("identities:\n  - name: a\n    inputs: {k: " + deep + "}\n"))
	require.ErrorContains(t, err, "more than")

	_, err = policycheck.ParseMatrix([]byte("identities:\n  - name: a\n    inputs: {k: [1, 2, {a: b}]}\n"))
	require.NoError(t, err)
}

// A decode error says what and where, and never the line it is about: in a
// matrix that line can be a claim or an input value.
func TestParseMatrixErrorsQuoteNoSource(t *testing.T) {
	t.Parallel()

	const secret = "SECRET-claim-value-42"

	docs := map[string]string{
		"an unknown key beside a claim":  "identities:\n  - name: a\n    expct: admitted\n    principal: {claims: {team: " + secret + "}}\n",
		"a type error beside a claim":    "identities:\n  - name: a\n    principal:\n      subject: [" + secret + "]\n      claims: {team: " + secret + "}\n",
		"a type error in the value":      "identities:\n  - name: a\n    principal: {claims: " + secret + "}\n",
		"a bad outcome beside an input":  "identities:\n  - name: a\n    inputs: {pin: " + secret + "}\n    expect: " + secret + "\n",
		"a syntax error beside a secret": "identities:\n  - name: a\n    principal: {claims: {team: " + secret + "\n  - : :\n",
	}

	for name, doc := range docs {
		t.Run(name, func(t *testing.T) {
			t.Parallel()

			_, err := policycheck.ParseMatrix([]byte(doc))
			require.Error(t, err)
			require.NotContains(t, err.Error(), secret)
			require.NotContains(t, err.Error(), "claims: {", "the source line is not echoed")
		})
	}
}
