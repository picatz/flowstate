package policycheck_test

import (
	"fmt"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/picatz/flowstate/cmd/flow/internal/policycheck"
)

// The constants a caller names are the schema's own boundaries: one at the
// limit is accepted and one over is refused, so neither can drift from
// proto/flowstate/v1/policy_check.proto unnoticed.
func TestMatrixConstantsMirrorTheSchema(t *testing.T) {
	t.Parallel()

	rows := func(n int) string {
		var b strings.Builder
		b.WriteString("identities:\n")
		for i := range n {
			fmt.Fprintf(&b, "  - name: n%d\n", i)
		}

		return b.String()
	}

	_, err := policycheck.ParseMatrix([]byte(rows(policycheck.MaxMatrixRows)))
	require.NoError(t, err)
	_, err = policycheck.ParseMatrix([]byte(rows(policycheck.MaxMatrixRows + 1)))
	require.Error(t, err)

	name := func(n int) string { return "identities:\n  - name: " + strings.Repeat("a", n) + "\n" }

	_, err = policycheck.ParseMatrix([]byte(name(policycheck.MaxRowNameRunes)))
	require.NoError(t, err)
	_, err = policycheck.ParseMatrix([]byte(name(policycheck.MaxRowNameRunes + 1)))
	require.Error(t, err)
}

func TestMatrixStrayShapesAreRefusedWithoutQuoting(t *testing.T) {
	t.Parallel()

	for name, doc := range map[string]string{
		"a row that is text":       "identities:\n  - just-text\n",
		"identities that is a map": "identities: {name: a}\n",
		"a non-text claim":         "identities:\n  - name: a\n    principal: {claims: {team: [x]}}\n",
	} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()

			_, err := policycheck.ParseMatrix([]byte(doc))
			require.Error(t, err)
			require.NotContains(t, err.Error(), "just-text")
		})
	}
}
