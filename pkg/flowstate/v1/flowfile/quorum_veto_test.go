package flowfile_test

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	v1 "github.com/picatz/flowstate/pkg/flowstate/v1"
	"github.com/picatz/flowstate/pkg/flowstate/v1/flowfile"
)

// TestAQuorumVetoMustBeABoolean pins the static check on `veto:`: it is read as a
// condition, so one that can only ever be another type is refused at validation
// rather than failing the gate at its first delivery. A dynamically typed
// expression is left to the tally, which still refuses a wrong type at run time.
func TestAQuorumVetoMustBeABoolean(t *testing.T) {
	t.Parallel()

	for _, test := range []struct {
		name    string
		veto    string
		wantMsg string
	}{
		{"a string", "${'no'}", "`veto:` is a condition, so this expression must be a bool, but it is typed string"},
		{"an int", "${1 + 1}", "`veto:` is a condition, so this expression must be a bool, but it is typed int"},
		{"a boolean over the payload", "${payload.approved == false}", ""},
		{"a dynamically typed expression", "${payload.verdict}", ""},
		{"a literal boolean", "${true}", ""},
	} {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()

			diagnostics, err := flowfile.ValidateSource([]byte(quorumSource("        approve: 2\n        veto: " + test.veto + "\n")))
			require.NoError(t, err)

			var mismatch bool
			for _, d := range diagnostics {
				if d.Code == v1.DiagnosticCodeTypeMismatch {
					mismatch = true
					assert.Equal(t, "gate", d.Step)
					assert.Contains(t, d.Message, test.wantMsg)
				}
			}
			assert.Equal(t, test.wantMsg != "", mismatch, "diagnostics: %v", diagnostics)
		})
	}
}
