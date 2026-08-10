package flowstatev1

import (
	"slices"
	"testing"

	"buf.build/gen/go/bufbuild/protovalidate/protocolbuffers/go/buf/validate"
	"github.com/stretchr/testify/assert"
)

// TestExclusiveBoundsKeepTheirWords pins the difference between gt and gte at
// the surface an author reads: a field constrained `gt: 0` refuses zero, so
// describing it as "at least 0" would teach the one value validation rejects.
func TestExclusiveBoundsKeepTheirWords(t *testing.T) {
	t.Parallel()

	rules := func(r *validate.Int32Rules) *validate.FieldRules {
		return &validate.FieldRules{Type: &validate.FieldRules_Int32{Int32: r}}
	}

	for _, test := range []struct {
		name  string
		rules *validate.FieldRules
		want  []string
	}{
		{
			name:  "gt is more than",
			rules: rules(&validate.Int32Rules{GreaterThan: &validate.Int32Rules_Gt{Gt: 0}}),
			want:  []string{"more than 0"},
		},
		{
			name:  "lt is less than",
			rules: rules(&validate.Int32Rules{LessThan: &validate.Int32Rules_Lt{Lt: 10}}),
			want:  []string{"less than 10"},
		},
		{
			name: "inclusive pair reads as a range",
			rules: rules(&validate.Int32Rules{
				GreaterThan: &validate.Int32Rules_Gte{Gte: 100},
				LessThan:    &validate.Int32Rules_Lte{Lte: 599},
			}),
			want: []string{"100 to 599"},
		},
		{
			name: "a mixed pair keeps each endpoint's own words",
			rules: rules(&validate.Int32Rules{
				GreaterThan: &validate.Int32Rules_Gt{Gt: 0},
				LessThan:    &validate.Int32Rules_Lte{Lte: 100},
			}),
			want: []string{"more than 0", "at most 100"},
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()

			assert.Equal(t, test.want, numericRangePhrases(test.rules))
		})
	}
}

// TestACredentialInputIsNotCalledSecretCapable is the negative direction of the
// secret note: `credential` needs the identity-aware activity for JIT exchange,
// but its value is a literal target name and the task refuses a secret
// reference there, so the note must not invite one.
func TestACredentialInputIsNotCalledSecretCapable(t *testing.T) {
	t.Parallel()

	def, ok := LookupTask("http")
	if !ok {
		t.Fatal("the http task is not registered")
	}

	notes := taskInputNotes(def)
	assert.NotContains(t, notes["credential"], "may hold a secret reference",
		"an author following that note writes the one spelling execution refuses")
	assert.Contains(t, notes["credential"], "names a deployment credential target")
	assert.Contains(t, notes["bearer"], "may hold a secret reference",
		"the input that genuinely takes a reference must keep saying so")

	for _, name := range def.CredentialInputs {
		assert.False(t, slices.Contains(notes[name], "may hold a secret reference"),
			"credential input %q is described as secret-capable", name)
	}
}
