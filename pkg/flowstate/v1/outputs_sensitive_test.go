package flowstatev1_test

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	v1 "github.com/picatz/flowstate/pkg/flowstate/v1"
)

// secretAnswer is the value the workflows below compute. Closed rather than
// read from anywhere, so what these tests assert is a fact about the diagnostic
// rather than about a task or a server.
const secretAnswer = "sk-live-0PENSESAME"

// TestEvalRunOutputsWithholdsASensitiveValueFromItsOwnRefusal is the regression
// for #1396, found by review of #1392.
//
// An output declared `sensitive:` that violates its own declaration used to be
// refused in a sentence quoting the computed value verbatim. That sentence is
// the run's failure text, which is the one surface with none of the redaction
// the value itself gets: [v1.EvalRunOutputs] returns it before there is a
// [v1.RunOutputs] for `cmd/flow`'s `redactRunOutputs` to redact, and that
// package's `redactFailureError` holds only the run's sensitive *inputs*. So a
// token a task produced reached the run's failure text, and from there durable
// history and every surface that reads a failure back out of it — AGENTS.md
// invariant 7 broken by the check written to protect the answer.
//
// Both refusals a sensitive output can earn are covered, because both named the
// value: the enum-membership one #1392 added, and the pre-existing `must:` one
// beside it in the same evaluation.
func TestEvalRunOutputsWithholdsASensitiveValueFromItsOwnRefusal(t *testing.T) {
	t.Parallel()

	for _, test := range []struct {
		name        string
		declaration *v1.OutputDeclaration
		contains    string
	}{
		{
			name: "an enum output outside its declared set",
			declaration: &v1.OutputDeclaration{
				Name:      "token",
				Value:     v1.NewExpr(`"` + secretAnswer + `"`),
				Sensitive: true,
				Type:      v1.InputDeclaration_TYPE_ENUM,
				Values:    []string{"stable", "beta"},
			},
			// The declaration's own choices stay in the sentence: they are
			// written in the file rather than computed by the run, and they are
			// what an author reading the failure needs.
			contains: `output "token" is ` + v1.SensitiveMarker +
				`, which is not one of the values token declares: "stable", "beta"`,
		},
		{
			name: "an output that fails its own must",
			declaration: &v1.OutputDeclaration{
				Name:      "token",
				Value:     v1.NewExpr(`"` + secretAnswer + `"`),
				Sensitive: true,
				Must:      strPtr(`this == "expected"`),
			},
			contains: "output \"token\" must satisfy `this == \"expected\"`; got " + v1.SensitiveMarker,
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()

			wf := &v1.Workflow{Name: "wf", DeclaredOutputs: []*v1.OutputDeclaration{test.declaration}}

			out, err := v1.EvalRunOutputs(t.Context(), wf, v1.NewScope("", &v1.Workflow_StepOutputs{}))
			require.Error(t, err, "an output violating its own declaration must fail the run")
			require.Nil(t, out)
			assert.Contains(t, err.Error(), test.contains)

			// Every containment shape invariant 7 names, because a value kept
			// out of %v can still surface under %+v or %#v — and this error is
			// rendered by all of them on the way to durable history: the engine
			// wraps it, Temporal's converter carries it, and `cmd/flow` prints
			// it with %s.
			for _, verb := range []string{"%v", "%+v", "%#v", "%s"} {
				assert.NotContains(t, fmt.Sprintf(verb, err), secretAnswer,
					"the computed value of a `sensitive:` output appeared under "+verb)
			}
		})
	}
}

// TestEvalRunOutputsNamesAnUnredactedValueInItsRefusal is the other direction,
// which is what keeps the withholding above a redaction rather than a blanket
// loss of the most useful word in the sentence: an output nobody marked
// `sensitive:` is still refused by name and by value, `did you mean` included,
// and so is its `must:`.
func TestEvalRunOutputsNamesAnUnredactedValueInItsRefusal(t *testing.T) {
	t.Parallel()

	for _, test := range []struct {
		name        string
		declaration *v1.OutputDeclaration
		contains    string
	}{
		{
			name: "an enum output outside its declared set",
			declaration: &v1.OutputDeclaration{
				Name:   "channel",
				Value:  v1.NewExpr(`"stabel"`),
				Type:   v1.InputDeclaration_TYPE_ENUM,
				Values: []string{"stable", "beta"},
			},
			contains: `output "channel" is "stabel", which is not one of the values channel ` +
				`declares: "stable", "beta"; did you mean "stable"?`,
		},
		{
			name: "an output that fails its own must",
			declaration: &v1.OutputDeclaration{
				Name:  "channel",
				Value: v1.NewExpr(`"canary"`),
				Must:  strPtr(`this == "stable"`),
			},
			contains: "output \"channel\" must satisfy `this == \"stable\"`; got canary",
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()

			wf := &v1.Workflow{Name: "wf", DeclaredOutputs: []*v1.OutputDeclaration{test.declaration}}

			_, err := v1.EvalRunOutputs(t.Context(), wf, v1.NewScope("", &v1.Workflow_StepOutputs{}))
			require.Error(t, err)
			assert.Contains(t, err.Error(), test.contains)
			assert.NotContains(t, err.Error(), v1.SensitiveMarker,
				"nothing declared this output sensitive, so nothing about it may be withheld")
		})
	}
}

// TestSensitiveEnumOutputEarnsNoSpellingSuggestion is the half of the
// withholding a marker assertion alone would miss.
//
// [v1.SensitiveMarker] in place of the value is not the whole rule: the
// `did you mean` clause beside it is computed *from* that value, and offering
// one narrows a reader's guess to the strings within one repository-wide edit
// distance of a declared choice. So the suggestion goes with the value rather
// than staying to describe it.
//
// The value here is one edit from a declared choice, which is exactly what
// earns a suggestion for the unsensitive output above.
func TestSensitiveEnumOutputEarnsNoSpellingSuggestion(t *testing.T) {
	t.Parallel()

	wf := &v1.Workflow{
		Name: "wf",
		DeclaredOutputs: []*v1.OutputDeclaration{
			{
				Name:      "token",
				Value:     v1.NewExpr(`"stabel"`),
				Sensitive: true,
				Type:      v1.InputDeclaration_TYPE_ENUM,
				Values:    []string{"stable", "beta"},
			},
		},
	}

	_, err := v1.EvalRunOutputs(t.Context(), wf, v1.NewScope("", &v1.Workflow_StepOutputs{}))
	require.Error(t, err)
	assert.NotContains(t, err.Error(), "stabel", "the computed value must not reach the failure text")
	assert.NotContains(t, err.Error(), "did you mean",
		"a suggestion is computed from the value it must not describe")
}

// TestASensitiveOutputsTypeMismatchNamesTypesRatherThanValues pins the third
// refusal [v1.EvalRunOutputs] can produce about a declared output, which #1396
// records as already safe: a type mismatch names the declared type and the
// computed one, both of which are vocabulary rather than data.
//
// Asserted rather than assumed, because "this one happens not to interpolate
// the value" is a property of the sentence's current wording and nothing else
// was holding it.
func TestASensitiveOutputsTypeMismatchNamesTypesRatherThanValues(t *testing.T) {
	t.Parallel()

	wf := &v1.Workflow{
		Name: "wf",
		DeclaredOutputs: []*v1.OutputDeclaration{
			{
				Name:      "token",
				Value:     v1.NewExpr(`"` + secretAnswer + `"`),
				Sensitive: true,
				Type:      v1.InputDeclaration_TYPE_INT,
			},
		},
	}

	_, err := v1.EvalRunOutputs(t.Context(), wf, v1.NewScope("", &v1.Workflow_StepOutputs{}))
	require.Error(t, err)
	assert.Contains(t, err.Error(), `output "token" is declared int but computed string`)
	assert.NotContains(t, err.Error(), secretAnswer)
}
