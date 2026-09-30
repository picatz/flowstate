package server

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	v1 "github.com/picatz/flowstate/pkg/flowstate/v1"
)

// subjectGatedWorkflow gates a signal on a subject read from its `approver`
// input, declared sensitive or not.
func subjectGatedWorkflow(sensitive bool) *v1.Workflow {
	return &v1.Workflow{
		Name:           "gated",
		DeclaredInputs: []*v1.InputDeclaration{{Name: "approver", Type: v1.InputDeclaration_TYPE_STRING, Sensitive: sensitive}},
		Signals: map[string]*v1.SignalPolicy{"approved": {
			DistinctFromStarter: true,
			Allow:               []*v1.SignalPolicyRule{{SubjectFrom: v1.NewExpr("inputs.approver")}},
		}},
	}
}

// TestAWithheldPolicyRefusalTakesOutWhatTheSpecificationDeclaresSensitive is
// #2100 at the server: a rule's `subject:` read from an input and resolved to
// something that is not `<issuer>#<subject>` is refused quoting what it
// resolved to, and [withheldPolicyRefusal] takes the value out when it is
// asked to withhold an input the specification declares sensitive. An input nobody declared
// sensitive is still quoted, so the refusal stays diagnosable.
func TestAWithheldPolicyRefusalTakesOutWhatTheSpecificationDeclaresSensitive(t *testing.T) {
	t.Parallel()

	const approver = `approver-"lead"@corp.example`
	inputs := map[string]*v1.Value{"approver": v1.NewLiteral(approver)}
	for _, sensitive := range []bool{true, false} {
		wf := subjectGatedWorkflow(sensitive)
		_, refusal := policyMemoEntries(t.Context(), wf, inputs)
		require.Error(t, refusal, "a bare subject is refused")
		require.Contains(t, refusal.Error(), "lead", "the resolver's refusal no longer quotes the value, so this proves nothing")

		err := withheldPolicyRefusal(refusal, v1.SensitiveInputNames(wf), inputs)
		require.Contains(t, err.Error(), "<issuer>#<subject>", "the refusal no longer says what was wrong")
		if sensitive {
			assert.NotContains(t, err.Error(), "lead", "the refusal quotes an input the specification declares sensitive")
			assert.Contains(t, err.Error(), v1.SensitiveMarker)

			continue
		}
		assert.Contains(t, err.Error(), "lead", "an ordinary input's refusal lost the value it is about")
	}
}

// TestOnlyWhatTheSubmitterCannotKnowIsUnknown: a refusal leaves to the client
// every input its own file declares sensitive and whose value it holds, since
// the client redacts, or reveals, those: a value it sent, or its file's
// default when that is the default the run binds. What a deployment-owned
// copy alone declares, or a default of the copy's the caller never held, is
// the server's to withhold, and nothing is when no copy replaced the file.
func TestOnlyWhatTheSubmitterCannotKnowIsUnknown(t *testing.T) {
	t.Parallel()

	defaulted := func(sensitive bool, value string) *v1.Workflow {
		wf := subjectGatedWorkflow(sensitive)
		wf.DeclaredInputs[0].Default = v1.NewLiteral(value)
		return wf
	}
	sent := map[string]*v1.Value{"approver": v1.NewLiteral("sent")}

	assert.Empty(t, unknownSensitiveInputs(subjectGatedWorkflow(true), subjectGatedWorkflow(false), sent, false),
		"a run of the submitted workflow withheld at the server")
	assert.Empty(t, unknownSensitiveInputs(subjectGatedWorkflow(true), subjectGatedWorkflow(true), sent, true),
		"an input the submitter's file declares sensitive and sent was withheld at the server")
	assert.Empty(t, unknownSensitiveInputs(defaulted(true, "same"), defaulted(true, "same"), nil, true),
		"a default the submitter's file shares was withheld at the server")
	assert.Empty(t, unknownSensitiveInputs(defaulted(true, "deployment's"), defaulted(true, "caller's"), sent, true),
		"a value the caller sent was withheld at the server because the copies' defaults differ")
	assert.Equal(t, map[string]bool{"approver": true},
		unknownSensitiveInputs(subjectGatedWorkflow(true), subjectGatedWorkflow(false), sent, true))
	assert.Equal(t, map[string]bool{"approver": true},
		unknownSensitiveInputs(defaulted(true, "deployment's"), defaulted(true, "caller's"), nil, true),
		"a deployment-owned default the caller never held was left to the client")
}
