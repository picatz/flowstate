package flowstatev1_test

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	v1 "github.com/picatz/flowstate/pkg/flowstate/v1"
)

// manualWorkflow is a workflow whose `manual:` block admits only these
// principals, the predicate `manual: allow: ${sender.identity.principal in
// [...]}` writes. No principals is an open workflow, as with no `manual:`.
func manualWorkflow(principals ...string) *v1.Workflow {
	if len(principals) == 0 {
		return &v1.Workflow{Name: "manual-policy"}
	}

	quoted := make([]string, len(principals))
	for i, principal := range principals {
		quoted[i] = `"` + principal + `"`
	}

	return &v1.Workflow{
		Name: "manual-policy",
		Triggers: &v1.Triggers{Manual: &v1.ManualTrigger{
			Allow: `sender.identity.principal in [` + strings.Join(quoted, ", ") + `]`,
		}},
	}
}

func TestCheckManualStartMatchesTheWholeQualifiedPrincipal(t *testing.T) {
	t.Parallel()

	workflow := manualWorkflow("https://issuer-a.example.com#runner")

	caller := manualCaller("https://issuer-a.example.com", "runner", nil)
	require.NoError(t, v1.CheckManualStart(t.Context(), workflow, caller, "https://issuer-a.example.com#runner", "", nil))

	for name, other := range map[string]struct {
		issuer, subject string
	}{
		"the same subject from another issuer": {"https://issuer-b.example.com", "runner"},
		"a bare subject":                       {"", "runner"},
		"another subject":                      {"https://issuer-a.example.com", "someone-else"},
	} {
		caller := manualCaller(other.issuer, other.subject, nil)
		err := v1.CheckManualStart(t.Context(), workflow, caller, v1.QualifiedSubject(other.issuer, other.subject), "", nil)
		require.Error(t, err, "%s was admitted", name)
	}
}

// TestCheckManualStartRefusesAnAnonymousCallerWhateverThePredicateSays is the
// fail-closed line for a caller nobody authenticated: a predicate such as
// `principal != "x"` would otherwise admit nobody-at-all, so a start with no
// principal is refused before the predicate is consulted.
func TestCheckManualStartRefusesAnAnonymousCallerWhateverThePredicateSays(t *testing.T) {
	t.Parallel()

	for name, expression := range map[string]string{
		"a list naming someone":                           `sender.identity.principal in ["https://issuer.example.com#runner"]`,
		"a comparison that is true of an empty principal": `sender.identity.principal != "https://issuer.example.com#runner"`,
		"a predicate that is always true":                 `sender.identity.principal == sender.identity.principal`,
	} {
		workflow := &v1.Workflow{Name: "manual-policy", Triggers: &v1.Triggers{Manual: &v1.ManualTrigger{Allow: expression}}}

		for _, caller := range []*v1.WorkloadIdentity{nil, {}, {Principal: &v1.Principal{Issuer: "https://issuer.example.com", Subject: "runner"}}} {
			err := v1.CheckManualStart(t.Context(), workflow, caller, "", "", nil)
			require.Error(t, err, "%s admitted an anonymous start", name)
			assert.Contains(t, err.Error(), "anonymous", name)
		}
	}
}

func TestCheckManualTriggerRefusesAnUnusablePredicate(t *testing.T) {
	t.Parallel()

	for name, expression := range map[string]string{
		"does not parse":          `sender.identity.principal in [`,
		"is not a bool":           `sender.identity.principal`,
		"reads outside the scope": `run.identity.principal == "x"`,
		"reads inputs unnarrowed": `inputs.who == "x"`,
		"names an unknown field":  `sender.identity.bogus == "x"`,
	} {
		err := v1.CheckManualTrigger(&v1.ManualTrigger{Allow: expression})
		require.Error(t, err, name)
		assert.Contains(t, err.Error(), "manual.allow", name)
	}

	require.NoError(t, v1.CheckManualTrigger(&v1.ManualTrigger{Allow: `sender.identity.principal in ["https://issuer.example.com#runner"]`}))
}

func TestManualTriggerSchemaBoundsThePredicate(t *testing.T) {
	t.Parallel()

	require.NoError(t, v1.Validate(&v1.ManualTrigger{Allow: `sender.identity.principal == "https://issuer.example.com#runner"`}))
	require.Error(t, v1.Validate(&v1.ManualTrigger{Allow: strings.Repeat("a", 2049)}),
		"the source of a predicate is bounded where it is stored")
}

func TestCheckManualStartPreservesOpenDeniedAndReasonBehavior(t *testing.T) {
	t.Parallel()

	require.NoError(t, v1.CheckManualStart(t.Context(), &v1.Workflow{Name: "open"}, nil, "", "", nil),
		"no manual policy remains open")
	require.NoError(t, v1.CheckManualStart(t.Context(), manualWorkflow(), nil, "", "", nil),
		"a workflow naming no one remains open")

	denied := &v1.Workflow{Name: "denied", Triggers: &v1.Triggers{Manual: &v1.ManualTrigger{Denied: true}}}
	assert.ErrorContains(t, v1.CheckManualStart(t.Context(), denied, nil, "https://issuer.example.com#runner", "", nil), "manual: denied")

	reason := &v1.Workflow{Name: "reason", Triggers: &v1.Triggers{Manual: &v1.ManualTrigger{RequireReason: true}}}
	assert.ErrorContains(t, v1.CheckManualStart(t.Context(), reason, nil, "https://issuer.example.com#runner", " ", nil), "requires a reason")
	require.NoError(t, v1.CheckManualStart(t.Context(), reason, nil, "https://issuer.example.com#runner", "operator approved", nil))
}
