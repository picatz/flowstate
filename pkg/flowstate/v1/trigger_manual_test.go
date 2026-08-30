package flowstatev1_test

import (
	"fmt"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	v1 "github.com/picatz/flowstate/pkg/flowstate/v1"
)

func manualWorkflow(principals ...string) *v1.Workflow {
	return &v1.Workflow{
		Name: "manual-policy",
		Triggers: &v1.Triggers{Manual: &v1.ManualTrigger{
			AllowedPrincipals: principals,
		}},
	}
}

func TestCheckManualStartMatchesTheWholeQualifiedPrincipal(t *testing.T) {
	t.Parallel()

	workflow := manualWorkflow("https://issuer-a.example.com#runner")
	require.NoError(t, v1.CheckManualStart(workflow, "https://issuer-a.example.com#runner", ""))

	for _, principal := range []string{
		"https://issuer-b.example.com#runner",
		"runner",
		"",
	} {
		err := v1.CheckManualStart(workflow, principal, "")
		require.Error(t, err, "principal %q was admitted", principal)
	}
}

func TestCheckManualTriggerRequiresBoundedQualifiedUniquePrincipals(t *testing.T) {
	t.Parallel()

	tooMany := make([]string, 65)
	for i := range tooMany {
		tooMany[i] = fmt.Sprintf("https://issuer.example.com#runner-%d", i)
	}

	for _, test := range []struct {
		name       string
		principals []string
		want       string
	}{
		{name: "qualified", principals: []string{"https://issuer.example.com#runner"}},
		{name: "empty entry", principals: []string{""}, want: "<issuer>#<subject>"},
		{name: "bare subject", principals: []string{"runner"}, want: "<issuer>#<subject>"},
		{name: "empty issuer", principals: []string{"#runner"}, want: "<issuer>#<subject>"},
		{name: "empty subject", principals: []string{"https://issuer.example.com#"}, want: "<issuer>#<subject>"},
		{name: "duplicate", principals: []string{
			"https://issuer.example.com#runner",
			"https://issuer.example.com#runner",
		}, want: "twice"},
		{name: "too many", principals: tooMany, want: "limit of 64"},
		{name: "entry too long", principals: []string{"https://issuer.example.com#" + strings.Repeat("x", 257)}, want: "256-character"},
	} {
		t.Run(test.name, func(t *testing.T) {
			err := v1.CheckManualTrigger(&v1.ManualTrigger{AllowedPrincipals: test.principals})
			if test.want == "" {
				require.NoError(t, err)
				return
			}
			require.Error(t, err)
			assert.Contains(t, err.Error(), test.want)
		})
	}
}

func TestManualTriggerSchemaRequiresQualifiedUniquePrincipals(t *testing.T) {
	t.Parallel()

	for _, test := range []struct {
		name       string
		principals []string
		wantError  bool
	}{
		{name: "qualified", principals: []string{"https://issuer.example.com#runner"}},
		{name: "bare", principals: []string{"runner"}, wantError: true},
		{name: "malformed", principals: []string{"#runner"}, wantError: true},
		{name: "empty", principals: []string{""}, wantError: true},
		{name: "duplicate", principals: []string{
			"https://issuer.example.com#runner",
			"https://issuer.example.com#runner",
		}, wantError: true},
	} {
		t.Run(test.name, func(t *testing.T) {
			err := v1.Validate(&v1.ManualTrigger{AllowedPrincipals: test.principals})
			if test.wantError {
				require.Error(t, err)
				return
			}
			require.NoError(t, err)
		})
	}
}

func TestCheckManualStartPreservesOpenDeniedAndReasonBehavior(t *testing.T) {
	t.Parallel()

	require.NoError(t, v1.CheckManualStart(&v1.Workflow{Name: "open"}, "", ""),
		"no manual policy remains open")
	require.NoError(t, v1.CheckManualStart(manualWorkflow(), "", ""),
		"an empty allowlist remains open")

	denied := &v1.Workflow{Name: "denied", Triggers: &v1.Triggers{Manual: &v1.ManualTrigger{Denied: true}}}
	assert.ErrorContains(t, v1.CheckManualStart(denied, "https://issuer.example.com#runner", ""), "manual: denied")

	reason := &v1.Workflow{Name: "reason", Triggers: &v1.Triggers{Manual: &v1.ManualTrigger{RequireReason: true}}}
	assert.ErrorContains(t, v1.CheckManualStart(reason, "https://issuer.example.com#runner", " "), "requires a reason")
	require.NoError(t, v1.CheckManualStart(reason, "https://issuer.example.com#runner", "operator approved"))
}
