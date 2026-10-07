package flowstatev1_test

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	v1 "github.com/picatz/flowstate/pkg/flowstate/v1"
)

func TestIdentityClaimReads(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		src  string
		want []string
	}{
		{"a field read on a task, egress, exec, secret or assumption rule", `identity.claims.team == "a"`, []string{"team"}},
		{"an index by literal", `identity.claims["realm_access.roles"] == "x"`, []string{"realm_access.roles"}},
		{"membership in a list claim", `"sre" in identity.claims.groups`, []string{"groups"}},
		{"membership of a name in the map", `"team" in identity.claims`, []string{"team"}},
		{"has()", `has(identity.claims.team) && identity.claims.slack.user == "u"`, []string{"slack", "team"}},
		{"the sender of a signals, debug or manual predicate", `sender.identity.claims.team == "ops"`, []string{"team"}},
		{"the starter of a run", `sender.identity.claims.team == run.identity.claims.team`, []string{"team"}},
		{"several, sorted and without repeats", `identity.claims.b == "1" || identity.claims.a == "2" || identity.claims.b == "3"`, []string{"a", "b"}},
		{"a computed key names nothing", `identity.claims[inputs.which] == "x"`, nil},
		{"claims passed whole name nothing", `size(identity.claims) > 0`, nil},
		{"another map's claims are not the caller's", `inputs.claims.team == "x" && request.claims.team == "x"`, nil},
		{"a local named identity is not the scope", `[{"claims": {"team": "x"}}].exists(identity, identity.claims.team == "x")`, nil},
		{"no identity", `task == "log"`, nil},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()

			got, err := v1.IdentityClaimReads(test.src)
			require.NoError(t, err)
			assert.Equal(t, test.want, got)
		})
	}

	_, err := v1.IdentityClaimReads(`identity.claims.team ==`)
	require.Error(t, err)
}

func TestWorkflowIdentityExpressions(t *testing.T) {
	t.Parallel()

	wf := &v1.Workflow{
		Signals: map[string]*v1.SignalPolicy{
			"b": {Allow: `sender.identity.claims.team == "b"`},
			"a": {Allow: `sender.identity.claims.team == "a"`},
			"c": {},
		},
		Debug:    &v1.SignalPolicy{Allow: `sender.identity.claims.team == "sre"`},
		Triggers: &v1.Triggers{Manual: &v1.ManualTrigger{Allow: `sender.identity.claims.team == "ops"`}},
	}

	got := v1.WorkflowIdentityExpressions(wf)
	wheres := make([]string, len(got))
	for i, expression := range got {
		wheres[i] = expression.Where
	}
	assert.Equal(t, []string{"signals.a.allow", "signals.b.allow", "debug.allow", "triggers.manual.allow"}, wheres)
}
