package server_test

import (
	"testing"

	"connectrpc.com/connect"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	v1 "github.com/picatz/flowstate/pkg/flowstate/v1"
	"github.com/picatz/flowstate/pkg/flowstate/v1/auth"
)

// SignalWithStart's create branch delivers the first signal inside the new
// run's own state, never through Signal, so the signal's policy has to be asked
// there too: a caller `signals:` would refuse must not be able to create the
// run and deliver as its starter.
func TestSignalWithStartCreateBranchAsksTheSignalPolicy(t *testing.T) {
	t.Parallel()

	const issuer = "https://issuer.example.com"
	owner := auth.Principal{Issuer: issuer, Subject: "owner@example.com", Claims: map[string]any{"team": "release"}}
	stranger := auth.Principal{Issuer: issuer, Subject: "stranger@example.com"}
	qualified := v1.QualifiedSubject(issuer, owner.Subject)

	for _, tc := range []struct {
		name     string
		policy   map[string]*v1.SignalPolicy
		caller   auth.Principal
		admitted bool
	}{
		{
			name:     "rule list admits the named caller",
			policy:   map[string]*v1.SignalPolicy{"update": {Allow: []*v1.SignalPolicyRule{{Subject: qualified}}}},
			caller:   owner,
			admitted: true,
		},
		{
			name:   "rule list refuses another caller",
			policy: map[string]*v1.SignalPolicy{"update": {Allow: []*v1.SignalPolicyRule{{Subject: qualified}}}},
			caller: stranger,
		},
		{
			name:     "predicate admits the named caller",
			policy:   map[string]*v1.SignalPolicy{"update": {AllowExpr: `sender.identity.principal == "` + qualified + `"`}},
			caller:   owner,
			admitted: true,
		},
		{
			name:   "predicate refuses another caller",
			policy: map[string]*v1.SignalPolicy{"update": {AllowExpr: `sender.identity.principal == "` + qualified + `"`}},
			caller: stranger,
		},
		{
			name: "distinct_from_starter refuses the creator, who is the sender",
			policy: map[string]*v1.SignalPolicy{"update": {
				Allow:               []*v1.SignalPolicyRule{{Subject: qualified}},
				DistinctFromStarter: true,
			}},
			caller: owner,
		},
		{
			name:   "a predicate comparing sender and starter refuses the creator",
			policy: map[string]*v1.SignalPolicy{"update": {AllowExpr: `sender.identity.principal != run.identity.principal`}},
			caller: owner,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			fixture := newTenantFixture(t)

			ctx := auth.ContextWithPrincipal(t.Context(), tc.caller)
			resp, err := fixture.teamA.SignalWithStart(ctx, connect.NewRequest(&v1.SignalWithStartRequest{
				EntityKey: "gated-order",
				Workflow:  entityWorkflow(tc.policy),
				Name:      "update",
				Payload:   updatePayload(1, false),
			}))

			if tc.admitted {
				require.NoError(t, err)
				assert.True(t, resp.Msg.GetCreated())

				return
			}

			require.Error(t, err, "a caller the policy refuses created the run and delivered the first signal")
			assert.Equal(t, connect.CodePermissionDenied, connect.CodeOf(err))

			// And nothing was created: the entity key is still free.
			_, err = fixture.teamA.Get(auth.ContextWithPrincipal(t.Context(), tc.caller), connect.NewRequest(&v1.GetRequest{
				WorkflowId: "flowstate-entity-team-a_gated-order",
			}))
			require.Error(t, err)
			assert.Equal(t, connect.CodeNotFound, connect.CodeOf(err), "the refused create left a run behind")
		})
	}
}
