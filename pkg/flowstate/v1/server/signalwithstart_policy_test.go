package server_test

import (
	"context"
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

// The create check decides a create and nothing else: its memo records the
// *caller* as the starter. An entity somebody else already started is answered
// on its own memo, where `distinct_from_starter` and `run.identity` mean the
// real starter, so a caller the existing run admits must not be refused by the
// create check's different answer.
func TestSignalWithStartAnswersAnExistingEntityOnItsOwnMemo(t *testing.T) {
	t.Parallel()

	const issuer = "https://issuer.example.com"
	creator := auth.Principal{Issuer: issuer, Subject: "creator@example.com"}
	approver := auth.Principal{Issuer: issuer, Subject: "approver@example.com"}
	outsider := auth.Principal{Issuer: issuer, Subject: "outsider@example.com"}
	approverID := v1.QualifiedSubject(issuer, approver.Subject)

	for name, policy := range map[string]*v1.SignalPolicy{
		"rule list with distinct_from_starter": {
			Allow:               []*v1.SignalPolicyRule{{Subject: approverID}, {Subject: v1.QualifiedSubject(issuer, creator.Subject)}},
			DistinctFromStarter: true,
		},
		"predicate over the starter": {
			AllowExpr: `sender.identity.principal in ["` + approverID + `", "` + v1.QualifiedSubject(issuer, creator.Subject) +
				`"] && sender.identity.principal != run.identity.principal`,
		},
	} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()

			fixture := newTenantFixture(t)
			as := func(p auth.Principal) context.Context { return auth.ContextWithPrincipal(t.Context(), p) }
			signal := func(p auth.Principal, key string) (*connect.Response[v1.SignalWithStartResponse], error) {
				return fixture.teamA.SignalWithStart(as(p), connect.NewRequest(&v1.SignalWithStartRequest{
					EntityKey: key,
					Workflow:  entityWorkflow(map[string]*v1.SignalPolicy{"update": policy}),
					Name:      "update",
					Payload:   updatePayload(1, false),
				}))
			}

			// The creator starts the entity through Run, which asks no signal
			// policy: SignalWithStart by the creator would be refused, as the
			// creator is then the sender.
			key := "shared-entity"
			_, err := fixture.teamA.Run(as(creator), connect.NewRequest(&v1.RunRequest{
				Workflow:  entityWorkflow(map[string]*v1.SignalPolicy{"update": policy}),
				EntityKey: &key,
			}))
			require.NoError(t, err)

			resp, err := signal(approver, key)
			require.NoError(t, err, "a caller the existing entity's own memo admits was refused by the create check")
			assert.False(t, resp.Msg.GetCreated())

			_, err = signal(creator, key)
			require.Error(t, err, "the starter signalled its own existing entity past a separation of duties")
			assert.Equal(t, connect.CodePermissionDenied, connect.CodeOf(err))

			_, err = signal(outsider, key)
			require.Error(t, err, "a caller outside the allow list reached an existing entity")
			assert.Equal(t, connect.CodePermissionDenied, connect.CodeOf(err))

			// And on create: the outsider is refused, and nothing is created.
			_, err = signal(outsider, "fresh-entity")
			require.Error(t, err)
			assert.Equal(t, connect.CodePermissionDenied, connect.CodeOf(err))
			_, err = fixture.teamA.Get(as(outsider), connect.NewRequest(&v1.GetRequest{
				WorkflowId: "flowstate-entity-team-a_fresh-entity",
			}))
			require.Error(t, err)
			assert.Equal(t, connect.CodeNotFound, connect.CodeOf(err))
		})
	}
}
