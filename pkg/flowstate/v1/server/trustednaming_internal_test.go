package server

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	v1 "github.com/picatz/flowstate/pkg/flowstate/v1"
)

// The other half of the nameless-registration refusal, on the path
// [FlowstateServer.NewWebhookReceiver] takes. Its external sibling —
// TestANamelessTrustedRegistrationRefusesConstruction — covers
// [WithTrustedWorkflows]; this covers [FlowstateServer.registerTrustedWorkflows],
// which is unexported and is the path a receiver extends the trusted set
// through after the server is already built.
//
// Written here rather than through NewWebhookReceiver on purpose: that
// constructor's own register() refuses a nameless workflow first, with a
// different message about delivery addressing, so a test driven through it would
// pass whether or not this function checks anything. What has to be false is that
// *this* function reports success for trust it did not install, since its doc
// comment invites callers other than the receiver.

// TestRegisteringANamelessTrustedWorkflowIsRefused is the negative direction: a
// registration naming nothing occupies no key, so accepting it quietly would
// leave the name the caller meant to protect open while telling them it was
// registered.
func TestRegisteringANamelessTrustedWorkflowIsRefused(t *testing.T) {
	t.Parallel()

	for _, test := range []struct {
		name     string
		workflow *v1.Workflow
	}{
		{name: "a nil entry", workflow: nil},
		{name: "a workflow with no name", workflow: &v1.Workflow{
			Profile: v1.CurrentProfile,
			Steps:   []*v1.Node{{Id: "rotate", Kind: &v1.Node_Value{Value: v1.NewLiteral("rotated")}}},
		}},
	} {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()

			s := mustNew(t, nil)

			err := s.registerTrustedWorkflows("team-a", []*v1.Workflow{test.workflow})

			require.Error(t, err, "a trusted registration naming nothing was accepted")
			assert.Contains(t, err.Error(), "team-a")
			assert.Contains(t, err.Error(), "`name:`")

			// And nothing was written: a refused registration leaves the
			// trusted set exactly as it found it, which is the same
			// all-or-nothing property the conflict arm relies on.
			s.trustedWorkflowsMu.RLock()
			defer s.trustedWorkflowsMu.RUnlock()
			assert.Empty(t, s.trustedWorkflows,
				"a refused registration still wrote to the trusted set")
		})
	}
}

// TestANamelessWorkflowCannotDisplaceANamedOneAlreadyTrusted joins the two
// halves: the refusal has to take the whole call with it, so a batch whose
// second entry is nameless cannot half-apply and cannot disturb a registration
// already in place under a name a caller submits.
func TestANamelessWorkflowCannotDisplaceANamedOneAlreadyTrusted(t *testing.T) {
	t.Parallel()

	trusted := &v1.Workflow{
		Name:    "break-glass",
		Profile: v1.CurrentProfile,
		Triggers: &v1.Triggers{Manual: &v1.ManualTrigger{
			RequireReason:     true,
			AllowedPrincipals: []string{"https://issuer.example.com#oncall@example.com"},
		}},
		Steps: []*v1.Node{{Id: "rotate", Kind: &v1.Node_Value{Value: v1.NewLiteral("rotated")}}},
	}

	s := mustNew(t, nil, WithTrustedWorkflows("team-a", trusted))

	weaker := &v1.Workflow{
		Profile: v1.CurrentProfile,
		Steps:   []*v1.Node{{Id: "rotate", Kind: &v1.Node_Value{Value: v1.NewLiteral("rotated")}}},
	}

	require.Error(t, s.registerTrustedWorkflows("team-a", []*v1.Workflow{trusted, weaker}))

	// The narrowed policy is still what this tenant's name resolves to.
	effective, _, err := s.trustedWorkflow("team-a", &v1.Workflow{Name: "break-glass"})
	require.NoError(t, err, "a refused batch poisoned a key it was never allowed to touch")
	assert.Equal(t, []string{"https://issuer.example.com#oncall@example.com"},
		effective.GetTriggers().GetManual().GetAllowedPrincipals(),
		"the registered `manual:` policy stopped binding after a refused batch")
}
