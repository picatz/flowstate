package server

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	v1 "github.com/picatz/flowstate/pkg/flowstate/v1"
)

// Two properties of [FlowstateServer.registerTrustedWorkflows] that its only
// production caller cannot expose, tested here for the reason
// trustednaming_internal_test.go gives for the nameless case:
// [FlowstateServer.NewWebhookReceiver]'s own register() runs the schema rules
// and refuses a duplicate name *first*, so a test driven through the
// constructor passes whether or not this function checks anything. Its doc
// comment invites callers other than the receiver, and what has to be false is
// that it reports success for trust it did not install.
//
// Both were written by mutation: each names the line whose removal leaves the
// whole `./pkg/flowstate/v1/server/` suite green.

// TestRegisteringAnInvalidTrustedWorkflowIsRefused pins the schema check on the
// post-construction registration path.
//
// A trusted copy is substituted for the caller's *after* the RPC handlers have
// run [v1.Validate] on the request, and validateSubmission does not ask again
// once the substitution has happened — so a malformed deployment-owned copy
// reaches Temporal and the engine on the strength of being deployment-owned.
// That is the finding [WithTrustedWorkflows] answers by poisoning the key; this
// path answers it by refusing the call, which it can, because it returns an
// error.
//
// The mutation this fails against: delete the `v1.Validate` arm from
// [FlowstateServer.registerTrustedWorkflows] (the `cannot be trusted as a
// deployment-owned specification` refusal).
func TestRegisteringAnInvalidTrustedWorkflowIsRefused(t *testing.T) {
	t.Parallel()

	s := mustNew(t, nil)

	// A valid name and no steps: the shape a Go embedder builds
	// programmatically and the schema refuses.
	malformed := &v1.Workflow{Name: "break-glass", Profile: v1.CurrentProfile}
	require.Error(t, v1.Validate(malformed), "this test's premise is that the schema refuses this")

	err := s.registerTrustedWorkflows("team-a", []*v1.Workflow{malformed})
	require.Error(t, err, "a trusted registration the schema refuses was installed anyway")
	assert.Contains(t, err.Error(), "break-glass")

	// Nothing was written, so a caller submitting this name is an ordinary
	// ungoverned submission rather than one substituted with a specification
	// this deployment would refuse.
	s.trustedWorkflowsMu.RLock()
	defer s.trustedWorkflowsMu.RUnlock()
	assert.Empty(t, s.trustedWorkflows, "a refused registration still wrote to the trusted set")
}

// TestARefusedTrustedBatchInstallsNoneOfIt pins the all-or-nothing property the
// function's doc comment claims: "the whole call is one transaction".
//
// Its sibling TestANamelessWorkflowCannotDisplaceANamedOneAlreadyTrusted covers
// the case where the batch's good entry was *already* trusted with the same
// specification, which a write-as-you-go implementation would rewrite to the
// identical value — leaving nothing to observe. What distinguishes the two
// implementations is a batch whose good entry is not registered yet: a
// write-as-you-go loop leaves that name trusted by a call that reported
// failure, so `Run` substitutes a specification for a workflow this deployment
// never finished admitting.
//
// The mutation this fails against: move the write in
// [FlowstateServer.registerTrustedWorkflows] up into the validating loop and
// drop the second pass.
func TestARefusedTrustedBatchInstallsNoneOfIt(t *testing.T) {
	t.Parallel()

	s := mustNew(t, nil)

	// Not registered anywhere yet, and valid, so it passes every check the
	// first loop makes before the second entry fails.
	admitted := &v1.Workflow{
		Name:    "break-glass",
		Profile: v1.CurrentProfile,
		Triggers: &v1.Triggers{Manual: &v1.ManualTrigger{
			AllowedPrincipals: []string{"https://issuer.example.com#oncall@example.com"},
		}},
		Steps: []*v1.Node{{Id: "rotate", Kind: &v1.Node_Value{Value: v1.NewLiteral("rotated")}}},
	}

	// The entry that takes the call down, after the one above has been seen.
	nameless := &v1.Workflow{
		Profile: v1.CurrentProfile,
		Steps:   []*v1.Node{{Id: "rotate", Kind: &v1.Node_Value{Value: v1.NewLiteral("rotated")}}},
	}

	require.Error(t, s.registerTrustedWorkflows("team-a", []*v1.Workflow{admitted, nameless}),
		"a batch carrying a nameless registration was accepted")

	s.trustedWorkflowsMu.RLock()
	defer s.trustedWorkflowsMu.RUnlock()
	assert.Empty(t, s.trustedWorkflows,
		"a refused batch left its earlier entries trusted, so trust was granted by a call that failed")
}
