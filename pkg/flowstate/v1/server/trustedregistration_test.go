package server_test

import (
	"testing"

	"connectrpc.com/connect"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	v1 "github.com/picatz/flowstate/pkg/flowstate/v1"
	"github.com/picatz/flowstate/pkg/flowstate/v1/server"
)

// The registration paths, joined.
//
// [server.WithTrustedWorkflows] and [server.FlowstateServer.NewWebhookReceiver]
// are two ways into one trusted set, and every test that existed for either of
// them stayed inside its own path: trustedconflict_test.go registers twice
// through the option, and webhook_test.go's
// TestAFailedWebhookReceiverGrantsNoTrust registers twice through one receiver
// call. The bypass the review of #709 named is neither of those — it is a
// receiver registering a name the *option* already registered, which is the
// join of the two features and the place CLAUDE.md says to look ("Test the join
// of two features and not only each half").
//
// Both tests below were written by mutation: each names the line whose removal
// leaves the whole `./pkg/flowstate/v1/server/` suite green.

// TestAReceiverCannotDisplaceATrustedSpecification pins the cross-path half of
// the duplicate-registration refusal.
//
// A deployment registers `break-glass-webhook` through
// [server.WithTrustedWorkflows] with `manual:` narrowed to one principal, and
// then mounts a receiver serving a specification of the same name whose
// `manual:` block permits anyone. Last-writer-wins would make the receiver's
// weaker copy the one every `Run`, `SignalWithStart` and `CreateSchedule`
// authorizes against — a policy displaced by nothing an operator wrote down,
// only by the order two configuration calls happened to run in.
//
// The mutation this fails against: delete the `!proto.Equal(existing, workflow)`
// conflict arm from [server.FlowstateServer.registerTrustedWorkflows]
// (server.go, the `is already registered for this namespace with a different
// specification` refusal). Without this test the suite stays green, because the
// option's own conflict arm — which trustedconflict_test.go does cover — is a
// different line in a different function.
func TestAReceiverCannotDisplaceATrustedSpecification(t *testing.T) {
	t.Parallel()

	temporal, _ := newTemporalNamespace(t)

	// The deployment's own registration: the narrow policy.
	strict := webhookOnlyWorkflowWithManualDenied()
	flowstate := mustNew(t, temporal, server.WithTrustedWorkflows("", strict))

	// A receiver mounted afterwards, serving the same name with `manual:`
	// wide open. It is servable — same webhooks, same verification — so
	// nothing but the trusted-set conflict can be what refuses it.
	weaker := webhookOnlyWorkflowWithManualDenied()
	weaker.Triggers.Manual = &v1.ManualTrigger{}

	_, err := flowstate.NewWebhookReceiver(t.Context(), "",
		[]*v1.Workflow{weaker}, keyStore(t, webhookSecret))
	require.Error(t, err,
		"a receiver replaced a trusted specification this deployment had already registered")
	assert.Contains(t, err.Error(), "already registered")

	// And the narrow policy is still the one that binds: a caller naming the
	// workflow is held to `manual: denied`, not to the receiver's copy.
	submitted := webhookOnlyWorkflowWithManualDenied()
	submitted.Triggers = &v1.Triggers{Webhooks: submitted.Triggers.GetWebhooks()}

	_, err = flowstate.Run(t.Context(), connect.NewRequest(&v1.RunRequest{
		Workflow: submitted,
		Reason:   "a caller's own copy, after a receiver tried to weaken the trusted one",
	}))
	require.Error(t, err, "the registered `manual: denied` stopped binding after a refused receiver")
	assert.Equal(t, connect.CodePermissionDenied, connect.CodeOf(err))
}

// TestAReceiverCannotServeAWorkflowWhoseTrustIsPoisoned pins the other
// cross-path refusal: a key [server.WithTrustedWorkflows] could not answer for.
//
// Registering one name twice with different specifications does not fail the
// construction — a conflict is scoped to one tenant's one workflow, and failing
// there would take the deployment's other tenants down with it — so the key is
// poisoned instead and every request for it is refused. A receiver mounting that
// same name must not then start serving deliveries into it: every delivery would
// reach a `Run` the trusted lookup refuses, which is an endpoint advertised at
// startup that answers nothing but errors, at whatever hour the provider first
// fires.
//
// The mutation this fails against: delete the
// `if reason := s.trustedWorkflowRefusals[key]; reason != ""` arm from
// [server.FlowstateServer.registerTrustedWorkflows]. The refusal still fails
// closed per request, which is why nothing else notices; what is lost is the
// refusal happening at construction, where an operator is watching.
func TestAReceiverCannotServeAWorkflowWhoseTrustIsPoisoned(t *testing.T) {
	t.Parallel()

	temporal, _ := newTemporalNamespace(t)

	// Two registrations of one name that disagree — the key is poisoned.
	first := webhookOnlyWorkflowWithManualDenied()
	second := webhookOnlyWorkflowWithManualDenied()
	second.Triggers.Manual = &v1.ManualTrigger{AllowedPrincipals: []string{"oncall@example.com"}}

	flowstate := mustNew(t, temporal, server.WithTrustedWorkflows("", first, second))

	_, err := flowstate.NewWebhookReceiver(t.Context(), "",
		[]*v1.Workflow{webhookOnlyWorkflowWithManualDenied()}, keyStore(t, webhookSecret))
	require.Error(t, err,
		"a receiver began serving deliveries for a workflow whose trusted specification this "+
			"deployment cannot decide")
	assert.Contains(t, err.Error(), "registered twice")
}
