package server_test

import (
	"log/slog"
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.temporal.io/sdk/client"
	"go.temporal.io/sdk/log"

	v1 "github.com/picatz/flowstate/pkg/flowstate/v1"
	"github.com/picatz/flowstate/pkg/flowstate/v1/secrets"
	"github.com/picatz/flowstate/pkg/flowstate/v1/server"
	"github.com/picatz/flowstate/pkg/flowstate/v1/temporalclient"
)

// Which tenant a webhook belongs to, which is two questions that have to have one
// answer.
//
// A sender presents a signature rather than a credential, so a delivery names no
// tenant and the operator establishes one. It decides where the run goes and where
// the signing key is read from, and the receiver had neither: the run was
// attributed to the empty tenant — unroutable on any deployment that maps tenants
// onto Temporal namespaces, so every correctly signed delivery was refused — and
// the keys were resolved in whatever tenant the *caller* happened to scope the
// resolver to.
//
// The tests here are written in the direction that finds the second one. Asserting
// that a webhook reads its own key passes on a receiver that reads everybody's; what
// fails is asserting that the backend was never asked in anyone else's tenant.

const (
	// The two tenants' signing keys, deliberately different: a receiver holding
	// the wrong one verifies nothing, which is how a delivery test can tell which
	// tenant a key came from without ever printing one.
	teamASecret = "whsec_team_a"
	teamBSecret = "whsec_team_b"
)

// unreachableTemporal is a client that dials only when used, at a port nothing
// answers on: enough to exercise every arm before a run is created, without a
// cluster and without the nil client a receiver would dereference.
func unreachableTemporal(t *testing.T) client.Client {
	t.Helper()

	temporal, err := client.NewLazyClient(client.Options{
		HostPort: "127.0.0.1:1",
		Logger:   log.NewStructuredLogger(slog.New(slog.DiscardHandler)),
	})
	require.NoError(t, err)

	return temporal
}

// signedWith is the signature a sender holding key computes over body.
func signedWith(key string) func(string) string {
	return func(body string) string {
		return v1.SignWebhookBody(secrets.NewSecret(secrets.NewRef("env", "k"), key), []byte(body))
	}
}

// TestAWebhookResolvesOnlyItsOwnTenantsSigningKey is the negative direction of the
// tenant boundary, which is the only direction that can fail.
//
// Both tenants hold a key under the same reference, which is the arrangement a
// deployment with a per-tenant secret backend actually has: `verify:` names
// `env:STOREFRONT_WEBHOOK_SECRET`, and what that resolves to is decided entirely by
// the namespace the resolution carries. So a receiver serving team-a must ask in
// team-a and nowhere else — and the assertion is on where it asked, not on what it
// got back, because a receiver that asked in both tenants and kept the first answer
// would return the right key and still be a cross-tenant read.
func TestAWebhookResolvesOnlyItsOwnTenantsSigningKey(t *testing.T) {
	t.Parallel()

	backend := &keyProvider{keys: map[string]string{
		"team-a": teamASecret,
		"team-b": teamBSecret,
	}}

	// A cluster that is never reached, because what is under test here ends at
	// verification: a delivery that gets that far is answered 503 by the arm
	// [TestADeliveryTheDeploymentCannotStartIsRetryable] covers, and the two
	// deliveries below part company well before it.
	receiver, err := mustNew(t, unreachableTemporal(t)).NewWebhookReceiver(t.Context(), "team-a",
		[]*v1.Workflow{orderWebhookWorkflow()}, storeOf(t, backend))
	require.NoError(t, err, "a receiver could not be built for a tenant that holds the key it needs")

	// Where it asked. Every resolution this receiver performed, and there is
	// exactly one tenant in the list.
	assert.Equal(t, []string{"team-a"}, backend.namespaces(),
		"the receiver resolved a signing key outside its own tenant")

	body := deliveryBody("evt_tenancy")

	// And what it holds, established without printing a key: a delivery signed
	// with team-b's key does not verify against a receiver serving team-a. Were
	// the receiver holding team-b's key, this would be a genuine delivery.
	refused := deliver(t, receiver, "/webhooks/order-webhook/storefront", body, signedWith(teamBSecret))
	assert.Equal(t, http.StatusNotFound, refused.StatusCode,
		"a delivery signed with another tenant's key verified, so the receiver holds that tenant's secret")

	// The positive half, so the assertion above is not passing merely because
	// nothing verifies: team-a's key gets past verification. What happens next is
	// a run this server has no Temporal to start, which is a different arm — the
	// claim here is only that verification is where the two deliveries part.
	accepted := deliver(t, receiver, "/webhooks/order-webhook/storefront", body, signedWith(teamASecret))
	assert.NotEqual(t, http.StatusNotFound, accepted.StatusCode,
		"a delivery signed with this receiver's own tenant's key was refused as unverifiable")
}

// TestAReceiverRefusesATenantThatHoldsNoKey is the fail-closed direction of the
// same boundary: a webhook served for a tenant with no key of its own must not
// quietly find one somewhere else.
func TestAReceiverRefusesATenantThatHoldsNoKey(t *testing.T) {
	t.Parallel()

	// Only team-b has configured a signing key.
	backend := &keyProvider{keys: map[string]string{"team-b": teamBSecret}}

	_, err := mustNew(t, nil).NewWebhookReceiver(t.Context(), "team-a",
		[]*v1.Workflow{orderWebhookWorkflow()}, storeOf(t, backend))
	require.Error(t, err,
		"a receiver serving a tenant with no signing key started anyway, so it found one somewhere")
	assert.Contains(t, err.Error(), "team-a", "the refusal does not name the tenant that has no key")

	assert.Equal(t, []string{"team-a"}, backend.namespaces(),
		"a receiver that could not find its own tenant's key went looking in another tenant's")
}

// pooledWebhookServer is a deployment that maps tenants onto Temporal namespaces,
// which is the configuration the missing namespace made unusable.
//
// Every routed name maps onto this test's own Temporal namespace, for
// [newPooledServer]'s reason: what a tenant maps *to* is registered per test, so a
// mapping written as a constant would name somebody else's namespace.
func pooledWebhookServer(t *testing.T, routed ...string) (*server.FlowstateServer, client.Client) {
	t.Helper()

	temporal, namespace := newTemporalNamespace(t)

	mapping := make(map[string]string, len(routed))
	for _, name := range routed {
		mapping[name] = namespace
	}

	pool, err := temporalclient.NewPool(t.Context(), temporalclient.Config{
		Address:   devServer.FrontendHostPort(),
		Namespace: namespace,
	}, mapper{mapping: mapping}, nil)
	require.NoError(t, err)
	t.Cleanup(pool.Close)

	return mustNew(t, temporal, server.WithNamespacePool(pool)), temporal
}

// TestADeliveryStartsARunInTheReceiversTenant is the finding itself: on a
// deployment that maps tenants, a correctly signed delivery has to start a run.
//
// Before the receiver had a namespace, this deployment refused every one of them.
// The run was attributed to the empty tenant, which a mapping with named entries
// and no default cannot route, so the submission failed inside the receiver and the
// sender was told 422 — permanently, on configuration that is otherwise correct.
func TestADeliveryStartsARunInTheReceiversTenant(t *testing.T) {
	t.Parallel()

	flowstate, temporal := pooledWebhookServer(t, "team-a")

	receiver, err := flowstate.NewWebhookReceiver(t.Context(), "team-a",
		[]*v1.Workflow{orderWebhookWorkflow()},
		storeOf(t, &keyProvider{keys: map[string]string{"team-a": teamASecret}}))
	require.NoError(t, err)

	resp := deliver(t, receiver, "/webhooks/order-webhook/storefront",
		deliveryBody("evt_tenant"), signedWith(teamASecret))
	require.Equal(t, http.StatusAccepted, resp.StatusCode,
		"a correctly signed delivery to a tenanted deployment did not start a run")

	accepted := readAccepted(t, resp)

	// And it belongs to the tenant the operator named, read off the run rather
	// than off the response: this is what every later authorization decision about
	// the run compares against, and what routed it to a namespace at all.
	described, err := temporal.DescribeWorkflowExecution(t.Context(), accepted.WorkflowID, accepted.RunID)
	require.NoError(t, err)

	memo := described.GetWorkflowExecutionInfo().GetMemo().GetFields()
	assert.Equal(t, `"team-a"`, string(memo["flowstate.namespace"].GetData()),
		"the run a delivery started belongs to a different tenant than the receiver serving it")
}

// TestAReceiverRefusesANamespaceThisDeploymentCannotRoute moves that refusal to
// startup, which is what `--webhook` promises.
//
// A deployment mapping tenants with no entry for the receiver's own can serve no
// delivery at all. Discovering that per delivery means an endpoint advertised at
// startup, a provider configured against it, and a 422 whose reason exists only in
// a log line — so it is decided when the configuration loads instead, where the
// operator who wrote it is still watching.
func TestAReceiverRefusesANamespaceThisDeploymentCannotRoute(t *testing.T) {
	t.Parallel()

	for name, namespace := range map[string]string{
		"an unmapped tenant": "team-b",
		"no tenant at all":   "",
	} {
		t.Run(name, func(t *testing.T) {
			flowstate, _ := pooledWebhookServer(t, "team-a")

			_, err := flowstate.NewWebhookReceiver(t.Context(), namespace,
				[]*v1.Workflow{orderWebhookWorkflow()},
				storeOf(t, &keyProvider{keys: map[string]string{
					"":       webhookSecret,
					"team-a": teamASecret,
					"team-b": teamBSecret,
				}}))

			require.Error(t, err,
				"a receiver started serving a tenant whose runs this deployment cannot route anywhere")
			assert.Contains(t, err.Error(), "Temporal namespace",
				"the refusal does not say what an operator has to configure")
		})
	}
}

// TestAReceiverRefusesAWorkflowThisDeploymentCannotSatisfy is the second half of
// deciding at load: the deployment-dependent checks a submission makes about the
// specification alone.
//
// A plugin requirement this catalog cannot meet is not a fact about the delivery.
// It is settled before the server binds a socket, and asking it at delivery time
// produced exactly the failure "fail closed" exists to prevent — an advertised
// webhook, permanently unusable, refusing genuine deliveries with a 422 for a
// reason an operator only finds by reading logs.
func TestAReceiverRefusesAWorkflowThisDeploymentCannotSatisfy(t *testing.T) {
	t.Parallel()

	needsPlugin := func() *v1.Workflow {
		workflow := orderWebhookWorkflow()
		workflow.PluginRequirements = []*v1.PluginRequirement{{
			Name: "storefront", MinimumVersion: "v2.0.0",
		}}

		return workflow
	}

	installed := func(version string) *v1.PluginCatalog {
		return &v1.PluginCatalog{ClaimsSchemaVersion: v1.CurrentClaimsSchemaVersion, Plugins: []*v1.PluginDescription{{
			Name: "storefront", Version: version, ProtocolVersion: 2,
			TaskSchemaDigest: "sha256:schema", DistributionDigest: "sha256:binary", ClaimsDigest: "sha256:claims",
		}}}
	}

	// Installed, and below the floor the file declares — the case that reads as
	// working configuration right up until a delivery arrives.
	_, err := mustNew(t, nil, server.WithPluginCatalog(installed("v1.4.0"))).
		NewWebhookReceiver(t.Context(), "", []*v1.Workflow{needsPlugin()}, keyStore(t, webhookSecret))
	require.Error(t, err,
		"a receiver started serving a workflow whose plugin requirement this deployment cannot satisfy")
	assert.Contains(t, err.Error(), "storefront", "the refusal does not name what cannot be satisfied")
	assert.Contains(t, err.Error(), "order-webhook", "the refusal does not name the workflow at fault")

	// And the same workflow against a catalog that satisfies it, so the check
	// refuses what it should rather than everything with a requirement.
	_, err = mustNew(t, nil, server.WithPluginCatalog(installed("v2.1.0"))).
		NewWebhookReceiver(t.Context(), "", []*v1.Workflow{needsPlugin()}, keyStore(t, webhookSecret))
	require.NoError(t, err, "a receiver refused a workflow this deployment can serve")
}
