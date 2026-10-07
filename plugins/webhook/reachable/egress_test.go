package reachable

import (
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	flowstatev1 "github.com/picatz/flowstate/pkg/flowstate/v1"
	"github.com/picatz/flowstate/pkg/flowstate/v1/auth"
	"github.com/picatz/flowstate/pkg/flowstate/v1/netpolicy"
	"github.com/picatz/flowstate/pkg/flowstate/v1/plugin"
	"github.com/picatz/flowstate/pkg/flowstate/v1/secrets"
)

// testKeyName is the reference the calls below pass; the environment variable
// behind it is FLOWSTATE_SECRET_ plus this name, the env provider's own prefix.
const (
	testKeyName  = "WEBHOOK_REACHABLE_TEST_KEY"
	testKeyValue = "whsec_reachable_test_key_0123456789"
)

// launch starts the real plugin binary under operatorPolicy and returns its one
// task, called the way a worker calls it: the key is a whole secret reference
// that the host resolves under a policy before the plugin sees a byte.
func launch(t *testing.T, operatorPolicy string) func(url, body string) (*flowstatev1.Node_Outputs, error) {
	t.Helper()

	if testing.Short() {
		t.Skip("builds a real plugin binary; skipped under -short, run in CI and by `make check`")
	}
	if _, err := exec.LookPath("go"); err != nil {
		t.Skip("the Go toolchain is not available, so this plugin cannot be built")
	}

	dir := t.TempDir()
	buildPlugin(t, filepath.Join(dir, plugin.BinaryPrefix+"webhook"))

	host := openHost(t, plugin.Config{
		SearchPath:          []string{dir},
		HandshakeTimeout:    10 * time.Second,
		DescribeTimeout:     10 * time.Second,
		CallTimeout:         30 * time.Second,
		ShutdownGrace:       5 * time.Second,
		DisableHealthChecks: true,
		EgressPolicy:        []byte(operatorPolicy),
	})

	defs := host.TaskDefs()
	if len(defs) != 1 || defs[0].Name != "webhook.send" {
		t.Fatalf("the launched plugin does not offer exactly webhook.send: %v", defs)
	}

	ctx := plugin.NewContextWithIdentity(t.Context(), &flowstatev1.WorkloadIdentity{Principal: &flowstatev1.Principal{Subject: "https://issuer.example.com#worker"}, Mode: flowstatev1.WorkloadIdentityMode_WORKLOAD_IDENTITY_MODE_PRODUCTION})
	ctx = flowstatev1.ContextWithTaskRuntime(ctx, taskRuntimeResolvingTheTestKey(t))

	return func(url, body string) (*flowstatev1.Node_Outputs, error) {
		return defs[0].Fn(ctx, map[string]*flowstatev1.Value{
			"url":             flowstatev1.NewValue(url),
			"body":            flowstatev1.NewValue(body),
			"idempotency_key": flowstatev1.NewValue("delivery-1"),
			"signing_key": {Kind: &flowstatev1.Value_SecretRef{SecretRef: &flowstatev1.SecretRef{
				Scheme: "env", Name: testKeyName,
			}}},
		}, nil)
	}
}

// TestAnOperatorDenyRuleStopsADeliveryBeforeItIsDialed is the fail-closed
// direction through a real launched process: the key resolves, the call is
// otherwise valid, and the operator's policy still wins on the destination.
func TestAnOperatorDenyRuleStopsADeliveryBeforeItIsDialed(t *testing.T) {
	var hits atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(http.ResponseWriter, *http.Request) { hits.Add(1) }))
	t.Cleanup(server.Close)

	// No allow_loopback: the receiver is on 127.0.0.1, which the deployment
	// default (what this policy grants on top of) denies.
	send := launch(t, "egress:\n  deny:\n    - credentials\n")
	_, err := send(server.URL, `{"id":"1"}`)
	if err == nil || !strings.Contains(err.Error(), "deployment egress policy denied webhook.send") {
		t.Fatalf("webhook.send failed for some reason other than the operator's policy, or succeeded: %v", err)
	}
	if hits.Load() != 0 {
		t.Fatal("a destination the operator's policy denies was dialed")
	}
	if strings.Contains(err.Error(), testKeyValue) || strings.Contains(err.Error(), server.URL) {
		t.Fatalf("the refusal echoed a value: %v", err)
	}
}

// TestADeliveryThroughTheLaunchedPluginVerifiesAndLeaksNothing is the end to
// end round trip: the host resolves the key, the plugin signs with the
// engine's signer, and the engine's verifier accepts the delivery. The outputs
// the host records carry neither the key nor the signature.
func TestADeliveryThroughTheLaunchedPluginVerifiesAndLeaksNothing(t *testing.T) {
	var (
		gotBody    atomic.Pointer[string]
		gotHeaders atomic.Pointer[map[string]string]
	)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		raw, _ := io.ReadAll(r.Body)
		body := string(raw)
		headers := map[string]string{}
		for name := range r.Header {
			headers[strings.ToLower(name)] = r.Header.Get(name)
		}
		gotBody.Store(&body)
		gotHeaders.Store(&headers)
		// A receiver that echoes its inputs back, signature and all.
		_, _ = io.WriteString(w, "got "+headers[strings.ToLower(flowstatev1.WebhookSignatureHeader)]+" "+testKeyValue)
	}))
	t.Cleanup(server.Close)

	send := launch(t, "egress:\n  allow_loopback: true\n")
	out, err := send(server.URL, `{"id":"evt_1"}`)
	if err != nil {
		t.Fatalf("webhook.send: %v", err)
	}

	headers := *gotHeaders.Load()
	trigger := &flowstatev1.WebhookTrigger{
		Name:           "peer",
		IdempotencyKey: flowstatev1.NewExpr(`event.body.id`),
		Verify: map[string]*flowstatev1.Value{flowstatev1.WebhookSchemeHMACSHA256: {
			Kind: &flowstatev1.Value_SecretRef{SecretRef: &flowstatev1.SecretRef{Scheme: "env", Name: testKeyName}},
		}},
	}
	key := secrets.NewSecret(secrets.NewRef("env", testKeyName), testKeyValue)
	keys := map[string]secrets.Secret{flowstatev1.WebhookSchemeHMACSHA256: key}
	if err := flowstatev1.VerifyWebhookDelivery(trigger, keys, headers, []byte(*gotBody.Load()), time.Now()); err != nil {
		t.Fatalf("the engine's verifier refused the launched plugin's delivery: %v", err)
	}
	if err := flowstatev1.VerifyWebhookDelivery(trigger, keys, headers, []byte(*gotBody.Load()+" "), time.Now()); err == nil {
		t.Fatal("a tampered body verified")
	}
	if headers["idempotency-key"] != "delivery-1" {
		t.Fatalf("idempotency-key = %q", headers["idempotency-key"])
	}

	recorded := out.String()
	for _, secret := range []string{testKeyValue, headers[strings.ToLower(flowstatev1.WebhookSignatureHeader)]} {
		if strings.Contains(recorded, secret) {
			t.Fatalf("the recorded outputs carry %q: %s", secret, recorded)
		}
	}
}

// taskRuntimeResolvingTheTestKey is the smallest runtime that lets a whole
// secret reference resolve, written as narrowly as the tests need.
func taskRuntimeResolvingTheTestKey(t *testing.T) flowstatev1.TaskRuntime {
	t.Helper()

	t.Setenv(secrets.DefaultEnvPrefix+testKeyName, testKeyValue)

	provider, err := secrets.NewEnvProvider(secrets.WithEnvAllow(testKeyName))
	if err != nil {
		t.Fatalf("building the env secret provider: %v", err)
	}
	store, err := secrets.NewStore(provider)
	if err != nil {
		t.Fatalf("building the secret store: %v", err)
	}
	policy, err := auth.SecretAccessPolicy{
		Allow: []string{`secret.scheme == "env" && secret.name == "` + testKeyName + `"`},
	}.Compile()
	if err != nil {
		t.Fatalf("compiling the secret access policy: %v", err)
	}

	return flowstatev1.TaskRuntime{
		Store:  store,
		Policy: policy,
		Identity: auth.WorkloadIdentity{
			Subject: "worker",
			Issuer:  "https://issuer.example.com",
		},
		Step: auth.StepRef{Workflow: "order-paid-notify", Run: "reachable-test", Step: "notify"},
	}
}

// TestTheShippedEgressPolicyBuildsAndKeepsASignedDeliveryOnItsReceiver judges
// the file the example tells an operator to copy: it compiles, it permits the
// one receiver, and it refuses a signed (credentialed) delivery anywhere else.
func TestTheShippedEgressPolicyBuildsAndKeepsASignedDeliveryOnItsReceiver(t *testing.T) {
	data, err := os.ReadFile("../../../examples/plugins/webhook/egress-policy.yaml")
	if err != nil {
		t.Fatalf("reading the shipped policy: %v", err)
	}
	cfg, err := netpolicy.ParseConfig(data)
	if err != nil {
		t.Fatalf("the shipped policy does not parse: %v", err)
	}
	policy, err := cfg.Policy()
	if err != nil {
		t.Fatalf("the shipped policy does not compile: %v", err)
	}

	check := func(target string) error {
		u, err := url.Parse(target)
		if err != nil {
			t.Fatal(err)
		}
		return policy.CheckURL(netpolicy.ContextWithCredentials(t.Context(), true), http.MethodPost, u)
	}
	if err := check("https://flowstate.peer.example.com/webhooks/order-fulfilment/order-paid"); err != nil {
		t.Errorf("the policy refused its own receiver: %v", err)
	}
	if err := check("https://elsewhere.example.com/hook"); err == nil {
		t.Error("the policy permitted a signed delivery to another host")
	}
}
