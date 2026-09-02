package plugin

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"net/netip"
	"os"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	flowstatev1 "github.com/picatz/flowstate/pkg/flowstate/v1"
	"github.com/picatz/flowstate/pkg/flowstate/v1/netpolicy"
	"github.com/picatz/flowstate/pkg/flowstate/v1/plugin/sdk"
	"github.com/picatz/flowstate/pkg/flowstate/v1/secrets"
)

// runEgressGrantPlugin is a real SDK plugin that reports what the grant it was
// launched with permits.
//
// A real one rather than a hand-rolled handler, for the reason the errors and
// caller-mode fixtures are: the claim under test is that a plugin process
// receives the deployment's policy through [sdk.EgressPolicy], and a fake that
// read the environment itself would prove the environment and not the SDK.
func runEgressGrantPlugin() int {
	report := func(context.Context, map[string]*flowstatev1.Value, *flowstatev1.Scope) (*flowstatev1.Node_Outputs, error) {
		outputs := map[string]*flowstatev1.Value{}

		policy, err := sdk.EgressPolicy()
		if err != nil {
			outputs["refusal"] = flowstatev1.NewLiteral(err.Error())
			return &flowstatev1.Node_Outputs{NamedValues: outputs}, nil
		}
		outputs["refusal"] = flowstatev1.NewLiteral("")

		// A destination the two policies below disagree about, so the test can
		// tell "some policy arrived" from "the operator's policy arrived".
		if err := policy.CheckAddr(netip.MustParseAddrPort("127.0.0.1:443")); err != nil {
			outputs["loopback"] = flowstatev1.NewLiteral(err.Error())
		} else {
			outputs["loopback"] = flowstatev1.NewLiteral("")
		}

		// Read straight out of this process's own environment, which is what
		// http.ProxyFromEnvironment reads and therefore the only thing that
		// decides where a proxying policy actually sends a request.
		outputs["http_proxy"] = flowstatev1.NewLiteral(os.Getenv("HTTP_PROXY"))

		return &flowstatev1.Node_Outputs{NamedValues: outputs}, nil
	}

	err := sdk.Run(context.Background(), sdk.Plugin{
		Name:        "egress-grant",
		Version:     "0.0.1",
		Description: "reports the egress policy the host granted it",
		Tasks: []sdk.Task{{
			Name:   "report",
			Input:  &flowstatev1.Task_Log_Inputs{},
			Output: &flowstatev1.Task_Log_Outputs{},
			Fn:     report,
		}},
	})
	if err != nil {
		fmt.Fprintf(os.Stderr, "egress-grant fixture: %v\n", err)
		return 1
	}

	return 0
}

// TestTheEgressGrantReachesThePluginProcess closes the chain the two unit tests
// leave open at its ends: [Config.EgressPolicy] on one side, [sdk.EgressPolicy]
// on the other, and a real launched process in between.
//
// It is the issue's acceptance criterion stated as a test — a plugin the host
// knows nothing about, using nothing but the SDK constructor, is governed by the
// deployment's policy (#1332) — and the loopback case is what makes it a test of
// *which* policy rather than of whether any arrived.
func TestTheEgressGrantReachesThePluginProcess(t *testing.T) {
	t.Parallel()

	for _, test := range []struct {
		name string

		// grant is what the deployment configured, as Config.EgressPolicy.
		grant []byte

		// wantRefusal, when true, expects sdk.EgressPolicy to refuse rather
		// than return a policy.
		wantRefusal bool

		// wantLoopbackDenied is the check the two granted policies disagree
		// about.
		wantLoopbackDenied bool
	}{
		{
			name:               "the operator's policy governs the plugin",
			grant:              []byte("egress:\n  schemes: [https]\n"),
			wantLoopbackDenied: true,
		},
		{
			// The same plugin, the same launch path, a policy that says
			// something different: what the plugin enforces tracks the file the
			// operator wrote rather than a default compiled into either side.
			name:  "a loosened policy reaches it too",
			grant: []byte("egress:\n  schemes: [https]\n  allow_loopback: true\n"),
		},
		{
			// An operator's zero-byte --egress-policy file. The worker parses
			// that empty document and registers the built-in http task under
			// what it builds, so a plugin that read the same deployment as
			// ungranted denied where the built-in task allowed. The expectation
			// is computed rather than written down, because the claim is parity
			// with the host's own answer, not agreement with a posture this
			// test happens to believe in.
			name:               "an explicitly empty policy is a policy on both sides",
			grant:              []byte{},
			wantLoopbackDenied: hostDeniesLoopbackUnderAnEmptyPolicy(t),
		},
		{
			name:        "no policy is refused rather than defaulted",
			grant:       nil,
			wantRefusal: true,
		},
		{
			// The plugin still launched, and its task still ran: reaching the
			// task at all is the assertion. The SDK captures the grant while it
			// reads the launch environment, and a grant that does not parse
			// must not turn into a plugin the host refuses — a plugin that
			// never touches the network has no use for it, and failing the
			// launch would make a bad policy file break tasks that do not
			// depend on one. The refusal belongs to whoever asks for a policy,
			// which is where it arrives.
			name:        "a malformed grant refuses at the ask, not at the launch",
			grant:       []byte("egress:\n  schemes: [https\n"),
			wantRefusal: true,
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()

			cfg := testConfig(t, pluginDir(t, "egress-grant"))
			cfg.EgressPolicy = test.grant

			host := openHost(t, cfg)
			require.Len(t, host.TaskDefs(), 1)

			outputs, err := host.TaskDefs()[0].Fn(t.Context(), nil, nil)
			require.NoError(t, err)

			refusal := outputs.GetNamedValues()["refusal"].GetLiteral().GetStringValue()
			if test.wantRefusal {
				assert.Contains(t, refusal, "FLOWSTATE_EGRESS_POLICY_B64",
					"a plugin launched with no grant was not refused by name")
				return
			}
			require.Empty(t, refusal, "the granted policy did not reach the plugin process")

			loopback := outputs.GetNamedValues()["loopback"].GetLiteral().GetStringValue()
			if test.wantLoopbackDenied {
				assert.NotEmpty(t, loopback, "the plugin permitted what the operator's policy denies")
			} else {
				assert.Empty(t, loopback, "the plugin denied what the operator's policy permits: %s", loopback)
			}
		})
	}
}

// hostDeniesLoopbackUnderAnEmptyPolicy makes the two calls applyEgressPolicy
// makes (cmd/flow/egress.go) for a zero-byte policy file, and reports what the
// built-in http task would then decide about the address the fixture checks.
//
// It is the host half of the parity claim. Stating it as a constant would make
// the test agree with whatever netpolicy's empty-document default happens to be
// today rather than with whatever the built-in task is actually running.
func hostDeniesLoopbackUnderAnEmptyPolicy(t *testing.T) bool {
	t.Helper()

	cfg, err := netpolicy.ParseConfig(nil)
	require.NoError(t, err)

	policy, err := cfg.Policy()
	require.NoError(t, err)

	return policy.CheckAddr(netip.MustParseAddrPort("127.0.0.1:443")) != nil
}

// runEgressIdentityPlugin is a real SDK plugin that makes an ordinary outbound
// request the documented way — [sdk.HTTPClient] with the task's own context —
// and reports what happened.
//
// It exists because that is the exact pattern PLUGINS.md teaches and the exact
// pattern that under-enforced: netpolicy reads the caller identity from its own
// context key, and a task context carried only the SDK's. A fixture that bridged
// the identity by hand, as the first-party plugins do on their own dial paths,
// would prove the bridge rather than the SDK.
func runEgressIdentityPlugin() int {
	fetch := func(ctx context.Context, inputs map[string]*flowstatev1.Value, _ *flowstatev1.Scope) (*flowstatev1.Node_Outputs, error) {
		outputs := map[string]*flowstatev1.Value{}

		client, err := sdk.HTTPClient()
		if err != nil {
			outputs["error"] = flowstatev1.NewLiteral(err.Error())
			return &flowstatev1.Node_Outputs{NamedValues: outputs}, nil
		}

		// The task's own context, unmodified. Anything this fixture added to it
		// would be the thing under test doing the work.
		request, err := http.NewRequestWithContext(ctx, http.MethodGet,
			inputs["message"].GetLiteral().GetStringValue(), nil)
		if err != nil {
			outputs["error"] = flowstatev1.NewLiteral(err.Error())
			return &flowstatev1.Node_Outputs{NamedValues: outputs}, nil
		}

		response, err := client.Do(request)
		if err != nil {
			outputs["error"] = flowstatev1.NewLiteral(err.Error())
			return &flowstatev1.Node_Outputs{NamedValues: outputs}, nil
		}
		defer response.Body.Close()

		outputs["error"] = flowstatev1.NewLiteral("")
		return &flowstatev1.Node_Outputs{NamedValues: outputs}, nil
	}

	err := sdk.Run(context.Background(), sdk.Plugin{
		Name:        "egress-identity",
		Version:     "0.0.1",
		Description: "fetches a URL through the SDK's governed client",
		Tasks: []sdk.Task{{
			Name:   "fetch",
			Input:  &flowstatev1.Task_Log_Inputs{},
			Output: &flowstatev1.Task_Log_Outputs{},
			Fn:     fetch,
		}},
	})
	if err != nil {
		fmt.Fprintf(os.Stderr, "egress-identity fixture: %v\n", err)
		return 1
	}

	return 0
}

// TestTheGrantsIdentityRulesGovernThePluginsOwnRequests is the half of the grant
// a policy snapshot alone does not deliver.
//
// An operator's `identity.*` rule is evaluated against whatever netpolicy finds
// on the request's context, under netpolicy's own key. The SDK installed the
// caller under its key and nothing under netpolicy's, so a plugin following the
// documented pattern had every tenant rule evaluated against the zero identity:
// `deny: ['identity.namespace == "team-b"']` simply did not fire for a team-b
// workload, the request succeeded, and nothing anywhere reported that a rule had
// been skipped. That is a policy failing open, quietly, on the path an author is
// told to use.
//
// Both directions are here because only the pair distinguishes "the identity
// arrived" from "everything is denied": the same plugin, the same grant, the
// same URL, two namespaces, opposite outcomes.
func TestTheGrantsIdentityRulesGovernThePluginsOwnRequests(t *testing.T) {
	t.Parallel()

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusNoContent)
	}))
	t.Cleanup(server.Close)

	// Loopback is permitted, so the only thing that can refuse this request is
	// the tenant rule — which is what makes a denial evidence about identity
	// rather than about the destination.
	grant := []byte("egress:\n  schemes: [http, https]\n  allow_loopback: true\n" +
		"  deny: ['identity.namespace == \"team-b\"']\n")

	for _, test := range []struct {
		name      string
		namespace string

		// wantDenied is whether the operator's rule should refuse this tenant.
		wantDenied bool
	}{
		{
			name:       "the denied tenant is refused",
			namespace:  "team-b",
			wantDenied: true,
		},
		{
			// The falsifier. Without it a bridge that installed a permanently
			// empty identity, or a policy that denied everything, would pass
			// the case above.
			name:      "another tenant is permitted",
			namespace: "team-a",
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()

			cfg := testConfig(t, pluginDir(t, "egress-identity"))
			cfg.EgressPolicy = grant

			host := openHost(t, cfg)
			require.Len(t, host.TaskDefs(), 1)

			ctx := NewContextWithIdentity(t.Context(), &flowstatev1.WorkloadIdentity{
				Subject:   "workflow/probe",
				Issuer:    "https://issuer.invalid",
				Namespace: test.namespace,
			})

			outputs, err := host.TaskDefs()[0].Fn(ctx,
				map[string]*flowstatev1.Value{"message": flowstatev1.NewLiteral(server.URL)}, nil)
			require.NoError(t, err)

			failure := outputs.GetNamedValues()["error"].GetLiteral().GetStringValue()
			if test.wantDenied {
				assert.Contains(t, failure, "denied by egress policy",
					"the operator's tenant rule did not refuse a request from the tenant it names")
				return
			}
			assert.Empty(t, failure, "a permitted tenant was refused: %s", failure)
		})
	}
}

// TestTheProxyGrantReachesTheLaunchedPluginProcess is the end-to-end half of the
// proxy grant: not that the variable is in a slice, but that it survives exec
// and is there for http.ProxyFromEnvironment to find.
//
// It matters that this is a real process. The claim the finding disproved was
// about what a plugin's environment contains after being built from nothing, and
// a test that only inspected what the host assembled would be checking the same
// belief that was wrong.
func TestTheProxyGrantReachesTheLaunchedPluginProcess(t *testing.T) {
	// Not t.Parallel(): t.Setenv, since the worker's own environment is the
	// source of what gets granted.
	const proxy = "http://proxy.invalid:3128"
	t.Setenv("HTTP_PROXY", proxy)

	for _, test := range []struct {
		name      string
		grant     []byte
		wantProxy string
	}{
		{
			name:      "a proxying policy carries the worker's proxy into the plugin",
			grant:     []byte("egress:\n  schemes: [https]\n  proxy_from_environment: true\n"),
			wantProxy: proxy,
		},
		{
			// The same worker, the same variable set, one line of policy
			// different: the plugin sees nothing, because the environment is
			// built from nothing and this deployment granted no proxy.
			name:  "a policy that does not proxy leaves it out",
			grant: []byte("egress:\n  schemes: [https]\n"),
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			cfg := testConfig(t, pluginDir(t, "egress-grant"))
			cfg.EgressPolicy = test.grant

			host := openHost(t, cfg)
			require.Len(t, host.TaskDefs(), 1)

			outputs, err := host.TaskDefs()[0].Fn(t.Context(), nil, nil)
			require.NoError(t, err)

			got := outputs.GetNamedValues()["http_proxy"].GetLiteral().GetStringValue()
			assert.Equal(t, test.wantProxy, got,
				"the launched plugin's own HTTP_PROXY is not what this policy grants")
		})
	}
}

// runEgressResolverPlugin is a real SDK plugin whose *secret resolver* reaches
// the network through the documented constructor.
//
// A resolver is as able to make an outbound request as a task is — that is what
// a network-backed secret backend is — and it runs under a different handler,
// which is exactly why the identity install had to stop being a property of the
// task path.
func runEgressResolverPlugin() int {
	resolve := func(ctx context.Context, req sdk.SecretRequest) (sdk.SecretResponse, error) {
		client, err := sdk.HTTPClient()
		if err != nil {
			return sdk.SecretResponse{}, sdk.Failed("egress: %v", err)
		}

		// The resolver's own context, unmodified.
		request, err := http.NewRequestWithContext(ctx, http.MethodGet, os.Getenv("FLOWSTATE_TEST_BACKEND"), nil)
		if err != nil {
			return sdk.SecretResponse{}, sdk.Failed("request: %v", err)
		}

		response, err := client.Do(request)
		if err != nil {
			return sdk.SecretResponse{}, sdk.Failed("fetching the secret: %v", err)
		}
		defer response.Body.Close()

		return sdk.SecretResponse{Value: []byte("resolved")}, nil
	}

	err := sdk.Run(context.Background(), sdk.Plugin{
		Name:        "egress-resolver",
		Version:     "0.0.1",
		Description: "resolves a secret by fetching it through the SDK's governed client",
		Secrets: &sdk.Secrets{
			Schemes: []string{"egressresolver"},
			Resolve: resolve,
		},
	})
	if err != nil {
		fmt.Fprintf(os.Stderr, "egress-resolver fixture: %v\n", err)
		return 1
	}

	return 0
}

// TestTheGrantsIdentityRulesGovernASecretResolversRequests is the same claim as
// the task-path test, at the entry point that did not have it.
//
// The wire carries the identity to a resolver — the host sets it in
// secrets.go's Resolve — and the SDK handed the resolver its untouched RPC
// context, so netpolicy evaluated every `identity.*` rule against the zero
// identity. A network-backed resolver invoked for team-b therefore passed
// `deny: ['identity.namespace == "team-b"']`, fetching the tenant's secret from
// a destination the operator's rule was written to keep it away from. Nothing
// reported it, because the rule did evaluate — against a value that was not
// there.
//
// Both directions, for the reason the task-path test has both: the denial alone
// would pass on an install that always produced an empty identity.
func TestTheGrantsIdentityRulesGovernASecretResolversRequests(t *testing.T) {
	t.Parallel()

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusNoContent)
	}))
	t.Cleanup(server.Close)

	grant := []byte("egress:\n  schemes: [http, https]\n  allow_loopback: true\n" +
		"  deny: ['identity.namespace == \"team-b\"']\n")

	for _, test := range []struct {
		name       string
		namespace  string
		wantDenied bool
	}{
		{
			name:       "the denied tenant's resolver is refused",
			namespace:  "team-b",
			wantDenied: true,
		},
		{
			name:      "another tenant's resolver is permitted",
			namespace: "team-a",
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()

			cfg := testConfig(t, pluginDir(t, "egress-resolver"))
			cfg.EgressPolicy = grant
			cfg.Env = []string{"FLOWSTATE_TEST_BACKEND=" + server.URL}

			host := openHost(t, cfg)
			providers := host.SecretProviders()
			require.Len(t, providers, 1)

			ctx := NewContextWithIdentity(t.Context(), &flowstatev1.WorkloadIdentity{
				Subject:   "workflow/probe",
				Issuer:    "https://issuer.invalid",
				Namespace: test.namespace,
			})

			secret, err := providers[0].Resolve(ctx, secrets.Request{
				Namespace: test.namespace,
				Ref:       secrets.NewRef("egressresolver", "k"),
			})

			if test.wantDenied {
				require.Error(t, err,
					"the operator's tenant rule did not refuse a resolver acting for the tenant it names")
				assert.Contains(t, err.Error(), "denied by egress policy")
				return
			}

			require.NoError(t, err, "a permitted tenant's resolver was refused")
			assert.True(t, secret.EqualString("resolved"))
		})
	}
}
