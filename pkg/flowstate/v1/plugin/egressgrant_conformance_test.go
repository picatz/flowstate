package plugin

import (
	"context"
	"fmt"
	"net/netip"
	"os"
	"strconv"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	flowstatev1 "github.com/picatz/flowstate/pkg/flowstate/v1"
	"github.com/picatz/flowstate/pkg/flowstate/v1/netpolicy"
	"github.com/picatz/flowstate/pkg/flowstate/v1/plugin/sdk"
)

// probeAddresses are the address categories the deployment's default policy
// separates: one it permits, and the three internal ones it exists to deny.
//
// Named here rather than inline so the plugin fixture and the expectations
// computed for it are reading one list; a category checked on one side and not
// the other is a posture nobody compared.
var probeAddresses = map[string]netip.AddrPort{
	"loopback": netip.MustParseAddrPort("127.0.0.1:443"),
	"private":  netip.MustParseAddrPort("10.0.0.1:443"),
	"metadata": netip.MustParseAddrPort("169.254.169.254:80"),
	"public":   netip.MustParseAddrPort("93.184.216.34:443"),
}

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

		isDefault, err := sdk.EgressPolicyIsDeploymentDefault()
		if err != nil {
			outputs["refusal"] = flowstatev1.NewLiteral(err.Error())
			return &flowstatev1.Node_Outputs{NamedValues: outputs}, nil
		}
		outputs["deployment_default"] = flowstatev1.NewLiteral(strconv.FormatBool(isDefault))

		// One output per address category the deployment default separates, so
		// a case can assert the whole posture rather than one answer. Loopback
		// is the one the operator policies below disagree about, which is what
		// tells "some policy arrived" from "the operator's policy arrived".
		for name, addr := range probeAddresses {
			if err := policy.CheckAddr(addr); err != nil {
				outputs[name] = flowstatev1.NewLiteral(err.Error())
			} else {
				outputs[name] = flowstatev1.NewLiteral("")
			}
		}

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

		// wantDeploymentDefault expects the plugin to report the grant as the
		// worker's own default, and its posture to match that default's on
		// every address in probeAddresses.
		wantDeploymentDefault bool
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
			// The launch every default worker makes (#1332, point 6). The
			// plugin is governed by the same policy the worker's own built-in
			// http task runs under, and can see that nobody wrote it — which is
			// what lets sql refuse a database here while git, vcs, github and
			// slack reach public hosts, on the one grant.
			name:                  "a worker with no operator policy grants its own default",
			grant:                 flowstatev1.DefaultEgressPolicyDocument(),
			wantDeploymentDefault: true,
			wantLoopbackDenied:    flowstatev1.DefaultEgressPolicy().CheckAddr(probeAddresses["loopback"]) != nil,
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

			assert.Equal(t, strconv.FormatBool(test.wantDeploymentDefault),
				outputs.GetNamedValues()["deployment_default"].GetLiteral().GetStringValue(),
				"the plugin cannot tell whether an operator decided this policy, so it cannot take a posture toward the default")

			if !test.wantDeploymentDefault {
				return
			}

			// The whole posture, computed from the policy this worker's own
			// http task is running under rather than written down: what makes
			// the default grant right is that it is that policy, not that it
			// resembles one. A test naming the categories itself would keep
			// agreeing after the built-in default moved.
			workerDefault := flowstatev1.DefaultEgressPolicy()
			for name, addr := range probeAddresses {
				denied := outputs.GetNamedValues()[name].GetLiteral().GetStringValue() != ""
				assert.Equalf(t, workerDefault.CheckAddr(addr) != nil, denied,
					"the plugin's answer for a %s address differs from the worker's own default", name)
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
