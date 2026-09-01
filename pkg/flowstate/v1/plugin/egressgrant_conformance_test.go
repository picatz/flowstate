package plugin

import (
	"context"
	"fmt"
	"net/netip"
	"os"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	flowstatev1 "github.com/picatz/flowstate/pkg/flowstate/v1"
	"github.com/picatz/flowstate/pkg/flowstate/v1/plugin/sdk"
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
			name:        "no policy is refused rather than defaulted",
			grant:       nil,
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
