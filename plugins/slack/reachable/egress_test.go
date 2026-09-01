package reachable

import (
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"

	flowstatev1 "github.com/picatz/flowstate/pkg/flowstate/v1"
	"github.com/picatz/flowstate/pkg/flowstate/v1/auth"
	"github.com/picatz/flowstate/pkg/flowstate/v1/plugin"
	"github.com/picatz/flowstate/pkg/flowstate/v1/secrets"
)

// deniedHost is where every slack.post goes: the endpoint is a constant in the
// plugin, not an input, so an operator's only lever over it is the egress
// policy. Denying it by name means the refusal happens before any name
// resolution, so nothing here reaches the internet.
const deniedHost = "slack.com"

// operatorPolicyDenying is the file an operator writes to stop this plugin
// reaching Slack — a deployment deciding that this worker may not post, without
// uninstalling the plugin.
const operatorPolicyDenying = "egress:\n  deny:\n    - host == \"" + deniedHost + "\"\n"

// TestAnOperatorDenyRuleStopsASlackPost is the accepting-posture plugin's other
// half: `slack` takes the deployment default rather than refusing it (#1332,
// point 7), which is only safe because an operator who does write a policy is
// obeyed. This is that direction, through a real launched plugin process.
//
// The call carries a production identity, because slack.post refuses a
// rehearsal write before it looks at anything else — that gate is #1320's and is
// not what this test is about, but it has to be passed to reach the one that is.
func TestAnOperatorDenyRuleStopsASlackPost(t *testing.T) {
	if testing.Short() {
		t.Skip("builds a real plugin binary; skipped under -short, run in CI and by `make check`")
	}
	if _, err := exec.LookPath("go"); err != nil {
		t.Skip("the Go toolchain is not available, so this plugin cannot be built")
	}

	dir := t.TempDir()
	binaryPath := filepath.Join(dir, plugin.BinaryPrefix+"slack")
	buildPlugin(t, binaryPath)

	host := openHost(t, plugin.Config{
		SearchPath:          []string{dir},
		HandshakeTimeout:    10 * time.Second,
		DescribeTimeout:     10 * time.Second,
		CallTimeout:         30 * time.Second,
		ShutdownGrace:       5 * time.Second,
		DisableHealthChecks: true,

		// The field, not a hand-composed Env entry: the host owns encoding the
		// grant, and this is the launch path a worker actually takes.
		EgressPolicy: []byte(operatorPolicyDenying),
	})

	defs := host.TaskDefs()
	if len(defs) != 1 || defs[0].Name != "slack.post" {
		t.Fatalf("the launched plugin does not offer exactly slack.post: %v", defs)
	}

	ctx := plugin.NewContextWithIdentity(t.Context(), &flowstatev1.WorkloadIdentity{
		Subject: "https://issuer.example.com#worker",
		Mode:    flowstatev1.WorkloadIdentityMode_WORKLOAD_IDENTITY_MODE_PRODUCTION,
	})
	ctx = flowstatev1.ContextWithTaskRuntime(ctx, taskRuntimeResolvingTheTestToken(t))

	// A whole secret reference, not a literal: the host refuses a literal for
	// this input before dispatch, which is a different control (#1320's
	// credential rule) this test has to satisfy rather than test. Destination
	// authorization and credential release stay separate all the way down —
	// this call has a credential and is refused anyway, on the destination.
	_, err := defs[0].Fn(ctx, map[string]*flowstatev1.Value{
		"channel":     flowstatev1.NewValue("C0123456789"),
		"text":        flowstatev1.NewValue("a message the operator's policy should stop"),
		"message_key": flowstatev1.NewValue("018f0e6c-7b42-7cc1-8a31-65c0f8758f4a"),
		"token": {Kind: &flowstatev1.Value_SecretRef{SecretRef: &flowstatev1.SecretRef{
			Scheme: "env", Name: testTokenName,
		}}},
	}, nil)
	if err == nil {
		t.Fatal("slack.post reached a host the operator's egress policy denies")
	}
	// The plugin's own wording for a netpolicy denial (post.go), which is
	// deliberately host-free: the destination came from this plugin, but the
	// message an author reads must not become a place a denied name is echoed.
	if !strings.Contains(err.Error(), "deployment egress policy denied slack.post") {
		t.Fatalf("slack.post failed for some reason other than the operator's policy: %v", err)
	}
}

// testTokenName is the reference the call above passes, and the environment
// variable behind it is FLOWSTATE_SECRET_ plus this name — the env provider's
// own prefix, so nothing but a variable named for this test is readable.
const testTokenName = "SLACK_EGRESS_TEST_TOKEN"

// taskRuntimeResolvingTheTestToken is the smallest runtime that lets a whole
// secret reference resolve: a store over the process environment, and a policy
// that permits this one reference.
//
// The host refuses a literal for a required secret input before it dispatches,
// so a task that takes a credential cannot be reached without one — which is a
// control worth having and an obstacle for a test about somewhere else. The
// policy is written as narrowly as the test needs rather than as `true`, so it
// cannot quietly become a fixture that permits everything.
func taskRuntimeResolvingTheTestToken(t *testing.T) flowstatev1.TaskRuntime {
	t.Helper()

	t.Setenv(secrets.DefaultEnvPrefix+testTokenName, "xoxb-not-a-real-token")

	provider, err := secrets.NewEnvProvider(secrets.WithEnvAllow(testTokenName))
	if err != nil {
		t.Fatalf("building the env secret provider: %v", err)
	}
	store, err := secrets.NewStore(provider)
	if err != nil {
		t.Fatalf("building the secret store: %v", err)
	}
	policy, err := auth.SecretAccessPolicy{
		Allow: []string{`secret.scheme == "env" && secret.name == "` + testTokenName + `"`},
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
		Step: auth.StepRef{Workflow: "slack-egress", Run: "egress-test", Step: "notify"},
	}
}
