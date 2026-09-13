// Package reachable proves that examples/plugins/scim/workflow.yaml can reach
// the "scim" plugin: the property AGENTS.md requires of every capability, and
// one a README can otherwise only assert.
//
// Its own package for the reason plugins/vcs records: a test beside main.go
// would register scim/v1/scim.proto in this binary's global proto registry
// before the test ran, and a process holding the schema cannot tell a working
// descriptor reconstruction from a hit against its own registry.
package reachable

import (
	"os/exec"
	"path/filepath"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/picatz/flowstate/internal/pluginreachtest"
	flowstatev1 "github.com/picatz/flowstate/pkg/flowstate/v1"
	"github.com/picatz/flowstate/pkg/flowstate/v1/auth"
	"github.com/picatz/flowstate/pkg/flowstate/v1/flowfile"
	"github.com/picatz/flowstate/pkg/flowstate/v1/plugin"
	"github.com/picatz/flowstate/pkg/flowstate/v1/secrets"
)

const (
	scimModule = "github.com/picatz/flowstate/plugins/scim"
	exampleDir = "../../../examples/plugins/scim"

	// deniedHost is the provider the operator policy below refuses, by name, so
	// the refusal happens before any name resolution and this test reaches no
	// network.
	deniedHost = "example.okta.com"
)

// operatorPolicyDenying is the file an operator writes to stop this worker
// calling one identity provider - the deployment deciding which directory a
// workflow may read and write, without uninstalling the plugin.
const operatorPolicyDenying = "egress:\n  deny:\n    - host == \"" + deniedHost + "\"\n"

// TestAFlowfileCanNameTheSCIMPluginsTasks registers into the default registry,
// which is a one-way door, so it is deliberately the only test in this binary
// that does.
func TestAFlowfileCanNameTheSCIMPluginsTasks(t *testing.T) {
	if testing.Short() {
		t.Skip("builds a real plugin binary; skipped under -short, run in CI and by `make check`")
	}
	if _, err := exec.LookPath("go"); err != nil {
		t.Skip("the Go toolchain is not available, so this plugin cannot be built")
	}

	dir := t.TempDir()
	binaryPath := filepath.Join(dir, plugin.BinaryPrefix+"scim")
	pluginreachtest.BuildPlugin(t, scimModule, binaryPath)

	source := pluginreachtest.ReadFile(t, filepath.Join(exampleDir, "workflow.yaml"))
	tasks := []string{"scim.user_get", "scim.user_list", "scim.user_deactivate"}

	for _, name := range tasks {
		if _, ok := flowstatev1.LookupTask(name); ok {
			t.Fatalf("%q is in the default registry before this test registered it, so nothing below "+
				"distinguishes a working seam from a task that was always there", name)
		}
	}

	before, err := flowfile.ValidateSource(source)
	if err != nil {
		t.Fatalf("ValidateSource: %v", err)
	}
	if len(before) == 0 {
		t.Fatal("the validator accepted a step naming a task no registry holds")
	}

	host := pluginreachtest.OpenHost(t, plugin.Config{
		SearchPath:          []string{dir},
		HandshakeTimeout:    10 * time.Second,
		DescribeTimeout:     10 * time.Second,
		CallTimeout:         10 * time.Second,
		HealthTimeout:       5 * time.Second,
		ShutdownGrace:       5 * time.Second,
		DisableHealthChecks: true,
		Logger:              pluginreachtest.Logger(t),
	})

	if err := host.Register(flowstatev1.DefaultRegistry(), nil); err != nil {
		t.Fatalf("Register: %v", err)
	}

	t.Run("the validator accepts the real example file", func(t *testing.T) {
		after, err := flowfile.ValidateSource(source)
		if err != nil {
			t.Fatalf("ValidateSource: %v", err)
		}
		if len(after) != 0 {
			t.Errorf("the example does not validate against the descriptors the plugin shipped:\n%s",
				pluginreachtest.DiagnosticText(after))
		}
	})

	t.Run("a directory credential may only be written as a secret reference", func(t *testing.T) {
		for _, def := range host.TaskDefs() {
			if !slices.Contains(def.RequiredSecretInputs, "token") {
				t.Errorf("%s does not require token to be a secret reference, so a directory credential could "+
					"be written into a Flowfile as a literal", def.Name)
			}
		}
	})
}

// TestAnOperatorDenyRuleStopsADirectoryCall is the other half of this plugin's
// accepting posture toward the deployment default: an operator who writes a
// policy is obeyed, on the real dial path, through a launched process.
func TestAnOperatorDenyRuleStopsADirectoryCall(t *testing.T) {
	if testing.Short() {
		t.Skip("builds a real plugin binary; skipped under -short, run in CI and by `make check`")
	}
	if _, err := exec.LookPath("go"); err != nil {
		t.Skip("the Go toolchain is not available, so this plugin cannot be built")
	}

	dir := t.TempDir()
	binaryPath := filepath.Join(dir, plugin.BinaryPrefix+"scim")
	pluginreachtest.BuildPlugin(t, scimModule, binaryPath)

	host := pluginreachtest.OpenHost(t, plugin.Config{
		SearchPath:          []string{dir},
		HandshakeTimeout:    10 * time.Second,
		DescribeTimeout:     10 * time.Second,
		CallTimeout:         30 * time.Second,
		ShutdownGrace:       5 * time.Second,
		DisableHealthChecks: true,
		Logger:              pluginreachtest.Logger(t),
		EgressPolicy:        []byte(operatorPolicyDenying),
	})

	var get *flowstatev1.TaskDef
	for _, def := range host.TaskDefs() {
		if def.Name == "scim.user_get" {
			get = &def
			break
		}
	}
	if get == nil {
		t.Fatal("the launched plugin offers no scim.user_get, so nothing below tests the policy")
	}

	// The host refuses a literal for a required secret input before dispatch,
	// so the call has to carry a whole reference and a runtime able to resolve
	// it. That is a control this test satisfies rather than tests: the subject
	// here is the destination, and this call is refused on it while holding a
	// perfectly good credential, which is what keeps destination authorization
	// and credential release separate all the way down.
	ctx := flowstatev1.ContextWithTaskRuntime(t.Context(), taskRuntimeResolvingTheTestToken(t))

	_, err := get.Fn(ctx, map[string]*flowstatev1.Value{
		"base_url": flowstatev1.NewValue("https://" + deniedHost + "/scim/v2"),
		"id":       flowstatev1.NewValue("2819c223"),
		"token": {Kind: &flowstatev1.Value_SecretRef{SecretRef: &flowstatev1.SecretRef{
			Scheme: "env", Name: testTokenName,
		}}},
	}, nil)
	if err == nil {
		t.Fatal("scim.user_get called a provider the operator's egress policy denies")
	}
	// The plugin's own wording for a netpolicy denial, which is deliberately
	// host-free in the message an author reads.
	if !strings.Contains(err.Error(), "egress policy denied reaching "+deniedHost) {
		t.Fatalf("scim.user_get failed for some reason other than the operator's policy: %v", err)
	}
}

// testTokenName is the reference the call above passes; the variable behind it
// is the env provider's own prefix plus this name, so nothing but a variable
// named for this test is readable.
const testTokenName = "SCIM_REACHABLE_TEST_TOKEN"

// taskRuntimeResolvingTheTestToken is the smallest runtime that lets a whole
// secret reference resolve: a store over the process environment, and a policy
// that permits this one reference and nothing else.
func taskRuntimeResolvingTheTestToken(t *testing.T) flowstatev1.TaskRuntime {
	t.Helper()

	t.Setenv(secrets.DefaultEnvPrefix+testTokenName, "not-a-real-directory-token")

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
		Step: auth.StepRef{Workflow: "scim-egress", Run: "egress-test", Step: "under_review"},
	}
}
