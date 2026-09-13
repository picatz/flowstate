// Package reachable proves that examples/plugins/jose/workflow.yaml can reach
// the "jose" plugin, and that an unconfigured one verifies nothing.
//
// Its own package for the reason plugins/vcs records: a test beside main.go
// would register jose/v1/jose.proto in this binary's global proto registry
// before the test ran.
package reachable

import (
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/picatz/flowstate/internal/pluginreachtest"
	flowstatev1 "github.com/picatz/flowstate/pkg/flowstate/v1"
	"github.com/picatz/flowstate/pkg/flowstate/v1/flowfile"
	"github.com/picatz/flowstate/pkg/flowstate/v1/plugin"
)

const (
	joseModule = "github.com/picatz/flowstate/plugins/jose"
	exampleDir = "../../../examples/plugins/jose"

	// trustEnv is the variable an operator points at a trust policy with,
	// spelled here because this package imports nothing of the plugin's own.
	trustEnv = "FLOWSTATE_JOSE_TRUST"
)

// TestAFlowfileCanNameTheJOSEPluginsTask registers into the default registry, a
// one-way door, so it is the only test in this binary that does.
func TestAFlowfileCanNameTheJOSEPluginsTask(t *testing.T) {
	if testing.Short() {
		t.Skip("builds a real plugin binary; skipped under -short, run in CI and by `make check`")
	}
	if _, err := exec.LookPath("go"); err != nil {
		t.Skip("the Go toolchain is not available, so this plugin cannot be built")
	}

	dir := t.TempDir()
	binaryPath := filepath.Join(dir, plugin.BinaryPrefix+"jose")
	pluginreachtest.BuildPlugin(t, joseModule, binaryPath)

	source := pluginreachtest.ReadFile(t, filepath.Join(exampleDir, "workflow.yaml"))

	if _, ok := flowstatev1.LookupTask("jose.verify"); ok {
		t.Fatal("jose.verify is in the default registry before this test registered it")
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
		EnvByPlugin: map[string][]string{
			"jose": {trustEnv + "=" + mustAbs(t, filepath.Join(exampleDir, "trust-policy.yaml"))},
		},
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

	t.Run("the example's trust policy is one the plugin accepts", func(t *testing.T) {
		// The plugin builds its verifier at startup and reports unhealthy when
		// it cannot, so this asks the running process whether the document
		// beside the example actually works.
		for name, health := range host.CheckHealth(t.Context()) {
			if health.Status != plugin.HealthServing {
				t.Errorf("%s is unhealthy with the example's own trust policy: %v (%s)", name, health.Err, health.Message)
			}
		}
	})
}

// TestAnUnconfiguredPluginVerifiesNothing is the fail-closed direction through a
// real launched process: with no trust policy there is no issuer to believe, and
// a call says so rather than decoding the token and trusting its own claims.
func TestAnUnconfiguredPluginVerifiesNothing(t *testing.T) {
	if testing.Short() {
		t.Skip("builds a real plugin binary; skipped under -short, run in CI and by `make check`")
	}
	if _, err := exec.LookPath("go"); err != nil {
		t.Skip("the Go toolchain is not available, so this plugin cannot be built")
	}

	dir := t.TempDir()
	binaryPath := filepath.Join(dir, plugin.BinaryPrefix+"jose")
	pluginreachtest.BuildPlugin(t, joseModule, binaryPath)

	host := pluginreachtest.OpenHost(t, plugin.Config{
		SearchPath:          []string{dir},
		HandshakeTimeout:    10 * time.Second,
		DescribeTimeout:     10 * time.Second,
		CallTimeout:         10 * time.Second,
		ShutdownGrace:       5 * time.Second,
		DisableHealthChecks: true,
		Logger:              pluginreachtest.Logger(t),
	})

	defs := host.TaskDefs()
	if len(defs) != 1 || defs[0].Name != "jose.verify" {
		t.Fatalf("the launched plugin does not offer exactly jose.verify: %v", defs)
	}

	_, err := defs[0].Fn(t.Context(), map[string]*flowstatev1.Value{
		"token": flowstatev1.NewValue("a.b.c"),
	}, nil)
	if err == nil {
		t.Fatal("an unconfigured jose plugin verified something")
	}
	if !strings.Contains(err.Error(), trustEnv) {
		t.Fatalf("the refusal does not name what an operator would set: %v", err)
	}
}

// mustAbs resolves a path for the launched plugin, which runs with a working
// directory of its own.
func mustAbs(t *testing.T, path string) string {
	t.Helper()

	absolute, err := filepath.Abs(path)
	if err != nil {
		t.Fatalf("resolving %q: %v", path, err)
	}
	return absolute
}
