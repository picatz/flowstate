// Package reachable proves that examples/plugins/oidc/workflow.yaml can reach
// the "oidc" plugin's secret scheme, and that an unconfigured one mints
// nothing.
//
// A secret provider's reachability is a different seam from a task's: what a
// Flowfile names is a scheme, and what has to work is the host registering this
// plugin as the answer for it. That is what this package exercises, through a
// real launched process.
package reachable

import (
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/picatz/flowstate/internal/pluginreachtest"
	"github.com/picatz/flowstate/pkg/flowstate/v1/plugin"
	"github.com/picatz/flowstate/pkg/flowstate/v1/secrets"
)

const (
	oidcModule = "github.com/picatz/flowstate/plugins/oidc"
	exampleDir = "../../../examples/plugins/oidc"

	// providersEnv is the variable an operator points at a providers file with,
	// spelled here because this package imports nothing of the plugin's own.
	providersEnv = "FLOWSTATE_OIDC_PROVIDERS"
)

// TestTheSchemeAFlowfileNamesIsTheOneThisPluginAnswersFor is the seam: a
// `${secret('oidc:…')}` reference resolves through whatever the host registered
// for "oidc", and this proves the launched plugin is that.
func TestTheSchemeAFlowfileNamesIsTheOneThisPluginAnswersFor(t *testing.T) {
	if testing.Short() {
		t.Skip("builds a real plugin binary; skipped under -short, run in CI and by `make check`")
	}
	if _, err := exec.LookPath("go"); err != nil {
		t.Skip("the Go toolchain is not available, so this plugin cannot be built")
	}

	dir := t.TempDir()
	binaryPath := filepath.Join(dir, plugin.BinaryPrefix+"oidc")
	pluginreachtest.BuildPlugin(t, oidcModule, binaryPath)

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
			"oidc": {providersEnv + "=" + mustAbs(t, filepath.Join(exampleDir, "providers.yaml"))},
		},
	})

	providers := host.SecretProviders()
	if len(providers) != 1 {
		t.Fatalf("the launched plugin offers %d secret providers, want the one a Flowfile names", len(providers))
	}
	if scheme := providers[0].Scheme(); scheme != "oidc" {
		t.Fatalf("the provider answers for %q, not the scheme the example writes", scheme)
	}

	if defs := host.TaskDefs(); len(defs) != 0 {
		t.Errorf("this plugin offers tasks %v; minting belongs behind the secret boundary, not in front of it", defs)
	}

	t.Run("the example's providers file is one the plugin accepts", func(t *testing.T) {
		for name, health := range host.CheckHealth(t.Context()) {
			if health.Status != plugin.HealthServing {
				t.Errorf("%s is unhealthy with the example's own providers file: %v (%s)", name, health.Err, health.Message)
			}
		}
	})

	t.Run("a reference to a provider nobody configured is refused by name", func(t *testing.T) {
		_, err := providers[0].Resolve(t.Context(), secrets.Request{Ref: secrets.NewRef("oidc", "not-configured")})
		if err == nil {
			t.Fatal("a reference to an unconfigured provider resolved")
		}
		if !strings.Contains(err.Error(), "billing-api") {
			t.Errorf("the refusal does not name what this worker does configure: %v", err)
		}
	})
}

// TestAnUnconfiguredPluginMintsNothing is the fail-closed direction: with no
// providers file there is nothing to mint from, and a reference says so rather
// than reaching for an authorization server.
func TestAnUnconfiguredPluginMintsNothing(t *testing.T) {
	if testing.Short() {
		t.Skip("builds a real plugin binary; skipped under -short, run in CI and by `make check`")
	}
	if _, err := exec.LookPath("go"); err != nil {
		t.Skip("the Go toolchain is not available, so this plugin cannot be built")
	}

	dir := t.TempDir()
	binaryPath := filepath.Join(dir, plugin.BinaryPrefix+"oidc")
	pluginreachtest.BuildPlugin(t, oidcModule, binaryPath)

	host := pluginreachtest.OpenHost(t, plugin.Config{
		SearchPath:          []string{dir},
		HandshakeTimeout:    10 * time.Second,
		DescribeTimeout:     10 * time.Second,
		CallTimeout:         10 * time.Second,
		ShutdownGrace:       5 * time.Second,
		DisableHealthChecks: true,
		Logger:              pluginreachtest.Logger(t),
	})

	providers := host.SecretProviders()
	if len(providers) != 1 {
		t.Fatalf("the launched plugin offers %d secret providers", len(providers))
	}

	_, err := providers[0].Resolve(t.Context(), secrets.Request{Ref: secrets.NewRef("oidc", "billing-api")})
	if err == nil {
		t.Fatal("an unconfigured oidc plugin minted something")
	}
	if !strings.Contains(err.Error(), providersEnv) {
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
