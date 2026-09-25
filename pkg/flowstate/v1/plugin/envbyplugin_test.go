package plugin

import (
	"errors"
	"slices"
	"strings"
	"testing"

	"github.com/picatz/flowstate/pkg/flowstate/v1/plugin/internal/protocol"
	"github.com/picatz/flowstate/pkg/flowstate/v1/secrets"
)

// TestAPluginsConfigurationReachesThatPluginAndNoOther is the negative
// direction that makes [Config.EnvByPlugin] worth having: a plugin's
// environment is readable to anything running as the same user, so an operator
// configuring `oci` with a registry credential path must not thereby hand it to
// `codex`. Config.Env is the channel for a fact about the deployment; this one
// is narrower by construction.
func TestAPluginsConfigurationReachesThatPluginAndNoOther(t *testing.T) {
	t.Parallel()

	cfg := testConfig(t, t.TempDir()).withDefaults()
	cfg.EnvByPlugin = map[string][]string{
		"oci":   {"FLOWSTATE_OCI_REGISTRIES=/etc/flowstate/oci.yaml"},
		"codex": {"FLOWSTATE_CODEX_BASE_CONFIG=/etc/flowstate/codex.toml"},
	}

	oci := strings.Join(pluginEnv(cfg, "oci", "/tmp/s"), "\n")
	if !strings.Contains(oci, "FLOWSTATE_OCI_REGISTRIES=/etc/flowstate/oci.yaml") {
		t.Errorf("oci was launched without the configuration written for it: %q", oci)
	}
	if strings.Contains(oci, "FLOWSTATE_CODEX_BASE_CONFIG") {
		t.Errorf("oci was launched with another plugin's configuration: %q", oci)
	}

	codex := strings.Join(pluginEnv(cfg, "codex", "/tmp/s"), "\n")
	if strings.Contains(codex, "FLOWSTATE_OCI_REGISTRIES") {
		t.Errorf("codex was launched with another plugin's configuration: %q", codex)
	}

	// A plugin this deployment configured nothing for is launched exactly as it
	// was before this field existed.
	other := strings.Join(pluginEnv(cfg, "example", "/tmp/s"), "\n")
	if strings.Contains(other, "FLOWSTATE_OCI_REGISTRIES") || strings.Contains(other, "FLOWSTATE_CODEX_BASE_CONFIG") {
		t.Errorf("an unconfigured plugin received another plugin's configuration: %q", other)
	}
}

// TestThePerPluginValueWinsOverTheDeploymentWideOne pins the precedence, which
// is the only reason the order of the two loops matters: os/exec keeps the last
// entry for a repeated key, so the narrower statement has to come last.
func TestThePerPluginValueWinsOverTheDeploymentWideOne(t *testing.T) {
	t.Parallel()

	cfg := testConfig(t, t.TempDir()).withDefaults()
	cfg.Env = []string{"FLOWSTATE_REGION=deployment-wide"}
	cfg.EnvByPlugin = map[string][]string{"oci": {"FLOWSTATE_REGION=for-oci"}}

	env := pluginEnv(cfg, "oci", "/tmp/s")

	wide := slices.Index(env, "FLOWSTATE_REGION=deployment-wide")
	narrow := slices.Index(env, "FLOWSTATE_REGION=for-oci")
	if wide < 0 || narrow < 0 {
		t.Fatalf("both entries should be present, got %v", env)
	}
	if narrow < wide {
		t.Errorf("the per-plugin entry is at %d and the deployment-wide one at %d; os/exec keeps the last, so this plugin reads the wrong value: %v", narrow, wide, env)
	}
}

// TestAPluginsConfigurationCannotRedefineAProtocolVariable is the same refusal
// [TestPluginEnvironmentIsMinimal] makes for Config.Env, on the surface an
// operator is more likely to reach for: the handshake is not configuration.
func TestAPluginsConfigurationCannotRedefineAProtocolVariable(t *testing.T) {
	t.Parallel()

	cfg := testConfig(t, t.TempDir()).withDefaults()
	cfg.EnvByPlugin = map[string][]string{"oci": {
		protocol.TokenFDEnv + "=9",
		protocol.SocketEnv + "=/tmp/hijacked.sock",
		"FLOWSTATE_OCI_REGISTRIES=/etc/flowstate/oci.yaml",
	}}

	env := pluginEnv(cfg, "oci", "/tmp/s")
	joined := strings.Join(env, "\n")

	if got := strings.Count(joined, protocol.TokenFDEnv+"="); got != 1 {
		t.Errorf("%s appears %d times, want only the one the host set: %v", protocol.TokenFDEnv, got, env)
	}
	if strings.Contains(joined, "/tmp/hijacked.sock") {
		t.Errorf("a per-plugin entry redefined the socket path: %v", env)
	}
	if !strings.Contains(joined, "FLOWSTATE_OCI_REGISTRIES=/etc/flowstate/oci.yaml") {
		t.Errorf("the entries beside the refused ones were dropped too: %v", env)
	}
}

// TestConfigurationForANameNoPluginCouldAnswerToIsRefused is the fail-open typo
// [validateDigestPin] refuses for a pin, refused for the same reason here: a key
// discovery can never produce is configuration that silently reaches nothing,
// leaving the plugin the operator meant to configure running without it.
func TestConfigurationForANameNoPluginCouldAnswerToIsRefused(t *testing.T) {
	t.Parallel()

	for _, name := range []string{"OCI", "oci_registry", "", "-oci", strings.Repeat("o", MaxNameLen+1)} {
		cfg := testConfig(t, t.TempDir()).withDefaults()
		cfg.EnvByPlugin = map[string][]string{name: {"K=v"}}

		err := cfg.validate()
		if err == nil {
			t.Errorf("EnvByPlugin keyed by %q was accepted; no discovered plugin could ever receive it", name)
			continue
		}
		if !errors.Is(err, ErrPluginEnv) {
			t.Errorf("EnvByPlugin keyed by %q: error is %v, want one that is ErrPluginEnv", name, err)
		}
	}
}

// TestAnEntryThatIsNotKeyValueIsRefused keeps the refusal at startup, where the
// operator can see which entry it was: exec(2) reports neither.
func TestAnEntryThatIsNotKeyValueIsRefused(t *testing.T) {
	t.Parallel()

	cfg := testConfig(t, t.TempDir()).withDefaults()
	cfg.EnvByPlugin = map[string][]string{"oci": {"=novalue"}}

	err := cfg.validate()
	if err == nil || !errors.Is(err, ErrPluginEnv) {
		t.Errorf("an entry with no key was accepted: %v", err)
	}
}

// TestAnOversizedConfigurationIsRefusedBeforeExec is the bound in the units a
// launch fails in: a grant past it fails exec with an errno naming neither the
// variable nor the operator who set it, so it is refused where both are known.
func TestAnOversizedConfigurationIsRefusedBeforeExec(t *testing.T) {
	t.Parallel()

	cfg := testConfig(t, t.TempDir()).withDefaults()
	cfg.EnvByPlugin = map[string][]string{"oci": {
		"FLOWSTATE_OCI_REGISTRIES=" + strings.Repeat("x", MaxPluginEnvBytes),
	}}

	err := cfg.validate()
	if err == nil || !errors.Is(err, ErrPluginEnv) {
		t.Fatalf("an oversized per-plugin environment was accepted: %v", err)
	}
	if !strings.Contains(err.Error(), "oci") {
		t.Errorf("the refusal does not name the plugin whose configuration it is: %v", err)
	}
}

// TestTheConfigurationIsCopiedWhenItIsAccepted closes the gap a shared map
// leaves: a caller who keeps the map they handed over could otherwise change
// what a plugin is launched with after the Config that was validated said
// otherwise.
func TestTheConfigurationIsCopiedWhenItIsAccepted(t *testing.T) {
	t.Parallel()

	entries := []string{"FLOWSTATE_OCI_REGISTRIES=/etc/flowstate/oci.yaml"}
	caller := map[string][]string{"oci": entries}

	cfg := testConfig(t, t.TempDir())
	cfg.EnvByPlugin = caller
	cfg = cfg.withDefaults()

	caller["oci"] = []string{"FLOWSTATE_OCI_REGISTRIES=/tmp/attacker.yaml"}
	entries[0] = "FLOWSTATE_OCI_REGISTRIES=/tmp/attacker.yaml"

	if joined := strings.Join(pluginEnv(cfg, "oci", "/tmp/s"), "\n"); strings.Contains(joined, "attacker") {
		t.Errorf("mutating the map handed to the host changed a later launch: %q", joined)
	}
}

// TestAConfiguredVariableReachesTheRunningPluginProcess is the same claim as
// [TestAPluginsConfigurationReachesThatPluginAndNoOther] made where it counts:
// through a launched process, read back by the plugin itself, rather than
// asserted against the slice this package builds. A grant nothing proves
// crosses exec(2) is a grant an operator cannot rely on — which is exactly the
// state plugins/codex's documented FLOWSTATE_CODEX_BASE_CONFIG and plugins/git's
// GIT_SECRET_* were in before this field existed.
func TestAConfiguredVariableReachesTheRunningPluginProcess(t *testing.T) {
	t.Parallel()

	cfg := testConfig(t, pluginDir(t, "ok"))
	cfg.EnvByPlugin = map[string][]string{"ok": {fakeConfigEnv + "=/etc/flowstate/ok.yaml"}}

	provider := openHost(t, cfg).SecretProviders()[0]

	secret, err := provider.Resolve(t.Context(), secrets.Request{
		Ref: secrets.NewRef("ok", "from-environment"),
	})
	if err != nil {
		t.Fatalf("the plugin could not read what the deployment configured it with: %v", err)
	}
	if !secret.EqualString("/etc/flowstate/ok.yaml") {
		t.Errorf("the plugin read a different value than the one configured for it")
	}
}

// TestAVariableConfiguredForAnotherPluginDoesNotReachThisOne is that test's
// negative half, and the one that makes the field's scoping a boundary rather
// than a convention: the same variable, configured under a different plugin's
// name, leaves this process unable to see it at all.
func TestAVariableConfiguredForAnotherPluginDoesNotReachThisOne(t *testing.T) {
	t.Parallel()

	cfg := testConfig(t, pluginDir(t, "ok"))
	cfg.EnvByPlugin = map[string][]string{"other": {fakeConfigEnv + "=/etc/flowstate/other.yaml"}}

	provider := openHost(t, cfg).SecretProviders()[0]

	_, err := provider.Resolve(t.Context(), secrets.Request{
		Ref: secrets.NewRef("ok", "from-environment"),
	})
	if err == nil {
		t.Fatal("a plugin read a variable configured for a different plugin")
	}
	// The plugin's own refusal, not some earlier failure standing in for it:
	// this has to be the process reporting an empty environment.
	if !strings.Contains(err.Error(), fakeConfigEnv) {
		t.Errorf("the failure is %v, which is not the plugin reporting %s unset", err, fakeConfigEnv)
	}
}
