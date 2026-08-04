package main

import (
	"os"
	"path/filepath"
	"strings"
	"testing"

	codexv1 "github.com/picatz/flowstate/plugins/codex/gen/codex/v1"
)

func TestLoadOperatorPolicyDefaultsToTheFailClosedBaseline(t *testing.T) {
	t.Setenv(policyEnv, "")

	policy, err := loadOperatorPolicy()
	if err != nil {
		t.Fatalf("loadOperatorPolicy: unexpected error: %v", err)
	}
	if policy.MaxSandbox != codexv1.SandboxMode_SANDBOX_MODE_READ_ONLY {
		t.Errorf("MaxSandbox = %v, want SANDBOX_MODE_READ_ONLY - the fail-closed baseline with no operator config", policy.MaxSandbox)
	}
	if policy.AllowNetwork {
		t.Error("AllowNetwork = true, want false as the fail-closed baseline")
	}
	if len(policy.RawConfig) != 0 {
		t.Error("RawConfig is non-empty with no operator config file - nothing should be copied into the ephemeral CODEX_HOME")
	}
}

func writePolicyFile(t *testing.T, contents string) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), "config.toml")
	if err := os.WriteFile(path, []byte(contents), 0o600); err != nil {
		t.Fatalf("WriteFile: %v", err)
	}
	return path
}

func TestLoadOperatorPolicyParsesTheTwoKeysItReads(t *testing.T) {
	path := writePolicyFile(t, `
sandbox_mode = "workspace-write"

[sandbox_workspace_write]
network_access = true

# Everything else in this file is opaque to the plugin and copied through
# verbatim into the ephemeral CODEX_HOME.
model_reasoning_effort = "high"
`)
	t.Setenv(policyEnv, path)

	policy, err := loadOperatorPolicy()
	if err != nil {
		t.Fatalf("loadOperatorPolicy: unexpected error: %v", err)
	}
	if policy.MaxSandbox != codexv1.SandboxMode_SANDBOX_MODE_WORKSPACE_WRITE {
		t.Errorf("MaxSandbox = %v, want SANDBOX_MODE_WORKSPACE_WRITE", policy.MaxSandbox)
	}
	if !policy.AllowNetwork {
		t.Error("AllowNetwork = false, want true")
	}
	if !strings.Contains(string(policy.RawConfig), "model_reasoning_effort") {
		t.Error("RawConfig does not contain the rest of the operator's file - it must be copied through opaquely")
	}
}

func TestLoadOperatorPolicyEmptySandboxModeIsReadOnly(t *testing.T) {
	// codex's own documented default when sandbox_mode is absent from
	// config.toml is read-only; this plugin's ceiling must match that,
	// not treat an absent key as "unbounded."
	path := writePolicyFile(t, `model_reasoning_effort = "high"`)
	t.Setenv(policyEnv, path)

	policy, err := loadOperatorPolicy()
	if err != nil {
		t.Fatalf("loadOperatorPolicy: unexpected error: %v", err)
	}
	if policy.MaxSandbox != codexv1.SandboxMode_SANDBOX_MODE_READ_ONLY {
		t.Errorf("MaxSandbox = %v, want SANDBOX_MODE_READ_ONLY for a file with no sandbox_mode key", policy.MaxSandbox)
	}
}

func TestLoadOperatorPolicyRefusesAnUnrecognizedSandboxModeValue(t *testing.T) {
	path := writePolicyFile(t, `sandbox_mode = "not-a-real-mode"`)
	t.Setenv(policyEnv, path)

	if _, err := loadOperatorPolicy(); err == nil {
		t.Fatal("loadOperatorPolicy with an unrecognized sandbox_mode: got no error, want one")
	}
}

func TestLoadOperatorPolicyRefusesMalformedTOML(t *testing.T) {
	path := writePolicyFile(t, `this is not toml at all {{{`)
	t.Setenv(policyEnv, path)

	if _, err := loadOperatorPolicy(); err == nil {
		t.Fatal("loadOperatorPolicy with malformed TOML: got no error, want one")
	}
}

func TestLoadOperatorPolicyRefusesAMissingFile(t *testing.T) {
	t.Setenv(policyEnv, filepath.Join(t.TempDir(), "does-not-exist.toml"))
	if _, err := loadOperatorPolicy(); err == nil {
		t.Fatal("loadOperatorPolicy with a missing file: got no error, want one")
	}
}

func TestLoadOperatorPolicyRefusesOverTheByteCeiling(t *testing.T) {
	path := writePolicyFile(t, "sandbox_mode = \"read-only\"\n"+strings.Repeat("# padding\n", maxPolicyBytes))
	t.Setenv(policyEnv, path)
	if _, err := loadOperatorPolicy(); err == nil {
		t.Fatal("loadOperatorPolicy over the byte ceiling: got no error, want one")
	}
}

// TestNarrowSandboxRefusesExceedingTheCeiling and
// TestNarrowSandboxAllowsWithinTheCeiling are the two directions CLAUDE.md's
// "test that A cannot reach B" asks for applied to a policy ceiling rather
// than a tenant boundary: a request within the ceiling must work, and one
// that exceeds it must not silently become the ceiling's own value - it
// must be refused.
func TestNarrowSandboxRefusesExceedingTheCeiling(t *testing.T) {
	policy := operatorPolicy{MaxSandbox: codexv1.SandboxMode_SANDBOX_MODE_READ_ONLY}

	if err := narrowSandbox(codexv1.SandboxMode_SANDBOX_MODE_WORKSPACE_WRITE, policy); err == nil {
		t.Error("narrowSandbox(WORKSPACE_WRITE) against a READ_ONLY ceiling: got no error, want one")
	}
	if err := narrowSandbox(codexv1.SandboxMode_SANDBOX_MODE_DANGER_FULL_ACCESS, policy); err == nil {
		t.Error("narrowSandbox(DANGER_FULL_ACCESS) against a READ_ONLY ceiling: got no error, want one")
	}
}

func TestNarrowSandboxAllowsWithinTheCeiling(t *testing.T) {
	policy := operatorPolicy{MaxSandbox: codexv1.SandboxMode_SANDBOX_MODE_WORKSPACE_WRITE}

	if err := narrowSandbox(codexv1.SandboxMode_SANDBOX_MODE_READ_ONLY, policy); err != nil {
		t.Errorf("narrowSandbox(READ_ONLY) against a WORKSPACE_WRITE ceiling: unexpected error: %v", err)
	}
	if err := narrowSandbox(codexv1.SandboxMode_SANDBOX_MODE_WORKSPACE_WRITE, policy); err != nil {
		t.Errorf("narrowSandbox(WORKSPACE_WRITE) against a WORKSPACE_WRITE ceiling: unexpected error: %v", err)
	}
	if err := narrowSandbox(codexv1.SandboxMode_SANDBOX_MODE_DANGER_FULL_ACCESS, policy); err == nil {
		t.Error("narrowSandbox(DANGER_FULL_ACCESS) against a WORKSPACE_WRITE ceiling: got no error, want one - " +
			"a ceiling one level below must still refuse the level above it")
	}
}

func TestNarrowNetworkRefusesWithoutOperatorGrant(t *testing.T) {
	policy := operatorPolicy{AllowNetwork: false}
	if err := narrowNetwork(true, policy); err == nil {
		t.Fatal("narrowNetwork(true) against a policy that has not granted it: got no error, want one")
	}
	if err := narrowNetwork(false, policy); err != nil {
		t.Errorf("narrowNetwork(false): unexpected error: %v", err)
	}
}

func TestNarrowNetworkAllowsWithOperatorGrant(t *testing.T) {
	policy := operatorPolicy{AllowNetwork: true}
	if err := narrowNetwork(true, policy); err != nil {
		t.Errorf("narrowNetwork(true) against a policy that granted it: unexpected error: %v", err)
	}
}
