package main

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestInstallEgressPolicyRefusesMalformedConfiguration(t *testing.T) {
	old := egressPolicy
	egressPolicy = nil
	t.Cleanup(func() { egressPolicy = old })

	path := filepath.Join(t.TempDir(), "egress.yaml")
	if err := os.WriteFile(path, []byte("egress:\n  schemes: [postgres\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	t.Setenv(sqlEgressPolicyEnv, path)

	err := installEgressPolicy()
	if err == nil || !strings.Contains(err.Error(), "parsing SQL egress policy") {
		t.Fatalf("installEgressPolicy error = %v, want malformed-policy refusal", err)
	}
	if egressPolicy != nil {
		t.Fatal("malformed SQL egress policy installed a policy")
	}
}

func TestInstallEgressPolicyLeavesNetworkDeniedWhenAbsent(t *testing.T) {
	old := egressPolicy
	egressPolicy = nil
	t.Cleanup(func() { egressPolicy = old })
	t.Setenv(sqlEgressPolicyEnv, "")

	if err := installEgressPolicy(); err != nil {
		t.Fatalf("installEgressPolicy: %v", err)
	}
	if egressPolicy != nil {
		t.Fatal("absent SQL egress policy installed a policy")
	}
}
