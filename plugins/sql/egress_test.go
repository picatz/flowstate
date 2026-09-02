package main

import (
	"encoding/base64"
	"os"
	"strings"
	"testing"
)

func TestInstallEgressPolicyBuildsTheForwardedSnapshot(t *testing.T) {
	old := egressPolicy
	egressPolicy = nil
	t.Cleanup(func() { egressPolicy = old })
	t.Setenv(sqlEgressPolicyEnv, base64.StdEncoding.EncodeToString([]byte("egress:\n  schemes: [postgres]\n")))

	if err := installEgressPolicy(); err != nil {
		t.Fatalf("installEgressPolicy: %v", err)
	}
	if egressPolicy == nil {
		t.Fatal("valid SQL egress policy snapshot did not install a policy")
	}
}

func TestInstallEgressPolicyRefusesMalformedConfiguration(t *testing.T) {
	old := egressPolicy
	egressPolicy = nil
	t.Cleanup(func() { egressPolicy = old })

	t.Setenv(sqlEgressPolicyEnv, base64.StdEncoding.EncodeToString([]byte("egress:\n  schemes: [postgres\n")))

	err := installEgressPolicy()
	if err == nil || !strings.Contains(err.Error(), "parsing SQL egress policy") {
		t.Fatalf("installEgressPolicy error = %v, want malformed-policy refusal", err)
	}
	if egressPolicy != nil {
		t.Fatal("malformed SQL egress policy installed a policy")
	}
}

func TestInstallEgressPolicyRefusesInvalidSnapshotEncoding(t *testing.T) {
	old := egressPolicy
	egressPolicy = nil
	t.Cleanup(func() { egressPolicy = old })
	t.Setenv(sqlEgressPolicyEnv, "not-base64")

	err := installEgressPolicy()
	if err == nil || !strings.Contains(err.Error(), "decoding SQL egress policy snapshot") {
		t.Fatalf("installEgressPolicy error = %v, want invalid-snapshot refusal", err)
	}
	if egressPolicy != nil {
		t.Fatal("invalid SQL egress policy snapshot installed a policy")
	}
}

func TestInstallEgressPolicyLeavesNetworkDeniedWhenAbsent(t *testing.T) {
	old := egressPolicy
	egressPolicy = nil
	t.Cleanup(func() { egressPolicy = old })

	// Absent, not empty. An empty value is a grant whose document is empty, and
	// t.Setenv cannot express absence — so the variable is set (for the restore
	// t.Setenv registers) and then removed.
	t.Setenv(sqlEgressPolicyEnv, "placeholder")
	if err := os.Unsetenv(sqlEgressPolicyEnv); err != nil {
		t.Fatalf("unsetting %s: %v", sqlEgressPolicyEnv, err)
	}

	if err := installEgressPolicy(); err != nil {
		t.Fatalf("installEgressPolicy: %v", err)
	}
	if egressPolicy != nil {
		t.Fatal("absent SQL egress policy installed a policy")
	}
}

// TestInstallEgressPolicyInstallsAnExplicitlyEmptySnapshot is the case a length
// check dropped.
//
// A worker whose --egress-policy names a zero-byte file has configured a policy
// — the built-in http task runs under what an empty document builds — and the
// host grants it as the empty string. Reading that as "no grant" made this
// plugin refuse PostgreSQL connections on a worker whose own http task was
// connecting, from one file, on one deployment.
func TestInstallEgressPolicyInstallsAnExplicitlyEmptySnapshot(t *testing.T) {
	old := egressPolicy
	egressPolicy = nil
	t.Cleanup(func() { egressPolicy = old })
	t.Setenv(sqlEgressPolicyEnv, "")

	if err := installEgressPolicy(); err != nil {
		t.Fatalf("installEgressPolicy: %v", err)
	}
	if egressPolicy == nil {
		t.Fatal("an explicitly configured empty policy was read as no grant at all")
	}
}
