package main

import (
	"encoding/base64"
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
	t.Setenv(sqlEgressPolicyEnv, "")

	if err := installEgressPolicy(); err != nil {
		t.Fatalf("installEgressPolicy: %v", err)
	}
	if egressPolicy != nil {
		t.Fatal("absent SQL egress policy installed a policy")
	}
}
