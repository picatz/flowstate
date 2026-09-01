package main

import (
	"encoding/base64"
	"os"
	"strings"
	"testing"
)

func TestInstallEgressPolicyUsesTheHostSnapshot(t *testing.T) {
	old := egressPolicy
	egressPolicy = nil
	t.Cleanup(func() { egressPolicy = old })
	t.Setenv(slackEgressPolicyEnv, base64.StdEncoding.EncodeToString([]byte("egress:\n  schemes: [https]\n")))
	if err := installEgressPolicy(); err != nil {
		t.Fatalf("installEgressPolicy: %v", err)
	}
	if egressPolicy == nil {
		t.Fatal("valid operator snapshot did not install a policy")
	}
}

func TestInstallEgressPolicyFailsClosedOnMalformedSnapshot(t *testing.T) {
	old := egressPolicy
	egressPolicy = nil
	t.Cleanup(func() { egressPolicy = old })
	t.Setenv(slackEgressPolicyEnv, base64.StdEncoding.EncodeToString([]byte("egress:\n  schemes: [https\n")))
	err := installEgressPolicy()
	if err == nil || !strings.Contains(err.Error(), "parsing Slack egress policy") {
		t.Fatalf("installEgressPolicy error = %v", err)
	}
	if egressPolicy != nil {
		t.Fatal("malformed snapshot installed a policy")
	}
}

func TestInstallEgressPolicyLeavesNetworkDeniedWhenAbsent(t *testing.T) {
	old := egressPolicy
	egressPolicy = nil
	t.Cleanup(func() { egressPolicy = old })

	// Absent, not empty. An empty value is a grant whose document is empty, and
	// t.Setenv cannot express absence — so the variable is set (for the restore
	// t.Setenv registers) and then removed.
	t.Setenv(slackEgressPolicyEnv, "placeholder")
	if err := os.Unsetenv(slackEgressPolicyEnv); err != nil {
		t.Fatalf("unsetting %s: %v", slackEgressPolicyEnv, err)
	}

	if err := installEgressPolicy(); err != nil {
		t.Fatalf("installEgressPolicy: %v", err)
	}
	if egressPolicy != nil {
		t.Fatal("absent Slack egress policy installed a policy")
	}
}

// TestInstallEgressPolicyInstallsAnExplicitlyEmptySnapshot is the case a length
// check dropped.
//
// A worker whose --egress-policy names a zero-byte file has configured a policy
// — the built-in http task runs under what an empty document builds — and the
// host grants it as the empty string. Reading that as "no grant" made this
// plugin refuse Slack requests on a worker whose own http task was making them,
// from one file, on one deployment.
func TestInstallEgressPolicyInstallsAnExplicitlyEmptySnapshot(t *testing.T) {
	old := egressPolicy
	egressPolicy = nil
	t.Cleanup(func() { egressPolicy = old })
	t.Setenv(slackEgressPolicyEnv, "")

	if err := installEgressPolicy(); err != nil {
		t.Fatalf("installEgressPolicy: %v", err)
	}
	if egressPolicy == nil {
		t.Fatal("an explicitly configured empty policy was read as no grant at all")
	}
}
