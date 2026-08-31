package main

import (
	"encoding/base64"
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
