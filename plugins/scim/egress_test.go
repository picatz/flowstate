package main

import (
	"encoding/base64"
	"os"
	"testing"

	"github.com/picatz/flowstate/pkg/flowstate/v1/plugin/sdk"
)

// TestMain gives this test binary the grant a worker gives a launched plugin,
// installs it exactly as main does, and lets the fake provider in this package
// be reached over loopback - which is the one thing a running plugin never
// does, and the reason httpsScheme is a variable rather than a constant.
func TestMain(m *testing.M) {
	if err := os.Setenv(sdk.EgressPolicyEnv,
		base64.StdEncoding.EncodeToString([]byte("deployment_default: true\negress: {}\n"))); err != nil {
		panic(err)
	}

	installEgressPolicy()

	os.Exit(m.Run())
}

// TestTheDeploymentDefaultIsAcceptedAsTheGrant records this plugin's posture
// toward a policy no operator wrote: a SCIM call is an HTTPS request to the
// organization's own identity provider, which the default permits. An operator
// who wants it confined to one host writes the policy and is obeyed, which the
// reachability test proves through a launched process.
func TestTheDeploymentDefaultIsAcceptedAsTheGrant(t *testing.T) {
	isDefault, err := sdk.EgressPolicyIsDeploymentDefault()
	if err != nil {
		t.Fatalf("EgressPolicyIsDeploymentDefault: %v", err)
	}
	if !isDefault {
		t.Fatal("this test binary did not receive the deployment default, so it proves nothing about the posture toward one")
	}
	if egressPolicy == nil {
		t.Fatalf("the deployment default was refused, which denies every task on a worker with no --egress-policy: %v", egressRefusal)
	}
}

// TestABaseUrlMustBeHttps is the refusal that keeps a directory credential off
// the wire in cleartext, checked before anything is dialed.
func TestABaseUrlMustBeHttps(t *testing.T) {
	for _, raw := range []string{
		"http://example.okta.com/scim/v2",
		"ftp://example.okta.com/scim/v2",
		"https://user:pass@example.okta.com/scim/v2",
		"https://example.okta.com/scim/v2?tenant=a",
		"example.okta.com/scim/v2",
	} {
		if _, err := newClient(raw, "not-a-real-token"); err == nil {
			t.Errorf("base_url %q was accepted", raw)
		} else if !sdk.IsInvalidInput(err) {
			t.Errorf("base_url %q: error is %v, want invalid input", raw, err)
		}
	}
}
