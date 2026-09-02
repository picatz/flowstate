package main

import (
	"encoding/base64"
	"os"
	"testing"

	"github.com/picatz/flowstate/pkg/flowstate/v1/plugin/sdk"
)

// TestMain gives this test binary the grant a worker gives a launched plugin,
// and installs it exactly as main does.
//
// The grant is captured once per process, on purpose (a later os.Setenv is the
// self-granting that capture exists to prevent), so a test binary gets one
// grant and this is where it is set. The cases this file used to run one per
// test — an absent grant, a malformed one, an explicitly empty one — are the
// SDK's now, and are proved there (pkg/flowstate/v1/plugin/sdk/egress_test.go)
// rather than restated here against a second decode path this plugin no longer
// has (#1332).
func TestMain(m *testing.M) {
	if err := os.Setenv(sdk.EgressPolicyEnv,
		base64.StdEncoding.EncodeToString([]byte("deployment_default: true\negress: {}\n"))); err != nil {
		panic(err)
	}

	installEgressPolicy()

	os.Exit(m.Run())
}

// TestTheDeploymentDefaultIsAcceptedAsTheGrant is this plugin's half of point 7
// of #1332: which posture it takes toward a policy no operator wrote.
//
// Slack is an HTTPS POST to a public host, which is what the default policy
// permits and what this plugin could always do on a worker started with no
// --egress-policy. Refusing the default would make installing this plugin
// require writing a policy file to get back what the worker already does. `sql`
// refuses under the same grant, deliberately, because a database is not an
// HTTPS POST — so this asserts what was accepted was in fact the default, not
// merely that something arrived.
func TestTheDeploymentDefaultIsAcceptedAsTheGrant(t *testing.T) {
	isDefault, err := sdk.EgressPolicyIsDeploymentDefault()
	if err != nil {
		t.Fatalf("EgressPolicyIsDeploymentDefault: %v", err)
	}
	if !isDefault {
		t.Fatal("this test binary did not receive the deployment default, so it proves nothing about the posture toward one")
	}

	if egressPolicy == nil {
		t.Fatalf("the deployment default was refused, which denies slack.post on every worker with no --egress-policy: %v", egressRefusal)
	}
}
