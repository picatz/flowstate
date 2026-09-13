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
// The grant is captured once per process, deliberately - a later os.Setenv is
// the self-granting that capture exists to prevent - so a test binary gets one
// grant, and this is where it is set. The cases for an absent or malformed
// grant belong to the SDK and are proved there rather than restated here
// against a decode path this plugin does not have.
func TestMain(m *testing.M) {
	if err := os.Setenv(sdk.EgressPolicyEnv,
		base64.StdEncoding.EncodeToString([]byte("deployment_default: true\negress: {}\n"))); err != nil {
		panic(err)
	}

	installEgressPolicy()

	os.Exit(m.Run())
}

// TestTheDeploymentDefaultIsAcceptedAsTheGrant records which posture this
// plugin takes toward a policy no operator wrote (#1332, point 7).
//
// A registry read is an HTTPS GET to a public host, which is what the default
// policy permits and what this plugin can do on a worker started with no
// --egress-policy. Refusing the default would mean installing this plugin
// requires writing a policy file to get back what the worker's own http task
// already does. `sql` refuses under the same grant, deliberately, because a
// database connection is not an HTTPS GET.
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

// TestTheDeploymentDefaultDeniesALoopbackRegistry is the other half of that
// acceptance, and the reason accepting it is safe: the default policy this
// plugin takes is a real policy, and it refuses the destinations it says it
// refuses. The task entry point is used rather than an injected client,
// because this is a claim about the grant the process actually runs under.
func TestTheDeploymentDefaultDeniesALoopbackRegistry(t *testing.T) {
	server := newFakeRegistry(t)

	_, err := ociResolve(t.Context(), inputsFor(t, resolveInputs(server.reference("app", "1.0"), "", "")), nil)
	if err == nil {
		t.Fatal("a loopback registry was reached under the deployment default policy")
	}
	if !isPermissionDenied(err) {
		t.Errorf("the refusal is %v, want the egress policy denying the destination", err)
	}
}
