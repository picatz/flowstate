package main

import (
	"encoding/base64"
	"os"
	"testing"

	"github.com/picatz/flowstate/pkg/flowstate/v1/plugin/sdk"
)

// TestMain gives this test binary the grant a worker gives a launched plugin.
//
// The policy this plugin enforces is the deployment's now, not one it builds
// for itself (#1321), so a test binary that never received a grant is a plugin
// that refuses every task — correctly, and uselessly for the tests below, which
// are about clone bounds, usernames and error classification rather than about
// egress. What it receives is the document a worker with no --egress-policy
// grants, because that is the launch every one of these tests is standing in
// for.
//
// It is set here rather than per test because the SDK captures the grant once
// per process, on purpose: a later os.Setenv is exactly the self-granting the
// capture exists to prevent, so the only place a test binary can supply one is
// before anything asks.
func TestMain(m *testing.M) {
	if err := os.Setenv(sdk.EgressPolicyEnv,
		base64.StdEncoding.EncodeToString([]byte("deployment_default: true\negress: {}\n"))); err != nil {
		panic(err)
	}

	installEgressPolicy()

	os.Exit(m.Run())
}
