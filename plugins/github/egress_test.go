package main

import (
	"encoding/base64"
	"os"

	"github.com/picatz/flowstate/pkg/flowstate/v1/plugin/sdk"
)

// grantTestEgressPolicy gives this test binary the grant a worker gives a
// launched plugin, and installs it exactly as main does.
//
// The policy this plugin enforces is the deployment's now, not one it builds for
// itself (#1323), so a test binary that never received a grant is a plugin whose
// every client is refused — correctly, and uselessly for tests about API bases,
// cursors and error classification. What it receives is the document a worker
// with no --egress-policy grants, because that is the launch these tests stand
// in for.
//
// Called from [TestMain] rather than from each test because the SDK captures the
// grant once per process, on purpose: a later os.Setenv is exactly the
// self-granting the capture exists to prevent, so the only place a test binary
// can supply one is before anything asks.
func grantTestEgressPolicy() {
	if err := os.Setenv(sdk.EgressPolicyEnv,
		base64.StdEncoding.EncodeToString([]byte("deployment_default: true\negress: {}\n"))); err != nil {
		panic(err)
	}

	installEgressPolicy()
}
