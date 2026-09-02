package main

import (
	"context"
	"encoding/base64"
	"errors"
	"net/http"
	"net/url"
	"os"
	"strings"
	"testing"

	"github.com/picatz/flowstate/pkg/flowstate/v1/netpolicy"
	"github.com/picatz/flowstate/pkg/flowstate/v1/plugin/sdk"
	"github.com/picatz/flowstate/pkg/flowstate/v1/secrets"

	sqlv1 "github.com/picatz/flowstate/plugins/sql/gen/sql/v1"
)

// TestMain launches this test binary the way a worker with no --egress-policy
// launches this plugin: with the deployment default as the grant.
//
// That is the launch this plugin's own posture is about, so it is the one the
// binary stands in for. The grant is captured once per process, on purpose (a
// later os.Setenv is the self-granting that capture exists to prevent), so it is
// set here and the tests that need a different policy install one directly
// through withPostgresPolicy. The cases this file used to run one per test — an
// absent grant, a malformed one, an unreadable encoding, an explicitly empty one
// — are the SDK's now, proved in pkg/flowstate/v1/plugin/sdk/egress_test.go
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

// TestTheDeploymentDefaultIsRefusedForPostgres is this plugin's half of point 7
// of #1332, and the one first-party plugin that refuses where the others accept.
//
// A worker with no --egress-policy grants the policy its own http task runs
// under. For an HTTPS fetch that is a decision a deployment can be said to have
// made; for a database it is not, because the destination is the whole meaning
// of the credential this task carries and nothing about the default says which
// database this deployment authorizes. So the refusal #1320 established survives
// the grant becoming universal — and it names the flag, because "denied" leaves
// an operator guessing which of several controls decided.
func TestTheDeploymentDefaultIsRefusedForPostgres(t *testing.T) {
	isDefault, err := sdk.EgressPolicyIsDeploymentDefault()
	if err != nil {
		t.Fatalf("EgressPolicyIsDeploymentDefault: %v", err)
	}
	if !isDefault {
		t.Fatal("this test binary did not receive the deployment default, so it proves nothing about the posture toward one")
	}

	if egressPolicy != nil {
		t.Fatal("the deployment default was taken as an operator policy; postgres would connect on a worker that authorized no destination")
	}

	_, err = openDB(t.Context(), sqlv1.Engine_ENGINE_POSTGRES,
		postgresDSN("database.example", 5432), secrets.NewScrubber())
	if err == nil {
		t.Fatal("openDB opened a postgres connection under the deployment default")
	}
	if !strings.Contains(err.Error(), "--egress-policy") {
		t.Fatalf("the refusal does not name the flag that would grant it: %v", err)
	}
}

// TestAnInterruptedPolicyCheckIsNotADenial keeps a decision the operator's
// policy never made from being reported as one.
//
// A rule that cannot be evaluated because the context is gone reports exactly
// that: netpolicy returns a [netpolicy.UndecidedError], which unwraps to the
// context error and deliberately does not wrap [netpolicy.ErrDenied]. Reporting
// it as PermissionDenied told the caller two false things at once — that this
// destination is refused, and that no retry will help. The rule here fails to
// evaluate on purpose (int(host) is not a number), which is what makes the
// cancelled context the thing the error reports.
func TestAnInterruptedPolicyCheckIsNotADenial(t *testing.T) {
	policy, err := netpolicy.New(
		netpolicy.WithSchemes("postgres"),
		netpolicy.WithAllowLoopback(),
		netpolicy.WithDenyRules(`int(host) > 0`),
	)
	if err != nil {
		t.Fatalf("building the test policy: %v", err)
	}

	ctx, cancel := context.WithCancel(t.Context())
	cancel()

	target := &url.URL{Scheme: "postgres", Host: "database.example:5432"}
	checkErr := policy.CheckURL(ctx, http.MethodConnect, target)
	if checkErr == nil {
		t.Fatal("the policy check succeeded against a cancelled context, so this test proves nothing")
	}
	if errors.Is(checkErr, netpolicy.ErrDenied) {
		t.Fatalf("netpolicy reported an interrupted evaluation as a denial: %v", checkErr)
	}
	var undecided *netpolicy.UndecidedError
	if !errors.As(checkErr, &undecided) {
		t.Fatalf("an interrupted evaluation is not reported as undecided, so this task cannot tell it from a decision: %v", checkErr)
	}

	got := classifyEgressCheck(checkErr)
	if strings.Contains(got.Error(), "denied by deployment egress policy") {
		t.Fatalf("an interrupted policy check was reported as a permanent denial: %v", got)
	}
	if !errors.Is(got, context.Canceled) {
		t.Fatalf("the caller cannot see why the check did not finish: %v", got)
	}

	// The direction that must not change with it: a real denial stays the
	// permanent refusal, with a message that names no host.
	denied := classifyEgressCheck(policy.CheckURL(t.Context(), http.MethodConnect,
		&url.URL{Scheme: "https", Host: "database.example:443"}))
	if denied == nil || !strings.Contains(denied.Error(), "denied by deployment egress policy") {
		t.Fatalf("a policy denial stopped being reported as one: %v", denied)
	}
	if strings.Contains(denied.Error(), "database.example") {
		t.Fatalf("the denial names the destination, which comes from the caller's DSN: %v", denied)
	}
}
