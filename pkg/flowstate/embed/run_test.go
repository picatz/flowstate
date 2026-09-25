package embed

import (
	"context"
	"strings"
	"testing"

	v1 "github.com/picatz/flowstate/pkg/flowstate/v1"
	"github.com/picatz/flowstate/pkg/flowstate/v1/auth"
	"github.com/picatz/flowstate/pkg/flowstate/v1/netpolicy"
	"github.com/picatz/flowstate/pkg/flowstate/v1/secrets"
)

// bearerWorkflow is an http step reading a static secret reference as its
// bearer token, against an address the egress policy refuses before any
// request is dialed — so what fails, and why, is entirely about secret
// resolution, never about the network.
const bearerWorkflow = `
edition: v2026.3
name: bearer-test
steps:
  - id: step1
    retry:
      attempts: 1
    http:
      url: https://bearer-test.invalid/never-dialed
      bearer: ${secret('fixture:API_TOKEN')}
`

func TestRunLocal_SecretsNilRefusesReference(t *testing.T) {
	workflow, diags, err := Compile([]byte(bearerWorkflow))
	if err != nil {
		t.Fatalf("Compile: %v diags=%v", err, diags)
	}

	// A zero RunOptions: no Secrets configured at all.
	_, runErr := RunLocal(context.Background(), workflow, RunOptions{})
	if runErr == nil {
		t.Fatal("RunLocal: expected a secret reference to be refused with no Secrets configured")
	}
	if !strings.Contains(runErr.Error(), "secret access is not configured on this worker") {
		t.Errorf("RunLocal: error = %q, want it to name secret access as unconfigured", runErr.Error())
	}
}

func fixtureSecrets(t *testing.T, value string) *Secrets {
	t.Helper()

	store, err := secrets.NewStore(fixtureProvider{value: value})
	if err != nil {
		t.Fatalf("secrets.NewStore: %v", err)
	}
	policy, err := (auth.SecretAccessPolicy{Allow: []string{"true"}}).Compile()
	if err != nil {
		t.Fatalf("compiling secret access policy: %v", err)
	}
	return &Secrets{
		Store:  store,
		Policy: policy,
		Identity: auth.WorkloadIdentity{
			Subject: "test-workload", Issuer: "https://issuer.example", Namespace: "test-tenant",
		},
	}
}

type fixtureProvider struct{ value string }

func (fixtureProvider) Scheme() string { return "fixture" }
func (p fixtureProvider) Resolve(_ context.Context, req secrets.Request) (secrets.Secret, error) {
	return secrets.NewSecret(req.Ref, p.value), nil
}

func TestRunLocal_SecretsConfiguredResolves(t *testing.T) {
	workflow, diags, err := Compile([]byte(bearerWorkflow))
	if err != nil {
		t.Fatalf("Compile: %v diags=%v", err, diags)
	}

	_, runErr := RunLocal(context.Background(), workflow, RunOptions{
		Secrets: fixtureSecrets(t, "shh-its-a-secret"),
	})
	if runErr == nil {
		t.Fatal("RunLocal: expected the run to fail (unreachable host), but resolution itself must have succeeded")
	}
	if strings.Contains(runErr.Error(), "secret access is not configured on this worker") {
		t.Errorf("RunLocal: secret resolution was refused even though Secrets was configured: %v", runErr)
	}
	if strings.Contains(runErr.Error(), "shh-its-a-secret") {
		t.Errorf("RunLocal: the resolved secret material leaked into the recorded error: %v", runErr)
	}
}

func TestRunLocal_SecretsWithNoPolicyDeniesEverything(t *testing.T) {
	// A non-nil Secrets with a Store but no Policy must still deny every
	// reference — [v1.ResolveSecret] treats a nil Policy exactly like a nil
	// Store, "not configured on this worker", so configuring a store alone
	// is never enough on its own to permit a read.
	workflow, diags, err := Compile([]byte(bearerWorkflow))
	if err != nil {
		t.Fatalf("Compile: %v diags=%v", err, diags)
	}

	store, err := secrets.NewStore(fixtureProvider{value: "x"})
	if err != nil {
		t.Fatalf("secrets.NewStore: %v", err)
	}

	_, runErr := RunLocal(context.Background(), workflow, RunOptions{
		Secrets: &Secrets{
			Store: store,
			Identity: auth.WorkloadIdentity{
				Subject: "test-workload", Issuer: "https://issuer.example", Namespace: "test-tenant",
			},
		},
	})
	if runErr == nil {
		t.Fatal("RunLocal: expected the run to fail")
	}
	if !strings.Contains(runErr.Error(), "secret access is not configured on this worker") {
		t.Errorf("RunLocal: expected the same refusal a fully unconfigured Secrets gets, got: %v", runErr)
	}
}

const loopbackWorkflow = `
edition: v2026.3
name: egress-test
steps:
  - id: step1
    retry:
      attempts: 1
    http:
      url: http://127.0.0.1:1/
`

func TestRunLocal_EgressPolicyZeroValueDeniesLoopback(t *testing.T) {
	workflow, diags, err := Compile([]byte(loopbackWorkflow))
	if err != nil {
		t.Fatalf("Compile: %v diags=%v", err, diags)
	}

	// A zero RunOptions: no EgressPolicy configured.
	_, runErr := RunLocal(context.Background(), workflow, RunOptions{})
	if runErr == nil {
		t.Fatal("RunLocal: expected a loopback request to be denied by the default egress policy")
	}
	if !strings.Contains(runErr.Error(), "denied by egress policy") || !strings.Contains(runErr.Error(), "loopback") {
		t.Errorf("RunLocal: error = %q, want a loopback denial from the default policy", runErr.Error())
	}
}

// TestRunLocal_EgressPolicyZeroValueIsConstantNotAmbient is the P1-1 fix
// from PR #232's review: a nil RunOptions.EgressPolicy must be the
// documented deny-by-default posture regardless of what
// [v1.DefaultRegistry] currently holds for "http" — never inherited from
// whatever happens to be registered there, which another component (
// cmd/flow/egress.go's own process-wide mutation, or a different embedder's
// [Tasks.Install] overriding "http") can change out from under a caller who
// configured nothing at all.
func TestRunLocal_EgressPolicyZeroValueIsConstantNotAmbient(t *testing.T) {
	workflow, diags, err := Compile([]byte(loopbackWorkflow))
	if err != nil {
		t.Fatalf("Compile: %v diags=%v", err, diags)
	}

	original, ok := v1.LookupTask("http")
	if !ok {
		t.Fatal("expected the built-in http task to be registered")
	}
	t.Cleanup(func() {
		if err := v1.DefaultRegistry().Replace(original); err != nil {
			t.Fatalf("restoring the original http task: %v", err)
		}
	})

	// Replace the *global* http task with a fully permissive one — exactly
	// what `cmd/flow/egress.go` does process-wide, and exactly what a
	// different embedder's Tasks.Install could do by naming "http" in its
	// own set. Nothing in this test touches RunOptions.
	permissive, err := netpolicy.New(netpolicy.WithAllowLoopback())
	if err != nil {
		t.Fatalf("netpolicy.New: %v", err)
	}
	if err := v1.DefaultRegistry().Replace(v1.HTTPTaskDef(permissive)); err != nil {
		t.Fatalf("registering a permissive http task globally: %v", err)
	}

	// A zero RunOptions must still deny — the documented constant, not
	// whatever the registry now says.
	_, runErr := RunLocal(context.Background(), workflow, RunOptions{})
	if runErr == nil {
		t.Fatal("RunLocal: expected a loopback request to be denied despite the permissive global override")
	}
	if !strings.Contains(runErr.Error(), "denied by egress policy") || !strings.Contains(runErr.Error(), "loopback") {
		t.Errorf("RunLocal: error = %q, want the documented default's loopback denial, "+
			"not whatever the global registry's http task currently allows", runErr.Error())
	}
}

func TestRunLocal_EgressPolicyOverrideAppliesPerRun(t *testing.T) {
	workflow, diags, err := Compile([]byte(loopbackWorkflow))
	if err != nil {
		t.Fatalf("Compile: %v diags=%v", err, diags)
	}

	policy, err := netpolicy.New(netpolicy.WithAllowLoopback())
	if err != nil {
		t.Fatalf("netpolicy.New: %v", err)
	}

	_, runErr := RunLocal(context.Background(), workflow, RunOptions{EgressPolicy: policy})
	if runErr == nil {
		t.Fatal("RunLocal: expected the run to fail (nothing listens on 127.0.0.1:1)")
	}
	// The failure reason must have moved from a policy denial to an actual
	// dial attempt — proof the configured policy, not the default, governed
	// this run.
	if strings.Contains(runErr.Error(), "denied by egress policy") {
		t.Errorf("RunLocal: request was still denied by the egress policy despite WithAllowLoopback: %v", runErr)
	}

	// A concurrent, unconfigured call must still get the default, unaffected
	// posture — the whole point of a per-run registry rather than a global
	// mutation.
	_, runErr2 := RunLocal(context.Background(), workflow, RunOptions{})
	if runErr2 == nil || !strings.Contains(runErr2.Error(), "denied by egress policy") {
		t.Errorf("RunLocal: a call with no EgressPolicy configured was affected by another call's override: %v", runErr2)
	}
}

func TestRunLocal_CustomTaskEndToEnd(t *testing.T) {
	tasks := NewTasks()
	if err := tasks.Register(Task{
		Name: "double",
		Fn:   doubleTaskFn,
	}); err != nil {
		t.Fatalf("Register: %v", err)
	}

	workflow, diags, err := Compile([]byte(`
edition: v2026.3
name: double-test
steps:
  - id: step1
    double:
      n: 21
`))
	if err != nil {
		t.Fatalf("Compile: %v diags=%v", err, diags)
	}

	outputs, runErr := RunLocal(context.Background(), workflow, RunOptions{Tasks: tasks})
	if runErr != nil {
		t.Fatalf("RunLocal: %v", runErr)
	}

	got := outputs.GetStepValues()["step1"].GetNamedValues()["result"].GetLiteral().GetInt64Value()
	if got != 42 {
		t.Errorf("step1 result = %d, want 42", got)
	}
}
