package conformance

import (
	"maps"
	"testing"

	v1 "github.com/picatz/flowstate/pkg/flowstate/v1"
)

// The `principal` an expression reads under `run.identity` and under a wait's
// `sender.identity` is `issuer#subject`, and "" unless both halves are present.
// These cases pin that one rule, and the rest of the identity shape beside it
// (`subject`, `issuer`, and `claims` for `run.identity` only), to the same answer on both drivers.
//
// The workflows only report: they declare outputs that read the identity back,
// so what is compared is what an expression actually evaluated on each driver,
// not what was carried in.

// IdentityCase is one identity and what an expression must read back from it.
type IdentityCase struct {
	// Name says what the case is about, and becomes the subtest name.
	Name string

	// Identity is who is carried in; nil is "nobody said".
	Identity *v1.WorkloadIdentity

	// Principal is the `principal` both drivers must report.
	Principal string

	// Why says what the case pins.
	Why string
}

// IdentityCases is the shared table, used for both `run.identity` and
// `sender.identity`.
func IdentityCases() []IdentityCase {
	return []IdentityCase{
		{
			Name: "issuer and subject",
			Identity: &v1.WorkloadIdentity{
				Subject: "sre-lead@example.com", Issuer: "https://issuer.example.com",
				Claims: map[string]string{"team": "release-managers", "role": "sre-lead"},
			},
			Principal: "https://issuer.example.com#sre-lead@example.com",
			Why:       "the joined form is what a policy compares across identity providers",
		},
		{
			Name:      "no identity at all",
			Identity:  nil,
			Principal: "",
			Why:       "an unauthenticated or local caller is \"\", never \"#\", so two anonymous callers cannot compare equal as a principal",
		},
		{
			Name:      "subject without issuer",
			Identity:  &v1.WorkloadIdentity{Subject: "sre-lead@example.com"},
			Principal: "",
			Why:       "a subject is only unique within its issuer, so half a principal is no principal",
		},
		{
			Name:      "issuer without subject",
			Identity:  &v1.WorkloadIdentity{Issuer: "https://issuer.example.com"},
			Principal: "",
			Why:       "an issuer with nobody under it must not read as a principal that matches every anonymous caller of it",
		},
	}
}

// SenderIdentitySignal is the signal [SenderIdentityWorkflow] waits for.
const SenderIdentitySignal = "approve"

// SenderIdentityWorkflow returns a workflow that waits for the
// [SenderIdentitySignal] signal and reports the sender's identity through its
// declared outputs.
func SenderIdentityWorkflow() *v1.Workflow {
	return &v1.Workflow{
		Name: "sender-identity-shape",
		Steps: []*v1.Node{{
			Id:   "approval",
			Kind: &v1.Node_Wait{Wait: &v1.Wait{Kind: &v1.Wait_Signal{Signal: &v1.Signal{Name: SenderIdentitySignal}}}},
		}},
		DeclaredOutputs: []*v1.OutputDeclaration{
			{Name: "principal", Value: v1.NewExpr("approval.sender.identity.principal")},
			{Name: "subject", Value: v1.NewExpr("approval.sender.identity.subject")},
			{Name: "issuer", Value: v1.NewExpr("approval.sender.identity.issuer")},
		},
	}
}

// AssertSenderIdentity checks what [SenderIdentityWorkflow] reported against the case.
func AssertSenderIdentity(t testing.TB, outputs *v1.Workflow_StepOutputs, c IdentityCase) {
	t.Helper()

	assertIdentityOutputs(t, "sender.identity", outputs, c, false)
}

// RunIdentityPrincipalWorkflow is [RunIdentityWorkflow] plus `principal` and
// `claims`, under the same output names [SenderIdentityWorkflow] declares so one
// assertion reads both.
func RunIdentityPrincipalWorkflow() *v1.Workflow {
	w := RunIdentityWorkflow()
	w.DeclaredOutputs = append(w.DeclaredOutputs,
		&v1.OutputDeclaration{Name: "principal", Value: v1.NewExpr("run.identity.principal")},
		&v1.OutputDeclaration{Name: "claims", Value: v1.NewExpr("run.identity.claims")},
	)

	return w
}

// AssertRunIdentityPrincipal checks what [RunIdentityPrincipalWorkflow] reported against the case.
func AssertRunIdentityPrincipal(t testing.TB, outputs *v1.Workflow_StepOutputs, c IdentityCase) {
	t.Helper()

	assertIdentityOutputs(t, "run.identity", outputs, c, true)
}

func assertIdentityOutputs(t testing.TB, what string, outputs *v1.Workflow_StepOutputs, c IdentityCase, withClaims bool) {
	t.Helper()

	values := outputs.GetRunOutputs().GetValues()
	if values == nil {
		t.Fatalf("the run produced no declared outputs")
	}

	str := func(name string) string {
		v, ok := values[name]
		if !ok {
			t.Fatalf("the run's outputs have no %q field", name)
		}

		return v.GetLiteral().GetStringValue()
	}

	if got := str("principal"); got != c.Principal {
		t.Fatalf("%s.principal = %q, want %q (%s)", what, got, c.Principal, c.Why)
	}
	if got := str("subject"); got != c.Identity.GetSubject() {
		t.Fatalf("%s.subject = %q, want %q", what, got, c.Identity.GetSubject())
	}
	if got := str("issuer"); got != c.Identity.GetIssuer() {
		t.Fatalf("%s.issuer = %q, want %q", what, got, c.Identity.GetIssuer())
	}

	if !withClaims {
		if _, ok := values["claims"]; ok {
			t.Fatalf("%s reported claims, which a wait's sender does not carry", what)
		}

		return
	}

	claims, ok := values["claims"]
	if !ok {
		t.Fatalf("the run's outputs have no %q field", "claims")
	}
	got := map[string]string{}
	for _, entry := range claims.GetLiteral().GetMapValue().GetEntries() {
		got[entry.GetKey().GetStringValue()] = entry.GetValue().GetStringValue()
	}
	want := c.Identity.GetClaims()
	if want == nil {
		want = map[string]string{}
	}
	if !maps.Equal(got, want) {
		t.Fatalf("%s.claims = %v, want %v", what, got, want)
	}
}
