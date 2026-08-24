package conformance

import (
	"testing"

	v1 "github.com/picatz/flowstate/pkg/flowstate/v1"
)

// RunIdentityWorkflow returns a one-step workflow that reports the run's own
// starter identity through its declared outputs — `run.local`,
// `run.identity.subject`, `run.identity.issuer`, `run.identity.namespace` — so
// a test can compare what each driver actually exposed under [v1.RunRoot]
// rather than trusting that it matches what was carried in.
//
// One workflow for both drivers, for the reason every shared [Case] is: a value
// with one meaning has to be checked once, from one definition, or the two
// drivers can silently disagree about it the way CLAUDE.md's retry-attempts
// story describes. The one step exists only so the run has something to do;
// what is under test is the declared outputs, evaluated the same moment a run's
// outputs always are.
func RunIdentityWorkflow() *v1.Workflow {
	return &v1.Workflow{
		Name: "run-identity-shape",
		Steps: []*v1.Node{
			{
				Id: "report",
				Kind: &v1.Node_Task{Task: &v1.Task{
					Name:   "log",
					Inputs: map[string]*v1.Value{"message": v1.NewLiteral("reporting run identity")},
				}},
			},
		},
		DeclaredOutputs: []*v1.OutputDeclaration{
			{Name: "local", Value: v1.NewExpr("run.local")},
			{Name: "subject", Value: v1.NewExpr("run.identity.subject")},
			{Name: "issuer", Value: v1.NewExpr("run.identity.issuer")},
			{Name: "namespace", Value: v1.NewExpr("run.identity.namespace")},
		},
	}
}

// AssertRunIdentityShape checks that a run's declared outputs report the run's
// starter identity with the shape every driver must produce, and that `local`
// never lies: true only for a run nothing authenticated, false only for one a
// driver attests — the identical rule [AssertSignalSenderShape] states for a
// wait's `sender`, applied to the run itself.
func AssertRunIdentityShape(t testing.TB, outputs *v1.Workflow_StepOutputs, wantLocal bool, wantSubject string) {
	t.Helper()

	run := outputs.GetRunOutputs()
	if run == nil {
		t.Fatalf("the run produced no declared outputs")
	}

	values := run.GetValues()

	local, ok := values["local"]
	if !ok {
		t.Fatalf("the run's outputs have no %q field", "local")
	}
	if got := local.GetLiteral().GetBoolValue(); got != wantLocal {
		t.Fatalf("run.local = %v, want %v — a local run must never look like an attested "+
			"production one, and an attested one must never be reported as local",
			got, wantLocal)
	}

	subject, ok := values["subject"]
	if !ok {
		t.Fatalf("the run's outputs have no %q field", "subject")
	}
	if got := subject.GetLiteral().GetStringValue(); got != wantSubject {
		t.Fatalf("run.identity.subject = %q, want %q", got, wantSubject)
	}

	for _, field := range []string{"issuer", "namespace"} {
		if _, ok := values[field]; !ok {
			t.Fatalf("the run's outputs have no %q field", field)
		}
	}
}
