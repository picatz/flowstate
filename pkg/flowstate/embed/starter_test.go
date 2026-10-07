package embed

import (
	"context"
	"strings"
	"testing"

	v1 "github.com/picatz/flowstate/pkg/flowstate/v1"
	"github.com/picatz/flowstate/pkg/flowstate/v1/auth"
)

const starterIssuer = "https://issuer.example"

// starterWorkflow runs one custom task, so a run that reached its step is
// visible through the returned output and a refused one never produced it.
func starterWorkflow(t *testing.T, manual string) (*Workflow, RunOptions) {
	t.Helper()

	tasks := NewTasks()
	if err := tasks.Register(Task{
		Name: "starter_probe", Summary: "reports that it ran",
		Fn: func(context.Context, map[string]*v1.Value, *v1.Scope) (*v1.Node_Outputs, error) {
			return &v1.Node_Outputs{NamedValues: v1.NewNamedValues(map[string]any{"ran": true})}, nil
		},
	}); err != nil {
		t.Fatal(err)
	}
	uninstall, err := tasks.Install()
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(uninstall)

	workflow, diags, err := Compile([]byte(`
edition: v2026.4
name: starter-test
triggers:
` + manual + `
steps:
  - id: probe
    starter_probe: {}
`))
	if err != nil {
		t.Fatalf("Compile: %v diags=%v", err, diags)
	}

	return workflow, RunOptions{Tasks: tasks}
}

// TestRunLocal_StarterIsHeldToTheManualBlock proves RunOptions.Starter applies
// `triggers.manual:` the way a server does, and that an unset Starter is the
// ungated rehearsal `flow run local` is. Each refusal also asserts the step
// never ran, so a check that only reported an error after running would fail.
func TestRunLocal_StarterIsHeldToTheManualBlock(t *testing.T) {
	oncall := auth.Principal{Issuer: starterIssuer, Subject: "oncall"}
	allow := "  - manual:\n      allow: ${sender.identity.principal in [\"" + starterIssuer + "#oncall\"]}"

	tests := []struct {
		name    string
		manual  string
		starter *Starter
		refused string
	}{
		{"no starter is not gated", allow, nil, ""},
		{"a listed caller starts", allow, &Starter{Principal: oncall}, ""},
		{"an unlisted caller is refused", allow,
			&Starter{Principal: auth.Principal{Issuer: starterIssuer, Subject: "stranger"}}, "refuses this manual start"},
		{"the same subject under another issuer is refused", allow,
			&Starter{Principal: auth.Principal{Issuer: "https://other.example", Subject: "oncall"}}, "refuses this manual start"},
		{"a zero principal can satisfy no predicate", allow, &Starter{}, "no authenticated issuer-qualified principal"},
		{"the anonymous principal can satisfy no predicate", allow,
			&Starter{Principal: auth.Principal{Issuer: auth.AnonymousIssuer, Subject: "anonymous"}}, "no authenticated issuer-qualified principal"},
		{"denied refuses every starter", "  - manual: denied", &Starter{Principal: oncall}, "manual: denied"},
		{"a missing reason is refused", "  - manual:\n      require_reason: true", &Starter{Principal: oncall}, "requires a reason"},
		{"a given reason starts", "  - manual:\n      require_reason: true", &Starter{Principal: oncall, Reason: "incident 42"}, ""},
		{"no manual block admits any starter", "  - schedule: { cron: \"0 * * * *\" }", &Starter{}, ""},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			workflow, opts := starterWorkflow(t, test.manual)
			opts.Starter = test.starter

			outputs, err := RunLocal(context.Background(), workflow, opts)
			if test.refused == "" {
				if err != nil {
					t.Fatalf("RunLocal: %v", err)
				}
				if ran, _ := StepOutput(outputs, "probe", "ran"); ran != true {
					t.Fatalf("the step did not run: %v", outputs)
				}

				return
			}

			if err == nil || !strings.Contains(err.Error(), test.refused) {
				t.Fatalf("RunLocal error = %v, want it to contain %q", err, test.refused)
			}
			if outputs != nil {
				t.Fatalf("a refused start ran a step: %v", outputs)
			}
		})
	}
}
