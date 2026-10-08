package flowfile_test

import (
	"context"
	"strings"
	"testing"

	v1 "github.com/picatz/flowstate/pkg/flowstate/v1"
	"github.com/picatz/flowstate/pkg/flowstate/v1/flowfile"
)

// TestCredentialReferenceCompiles pins that ${credential(...)} becomes a
// reference in the specification, the way ${secret(...)} does, rather than a call
// for something to evaluate later.
func TestCredentialReferenceCompiles(t *testing.T) {
	tests := []struct {
		name  string
		input string
	}{
		{name: "single quotes", input: `api_key: ${credential('anthropic')}`},
		{name: "double quotes", input: `api_key: ${credential("anthropic")}`},
		{name: "spacing inside the fence does not matter", input: `api_key: "${ credential( 'anthropic' ) }"`},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			workflow, err := flowfile.Unmarshal([]byte(taskInput(tt.input)))
			if err != nil {
				t.Fatalf("Unmarshal() error: %v", err)
			}

			value := workflow.GetSteps()[0].GetTask().GetInputs()["api_key"]
			if got := value.GetCredentialRef().GetTarget(); got != "anthropic" {
				t.Fatalf("input is %v, want a credential reference to %q", value, "anthropic")
			}
			// Nothing but the reference: an expression or literal beside it would
			// be a value the workflow could hold.
			if value.GetExpr() != nil || value.GetLiteral() != nil || value.GetSecretRef() != nil {
				t.Errorf("value carries more than a reference: %v", value)
			}
			if !v1.ValueHoldsCredentialRef(value) {
				t.Error("ValueHoldsCredentialRef = false for a credential reference")
			}
			if v1.ValueHoldsSecretRef(value) {
				t.Error("ValueHoldsSecretRef = true for a credential reference; the two answer different questions")
			}

			requireRoundTrip(t, workflow)
		})
	}
}

// TestCredentialReferenceRejected covers every placement a credential reference
// cannot survive and the malformed calls, and asserts the message as well as the
// failure: a validator that passes and then breaks is worse than none.
func TestCredentialReferenceRejected(t *testing.T) {
	const whole = "has to be the whole value of a task input"
	const notHere = "can only be the whole value of a task input"
	const nested = "cannot be nested inside a list or a mapping"

	tests := []struct {
		name string
		src  string
		want string
	}{
		{
			name: "combined with literal text in an expression",
			src:  taskInput(`api_key: ${'Bearer ' + credential('anthropic')}`),
			want: whole,
		},
		{
			name: "passed to another call",
			src:  taskInput(`api_key: ${string(credential('anthropic'))}`),
			want: whole,
		},
		{
			name: "interpolated among text",
			src:  taskInput(`api_key: Bearer ${credential('anthropic')}`),
			want: whole,
		},
		{
			// A credential is the whole value of an input and nothing else, so
			// where a secret may nest (http's headers) a credential may not.
			name: "nested in a mapping the task would apply entry by entry",
			src: httpInput(`headers:
        Authorization: ${credential('anthropic')}`),
			want: nested,
		},
		{
			name: "nested in a list",
			src: taskInput(`args:
          - ${credential('anthropic')}
          - plain`),
			want: nested,
		},
		{
			name: "beside a secret in one structure",
			src: httpInput(`headers:
        Authorization: ${credential('anthropic')}
        X-Other: ${secret('env:TOKEN')}`),
			want: nested,
		},
		{
			name: "in a condition",
			src:  stepWith(`if: ${credential('anthropic') == 'x'}`),
			want: notHere,
		},
		{
			name: "as the whole condition",
			src:  stepWith(`if: ${credential('anthropic')}`),
			want: notHere,
		},
		{
			name: "in a loop's items",
			src: `edition: v2026.4
name: t
steps:
  - id: a
    for_each:
      items: ${credential('anthropic')}
      steps:
        - id: b
          log:
            message: hi
`,
			want: notHere,
		},
		{
			name: "in a wait's shaped output",
			src: `edition: v2026.4
name: t
steps:
  - id: gate
    wait_for_signal:
      name: approved
      outputs:
        token: ${credential('anthropic')}
`,
			want: notHere,
		},
		{
			name: "in a loop's init",
			src: `edition: v2026.4
name: t
steps:
  - id: a
    loop:
      as: state
      init: ${credential('anthropic')}
      until: ${true}
      steps:
        - id: inner
          log:
            message: hi
`,
			want: notHere,
		},
		{
			name: "in a workflow var",
			src: `edition: v2026.4
name: t
vars:
  key: ${credential('anthropic')}
steps:
  - id: a
    log:
      message: hi
`,
			want: notHere,
		},
		{
			name: "no argument",
			src:  taskInput(`api_key: ${credential()}`),
			want: "takes one federation target, written out",
		},
		{
			name: "two arguments",
			src:  taskInput(`api_key: ${credential('a', 'b')}`),
			want: "takes one federation target, written out",
		},
		{
			name: "a computed target",
			src:  taskInput(`api_key: ${credential('any' + which.result)}`),
			want: "takes one federation target, written out",
		},
		{
			name: "a non-string target",
			src:  taskInput(`api_key: ${credential(42)}`),
			want: "takes one federation target, written out",
		},
		{
			name: "an empty target",
			src:  taskInput(`api_key: ${credential('')}`),
			want: "credential target must not be empty",
		},
		{
			name: "a control character in the target",
			src:  taskInput(`api_key: ${credential('any\u0007thing')}`),
			want: "control character",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := flowfile.Unmarshal([]byte(tt.src))
			if err == nil {
				t.Fatal("Unmarshal() succeeded; a credential reference here should be a compile error")
			}
			if !strings.Contains(err.Error(), tt.want) {
				t.Errorf("diagnostics do not mention %q; got:\n%v", tt.want, err)
			}
			// Said in the credential's own terms, never the secret's: an author
			// told to "keep the secret out of history" for a credential is sent
			// hunting for the wrong mistake.
			if strings.Contains(err.Error(), "a secret reference") {
				t.Errorf("diagnostics describe a credential as a secret reference:\n%v", err)
			}
		})
	}
}

// TestCredentialReferenceInsideACallBoundary pins that a call's `with:` refuses a
// credential reference as it does a secret, in the position's own terms.
func TestCredentialReferenceInsideACallBoundary(t *testing.T) {
	dir := t.TempDir()
	writeFile(t, dir, "callee.yaml", simpleCalleeSource)

	for name, with := range map[string]string{
		"bare":                  "tenant: ${credential('anthropic')}",
		"nested in a structure": "tenant: ${credential('anthropic')}\n      other: plain",
	} {
		t.Run(name, func(t *testing.T) {
			caller := writeFile(t, dir, "caller-"+strings.ReplaceAll(name, " ", "-")+".yaml", `edition: v2026.4
name: caller
steps:
  - id: provision
    call: ./callee.yaml
    with:
      `+with+`
`)

			_, _, err := flowfile.ParseFile(caller)
			if err == nil {
				t.Fatal("ParseFile() succeeded; a credential reference may not cross a call boundary")
			}
			if !strings.Contains(err.Error(), "credential reference") {
				t.Errorf("diagnostics do not name a credential reference:\n%v", err)
			}
		})
	}
}

// TestCredentialReferenceReportsPosition pins that a refusal lands on the call,
// not at the start of the value.
func TestCredentialReferenceReportsPosition(t *testing.T) {
	src := `edition: v2026.4
name: t
steps:
  - id: a
    http:
      url: https://example.com
      auth: ${'Bearer ' + credential('anthropic')}
`
	_, _, err := flowfile.Parse([]byte(src))
	if err == nil {
		t.Fatal("Parse() succeeded, want a diagnostic")
	}

	var ds flowfile.Diagnostics
	if !asDiagnostics(err, &ds) {
		t.Fatalf("Parse() error is %T, want Diagnostics: %v", err, err)
	}
	if len(ds) != 1 {
		t.Fatalf("expected exactly one diagnostic, got %d:\n%s", len(ds), ds.Error())
	}

	// `credential` begins at column 27 of line 7, as `secret` does in the sibling
	// test: the expression source starts at column 15 and `'Bearer ' + ` is
	// twelve characters.
	if ds[0].Line != 7 || ds[0].Column != 27 {
		t.Errorf("position = %d:%d, want 7:27\nreported: %s", ds[0].Line, ds[0].Column, ds[0].Error())
	}
}

// TestCredentialMarkerIsOnlyACall pins that the marker is a call and nothing
// else, so a step or an input named `credential` keeps working: the http task has
// a `credential:` input of its own.
func TestCredentialMarkerIsOnlyACall(t *testing.T) {
	src := `edition: v2026.4
name: t
steps:
  - id: credential
    log:
      message: hello
  - id: user
    log:
      from_step: ${credential.result}
      bare: ${credential}
      nested: ${credential.result.size()}
`
	workflow, err := flowfile.Unmarshal([]byte(src))
	if err != nil {
		t.Fatalf("Unmarshal() error: %v", err)
	}

	for _, name := range []string{"from_step", "bare", "nested"} {
		value := workflow.GetSteps()[1].GetTask().GetInputs()[name]
		if value.GetCredentialRef() != nil {
			t.Errorf("input %q compiled to a credential reference: %v", name, value)
		}
		if value.GetExpr() == nil {
			t.Errorf("input %q = %v, want an ordinary expression", name, value)
		}
	}
}

// TestCredentialReferenceValidates covers the authoring path: `flow validate`
// accepts a well-formed reference and refuses a malformed one at the line, rather
// than reporting ok and failing at run time.
func TestCredentialReferenceValidates(t *testing.T) {
	// A credential is minted for a task's declared secret input, so the task it
	// is written on has to declare one; see
	// TestCredentialReferenceIsAcceptedOnlyWhereATaskTakesASecret for the refusal.
	const task = "test_credential_validates_probe"
	if err := v1.DefaultRegistry().Register(v1.TaskDef{
		Name:         task,
		Inputs:       (&v1.Task_Log_Inputs{}).ProtoReflect().Descriptor(),
		SecretInputs: []string{"message"},
		Fn: func(context.Context, map[string]*v1.Value, *v1.Scope) (*v1.Node_Outputs, error) {
			return nil, nil
		},
	}); err != nil {
		t.Fatalf("Register() error: %v", err)
	}
	t.Cleanup(func() { v1.DefaultRegistry().Unregister(task) })

	good := []byte(`edition: v2026.4
name: uses-a-credential
steps:
  - id: notify
    ` + task + `:
      message: ${credential('anthropic')}
`)
	ds, err := flowfile.ValidateSource(good)
	if err != nil {
		t.Fatalf("ValidateSource() error: %v", err)
	}
	if len(ds) != 0 {
		t.Fatalf("expected no diagnostics, got:\n%s", ds.Error())
	}

	if _, err := flowfile.ValidateSource([]byte(strings.Replace(string(good), "credential('anthropic')", "credential(which.result)", 1))); err == nil {
		t.Error("ValidateSource() accepted a computed target; it must be written out")
	}
}

// TestCredentialReferenceMarshal pins both directions Marshal has to get right: a
// reference is written back as the marker, and one built by hand somewhere it
// cannot go is refused rather than written.
func TestCredentialReferenceMarshal(t *testing.T) {
	reference := v1.NewCredentialRef("anthropic")

	workflow := &v1.Workflow{
		Name: "t",
		Steps: []*v1.Node{{
			Id: "a",
			Kind: &v1.Node_Task{Task: &v1.Task{
				Name:   "log",
				Inputs: map[string]*v1.Value{"api_key": reference},
			}},
		}},
	}

	data, err := flowfile.Marshal(workflow)
	if err != nil {
		t.Fatalf("Marshal() error: %v", err)
	}
	if !strings.Contains(string(data), `${credential('anthropic')}`) {
		t.Errorf("reference was not written as the marker:\n%s", data)
	}
	requireRoundTrip(t, workflow)

	condition := &v1.Workflow{
		Name: "t",
		Steps: []*v1.Node{{
			Id:        "a",
			Condition: reference,
			Kind: &v1.Node_Task{Task: &v1.Task{
				Name:   "log",
				Inputs: map[string]*v1.Value{"message": v1.NewLiteral("hi")},
			}},
		}},
	}
	if _, err := flowfile.Marshal(condition); err == nil {
		t.Error("Marshal() wrote a credential reference as a condition")
	}

	malformed := &v1.Workflow{
		Name: "t",
		Steps: []*v1.Node{{
			Id: "a",
			Kind: &v1.Node_Task{Task: &v1.Task{
				Name:   "log",
				Inputs: map[string]*v1.Value{"api_key": v1.NewCredentialRef("")},
			}},
		}},
	}
	if _, err := flowfile.Marshal(malformed); err == nil {
		t.Error("Marshal() wrote a credential reference with no target")
	}
}
