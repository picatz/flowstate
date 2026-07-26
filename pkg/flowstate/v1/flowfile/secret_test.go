package flowfile_test

import (
	"strings"
	"testing"

	v1 "github.com/picatz/flowstate/pkg/flowstate/v1"
	"github.com/picatz/flowstate/pkg/flowstate/v1/flowfile"
)

// TestSecretReferenceCompiles pins that ${secret(...)} becomes a reference in the
// specification rather than a call for something to evaluate later.
func TestSecretReferenceCompiles(t *testing.T) {
	tests := []struct {
		name   string
		input  string
		scheme string
		ref    string
	}{
		{
			name:   "single quotes",
			input:  `token: ${secret('env:API_KEY')}`,
			scheme: "env",
			ref:    "API_KEY",
		},
		{
			name:   "double quotes",
			input:  `token: ${secret("env:API_KEY")}`,
			scheme: "env",
			ref:    "API_KEY",
		},
		{
			// Everything after the first colon is the name, and what it means is
			// the backend's business. The DSL does not learn vault's path syntax.
			name:   "the name is opaque to the DSL",
			input:  `token: ${secret('vault:prod/api#token')}`,
			scheme: "vault",
			ref:    "prod/api#token",
		},
		{
			name:   "a name may contain colons",
			input:  `token: ${secret('keychain:login:github')}`,
			scheme: "keychain",
			ref:    "login:github",
		},
		{
			name:   "spacing inside the fence does not matter",
			input:  `token: "${ secret( 'env:API_KEY' ) }"`,
			scheme: "env",
			ref:    "API_KEY",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			workflow, err := flowfile.Unmarshal([]byte(taskInput(tt.input)))
			if err != nil {
				t.Fatalf("Unmarshal() error: %v", err)
			}

			value := workflow.GetSteps()[0].GetTask().GetInputs()["token"]
			reference := value.GetSecretRef()
			if reference == nil {
				t.Fatalf("input is %v, want a secret reference", value)
			}
			if reference.GetScheme() != tt.scheme || reference.GetName() != tt.ref {
				t.Errorf("reference = %q:%q, want %q:%q",
					reference.GetScheme(), reference.GetName(), tt.scheme, tt.ref)
			}

			// The specification must carry the reference and nothing resembling a
			// value, so the other kinds have to be empty.
			if value.GetExpr() != nil || value.GetLiteral() != nil {
				t.Errorf("value carries more than a reference: %v", value)
			}

			requireRoundTrip(t, workflow)
		})
	}
}

// TestSecretReferenceRejected covers every placement a reference cannot survive,
// and the malformed references that used to compile and fail at run time.
//
// The bug this replaced is the reason each case asserts a message as well as a
// failure: `flow validate` reported "ok" for all of these, because CEL's parser
// accepts a call to a function it has never heard of.
func TestSecretReferenceRejected(t *testing.T) {
	tests := []struct {
		name string
		src  string
		want string
	}{
		{
			name: "combined with literal text",
			src:  taskInput(`token: ${'Bearer ' + secret('env:TOKEN')}`),
			want: "has to be the whole value of a task input",
		},
		{
			name: "nested in a list",
			src: taskInput(`args:
          - ${secret('env:TOKEN')}
          - plain`),
			want: "cannot be nested inside a list or a mapping",
		},
		{
			// The header case, which is the first thing anyone writes. It is
			// refused because there is no reference to emit: the whole mapping
			// compiles to one expression the workflow evaluates. The message has
			// to say that rather than cite the combination rule, which does not
			// apply — nothing is being combined with this secret.
			name: "an authorization header",
			src: taskInput(`headers:
          Authorization: ${secret('env:API_TOKEN')}
          Accept: application/json`),
			want: "current limitation rather than a mistake in the file",
		},
		{
			name: "passed to another call",
			src:  taskInput(`token: ${string(secret('env:TOKEN'))}`),
			want: "has to be the whole value of a task input",
		},
		{
			name: "in a condition",
			src:  stepWith(`if: ${secret('env:TOKEN') == 'x'}`),
			want: "cannot go where the workflow evaluates the value itself",
		},
		{
			name: "as the whole condition",
			src:  stepWith(`if: ${secret('env:TOKEN')}`),
			want: "cannot go where the workflow evaluates the value itself",
		},
		{
			name: "in a loop's items",
			src: `name: t
steps:
  - id: a
    for_each:
      items: ${secret('env:LIST')}
      steps:
        - id: b
          task:
            name: echo
            inputs:
              message: hi
`,
			want: "cannot go where the workflow evaluates the value itself",
		},
		{
			name: "no scheme",
			src:  taskInput(`token: ${secret('API_KEY')}`),
			want: `"API_KEY" has no provider, want a reference of the form "scheme:name"`,
		},
		{
			name: "empty name",
			src:  taskInput(`token: ${secret('env:')}`),
			want: "name must not be empty",
		},
		{
			name: "scheme with illegal characters",
			src:  taskInput(`token: ${secret('Env Vars:API_KEY')}`),
			want: "may only contain lowercase letters, digits, and dashes",
		},
		{
			// The escape reaches CEL as source and becomes a real control
			// character in the compiled name, which is why this check lives in
			// code: the schema's pattern cannot express it.
			name: "control character in the name",
			src:  taskInput(`token: ${secret('env:API\u0007KEY')}`),
			want: "control character",
		},
		{
			name: "no argument",
			src:  taskInput(`token: ${secret()}`),
			want: "takes one reference, written out",
		},
		{
			name: "two arguments",
			src:  taskInput(`token: ${secret('env', 'API_KEY')}`),
			want: "takes one reference, written out",
		},
		{
			name: "a computed reference",
			src:  taskInput(`token: ${secret('env:' + which.result)}`),
			want: "takes one reference, written out",
		},
		{
			name: "a non-string reference",
			src:  taskInput(`token: ${secret(42)}`),
			want: "takes one reference, written out",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := flowfile.Unmarshal([]byte(tt.src))
			if err == nil {
				t.Fatal("Unmarshal() succeeded; a secret reference here should be a compile error")
			}
			if !strings.Contains(err.Error(), tt.want) {
				t.Errorf("diagnostics do not mention %q; got:\n%v", tt.want, err)
			}
			t.Logf("reported: %v", err)
		})
	}
}

// TestSecretReferenceReportsPosition pins that a reference inside a longer
// expression is reported at the call rather than at the start of the value, since
// the whole point of catching this at compile time is being able to point at it.
func TestSecretReferenceReportsPosition(t *testing.T) {
	src := `name: t
steps:
  - id: a
    task:
      name: http
      inputs:
        url: https://example.com
        auth: ${'Bearer ' + secret('env:TOKEN')}
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

	// `secret` begins at column 29 of line 8: the expression source starts at
	// column 17, and `'Bearer ' + ` is twelve characters.
	if ds[0].Line != 8 || ds[0].Column != 29 {
		t.Errorf("position = %d:%d, want 8:29\nreported: %s", ds[0].Line, ds[0].Column, ds[0].Error())
	}
	if ds[0].Step != "a" || ds[0].Field != "auth" {
		t.Errorf("diagnostic names step %q input %q, want \"a\" and \"auth\"", ds[0].Step, ds[0].Field)
	}
	t.Logf("reported: %s", ds[0].Error())
}

// TestSecretMarkerIsOnlyACall pins that the marker is a call and nothing else, so a
// step or an output named `secret` keeps working.
func TestSecretMarkerIsOnlyACall(t *testing.T) {
	src := `name: t
steps:
  - id: secret
    task:
      name: echo
      inputs:
        message: hello
  - id: user
    task:
      name: echo
      inputs:
        from_step: ${secret.result}
        bare: ${secret}
        nested: ${secret.result.size()}
`
	workflow, err := flowfile.Unmarshal([]byte(src))
	if err != nil {
		t.Fatalf("Unmarshal() error: %v", err)
	}

	for _, name := range []string{"from_step", "bare", "nested"} {
		value := workflow.GetSteps()[1].GetTask().GetInputs()[name]
		if value.GetSecretRef() != nil {
			t.Errorf("input %q compiled to a secret reference: %v", name, value)
		}
		if value.GetExpr() == nil {
			t.Errorf("input %q = %v, want an ordinary expression", name, value)
		}
	}
}

// TestSecretReferenceValidates covers the whole authoring path, which is what the
// bug was really about: `flow validate` reported "ok" and the run failed.
func TestSecretReferenceValidates(t *testing.T) {
	good := []byte(`name: uses-a-secret
steps:
  - id: notify
    task:
      name: http
      inputs:
        method: POST
        url: https://api.example.com/events
        body: ${secret('vault:prod/api#token')}
`)
	ds, err := flowfile.ValidateSource(good)
	if err != nil {
		t.Fatalf("ValidateSource() error: %v", err)
	}
	if len(ds) != 0 {
		t.Fatalf("expected no diagnostics, got:\n%s", ds.Error())
	}

	bad := []byte(`name: broken-secret
steps:
  - id: notify
    task:
      name: http
      inputs:
        method: POST
        url: https://api.example.com/events
        body: ${secret('API_KEY')}
`)
	if _, err := flowfile.ValidateSource(bad); err == nil {
		t.Error("ValidateSource() accepted a malformed reference; it used to say ok and fail at run time")
	} else {
		t.Logf("reported: %v", err)
	}
}

// TestSecretReferenceMarshal pins the two directions Marshal has to get right: a
// reference is written back as the marker, and one built by hand somewhere it
// cannot go is refused rather than written.
func TestSecretReferenceMarshal(t *testing.T) {
	reference := &v1.Value{Kind: &v1.Value_SecretRef{SecretRef: &v1.SecretRef{
		Scheme: "vault",
		Name:   "prod/api#token",
	}}}

	workflow := &v1.Workflow{
		Name: "t",
		Steps: []*v1.Node{{
			Id: "a",
			Kind: &v1.Node_Task{Task: &v1.Task{
				Name:   "http",
				Inputs: map[string]*v1.Value{"token": reference},
			}},
		}},
	}

	data, err := flowfile.Marshal(workflow)
	if err != nil {
		t.Fatalf("Marshal() error: %v", err)
	}
	if !strings.Contains(string(data), `${secret('vault:prod/api#token')}`) {
		t.Errorf("reference was not written as the marker:\n%s", data)
	}
	requireRoundTrip(t, workflow)

	// A condition cannot hold one, so writing a workflow that has one there would
	// produce a file that does not compile.
	condition := &v1.Workflow{
		Name: "t",
		Steps: []*v1.Node{{
			Id:        "a",
			Condition: reference,
			Kind: &v1.Node_Task{Task: &v1.Task{
				Name:   "echo",
				Inputs: map[string]*v1.Value{"message": v1.NewLiteral("hi")},
			}},
		}},
	}
	if _, err := flowfile.Marshal(condition); err == nil {
		t.Error("Marshal() wrote a secret reference as a condition")
	} else {
		t.Logf("reported: %v", err)
	}
}
