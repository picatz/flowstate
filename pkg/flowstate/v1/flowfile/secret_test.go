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
			// Nested is legal in an input the task applies itself; `args` is not
			// one of those, and neither is any input of `log`. The rule the
			// message states is about the input, not about the shape.
			name: "nested in a list of an input that cannot carry one",
			src: taskInput(`args:
          - ${secret('env:TOKEN')}
          - plain`),
			want: "cannot be nested inside this input's list or mapping",
		},
		{
			// The log task's fields are written to a worker's log and into
			// durable history, so no input of it accepts a reference — and the
			// message offers nothing, because there is nowhere on this task to
			// offer.
			name: "a log field",
			src: taskInput(`fields:
          token: ${secret('env:API_TOKEN')}`),
			want: "a secret the workflow resolved is a secret in durable history",
		},
		{
			// A reference and an expression in one structure. The schema cannot
			// hold both — see [flowstatev1.Value_Structure] — and the diagnostic
			// says which of the two to move, rather than reporting the reference
			// as misplaced when it is not.
			name: "an expression sharing the structure",
			src: httpInput(`headers:
        Authorization: ${secret('env:API_TOKEN')}
        X-Trace: ${steps.start.result}`),
			want: "cannot share a list or a mapping",
		},
		{
			// Nesting does not lift the combination rule: an entry is a whole
			// value like an input is, so the reference is still the whole of it
			// or nothing.
			name: "combined with text inside a header",
			src: httpInput(`headers:
        Authorization: ${'Bearer ' + secret('env:API_TOKEN')}`),
			want: "has to be the whole value of a task input",
		},
		{
			// The position query strings are refused in, refused where it is
			// written rather than on the first request.
			name: "a query parameter",
			src: httpInput(`query:
        token: ${secret('env:API_TOKEN')}`),
			want: `"http" accepts one in form, headers, json`,
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
			src: `edition: v2026.3
name: t
steps:
  - id: a
    for_each:
      items: ${secret('env:LIST')}
      steps:
        - id: b
          log:
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

// httpInput returns a workflow whose single step is an http step with the given
// input written under it.
//
// Separate from [taskInput], which builds a `log` step: where a reference may be
// nested is a property of the task, so a case about a header needs a task that has
// headers.
func httpInput(input string) string {
	return `edition: v2026.3
name: t
steps:
  - id: a
    http:
      url: https://api.example.com/events
      ` + input + "\n"
}

// TestSecretReferenceNestsWhereTheTaskAppliesIt covers the position this whole
// mechanism exists for: a reference inside a structure, compiled entry by entry so
// that the reference is still a reference in the specification.
//
// The assertion that matters is the *shape* of what compiled, not that it
// compiled: a structure whose Authorization entry is a SecretRef is the thing that
// travels; a mapping flattened into one expression would be a workflow evaluating
// a secret, which is what everything here is arranged to prevent.
func TestSecretReferenceNestsWhereTheTaskAppliesIt(t *testing.T) {
	tests := []struct {
		name  string
		src   string
		input string
		at    func(*v1.Value) *v1.Value
	}{
		{
			name: "a header",
			src: httpInput(`headers:
        Authorization: ${secret('env:API_TOKEN')}
        Accept: application/json`),
			input: "headers",
			at:    func(v *v1.Value) *v1.Value { return v.GetStructure().GetMap().GetEntries()["Authorization"] },
		},
		{
			name: "a form entry",
			src: httpInput(`form:
        client_secret: ${secret('env:API_TOKEN')}`),
			input: "form",
			at:    func(v *v1.Value) *v1.Value { return v.GetStructure().GetMap().GetEntries()["client_secret"] },
		},
		{
			name: "a json body, two levels down",
			src: httpInput(`json:
        auth:
          token: ${secret('env:API_TOKEN')}`),
			input: "json",
			at: func(v *v1.Value) *v1.Value {
				return v.GetStructure().GetMap().GetEntries()["auth"].
					GetStructure().GetMap().GetEntries()["token"]
			},
		},
		{
			name: "a json body's list element",
			src: httpInput(`json:
        tokens:
          - ${secret('env:API_TOKEN')}`),
			input: "json",
			at: func(v *v1.Value) *v1.Value {
				return v.GetStructure().GetMap().GetEntries()["tokens"].
					GetStructure().GetList().GetValues()[0]
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			workflow, err := flowfile.Unmarshal([]byte(tt.src))
			if err != nil {
				t.Fatalf("Unmarshal() error: %v", err)
			}

			value := workflow.GetSteps()[0].GetTask().GetInputs()[tt.input]
			if value.GetStructure() == nil {
				t.Fatalf("input %q is %v, want a structure carrying the reference", tt.input, value)
			}
			// Not an expression, which is the failure mode with teeth: a mapping
			// compiled into one CEL expression is a mapping the workflow evaluates.
			if value.GetExpr() != nil || value.GetLiteral() != nil {
				t.Errorf("input %q carries something other than a structure: %v", tt.input, value)
			}

			reference := tt.at(value).GetSecretRef()
			if reference == nil {
				t.Fatalf("the nested entry is %v, want a secret reference", tt.at(value))
			}
			if reference.GetScheme() != "env" || reference.GetName() != "API_TOKEN" {
				t.Errorf("reference = %q:%q, want env:API_TOKEN",
					reference.GetScheme(), reference.GetName())
			}

			// And `flow validate` accepts the file, which is the other half of a
			// capability being reachable: a spelling the compiler takes and the
			// validator refuses is not one an author can use.
			ds, err := flowfile.ValidateSource([]byte(tt.src))
			if err != nil {
				t.Fatalf("ValidateSource() error: %v", err)
			}
			if len(ds) != 0 {
				t.Fatalf("expected no diagnostics, got:\n%s", ds.Error())
			}

			requireRoundTrip(t, workflow)
		})
	}
}

// TestSecretReferenceReportsPosition pins that a reference inside a longer
// expression is reported at the call rather than at the start of the value, since
// the whole point of catching this at compile time is being able to point at it.
func TestSecretReferenceReportsPosition(t *testing.T) {
	src := `edition: v2026.3
name: t
steps:
  - id: a
    http:
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

	// `secret` begins at column 27 of line 6: the expression source starts at
	// column 15, and `'Bearer ' + ` is twelve characters.
	if ds[0].Line != 7 || ds[0].Column != 27 {
		t.Errorf("position = %d:%d, want 7:27\nreported: %s", ds[0].Line, ds[0].Column, ds[0].Error())
	}
	if ds[0].Step != "a" || ds[0].Field != "auth" {
		t.Errorf("diagnostic names step %q input %q, want \"a\" and \"auth\"", ds[0].Step, ds[0].Field)
	}
	t.Logf("reported: %s", ds[0].Error())
}

// TestSecretMarkerIsOnlyACall pins that the marker is a call and nothing else, so a
// step or an output named `secret` keeps working.
func TestSecretMarkerIsOnlyACall(t *testing.T) {
	src := `edition: v2026.3
name: t
steps:
  - id: secret
    log:
      message: hello
  - id: user
    log:
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
	good := []byte(`edition: v2026.3
name: uses-a-secret
steps:
  - id: notify
    http:
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

	bad := []byte(`edition: v2026.3
name: broken-secret
steps:
  - id: notify
    http:
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
				Name:   "log",
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
