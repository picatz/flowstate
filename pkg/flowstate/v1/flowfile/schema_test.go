package flowfile_test

import (
	"strings"
	"testing"

	"github.com/picatz/flowstate/pkg/flowstate/v1/flowfile"
)

// TestValidateTaskInputs covers what a task's own schema says about its inputs.
//
// Every case here used to run and fail on a worker, or — worse for the first one —
// run successfully with the input silently ignored.
func TestValidateTaskInputs(t *testing.T) {
	tests := []struct {
		name string
		src  string
		// want is a substring the diagnostics must contain; empty means the
		// Flowfile must validate cleanly.
		want string
	}{
		{
			name: "misspelled input",
			src:  echoInput("mesage: hello"),
			want: `task "echo" has no such input; did you mean "message"?`,
		},
		{
			name: "input no task declares, with nothing close",
			src:  echoInput("shout: hello"),
			want: `task "echo" has no such input; it accepts message`,
		},
		{
			name: "required input left out",
			src:  echoInput("{}"),
			want: `task "echo" requires input "message"`,
		},
		{
			// One mistake, one diagnostic: a typo is not also reported as the
			// required input it left unset.
			name: "a typo is not also a missing input",
			src:  echoInput("mesage: hello"),
			want: `did you mean "message"?`,
		},
		{
			name: "a list where a string belongs",
			src: echoInput(`message:
          - one
          - two`),
			want: "expected a string, but this is a list",
		},
		{
			name: "a number where a string belongs",
			src:  printfInput(`format: 42`),
			want: "expected a string, but this is a whole number",
		},
		{
			name: "a string where a mapping belongs",
			src: `name: t
steps:
  - id: a
    task:
      name: http
      inputs:
        url: https://example.com
        headers: nope
`,
			want: "expected a mapping, but this is a string",
		},
		{
			name: "a string where a list belongs",
			src: `name: t
steps:
  - id: a
    task:
      name: cel
      inputs:
        expr: "1 + 1"
        libs: math
`,
			want: "expected a list, but this is a string",
		},
		{
			name: "http without a url",
			src: `name: t
steps:
  - id: a
    task:
      name: http
      inputs:
        method: GET
`,
			want: `task "http" requires input "url"`,
		},

		// The cases below must produce nothing. A validator that reports a
		// workflow which runs correctly is worse than one that misses a mistake,
		// so these are the more important half of this table.
		{
			// The expression produces a list, which the field could not hold as a
			// literal — and it is still not reported, because an expression's type
			// is not knowable when the workflow is compiled.
			name: "an expression is not type-checked",
			src: `name: t
steps:
  - id: count
    task:
      name: echo
      inputs:
        message: one
  - id: a
    task:
      name: echo
      inputs:
        message: ${[count.result]}
`,
		},
		{
			name: "a whole number satisfies a floating-point field",
			src: `name: t
steps:
  - id: a
    retry:
      backoff: 2
    task:
      name: echo
      inputs:
        message: hi
`,
		},
		{
			name: "the cel task accepts inputs its schema does not declare",
			src: `name: t
steps:
  - id: a
    task:
      name: cel
      inputs:
        expr: "vars.anything"
        vars:
          anything: hello
`,
		},
		{
			name: "an input the task evaluates itself is not checked",
			src: `name: t
steps:
  - id: a
    task:
      name: http
      inputs:
        url: https://example.com
        outputs: "${ {'status': status_code} }"
`,
		},
		{
			name: "a literal mapping for a declared mapping",
			src: `name: t
steps:
  - id: a
    task:
      name: http
      inputs:
        url: https://example.com
        headers:
          Accept: application/json
`,
		},
		{
			name: "an unknown task is reported once, not input by input",
			src: `name: t
steps:
  - id: a
    task:
      name: nosuchtask
      inputs:
        whatever: 1
`,
			want: `unknown task "nosuchtask"`,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ds, err := flowfile.ValidateSource([]byte(tt.src))
			if err != nil {
				t.Fatalf("ValidateSource() error: %v", err)
			}

			if tt.want == "" {
				if len(ds) != 0 {
					t.Fatalf("expected no diagnostics, got:\n%s", ds.Error())
				}
				return
			}

			got := ds.Error()
			if !strings.Contains(got, tt.want) {
				t.Errorf("diagnostics do not mention %q; got:\n%s", tt.want, got)
			}
			t.Logf("reported: %s", got)
		})
	}
}

// TestValidateTaskInputsInNestedSteps pins that a body step and a branch step are
// checked too, since a loop body is where a workflow does most of its work.
func TestValidateTaskInputsInNestedSteps(t *testing.T) {
	src := `name: t
steps:
  - id: loop
    for_each:
      items: ${[1]}
      steps:
        - id: body
          task:
            name: echo
            inputs:
              mesage: hi
  - id: fan
    parallel:
      - steps:
          - id: branch
            task:
              name: echo
              inputs: {}
`
	ds, err := flowfile.ValidateSource([]byte(src))
	if err != nil {
		t.Fatalf("ValidateSource() error: %v", err)
	}

	got := ds.Error()
	for _, want := range []string{
		`step "body" input "mesage"`,
		`step "branch": task "echo" requires input "message"`,
	} {
		if !strings.Contains(got, want) {
			t.Errorf("diagnostics do not mention %q; got:\n%s", want, got)
		}
	}
	t.Logf("reported:\n%s", got)
}

// TestValidateTaskInputsPositions pins that these diagnostics carry the position of
// the input at fault, which is the whole reason they are worth having over a runtime
// failure.
func TestValidateTaskInputsPositions(t *testing.T) {
	src := `name: t
steps:
  - id: a
    task:
      name: echo
      inputs:
        mesage: hello
`
	ds, err := flowfile.ValidateSource([]byte(src))
	if err != nil {
		t.Fatalf("ValidateSource() error: %v", err)
	}
	if len(ds) != 1 {
		t.Fatalf("expected one diagnostic, got %d:\n%s", len(ds), ds.Error())
	}
	// The value of the offending input begins at column 17 of line 7.
	if ds[0].Line != 7 || ds[0].Column != 17 {
		t.Errorf("position = %d:%d, want 7:17\nreported: %s", ds[0].Line, ds[0].Column, ds[0].Error())
	}
}

// echoInput returns a workflow whose single echo step has the given inputs body.
func echoInput(inputs string) string {
	return `name: t
steps:
  - id: a
    task:
      name: echo
      inputs:
        ` + inputs + "\n"
}

// printfInput returns a workflow whose single printf step has the given inputs,
// plus the args printf requires.
func printfInput(inputs string) string {
	return `name: t
steps:
  - id: a
    task:
      name: printf
      inputs:
        args: [1]
        ` + inputs + "\n"
}
