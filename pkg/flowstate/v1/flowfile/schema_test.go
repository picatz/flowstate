package flowfile_test

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

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
    http:
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
    cel:
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
    http:
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
    echo:
      message: one
  - id: a
    echo:
      message: ${[steps.count.result]}
`,
		},
		{
			name: "a whole number satisfies a floating-point field",
			src: `name: t
steps:
  - id: a
    retry:
      backoff: 2
    echo:
      message: hi
`,
		},
		{
			name: "the cel task accepts inputs its schema does not declare",
			src: `name: t
steps:
  - id: a
    cel:
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
    http:
      url: https://example.com
      outputs: "${ {'status': status_code} }"
`,
		},
		{
			name: "a literal mapping for a declared mapping",
			src: `name: t
steps:
  - id: a
    http:
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
    nosuchtask:
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
          echo:
            mesage: hi
  - id: fan
    parallel:
      - steps:
          - id: branch
            echo:
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
    echo:
      mesage: hello
`
	ds, err := flowfile.ValidateSource([]byte(src))
	if err != nil {
		t.Fatalf("ValidateSource() error: %v", err)
	}
	if len(ds) != 1 {
		t.Fatalf("expected one diagnostic, got %d:\n%s", len(ds), ds.Error())
	}
	// The value of the offending input begins at column 15 of line 5.
	if ds[0].Line != 5 || ds[0].Column != 15 {
		t.Errorf("position = %d:%d, want 5:15\nreported: %s", ds[0].Line, ds[0].Column, ds[0].Error())
	}
}

// echoInput returns a workflow whose single echo step has the given inputs body.
func echoInput(inputs string) string {
	return `name: t
steps:
  - id: a
    echo:
      ` + inputs + "\n"
}

// printfInput returns a workflow whose single printf step has the given inputs,
// plus the args printf requires.
func printfInput(inputs string) string {
	return `name: t
steps:
  - id: a
    printf:
      args: [1]
      ` + inputs + "\n"
}

// An input a task evaluates still has one thing the schema can check.
//
// `Value expect = 9` is deliberately permissive — it has to be, because what an
// expression evaluates to is not knowable here — so a mapping written under
// `expect:` satisfied the descriptor, and every other check in validateTaskInputs
// correctly declines on a deferred input because their shape is the task's
// business. The result was a file `flow validate` called ok and the engine refused
// on its first request, which is the worst answer the tool can give: it moves the
// discovery from the author's terminal to production. One shipped example was in
// exactly that state before it was rewritten.
//
// Whether a value carries a `${...}` fence is not the task's business. It is
// lexical, decided by the parser before the task sees anything, and so is
// checkable with no scope and no type system.

// TestAnInputThatMustBeAnExpressionIsCheckedEvenThoughItIsDeferred is the case
// that was missed.
func TestAnInputThatMustBeAnExpressionIsCheckedEvenThoughItIsDeferred(t *testing.T) {
	t.Parallel()

	for name, src := range map[string]string{
		"a mapping": "name: t\nsteps:\n  - id: f\n    http:\n      url: https://example.com\n" +
			"      expect:\n        status_code: 200\n",
		"a bare string": "name: t\nsteps:\n  - id: f\n    http:\n      url: https://example.com\n" +
			"      expect: status_code == 200\n",
		"a literal boolean": "name: t\nsteps:\n  - id: f\n    http:\n      url: https://example.com\n" +
			"      expect: true\n",
	} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()

			workflow, err := flowfile.Unmarshal([]byte(src))
			require.NoError(t, err, "this parses; the question is whether it validates")

			ds := flowfile.Validate(workflow)
			require.NotEmpty(t, ds, "`expect:` written as %s validated, and the run would fail on its first request", name)

			message := ds.Error()
			assert.Contains(t, message, "has to be written as one")
			assert.Contains(t, message, "${...}", "the diagnostic does not say what to write instead")
		})
	}
}

// TestAnExpressionInputWrittenAsAnExpressionIsAccepted is the direction that keeps
// the check from being a refusal of everything.
func TestAnExpressionInputWrittenAsAnExpressionIsAccepted(t *testing.T) {
	t.Parallel()

	workflow, err := flowfile.Unmarshal([]byte(
		"name: t\nsteps:\n  - id: f\n    http:\n      url: https://example.com\n" +
			"      expect: ${status_code == 200}\n      outputs: \"${ {'code': status_code} }\"\n"))
	require.NoError(t, err)
	assert.Empty(t, flowfile.Validate(workflow),
		"a correctly written expression input was refused")
}

// TestADeferredInputThatNeedNotBeAnExpressionIsLeftAlone keeps the new rule from
// spreading to every deferred input.
//
// The cel task defers `expr`, and `expr` is a plain string the task parses itself
// — writing `${...}` there would be wrong. Only inputs a task names in
// ExpressionInputs are checked, so declaring one stays a decision rather than a
// consequence of deferring it.
func TestADeferredInputThatNeedNotBeAnExpressionIsLeftAlone(t *testing.T) {
	t.Parallel()

	workflow, err := flowfile.Unmarshal([]byte(
		"name: t\nsteps:\n  - id: c\n    cel:\n      expr: 1 + 1\n"))
	require.NoError(t, err)
	assert.Empty(t, flowfile.Validate(workflow),
		"the cel task's `expr` was required to carry a fence, which would be wrong")
}
