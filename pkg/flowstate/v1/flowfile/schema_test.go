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
			src:  logInput("mesage: hello"),
			want: `task "log" has no such input; did you mean "message"?`,
		},
		{
			name: "input no task declares, with nothing close",
			src:  logInput("shout: hello"),
			want: `task "log" has no such input; it accepts message`,
		},
		{
			name: "required input left out",
			src:  logInput("{}"),
			want: `task "log" requires input "message"`,
		},
		{
			// One mistake, one diagnostic: a typo is not also reported as the
			// required input it left unset.
			name: "a typo is not also a missing input",
			src:  logInput("mesage: hello"),
			want: `did you mean "message"?`,
		},
		{
			name: "a list where a string belongs",
			src: logInput(`message:
          - one
          - two`),
			want: "expected a string, but this is a list",
		},
		{
			name: "a number where a string belongs",
			src:  logInput(`message: 42`),
			want: "expected a string, but this is a whole number",
		},
		{
			name: "a string where a mapping belongs",
			src: `edition: v2026.3
name: t
steps:
  - id: a
    http:
      url: https://example.com
      headers: nope
`,
			want: "expected a mapping, but this is a string",
		},
		{
			name: "http without a url",
			src: `edition: v2026.3
name: t
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
			src: `edition: v2026.3
name: t
vars:
  count: one
steps:
  - id: a
    log:
      message: ${[vars.count]}
`,
		},
		{
			name: "a whole number satisfies a floating-point field",
			src: `edition: v2026.3
name: t
steps:
  - id: a
    retry:
      backoff: 2
    log:
      message: hi
`,
		},
		{
			name: "an input the task evaluates itself is not checked",
			src: `edition: v2026.3
name: t
steps:
  - id: a
    http:
      url: https://example.com
      outputs: "${ {'status': status_code} }"
`,
		},
		{
			name: "a literal mapping for a declared mapping",
			src: `edition: v2026.3
name: t
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
			src: `edition: v2026.3
name: t
steps:
  - id: a
    nosuchtask:
      whatever: 1
`,
			want: `unknown task "nosuchtask"`,
		},

		// #158: a task input written as a direct reference to a name this file types
		// gets the same schema check a literal gets. The type is a property of the
		// file — the var's literal, the input's declaration — so a value the field can
		// never hold is reported before the run rather than at it.
		{
			name: "a var literal of the wrong type routed through a reference",
			src: `edition: v2026.3
name: t
vars:
  flag: "yes"
steps:
  - id: a
    http:
      url: https://example.com
      parse_json: ${vars.flag}
`,
			want: "expected true or false, but this is a string (from ${vars.flag})",
		},
		{
			name: "a declared input of the wrong type routed through a reference",
			src: `edition: v2026.3
name: t
inputs:
  count:
    type: int
steps:
  - id: a
    log:
      message: ${inputs.count}
`,
			want: "expected a string, but this is a whole number (from ${inputs.count})",
		},

		// The negative direction, which matters more: a reference whose type the file
		// does *not* fix must not be refused, or the check trades a missing diagnostic
		// for a false one.
		{
			name: "a var literal of the right type is not refused",
			src: `edition: v2026.3
name: t
vars:
  note: hello
steps:
  - id: a
    log:
      message: ${vars.note}
`,
		},
		{
			name: "a declared input of the right type is not refused",
			src: `edition: v2026.3
name: t
inputs:
  note:
    type: string
steps:
  - id: a
    log:
      message: ${inputs.note}
`,
		},
		{
			// The false-diagnostic guard: a computed expression has a type no part of
			// the file fixes — `string(inputs.count)` is a string however `count` is
			// declared — so it is left to the run rather than judged here.
			name: "a computed expression over a typed input is not refused",
			src: `edition: v2026.3
name: t
inputs:
  count:
    type: int
steps:
  - id: a
    log:
      message: ${string(inputs.count)}
`,
		},
		{
			// A var whose value is itself an expression has no type knowable here, so
			// a reference to it stays unchecked even into a typed field.
			name: "a reference to an expression-valued var is not refused",
			src: `edition: v2026.3
name: t
vars:
  computed: ${1 + 1}
steps:
  - id: a
    http:
      url: https://example.com
      parse_json: ${vars.computed}
`,
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
	src := `edition: v2026.3
name: t
steps:
  - id: loop
    for_each:
      items: ${[1]}
      steps:
        - id: body
          log:
            mesage: hi
  - id: fan
    parallel:
      - steps:
          - id: branch
            log:
`
	ds, err := flowfile.ValidateSource([]byte(src))
	if err != nil {
		t.Fatalf("ValidateSource() error: %v", err)
	}

	got := ds.Error()
	for _, want := range []string{
		`step "body" input "mesage"`,
		`step "branch": task "log" requires input "message"`,
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
	src := `edition: v2026.3
name: t
steps:
  - id: a
    log:
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
	if ds[0].Line != 6 || ds[0].Column != 15 {
		t.Errorf("position = %d:%d, want 6:15\nreported: %s", ds[0].Line, ds[0].Column, ds[0].Error())
	}
}

// TestExpressionInputTypeMismatchIsPositionedAndNamesBothTypes pins the #158
// diagnostic exactly: one positioned message that names the type the field expects and
// the type the reference resolves to, so an author reads what is wrong and where
// without running the workflow.
func TestExpressionInputTypeMismatchIsPositionedAndNamesBothTypes(t *testing.T) {
	t.Parallel()

	src := `edition: v2026.3
name: t
vars:
  flag: "yes"
steps:
  - id: a
    http:
      url: https://example.com
      parse_json: ${vars.flag}
`
	ds, err := flowfile.ValidateSource([]byte(src))
	if err != nil {
		t.Fatalf("ValidateSource() error: %v", err)
	}

	var found *flowfile.Diagnostic
	for i := range ds {
		if ds[i].Code == "type-mismatch" {
			found = &ds[i]
			break
		}
	}
	if found == nil {
		t.Fatalf("no type_mismatch diagnostic; got:\n%s", ds.Error())
	}

	// Names the expected type (bool → "true or false") and the actual (string), and
	// the reference it came from — byte for byte.
	const want = `expected true or false, but this is a string (from ${vars.flag})`
	if got := found.Message; got != want {
		t.Errorf("message =\n  %q\nwant\n  %q", got, want)
	}
	if found.Step != "a" || found.Field != "parse_json" {
		t.Errorf("diagnostic names step %q input %q, want step \"a\" input \"parse_json\"", found.Step, found.Field)
	}

	// Positioned at the input's value, on the `parse_json:` line.
	if found.Line != 9 {
		t.Errorf("line = %d, want 9 (reported: %s)", found.Line, found.Error())
	}
}

// logInput returns a workflow whose single log step has the given inputs body.
func logInput(inputs string) string {
	return `edition: v2026.3
name: t
steps:
  - id: a
    log:
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
		"a mapping": "edition: v2026.3\nname: t\nsteps:\n  - id: f\n    http:\n      url: https://example.com\n" +
			"      expect:\n        status_code: 200\n",
		"a bare string": "edition: v2026.3\nname: t\nsteps:\n  - id: f\n    http:\n      url: https://example.com\n" +
			"      expect: status_code == 200\n",
		"a literal boolean": "edition: v2026.3\nname: t\nsteps:\n  - id: f\n    http:\n      url: https://example.com\n" +
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
		"edition: v2026.3\nname: t\nsteps:\n  - id: f\n    http:\n      url: https://example.com\n" +
			"      expect: ${status_code == 200}\n      outputs: \"${ {'code': status_code} }\"\n"))
	require.NoError(t, err)
	assert.Empty(t, flowfile.Validate(workflow),
		"a correctly written expression input was refused")
}

// TestALiteralOutputsMapIsStillAccepted pins the input this rule must not reach.
//
// `outputs` is deferred like `expect` and was declared an expression input
// alongside it, which read as symmetric and is not: `httpExpectSatisfied` refuses
// a literal `expect`, while `taskFuncHTTP` converts a literal `outputs` through
// literalToValueMap and returns those names. Declaring it made `flow validate`
// refuse a workflow the engine runs — this rule's own failure mode pointed the
// other way, and worse, because it breaks files that work today.
//
// Caught in review. Confirmed by calling the task with a literal map and watching
// it return `note: "constant"` before the declaration was narrowed.
//
// It carries the general claim too, now that the cel task is retired: only inputs a
// task names in ExpressionInputs are checked for a fence, so declaring one stays a
// decision rather than a consequence of deferring it. `outputs` is the deferred
// input that is not one.
func TestALiteralOutputsMapIsStillAccepted(t *testing.T) {
	t.Parallel()

	workflow, err := flowfile.Unmarshal([]byte(
		"edition: v2026.3\nname: t\nsteps:\n  - id: f\n    http:\n      url: https://example.com\n" +
			"      outputs:\n        note: constant\n"))
	require.NoError(t, err)
	assert.Empty(t, flowfile.Validate(workflow),
		"a literal outputs map was refused, and the engine accepts it")
}
