package flowfile_test

import (
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"

	v1 "github.com/picatz/flowstate/pkg/flowstate/v1"
	"github.com/picatz/flowstate/pkg/flowstate/v1/flowfile"
)

// TestValidateSource covers the authoring mistakes that previously surfaced only
// at run time, or never surfaced as errors at all.
func TestValidateSource(t *testing.T) {
	tests := []struct {
		name string
		src  string
		// want is a substring the diagnostics must contain; empty means the
		// Flowfile must validate cleanly.
		want string
	}{
		{
			name: "valid workflow",
			src: `
name: valid
steps:
  - id: a
    task:
      name: echo
      inputs:
        message: hello
  - id: b
    task:
      name: echo
      inputs:
        message: ${a.result}
`,
		},
		{
			name: "duplicate step ids",
			src: `
name: dupes
steps:
  - id: a
    task:
      name: echo
      inputs:
        message: one
  - id: a
    task:
      name: echo
      inputs:
        message: two
`,
			want: "duplicate id",
		},
		{
			name: "missing step id",
			src: `
name: no-id
steps:
  - task:
      name: echo
      inputs:
        message: hello
`,
			want: "step has no id",
		},
		{
			name: "unknown task",
			src: `
name: unknown-task
steps:
  - id: a
    task:
      name: shell
      inputs:
        command: ls
`,
			want: `unknown task "shell"`,
		},
		{
			name: "step id is a CEL reserved word",
			src: `
name: reserved
steps:
  - id: loop
    task:
      name: echo
      inputs:
        message: hello
`,
			want: "reserved word",
		},
		{
			name: "step id is not a valid identifier",
			src: `
name: bad-ident
steps:
  - id: my-step
    task:
      name: echo
      inputs:
        message: hello
`,
			want: "not a valid identifier",
		},
		{
			name: "reference to unknown step",
			src: `
name: unknown-ref
steps:
  - id: a
    task:
      name: echo
      inputs:
        message: ${nope.result}
`,
			want: `unknown step "nope"`,
		},
		{
			name: "forward reference",
			src: `
name: forward-ref
steps:
  - id: a
    task:
      name: echo
      inputs:
        message: ${b.result}
  - id: b
    task:
      name: echo
      inputs:
        message: hello
`,
			want: "runs later",
		},
		{
			name: "self reference",
			src: `
name: self-ref
steps:
  - id: a
    task:
      name: echo
      inputs:
        message: ${a.result}
`,
			want: "its own step",
		},
		{
			name: "workflow with no name",
			src: `
steps:
  - id: a
    task:
      name: echo
      inputs:
        message: hello
`,
			want: "no name",
		},
		{
			// The http task evaluates `outputs` itself, against the response, so
			// status_code/body/headers are not step references. Reporting them
			// would flag every correct use of output shaping, and a validator
			// that cries wolf gets ignored.
			name: "task-evaluated inputs are not checked for step references",
			src: `
name: output-shaping
steps:
  - id: web
    task:
      name: http
      inputs:
        method: GET
        url: https://example.com/json
        outputs: "${ {'status': status_code, 'title': body} }"
`,
		},
		{
			// A condition is an expression resolving against the same names as
			// any input, so it must be reference-checked too. It was not, so a
			// condition referencing a later step validated cleanly and failed at
			// run time.
			name: "condition referencing a later step",
			src: `
name: forward-condition
steps:
  - id: a
    if: ${later.result == 'x'}
    task:
      name: echo
      inputs:
        message: hi
  - id: later
    task:
      name: echo
      inputs:
        message: hi
`,
			want: "runs later",
		},
		{
			name: "condition referencing an unknown step",
			src: `
name: unknown-condition
steps:
  - id: a
    if: ${nope.result}
    task:
      name: echo
      inputs:
        message: hi
`,
			want: `unknown step "nope"`,
		},
		{
			name: "condition inside a loop body may use the iterator",
			src: `
name: loop-condition
steps:
  - id: each
    for_each:
      items: "${['a', 'b']}"
      steps:
        - id: act
          if: ${item == 'a'}
          task:
            name: echo
            inputs:
              message: ${item}
`,
		},
		{
			name: "loop iterator colliding with a step id",
			src: `
name: collide
steps:
  - id: item
    task:
      name: echo
      inputs:
        message: hi
  - id: each
    for_each:
      items: "${['a']}"
      steps:
        - id: act
          task:
            name: echo
            inputs:
              message: ${item}
`,
			want: "also a step id",
		},
		{
			name: "parallel branch referencing a sibling branch",
			src: `
name: cross-branch
steps:
  - id: fan
    parallel:
      - steps:
          - id: left
            task:
              name: echo
              inputs:
                message: L
      - steps:
          - id: right
            task:
              name: echo
              inputs:
                message: ${left.result}
`,
			want: `unknown step "left"`,
		},
		{
			name: "step after a parallel block may reference branch outputs",
			src: `
name: join
steps:
  - id: fan
    parallel:
      - steps:
          - id: left
            task:
              name: echo
              inputs:
                message: L
      - steps:
          - id: right
            task:
              name: echo
              inputs:
                message: R
  - id: join
    task:
      name: printf
      inputs:
        format: "%s%s"
        args:
          - ${left.result}
          - ${right.result}
`,
		},
		{
			// A loop's body outputs are reported through its own results output,
			// so referencing a body step from outside cannot resolve.
			name: "step after a loop may not reference body steps",
			src: `
name: loop-leak
steps:
  - id: each
    for_each:
      items: "${['a']}"
      steps:
        - id: inner
          task:
            name: echo
            inputs:
              message: hi
  - id: after
    task:
      name: echo
      inputs:
        message: ${inner.result}
`,
			want: `unknown step "inner"`,
		},
		{
			name: "comprehension variables are not step references",
			src: `
name: comprehension
steps:
  - id: a
    task:
      name: cel
      inputs:
        expr: "[1, 2, 3].map(x, x * 2)"
  - id: b
    task:
      name: cel
      inputs:
        expr: "size(a.result)"
`,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ds, err := flowfile.ValidateSource([]byte(tt.src))
			if err != nil {
				t.Fatalf("ValidateSource() parse error: %v", err)
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
			t.Logf("reported:\n%s", got)
		})
	}
}

// TestValidateSourceReportsLineNumbers verifies diagnostics carry a source
// position, so an editor can place them and a human can find them.
func TestValidateSourceReportsLineNumbers(t *testing.T) {
	src := `name: positions
steps:
  - id: first
    task:
      name: echo
      inputs:
        message: hello
  - id: second
    task:
      name: nosuchtask
      inputs:
        message: hello
`
	ds, err := flowfile.ValidateSource([]byte(src))
	if err != nil {
		t.Fatalf("ValidateSource() error: %v", err)
	}
	if len(ds) != 1 {
		t.Fatalf("expected exactly one diagnostic, got %d:\n%s", len(ds), ds.Error())
	}
	// "- id: second" is on line 8.
	if ds[0].Line != 8 {
		t.Errorf("diagnostic line = %d, want 8 (the line declaring the offending step)", ds[0].Line)
	}
	if !strings.HasPrefix(ds[0].Error(), "8:") {
		t.Errorf("rendered diagnostic should start with the line number; got %q", ds[0].Error())
	}
}

// TestExprRules pins how a scalar is classified as an expression.
//
// The rule is that a fenced scalar's contents are validated by CEL, not by pattern
// matching. Before that, a regex decided it: `${a} and ${b}` silently compiled the
// corrupted middle, and `${...} trailing` and `${unterminated` were silently
// accepted as literal text.
func TestExprRules(t *testing.T) {
	tests := []struct {
		name    string
		in      string
		expr    string
		isExpr  bool
		wantErr string
	}{
		{name: "plain literal", in: "plain text"},
		{name: "whole-value expression", in: "${a.result}", expr: "a.result", isExpr: true},
		{
			// Braces inside the expression work because CEL decides where the
			// expression ends, not brace counting here.
			name: "expression containing braces", in: "${ {'k': 1} }", expr: " {'k': 1} ", isExpr: true,
		},
		{name: "expression with concatenation", in: "${'a' + b}", expr: "'a' + b", isExpr: true},
		{
			name: "two expressions in one value", in: "${a} and ${b}",
			wantErr: "invalid expression",
		},
		{
			name: "expression with trailing text", in: "${a.result} trailing",
			wantErr: "must be the whole value",
		},
		{
			name: "expression with leading text", in: "hello ${name}",
			wantErr: "must be the whole value",
		},
		{
			name: "unterminated expression", in: "${oops",
			wantErr: "unterminated expression",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			expr, isExpr := flowfile.ExprSource(tt.in)
			err := flowfile.ExprError(tt.in)

			if tt.wantErr != "" {
				if err == nil {
					t.Fatalf("ExprError(%q) = nil, want an error mentioning %q", tt.in, tt.wantErr)
				}
				if !strings.Contains(err.Error(), tt.wantErr) {
					t.Errorf("ExprError(%q) = %v, want it to mention %q", tt.in, err, tt.wantErr)
				}
				if isExpr {
					t.Errorf("ExprSource(%q) reported an expression, but it is malformed", tt.in)
				}
				return
			}

			if err != nil {
				t.Fatalf("ExprError(%q) = %v, want nil", tt.in, err)
			}
			if isExpr != tt.isExpr {
				t.Errorf("ExprSource(%q) isExpr = %v, want %v", tt.in, isExpr, tt.isExpr)
			}
			if expr != tt.expr {
				t.Errorf("ExprSource(%q) = %q, want %q", tt.in, expr, tt.expr)
			}
		})
	}
}

// TestExprErrorsSurfaceFromCompilation verifies a malformed expression fails
// compilation rather than becoming literal text.
func TestExprErrorsSurfaceFromCompilation(t *testing.T) {
	src := `
name: bad-expr
steps:
  - id: a
    task:
      name: echo
      inputs:
        message: hello ${name}
`
	if _, err := flowfile.Unmarshal([]byte(src)); err == nil {
		t.Fatal("expected compilation to reject an interpolated string, got no error")
	} else {
		t.Logf("reported: %v", err)
	}
}

// TestEveryExampleSurvivesTheSchema walks the examples through the check the
// server now makes, which is not the check `flow validate` was making.
//
// `flowfile.Validate` reports what an author can fix by reading their file:
// unknown tasks, bad references, duplicate ids. `v1.Validate` enforces what the
// schema declares: patterns, lengths, ceilings. Nothing ran the second one over
// the examples, and nothing on the submit path ran it at all — so eight of the
// fifteen shipped examples had names the schema refuses, compiled cleanly, said
// "ok", and would have been rejected the first time anyone ran them against a
// server.
//
// This is the join CLAUDE.md warns about: two validators, each tested, and the
// defect living in the gap between them. So this asserts the composition rather
// than either half.
func TestEveryExampleSurvivesTheSchema(t *testing.T) {
	t.Parallel()

	paths, err := filepath.Glob(filepath.Join("..", "..", "..", "..", "examples", "*", "workflow.yaml"))
	require.NoError(t, err)
	require.NotEmpty(t, paths, "no examples were found, so this test proves nothing")

	for _, path := range paths {
		t.Run(filepath.Base(filepath.Dir(path)), func(t *testing.T) {
			t.Parallel()

			source, err := os.ReadFile(path)
			require.NoError(t, err)

			workflow, _, err := flowfile.Parse(source)
			require.NoError(t, err, "the example does not compile")

			require.NoError(t, v1.Validate(workflow),
				"the example compiles but the schema refuses it, so `flow run` would fail on a file "+
					"this repo ships as a worked example")
		})
	}
}

// TestAnIllegalWorkflowNameIsReportedBeforeItIsSubmitted is the other half.
//
// The schema's refusal names a protobuf field path, which is true and useless to
// somebody looking at a line of YAML. The diagnostic has a position and offers a
// name to paste, because "may not contain spaces" is a rule and `http-expect` is
// an answer.
func TestAnIllegalWorkflowNameIsReportedBeforeItIsSubmitted(t *testing.T) {
	t.Parallel()

	const src = `name: my workflow
steps:
  - id: a
    task:
      name: echo
      inputs:
        message: hi
`

	diagnostics, err := flowfile.ValidateSource([]byte(src))
	require.NoError(t, err)
	require.NotEmpty(t, diagnostics, "a name the schema refuses was accepted by flow validate")

	reported := diagnostics.Error()
	require.Contains(t, reported, "spaces", "the diagnostic does not say what is wrong: %s", reported)
	require.Contains(t, reported, "my-workflow", "the diagnostic does not offer a name that works: %s", reported)
}

// TestUnknownCELLibraryIsReportedAtValidateTime covers a misspelling that used to
// survive every check and fail during a run.
//
// The library names are a closed set the registry knows, so there was never a
// reason for `stirngs` to be a run-time answer — the workflow started, the step was
// scheduled, an activity ran, and only then did anyone learn. CLAUDE.md's rule is
// that a misspelled key must be reported, because silently doing nothing gives the
// author no reason to doubt the file.
func TestUnknownCELLibraryIsReportedAtValidateTime(t *testing.T) {
	t.Parallel()

	const src = `name: bad-library
steps:
  - id: shout
    task:
      name: cel
      inputs:
        expr: "'hi'.upperAscii()"
        libs: [stirngs]
`

	diagnostics, err := flowfile.ValidateSource([]byte(src))
	require.NoError(t, err)
	require.NotEmpty(t, diagnostics, "a misspelled CEL library was accepted")

	reported := diagnostics.Error()
	require.Contains(t, reported, "stirngs", "the diagnostic does not name what was misspelled: %s", reported)
	require.Contains(t, reported, "strings", "the diagnostic does not list what is available: %s", reported)
}

// TestAComputedLibraryListIsNotReported is the other side, and the one that keeps
// the check honest.
//
// A `libs:` produced by an expression is resolved at run time against a scope this
// validator cannot see. Reporting it would be a false diagnostic, and this package
// holds those to be worse than missing ones — they train authors to ignore the
// tool.
func TestAComputedLibraryListIsNotReported(t *testing.T) {
	t.Parallel()

	const src = `name: computed-libraries
steps:
  - id: pick
    task:
      name: echo
      inputs:
        message: strings
  - id: shout
    task:
      name: cel
      inputs:
        expr: "'hi'.upperAscii()"
        libs: ${[pick.result]}
`

	diagnostics, err := flowfile.ValidateSource([]byte(src))
	require.NoError(t, err)

	for _, d := range diagnostics {
		require.NotContains(t, d.Message, "extension library",
			"a library list this validator cannot see was reported anyway: %s", d.Message)
	}
}

// TestTheEvaluatorRefusesAnUnknownLibraryBeforeItCaches is the resource bound
// underneath the diagnostic.
//
// The evaluator caches an environment per library set, keyed on names a workflow
// supplies, and cached failures too — so every distinct unknown name became a
// permanent entry in a process-wide map with no eviction. A loop with
// `continue_on_error` carries on past the fail-closed error, so one run could add
// an entry per iteration.
//
// The name is refused before it can become a key, which bounds the key space at
// the subsets of the known libraries by construction rather than by hoping nobody
// asks twice.
func TestTheEvaluatorRefusesAnUnknownLibraryBeforeItCaches(t *testing.T) {
	t.Parallel()

	_, err := v1.DefaultEvaluator().Env("definitely-not-a-library")
	require.Error(t, err, "an unknown library was accepted, and is now cached forever")
	require.Contains(t, err.Error(), "definitely-not-a-library")
	require.Contains(t, err.Error(), "strings", "the refusal does not say what is available")
}
