package flowfile_test

import (
	"fmt"
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
    echo:
      message: hello
  - id: b
    echo:
      message: ${a.result}
`,
		},
		{
			name: "duplicate step ids",
			src: `
name: dupes
steps:
  - id: a
    echo:
      message: one
  - id: a
    echo:
      message: two
`,
			want: "duplicate id",
		},
		{
			// Written in the flattened grammar like every other fixture here, which
			// it has to be to still test what it says: `task:` is no longer a step
			// property, so the old spelling reported an unknown *task* named "task"
			// as well, and this case would have passed on a diagnostic about the
			// wrong thing.
			name: "missing step id",
			src: `
name: no-id
steps:
  - echo:
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
    shell:
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
    echo:
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
    echo:
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
    echo:
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
    echo:
      message: ${b.result}
  - id: b
    echo:
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
    echo:
      message: ${a.result}
`,
			want: "its own step",
		},
		{
			name: "workflow with no name",
			src: `
steps:
  - id: a
    echo:
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
    http:
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
    echo:
      message: hi
  - id: later
    echo:
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
    echo:
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
          echo:
            message: ${item}
`,
		},
		{
			name: "loop iterator colliding with a step id",
			src: `
name: collide
steps:
  - id: item
    echo:
      message: hi
  - id: each
    for_each:
      items: "${['a']}"
      steps:
        - id: act
          echo:
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
            echo:
              message: L
      - steps:
          - id: right
            echo:
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
            echo:
              message: L
      - steps:
          - id: right
            echo:
              message: R
  - id: join
    printf:
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
          echo:
            message: hi
  - id: after
    echo:
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
    cel:
      expr: "[1, 2, 3].map(x, x * 2)"
  - id: b
    cel:
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
    echo:
      message: hello
  - id: second
    nosuchtask:
      message: hello
`
	ds, err := flowfile.ValidateSource([]byte(src))
	if err != nil {
		t.Fatalf("ValidateSource() error: %v", err)
	}
	if len(ds) != 1 {
		t.Fatalf("expected exactly one diagnostic, got %d:\n%s", len(ds), ds.Error())
	}
	// "nosuchtask:" is on line 7, and the diagnostic names it rather than the step
	// it sits in — which is the improvement the flattening buys and the reason
	// this asserts 7 rather than 6.
	//
	// A task's name used to be a value inside a `task:` block, so the best a
	// diagnostic could do was name the step and leave the reader to find which of
	// its lines was meant. The name is now a key the author wrote, with a span of
	// its own, so the position is the token they have to change. Line 6
	// ("- id: second") is still findable and is still the wrong answer: nothing is
	// wrong with the id.
	if ds[0].Line != 7 {
		t.Errorf("diagnostic line = %d, want 7 (the line naming the unknown task)", ds[0].Line)
	}
	if !strings.HasPrefix(ds[0].Error(), "7:") {
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
    echo:
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
    echo:
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
    cel:
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
    echo:
      message: strings
  - id: shout
    cel:
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

// TestAWorkflowTooLargeToRunIsReportedAtValidateTime moves a failure from the
// substrate to the desk.
//
// Size is not a schema rule and cannot be one: the schema says what a workflow is,
// and this is about what Temporal will store for a run. An author who learns it at
// submit learns it from the wrong place — and what they learned before this check
// existed was "Blob data size exceeds limit", which is true and useless.
//
// The source bound does not cover this, which is the whole reason the check is
// separate. Source and specification are bounded at the same 1 MiB, but a
// specification is not the same size as the file it came from: an expression is a
// few bytes of text and a parsed syntax tree once compiled. Measured at 5.4x for
// a file of ordinary expressions — so a 200 KiB Flowfile, comfortably inside every
// limit it can see, compiles to something no run can carry.
func TestAWorkflowTooLargeToRunIsReportedAtValidateTime(t *testing.T) {
	t.Parallel()

	// Ninety-nine steps, inside the schema's hundred-step ceiling, each holding
	// one long expression. Nothing here is malformed and nothing is even unusual;
	// it is a generated workflow of the kind a catalog or a fan-out produces.
	expression := "first.result" + strings.Repeat(" + first.result", 180)

	var src strings.Builder
	src.WriteString("name: expands\nsteps:\n  - id: first\n    echo:\n      message: hello\n")
	for i := range 99 {
		fmt.Fprintf(&src, "  - id: s%d\n    echo:\n      message: ${%s}\n", i, expression)
	}

	require.Less(t, src.Len(), 1<<20,
		"the fixture is caught by the source bound, so it never reaches the check it is about")

	diagnostics, err := flowfile.ValidateSource([]byte(src.String()))
	require.NoError(t, err, "the fixture no longer parses, so it tests nothing")
	require.NotEmpty(t, diagnostics, "a workflow too large to run was accepted")

	reported := diagnostics.Error()
	require.Contains(t, reported, "step outputs",
		"the diagnostic does not explain that a run carries more than the workflow: %s", reported)
}
