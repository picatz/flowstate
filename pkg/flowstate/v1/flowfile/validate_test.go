package flowfile_test

import (
	"fmt"
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
edition: v2026.3
name: valid
steps:
  - id: a
    http:
      url: https://example.com
  - id: b
    log:
      message: ${steps.a.body}
`,
		},
		{
			name: "duplicate step ids",
			src: `
edition: v2026.3
name: dupes
steps:
  - id: a
    log:
      message: one
  - id: a
    log:
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
edition: v2026.3
name: no-id
steps:
  - log:
      message: hello
`,
			want: "step has no id",
		},
		{
			name: "unknown task",
			src: `
edition: v2026.3
name: unknown-task
steps:
  - id: a
    shell:
      command: ls
`,
			want: `unknown task "shell"`,
		},
		{
			// The refusal that survived rooting, and the only ground it survived
			// on. `true` is a token the lexer takes before any identifier rule
			// applies, so `steps.true` is a syntax error in CEL's grammar rather
			// than a name CEL declines to resolve: there is no spelling of a
			// reference to a step called `true` that parses at all. The same is
			// so of `false` and `null`, and of `in`, which the grammar reads as
			// an operator.
			name: "step id is a CEL literal",
			src: `
edition: v2026.3
name: literal-id
steps:
  - id: "true"
    log:
      message: hello
`,
			want: "is punctuation in CEL",
		},
		{
			// The other side of the same narrowing, and the reason it is worth a
			// case of its own: cel-go refuses a reserved word in *identifier*
			// position and nowhere else, and `steps.<id>` is a field select. So
			// `loop` — refused as a step id for as long as a step was named bare —
			// is now a name a step may have and a later step may read, and this
			// asserts the reading rather than only the naming.
			//
			// Seventeen of the twenty-one words moved this way. The list is still
			// carried for `for_each` iterators, which are written bare and are
			// still identifiers.
			name: "step id is a word CEL reserves only in identifier position",
			src: `
edition: v2026.3
name: reserved-but-selectable
steps:
  - id: loop
    http:
      url: https://example.com
  - id: after
    log:
      message: ${steps.loop.body}
`,
		},
		{
			// A step called `now` used to be refused, because a wait binds `now`
			// to the moment it is evaluated and a bound name wins — so the step
			// would have meant one thing everywhere and something else inside
			// `wait_until:`. Rooting removes the overlap rather than the binding
			// order: the clock is bare and the step is `steps.now`, so both
			// spellings appear here and neither can be read as the other.
			name: "a step may be called now",
			src: `
edition: v2026.3
name: now-and-the-clock
steps:
  - id: now
    http:
      url: https://example.com
  - id: hold
    wait_until: ${now + days(1)}
  - id: after
    log:
      message: ${steps.now.body}
`,
		},
		{
			name: "step id is not a valid identifier",
			src: `
edition: v2026.3
name: bad-ident
steps:
  - id: my-step
    log:
      message: hello
`,
			want: "not a valid identifier",
		},
		{
			name: "reference to unknown step",
			src: `
edition: v2026.3
name: unknown-ref
steps:
  - id: a
    log:
      message: ${steps.nope.result}
`,
			want: `references unknown step "nope"`,
		},
		// The retired spelling and a plain mistake are written identically, so
		// the only thing that can tell them apart is the workflow around them —
		// and they want different answers. These two fixtures differ in one
		// character of one id, which is exactly the difference the validator has
		// to notice.
		{
			name: "a bare reference to a step is the retired spelling",
			src: `
edition: v2026.3
name: retired-spelling
steps:
  - id: a
    http:
      url: https://example.com
  - id: b
    log:
      message: ${a.body}
`,
			want: "flow fix",
		},
		{
			name: "a bare reference to something that is not a step is still a mistake",
			src: `
edition: v2026.3
name: not-a-step
steps:
  - id: a
    log:
      message: hello
  - id: b
    log:
      message: ${nope.result}
`,
			want: `references unknown name "nope"`,
		},
		{
			name: "forward reference",
			src: `
edition: v2026.3
name: forward-ref
steps:
  - id: a
    log:
      message: ${steps.b.body}
  - id: b
    http:
      url: https://example.com
`,
			want: "runs later",
		},
		{
			name: "self reference",
			src: `
edition: v2026.3
name: self-ref
steps:
  - id: a
    http:
      url: ${steps.a.body}
`,
			want: "its own step",
		},
		{
			name: "workflow with no name",
			src: `
edition: v2026.3
steps:
  - id: a
    log:
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
edition: v2026.3
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
edition: v2026.3
name: forward-condition
steps:
  - id: a
    if: ${steps.later.status_code == 200}
    log:
      message: hi
  - id: later
    http:
      url: https://example.com
`,
			want: "runs later",
		},
		{
			name: "condition referencing an unknown step",
			src: `
edition: v2026.3
name: unknown-condition
steps:
  - id: a
    if: ${steps.nope.result}
    log:
      message: hi
`,
			want: `references unknown step "nope"`,
		},
		{
			name: "condition inside a loop body may use the iterator",
			src: `
edition: v2026.3
name: loop-condition
steps:
  - id: each
    for_each:
      items: "${['a', 'b']}"
      steps:
        - id: act
          if: ${item == 'a'}
          log:
            message: ${item}
`,
		},
		{
			// This case has changed sides, and it is the one rooting was done for.
			//
			// An iterator sharing a step's id used to be refused, because both
			// resolved from one namespace and a bare `${item}` inside the body
			// could only mean whichever the engine bound last. There is no longer
			// anything to forbid: the binding is bare and the step is
			// `steps.item`, so this asserts not merely that the two may coexist
			// but that one expression can name both and be understood.
			name: "a loop iterator may share a step's id",
			src: `
edition: v2026.3
name: iterator-shares-an-id
steps:
  - id: item
    http:
      url: https://example.com
  - id: each
    for_each:
      items: "${['a']}"
      steps:
        - id: act
          log:
            message: ${'%s from %s'.format([item, steps.item.body])}
`,
		},
		{
			// The negative direction of the case above, and the half that keeps
			// it from being a functionality test wearing a security test's
			// clothes: the two namespaces are only separate if the root cannot
			// reach into the local one. No step is called `item` here, so
			// `steps.item` has to be unresolved rather than quietly finding the
			// loop's binding.
			name: "the steps root does not reach a loop binding",
			src: `
edition: v2026.3
name: root-misses-the-binding
steps:
  - id: each
    for_each:
      items: "${['a']}"
      steps:
        - id: act
          log:
            message: ${steps.item.result}
`,
			want: `unknown step "item"`,
		},
		{
			name: "parallel branch referencing a sibling branch",
			src: `
edition: v2026.3
name: cross-branch
steps:
  - id: fan
    parallel:
      - steps:
          - id: left
            log:
              message: L
      - steps:
          - id: right
            log:
              message: ${steps.left.result}
`,
			want: `unknown step "left"`,
		},
		{
			name: "step after a parallel block may reference branch outputs",
			src: `
edition: v2026.3
name: join
steps:
  - id: fan
    parallel:
      - steps:
          - id: left
            http:
              url: https://example.com/left
      - steps:
          - id: right
            http:
              url: https://example.com/right
  - id: join
    log:
      message: ${'%s%s'.format([steps.left.body, steps.right.body])}
`,
		},
		{
			// A loop's body outputs are reported through its own results output,
			// so referencing a body step from outside cannot resolve.
			//
			// It has to be written rooted to still say that. `inner` is a step
			// somewhere in this file, so the bare spelling is answered by the
			// migration diagnostic before scope is ever consulted — which would
			// leave the case green and testing the rewriter instead of the rule.
			name: "step after a loop may not reference body steps",
			src: `
edition: v2026.3
name: loop-leak
steps:
  - id: each
    for_each:
      items: "${['a']}"
      steps:
        - id: inner
          log:
            message: hi
  - id: after
    log:
      message: ${steps.inner.result}
`,
			want: `unknown step "inner"`,
		},
		{
			// A comprehension's variable is introduced by the expression itself,
			// so it is neither a step nor an unresolved name, and reporting it
			// would make every comprehension look broken.
			//
			// Written as a fenced `${...}` because that is the only spelling the
			// reference checker sees — an expression a task compiled for itself
			// would reach nothing here and assert nothing, which is what this
			// fixture used to do.
			name: "a comprehension binds its own variable",
			src: `
edition: v2026.3
name: comprehension
steps:
  - id: a
    http:
      url: https://example.com
  - id: b
    log:
      message: ${[steps.a.body].map(x, x + '!')[0]}
`,
		},
		{
			// The root is a name like any other, so a comprehension may bind it —
			// and then `steps.title` is a field of the item being iterated and
			// not a step at all. Reading it as one would report `title` as an
			// unknown step in an expression that is entirely correct.
			name: "a comprehension may bind the steps root",
			src: `
edition: v2026.3
name: shadowed-root
steps:
  - id: a
    log:
      message: "${[{'title': 'x'}].map(steps, steps.title)[0]}"
`,
		},
		{
			// #215's second finding: `run`'s shape is statically known —
			// unlike `steps` and `inputs`, no file can add a field to it — so a
			// typo below `run.` is diagnosable here rather than failing only at
			// run time, three steps into a run nobody can act on.
			name: "a typo'd field of run.identity is refused",
			src: `
edition: v2026.3
name: run-identity-typo
steps:
  - id: a
    log:
      message: ${run.identitty.subject}
`,
			want: `references unknown field "identitty" of ` + "`run`" + `; did you mean "identity"?`,
		},
		{
			name: "a typo'd field directly under run is refused",
			src: `
edition: v2026.3
name: run-field-typo
steps:
  - id: a
    log:
      message: ${run.locl}
`,
			want: `references unknown field "locl" of ` + "`run`" + `; did you mean "local"?`,
		},
		{
			name: "every legal field of run validates cleanly",
			src: `
edition: v2026.3
name: run-legal-fields
steps:
  - id: a
    log:
      message: >-
        ${run.identity.subject + run.identity.issuer + run.identity.namespace +
          run.workflow_id + run.run_id + (run.local ? "local" : "not local")}
`,
		},
		{
			// The gap #272 measured: a run could not learn its own address, so a
			// workload had no way to tell an external system where to send a
			// callback. Both halves are ordinary fields of the root now, and the
			// closed set below is what keeps the diagnostic above honest.
			name: "a run may read its own address",
			src: `
edition: v2026.3
name: run-address
steps:
  - id: a
    log:
      message: ${run.workflow_id + "/" + run.run_id}
`,
		},
		{
			// The two fields deliberately absent, checked as a refusal rather
			// than left to a comment: a start time is a clock read by another
			// name, and `now` is bound only inside a wait precisely so a task
			// cannot read a clock. If either is ever added, this case is the
			// thing that has to be deleted on purpose.
			name: "run has no clock and no attempt count",
			src: `
edition: v2026.3
name: run-start-time
steps:
  - id: a
    log:
      message: ${string(run.start_time)}
`,
			want: `references unknown field "start_time" of ` + "`run`" + `; ` +
				"`run` has identity, local, workflow_id, run_id",
		},
		{
			// claims is a map keyed by whatever the identity provider issued, so
			// an arbitrary key must stay legal — reporting into a map's dynamic
			// keys would be exactly the false diagnostic this package's own
			// standard refuses to draw.
			name: "an arbitrary claims key, indexed, validates cleanly",
			src: `
edition: v2026.3
name: run-claims-index
steps:
  - id: a
    log:
      message: ${run.identity.claims["team"]}
`,
		},
		{
			name: "an arbitrary claims key, dotted, validates cleanly",
			src: `
edition: v2026.3
name: run-claims-dot
steps:
  - id: a
    log:
      message: ${run.identity.claims.team}
`,
		},
		{
			// A regression guard for the change that taught rootedName to
			// recognise `run`: before, `${run.identity.subject}` in a var
			// reached this refusal through the generic bare-name fallback,
			// which the added root recognition bypasses unless the vars walk
			// is taught about run refs too.
			name: "a workflow var may not read run, even through a legal field",
			src: `
edition: v2026.3
name: run-in-vars
vars:
  starter: ${run.identity.subject}
steps:
  - id: a
    log:
      message: ${vars.starter}
`,
			want: "a var may not read `run`",
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

// TestBareNameDoesNotFabricateStepSuggestion covers the self-defeating diagnostic
// that spliced the failed name back in after `steps.`: `${step.a.result}` — a typo
// for `steps.a.result` — was told to write `steps.step`, a spelling that resolves to
// nothing and was never a real suggestion. A bare name that matches no declared step
// must state the step-output form in the general (`steps.<id>.<output>`) rather than
// as `steps.<thatname>`, and offer a concrete `steps.<id>` only for a genuine
// near-miss.
func TestBareNameDoesNotFabricateStepSuggestion(t *testing.T) {
	t.Parallel()

	t.Run("a typo'd root does not become a fabricated step suggestion", func(t *testing.T) {
		t.Parallel()
		// `step.a.result` is a typo for `steps.a.result`; `step` arrives here as a
		// bare name. `a` is a real step but not a near edit-distance match to `step`,
		// so no "did you mean" is offered.
		src := `
edition: v2026.3
name: typo-root
steps:
  - id: a
    http:
      url: https://example.com
  - id: b
    log:
      message: ${step.a.result}
`
		got := diagnose(t, src)

		// The whole point: never suggest the name that does not exist.
		require.NotContains(t, got, "steps.step",
			"the diagnostic fabricated a steps.<thatname> suggestion that does not resolve")

		// Byte-exact the message this now produces.
		const want = "references unknown name \"step\"; a bare name is a loop's iterator, a name this step " +
			"declares in its own `vars:`, or `now`, and a step output is written `steps.<id>.<output>`"
		require.Contains(t, got, want)
	})

	t.Run("a near-miss to a real step is offered concretely", func(t *testing.T) {
		t.Parallel()
		// `totl` is one edit from the declared step `total`, so the diagnostic names
		// it — a suggestion that actually resolves once rooted.
		src := `
edition: v2026.3
name: near-miss
steps:
  - id: total
    http:
      url: https://example.com
  - id: b
    log:
      message: ${totl.body}
`
		got := diagnose(t, src)

		require.Contains(t, got, "did you mean `steps.total`?")
		// Still never the fabricated form.
		require.NotContains(t, got, "steps.totl")
	})
}

// TestValidateSourceReportsLineNumbers verifies diagnostics carry a source
// position, so an editor can place them and a human can find them.
func TestValidateSourceReportsLineNumbers(t *testing.T) {
	src := `edition: v2026.3
name: positions
steps:
  - id: first
    log:
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
	// "nosuchtask:" is on line 8, and the diagnostic names it rather than the step
	// it sits in — which is the improvement the flattening buys and the reason
	// this asserts the task's line rather than the step's.
	//
	// A task's name used to be a value inside a `task:` block, so the best a
	// diagnostic could do was name the step and leave the reader to find which of
	// its lines was meant. The name is now a key the author wrote, with a span of
	// its own, so the position is the token they have to change. The line above
	// ("- id: second") is still findable and is still the wrong answer: nothing is
	// wrong with the id.
	if ds[0].Line != 8 {
		t.Errorf("diagnostic line = %d, want 8 (the line naming the unknown task)", ds[0].Line)
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
			wantErr: "has to be the whole value here",
		},
		{
			name: "expression with leading text", in: "hello ${name}",
			wantErr: "has to be the whole value here",
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
//
// The shape it used to use — text beside a fence — became legal with #413, so
// the case moved to a fence whose contents are not an expression. That is the
// claim worth keeping: what is inside a fence is code, and code that does not
// parse is an error rather than characters.
func TestExprErrorsSurfaceFromCompilation(t *testing.T) {
	src := `
edition: v2026.3
name: bad-expr
steps:
  - id: a
    log:
      message: hello ${name +}
`
	if _, err := flowfile.Unmarshal([]byte(src)); err == nil {
		t.Fatal("expected compilation to reject a malformed expression, got no error")
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

			workflow, _, err := flowfile.ParseFile(path)
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

	const src = `edition: v2026.3
name: my workflow
steps:
  - id: a
    log:
      message: hi
`

	diagnostics, err := flowfile.ValidateSource([]byte(src))
	require.NoError(t, err)
	require.NotEmpty(t, diagnostics, "a name the schema refuses was accepted by flow validate")

	reported := diagnostics.Error()
	require.Contains(t, reported, "spaces", "the diagnostic does not say what is wrong: %s", reported)
	require.Contains(t, reported, "my-workflow", "the diagnostic does not offer a name that works: %s", reported)
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
	src.WriteString("edition: v2026.3\nname: expands\nsteps:\n  - id: first\n    log:\n      message: hello\n")
	for i := range 99 {
		fmt.Fprintf(&src, "  - id: s%d\n    log:\n      message: ${%s}\n", i, expression)
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

// TestAnInputIsRefusedInWorkflowVarsAndAcceptedInAStepsVars covers both directions of
// the one position `${inputs.<name>}` may not be written in.
//
// Written as a pair deliberately. A refusal tested on its own is half a rule: the
// same walk decides both, and a check subtracting `inputs.` too widely would refuse
// the step-level `vars:` this file's other half depends on and still look correct
// from the negative case alone. That failure is the shape CLAUDE.md names — a test
// asserting a party reaches its own resource, never that the other direction is
// closed — turned around: here it is the *permitted* direction that would go
// unnoticed.
//
// And each half is checked against what the engine actually does rather than only
// against the validator, because a diagnostic that is not true of the run is worse
// than no diagnostic. The refused file really does die before its first step, and the
// accepted one really does read the argument.
func TestAnInputIsRefusedInWorkflowVarsAndAcceptedInAStepsVars(t *testing.T) {
	t.Parallel()

	const inWorkflowVars = `edition: ` + flowfile.CurrentEdition + `
name: reading-an-input
inputs:
  service:
    type: string
    required: true
vars:
  target: ${inputs.service}
steps:
  - id: plan
    log:
      message: ${vars.target}
`

	diagnostics, err := flowfile.ValidateSource([]byte(inWorkflowVars))
	require.NoError(t, err, "the fixture no longer compiles, so it tests nothing")
	require.Len(t, diagnostics, 1,
		"a workflow-level var reading an input was not refused: %s", diagnostics.Error())

	reported := diagnostics[0]
	require.Equal(t, "vars.target", reported.Field,
		"the diagnostic does not name the var it is about")
	require.Equal(t, "service", reported.Value)
	require.NotZero(t, reported.Line, "the diagnostic has no position: %s", reported.Error())
	require.NotZero(t, reported.Column, "the diagnostic has no column: %s", reported.Error())
	require.Contains(t, reported.Message, "a var may not read an input")
	require.Contains(t, reported.Message, "inputs.service",
		"the diagnostic does not say where to write the reference instead")

	// The refusal is true of the engine, not a rule the validator invented.
	// `EvalWorkflowVars` evaluates the block against `NewScope(profile, nil)` — the
	// run's arguments are bound into the scope only afterwards (eval.go), and the
	// durable driver evaluates them in an activity handed the declared vars and the
	// profile and nothing else (engine.WorkflowVars) — so the name resolves against
	// no inputs at all.
	refused, err := flowfile.Unmarshal([]byte(inWorkflowVars))
	require.NoError(t, err)

	_, err = v1.RunWithInputs(t.Context(), refused,
		map[string]*v1.Value{"service": v1.NewLiteral("checkout")})
	require.ErrorContains(t, err, `var "target"`,
		"the file the validator refuses now runs, so the diagnostic reports something that is not so")

	// The other direction: a step's own `vars:` is evaluated where the run's
	// arguments are in scope, so it may read one — and must keep being able to.
	const inStepVars = `edition: ` + flowfile.CurrentEdition + `
name: reading-an-input-in-a-step
inputs:
  service:
    type: string
    required: true
steps:
  - id: plan
    vars:
      target: ${inputs.service}
    if: ${inputs.service != ''}
    log:
      message: ${'deploying ' + target}
`

	diagnostics, err = flowfile.ValidateSource([]byte(inStepVars))
	require.NoError(t, err)
	require.Empty(t, diagnostics,
		"a step's own `vars:` may read an input, and this file was refused: %s", diagnostics.Error())

	accepted, err := flowfile.Unmarshal([]byte(inStepVars))
	require.NoError(t, err)

	_, err = v1.RunWithInputs(t.Context(), accepted,
		map[string]*v1.Value{"service": v1.NewLiteral("checkout")})
	require.NoError(t, err, "a step's `vars:` reading an input validates but does not run")
}
