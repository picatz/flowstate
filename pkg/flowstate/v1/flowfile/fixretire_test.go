package flowfile_test

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/picatz/flowstate/pkg/flowstate/v1/flowfile"
)

// Retiring three tasks is only affordable because the migration is a program, so what
// this rewriter has to earn is the trust to be run over a repository without the diff
// being read line by line.
//
// Every test here is written against [flowflile.Fix]'s output rather than against its
// internals, and most of them assert the *whole* document. That is deliberate: the one
// failure a migration cannot have is writing something that means something else, and a
// test asserting only the line it cared about cannot see the line it damaged.

// fixed runs the rewriter and returns the document, failing on a refusal.
func fixed(t *testing.T, src string) string {
	t.Helper()

	result, err := flowfile.Fix([]byte(src))
	require.NoError(t, err)
	require.Empty(t, result.Refusals, "the rewriter refused a document this test expects it to migrate")

	// Every file this rewriter writes has to be one the compiler accepts. A migration
	// that produces an invalid document has done worse than nothing: the author's
	// original is gone and what replaced it does not run.
	require.Empty(t, diagnose(t, string(result.Source)),
		"the rewritten document does not validate:\n%s", result.Source)

	return string(result.Source)
}

// refusals runs the rewriter and returns the refusal messages, failing if there are none.
func refusals(t *testing.T, src string) []string {
	t.Helper()

	result, err := flowfile.Fix([]byte(src))
	require.NoError(t, err)
	require.NotEmpty(t, result.Refusals, "the rewriter acted on a document it cannot know the intent of")

	out := make([]string, 0, len(result.Refusals))
	for _, refusal := range result.Refusals {
		out = append(out, refusal.Message)
	}

	return out
}

// TestARetiredStepWhoseValueIsReadMovesIntoVars is the mechanical case, and the only
// one there is.
//
// Nothing is guessed. The var takes the id the author already chose, which is what
// every reference already spells — so re-rooting `steps.greet.result` to `vars.greet`
// renames nothing, and that is what makes it safe to do without asking.
func TestARetiredStepWhoseValueIsReadMovesIntoVars(t *testing.T) {
	t.Parallel()

	got := fixed(t, `edition: v2026.2
name: t
steps:
  - id: greet
    echo:
      message: hello
  - id: show
    log:
      message: ${steps.greet.result}
`)

	assert.Equal(t, `edition: v2026.2
name: t
vars:
  greet: ${"hello"}
steps:
  - id: show
    log:
      message: ${vars.greet}
`, got)
}

// TestTheDeletionIsBoundedByTheDashIsTheWholeStep is the bug that made a file worse
// rather than better.
//
// A step's keys all sit at one indent, so a range measured from the first key ends at
// the second: `- id: greet` went and `echo:` stayed, leaving an orphan block where a
// step used to be. The dash is one level further out, and everything indented past it
// belongs to the step.
//
// Asserted as the whole document because that is the only way to see it. A test that
// checked "the file no longer contains `echo:`" passes on the broken output too.
func TestTheDeletionIsBoundedByTheDashIsTheWholeStep(t *testing.T) {
	t.Parallel()

	got := fixed(t, `edition: v2026.2
name: t
steps:
  - id: greet
    description: says hello
    echo:
      message: hello
  - id: show
    log:
      message: ${steps.greet.result}
`)

	assert.NotContains(t, got, "echo:", "the task block outlived the step it belonged to")
	assert.NotContains(t, got, "description: says hello", "a key of the deleted step was left behind")
	assert.Equal(t, `edition: v2026.2
name: t
vars:
  greet: ${"hello"}
steps:
  - id: show
    log:
      message: ${vars.greet}
`, got)
}

// TestAMovedValueIsFoldedIntoTheOneThatReadsIt is the limit of the destination showing
// through into the rewrite.
//
// Two moved values cannot become two vars, because a workflow var may not read another
// one: `vars:` is a mapping, so there is no order that would make one available to the
// other, and the validator says so. The earlier value is therefore *inlined* into the
// later one, parenthesised because CEL's precedence is not the author's line breaks.
//
// It terminates because a step may only read steps before it, so the moves form a chain
// rather than a cycle and each fold consumes one link.
func TestAMovedValueIsFoldedIntoTheOneThatReadsIt(t *testing.T) {
	t.Parallel()

	got := fixed(t, `edition: v2026.2
name: t
steps:
  - id: greet
    echo:
      message: hello
  - id: shout
    printf:
      format: "%s!"
      args:
        - ${steps.greet.result}
  - id: show
    log:
      message: ${steps.shout.result}
`)

	assert.Equal(t, `edition: v2026.2
name: t
vars:
  greet: ${"hello"}
  shout: ${"%s!".format([("hello")])}
steps:
  - id: show
    log:
      message: ${vars.shout}
`, got)
}

// TestACelStepsExpressionIsAlreadyTheSource covers the one input that is not a value
// carrying an expression but the expression itself.
//
// Every other input is fenced, and a moved literal has to be *quoted* on the way — it
// is travelling from a position where text is text into one where text is code. `expr`
// is the exception, and quoting it would turn an expression into a string.
func TestACelStepsExpressionIsAlreadyTheSource(t *testing.T) {
	t.Parallel()

	got := fixed(t, `edition: v2026.2
name: t
steps:
  - id: total
    cel:
      expr: "1 + 2"
  - id: show
    log:
      message: ${string(steps.total.result)}
`)

	assert.Contains(t, got, "total: ${1 + 2}",
		"the expression was quoted into a string instead of moved as code")
}

// TestAMovedMapLiteralIsQuotedForYAML is where the two languages disagree about braces.
//
// `${{'outer': 'inner'}}` is a fenced expression to this language and a flow mapping to
// YAML, so writing it unquoted produces a document that is no longer the value it was.
// The value is code now: it came from a position where YAML held it as a scalar and it
// is going into one where YAML has opinions.
func TestAMovedMapLiteralIsQuotedForYAML(t *testing.T) {
	t.Parallel()

	got := fixed(t, `edition: v2026.2
name: t
steps:
  - id: shape
    cel:
      expr: "{'outer': 'inner'}"
  - id: show
    log:
      message: ${steps.shape.result['outer']}
`)

	assert.Contains(t, got, `shape: "${{'outer': 'inner'}}"`,
		"a moved map literal was written unquoted, so YAML reads it as a mapping")
}

// TestNothingReadsItSoTheRewriterRefuses is the case that is not mechanical, and the
// design says it must stay that way.
//
// A step whose result nothing reads is intent this cannot see: it might have meant
// "show a human this line", which is `log:`, and it might have meant nothing at all and
// want deleting. Choosing would be the rewriter writing a step the author did not, so
// it says which two things it cannot tell apart and leaves the file alone.
func TestNothingReadsItSoTheRewriterRefuses(t *testing.T) {
	t.Parallel()

	src := `edition: v2026.2
name: t
steps:
  - id: greet
    echo:
      message: hello
`

	messages := strings.Join(refusals(t, src), "\n")
	assert.Contains(t, messages, "`log:`", "the refusal does not name one of the two ways out")
	assert.Contains(t, messages, "nothing reads", "the refusal does not say why it cannot choose")

	result, err := flowfile.Fix([]byte(src))
	require.NoError(t, err)
	assert.Equal(t, src, string(result.Source), "a refused document was rewritten anyway")
}

// TestAValueReadingASurvivingStepHasNowhereToGo is the limit of the destination, seen
// from the side the author has to act on.
//
// A workflow var is evaluated before the first step runs, so a value reading a step
// that is *staying* cannot become one. Both ways out need a judgement the rewriter does
// not have — inline the expression at each use, or move it to the `vars:` of the one
// step that uses it — so it names them and stops.
func TestAValueReadingASurvivingStepHasNowhereToGo(t *testing.T) {
	t.Parallel()

	messages := strings.Join(refusals(t, `edition: v2026.2
name: t
steps:
  - id: web
    http:
      method: GET
      url: https://example.com
  - id: code
    cel:
      expr: string(steps.web.status_code)
  - id: show
    log:
      message: ${steps.code.result}
`), "\n")

	assert.Contains(t, messages, "steps.web", "the refusal does not name the step that cannot be read")
	assert.Contains(t, messages, "evaluated before the first step runs",
		"the refusal does not say why a workflow var cannot read a step")
}

// TestAValueReadingADeclaredVarHasNowhereToGoEither is the same limit from the other
// side, and the one that is easy to miss.
//
// A var may not read a var, for a different reason than it may not read a step: `vars:`
// is a mapping and a mapping has no order, so "the one above" is not something the file
// can mean. A value the rewriter has already folded in is fine — that is what the fold
// is for — but one reading a var the *author* declared has nowhere to land.
func TestAValueReadingADeclaredVarHasNowhereToGoEither(t *testing.T) {
	t.Parallel()

	messages := strings.Join(refusals(t, `edition: v2026.2
name: t
vars:
  who: world
steps:
  - id: greet
    cel:
      expr: '"hello " + vars.who'
  - id: show
    log:
      message: ${steps.greet.result}
`), "\n")

	assert.Contains(t, messages, "vars.who", "the refusal does not name the var that cannot be read")
	assert.Contains(t, messages, "no order", "the refusal does not say why one var cannot read another")
}

// TestAStepAloneInABlockCannotBeMovedOut is a structural refusal rather than a semantic
// one.
//
// Removing the only step in a loop body or a parallel branch leaves `- steps:` with
// nothing under it, which is not a document. The value has somewhere to go and the step
// does not have somewhere to go *from*, so this says which and asks for the one thing
// that would make it possible.
func TestAStepAloneInABlockCannotBeMovedOut(t *testing.T) {
	t.Parallel()

	messages := strings.Join(refusals(t, `edition: v2026.2
name: t
steps:
  - id: fan
    parallel:
      - steps:
          - id: left
            echo:
              message: L
      - steps:
          - id: right
            log:
              message: R
  - id: show
    log:
      message: ${steps.left.result}
`), "\n")

	assert.Contains(t, messages, "only step in its",
		"the refusal does not say that the block would be left empty")
}

// TestAMovedValueWillNotOverwriteAnExistingVar is the collision the id-as-name rule
// makes possible.
//
// The name is the step's id, which is not a choice — it is what every reference already
// spells. That is exactly why it can collide with a var the author declared, and
// overwriting one would change what every reference to *it* means. Two things silently
// becoming one is the failure a rewriter may not have.
func TestAMovedValueWillNotOverwriteAnExistingVar(t *testing.T) {
	t.Parallel()

	messages := strings.Join(refusals(t, `edition: v2026.2
name: t
vars:
  greet: something else
steps:
  - id: greet
    echo:
      message: hello
  - id: show
    log:
      message: ${steps.greet.result}
`), "\n")

	assert.Contains(t, messages, "already declared",
		"the refusal does not say that the name is taken")
}

// TestAMovedValueJoinsAnExistingVarsBlock keeps an author's own vars where they were.
//
// Appended rather than merged into a rewritten block, so the order they were written in
// and the comments among them survive. A migration is a diff people read.
func TestAMovedValueJoinsAnExistingVarsBlock(t *testing.T) {
	t.Parallel()

	got := fixed(t, `edition: v2026.2
name: t
vars:
  # the region this deploys to
  region: eu-west-1
steps:
  - id: greet
    echo:
      message: hello
  - id: show
    log:
      message: ${vars.region + steps.greet.result}
`)

	assert.Equal(t, `edition: v2026.2
name: t
vars:
  # the region this deploys to
  region: eu-west-1
  greet: ${"hello"}
steps:
  - id: show
    log:
      message: ${vars.region + vars.greet}
`, got)
}

// TestTheRetirementRewriteIsIdempotent is the property that makes running this over a
// directory safe.
//
// A file it has already migrated must come back untouched, because the alternative is a
// tool nobody can run twice — and `flow fix --check` in CI runs it on every file every
// time.
func TestTheRetirementRewriteIsIdempotent(t *testing.T) {
	t.Parallel()

	once := fixed(t, `edition: v2026.2
name: t
steps:
  - id: greet
    echo:
      message: hello
  - id: shout
    printf:
      format: "%s!"
      args:
        - ${steps.greet.result}
  - id: show
    log:
      message: ${steps.shout.result}
`)

	twice, err := flowfile.Fix([]byte(once))
	require.NoError(t, err)
	assert.False(t, twice.Changed(), "a second run found something else to do")
	assert.Equal(t, once, string(twice.Source))
}

// TestAFlattenedTaskIsThenRetired is the pair of rules that needed the rewriter to run
// to a fixed point.
//
// The walk dispatches on the key it *found*, so flattening `task:` / `name: echo` into
// `echo:` hands that key to a walk which has already gone past it. One pass wrote a file
// `flow validate` refuses, exit 0, with a diagnostic saying to run `flow fix` — which
// had just run.
func TestAFlattenedTaskIsThenRetired(t *testing.T) {
	t.Parallel()

	got := fixed(t, `edition: v2026.2
name: t
steps:
  - id: greet
    task:
      name: echo
      inputs:
        message: hello
  - id: show
    log:
      message: ${steps.greet.result}
`)

	assert.Equal(t, `edition: v2026.2
name: t
vars:
  greet: ${"hello"}
steps:
  - id: show
    log:
      message: ${vars.greet}
`, got)
}
