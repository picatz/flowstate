package flowfile_test

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/picatz/flowstate/pkg/flowstate/v1/flowfile"
)

// diagnose returns everything the tool would tell an author about a source, as one
// string, empty when it would say nothing.
//
// One string because these tests assert what an author *reads*, and a diagnostic is
// only useful in the form they see it: a parse failure and a validation finding arrive
// through different returns and read identically at the terminal. Joining them here
// means a case moving from one to the other — a rule that becomes a grammar error, or
// stops being one — does not silently stop being tested.
func diagnose(tb testing.TB, src string) string {
	tb.Helper()

	ds, err := flowfile.ValidateSource([]byte(src))
	if err != nil {
		return err.Error()
	}

	lines := make([]string, 0, len(ds))
	for _, d := range ds {
		lines = append(lines, d.Error())
	}

	return strings.Join(lines, "\n")
}

// `vars:` is one word at two positions with deliberately different rules, and almost
// everything worth asserting about it is a refusal. The block runs, the names resolve,
// and both drivers agree — that is covered by the shared execution cases. What those
// cannot see is the half of the design that exists to stop a file being written: a var
// reading a sibling, a step's name shadowing a loop's, a name escaping the step that
// declared it.
//
// Each case below states the refusal *and* the sentence an author reads, because a
// rule enforced by a diagnostic nobody can act on is a rule that gets worked around.

// TestWorkflowVarsMayReferenceNothing covers the block that is evaluated before
// anything has happened.
//
// Nothing is in scope there and nothing ever will be, so every reference is an error —
// but the three kinds are three different misunderstandings and get three different
// answers. Until this existed the file compiled and the run died before its first step,
// which is the worst place to learn it: the workflow had already started.
func TestWorkflowVarsMayReferenceNothing(t *testing.T) {
	t.Parallel()

	for _, test := range []struct {
		name string
		src  string
		want string
	}{
		{
			name: "a var reading another var",
			src: `
name: t
vars:
  a: ${vars.b}
  b: two
steps:
  - id: s
    echo:
      message: ${vars.a}
`,
			want: "a var may not read another var",
		},
		{
			name: "a var reading a step",
			src: `
name: t
vars:
  a: ${steps.s.result}
steps:
  - id: s
    echo:
      message: hi
`,
			want: "a var may not read a step",
		},
		{
			name: "a var reading a root as an operand",
			src: `
name: t
vars:
  a: ${size(vars)}
steps:
  - id: s
    echo:
      message: ${vars.a}
`,
			want: "a var may not read `vars`",
		},
		{
			name: "a var reading a name that means nothing",
			src: `
name: t
vars:
  a: ${nope}
steps:
  - id: s
    echo:
      message: ${vars.a}
`,
			want: "references unknown name",
		},
		{
			name: "a var of literals and functions is fine",
			src: `
name: t
vars:
  a: ${"x".upperAscii() + string(1 + 1)}
steps:
  - id: s
    echo:
      message: ${vars.a}
`,
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()

			reported := diagnose(t, test.src)
			if test.want == "" {
				require.Empty(t, reported, "a var that references nothing was refused")

				return
			}

			require.Contains(t, reported, test.want,
				"a var referencing something was accepted, and would fail the run instead")
		})
	}
}

// TestWorkflowVarDiagnosticsCarryAPosition checks the half of a diagnostic that is not
// its sentence.
//
// A workflow-level var belongs to no step, and every position this validator reports is
// looked up from a step id — so the natural result of adding a rule here is a correct
// message on line zero, which an editor cannot underline and a terminal prints without
// a place to go. Asserted separately from the messages because it fails separately:
// rewording a message leaves the position, and moving where a var is recorded leaves
// the message.
func TestWorkflowVarDiagnosticsCarryAPosition(t *testing.T) {
	t.Parallel()

	src := "name: t\nvars:\n  a: ${nope}\nsteps:\n  - id: s\n    echo:\n      message: hi\n"

	reported := diagnose(t, src)
	require.NotEmpty(t, reported)

	// The var is on line 3, and the expression is what is wrong rather than the
	// `vars:` key two lines above it.
	require.Contains(t, reported, "3:", "a workflow var diagnostic was reported without a position")
}

// TestAStepVarIsPrivateToItsStep is the negative direction.
//
// A step's vars are bare, which is only safe because they cannot be seen from anywhere
// else — and a test that each step reads its own is a functionality test wearing a
// security test's clothes. This asserts the other direction: the *next* step cannot.
func TestAStepVarIsPrivateToItsStep(t *testing.T) {
	t.Parallel()

	src := `
name: t
steps:
  - id: first
    vars:
      tag: one
    echo:
      message: ${tag}
  - id: second
    echo:
      message: ${tag}
`

	reported := diagnose(t, src)
	require.Contains(t, reported, `references unknown name "tag"`,
		"a name declared by one step was readable from the next")

	// And the sentence says what a bare name can be, because an author who wrote this
	// believed one of those things was true of it.
	require.Contains(t, reported, "own `vars:`")
}

// TestAStepVarIsRefusedRatherThanShadowing covers the rule the design is most likely to
// be argued out of.
//
// Resolving a collision by precedence is the cheap answer and it costs a reader: two
// bindings of `body` eleven lines apart, and knowing which one an expression means
// requires knowing a rule nothing in the file states. Refusing costs an author one
// rename, once, at the moment they are already editing the line.
func TestAStepVarIsRefusedRatherThanShadowing(t *testing.T) {
	t.Parallel()

	for _, test := range []struct {
		name string
		src  string
		want string
	}{
		{
			name: "shadowing a loop's iterator",
			src: `
name: t
steps:
  - id: each
    for_each:
      items: ${["a"]}
      iterator: name
      steps:
        - id: inner
          vars:
            name: other
          echo:
            message: ${name}
`,
			want: "already bound here by an enclosing loop or step",
		},
		{
			name: "shadowing an enclosing step's var",
			src: `
name: t
steps:
  - id: outer
    vars:
      shared: a
    for_each:
      items: ${["x"]}
      iterator: item
      steps:
        - id: inner
          vars:
            shared: b
          echo:
            message: ${shared}
`,
			want: "already bound here by an enclosing loop or step",
		},
		{
			name: "taking the name now",
			src: `
name: t
steps:
  - id: s
    vars:
      now: x
    echo:
      message: ${now}
`,
			want: "is the moment a `wait_until:` is evaluated",
		},
		{
			name: "a name of its own is fine",
			src: `
name: t
steps:
  - id: each
    for_each:
      items: ${["a"]}
      iterator: name
      steps:
        - id: inner
          vars:
            loud: ${name.upperAscii()}
          echo:
            message: ${loud}
`,
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()

			reported := diagnose(t, test.src)
			if test.want == "" {
				require.Empty(t, reported, "a step var that shadows nothing was refused")

				return
			}

			require.Contains(t, reported, test.want,
				"a step var shadowed a name already bound and was accepted")
		})
	}
}

// TestAStepVarMayNotReadItsSiblings holds the rule that keeps a `vars:` block a
// mapping rather than a sequence in disguise.
//
// A protobuf map has no order, so "the one above" is not something the file can mean —
// and accepting it would work exactly as often as the map happened to iterate the right
// way, which is the worst failure mode available: correct in the test, wrong in
// production, and different again on the next build.
func TestAStepVarMayNotReadItsSiblings(t *testing.T) {
	t.Parallel()

	src := `
name: t
steps:
  - id: s
    vars:
      a: one
      b: ${a}
    echo:
      message: ${b}
`

	require.Contains(t, diagnose(t, src), `references unknown name "a"`,
		"a step var read a sibling, which no ordering makes reliable")
}

// TestAVarsRootIsUsableAsAnOperand checks a working file is not reported as broken.
//
// The activation answers `vars` whole, the way it answers `steps`, so both are legal
// written bare — `vars["region"]` with a computed key, or `size(vars)`. `steps` was
// exempted from the bare-name check when rooting landed and `vars` was not, which is
// the shape a second root always takes: the exemption goes where the first one needed
// it rather than where the category does. A false diagnostic is worse than a missing
// one, because it teaches authors to stop reading them.
func TestAVarsRootIsUsableAsAnOperand(t *testing.T) {
	t.Parallel()

	src := `
name: t
vars:
  region: eu-west-1
steps:
  - id: s
    echo:
      message: ${vars["region"] + string(size(vars))}
`

	require.Empty(t, diagnose(t, src),
		"a root written as an operand was reported as an unknown name")
}

// TestStepVarsSurviveARoundTrip is the `flow fix` guard.
//
// Marshal is the inverse of the parser, and a key the parser reads but Marshal does not
// write is not a formatting bug — `flow fix` rewrites the file it is handed, so the
// block silently disappears from the author's source. The workflow-level block had
// exactly this hole before a test caught it; the step-level one is the same hole one
// nesting level down.
func TestStepVarsSurviveARoundTrip(t *testing.T) {
	t.Parallel()

	src := `edition: 2026.1
name: t
vars:
  region: eu-west-1
steps:
  - id: s
    vars:
      target: ${vars.region + "-a"}
    echo:
      message: ${target}
`

	wf, err := flowfile.Unmarshal([]byte(src))
	require.NoError(t, err)

	out, err := flowfile.Marshal(wf)
	require.NoError(t, err)

	require.Contains(t, string(out), "target:", "a step's `vars:` was dropped by Marshal, so `flow fix` would delete it")

	// And the rewritten file still means the same thing, which is the claim `flow fix`
	// actually makes. Comparing the compiled forms rather than the bytes, since layout
	// is exactly what it is allowed to change.
	again, err := flowfile.Unmarshal(out)
	require.NoError(t, err, "a marshalled file did not parse:\n%s", out)
	require.Equal(t, len(wf.GetSteps()), len(again.GetSteps()))
	require.Equal(t, wf.GetSteps()[0].GetVars()["target"].GetExpr().String(),
		again.GetSteps()[0].GetVars()["target"].GetExpr().String())
}

// TestAStepVarNamesItselfInADiagnostic checks that a failure inside a `vars:` block
// says which var, not just which step.
//
// A step may declare several, and "step \"s\": unknown name" sends an author to read
// all of them. The field is `vars.<name>` for the same reason an input diagnostic names
// the input.
func TestAStepVarNamesItselfInADiagnostic(t *testing.T) {
	t.Parallel()

	src := `
name: t
steps:
  - id: s
    vars:
      first: a
      second: ${nope}
    echo:
      message: ${first}
`

	message := diagnose(t, src)
	require.True(t, strings.Contains(message, "vars.second"),
		"a diagnostic inside a `vars:` block did not say which var:\n%s", message)
}
