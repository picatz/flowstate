package flowfile_test

import (
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/picatz/flowstate/pkg/flowstate/v1/flowfile"
)

// Every bound in this package is against input an outside party chooses — the
// language server compiles whatever an editor opens, and a server compiles
// whatever is submitted — so each one needs a test that reaches it.
//
// The rule these are written to is that a bound has to match the shape of the
// attack. Depth bounds do not stop breadth explosions; a bound on the values a
// walk descends into does not stop values produced without descending.

// TestMergeExpansionIsBounded is the case that was not.
//
// The document bound counts values the compiler descends into. Merging produces
// values without descending: one anchored mapping of N keys merged into D steps
// is N×D entries from a file of size N+D, so the cost is quadratic in something
// the file does not have to be large to say. The limit was never reached, because
// it was counting steps.
//
// Measured before the bound, the document built here — 110 KiB, well inside the
// 1 MiB ceiling — took 27 seconds, and the cost grows quadratically from there. A
// language server runs this on whatever file an editor opens.
func TestMergeExpansionIsBounded(t *testing.T) {
	t.Parallel()

	// Deliberately modest next to maxNodes. The point is that neither dimension
	// looks alarming on its own: 800 keys is a large mapping and 800 steps is a
	// large workflow, and neither is anywhere near a limit. Their product is.
	const keys, steps = 800, 800

	var b strings.Builder
	b.WriteString("name: bomb\nsteps:\n")
	for d := range steps {
		b.WriteString("  - id: s" + strconv.Itoa(d) + "\n")
		b.WriteString("    <<: *base\n")
		b.WriteString("    echo:\n      message: hi\n")
	}
	// The anchor is written last so that nothing depends on declaration order
	// being what makes this terminate.
	b.WriteString("anchored: &base\n")
	for i := range keys {
		b.WriteString("  k" + strconv.Itoa(i) + ": v" + strconv.Itoa(i) + "\n")
	}

	src := []byte(b.String())
	require.Less(t, len(src), 1<<20, "premise: the file is inside the size limit, so size is not what stops this")

	done := make(chan flowfile.Diagnostics, 1)
	go func() {
		_, _, err := flowfile.Parse(src)
		var ds flowfile.Diagnostics
		if !assert.ErrorAs(t, err, &ds) {
			done <- nil
			return
		}
		done <- ds
	}()

	select {
	case ds := <-done:
		require.NotNil(t, ds)
		// Reached, not merely survived. A run that finished quickly because the
		// document was rejected for some unrelated reason would prove nothing about
		// the bound, so the diagnostic itself is what is asserted.
		assert.Contains(t, ds.Error(), "once aliases are expanded",
			"the expansion bound is what should have stopped this")
	case <-time.After(20 * time.Second):
		t.Fatal("compiling a 110 KiB document did not finish; the merge expansion is unbounded again")
	}
}

// TestMergeExpansionWithinTheBoundStillWorks is the other half, and the reason
// the bound is a count rather than a refusal.
//
// `<<:` is how step boilerplate is shared, which is a thing people should do. A
// bound that made ordinary sharing fail would be a bound that removed a feature
// to close a hole.
func TestMergeExpansionWithinTheBoundStillWorks(t *testing.T) {
	t.Parallel()

	// The anchor is on a step, because a Flowfile has nowhere else to put one: a
	// top-level key added purely to hold an anchor is an unknown key, and reported
	// as one. So boilerplate is shared by anchoring the first step that carries it.
	src := `name: shared
steps:
  - &policy
    id: a
    timeout: 30s
    continue_on_error: true
    echo:
      message: one
  - id: b
    <<: *policy
    echo:
      message: two
`
	wf, _, err := flowfile.Parse([]byte(src))
	require.NoError(t, err)
	require.Len(t, wf.GetSteps(), 2)

	for _, step := range wf.GetSteps() {
		assert.True(t, step.GetPolicy().GetContinueOnError(), "step %q", step.GetId())
		assert.Equal(t, 30*time.Second, step.GetPolicy().GetTimeout().AsDuration(), "step %q", step.GetId())
	}

	// The merged step keeps its own id and its own work, which is what makes the
	// pattern usable at all: merging a whole step in would otherwise give two steps
	// the same id, and this file would be refused rather than shared.
	assert.Equal(t, "b", wf.GetSteps()[1].GetId())
	assert.Equal(t, "two", wf.GetSteps()[1].GetTask().GetInputs()["message"].GetLiteral().GetStringValue())
}

// TestWrittenKeysWinOverMergedOnes pins the precedence the merge pass exists to
// get right, which the bound above must not have disturbed.
//
// A merged mapping does not shadow a key the step writes for itself. That is
// YAML's rule, and it is the whole reason `<<:` is usable for sharing
// boilerplate: a step opts into the shared policy and then overrides the one part
// it needs different.
func TestWrittenKeysWinOverMergedOnes(t *testing.T) {
	t.Parallel()

	src := `name: shared
steps:
  - &policy
    id: base
    timeout: 30s
    echo:
      message: base
  - id: overrides
    <<: *policy
    timeout: 5s
    echo:
      message: one
  - id: written-first
    timeout: 5s
    <<: *policy
    echo:
      message: two
`
	wf, _, err := flowfile.Parse([]byte(src))
	require.NoError(t, err)
	require.Len(t, wf.GetSteps(), 3)
	wf.Steps = wf.GetSteps()[1:]

	// Both directions, because the pass that decides this walks the keys in source
	// order and a bug here would show in only one of them.
	for _, step := range wf.GetSteps() {
		assert.Equal(t, 5*time.Second, step.GetPolicy().GetTimeout().AsDuration(),
			"step %q writes its own timeout, so the merged one must not win", step.GetId())
	}
}

// The root is the one name rooting *creates* a collision for, which is worth
// stating plainly in a change that is otherwise about deleting collision rules.
//
// It has to be refused at compile time rather than left to resolve, because the
// runtime deliberately lets a step of this name win: a spec compiled before the
// root existed may contain one, and a worker replaying it must resolve the way it
// always did. That compatibility is only safe while no *new* file can create the
// situation.

// TestNothingMayBeCalledSteps covers every route a name reaches an expression's
// scope by, because closing one and leaving the others is how this kind of hole
// survives.
func TestNothingMayBeCalledSteps(t *testing.T) {
	t.Parallel()

	tests := map[string]string{
		"a top-level step": `name: t
steps:
  - id: steps
    echo:
      message: hi
`,
		"a step inside a loop body": `name: t
steps:
  - id: a
    for_each:
      items: ${[1]}
      steps:
        - id: steps
          echo:
            message: hi
`,
		"a step inside a parallel branch": `name: t
steps:
  - id: a
    parallel:
      - steps:
          - id: steps
            echo:
              message: hi
`,
		// The other route into a body's scope. A bound name wins over the scope it
		// is bound into, so this hides every step from exactly the place rooted
		// references are written.
		"a loop iterator": `name: t
steps:
  - id: a
    for_each:
      items: ${[1]}
      iterator: steps
      steps:
        - id: b
          echo:
            message: hi
`,
	}

	for name, src := range tests {
		t.Run(name, func(t *testing.T) {
			t.Parallel()

			ds, err := flowfile.ValidateSource([]byte(src))
			require.NoError(t, err, "the document is valid YAML; the name is a semantic problem")
			require.NotEmpty(t, ds, "%s called `steps` was accepted", name)
			assert.Contains(t, ds.Error(), "hide all",
				"the diagnostic has to say what goes wrong, not only that the name is taken")
		})
	}
}

// TestAStepCalledStepsWouldHaveFailedAtRunTime is the evidence the rule above is
// worth having, rather than a name reserved out of tidiness.
//
// Without the refusal this document validates clean and then dies on its third
// step with `no such key: other` — the shape of failure this repo cares most
// about, because nothing an author can see says why.
func TestAStepCalledStepsWouldHaveFailedAtRunTime(t *testing.T) {
	t.Parallel()

	src := `name: shadowed-root
steps:
  - id: steps
    echo:
      message: i am a step called steps
  - id: other
    echo:
      message: hello
  - id: read
    echo:
      message: ${steps.other.result}
`
	ds, err := flowfile.ValidateSource([]byte(src))
	require.NoError(t, err)
	require.NotEmpty(t, ds)

	// The reference is *not* what is reported. It is correct; the id is the
	// problem, and a diagnostic on the reference would send an author to fix the
	// wrong line.
	assert.NotContains(t, ds.Error(), "unknown step",
		"the reference is fine; the id is what has to change")
}
