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
