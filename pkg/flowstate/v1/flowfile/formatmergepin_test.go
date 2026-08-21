package flowfile_test

import (
	"strconv"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/picatz/flowstate/pkg/flowstate/v1/flowfile"
)

// A `digest:` pin merged in through a `<<:`, or reached through an `&anchor`
// reused elsewhere, was the gap #639 recorded and #640 closed. The strict YAML
// profile (#653) subsumes that half of #640 by a shorter route: the compiler
// refuses a document holding a merge key, alias, or anchor, so a pin can no
// longer arrive by any of those ways. The four tests that pinned the merged-pin
// formatting behaviour were removed with the constructs they exercised.
//
// What remains is the formatter's *own* bound on pin collection, driven through
// [flowfile.Format] directly. That bound guards the formatter regardless of what
// the compiler accepts, so it outlives the merge key the compiler now refuses —
// until the follow-up that removes the formatter's merge handling too.

// TestFormatBoundsPinCollectionByTotalNodes holds the bound that following a
// merge key needs. Resolving an alias is expansion, and expansion is what a
// short document multiplies: the walk that reads pins therefore counts total
// nodes the way the compiler counts its own ([maxNodes]), because a depth bound
// does not stop a breadth explosion.
//
// Driven through [flowfile.Format] directly rather than through `flow fmt`,
// because the compiler refuses this document long before a formatter would see
// it — which is exactly why the formatter's own bound needs a test of its own.
func TestFormatBoundsPinCollectionByTotalNodes(t *testing.T) {
	// Neither 800 keys nor 800 steps is alarming on its own, and their product is
	// what a file of this size buys an attacker — the shape the compiler's own
	// expansion bound was written against before the construct was refused outright.
	const keys, steps = 800, 800

	var b strings.Builder
	b.WriteString("edition: v2026.3\nname: bomb\nsteps:\n")
	for d := range steps {
		b.WriteString("  - id: s" + strconv.Itoa(d) + "\n")
		b.WriteString("    <<: *base\n")
		b.WriteString("    log:\n      message: hi\n")
	}
	b.WriteString("anchored: &base\n")
	for i := range keys {
		b.WriteString("  k" + strconv.Itoa(i) + ": v" + strconv.Itoa(i) + "\n")
	}
	require.Less(t, b.Len(), 1<<20, "premise: the file is inside the size limit, so size is not what stops this")

	// Any workflow will do: the refusal happens while reading the source's
	// pins, before anything is placed into what Marshal rendered.
	workflow, err := flowfile.Unmarshal([]byte(`edition: v2026.3
name: bomb
steps:
  - id: s
    value: 1
`))
	require.NoError(t, err)

	out, err := flowfile.Format([]byte(b.String()), workflow)
	require.Error(t, err, "a document that expands past the node bound was walked to the end")
	assert.Nil(t, out)
	assert.Contains(t, err.Error(), "once aliases are expanded",
		"the expansion bound is what should have stopped this")
}

// TestCallPinsReadsOnlyCallSteps is the finding on #833, in the direction that
// matters: `call` and `digest` are keys of a *step*, and everywhere else in the
// language a mapping may hold any key an author likes. `vars:` here declares two
// ordinary variables that happen to be named that — the second even naming a
// real file's digest — and reading them as a security pin makes `flow fix` fail
// a run over a workflow that has no call in it at all.
//
// Mutation proof: dropping the isStep guard in collectMapping collects the vars
// decoy and fails the first assertion here.
func TestCallPinsReadsOnlyCallSteps(t *testing.T) {
	pin := digestOf(t, simpleCalleeSource)
	decoy := `edition: v2026.3
name: decoy
vars:
  call: ./callee.yaml
  digest: ` + pin + `
steps:
  - id: say
    log:
      message: hi
`
	pins, err := flowfile.CallPins([]byte(decoy))
	require.NoError(t, err)
	assert.Empty(t, pins,
		"two ordinary variables named `call` and `digest` were read as a call pin")

	// The other direction, in the same test because the guard is only correct if
	// it keeps both answers: a real call step nested inside a loop body is still
	// a step, and its pin is still found.
	nested := `edition: v2026.3
name: nested
vars:
  call: ./callee.yaml
  digest: ` + pin + `
steps:
  - id: fan
    for_each:
      items: ${["a", "b"]}
      as: tenant
      steps:
        - id: provision
          call: ./callee.yaml
          digest: ` + pin + `
          with:
            tenant: ${tenant}
`
	pins, err = flowfile.CallPins([]byte(nested))
	require.NoError(t, err)
	require.Len(t, pins, 1, "a pinned call inside a for_each body was not read as a pin")
	assert.Equal(t, "provision", pins[0].Step)
	assert.Equal(t, "./callee.yaml", pins[0].Call)
	assert.Equal(t, pin, pins[0].Digest)
	assert.Equal(t, 14, pins[0].Line, "the pin is not positioned at the `digest:` inside the loop body")
}

// TestFormatKeepsAPinBesideADecoyItMustNotInvent is the same guard seen through
// `flow fmt`, on bytes: the decoy mapping must come back as the two variables it
// is, and the real pin must still be carried across. A collector that read the
// decoy as a pin would refuse to format this file at all, since the rendered
// `vars:` mapping is not a call for a pin to sit beside.
func TestFormatKeepsAPinBesideADecoyItMustNotInvent(t *testing.T) {
	dir := t.TempDir()
	writeFile(t, dir, "callee.yaml", simpleCalleeSource)
	pin := digestOf(t, simpleCalleeSource)
	src := `edition: v2026.3
name: caller
vars:
  call: not a call
  digest: not a digest
steps:
  - id: provision
    call: ./callee.yaml
    digest: ` + pin + `
    with:
      tenant: acme
`
	caller := writeFile(t, dir, "caller.yaml", src)

	workflow, _, err := flowfile.ParseFile(caller)
	require.NoError(t, err)

	got, err := flowfile.Format([]byte(src), workflow)
	require.NoError(t, err)

	want := `edition: v2026.3
name: caller
vars:
  call: not a call
  digest: not a digest
steps:
- id: provision
  call: ./callee.yaml
  digest: ` + pin + `
  with:
    tenant: acme
`
	assert.Equal(t, want, string(got))
}
