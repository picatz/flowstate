package flowfile_test

import (
	"strconv"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/picatz/flowstate/pkg/flowstate/v1/flowfile"
)

// A `digest:` pin does not have to be written on the step it pins. A step may
// reach `call:` and `digest:` through a `<<:` merge key, and may be written
// whole as an `&anchor` reused elsewhere — all of which fields.go resolves
// before the compiler sees a step. The pin collector used to read only a
// mapping's own written keys, so a pin arriving any of those ways formatted as
// though it had never been written: the gap #639 recorded and #640 closes.
//
// Compared as bytes throughout, for the reason formatpin_test.go's header
// gives and CLAUDE.md legislates: a rewrite that still validates is not proof
// it kept what the author wrote. A file whose pin was dropped validates
// perfectly — it simply no longer checks anything.

// mergedPinSource is the shape the gap was written about: one step anchored
// with its call and its pin, a second step merging both in.
func mergedPinSource(pin string) string {
	return `edition: v2026.3
name: caller
steps:
  - &pinned
    id: first
    call: ./callee.yaml
    digest: ` + pin + `
    with:
      tenant: acme
  - id: second
    <<: *pinned
    with:
      tenant: beta
`
}

// TestFormatKeepsAPinMergedThroughAMergeKey is the negative direction the gap
// needs: a document whose pin would previously have been dropped comes through
// with a pin on *both* steps — the one that wrote it and the one that merged
// it — in the position every other pin is written in.
func TestFormatKeepsAPinMergedThroughAMergeKey(t *testing.T) {
	dir := t.TempDir()
	writeFile(t, dir, "callee.yaml", simpleCalleeSource)
	pin := digestOf(t, simpleCalleeSource)
	src := mergedPinSource(pin)
	caller := writeFile(t, dir, "caller.yaml", src)

	workflow, _, err := flowfile.ParseFile(caller)
	require.NoError(t, err)

	got, err := flowfile.Format([]byte(src), workflow)
	require.NoError(t, err)

	want := `edition: v2026.3
name: caller
steps:
- id: first
  call: ./callee.yaml
  digest: ` + pin + `
  with:
    tenant: acme
- id: second
  call: ./callee.yaml
  digest: ` + pin + `
  with:
    tenant: beta
`
	assert.Equal(t, want, string(got))
}

// TestFormatMergedPinIsIdempotent holds the other half of the byte contract:
// the formatted document — which no longer has a merge key in it, because
// [Marshal] renders every step whole — formats to exactly itself. Input bytes
// out, unchanged, where nothing is warranted.
func TestFormatMergedPinIsIdempotent(t *testing.T) {
	dir := t.TempDir()
	writeFile(t, dir, "callee.yaml", simpleCalleeSource)
	pin := digestOf(t, simpleCalleeSource)
	callerPath := writeFile(t, dir, "caller.yaml", mergedPinSource(pin))

	workflow, _, err := flowfile.ParseFile(callerPath)
	require.NoError(t, err)
	once, err := flowfile.Format([]byte(mergedPinSource(pin)), workflow)
	require.NoError(t, err)

	// Compiled from the file the way `flow fmt` would run a second time,
	// rather than from the workflow already in hand.
	writeFile(t, dir, "caller.yaml", string(once))
	second, _, err := flowfile.ParseFile(callerPath)
	require.NoError(t, err, "the formatted merged pin no longer compiles")
	twice, err := flowfile.Format(once, second)
	require.NoError(t, err)

	assert.Equal(t, string(once), string(twice),
		"formatting a formatted merged pin changed it again")
}

// TestFormatKeepsAPinWhoseCallIsMerged covers the other arrangement of the
// same two keys: the call arrives through the merge and the pin is written on
// the step. It also pins the direction that would be a *forged* pin — the
// unpinned step that owns the anchor must not come back carrying one.
func TestFormatKeepsAPinWhoseCallIsMerged(t *testing.T) {
	dir := t.TempDir()
	writeFile(t, dir, "callee.yaml", simpleCalleeSource)
	pin := digestOf(t, simpleCalleeSource)
	src := `edition: v2026.3
name: caller
steps:
  - &base
    id: first
    call: ./callee.yaml
    with:
      tenant: acme
  - id: second
    <<: *base
    digest: ` + pin + `
    with:
      tenant: beta
`
	caller := writeFile(t, dir, "caller.yaml", src)

	workflow, _, err := flowfile.ParseFile(caller)
	require.NoError(t, err)

	got, err := flowfile.Format([]byte(src), workflow)
	require.NoError(t, err)

	want := `edition: v2026.3
name: caller
steps:
- id: first
  call: ./callee.yaml
  with:
    tenant: acme
- id: second
  call: ./callee.yaml
  digest: ` + pin + `
  with:
    tenant: beta
`
	assert.Equal(t, want, string(got))
}

// TestFormatKeepsTheWrittenPinOverTheMergedOne is the precedence the grammar
// defines, read the way a rewriter has to read it: a key written on a mapping
// claims its name before anything a `<<:` merges in can. Both steps here pin,
// and they pin *different callees*, so a collector that preferred the merged
// value would write the digest of one file onto a call to another — a pin
// carried across as a check of bytes nobody authorized, which is worse than
// the dropped pin this closes.
//
// The document compiles only under that precedence, which is what makes the
// fixture honest: the compiler and the formatter are being held to one rule.
func TestFormatKeepsTheWrittenPinOverTheMergedOne(t *testing.T) {
	dir := t.TempDir()
	other := strings.Replace(simpleCalleeSource, "name: callee", "name: other", 1)
	writeFile(t, dir, "callee.yaml", simpleCalleeSource)
	writeFile(t, dir, "other.yaml", other)
	calleePin, otherPin := digestOf(t, simpleCalleeSource), digestOf(t, other)

	src := `edition: v2026.3
name: caller
steps:
  - &base
    id: first
    call: ./callee.yaml
    digest: ` + calleePin + `
    with:
      tenant: acme
  - id: second
    <<: *base
    call: ./other.yaml
    digest: ` + otherPin + `
    with:
      tenant: beta
`
	caller := writeFile(t, dir, "caller.yaml", src)

	workflow, _, err := flowfile.ParseFile(caller)
	require.NoError(t, err)

	got, err := flowfile.Format([]byte(src), workflow)
	require.NoError(t, err)

	want := `edition: v2026.3
name: caller
steps:
- id: first
  call: ./callee.yaml
  digest: ` + calleePin + `
  with:
    tenant: acme
- id: second
  call: ./other.yaml
  digest: ` + otherPin + `
  with:
    tenant: beta
`
	assert.Equal(t, want, string(got))
}

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
	// The shape TestMergeExpansionIsBounded uses on the compiler, for the
	// identical reason: neither 800 keys nor 800 steps is alarming on its own,
	// and their product is what a file of this size buys an attacker.
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
