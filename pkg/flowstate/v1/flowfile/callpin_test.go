package flowfile_test

import (
	"crypto/sha256"
	"encoding/hex"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"

	"github.com/picatz/flowstate/pkg/flowstate/v1/flowfile"
)

// A `digest:` beside a `call:` is the caller saying which bytes it reviewed.
// These are the four things that has to mean: an unpinned call is untouched, a
// matching pin compiles to exactly what the unpinned file compiles to, a
// mismatch is refused with the digest to adopt, and a pin that is not a digest
// is refused for being the wrong shape rather than silently never checked.

// digestOf renders bytes the way the compiler does, so a fixture can pin itself
// without the test hard-coding a hash that a one-character edit would falsify.
func digestOf(t *testing.T, source string) string {
	t.Helper()
	sum := sha256.Sum256([]byte(source))
	return "sha256:" + hex.EncodeToString(sum[:])
}

// pinnedCallerSource writes a caller pinning ./callee.yaml at pin. The `digest:`
// lands on line 7, column 13, well away from the start of the file: a position
// assertion against line 1 passes for a compiler that reports every diagnostic
// at the top of the document.
func pinnedCallerSource(pin string) string {
	return `edition: v2026.3
name: caller
description: pins what it calls
steps:
  - id: provision
    call: ./callee.yaml
    digest: ` + pin + `
    with:
      tenant: acme
`
}

// TestCallDigestPinMatchesCompiles is the ordinary case: a pin naming the bytes
// on disk compiles, and the digest recorded on the compiled call is the pin.
func TestCallDigestPinMatchesCompiles(t *testing.T) {
	dir := t.TempDir()
	writeFile(t, dir, "callee.yaml", simpleCalleeSource)
	pin := digestOf(t, simpleCalleeSource)
	caller := writeFile(t, dir, "caller.yaml", pinnedCallerSource(pin))

	workflow, _, err := flowfile.ParseFile(caller)
	require.NoError(t, err)

	call := workflow.GetSteps()[0].GetCall()
	require.NotNil(t, call)
	require.Equal(t, "callee", call.GetWorkflow().GetName())
	require.Equal(t, pin, call.GetSourceDigest(),
		"the pin and the digest recorded on the call must be the one string, taken from the one read")

	require.Empty(t, mustValidate(t, caller))
}

// TestCallDigestPinAcceptsUpperCase covers the normalization the package
// documents: hex has no case, so a pin copied out of a tool that renders
// upper-case names the same bytes and is the same pin.
func TestCallDigestPinAcceptsUpperCase(t *testing.T) {
	dir := t.TempDir()
	writeFile(t, dir, "callee.yaml", simpleCalleeSource)
	lower := digestOf(t, simpleCalleeSource)
	caller := writeFile(t, dir, "caller.yaml", pinnedCallerSource(strings.ToUpper(lower)))

	workflow, _, err := flowfile.ParseFile(caller)
	require.NoError(t, err)
	require.Equal(t, lower, workflow.GetSteps()[0].GetCall().GetSourceDigest(),
		"the compiled call records the one form this package writes, whatever case the pin was typed in")
}

// TestUnpinnedCallIsUnchangedByThePinFeature is the regression guard. A file
// with no pin has to compile to exactly what it compiled to before pins
// existed, which is the same claim as: a matching pin adds nothing to the
// compiled workflow, because a pin is a property of the file rather than of the
// run.
//
// Compared as bytes both ways round, by proto equality and by the wire encoding
// of the compiled message, because either alone can miss a field the other
// renders.
func TestUnpinnedCallIsUnchangedByThePinFeature(t *testing.T) {
	dir := t.TempDir()
	writeFile(t, dir, "callee.yaml", simpleCalleeSource)

	unpinned := writeFile(t, dir, "unpinned.yaml", `edition: v2026.3
name: caller
description: pins what it calls
steps:
  - id: provision
    call: ./callee.yaml
    with:
      tenant: acme
`)
	pinned := writeFile(t, dir, "pinned.yaml", pinnedCallerSource(digestOf(t, simpleCalleeSource)))

	plain, _, err := flowfile.ParseFile(unpinned)
	require.NoError(t, err)
	withPin, _, err := flowfile.ParseFile(pinned)
	require.NoError(t, err)

	// The workflows differ by name only because they are different files, so the
	// comparison is of the step both files spell identically.
	require.True(t, proto.Equal(plain.GetSteps()[0], withPin.GetSteps()[0]),
		"a pin changed the compiled step:\nunpinned: %v\npinned:   %v", plain.GetSteps()[0], withPin.GetSteps()[0])

	plainStep, err := proto.MarshalOptions{Deterministic: true}.Marshal(plain.GetSteps()[0])
	require.NoError(t, err)
	pinnedStep, err := proto.MarshalOptions{Deterministic: true}.Marshal(withPin.GetSteps()[0])
	require.NoError(t, err)
	require.Equal(t, plainStep, pinnedStep, "the compiled step must be byte-identical with and without a pin")
}

// TestCallDigestMismatchIsRefused is the diagnostic an author meets when the
// callee has moved on: one message, on the pin itself, carrying the digest to
// adopt so that fixing it is a copy and a paste.
//
// The message is compared as bytes rather than searched for a substring. A
// diagnostic is a product surface, and "contains the word digest" is satisfied
// by a sentence that says nothing an author can act on.
func TestCallDigestMismatchIsRefused(t *testing.T) {
	dir := t.TempDir()
	writeFile(t, dir, "callee.yaml", simpleCalleeSource)

	// A real, valid callee that simply is not the one that was pinned. The pin
	// below names an earlier version of it.
	stale := digestOf(t, "edition: v2026.3\nname: callee\n")
	caller := writeFile(t, dir, "caller.yaml", pinnedCallerSource(stale))

	_, _, err := flowfile.ParseFile(caller)
	require.Error(t, err)

	var ds flowfile.Diagnostics
	require.ErrorAs(t, err, &ds)
	require.Len(t, ds, 1, "a mismatched pin is one problem, and compiling the callee anyway would bury it:\n%s", err)

	d := ds[0]
	assert.Equal(t, 7, d.Line, "the diagnostic must land on the `digest:` line: %s", d.Error())
	assert.Equal(t, 13, d.Column, "the diagnostic must land on the pin's value, not on its key: %s", d.Error())
	assert.Equal(t, "provision", d.Step)

	actual := digestOf(t, simpleCalleeSource)
	assert.Equal(t,
		`digest pins "./callee.yaml" at `+stale+`, but that file hashes to `+actual+
			` right now; a mismatch means the callee changed since the pin was written, `+
			`so read what it does now and then write `+"`digest: "+actual+"` to adopt it",
		d.Message)
}

// TestCallDigestMismatchIsRefusedThoughTheCalleeIsValid is the direction that
// makes a pin worth having.
//
// A pin is about *identity*, not validity: the callee here compiles perfectly on
// its own and would compile perfectly as a callee, and the call is still refused,
// because what the caller reviewed is not what is on disk. A check that only
// fired on a broken callee would be a slower spelling of `flow validate`.
func TestCallDigestMismatchIsRefusedThoughTheCalleeIsValid(t *testing.T) {
	dir := t.TempDir()
	calleePath := writeFile(t, dir, "callee.yaml", simpleCalleeSource)

	// The premise, asserted rather than assumed: nothing is wrong with the file
	// the pin is being held against.
	require.Empty(t, mustValidate(t, calleePath), "the callee must be valid, or this test proves nothing")

	// And the same caller, unpinned, compiles against it without complaint.
	unpinned := writeFile(t, dir, "unpinned.yaml", `edition: v2026.3
name: caller
steps:
  - id: provision
    call: ./callee.yaml
    with:
      tenant: acme
`)
	require.Empty(t, mustValidate(t, unpinned))

	caller := writeFile(t, dir, "caller.yaml", pinnedCallerSource(digestOf(t, "edition: v2026.3\nname: callee\n")))
	_, _, err := flowfile.ParseFile(caller)
	require.Error(t, err, "a valid callee that is not the pinned callee must still be refused")
	assert.Contains(t, err.Error(), "changed since the pin was written")
}

// TestCallDigestMalformedIsRefused covers the three ways a pin can fail to be a
// digest at all. Each is its own diagnostic about shape, positioned on the
// value, and each says what to write instead.
func TestCallDigestMalformedIsRefused(t *testing.T) {
	actual := digestOf(t, simpleCalleeSource)

	for name, pin := range map[string]string{
		"no algorithm":  strings.TrimPrefix(actual, "sha256:"),
		"wrong length":  "sha256:ab12",
		"not hex":       "sha256:" + strings.Repeat("z", 64),
		"another algo":  "sha512:" + strings.Repeat("a", 64),
		"an image tag":  "latest",
		"quoted digest": `"` + actual + ` "`,
	} {
		t.Run(name, func(t *testing.T) {
			dir := t.TempDir()
			writeFile(t, dir, "callee.yaml", simpleCalleeSource)
			caller := writeFile(t, dir, "caller.yaml", pinnedCallerSource(pin))

			_, _, err := flowfile.ParseFile(caller)
			require.Error(t, err)

			var ds flowfile.Diagnostics
			require.ErrorAs(t, err, &ds)
			require.Len(t, ds, 1, "%s", err)

			assert.Equal(t, 7, ds[0].Line, "positioned on the pin: %s", ds[0].Error())
			assert.Equal(t, 13, ds[0].Column, "positioned on the pin's value: %s", ds[0].Error())
			assert.Contains(t, ds[0].Message, "which is not the shape of a pin")
			assert.Contains(t, ds[0].Message, "`digest: "+actual+"`",
				"the shape diagnostic must still hand over the digest that would work")
		})
	}
}

// TestCallDigestOnANonCallStepIsRefused is the other half of the negative
// direction. A pin on a step with no `call:` under it is a check nothing
// performs, and a check nothing performs reads to whoever wrote it exactly like
// one that passed.
func TestCallDigestOnANonCallStepIsRefused(t *testing.T) {
	dir := t.TempDir()
	caller := writeFile(t, dir, "caller.yaml", `edition: v2026.3
name: caller
steps:
  - id: greet
    digest: sha256:`+strings.Repeat("a", 64)+`
    log:
      message: hi
`)

	_, _, err := flowfile.ParseFile(caller)
	require.Error(t, err, "a pin on a step that calls nothing must be reported, not ignored")

	var ds flowfile.Diagnostics
	require.ErrorAs(t, err, &ds)
	require.Len(t, ds, 1)
	assert.Equal(t, 5, ds[0].Line)
	assert.Contains(t, ds[0].Message, "only meaningful beside `call:`")
	assert.Contains(t, ds[0].Message, "a pin nobody checks reads like one that passed")
}

// TestCallDigestIsNotATaskName checks the ambiguity the reservation exists to
// prevent, from the position an author would meet it: `digest:` on a step is
// the grammar's word, so it can never also be a task the registry hands out.
func TestCallDigestIsNotATaskName(t *testing.T) {
	dir := t.TempDir()
	caller := writeFile(t, dir, "caller.yaml", `edition: v2026.3
name: caller
steps:
  - id: greet
    digest: sha256:`+strings.Repeat("a", 64)+`
`)

	_, _, err := flowfile.ParseFile(caller)
	require.Error(t, err)
	assert.NotContains(t, err.Error(), "unknown task",
		"`digest:` is a step property, so a step carrying only one has no kind of work rather than an unknown task")
	assert.Contains(t, err.Error(), "a step has to do something")
}
