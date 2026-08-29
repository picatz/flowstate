package flowfile_test

import (
	"errors"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/picatz/flowstate/pkg/flowstate/v1/flowfile"
)

// A `digest:` pin is not part of [v1.Workflow] — [v1.Call.SourceDigest] is set
// whether or not one was written, so Marshal has nothing to write back that
// says a pin existed. These tests are the fix for #339: a pin an author wrote
// has to survive `flow fmt`, in the exact position it was written, or the
// tool has silently turned off a security check with no diagnostic — the
// worst shape CLAUDE.md names. Compared as bytes throughout, for the reason
// format_test.go's own header gives: a rewrite that still validates is not
// proof it kept what the author wrote.

// TestFormatKeepsADigestPinBesideCall is the ordinary case, byte for byte: a
// pin written beside `call:` comes back in exactly the position `with:` sits
// beside — `call:`, then `digest:`, then `with:` — the order the parser reads
// them in and every example writes them in.
func TestFormatKeepsADigestPinBesideCall(t *testing.T) {
	dir := t.TempDir()
	writeFile(t, dir, "callee.yaml", simpleCalleeSource)
	pin := digestOf(t, simpleCalleeSource)
	src := pinnedCallerSource(pin)
	caller := writeFile(t, dir, "caller.yaml", src)

	workflow, _, err := flowfile.ParseFile(caller)
	require.NoError(t, err)

	got, err := flowfile.Format([]byte(src), workflow)
	require.NoError(t, err)

	// pin, not a hard-coded hash, the same way digestOf's own doc explains: a
	// one-character edit to simpleCalleeSource must not leave this test
	// asserting a stale hash.
	want := `edition: v2026.3
name: caller
description: pins what it calls
steps:
  - id: provision
    call: ./callee.yaml
    digest: ` + pin + `
    with:
      tenant: acme
`
	assert.Equal(t, want, string(got))
}

// A digest pin written through a scalar alias, and the accounting an anchor's
// depth needed, once had tests here. The grammar is now a strict subset of YAML
// that refuses anchors and aliases (#653), so a pin can no longer arrive through
// either — the compiler refuses the caller before the formatter sees it. Both
// tests were removed with the spellings they exercised.

// TestFormatPinIsIdempotent is [TestFormatIsIdempotent] for a pin: formatting
// the output of formatting a pinned caller must not move the pin, drop it, or
// duplicate it.
func TestFormatPinIsIdempotent(t *testing.T) {
	dir := t.TempDir()
	writeFile(t, dir, "callee.yaml", simpleCalleeSource)
	pin := digestOf(t, simpleCalleeSource)
	src := pinnedCallerSource(pin)
	callerPath := writeFile(t, dir, "caller.yaml", src)

	workflow, _, err := flowfile.ParseFile(callerPath)
	require.NoError(t, err)
	once, err := flowfile.Format([]byte(src), workflow)
	require.NoError(t, err)

	// Written back to disk so the second pass compiles it exactly the way
	// `flow fmt` would run it twice: from the file, not from the workflow
	// this test already has in hand.
	writeFile(t, dir, "caller.yaml", string(once))
	workflow2, _, err := flowfile.ParseFile(callerPath)
	require.NoError(t, err)
	twice, err := flowfile.Format(once, workflow2)
	require.NoError(t, err)

	assert.Equal(t, string(once), string(twice), "formatting a formatted pinned caller changed it again")
	assert.Contains(t, string(twice), "digest: "+pin, "the pin did not survive a second pass")
}

// TestFormatWithoutAPinInsertsNone is the regression guard the other
// direction: an unpinned call must format to exactly what Marshal alone
// writes, with no `digest:` line invented for it. A carve-out that leaked
// into the unpinned case would turn every unpinned call into a pinned one the
// next time somebody ran `flow fmt`.
func TestFormatWithoutAPinInsertsNone(t *testing.T) {
	dir := t.TempDir()
	writeFile(t, dir, "callee.yaml", simpleCalleeSource)
	src := `edition: v2026.3
name: caller
steps:
  - id: provision
    call: ./callee.yaml
    with:
      tenant: acme
`
	caller := writeFile(t, dir, "caller.yaml", src)

	workflow, _, err := flowfile.ParseFile(caller)
	require.NoError(t, err)

	marshalled, err := flowfile.Marshal(workflow)
	require.NoError(t, err)

	formatted, err := flowfile.Format([]byte(src), workflow)
	require.NoError(t, err)

	assert.Equal(t, string(marshalled), string(formatted))
	assert.NotContains(t, string(formatted), "digest:")
}

// TestFormatKeepsACommentOnAPin checks the ordering [Format]'s own doc
// promises: a pin is placed before comments are, so a comment written
// directly above or beside `digest:` has a key to attach to rather than
// finding the mapping still missing it.
func TestFormatKeepsACommentOnAPin(t *testing.T) {
	dir := t.TempDir()
	writeFile(t, dir, "callee.yaml", simpleCalleeSource)
	pin := digestOf(t, simpleCalleeSource)
	src := `edition: v2026.3
name: caller
steps:
  - id: provision
    call: ./callee.yaml
    # reviewed 2026-01-01
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
steps:
  - id: provision
    call: ./callee.yaml
    # reviewed 2026-01-01
    digest: ` + pin + `
    with:
      tenant: acme
`
	assert.Equal(t, want, string(got))
}

// TestFormatRefusesAPinItCannotPlace exercises [Format]'s fail-closed branch
// directly: given a source that pins a call and a workflow whose matching
// step is not that call — a mismatch [Format]'s exported signature allows a
// caller to construct, unlike `flow fmt`, which always compiles source into
// workflow itself before formatting it — nothing is written, and the
// refusal names the pin rather than the more generic "cannot be kept"
// wording a dropped comment gets, because a pin losing its home is a security
// check going missing rather than a stylistic loss.
func TestFormatRefusesAPinItCannotPlace(t *testing.T) {
	dir := t.TempDir()
	writeFile(t, dir, "callee.yaml", simpleCalleeSource)
	pin := digestOf(t, simpleCalleeSource)
	src := pinnedCallerSource(pin)

	// A workflow with the same shape (one step named "provision") but no
	// call at all, so the mapping [Format] renders for it never carries a
	// `call:` key for the pin to sit beside.
	mismatched, err := flowfile.Unmarshal([]byte(`edition: v2026.3
name: caller
description: pins what it calls
steps:
  - id: provision
    log:
      message: hi
`))
	require.NoError(t, err)

	out, err := flowfile.Format([]byte(src), mismatched)
	require.Error(t, err, "a pin with nowhere to go was written away silently")
	assert.Nil(t, out, "a refusal handed back bytes a caller could write")

	var diagnostics flowfile.Diagnostics
	require.True(t, errors.As(err, &diagnostics),
		"the refusal is not positioned, so an author cannot find the pin that caused it")
	require.Len(t, diagnostics, 1)
	assert.Contains(t, diagnostics[0].Message, "digest:")
	assert.Contains(t, diagnostics[0].Message, "security check",
		"a dropped pin should say plainly that it is a security check, not read like a dropped comment")
}

// TestFormatKeepsAPinInsideAForEach checks that the carry-across is not
// step-list-shaped: [collectPins] and [placePins] walk the whole document the
// way [collectComments] does, so a call nested inside a loop body gets the
// same treatment a top-level one does.
func TestFormatKeepsAPinInsideAForEach(t *testing.T) {
	dir := t.TempDir()
	writeFile(t, dir, "callee.yaml", simpleCalleeSource)
	pin := digestOf(t, simpleCalleeSource)
	src := `edition: v2026.3
name: caller
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
	caller := writeFile(t, dir, "caller.yaml", src)

	workflow, _, err := flowfile.ParseFile(caller)
	require.NoError(t, err)

	got, err := flowfile.Format([]byte(src), workflow)
	require.NoError(t, err)
	assert.Contains(t, string(got), "digest: "+pin)

	// Formatting again from the result must compile: the pin that came back
	// out is the pin the compiler still accepts, not text that merely looks
	// right.
	writeFile(t, dir, "caller.yaml", string(got))
	_, _, err = flowfile.ParseFile(caller)
	require.NoError(t, err, "the formatted file's pin no longer compiles")
}

// TestFormatAnchorIsNotALevelOfNesting was removed with the anchor it depended
// on: the grammar no longer accepts anchors, so a document carrying one is
// refused by the compiler rather than formatted, and there is no anchored-versus-
// plain depth comparison left to draw. See #653.
