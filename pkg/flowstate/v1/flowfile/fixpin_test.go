package flowfile_test

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/picatz/flowstate/pkg/flowstate/v1/flowfile"
)

// `flow fix` is a text rewriter rather than a marshal-then-write, and never
// touches a range it has no retired spelling for (fix.go's own doc). A
// `digest:` pin is not a retired spelling of anything, so nothing here should
// be reaching for it specifically — this file is the audit #339 asked for:
// proof, not assumption, that a pin sits outside every range [flowfile.Fix]
// rewrites, and that rewriting something *else* in the same step leaves the
// pin exactly as written.

// TestFixLeavesADigestPinByteForByte rewrites an unrelated retired spelling —
// the `task:` block TestFixRewritesTheRetiredTaskBlock already covers — on a
// step *beside* a pinned call, and checks the pinned step's own five lines
// (`call:`, `digest:`, and `with:`) come back unchanged, character for
// character, while the unrelated step changes shape around it.
func TestFixLeavesADigestPinByteForByte(t *testing.T) {
	dir := t.TempDir()
	writeFile(t, dir, "callee.yaml", simpleCalleeSource)
	pin := digestOf(t, simpleCalleeSource)

	pinnedLines := `  - id: provision
    call: ./callee.yaml
    digest: ` + pin + `
    with:
      tenant: acme
`

	src := `edition: v2026.3
name: caller
steps:
` + pinnedLines + `  - id: announce
    task:
      name: log
      inputs:
        message: done
`

	require.True(t, strings.Contains(src, pinnedLines), "premise: the fixture is built the way this test reads it")

	result, err := flowfile.Fix([]byte(src))
	require.NoError(t, err)
	require.Empty(t, result.Refusals, "nothing in this fixture needs guessing")
	require.True(t, result.Changed(), "the retired task: block is the thing this fixture exists to have rewritten")

	assert.Contains(t, string(result.Source), pinnedLines,
		"the pinned step changed even though nothing about it is a retired spelling:\n%s", result.Source)
	assert.NotContains(t, string(result.Source), "task:\n      name: log",
		"the retired block beside the pin was not actually rewritten, so this proves nothing")

	// What Fix left behind has to still compile, with the same pin still
	// verifying against the same, untouched callee — the point of the
	// byte-for-byte assertion above rather than a separate claim.
	callerPath := writeFile(t, dir, "caller.yaml", string(result.Source))
	_, _, err = flowfile.ParseFile(callerPath)
	require.NoError(t, err, "flow fix left a pinned call that no longer compiles")
}

// TestFixIsIdempotentOnAPinnedCall is [TestFixIsIdempotent]'s claim narrowed
// to a file `flow fix` has nothing to do to: a pinned call written in the
// current grammar must come back byte for byte, not just semantically
// unchanged.
func TestFixIsIdempotentOnAPinnedCall(t *testing.T) {
	dir := t.TempDir()
	writeFile(t, dir, "callee.yaml", simpleCalleeSource)
	pin := digestOf(t, simpleCalleeSource)
	src := pinnedCallerSource(pin)

	result, err := flowfile.Fix([]byte(src))
	require.NoError(t, err)
	require.Empty(t, result.Refusals)
	assert.False(t, result.Changed(), "a current-grammar pinned call has nothing for flow fix to rewrite")
	assert.Equal(t, src, string(result.Source))
}
