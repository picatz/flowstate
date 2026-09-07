package flowfile_test

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	v1 "github.com/picatz/flowstate/pkg/flowstate/v1"
	"github.com/picatz/flowstate/pkg/flowstate/v1/flowfile"
)

// #1682: `value: dhl` is an expression, so a plain word is an unresolved name,
// and the diagnostic listed three things a bare name can be and never the
// fourth — the string the author meant — nor the spelling that writes it.

const bareWordSource = `edition: v2026.3
name: bare-word
inputs:
  priority:
    type: string
steps:
  - id: route
    switch:
      value: ${inputs.priority}
      cases:
        - case: express
          steps:
            - id: express_carrier
              value: dhl
`

func TestABareWordIsOfferedItsStringSpelling(t *testing.T) {
	t.Parallel()

	ds, err := flowfile.ValidateSource([]byte(bareWordSource))
	require.NoError(t, err)
	require.Len(t, ds, 1, "one bare word is one diagnostic:\n%s", ds.Error())

	d := ds[0]
	assert.Equal(t, "express_carrier", d.Step)
	assert.Contains(t, d.Message, `references unknown name "dhl"`)
	assert.Contains(t, d.Message, `a string is written ${"dhl"}`,
		"the diagnostic does not say how to write the word as a string")
	assert.Equal(t, v1.DiagnosticCodeUnresolvedReference, d.Code)

	require.Len(t, d.Edits, 1, "the string spelling is offered as an edit a program can apply")
	assert.Equal(t, `write the string ${"dhl"}`, d.Edits[0].GetTitle())

	// Applied blind — the schema message and the bytes, nothing else — the
	// file validates: the edit replaced exactly the scalar the author wrote.
	fixed := applyBlind(t, []byte(bareWordSource), d.Edits[0])
	assert.Contains(t, string(fixed), `value: ${"dhl"}`)
	after, err := flowfile.ValidateSource(fixed)
	require.NoError(t, err)
	assert.Empty(t, after, "the file the edit produced still has diagnostics:\n%s", after.Error())
}

// TestAQuotedBareWordIsOfferedItsStringSpelling is the likeliest way an author
// reaches for a string, and it is still an expression: YAML's quotes are gone
// by the time the scalar is read as CEL. The edit replaces the quoted scalar,
// quotes included, with the fenced string.
func TestAQuotedBareWordIsOfferedItsStringSpelling(t *testing.T) {
	t.Parallel()

	src := "edition: v2026.3\nname: quoted\nsteps:\n  - id: carrier\n    value: \"dhl\"\n"

	ds, err := flowfile.ValidateSource([]byte(src))
	require.NoError(t, err)
	require.Len(t, ds, 1, "one bare word is one diagnostic:\n%s", ds.Error())

	d := ds[0]
	assert.Contains(t, d.Message, `a string is written ${"dhl"}`)
	require.Len(t, d.Edits, 1)

	fixed := applyBlind(t, []byte(src), d.Edits[0])
	assert.Contains(t, string(fixed), `value: ${"dhl"}`, "the edit left the quotes behind:\n%s", fixed)
	after, err := flowfile.ValidateSource(fixed)
	require.NoError(t, err)
	assert.Empty(t, after, "the file the edit produced still has diagnostics:\n%s", after.Error())
}

// TestAFencedNameIsNotOfferedTheStringReading is the guard: an author who wrote
// `${dhl}` reached for a reference, and the string reading would be a guess
// about a different mistake. The diagnostic they had stays byte for byte.
func TestAFencedNameIsNotOfferedTheStringReading(t *testing.T) {
	t.Parallel()

	src := "edition: v2026.3\nname: fenced\nsteps:\n  - id: carrier\n    value: ${dhl}\n"

	ds, err := flowfile.ValidateSource([]byte(src))
	require.NoError(t, err)
	require.Len(t, ds, 1, "one bad reference is one diagnostic:\n%s", ds.Error())

	d := ds[0]
	assert.Equal(t, `references unknown name "dhl"`+unknownBareNameHelp, d.Message)
	assert.Empty(t, d.Edits, "a fenced reference was offered a string it did not ask for")
}

// TestABareWordInScopeIsNotAName is the other guard: a bare identifier that
// resolves is a reference, and nothing about it is reported.
func TestABareWordInScopeIsNotAName(t *testing.T) {
	t.Parallel()

	src := "edition: v2026.3\nname: in-scope\nvars:\n  carriers: [\"dhl\", \"ups\"]\nsteps:\n  - id: each\n    for_each:\n      items: ${vars.carriers}\n      as: item\n      steps:\n        - id: pick\n          value: item\n"

	ds, err := flowfile.ValidateSource([]byte(src))
	require.NoError(t, err)
	assert.Empty(t, ds, "an iterator written bare was reported:\n%s", ds.Error())
}
