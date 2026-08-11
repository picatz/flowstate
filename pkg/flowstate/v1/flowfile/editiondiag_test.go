package flowfile_test

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/picatz/flowstate/pkg/flowstate/v1/flowfile"
)

// firstEditionSource is a file in the one edition this build knows and does not
// compile, with two more problems in it that were knowable on the first pass.
//
// Both sit on lines the edition rewrite does not move, which is the whole reason
// their positions can be reported at all.
const firstEditionSource = `edition: 2026.1
name: bad
steps:
  - id: greet
    iff: true
    log:
      message: ${steps.nope.result +}
`

// TestOlderEditionReportsTheRestOfTheFile is the regression for the second half
// of #385: a file at an edition this build can rewrite from used to report the
// edition line and nothing else, so the misspelled key and the broken expression
// cost the author two more edit-validate cycles.
func TestOlderEditionReportsTheRestOfTheFile(t *testing.T) {
	t.Parallel()

	_, err := flowfile.ValidateSource([]byte(firstEditionSource))
	require.Error(t, err, "the file is not in the edition this build compiles")

	var ds flowfile.Diagnostics
	require.ErrorAs(t, err, &ds)
	require.Len(t, ds, 3, "the edition line, the misspelled key, and the broken expression")

	// The edition diagnostic stays first, so the ordering tells the story: this
	// is why the rest of the report describes a file that has to be rewritten.
	assert.Equal(t, "edition", ds[0].Field)
	assert.Contains(t, ds[0].Message, "is older than this build compiles")

	rest := ds[1].Message + "\n" + ds[2].Message
	assert.Contains(t, rest, `did you mean "if"?`)
	assert.Contains(t, rest, "is not a valid expression")

	// Positions in the document the author is holding, not in the rewritten one.
	// `iff:` is on line 5 and the expression on line 7 of the source above.
	assert.Equal(t, 5, ds[1].Line)
	assert.Equal(t, 7, ds[2].Line)
}

// TestOlderEditionKeepsTheGateWhenTheRewriteMovesLines is the fail-closed half.
//
// `task:`/`name:` collapses into one key, so everything below it moves. A
// diagnostic carried out of that rewrite would name a line whose content in the
// author's file is something else, which is the false diagnostic this package
// refuses to emit. The gate therefore stays absolute for a rewrite that moves
// anything.
func TestOlderEditionKeepsTheGateWhenTheRewriteMovesLines(t *testing.T) {
	t.Parallel()

	const shifting = `edition: 2026.1
name: shift
steps:
  - id: greet
    task:
      name: log
      with:
        message: hello
  - id: after
    log:
      message: ${steps.nope.result}
`

	_, err := flowfile.ValidateSource([]byte(shifting))
	require.Error(t, err)

	var ds flowfile.Diagnostics
	require.ErrorAs(t, err, &ds)
	require.Len(t, ds, 1, "a rewrite that moves a line reports the edition and stops")
	assert.Equal(t, "edition", ds[0].Field)
}

// TestUnknownEditionKeepsTheGateAbsolute pins the case the gate was written for.
// A file from the future declares a grammar this build does not have, so every
// other diagnostic would describe the wrong language, and there is no rewrite to
// bring it back.
func TestUnknownEditionKeepsTheGateAbsolute(t *testing.T) {
	t.Parallel()

	const future = `edition: v2099.1
name: future
steps:
  - id: greet
    iff: true
    log:
      message: ${steps.nope.result +}
`

	_, err := flowfile.ValidateSource([]byte(future))
	require.Error(t, err)

	var ds flowfile.Diagnostics
	require.ErrorAs(t, err, &ds)
	require.Len(t, ds, 1)
	assert.Contains(t, ds[0].Message, "is not one this build knows")
}

// TestEditionsAreSpelledOneWay is the regression for the first half of #385.
//
// The known-editions list read `(2026.1, v2026.2)`, one bare and one
// `v`-prefixed in the same breath, which reads as one name an inconsistent
// formatter spelled twice rather than as two names each spelled the only way it
// can be. Every member now goes through one formatter, and so does the edition
// any other message names on its own.
func TestEditionsAreSpelledOneWay(t *testing.T) {
	t.Parallel()

	const unknown = `edition: 2026
name: bad
steps:
  - id: greet
    log:
      message: hello
`

	_, err := flowfile.ValidateSource([]byte(unknown))
	require.Error(t, err)

	var ds flowfile.Diagnostics
	require.ErrorAs(t, err, &ds)
	require.Len(t, ds, 1)

	message := ds[0].Message
	for _, edition := range flowfile.KnownEditions() {
		assert.Contains(t, message, `"`+edition+`"`,
			"every known edition must be rendered through the one formatter")
	}

	// The defect itself: a member of the list appearing bare beside a quoted one.
	// Checked as the absence of the unquoted spelling rather than by comparing the
	// whole sentence, so rewording the diagnostic does not fail this.
	for _, edition := range flowfile.KnownEditions() {
		bare := strings.ReplaceAll(message, `"`+edition+`"`, "")
		assert.NotContains(t, bare, edition,
			"an edition must not appear unquoted beside quoted ones")
	}
}

// TestOlderEditionNamesTheCurrentOneThroughTheSameFormatter keeps the two
// messages in step. An author reading the list and typing what it showed must
// meet the same spelling in the answer they get back.
func TestOlderEditionNamesTheCurrentOneThroughTheSameFormatter(t *testing.T) {
	t.Parallel()

	_, err := flowfile.ValidateSource([]byte(firstEditionSource))
	require.Error(t, err)

	var ds flowfile.Diagnostics
	require.ErrorAs(t, err, &ds)
	require.NotEmpty(t, ds)

	assert.Contains(t, ds[0].Message, `"`+flowfile.CurrentEdition+`"`)
}
