package flowfile_test

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/picatz/flowstate/pkg/flowstate/v1/flowfile"
)

// `edition:` exists so that a future build refuses a file rather than silently
// reinterpreting it. That makes it a fail-closed surface, and this repo's rule
// for those is that they deny by default and deny on error — so what is worth
// testing is every way the answer can be no.

// TestEditionAcceptsTheCurrentGrammar covers the two spellings an author will
// actually write, and the one they will write by accident.
func TestEditionAcceptsTheCurrentGrammar(t *testing.T) {
	t.Parallel()

	body := "\nname: t\nsteps:\n  - id: a\n    log:\n      message: hi\n"

	tests := []struct {
		name string
		line string
	}{
		{name: "quoted, which is what a careful author writes", line: `edition: "` + flowfile.CurrentEdition + `"`},
		{name: "unquoted, which YAML reads as a number", line: "edition: " + flowfile.CurrentEdition},
		{name: "single quoted", line: `edition: '` + flowfile.CurrentEdition + `'`},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ds, err := flowfile.ValidateSource([]byte(tt.line + body))
			require.NoError(t, err)
			assert.Empty(t, ds, "the current edition is the one this build compiles")
		})
	}
}

// TestEditionIsOptional pins the choice not to require it.
//
// Requiring it would put a line of ceremony at the top of every file to say the
// only thing it can currently say. A file that does not care which grammar it is
// written in is the common case, and the common case should not have to say so.
func TestEditionIsOptional(t *testing.T) {
	t.Parallel()

	ds, err := flowfile.ValidateSource([]byte("edition: v2026.3\nname: t\nsteps:\n  - id: a\n    log:\n      message: hi\n"))
	require.NoError(t, err)
	assert.Empty(t, ds)
}

// TestEditionFailsClosed is the point of the key.
func TestEditionFailsClosed(t *testing.T) {
	t.Parallel()

	body := "\nname: t\nsteps:\n  - id: a\n    log:\n      message: hi\n"

	tests := []struct {
		name string
		line string
		says string
	}{
		{
			// A build that does not know an edition must not guess at it. Saying
			// "upgrade" rather than "fix the file" matters: the file is probably
			// right and this binary is probably old.
			name: "an edition from the future",
			line: `edition: "2099.7"`,
			says: "upgrade rather than editing",
		},
		{
			// The trap the dated form sets. `2026.10` and `2026.1` are the same
			// float, so a build that converted the number would compile the tenth
			// edition of a year as the first — silently, and only in a year where
			// there had been ten.
			name: "a dotted edition is not read as a number",
			line: "edition: 2026.10",
			says: `"2026.10"`,
		},
		{
			name: "a value that is not an edition at all",
			line: "edition: true",
			says: "must be written as",
		},
		{
			name: "an empty edition",
			line: `edition: ""`,
			says: "not one this build knows",
		},
		{
			name: "a list where a version goes",
			line: "edition: [2026.1]",
			says: "must be written as",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, _, err := flowfile.Parse([]byte(tt.line + body))
			require.Error(t, err, "an edition this build cannot compile must be refused")

			var ds flowfile.Diagnostics
			require.ErrorAs(t, err, &ds)
			require.Len(t, ds, 1,
				"the edition is read first, so nothing else is reported about a file in a grammar this build does not have")
			assert.Contains(t, ds[0].Error(), tt.says)
			assert.Positive(t, ds[0].Line, "a diagnostic has to say where")
		})
	}
}

// TestEditionIsReportedBeforeAnythingElse covers why it is read first.
//
// A file written in a grammar this build does not have will have other problems
// — every one of them describing the wrong language. Reporting them alongside the
// edition would bury the one diagnostic that explains all the rest.
func TestEditionIsReportedBeforeAnythingElse(t *testing.T) {
	t.Parallel()

	// Two genuine problems below the edition: an unknown key and a step with no
	// kind of work.
	src := `edition: "2099.7"
name: t
nonsense: 1
steps:
  - id: a
    timeout: 5s
`
	_, _, err := flowfile.Parse([]byte(src))
	require.Error(t, err)

	var ds flowfile.Diagnostics
	require.ErrorAs(t, err, &ds)
	require.Len(t, ds, 1, "only the edition is reported; got:\n%s", ds.Error())
	assert.Contains(t, ds[0].Error(), "2099.7")
}

// TestKnownEditionsIncludesTheCurrentOne is the kind of check that looks
// tautological and is not: the two are separately written, and a build whose
// current edition is absent from its own list would refuse every file that
// declares it.
func TestKnownEditionsIncludesTheCurrentOne(t *testing.T) {
	t.Parallel()

	assert.Contains(t, flowfile.KnownEditions(), flowfile.CurrentEdition)
}

// TestKnownEditionsCannotBeMutatedByACaller covers a package-level slice handed
// out by an exported function. Returning the backing array would let any caller
// rewrite what every other caller sees is a legal edition.
func TestKnownEditionsCannotBeMutatedByACaller(t *testing.T) {
	t.Parallel()

	editions := flowfile.KnownEditions()
	require.NotEmpty(t, editions)
	editions[0] = "tampered"

	assert.Contains(t, flowfile.KnownEditions(), flowfile.CurrentEdition,
		"a caller must not be able to change what this build compiles")
}
