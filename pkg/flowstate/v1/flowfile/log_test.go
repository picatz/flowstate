package flowfile_test

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/picatz/flowstate/pkg/flowstate/v1/flowfile"
)

// `log:` brought the first enum-typed input into the DSL, and an enum is the one field
// kind where the validator knows the whole set of acceptable values. That is worth
// spending a diagnostic on: everywhere else the tool can only say what is wrong, and
// here it can say what to write instead, in a message short enough to read.

// TestALogLevelIsWrittenAsTheChoice checks the spellings a Flowfile accepts.
func TestALogLevelIsWrittenAsTheChoice(t *testing.T) {
	t.Parallel()

	for _, test := range []struct {
		name  string
		level string
		want  string
	}{
		{name: "the plain spelling", level: "warn"},
		{name: "the schema spelling", level: "LEVEL_ERROR"},
		{name: "an unimportant case", level: "Info"},

		{
			// The most likely mistake by far, and three edits from `warn` — far enough
			// that the general suggester will not reach it, which is why a closed set
			// gets its own rule.
			name:  "the other common spelling of the same word",
			level: "warning",
			want:  `"warning" is not one of info, warn, error; did you mean "warn"?`,
		},
		{
			name:  "a level from some other system",
			level: "critical",
			want:  `"critical" is not one of info, warn, error`,
		},
		{
			// Nothing about the enum's storage is part of the language, so the number
			// behind a level is not a way to write it.
			name:  "the number behind it",
			level: "2",
			want:  "expected one of info, warn, error",
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()

			src := "edition: v2026.2\nname: t\nsteps:\n  - id: a\n    log:\n      level: " + test.level +
				"\n      message: hi\n"

			reported := diagnose(t, src)
			if test.want == "" {
				require.Empty(t, reported, "a legal level was refused")

				return
			}
			require.Contains(t, reported, test.want)
		})
	}
}

// TestALogLevelDiagnosticPointsAtTheLevel checks the position, separately from the
// sentence.
//
// A diagnostic on the wrong line is a correct message an author cannot act on, and it
// fails independently of the wording: rewording leaves the position alone, and changing
// how an input is addressed leaves the wording alone.
func TestALogLevelDiagnosticPointsAtTheLevel(t *testing.T) {
	t.Parallel()

	src := "edition: v2026.2\nname: t\nsteps:\n  - id: a\n    log:\n      message: hi\n      level: nope\n"

	require.Contains(t, diagnose(t, src), "7:",
		"a bad level was not reported on the line it was written")
}

// TestALogStepNeedsAMessage checks the one required input.
//
// A `log:` with a level and no message is a step that emits an empty line, which is
// both useless and hard to notice in a transcript — so the schema requires it and this
// checks the requirement reaches an author.
func TestALogStepNeedsAMessage(t *testing.T) {
	t.Parallel()

	src := "edition: v2026.2\nname: t\nsteps:\n  - id: a\n    log:\n      level: warn\n"

	require.Contains(t, diagnose(t, src), `task "log" requires input "message"`)
}

// TestALogStepHasNoOutputToReference is the design refusal seen from the file.
//
// [TestLogHasNoOutputs] asserts the descriptor is empty; this asserts what that means
// for someone writing a workflow — `${steps.say.result}` does not resolve, and they are
// told so before the run rather than after it.
func TestALogStepHasNoOutputToReference(t *testing.T) {
	t.Parallel()

	src := `
edition: v2026.2
name: t
steps:
  - id: say
    log:
      message: hi
  - id: use
    log:
      message: ${steps.say.result}
`

	require.Contains(t, diagnose(t, src), "result",
		"a step read an output from a `log:` step, which has none")
}

// TestALogStepSurvivesARoundTrip is the `flow fix` guard.
//
// Marshal is the inverse of the parser, and an input the parser reads but Marshal does
// not write is not a formatting bug: `flow fix` rewrites the file it is handed, so the
// value disappears from the author's source. A level is the input at risk here, being
// the first of its kind.
func TestALogStepSurvivesARoundTrip(t *testing.T) {
	t.Parallel()

	src := `edition: v2026.2
name: t
steps:
  - id: say
    log:
      level: warn
      message: careful
      fields:
        region: eu-west-1
`

	wf, err := flowfile.Unmarshal([]byte(src))
	require.NoError(t, err)

	out, err := flowfile.Marshal(wf)
	require.NoError(t, err)

	rendered := string(out)
	require.Contains(t, rendered, "level: warn", "`flow fix` would drop the level")
	require.Contains(t, rendered, "region: eu-west-1", "`flow fix` would drop the fields")

	// And what it wrote is still a file this build accepts, which is the claim
	// `flow fix` actually makes.
	require.Empty(t, diagnose(t, rendered), "a marshalled log step no longer validates:\n%s", rendered)
}
