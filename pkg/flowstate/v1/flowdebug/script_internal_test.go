package flowdebug

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestEveryVerbTheVocabularyHasIsOneAScriptMayCarry walks the command table and
// checks that [CheckScript] accepts each verb, and each of its aliases, spelled
// the way `help` says to spell it.
//
// The walk is the test, in the shape `tools/fuzztargets` and the conformance
// callers rule already use: a checker holding its own list of verbs is a second
// copy of the vocabulary, and the way that copy fails is silent — a verb added
// to `commands` would be offered by the completer, understood by `dispatch`,
// printed by `help`, and refused by this as a misspelling, in a file, before
// the run, with a suggestion naming a word the author did not want.
//
// It is an internal test because the table is unexported and should stay so:
// what the vocabulary is belongs to this package, and the point is that nothing
// outside it has to know.
func TestEveryVerbTheVocabularyHasIsOneAScriptMayCarry(t *testing.T) {
	t.Parallel()

	require.NotEmpty(t, commands, "the vocabulary is empty, so this walk proves nothing")

	steps := []string{"build"}
	checked := 0

	for _, c := range commands {
		// What the verb's own table entry says follows it. Read from
		// `completes` rather than chosen per verb, so a new entry is covered
		// by declaring what it takes rather than by somebody remembering to
		// extend this.
		argument := ""
		switch c.completes {
		case completesStep, completesBreakpoint:
			argument = " build"
		case completesExpression:
			argument = " 1 + 1"
		}

		for _, spelling := range append([]string{c.verb}, c.aliases...) {
			checked++

			problems, total := CheckScript([]string{spelling + argument}, steps)

			assert.Empty(t, problems,
				"`%s` is a verb this session understands and a script carrying it was refused", spelling)
			assert.Zero(t, total)
		}
	}

	assert.GreaterOrEqual(t, checked, len(commands),
		"fewer spellings were checked than there are verbs")
}

// TestASpellingTheVocabularyDropsIsRefused is the other direction, and the one
// that says the walk above is checking anything at all.
//
// A checker that accepted every line would pass the walk perfectly.
func TestASpellingTheVocabularyDropsIsRefused(t *testing.T) {
	t.Parallel()

	for _, verb := range []string{"stepp", "cont", "brake", "insepct", "", " "} {
		problems, total := CheckScript([]string{verb}, []string{"build"})

		assert.NotEmpty(t, problems, "%q was accepted as a command", verb)
		assert.Positive(t, total)
	}
}

// TestArgumentOffsetPointsAtTheWordAfterTheVerb pins the position arithmetic a
// diagnostic's column depends on, including the case where there is no argument
// at all — which must point at something on the line rather than past its end.
func TestArgumentOffsetPointsAtTheWordAfterTheVerb(t *testing.T) {
	t.Parallel()

	for _, test := range []struct {
		line   string
		column int
	}{
		{"break build", 7},
		{"  break build", 9},
		{"break   build", 9},
		{"\tbreak\tbuild", 8},
		{"break", 1},
		{"break ", 1},
	} {
		assert.Equal(t, test.column, columnOf(test.line, argumentOffset(test.line)),
			"the argument column is wrong for %q", test.line)
	}
}

// TestCheckStepArgumentSkipsSuggestionWorkPastTheCap is the assertion
// [CheckScript]'s report cap needs and [TestCheckScriptBoundsInventoryWorkAfterTheReportIsFull]
// cannot give it on its own: that test's problem count and total hold whether
// or not [checkStepArgument] still pays for a suggestion search and an
// inventory render on every refused line past [MaxScriptProblems] — `report`
// already drops what it does not keep, so a regression there would leave that
// test green. `detail` is the seam: called directly, off the same typo, with
// detail true and false, the two calls must render different messages, and
// the false one must be the plain refusal even though a suggestion is one
// edit away for the taking.
func TestCheckStepArgumentSkipsSuggestionWorkPastTheCap(t *testing.T) {
	t.Parallel()

	known := map[string]struct{}{"build": {}, "deploy": {}}
	names := []string{"build", "deploy"}

	var messages []string
	report := func(_, _ int, format string, args ...any) {
		messages = append(messages, fmt.Sprintf(format, args...))
	}

	checkStepArgument(report, 1, "break biuld", "biuld", known, names, true)
	checkStepArgument(report, 2, "break biuld", "biuld", known, names, false)

	require.Len(t, messages, 2, "each call should report exactly one message")
	assert.Equal(t, `no step named "biuld": did you mean "build"?`, messages[0],
		"detail=true did not compute the suggestion it should have found")
	assert.Equal(t, `no step named "biuld"`, messages[1],
		"detail=false rendered a suggestion or the workflow's inventory — exactly "+
			"the per-line, inventory-sized work MaxScriptProblems exists to bound")
}

// TestAColumnCountsCharactersRatherThanBytes, which is what every other
// position in this repository counts ([flowfile.Diagnostic.Column]).
//
// A comment is the only place a script carries text somebody wrote in their own
// language, and a column measured in bytes would put an editor's cursor inside
// a rune.
func TestAColumnCountsCharactersRatherThanBytes(t *testing.T) {
	t.Parallel()

	assert.Equal(t, 4, columnOf("héllo", len("hél")),
		"the column counted bytes, so a multi-byte rune moved it")
}
