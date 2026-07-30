package flowfile

import (
	"strings"
	"testing"

	"github.com/google/cel-go/cel"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// Two lists here, and they answer two different questions, which is the whole
// reason both exist.
//
// celReservedIdentifiers is what cel-go will not accept as an *identifier*. It
// still matters, because a `for_each` iterator is written bare and so is still an
// identifier. It is a copy of a list cel-go does not export, and a copy with
// nothing checking it goes stale on a dependency bump nobody reviewed as a
// language change.
//
// celUnusableStepIDs is what no step may be called even under the root. Rooting
// moved step ids into field-select position, where cel-go's reserved-word check
// does not apply — so most of the first list became legal, and what is left is
// refused a level lower, by the lexer.
//
// Both are checked by asking cel-go rather than by re-copying its source, because
// a second copy has nothing to disagree with.

func TestCELReservedIdentifiersMatchTheParser(t *testing.T) {
	t.Parallel()

	env, err := cel.NewEnv()
	require.NoError(t, err)

	// reserved reports that no step could be named word, because `${word.output}`
	// would not resolve to it.
	reserved := func(word string) bool {
		ast, issues := env.Parse(word + ".output")
		if issues != nil && issues.Err() != nil {
			return true
		}
		parsed, err := cel.AstToParsedExpr(ast)
		if err != nil {
			return true
		}
		operand := parsed.GetExpr().GetSelectExpr().GetOperand()
		return operand.GetIdentExpr().GetName() != word
	}

	for _, word := range celReservedIdentifiers {
		assert.True(t, reserved(word),
			"%q is in celReservedIdentifiers but cel-go now parses it as an identifier; "+
				"a step may be named this again, so remove it from the list", word)
	}

	// The other direction, which is the one a copy cannot notice on its own: a
	// word cel-go started reserving. Probed over the vocabulary a step id is
	// plausibly written from — every lowercase word in cel-go's own grammar
	// neighbourhood, plus the ones this repo's own surfaces use — rather than over
	// every string, which is not a set anyone can enumerate.
	candidates := []string{
		// cel-go's reserved list as of writing, so the test states what it checks.
		"as", "break", "const", "continue", "else", "false", "for", "function",
		"if", "import", "in", "let", "loop", "namespace", "null", "package",
		"return", "true", "var", "void", "while",

		// Words a future cel-go might plausibly take, and words this DSL uses, so
		// that a collision between the two is found here rather than by an author.
		"and", "or", "not", "is", "this", "self", "super", "new", "delete",
		"switch", "case", "default", "do", "try", "catch", "throw", "yield",
		"async", "await", "match", "when", "then", "type", "enum", "struct",
		"steps", "inputs", "outputs", "vars", "run", "now", "secret", "task",
		"echo", "http", "printf", "cel", "sleep", "retry", "timeout", "id",
	}
	for _, word := range candidates {
		if !reserved(word) {
			continue
		}
		assert.Contains(t, celReservedIdentifiers, word,
			"cel-go refuses %q as an identifier and celReservedIdentifiers does not list it; "+
				"a step with that id would compile and then every ${%s.…} would fail to parse",
			word, word)
	}
}

// TestCELWordsUnusableAsStepIDs derives celUnusableStepIDs from cel-go, and is
// the test that caught the list being wrong.
//
// The probe has to ask the question a step id actually asks, which rooting
// changed: not "is this a legal identifier" but "does `${steps.<id>.result}`
// parse, and resolve to a select on the root". Asking the old question said
// eighteen words became legal. Asking this one says seventeen — `in` is an
// operator token, so `steps.in` is a syntax error in the grammar, exactly like
// the three literals.
//
// Getting that wrong is not harmless: the step compiles and every reference to it
// then fails to *parse*, so the author gets a syntax error pointing at an
// expression instead of a diagnostic pointing at the id — which is the failure
// the whole check exists to prevent.
func TestCELWordsUnusableAsStepIDs(t *testing.T) {
	t.Parallel()

	env, err := cel.NewEnv()
	require.NoError(t, err)

	// usable reports whether a step could be named word, by asking whether the
	// reference an author would write resolves to the root.
	usable := func(word string) bool {
		ast, issues := env.Parse("steps." + word + ".result")
		if issues != nil && issues.Err() != nil {
			return false
		}
		parsed, err := cel.AstToParsedExpr(ast)
		if err != nil {
			return false
		}
		inner := parsed.GetExpr().GetSelectExpr().GetOperand().GetSelectExpr()
		return inner != nil && inner.GetOperand().GetIdentExpr().GetName() == "steps"
	}

	for _, word := range celUnusableStepIDs {
		assert.False(t, usable(word),
			"%q is refused as a step id but cel-go now parses ${steps.%s.result}; remove it from celUnusableStepIDs",
			word, word)
	}

	// The other direction, over every word cel-go reserves plus the vocabulary a
	// step id is plausibly written from. A word the lexer refuses and this list
	// does not carry is a step that compiles and can never be referenced.
	candidates := append([]string{}, celReservedIdentifiers...)
	candidates = append(candidates,
		"and", "or", "not", "is", "this", "self", "super", "new", "delete",
		"switch", "case", "default", "do", "try", "catch", "throw", "yield",
		"steps", "inputs", "outputs", "vars", "run", "now", "secret", "task",
	)
	for _, word := range candidates {
		if usable(word) {
			continue
		}
		assert.Contains(t, celUnusableStepIDs, word,
			"cel-go cannot parse ${steps.%s.result} and celUnusableStepIDs does not list %q; "+
				"a step with that id would compile and then every reference to it would fail to parse",
			word, word)
	}
}

// TestMostReservedWordsBecameLegalStepIDs states the size of what rooting bought,
// so that a change quietly taking it back has something to fail.
func TestMostReservedWordsBecameLegalStepIDs(t *testing.T) {
	t.Parallel()

	var legal []string
	for _, word := range celReservedIdentifiers {
		if !slicesContains(celUnusableStepIDs, word) {
			legal = append(legal, word)
		}
	}
	assert.Len(t, legal, 17,
		"seventeen of cel-go's twenty-one reserved words are usable as step ids under the root; got %v", legal)

	// And one of them, end to end, because a count is not a step anyone can write.
	// Referenced as well as declared: the failure this guards against is a step that
	// compiles under a reserved id and whose every `${steps.<id>....}` then fails to
	// parse, which only a reference can see.
	ds, err := ValidateSource([]byte(
		"edition: v2026.2\nname: t\nsteps:\n  - id: loop\n    http:\n      url: https://example.com\n" +
			"  - id: after\n    log:\n      message: ${steps.loop.body}\n"))
	require.NoError(t, err)
	assert.Empty(t, ds, "a step called `loop` must be usable now")
}

// TestUnusableStepIDIsReportedOnTheID covers the diagnostic itself.
func TestUnusableStepIDIsReportedOnTheID(t *testing.T) {
	t.Parallel()

	for _, word := range celUnusableStepIDs {
		ds, err := ValidateSource([]byte(
			"edition: v2026.2\nname: t\nsteps:\n  - id: \"" + word + "\"\n    log:\n      message: hi\n"))
		require.NoError(t, err)
		require.NotEmpty(t, ds, "a step called %q must be refused", word)

		rendered := ds.Error()
		assert.Contains(t, rendered, "choose another id",
			"the diagnostic has to say what to do, not only what is wrong")
		assert.True(t, strings.Contains(rendered, word), "it has to name the id; got %q", rendered)
	}
}

// slicesContains is spelled out rather than imported so this file states its own
// membership test alongside the two lists it is about.
func slicesContains(haystack []string, needle string) bool {
	for _, s := range haystack {
		if s == needle {
			return true
		}
	}
	return false
}
