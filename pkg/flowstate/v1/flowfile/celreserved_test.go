package flowfile

import (
	"strings"
	"testing"

	"github.com/google/cel-go/cel"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// celReservedIdentifiers is a copy of a list cel-go does not export, and a copy
// with nothing checking it is a copy that goes stale on a dependency bump nobody
// reviewed as a language change.
//
// The failure it exists to prevent is quiet in both directions. A word cel-go
// adds and this list lacks means a step id compiles and then every `${id.…}`
// referencing it fails to parse, with the diagnostic pointing at the expression
// rather than at the id that caused it. A word cel-go drops and this list keeps
// means an id refused for no reason the author can see.
//
// Checked by asking cel-go rather than by re-copying its source, because a second
// copy has nothing to disagree with.
//
// The probe took two tries to get right, and the wrong ones are instructive
// because both look like they work.
//
// Parsing the bare word answers a different question: `false` parses perfectly as
// a boolean *literal* while being unusable as a name. Parsing `word + ".output"`
// — the shape a step id is actually written in — is closer and still wrong, for
// the same reason one level up: `true.output` parses too, as a field select on
// the literal `true`. It compiles and it does not name the step.
//
// The property is therefore neither "does it parse" nor "does the reference
// parse", but *does the reference resolve to an identifier by that name*. So the
// probe reads the AST: `word.output` must come back as a select whose operand is
// an Ident spelling `word`. A word that fails this is a word no step can be named,
// whether cel-go refuses it in identifier position (parser.go's VisitIdent) or the
// lexer took it for a literal first.
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

// TestCELReservedIdentifiersAreRefusedAsStepIDs closes the loop: the list is only
// worth keeping current if something reads it.
//
// One word stands for all of them — the list membership is what the test above
// pins — but the path from "in the list" to "reported with a usable message" has
// to be walked at least once, or the list could be correct and unused.
func TestCELReservedIdentifiersAreRefusedAsStepIDs(t *testing.T) {
	t.Parallel()

	ds, err := ValidateSource([]byte("name: t\nsteps:\n  - id: in\n    echo:\n      message: hi\n"))
	require.NoError(t, err, "the document is valid YAML and compiles; the id is a semantic problem")
	require.NotEmpty(t, ds)

	rendered := ds.Error()
	assert.Contains(t, rendered, "CEL reserved word")
	assert.Contains(t, rendered, "choose another id",
		"the diagnostic has to say what to do, not only what is wrong")
	assert.True(t, strings.Contains(rendered, `"in"`),
		"the diagnostic has to name the id at fault; got %q", rendered)
}
