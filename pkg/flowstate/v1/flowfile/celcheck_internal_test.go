package flowfile

import (
	"fmt"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	expr "google.golang.org/genproto/googleapis/api/expr/v1alpha1"
)

// TestTheEnvironmentCacheIsBounded is the rule this file's own cache has to follow.
//
// The key is the set of identifiers an expression mentions, which comes out of the
// document — so without a cap, how much a long-lived language server holds is a
// choice made by whoever wrote the file it was asked to check. Past the cap nothing
// is stored and the cost is the uncached one, which is the right way for a cache to
// fail: slower, not larger.
func TestTheEnvironmentCacheIsBounded(t *testing.T) {
	// Not parallel: it fills a package-level cache and then measures its size, which
	// another test validating anything at the same time would change.
	for i := range maxCachedEnvs * 2 {
		// A distinct name per file, so each one is a cache key nothing else uses.
		src := strings.Join([]string{
			"edition: v2026.4",
			"name: check",
			"steps:",
			"  - id: say",
			"    vars:",
			fmt.Sprintf("      n%d: hello", i),
			"    log:",
			fmt.Sprintf("      message: ${n%d}", i),
			"",
		}, "\n")

		ds, err := ValidateSource([]byte(src))
		require.NoError(t, err)
		require.Empty(t, ds, "the fixture itself is wrong, so this measures nothing")
	}

	envCacheMu.RLock()
	held := len(envCache)
	envCacheMu.RUnlock()

	assert.LessOrEqual(t, held, maxCachedEnvs,
		"the environment cache grew past its bound, so a document decides how much a language server holds")

	// And it was reached, rather than the test having quietly cached almost nothing
	// — `held <= 512` is also satisfied by a cache that never stores anything.
	assert.Equal(t, maxCachedEnvs, held,
		"the bound was never reached, so this does not test the bound")
}

// TestTheUnknownFunctionAdviceNamesBothVenues: this diagnostic is advice, and
// advice is only advice where the reader can act on it.
//
// Validation runs in three places — a terminal, an editor, and
// `flowstate_validate` over MCP — and the message sent every one of them to a
// shell command. An agent that reached this by submitting a Flowfile has no
// shell; what it has is the same catalog under another name. Naming both is
// one sentence rather than a venue-aware rewrite of one.
func TestTheUnknownFunctionAdviceNamesBothVenues(t *testing.T) {
	t.Parallel()

	said := forAnAuthor("undeclared reference to 'sum' (in container '')")

	assert.Contains(t, said, `no function called "sum"`)
	assert.Contains(t, said, "flow tasks", "the terminal reader keeps their verb")
	assert.Contains(t, said, "flowstate_get_catalog",
		"and the reader with no terminal gets the tool that answers the same question")
}

// A comprehension's result sees the accumulator and nothing the loop bound, so an
// iterator spelled `event` does not shadow the delivery there.
func TestUnknownEventFieldsResultScopeIsTheAccumulators(t *testing.T) {
	t.Parallel()

	ident := func(name string) *expr.Expr {
		return &expr.Expr{ExprKind: &expr.Expr_IdentExpr{IdentExpr: &expr.Expr_Ident{Name: name}}}
	}
	list := &expr.Expr{ExprKind: &expr.Expr_ListExpr{ListExpr: &expr.Expr_CreateList{}}}
	build := func(loopStep, result *expr.Expr) *expr.Expr {
		return &expr.Expr{ExprKind: &expr.Expr_ComprehensionExpr{ComprehensionExpr: &expr.Expr_Comprehension{
			IterVar: "event", IterRange: list, AccuVar: "__result__", AccuInit: list,
			LoopCondition: ident("__result__"), LoopStep: loopStep, Result: result,
		}}}
	}
	selectNonsense := &expr.Expr{ExprKind: &expr.Expr_SelectExpr{SelectExpr: &expr.Expr_Select{
		Operand: ident("event"), Field: "nonsense",
	}}}

	// In the result the iterator is out of scope, so this is the delivery.
	require.Len(t, unknownEventFields("f", build(ident("__result__"), selectNonsense), map[string]struct{}{}), 1)
	// In the loop step the iterator shadows it, so it is the iterator's own field.
	require.Empty(t, unknownEventFields("f", build(selectNonsense, ident("__result__")), map[string]struct{}{}))
}

// The membership advice is for a list only: a string's `indexOf` is real, so a
// string overload miss keeps cel-go's own sentence, and an unrelated unknown
// function still gets the catalog referral rather than an idiom.
func TestIdiomAdviceIsScopedToWhatItAnswers(t *testing.T) {
	t.Parallel()

	const onString = "found no matching overload for 'indexOf' applied to 'string.(int)'"
	assert.Equal(t, onString, forAnAuthor(onString))

	said := forAnAuthor("undeclared reference to 'nosuchfunc' (in container '')")
	assert.NotContains(t, said, "orValue")
	assert.Contains(t, said, "flow tasks")

	const indexOnList = "found no matching overload for 'indexOf' applied to 'list(int).(int)'"
	assert.Equal(t, indexOnList, forAnAuthor(indexOnList), "a position is not a membership test")

	assert.Contains(t, forAnAuthor("found no matching overload for 'contains' applied to 'list(int).(int)'"), "`x in xs`")
}
