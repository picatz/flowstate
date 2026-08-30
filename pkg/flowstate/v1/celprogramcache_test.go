package flowstatev1

import (
	"context"
	"errors"
	"sync"
	"testing"

	"github.com/google/cel-go/cel"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	expr "google.golang.org/genproto/googleapis/api/expr/v1alpha1"
)

// parsedExprForTest parses source in e's current-profile environment and
// returns the pair EvalParsed is keyed on. Parsing here mirrors how a
// specification site is born: flowfile compiles source once, stores the
// parsed expression, and the engine replays that stored pointer.
func parsedExprForTest(t *testing.T, e *Evaluator, source string) (*cel.Env, *expr.ParsedExpr) {
	t.Helper()

	env, err := e.ProfileEnv(CurrentProfile)
	require.NoError(t, err, "building the profile environment")

	ast, issues := env.Parse(source)
	require.NoError(t, issues.Err(), "parsing %q", source)

	parsed, err := cel.AstToParsedExpr(ast)
	require.NoError(t, err, "converting %q to a parsed expression", source)
	return env, parsed
}

// TestEvalParsedCompilesAnExpressionSiteOnce pins the mechanism itself: the
// engine hands EvalParsed the same parsed-expression pointer on every
// iteration of a loop, and the second and later evaluations must reuse the
// first compilation. Without the cache this test fails on the entry count,
// which is the assertion that keeps it from passing vacuously.
func TestEvalParsedCompilesAnExpressionSiteOnce(t *testing.T) {
	t.Parallel()

	e := NewEvaluator()
	env, parsed := parsedExprForTest(t, e, `vars.n * 2`)
	ctx := context.Background()

	// Different activations through one site, because a cache that froze the
	// first answer would be worse than no cache at all.
	for _, n := range []int64{1, 5, 21} {
		out, err := e.EvalParsed(ctx, env, parsed, map[string]any{
			"vars": map[string]any{"n": n},
		})
		require.NoError(t, err)
		assert.Equal(t, n*2, out.Value(), "a cached program must still answer from the activation it was handed")
	}
	assert.Equal(t, 1, e.programs.len(),
		"three evaluations of one site are one entry; zero means the cache does not exist")
	assert.Equal(t, 1, e.programs.storeCount(),
		"three evaluations of one site are one compilation — an entry count alone cannot see "+
			"recompile-and-restore, which is exactly what a neutered lookup degrades to")

	// A second environment is a second program: the same expression under a
	// different library set may not even compile the same, so the environment
	// is half the key.
	otherEnv, err := e.Env("strings")
	require.NoError(t, err)
	_, err = e.EvalParsed(ctx, otherEnv, parsed, map[string]any{
		"vars": map[string]any{"n": int64(2)},
	})
	require.NoError(t, err)
	assert.Equal(t, 2, e.programs.storeCount(), "the same expression in a different environment is a distinct compilation")

	// Identity, not content: reparsing the same source is how a REPL asks,
	// and the fresh pointer deliberately does not collide with the spec's.
	_, reparsed := parsedExprForTest(t, e, `vars.n * 2`)
	_, err = e.EvalParsed(ctx, env, reparsed, map[string]any{
		"vars": map[string]any{"n": int64(2)},
	})
	require.NoError(t, err)
	assert.Equal(t, 3, e.programs.storeCount(), "the key is the pointer a specification owns, not the text it spells")
}

// TestProgramCacheEvictsTheLeastRecentlyUsedSite proves the bound and the
// recency rule together: the cache never exceeds its stated size, what
// falls out is the entry nothing touched, and an evicted site still answers
// afterwards — eviction costs a recompilation, never a wrong result.
func TestProgramCacheEvictsTheLeastRecentlyUsedSite(t *testing.T) {
	t.Parallel()

	e := NewEvaluator()
	env, first := parsedExprForTest(t, e, `1 + 1`)
	ctx := context.Background()
	activation := map[string]any{}

	eval := func(p *expr.ParsedExpr) {
		t.Helper()
		out, err := e.EvalParsed(ctx, env, p, activation)
		require.NoError(t, err)
		require.Equal(t, int64(2), out.Value())
	}

	eval(first)

	// Fill the cache to capacity. Each reparse of the same source is a new
	// pointer and therefore a new site, which is exactly how many distinct
	// loaded specifications would look to the cache.
	var second *expr.ParsedExpr
	for i := 1; i < DefaultProgramCacheSize; i++ {
		_, p := parsedExprForTest(t, e, `1 + 1`)
		if i == 1 {
			second = p
		}
		eval(p)
	}
	require.Equal(t, DefaultProgramCacheSize, e.programs.len())

	// Touch the oldest entry so recency, not insertion order, decides.
	eval(first)

	// One past capacity evicts exactly one, and it is the untouched one.
	_, overflow := parsedExprForTest(t, e, `1 + 1`)
	eval(overflow)

	assert.Equal(t, DefaultProgramCacheSize, e.programs.len(), "the bound is the bound; nothing grows past it")
	_, firstHeld := e.programs.entries[programKey{env: env, parsed: first}]
	assert.True(t, firstHeld, "the entry a run just used must survive an eviction")
	_, secondHeld := e.programs.entries[programKey{env: env, parsed: second}]
	assert.False(t, secondHeld, "the entry nothing touched is the one that pays")

	// An evicted site is a miss, not a casualty.
	eval(second)
	assert.Equal(t, DefaultProgramCacheSize, e.programs.len())
}

// TestCachedProgramStillEnforcesTheCostBudget is the limit half of the cache's
// contract: the budget is compiled into the program, so serving a program from
// the cache must refuse an over-budget evaluation exactly like compiling it
// fresh did. A cache that dropped the limits would be a security regression
// dressed as a speedup, which is why the second, cache-served refusal is the
// one this test exists for.
func TestCachedProgramStillEnforcesTheCostBudget(t *testing.T) {
	t.Parallel()

	e := NewEvaluator(WithLimits(Limits{Cost: 1, InterruptCheckFrequency: DefaultInterruptCheckFrequency}))
	env, parsed := parsedExprForTest(t, e, `[1, 2, 3, 4, 5].map(x, x * 2)`)
	ctx := context.Background()

	for round := 1; round <= 2; round++ {
		_, err := e.EvalParsed(ctx, env, parsed, map[string]any{})
		require.Error(t, err, "round %d: a one-unit budget cannot afford a comprehension", round)
		var exprErr *ExpressionError
		require.ErrorAs(t, err, &exprErr,
			"round %d: an exhausted budget classifies as the author's expression failing, cached or not", round)
	}
	assert.Equal(t, 1, e.programs.storeCount(),
		"the second refusal must come from the cached program, or this test proved nothing about the cache")
}

// TestCachedProgramHonorsCancellation pins the other compiled-in limit:
// interrupt checks travel with the program, so a caller's dead context stops
// a cache-served evaluation the same way it stops a fresh one.
func TestCachedProgramHonorsCancellation(t *testing.T) {
	t.Parallel()

	e := NewEvaluator(WithLimits(Limits{Cost: DefaultCostLimit, InterruptCheckFrequency: 1}))
	env, parsed := parsedExprForTest(t, e, `[1, 2, 3, 4, 5, 6, 7, 8].map(x, x * 2)`)

	_, err := e.EvalParsed(context.Background(), env, parsed, map[string]any{})
	require.NoError(t, err, "priming the cache")

	canceled, cancel := context.WithCancel(context.Background())
	cancel()
	_, err = e.EvalParsed(canceled, env, parsed, map[string]any{})
	require.Error(t, err, "a canceled context must stop a cached program")
	require.ErrorIs(t, err, context.Canceled)
	require.Equal(t, 1, e.programs.storeCount(),
		"the refused evaluation must have run the cached program, not a fresh one")
}

// TestEvalParsedIsSafeForConcurrentUse exercises the one program many
// goroutines now share, under the race detector in the package's race leg.
// Every goroutine reads its own answer back, so a cross-wired evaluation —
// shared per-eval state, a torn cache entry — surfaces as a wrong value here
// even before -race names the access.
func TestEvalParsedIsSafeForConcurrentUse(t *testing.T) {
	t.Parallel()

	e := NewEvaluator()
	env, parsed := parsedExprForTest(t, e, `vars.n * 2`)
	ctx := context.Background()

	var wg sync.WaitGroup
	for g := range 8 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := range 200 {
				n := int64(g*1000 + i)
				out, err := e.EvalParsed(ctx, env, parsed, map[string]any{
					"vars": map[string]any{"n": n},
				})
				if err != nil {
					t.Errorf("goroutine %d: %v", g, err)
					return
				}
				if got := out.Value(); got != n*2 {
					t.Errorf("goroutine %d asked for %d*2 and got %v", g, n, got)
					return
				}
			}
		}()
	}
	wg.Wait()

	assert.Equal(t, 1, e.programs.len(), "1600 evaluations of one site are one retained entry")
	// Not an equality: goroutines racing on the first miss may each compile
	// and store, by design. What is bounded is how often that can happen —
	// once per goroutine at the very worst, against 1600 evaluations.
	assert.LessOrEqual(t, e.programs.storeCount(), 8, "reuse must dominate; a store per evaluation means the cache never served")
}

// TestEvalParsedRefusesANilExpression keeps the guard that predates the cache:
// nil is a caller bug, named before any key is built from it.
func TestEvalParsedRefusesANilExpression(t *testing.T) {
	t.Parallel()

	e := NewEvaluator()
	env, err := e.ProfileEnv(CurrentProfile)
	require.NoError(t, err)

	_, err = e.EvalParsed(context.Background(), env, nil, map[string]any{})
	require.Error(t, err)
	assert.Equal(t, 0, e.programs.len(), "a refused call caches nothing")
	assert.False(t, errors.As(err, new(*ExpressionError)),
		"a nil expression is the caller's defect, not the author's expression failing")
}
