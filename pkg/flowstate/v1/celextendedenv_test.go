package flowstatev1

import (
	"context"
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestExtendedEnvIsBuiltOncePerKey pins the mechanism #2034 asks for: Eval and
// EvalParsedWithCost both extend the same (base environment, cost limit) pair
// on every call, and the second and later calls must reuse the first
// extension rather than paying env.Extend's dispatcher rebuild again.
// env.Extend always returns a fresh *cel.Env, so pointer identity across two
// calls is only possible if the second call never ran it — the same proof
// TestEvalParsedCompilesAnExpressionSiteOnce uses for the program cache.
func TestExtendedEnvIsBuiltOncePerKey(t *testing.T) {
	t.Parallel()

	e := NewEvaluator()
	env, err := e.ProfileEnv(CurrentProfile)
	require.NoError(t, err)

	first, err := e.extendedEnvFor(env)
	require.NoError(t, err)
	second, err := e.extendedEnvFor(env)
	require.NoError(t, err)
	third, err := e.extendedEnvFor(env)
	require.NoError(t, err)

	assert.Same(t, first, second, "a repeated (env, cost) pair must return the same extended environment, not a fresh Extend per call")
	assert.Same(t, first, third)
	assert.Equal(t, 1, e.extendedEnvs.len(), "one key, one retained entry — three calls must not grow it")
}

// TestExtendedEnvKeyIncludesCostLimit pins the acceptance criterion directly
// against [Evaluator.extendedEnvFor] itself, not against the cache type in
// isolation: a key built without the cost limit would let a change to
// e.limits.Cost between two calls silently reuse the first call's extension.
// e.limits is never mutated after construction in production (WithLimits
// inside NewEvaluator is the only assignment this package makes outside this
// test), so this reaches into the unexported field to simulate the one thing
// that would make the cost field's presence in the key observable, the way
// extendedEnvKey's own doc comment explains it is meant to guard against.
func TestExtendedEnvKeyIncludesCostLimit(t *testing.T) {
	t.Parallel()

	e := NewEvaluator(WithLimits(Limits{Cost: 1, InterruptCheckFrequency: DefaultInterruptCheckFrequency}))
	env, err := e.ProfileEnv(CurrentProfile)
	require.NoError(t, err)

	low, err := e.extendedEnvFor(env)
	require.NoError(t, err)
	require.Equal(t, 1, e.extendedEnvs.len())

	e.limits.Cost = DefaultCostLimit
	high, err := e.extendedEnvFor(env)
	require.NoError(t, err)

	assert.NotSame(t, low, high, "a changed cost limit over the same base environment must build a new extension, not reuse the old one")
	assert.Equal(t, 2, e.extendedEnvs.len(), "two cost limits are two entries, not one overwriting the other")

	// Both stay reachable under their own cost, proving this is a real second
	// entry and not a coincidental pointer from a transient build.
	e.limits.Cost = 1
	stillLow, err := e.extendedEnvFor(env)
	require.NoError(t, err)
	assert.Same(t, low, stillLow)
	assert.Equal(t, 2, e.extendedEnvs.len(), "revisiting an already-keyed cost must not grow the cache further")
}

// TestEvalDistinctCostLimitsBothEnforceTheirOwnBudget is a different, weaker
// claim than TestExtendedEnvKeyIncludesCostLimit and is kept for what it
// actually proves: two [Evaluator]s, each with its own [Limits.Cost] and its
// own extendedEnvs cache (a struct field, never shared across evaluators),
// enforce their own program-level cost budget over one shared base
// environment — the shape an embedder handing Eval an arbitrary *cel.Env can
// produce — regardless of caching. This does not exercise extendedEnvKey's
// cost field at all: the overall cost meter is a *program* option
// ([Limits.programOptions]), reapplied at every env.Program call from
// whichever evaluator is calling, so this would pass even with an
// environment-only key. It is here because the property is still true and
// still worth pinning, under its own name rather than one that overclaims.
func TestEvalDistinctCostLimitsBothEnforceTheirOwnBudget(t *testing.T) {
	t.Parallel()

	tight := NewEvaluator(WithLimits(Limits{Cost: 1, InterruptCheckFrequency: DefaultInterruptCheckFrequency}))
	generous := NewEvaluator(WithLimits(Limits{Cost: DefaultCostLimit, InterruptCheckFrequency: DefaultInterruptCheckFrequency}))

	env, err := tight.ProfileEnv(CurrentProfile)
	require.NoError(t, err)

	ast, issues := env.Parse(`[1, 2, 3, 4, 5].map(x, x * 2)`)
	require.NoError(t, issues.Err())

	ctx := context.Background()

	_, err = tight.Eval(ctx, env, ast, map[string]any{})
	require.Error(t, err, "a one-unit budget must refuse a comprehension")

	_, err = generous.Eval(ctx, env, ast, map[string]any{})
	require.NoError(t, err, "a million-unit budget must allow the same comprehension over the same base environment")
}

// TestEvalParsedWithCostReusesTheExtendedEnvironment guards specifically
// against EvalParsedWithCost's own call site regressing to an inline
// env.Extend — a change none of the Eval-based tests above would catch,
// since they never call EvalParsed or EvalParsedWithCost. Two distinct
// specification sites (two parsed-expression pointers, the identity
// EvalParsed's own program cache keys on — see TestEvalParsedCompilesAnExpressionSiteOnce)
// evaluated against the same base environment must still share one extended
// environment: if EvalParsedWithCost stopped calling extendedEnvFor,
// e.extendedEnvs would stay at zero through this whole test, since nothing
// else on this path populates it.
func TestEvalParsedWithCostReusesTheExtendedEnvironment(t *testing.T) {
	t.Parallel()

	e := NewEvaluator()
	env, first := parsedExprForTest(t, e, `vars.n * 2`)
	_, second := parsedExprForTest(t, e, `vars.n * 3`)

	require.Equal(t, 0, e.extendedEnvs.len(), "nothing extended before the first evaluation")

	ctx := context.Background()
	out1, err := e.EvalParsed(ctx, env, first, map[string]any{"vars": map[string]any{"n": int64(2)}})
	require.NoError(t, err)
	assert.Equal(t, int64(4), out1.Value())

	out2, err := e.EvalParsed(ctx, env, second, map[string]any{"vars": map[string]any{"n": int64(2)}})
	require.NoError(t, err)
	assert.Equal(t, int64(6), out2.Value())

	assert.Equal(t, 1, e.extendedEnvs.len(),
		"two distinct specification sites over one base environment must share one extended environment")
	assert.Equal(t, 2, e.programs.len(), "two distinct sites are still two distinct compiled programs")
}

// TestEvalReusesTheExtendedEnvironmentAndStillEnforcesCost is the safety half
// of the memo: a cached extension must keep enforcing the cost limit it was
// built with, exactly like a freshly built one would — a cache that dropped
// the limit would be a security regression dressed as a speedup, the same
// property TestCachedProgramStillEnforcesTheCostBudget pins for the program
// cache.
func TestEvalReusesTheExtendedEnvironmentAndStillEnforcesCost(t *testing.T) {
	t.Parallel()

	e := NewEvaluator(WithLimits(Limits{Cost: 1, InterruptCheckFrequency: DefaultInterruptCheckFrequency}))
	env, err := e.ProfileEnv(CurrentProfile)
	require.NoError(t, err)

	ast, issues := env.Parse(`[1, 2, 3, 4, 5].map(x, x * 2)`)
	require.NoError(t, issues.Err())

	ctx := context.Background()
	for round := 1; round <= 2; round++ {
		_, err := e.Eval(ctx, env, ast, map[string]any{})
		require.Error(t, err, "round %d: a one-unit budget cannot afford a comprehension", round)
		var exprErr *ExpressionError
		require.ErrorAs(t, err, &exprErr,
			"round %d: the cached extended environment must still classify as an expression failure", round)
	}
	assert.Equal(t, 1, e.extendedEnvs.len(), "one (env, cost) pair retains one extended environment across repeated evaluations")
}

// TestEvalResultsAreUnaffectedByTheExtendedEnvCache proves the memo is
// transparent: the same site evaluated with different activations must keep
// answering from the activation it is handed, the way
// TestEvalParsedCompilesAnExpressionSiteOnce proves it for the program cache.
func TestEvalResultsAreUnaffectedByTheExtendedEnvCache(t *testing.T) {
	t.Parallel()

	e := NewEvaluator()
	env, err := e.ProfileEnv(CurrentProfile)
	require.NoError(t, err)

	ast, issues := env.Parse(`vars.n * 2`)
	require.NoError(t, issues.Err())

	ctx := context.Background()
	for _, n := range []int64{1, 5, 21} {
		out, err := e.Eval(ctx, env, ast, map[string]any{"vars": map[string]any{"n": n}})
		require.NoError(t, err)
		assert.Equal(t, n*2, out.Value(), "a cached extension must still answer from the activation it was handed")
	}
	assert.Equal(t, 1, e.extendedEnvs.len())
}

// TestExtendedEnvCacheIsBoundedPastItsCap covers invariant 5 directly against
// [extendedEnvCache]: Eval and EvalParsedWithCost are exported, so an
// embedder can hand them an unbounded number of distinct *cel.Env values, and
// past maxExtendedEnvs the cache must stop retaining new entries — the call
// still succeeds, uncached, exactly like [programCache]'s own over-budget
// entries and flowfile/celcheck.go's envCache once it is full.
func TestExtendedEnvCacheIsBoundedPastItsCap(t *testing.T) {
	t.Parallel()

	var c extendedEnvCache

	for i := 0; i < maxExtendedEnvs; i++ {
		env, err := buildEnv(nil)
		require.NoError(t, err)
		ext, err := env.Extend(orderedMapEnvOption(1))
		require.NoError(t, err)
		got := c.put(extendedEnvKey{env: env, cost: 1}, &extendedEnvResult{env: ext})
		require.Same(t, ext, got.env)
	}
	require.Equal(t, maxExtendedEnvs, c.len())

	overflowBase, err := buildEnv(nil)
	require.NoError(t, err)
	overflowExt, err := overflowBase.Extend(orderedMapEnvOption(1))
	require.NoError(t, err)

	got := c.put(extendedEnvKey{env: overflowBase, cost: 1}, &extendedEnvResult{env: overflowExt})
	assert.Same(t, overflowExt, got.env, "past the cap the caller's own freshly built result is used, not dropped")
	assert.Equal(t, maxExtendedEnvs, c.len(), "the bound is the bound; nothing stored past it")

	_, ok := c.get(extendedEnvKey{env: overflowBase, cost: 1})
	assert.False(t, ok, "an entry refused for being past the cap must not appear as a hit on a later call")
}

// TestEvalIsSafeForConcurrentUseOfTheExtendedEnv exercises the one extended
// environment many goroutines now share and call Program on concurrently —
// the usage [cel.Env]'s own sharedDispatcher/sync.Once is designed for, but
// new to this codebase's call pattern since every call used to build its own.
// Run under the race leg alongside TestEvalParsedIsSafeForConcurrentUse.
func TestEvalIsSafeForConcurrentUseOfTheExtendedEnv(t *testing.T) {
	t.Parallel()

	e := NewEvaluator()
	env, err := e.ProfileEnv(CurrentProfile)
	require.NoError(t, err)

	ast, issues := env.Parse(`vars.n * 2`)
	require.NoError(t, issues.Err())

	ctx := context.Background()
	var wg sync.WaitGroup
	for g := range 8 {
		wg.Go(func() {
			for i := range 200 {
				n := int64(g*1000 + i)
				out, err := e.Eval(ctx, env, ast, map[string]any{"vars": map[string]any{"n": n}})
				if err != nil {
					t.Errorf("goroutine %d: %v", g, err)
					return
				}
				if got := out.Value(); got != n*2 {
					t.Errorf("goroutine %d asked for %d*2 and got %v", g, n, got)
					return
				}
			}
		})
	}
	wg.Wait()

	assert.Equal(t, 1, e.extendedEnvs.len(), "1600 evaluations against one (env, cost) pair must share one extended environment")
}
