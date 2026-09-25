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

// TestExtendedEnvKeyIncludesCostLimit pins the acceptance criterion directly:
// the cost estimator orderedMapEnvOption installs is closed over the cost
// limit, so a memo keyed on the environment alone would let two evaluations
// under different Limits share one extended environment and enforce
// whichever limit happened to build it first. Two entries, not one
// overwritten, is what proves the key carries the cost.
func TestExtendedEnvKeyIncludesCostLimit(t *testing.T) {
	t.Parallel()

	base, err := DefaultEvaluator().ProfileEnv(CurrentProfile)
	require.NoError(t, err)

	lowExt, err := base.Extend(orderedMapEnvOption(1))
	require.NoError(t, err)
	highExt, err := base.Extend(orderedMapEnvOption(DefaultCostLimit))
	require.NoError(t, err)

	var c extendedEnvCache
	gotLow := c.put(extendedEnvKey{env: base, cost: 1}, &extendedEnvResult{env: lowExt})
	gotHigh := c.put(extendedEnvKey{env: base, cost: DefaultCostLimit}, &extendedEnvResult{env: highExt})

	assert.Same(t, lowExt, gotLow.env)
	assert.Same(t, highExt, gotHigh.env)
	assert.NotSame(t, gotLow.env, gotHigh.env, "two cost limits over the same base environment must not collide to one entry")
	assert.Equal(t, 2, c.len(), "distinct cost limits are distinct entries, not one overwriting the other")

	fromLow, ok := c.get(extendedEnvKey{env: base, cost: 1})
	require.True(t, ok)
	assert.Same(t, lowExt, fromLow.env)

	fromHigh, ok := c.get(extendedEnvKey{env: base, cost: DefaultCostLimit})
	require.True(t, ok)
	assert.Same(t, highExt, fromHigh.env)
}

// TestEvalDistinctCostLimitsDoNotShareAnExtendedEnv is the behavioral half of
// TestExtendedEnvKeyIncludesCostLimit: two [Evaluator]s built with different
// [Limits.Cost] but evaluating the same shared base environment (the shape
// an embedder handing Eval an arbitrary *cel.Env can produce) must enforce
// their own budget, not whichever one happened to extend the environment
// first.
func TestEvalDistinctCostLimitsDoNotShareAnExtendedEnv(t *testing.T) {
	t.Parallel()

	tight := NewEvaluator(WithLimits(Limits{Cost: 1, InterruptCheckFrequency: DefaultInterruptCheckFrequency}))
	generous := NewEvaluator(WithLimits(Limits{Cost: DefaultCostLimit, InterruptCheckFrequency: DefaultInterruptCheckFrequency}))

	// One base environment, shared between both evaluators — the identity
	// [Evaluator.Eval]'s exported contract allows and which is exactly the
	// case an environment-only key would collide on.
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
