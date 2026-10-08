package flowstatev1

import (
	"testing"

	"github.com/google/cel-go/cel"
	"github.com/google/cel-go/common/types"
	"github.com/google/cel-go/common/types/ref"
	"github.com/google/cel-go/common/types/traits"
	"github.com/stretchr/testify/require"
)

type invalidKeyMap struct {
	traits.Mapper
}

type preflightMap struct {
	traits.Mapper
	iterated *bool
}

func (invalidKeyMap) Size() ref.Val {
	return types.Int(1)
}

func (invalidKeyMap) Iterator() traits.Iterator {
	return types.NewRefValList(TypeAdapter, []ref.Val{types.Double(1)}).Iterator()
}

func (preflightMap) Size() ref.Val {
	return types.Int(5)
}

func (m preflightMap) Iterator() traits.Iterator {
	*m.iterated = true
	return types.NewRefValList(TypeAdapter, nil).Iterator()
}

// TestMapComprehensionsUseCanonicalKeyOrder is the direct replay regression for
// #1359. Before the evaluator wrapped comprehension ranges, cel-go delegated
// both maps below to Go's randomized map iteration and produced several answers
// in one hundred evaluations of the identical expression and inputs.
func TestMapComprehensionsUseCanonicalKeyOrder(t *testing.T) {
	libs, err := ProfileLibraries(CurrentProfile)
	require.NoError(t, err)

	tests := []struct {
		name       string
		expression string
		activation map[string]any
	}{
		{
			name:       "map literal",
			expression: `{'e': 5, 'c': 3, 'a': 1, 'd': 4, 'b': 2}.map(k, k).join('')`,
			activation: map[string]any{},
		},
		{
			name:       "activation map",
			expression: `items.map(k, k).join('')`,
			activation: map[string]any{"items": map[string]int{"e": 5, "c": 3, "a": 1, "d": 4, "b": 2}},
		},
		{
			name: "map produced by a two-variable comprehension",
			expression: `{'e': 5, 'c': 3, 'a': 1, 'd': 4, 'b': 2}` +
				`.transformMap(k, v, v).map(k, k).join('')`,
			activation: map[string]any{},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			evaluator := NewEvaluator()
			env, err := evaluator.Env(libs...)
			require.NoError(t, err)
			ast, issues := env.Parse(test.expression)
			require.NoError(t, issues.Err())
			parsed, err := cel.AstToParsedExpr(ast)
			require.NoError(t, err)

			for range 100 {
				// The same ParsedExpr pointer takes the cached-program path after
				// the first pass, matching repeated Temporal replay evaluation.
				value, err := evaluator.EvalParsed(t.Context(), env, parsed, test.activation)
				require.NoError(t, err)
				require.Equal(t, "abcde", value.Value())
			}
		})
	}
}

func TestCanonicalMapOrderingIsChargedForItsWork(t *testing.T) {
	expression := `{'e': 5, 'c': 3, 'a': 1, 'd': 4, 'b': 2}.map(k, k).join('')`
	value, err := evalInProfile(t, expression, map[string]any{})
	require.NoError(t, err)
	require.Equal(t, "abcde", value.Value())

	// Eval also accepts checked ASTs. The ordering rewrite must preserve their
	// type and overload maps rather than silently degrading them to parsed ASTs.
	libs, err := ProfileLibraries(CurrentProfile)
	require.NoError(t, err)
	evaluator := NewEvaluator()
	env, err := evaluator.Env(libs...)
	require.NoError(t, err)
	parsed, issues := env.Parse(expression)
	require.NoError(t, issues.Err())
	checked, issues := env.Check(parsed)
	require.NoError(t, issues.Err())
	value, err = evaluator.Eval(t.Context(), env, checked, map[string]any{})
	require.NoError(t, err)
	require.Equal(t, "abcde", value.Value())

	mapValue := TypeAdapter.NativeToValue(map[string]int{"a": 1, "b": 2, "c": 3, "d": 4, "e": 5})
	cost := evaluationCostEstimator.CallCost(orderedMapFunction, "", []ref.Val{mapValue}, mapValue)
	require.NotNil(t, cost)
	require.Equal(t, uint64(0), *cost)

	ordered := orderMap(mapValue)
	cost = evaluationCostEstimator.CallCost(orderedMapFunction, "", []ref.Val{mapValue}, ordered)
	require.NotNil(t, cost)
	require.Equal(t, uint64(30), *cost)

	iterated := false
	refused := orderMapWithinCost(preflightMap{iterated: &iterated}, 14)
	require.True(t, types.IsError(refused))
	require.False(t, iterated, "a sort that cannot fit the budget must fail before iteration")

	small := NewEvaluator(WithLimits(Limits{
		Cost:                    14,
		InterruptCheckFrequency: DefaultInterruptCheckFrequency,
	}))
	callerEnv, err := cel.NewEnv(cel.Variable("items", cel.DynType))
	require.NoError(t, err)
	callerAST, issues := callerEnv.Parse(`items.map(k, k)`)
	require.NoError(t, issues.Err())
	_, err = small.Eval(t.Context(), callerEnv, callerAST, map[string]any{
		"items": map[string]int{"a": 1, "b": 2, "c": 3, "d": 4, "e": 5},
	})
	require.ErrorContains(t, err, "map ordering cost 15 exceeds CEL cost limit 14")
}

func TestCanonicalMapOrderingWorksWithCallerEnvironment(t *testing.T) {
	env, err := cel.NewEnv()
	require.NoError(t, err)
	ast, issues := env.Parse(`[1].map(x, x)[0]`)
	require.NoError(t, issues.Err())

	evaluator := NewEvaluator()
	value, err := evaluator.Eval(t.Context(), env, ast, map[string]any{})
	require.NoError(t, err)
	require.Equal(t, int64(1), value.Value())

	parsed, err := cel.AstToParsedExpr(ast)
	require.NoError(t, err)
	value, err = evaluator.EvalParsed(t.Context(), env, parsed, map[string]any{})
	require.NoError(t, err)
	require.Equal(t, int64(1), value.Value())
}

func TestCanonicalMapOrderingFailsBeforeTraversal(t *testing.T) {
	value := orderMap(invalidKeyMap{})
	require.True(t, types.IsError(value))
	err, ok := value.Value().(error)
	require.True(t, ok)
	require.ErrorContains(t, err, "unsupported CEL type double")

	env, err := cel.NewEnv(cel.Variable("items", cel.DynType))
	require.NoError(t, err)
	ast, issues := env.Parse(`items.map(k, k)`)
	require.NoError(t, issues.Err())
	_, err = NewEvaluator().Eval(t.Context(), env, ast, map[string]any{"items": map[float64]int{1: 1}})
	require.ErrorContains(t, err, "unsupported CEL type double")
}

func TestCanonicalMapOrderingIsIdempotent(t *testing.T) {
	value := orderMap(TypeAdapter.NativeToValue(map[string]int{"a": 1}))
	require.IsType(t, orderedMap{}, value)
	require.Equal(t, value, orderMap(value))
}

// TestMapKeysTheTraversalCannotOrderAreRefusedAtCheck is #1859: a map literal
// whose key is provably a double, timestamp, duration, bytes or null failed
// only at run, inside the first comprehension to walk it. The checker now
// refuses it where it is written, including a map built by a comprehension,
// and leaves every orderable or undecidable key alone.
func TestMapKeysTheTraversalCannotOrderAreRefusedAtCheck(t *testing.T) {
	t.Parallel()

	env, err := NewEvaluator().ProfileEnv(CurrentProfile)
	require.NoError(t, err)

	for _, refused := range []string{
		`{1.5: 'a'}.map(k, k)`,
		`{1.5: 'a'}`,
		`[timestamp('2020-01-01T00:00:00Z')].map(t, {t: 1})`,
		`{duration('1h'): 1}`,
		`{b'x': 1}`,
		`{null: 1}`,
		`{[1]: 'a'}`,
		`{{'a': 1}: 'b'}`,
	} {
		_, issues := env.Compile(refused)
		require.Error(t, issues.Err(), refused)
		require.ErrorContains(t, issues.Err(), "map keys must be bool, int, uint or string", refused)
	}

	// A key the checker cannot type is left to the unchanged runtime refusal.
	dynEnv, err := env.Extend(cel.Variable("d", cel.DynType))
	require.NoError(t, err)
	_, issues := dynEnv.Compile(`{d: 1}.map(k, k)`)
	require.NoError(t, issues.Err())

	for _, accepted := range []string{
		`{1: 'a', 2: 'b'}.map(k, k)`,
		`{'a': 1}.filter(k, true)`,
		`{true: 1, false: 2u}`,
		`{1u: 1}`,
		`[1, 2].map(i, {i: i})`,
	} {
		_, issues := env.Compile(accepted)
		require.NoError(t, issues.Err(), accepted)
	}
}
