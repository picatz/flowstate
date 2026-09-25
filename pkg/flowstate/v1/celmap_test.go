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
