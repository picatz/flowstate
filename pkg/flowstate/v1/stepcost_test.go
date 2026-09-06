package flowstatev1

import (
	"context"
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// The cost of one step must not depend on how many ran before it (#1758).
//
// It did: `steps.<id>.<output>` was answered by building the whole `steps` root —
// every finished step, every value converted — so that CEL could select one step
// out of it, and a straight chain of N `value:` steps cost N² conversions. 4,000
// steps took 13.7 s locally where the same work written as a loop took 0.34 s.
// The root is now a [lazyStepsMap], which converts a step's outputs on the read
// that reaches it: the same value, at the cost of that one step, through the
// same qualifiers CEL applies to any map.

// chainWorkflow is n `value:` steps in a straight line, each one more than the
// step before it, the shape that pays the per-step cost and nothing else.
func chainWorkflow(n int) *Workflow {
	w := &Workflow{Name: "chain", Profile: CurrentProfile}
	w.Steps = append(w.Steps, &Node{Id: "s0", Kind: &Node_Value{Value: NewLiteral(0)}})
	for i := 1; i < n; i++ {
		w.Steps = append(w.Steps, &Node{
			Id:   fmt.Sprintf("s%d", i),
			Kind: &Node_Value{Value: NewExpr(fmt.Sprintf("steps.s%d.value + 1", i-1))},
		})
	}
	return w
}

// finishedSteps is the outputs of n steps that have already run.
func finishedSteps(n int) *Workflow_StepOutputs {
	prev := &Workflow_StepOutputs{StepValues: make(map[string]*Node_Outputs, n)}
	for i := range n {
		prev.StepValues[fmt.Sprintf("s%d", i)] = &Node_Outputs{
			NamedValues: map[string]*Value{ValueOutput: NewLiteral(i)},
		}
	}
	return prev
}

// TestReadingOneStepCostsOneStepHoweverManyRanBefore pins the mechanism where
// it lives: resolving `steps.<id>.<output>` against a scope holding 4,000
// finished steps allocates what resolving it against 10 does. Allocations
// rather than time, because they are deterministic under `-race` and on a
// loaded runner, and the regression this guards is not a slow path but a walk
// over every step — thousands of allocations where a handful are due.
func TestReadingOneStepCostsOneStepHoweverManyRanBefore(t *testing.T) {
	ctx := context.Background()
	read := NewExpr("steps.s9.value + 1").GetExpr()

	allocsAgainst := func(n int) float64 {
		activation := Activation(ctx, CurrentProfile, finishedSteps(n), nil, nil, nil, nil, true, nil, nil)
		// Once outside the measurement, so the parsed program is cached and
		// what is counted is resolution alone.
		out, err := DefaultEvaluator().EvalParsedBase(ctx, CurrentProfile, read, activation)
		require.NoError(t, err)
		require.Equal(t, int64(10), out.Value())

		return testing.AllocsPerRun(20, func() {
			if _, err := DefaultEvaluator().EvalParsedBase(ctx, CurrentProfile, read, activation); err != nil {
				t.Error(err)
			}
		})
	}

	few, many := allocsAgainst(10), allocsAgainst(4000)
	assert.LessOrEqual(t, many, few,
		"reading one step out of 4,000 finished steps allocates %.0f times where reading it out of 10 allocates %.0f; the resolution is walking every step", many, few)
}

// TestTheWholeRootIsStillOneMapOfEveryStep: the direct answer for one step
// must not change what the root itself says. `size(steps)` and a comprehension
// over it still see every finished step, and a step that has not run is still
// a missing key rather than an unresolved reference.
func TestTheWholeRootIsStillOneMapOfEveryStep(t *testing.T) {
	ctx := context.Background()
	activation := Activation(ctx, CurrentProfile, finishedSteps(3), nil, nil, nil, nil, true, nil, nil)
	eval := func(source string) (any, error) {
		out, err := DefaultEvaluator().EvalParsedBase(ctx, CurrentProfile, NewExpr(source).GetExpr(), activation)
		if err != nil {
			return nil, err
		}
		return out.Value(), nil
	}

	size, err := eval("size(steps)")
	require.NoError(t, err)
	assert.Equal(t, int64(3), size)

	total, err := eval("steps.map(id, steps[id].value).map(v, int(v)).sum()")
	require.NoError(t, err)
	assert.Equal(t, int64(3), total)

	direct, err := eval("steps.s2.value")
	require.NoError(t, err)
	assert.Equal(t, int64(2), direct)

	whole, err := eval("steps.s2 == {'value': 2}")
	require.NoError(t, err)
	assert.Equal(t, true, whole)

	_, err = eval("steps.s7.value")
	require.Error(t, err)
	assert.Contains(t, err.Error(), "s7")

	present, err := eval("has(steps.s1) && !has(steps.s7)")
	require.NoError(t, err)
	assert.Equal(t, true, present)
}

// BenchmarkLocalChain measures the local driver on a straight chain of N
// `value:` steps, reporting the cost of one step so the three sizes read
// against each other: a driver whose per-step cost is independent of run
// length reports the same ns/step at 4,000 as at 100. That is the number #1746
// needs a stable version of, and the one #1758 found growing with N.
//
// A map, not a gate, like every benchmark in this repository (see
// celeval_bench_test.go); the mechanism itself is pinned by
// [TestReadingOneStepCostsOneStepHoweverManyRanBefore].
func BenchmarkLocalChain(b *testing.B) {
	for _, n := range []int{100, 1000, 4000} {
		b.Run(fmt.Sprintf("%d", n), func(b *testing.B) {
			w := chainWorkflow(n)
			ctx := context.Background()

			b.ReportAllocs()
			b.ResetTimer()
			for range b.N {
				if _, err := Run(ctx, w); err != nil {
					b.Fatal(err)
				}
			}
			b.ReportMetric(float64(b.Elapsed().Nanoseconds())/float64(b.N)/float64(n), "ns/step")
		})
	}
}

// TestAStepThatCannotBeReadFailsOnlyTheReadThatReachesIt: the one thing the
// lazy root says differently. A secret reference stored under an output used
// to make the whole root unresolvable, so that `steps.a.value` failed as an
// unresolved reference because step b held a secret. Now b's read fails, with
// the reason, and a's read is a's read.
func TestAStepThatCannotBeReadFailsOnlyTheReadThatReachesIt(t *testing.T) {
	ctx := context.Background()
	prev := finishedSteps(1)
	prev.StepValues["b"] = &Node_Outputs{NamedValues: map[string]*Value{
		"token": {Kind: &Value_SecretRef{SecretRef: &SecretRef{Scheme: "env", Name: "TOKEN"}}},
	}}
	activation := Activation(ctx, CurrentProfile, prev, nil, nil, nil, nil, true, nil, nil)
	eval := func(source string) (any, error) {
		out, err := DefaultEvaluator().EvalParsedBase(ctx, CurrentProfile, NewExpr(source).GetExpr(), activation)
		if err != nil {
			return nil, err
		}
		return out.Value(), nil
	}

	healthy, err := eval("steps.s0.value")
	require.NoError(t, err)
	assert.Equal(t, int64(0), healthy)

	_, err = eval("steps.b.token")
	require.Error(t, err)
	assert.Contains(t, err.Error(), "a secret reference cannot be read in an expression")

	// The whole root, compared or converted, still refuses as one: an equality
	// over a map with a secret in it is not a map with a hole in it.
	_, err = eval("steps == {}")
	require.Error(t, err)
	assert.Contains(t, err.Error(), "secret reference")
}

// TestTheWholeRootNamesTheSameUnreadableStepEveryTime: with two steps that
// cannot be converted, the whole root's refusal names the first by id, not
// whichever Go's map order reached first, so the same run says the same thing
// every time it is evaluated.
func TestTheWholeRootNamesTheSameUnreadableStepEveryTime(t *testing.T) {
	ctx := context.Background()
	prev := &Workflow_StepOutputs{StepValues: map[string]*Node_Outputs{}}
	for _, id := range []string{"b", "a", "c"} {
		prev.StepValues[id] = &Node_Outputs{NamedValues: map[string]*Value{
			"token": {Kind: &Value_SecretRef{SecretRef: &SecretRef{Scheme: "env", Name: "TOKEN"}}},
		}}
	}
	activation := Activation(ctx, CurrentProfile, prev, nil, nil, nil, nil, true, nil, nil)

	for range 20 {
		_, err := DefaultEvaluator().EvalParsedBase(ctx, CurrentProfile, NewExpr("steps == {}").GetExpr(), activation)
		require.Error(t, err)
		assert.Contains(t, err.Error(), `step "a"`, "the whole root's refusal should name the first step by id")
	}
}
