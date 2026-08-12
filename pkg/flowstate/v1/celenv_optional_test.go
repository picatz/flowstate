package flowstatev1

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/google/cel-go/cel"
)

// TestOptionalTraversalCostMetered verifies — rather than assumes — that
// optional traversal is metered by the CEL cost accounting the way an ordinary
// select is, so a long `.?` chain spends [DefaultCostLimit] budget instead of
// walking for free (issue #412: the edition that documents `.?` must not open
// an unmetered path).
//
// Two directions, because each catches what the other cannot. A tiny budget
// must be the bound that trips — named, per TestEvaluatorCostLimit's reasoning:
// "some error happened" is satisfied by a typo. And the actual costs of the two
// spellings at the same depth must be within a small factor of each other,
// because "the chain trips *a* limit" would also be satisfied by an accounting
// that charged optional traversal at a millionth of a select's rate.
func TestOptionalTraversalCostMetered(t *testing.T) {
	t.Parallel()

	const depth = 200

	// A value deep enough that every step of either chain does real work.
	var nested any = true
	for range depth {
		nested = map[string]any{"k": nested}
	}
	activation := map[string]any{"a": nested}

	plain := "a" + strings.Repeat(".k", depth)
	optional := "a" + strings.Repeat(".?k", depth) + ".orValue(false)"

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	// Direction one: a budget far below the chain's length is spent and trips,
	// for both spellings, and the meter is the bound that says so.
	small := NewEvaluator(WithLimits(Limits{Cost: 20, InterruptCheckFrequency: DefaultInterruptCheckFrequency}))
	for name, expr := range map[string]string{
		"plain select chain":       plain,
		"optional traversal chain": optional,
	} {
		_, err := small.EvalString(ctx, expr, []string{"optional"}, activation)
		if err == nil {
			t.Fatalf("%s: expected the cost meter to refuse a %d-step chain under a budget of 20", name, depth)
		}
		if !strings.Contains(err.Error(), "actual cost limit exceeded") {
			t.Errorf("%s: refused by the wrong bound: %v", name, err)
		}
	}

	// Direction two: under the default budget both evaluate, and the optional
	// chain's spend is the same order as the plain one's.
	plainCost := actualCost(t, ctx, plain, activation)
	optionalCost := actualCost(t, ctx, optional, activation)

	if plainCost < depth/2 {
		t.Fatalf("plain chain cost %d does not scale with its %d steps; the comparison below would prove nothing",
			plainCost, depth)
	}
	if optionalCost < plainCost/4 {
		t.Errorf("optional traversal is metered at under a quarter of a select's rate (optional %d, plain %d); "+
			"a long `.?` chain would outrun the budget the profile promises", optionalCost, plainCost)
	}
	if optionalCost > plainCost*16 {
		t.Errorf("optional traversal costs over sixteen selects per step (optional %d, plain %d); "+
			"ordinary files would trip the limit on ordinary reads", optionalCost, plainCost)
	}
}

// actualCost evaluates one expression in the profile's environment under
// [DefaultCostLimit] and returns what the meter recorded it spending.
func actualCost(t *testing.T, ctx context.Context, expr string, activation map[string]any) uint64 {
	t.Helper()

	env, err := DefaultEvaluator().ProfileEnv(CurrentProfile)
	if err != nil {
		t.Fatal(err)
	}
	ast, issues := env.Parse(expr)
	if issues != nil && issues.Err() != nil {
		t.Fatalf("parse %q: %v", expr, issues.Err())
	}
	prg, err := env.Program(ast, cel.CostLimit(DefaultCostLimit))
	if err != nil {
		t.Fatal(err)
	}
	_, details, err := prg.ContextEval(ctx, activation)
	if err != nil {
		t.Fatalf("evaluate under DefaultCostLimit: %v", err)
	}
	cost := details.ActualCost()
	if cost == nil {
		t.Fatal("cost tracking was not enabled, so this test cannot see the meter")
	}
	return *cost
}
