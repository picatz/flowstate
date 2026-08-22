package flowstatev1

import (
	"context"
	"runtime"
	"strings"
	"testing"

	"github.com/google/cel-go/cel"
	"github.com/stretchr/testify/require"
)

// The activation is the attacker's half of every expression on this path, so
// these tests hold a *fixed* expression — one an author would plausibly write —
// and vary only the size of the data bound to it. That is the shape of the
// attack #847 describes: `items.map(i, prefix + body + suffix)` is unremarkable
// until `body` is a webhook payload.

// celCostTestEnv returns the profile environment extended with the variables
// these tests bind, along with a parsed (never checked) AST for expr.
//
// Parsed rather than compiled on purpose: a compiled specification carries an
// `expr.ParsedExpr`, so every expression this system evaluates in production
// reaches the runtime with its overload IDs unresolved. A test that compiled
// would be testing the one path flowstate does not take, and would have passed
// against the defect these tests exist to pin.
func celCostTestEnv(t *testing.T, expr string) (*cel.Env, *cel.Ast) {
	t.Helper()

	env, err := DefaultEvaluator().ProfileEnv(CurrentProfile)
	require.NoError(t, err)

	env, err = env.Extend(
		cel.Variable("body", cel.StringType),
		cel.Variable("items", cel.ListType(cel.IntType)),
	)
	require.NoError(t, err)

	ast, issues := env.Parse(expr)
	require.NoError(t, issues.Err())

	return env, ast
}

// evalWithActivation evaluates expr through [Evaluator.Eval] — the one place
// both execution drivers build a program, and so the only place a bound added
// here is worth anything — against a body of the given size.
func evalWithActivation(t *testing.T, expr string, bodySize, itemCount int) (allocatedMiB uint64, err error) {
	t.Helper()

	env, ast := celCostTestEnv(t, expr)

	items := make([]any, itemCount)
	for i := range items {
		items[i] = int64(i)
	}
	activation := map[string]any{
		"body":  strings.Repeat("a", bodySize),
		"items": items,
	}

	var before, after runtime.MemStats
	runtime.ReadMemStats(&before)
	_, err = DefaultEvaluator().Eval(context.Background(), env, ast, activation)
	runtime.ReadMemStats(&after)

	return (after.TotalAlloc - before.TotalAlloc) / (1 << 20), err
}

// TestCELCostPricesBytesProducedNotOperationsPerformed is #847's measurement,
// inverted into an assertion.
//
// One expression, two activations differing only in the size of a string the
// expression does not choose. Before the estimator in celcost.go both cost
// 1,912 units of a 1,000,000-unit budget — 0.19% — while allocating 172 MiB in
// the large case and nothing in the small one. The budget could not tell them
// apart because nothing on this path priced the bytes.
func TestCELCostPricesBytesProducedNotOperationsPerformed(t *testing.T) {
	const expr = `items.map(i, body + body + body + body)`

	t.Run("a small activation is shaped without complaint", func(t *testing.T) {
		_, err := evalWithActivation(t, expr, 10, 100)
		require.NoError(t, err,
			"a comprehension over 100 ten-character strings is ordinary data shaping and must stay affordable")
	})

	t.Run("the same expression over an admitted-size body is refused", func(t *testing.T) {
		allocated, err := evalWithActivation(t, expr, 200_000, 100)

		// Fail closed: refused, not truncated, not silently returning a
		// partial answer.
		require.Error(t, err,
			"an expression multiplying a 200,000-character activation string 400-fold must be refused")
		require.ErrorContains(t, err, "cost limit exceeded",
			"the refusal must come from the cost budget, not from a deadline or an allocator giving up: "+
				"a bound that only fires when the machine is already in trouble is not a bound")

		// The bound is reached *early*, which is the whole point of pricing
		// the resource that multiplies. The unpriced version of this
		// evaluation allocated 172 MiB and returned successfully.
		require.Less(t, allocated, uint64(64),
			"the evaluation allocated %d MiB before the budget stopped it; it is meant to be stopped "+
				"long before the 172 MiB the unpriced version completed", allocated)
	})
}

// TestCELCostBoundIsReachedByGrowthRatherThanSize distinguishes the two things
// a byte-priced budget must tell apart, because a bound that refuses any large
// string is not a bound, it is a smaller input limit wearing one's clothes.
//
// A single concatenation of a large body is legitimate — it produces one string
// roughly the size of an input already admitted at its own boundary. The
// refusal has to be for the *product*.
func TestCELCostBoundIsReachedByGrowthRatherThanSize(t *testing.T) {
	t.Run("one concatenation of a large body is allowed", func(t *testing.T) {
		_, err := evalWithActivation(t, `body + body`, 200_000, 1)
		require.NoError(t, err,
			"concatenating an admitted-size body once produces one string of admitted size; refusing it "+
				"would make the bound an input limit rather than a growth limit")
	})

	t.Run("multiplying that same body is refused", func(t *testing.T) {
		_, err := evalWithActivation(t, `items.map(i, body + body)`, 200_000, 100)
		require.Error(t, err,
			"the same concatenation performed once per item builds 40 MB and must be refused")
		require.ErrorContains(t, err, "cost limit exceeded")
	})
}

// TestCELCostRefusesASingleAmplifyingCallBeforeItAllocates is the review
// finding from PR #885 (thread r3836512090), kept as the expression Codex
// wrote.
//
// The first version of this file charged only for the bytes a call *produced*,
// and an estimator runs after the call that produced them. That bounds a
// multiplication spread over many calls and does nothing about one call that
// allocates everything at once. `lists.range(10000).map(i, body).join("")` is
// that call: 10,000 appends cost roughly 10,000 units — nothing against a
// 1,000,000-unit budget — and then a single `join` materialises 2 GB.
//
// Measured under GOMEMLIMIT=1GiB before the fix: refused, but only after
// **11,882 MiB of allocation, 6,215 MiB of heap and 16.4 seconds**. The budget
// did answer, and it answered long after the allocator had done the damage,
// which is the same "bound that only fires once the machine is in trouble" this
// file's other tests refuse to accept. After the fix: refused in 11 ms having
// allocated nothing measurable.
//
// The fix is that a list is charged for the bytes entering it, so the budget is
// spent during the comprehension that accumulates the 2 GB rather than at the
// call that concatenates it. That is why this test asserts an allocation
// ceiling rather than only an error: an assertion that this expression is
// refused passed before the fix too.
func TestCELCostRefusesASingleAmplifyingCallBeforeItAllocates(t *testing.T) {
	const expr = `lists.range(10000).map(i, body).join("")`

	allocated, err := evalWithActivation(t, expr, 200_000, 0)

	require.Error(t, err, "an expression materialising 2 GB from one 200,000-character body must be refused")
	require.ErrorContains(t, err, "cost limit exceeded")

	require.Less(t, allocated, uint64(64),
		"the evaluation allocated %d MiB; charging only for bytes produced let this reach 11,882 MiB "+
			"before the budget noticed, because the estimator runs after the call that allocates. "+
			"A regression here means the charge has moved back after the allocation it is meant to prevent",
		allocated)
}

// TestCELCostChargesBytesEnteringAListRatherThanItsLength is the mechanism the
// test above depends on, asserted directly so a failure says which half broke.
//
// Length is not the resource. A comprehension building a long list of small
// strings is ordinary and must stay affordable; one building a short list of
// large strings is the amplifier, because it is the bytes that a later `join`
// or comparison has to materialise.
func TestCELCostChargesBytesEnteringAListRatherThanItsLength(t *testing.T) {
	t.Run("a long list of small strings is affordable", func(t *testing.T) {
		_, err := evalWithActivation(t, `items.map(i, body)`, 10, 10_000)
		require.NoError(t, err,
			"accumulating 10,000 ten-character strings is 100,000 characters in total and must not be "+
				"refused; charging by element count rather than by bytes would refuse it")
	})

	t.Run("a short list of large strings is refused", func(t *testing.T) {
		_, err := evalWithActivation(t, `items.map(i, body)`, 200_000, 1_000)
		require.Error(t, err,
			"accumulating 1,000 copies of a 200,000-character body is 200 MB and must be refused while "+
				"it is still being accumulated")
		require.ErrorContains(t, err, "cost limit exceeded")
	})
}

// TestCELCostLeavesNonStringResultsToCELGo pins the estimator's restraint.
//
// A returned cost replaces cel-go's own decision rather than adding to it
// (cel-go v0.31.0, interpreter/runtimecost.go:269), so pricing a call this
// system has nothing to say about would mean maintaining a second copy of
// cel-go's cost table — and a copy of a cost table is a thing that drifts. The
// estimator answers only for calls producing a string or bytes; everything else
// must be priced exactly as it was before this file existed.
func TestCELCostLeavesNonStringResultsToCELGo(t *testing.T) {
	env, ast := celCostTestEnv(t, `items.map(i, i)`)

	items := make([]any, 100)
	for i := range items {
		items[i] = int64(i)
	}

	prg, err := env.Program(ast, cel.CostLimit(DefaultCostLimit), cel.CostTracking(evaluationCostEstimator))
	require.NoError(t, err)

	_, details, err := prg.ContextEval(context.Background(),
		map[string]any{"body": "", "items": items})
	require.NoError(t, err)
	require.NotNil(t, details.ActualCost())

	// The measured cost of this expression before the estimator existed. It
	// produces no strings, so the estimator declines every call in it and the
	// number must not move.
	require.Equal(t, uint64(1312), *details.ActualCost(),
		"an expression producing no strings must be priced exactly as cel-go prices it; a change here "+
			"means the estimator has started answering for calls it was meant to decline")
}

// TestCELCostAgreesWithCELGosCheckedPricing is the parity claim celcost.go
// rests on, and the reason it prices the result rather than the operands.
//
// cel-go already prices `AddString` correctly — by the size of both operands —
// but only for a checked AST, because the branch that does it switches on an
// overload ID the checker resolves. Flowstate never checks. Pricing the result
// reaches the same number without copying the switch that computes it: the
// result of `a + b` is `size(a) + size(b)` characters by construction.
//
// If these two diverge, the estimator has stopped deriving cel-go's answer and
// started guessing at it.
func TestCELCostAgreesWithCELGosCheckedPricing(t *testing.T) {
	const expr = `body + body`

	env, err := DefaultEvaluator().ProfileEnv(CurrentProfile)
	require.NoError(t, err)
	env, err = env.Extend(cel.Variable("body", cel.StringType))
	require.NoError(t, err)

	activation := map[string]any{"body": strings.Repeat("a", 200_000)}

	costOf := func(t *testing.T, ast *cel.Ast, opts ...cel.ProgramOption) uint64 {
		t.Helper()
		prg, err := env.Program(ast, append([]cel.ProgramOption{cel.CostLimit(DefaultCostLimit)}, opts...)...)
		require.NoError(t, err)
		_, details, err := prg.ContextEval(context.Background(), activation)
		require.NoError(t, err)
		require.NotNil(t, details.ActualCost())
		return *details.ActualCost()
	}

	checked, issues := env.Compile(expr)
	require.NoError(t, issues.Err())

	parsedAST, issues := env.Parse(expr)
	require.NoError(t, issues.Err())

	// cel-go's own size-aware pricing, available only because this AST was
	// checked.
	celGoChecked := costOf(t, checked)

	// The same expression as flowstate actually evaluates it: parsed, with the
	// estimator supplying the sizing the missing overload ID would have.
	flowstateParsed := costOf(t, parsedAST, cel.CostTracking(evaluationCostEstimator))

	require.Equal(t, celGoChecked, flowstateParsed,
		"pricing a concatenation by the bytes it produced must land on the same number cel-go reaches "+
			"by pricing its operands; a divergence means one of the two is being reimplemented")

	// And the defect itself, so this test fails if the estimator is ever
	// unwired: the parsed AST without it is priced as a single operation.
	unpriced := costOf(t, parsedAST)
	require.Less(t, unpriced, celGoChecked/1000,
		"without the estimator a parsed AST is priced as though it moved no bytes, which is the defect "+
			"#847 reports; this assertion exists so that removing the estimator fails a test rather than "+
			"quietly restoring the hole")
}
