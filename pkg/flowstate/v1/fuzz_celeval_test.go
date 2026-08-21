package flowstatev1_test

import (
	"context"
	"encoding/json"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	v1 "github.com/picatz/flowstate/pkg/flowstate/v1"
)

// FuzzCELEvaluate fuzzes CEL *evaluation* — an expression and the data it runs
// against, both chosen by the fuzzer — through [v1.Evaluator], under the limits
// every evaluation in this system runs under ([v1.DefaultLimits]).
//
// This is picatz/flowstate#403's item 2, and it is the half [FuzzCELCompile]
// (flowfile/fuzz_cel_test.go) does not have. That target proves *compilation* is
// bounded: bytes reaching cel-go's parser and type checker. Compilation is where
// a stack overflow lives; it is not where [v1.DefaultCostLimit] is enforced. The
// cost budget is a *runtime* accumulation, spent by the interpreter as it walks
// the program, and what it is spent on is decided as much by the activation's
// data as by the expression's text — `items.map(i, items.map(j, i + j))` costs
// what `items` happens to hold. So the two halves are fuzzed together here, as
// [FuzzWebhookEventBinding] fuzzes only the data half against one fixed
// expression.
//
// # The bound under test, and the direction it is tested in
//
// CLAUDE.md: "a bound nothing reaches is a bound nothing tests." The cost limit
// is asserted in both directions, and neither assertion lives entirely in the
// fuzz body, because a fuzzer has no oracle for what an arbitrary expression
// should cost:
//
//   - Not exceeded: the fuzz body gives each evaluation a context whose deadline
//     is far past what a million cost units can honestly take, and fails if the
//     deadline is what stopped it. That is the failure a broken cost limit
//     produces — evaluation that runs until something else stops it — and it is
//     the only shape of "unbounded" this target can observe from the outside.
//     Reached: [TestCELCostLimitStopsAnExpensiveEvaluation] below evaluates
//     expressions measured to cost more than the budget and requires each to be
//     refused for cost, so the budget is known to be a thing that fires rather
//     than a number in a struct.
//
// The deadline is deliberately generous (see [celFuzzEvalDeadline]) rather than
// tight. A tight one measures the machine, and this repository's own history has
// a bounded-wait test failing under contention on a loaded box and passing in
// isolation (#431). What is asserted is "something other than the cost limit had
// to stop this", which a generous deadline answers just as well.
//
// # The resource the cost limit does not bound
//
// Measured while writing this, and reported as picatz/flowstate#847 rather than
// asserted here: cel-go's *runtime* cost tracker — the one [cel.CostLimit]
// spends, as distinct from the size-aware static estimator — prices string
// concatenation at a flat rate independent of operand size. `s + s` costs 3
// units whether `s` holds ten bytes or two hundred thousand, and a
// hundred-iteration comprehension doing three concatenations per element costs
// 1921 units either way. So [v1.DefaultCostLimit] bounds how many operations an
// evaluation performs and not how many bytes it moves, and an activation whose
// strings an outside party chose — a webhook body, an HTTP task's decoded
// response — is the half of the product this system does not price.
//
// That is exactly CLAUDE.md's rule about asking which resource the attacker
// controls, so it is named here rather than left implicit: this target's
// deadline assertion observes *time*, the cost limit bounds *operations*, and
// memory is bounded by neither. Nothing here pins the current behaviour as
// correct, because it is not.
//
// # What is not asserted
//
// The result. There is no oracle for what an arbitrary expression over arbitrary
// data should evaluate to, and an error — a parse failure, an unknown identifier,
// a missing overload, an overflow, a division by zero, the cost limit itself — is
// an ordinary and expected answer for nearly every input a fuzzer builds. What is
// asserted is that producing one costs no more than a bounded evaluation should.
func FuzzCELEvaluate(f *testing.F) {
	for _, seed := range celEvalSeeds {
		f.Add(seed.expr, seed.activation)
	}

	evaluator := v1.NewEvaluator()
	env, err := evaluator.ProfileEnv(v1.CurrentProfile)
	require.NoError(f, err)

	f.Fuzz(func(t *testing.T, expression, activationJSON string) {
		// The activation is JSON rather than a second expression because that is
		// the shape untrusted data arrives in on every path that reaches an
		// evaluation here — a webhook body, a signal payload, an HTTP task's
		// decoded response. A blob that is not a JSON object is not an
		// activation, and skipping is honest: the fuzzer keeps the input in its
		// corpus only if it reached new coverage, so nothing is lost by
		// declining to invent a meaning for it.
		var activation map[string]any
		if err := json.Unmarshal([]byte(activationJSON), &activation); err != nil {
			t.Skip()
		}

		ast, issues := env.Parse(expression)
		if issues != nil && issues.Err() != nil {
			// Compilation's own refusal, which is [FuzzCELCompile]'s subject and
			// not this one's.
			return
		}

		ctx, cancel := context.WithTimeout(context.Background(), celFuzzEvalDeadline)
		defer cancel()

		start := time.Now()
		_, err := evaluator.Eval(ctx, env, ast, activation)
		elapsed := time.Since(start)

		// The assertion. An evaluation the cost limit was going to refuse is
		// refused for cost; one it was going to allow finishes. Neither of those
		// is the context expiring, so a context error means the cost limit did
		// not bound this evaluation and nothing else in this system would have
		// either — the worker running it would have been holding a goroutine for
		// as long as the expression felt like.
		//
		// Checked on ctx rather than by comparing elapsed against the deadline,
		// because a goroutine descheduled for the whole of its evaluation on a
		// contended machine can take longer than the deadline without ever
		// having been *stopped* by it. elapsed is reported rather than asserted
		// on, so a failure says how long it actually ran.
		require.NoError(t, ctx.Err(),
			"evaluation ran past its %s backstop (%s elapsed) rather than being stopped by the %d-unit cost limit; expression:\n%s\nactivation:\n%s\nevaluation error: %v",
			celFuzzEvalDeadline, elapsed, v1.DefaultCostLimit, expression, activationJSON, err)
	})
}

// celFuzzEvalDeadline is how long [FuzzCELEvaluate] lets one evaluation run
// before calling the cost limit broken rather than the evaluation expensive.
//
// A million cost units is milliseconds of interpretation. A minute is roughly
// five orders of magnitude of headroom, which is the point: this is a backstop
// against an *unbounded* evaluation, not a performance budget, and a number
// tight enough to measure the machine is a number that fails on a loaded one
// (#431). Thirty seconds — the value the language server's tests settled on for
// the same reason — was not enough here: under `-race`, on a box carrying other
// work, an expression that overshot the budget by eight times spent longer than
// that reaching the point where the accounting refused it. The case was made
// cheaper as well, since a slow case is the more useful half of that fix, but
// the backstop keeps the margin the lesson asks for.
const celFuzzEvalDeadline = 60 * time.Second

// A celEvalSeed is one expression and the activation it runs against.
type celEvalSeed struct {
	expr       string
	activation string
}

// celEvalSeeds are the pairs the fuzzer explores outward from.
//
// Chosen so that both halves of the input have somewhere to go. Seeding only
// cheap expressions means the fuzzer spends its budget on syntax errors and
// never reaches the interpreter's expensive machinery; seeding only cost bombs
// means every mutation is refused before it computes anything. Both are here,
// alongside the shapes where cost is a product of the *data* rather than of the
// text — which is the pair this target exists for, and which neither
// [FuzzCELCompile] nor [FuzzWebhookEventBinding] reaches.
var celEvalSeeds = []celEvalSeed{
	// The ordinary cases: a literal, a variable read, a member read, a call from
	// each of the extension libraries the current profile admits.
	{`1 + 1`, `{}`},
	{`name`, `{"name":"flowstate"}`},
	{`steps.a.body.items[0].id`, `{"steps":{"a":{"body":{"items":[{"id":"x"}]}}}}`},
	{`has(steps.a.error)`, `{"steps":{"a":{}}}`},
	{`size(items)`, `{"items":[1,2,3]}`},
	{`"a-b-c".split("-")`, `{}`},
	{`math.greatest(1, 2, 3)`, `{}`},
	{`[1, 2, 3].map(i, i * 2)`, `{}`},
	{`sets.contains([1, 2, 3], [1])`, `{}`},
	{`"x".matches("^[a-z]+$")`, `{}`},
	{`base64.encode(b"hi")`, `{}`},
	{`optional.of(1).orValue(2)`, `{}`},
	{`cel.bind(x, 1, x + x)`, `{}`},
	{`duration('1h') > duration('30m')`, `{}`},

	// Cost as a function of the data, not of the text. Each of these is a short,
	// entirely reasonable expression whose price the activation sets — the shape
	// an attacker controls when the expression is an author's and the data is
	// theirs. The activation here is small; the fuzzer's job is to grow it.
	{`items.map(i, items.map(j, i + j))`, `{"items":[1,2,3,4]}`},
	{`items.filter(i, items.exists(j, i == j))`, `{"items":[1,2,3,4]}`},
	{`items.map(i, i + i + i + i)`, `{"items":["ab","cd"]}`},
	{`body.matches(pattern)`, `{"body":"aaaaaaaaaaaaaaaaaaaaaaaa","pattern":"^(a+)+$"}`},
	{`size(a + b)`, `{"a":[1,2,3],"b":[4,5,6]}`},

	// Cost as a function of the text, which the budget must refuse rather than
	// spend a worker on. Written over literals so the seed carries its own
	// weight and does not depend on the activation. Small here on purpose —
	// [TestCELCostLimitStopsAnExpensiveEvaluation] holds the versions that
	// actually exceed the budget, because a seed that is always refused teaches
	// the fuzzer nothing about what happens past the refusal.
	{`[1,2].map(i, [1,2].map(j, [1,2].map(k, i + j + k)))`, `{}`},
	{`[1,2,3].filter(i, [1,2,3].exists(j, j == 9))`, `{}`},

	// Arithmetic edges, which are errors rather than crashes and which a fuzzer
	// finds quickly enough that seeding them saves it the trouble.
	{`1 / 0`, `{}`},
	{`9223372036854775807 + 1`, `{}`},
	{`items[10]`, `{"items":[]}`},
	{`items.a`, `{"items":[]}`},

	// A deeply nested activation, since the value tree the interpreter walks is
	// the attacker's to shape and depth is the other resource. Kept modest as a
	// seed — the fuzzer grows it, and CLAUDE.md's rule is that depth bounds and
	// breadth bounds are different bounds, so both directions are left reachable
	// rather than pinned here.
	{`a.a.a.a.a.a.a.a`, `{"a":{"a":{"a":{"a":{"a":{"a":{"a":{"a":1}}}}}}}}`},

	// An activation whose keys are not identifiers at all, which is what a
	// decoded webhook body routinely holds.
	{`has(m["a.b"])`, `{"m":{"a.b":1,"":2,"1":3}}`},
}

// TestCELCostLimitStopsAnExpensiveEvaluation is the reached direction of the
// bound [FuzzCELEvaluate] checks the not-exceeded direction of.
//
// A fuzz target that only ever asserts "this finished" is satisfied by a cost
// limit of zero, by a cost limit that is never installed, and by one whose
// accounting is broken in a way that never accumulates — every one of which
// leaves an evaluation unbounded in production while the fuzzer stays green. So
// the limit is required, here, to actually refuse something: each expression
// below must fail, and must fail *for cost* rather than for any of the other
// reasons an expression can fail, which is what distinguishes a working budget
// from a typo in an expression that would have been refused anyway.
func TestCELCostLimitStopsAnExpensiveEvaluation(t *testing.T) {
	t.Parallel()

	evaluator := v1.NewEvaluator()
	env, err := evaluator.ProfileEnv(v1.CurrentProfile)
	require.NoError(t, err)

	for name, tc := range map[string]struct {
		expression string
		activation map[string]any
	}{
		// Breadth from the *expression*: a comprehension over a comprehension
		// over a comprehension, which multiplies rather than adds — the shape
		// CLAUDE.md's alias-expansion note is about, in the expression language
		// instead of in YAML. 50 * 50 * 50 is 125,000 iterations, measured to
		// exceed the budget; 30 * 30 * 30 is measured not to, which is where
		// this number comes from rather than a guess.
		"nested comprehension": {
			expression: celRepeatedList(50) + `.map(i, ` + celRepeatedList(50) + `.map(j, ` + celRepeatedList(50) + `.map(k, i * j * k)))`,
			activation: map[string]any{},
		},
		// Breadth from the *data*, which is the direction that matters: the
		// expression is one short line an author could plausibly write, and the
		// hundred-element list that turns it into a million iterations arrives
		// from whoever sent the data. This is the pair [FuzzCELEvaluate] exists
		// to explore, and this case is the proof that the budget refuses it.
		"nested comprehension over an activation list": {
			expression: `items.map(i, items.map(j, items.map(k, i * j * k)))`,
			activation: map[string]any{"items": celOnes(100)},
		},
		// Not here, and worth saying why: a `filter` whose predicate is an
		// `exists` over a second list, which was drafted as a third case for
		// taking a different path through the interpreter. Measured, it prices
		// an iteration low enough that reaching the budget takes upwards of half
		// a million of them, and under `-race` that is tens of seconds of real
		// time for one assertion. An expression priced just past the budget
		// tests the budget exactly as well as one priced far past it —
		// overshooting is not strengthening — so the cheap shapes are the ones
		// kept.
	} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()

			ast, issues := env.Parse(tc.expression)
			require.NoError(t, issues.Err())

			// A deadline, so a failure of this test is a failed assertion rather
			// than a hung package: if the cost limit does not stop these, nothing
			// else in the process will.
			ctx, cancel := context.WithTimeout(t.Context(), celFuzzEvalDeadline)
			defer cancel()

			_, err := evaluator.Eval(ctx, env, ast, tc.activation)
			require.Error(t, err, "an evaluation priced past the %d-unit budget was allowed to finish", v1.DefaultCostLimit)
			require.NoError(t, ctx.Err(), "the deadline stopped this evaluation rather than the cost limit")
			require.Contains(t, strings.ToLower(err.Error()), "cost",
				"the evaluation failed for a reason other than cost, so this expression is not testing the budget: %v", err)
		})
	}
}

// celOnes returns a list of n ones, for the case whose weight comes from the
// activation rather than from the expression.
func celOnes(n int) []any {
	out := make([]any, n)
	for i := range out {
		out[i] = int64(1)
	}
	return out
}

// celRepeatedList renders a CEL list literal of n ones.
//
// A literal rather than an activation value, so that the case using it states
// its own weight and a reader can see what it costs without also reading the
// data it was handed.
func celRepeatedList(n int) string {
	elements := make([]string, n)
	for i := range elements {
		elements[i] = "1"
	}
	return "[" + strings.Join(elements, ",") + "]"
}
