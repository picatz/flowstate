package flowstatev1

import (
	"math"

	"github.com/google/cel-go/common"
	"github.com/google/cel-go/common/types"
	"github.com/google/cel-go/common/types/ref"
	"github.com/google/cel-go/common/types/traits"
	"github.com/google/cel-go/interpreter"
)

// [DefaultCostLimit] bounds how much an evaluation may spend. What a unit of
// that budget *buys* is decided here, and until this file existed the answer on
// this system's evaluation path was "one operation, whatever it moves".
//
// # The resource the attacker controls
//
// On the evaluation path the expression is usually an author's and the data is
// usually not. A webhook body ([BindWebhookTriggerInputs]), a decoded HTTP
// response bound as a step output, a signal payload — each is admitted under a
// byte bound of its own, and each then becomes a variable in an activation that
// `items.map(i, prefix + body + suffix)` may reference once per iteration. The
// per-input byte bounds are real, and none of them bounds the *product* of an
// input's size and an expression's iteration count, which is what a
// comprehension computes. So the resource to price is bytes produced, not calls
// made — CLAUDE.md's rule, applied to the one budget that was not applying it.
//
// # Why cel-go's own sizing did not reach us
//
// cel-go does price the O(m+n) string overloads by operand size, and the branch
// that does it is not subtle: `overloads.AddString` charges
// `ceil((size(lhs)+size(rhs)) * StringTraversalCostFactor)`
// (interpreter/runtimecost.go:298 in v0.31.0). That branch is a switch on
// `call.OverloadID()`, and an overload ID is something the *checker* resolves.
// Flowstate evaluates parsed ASTs — a compiled specification carries an
// `expr.ParsedExpr`, and activations are dynamic maps whose shapes no
// declaration describes — so every call on this path arrives with an empty
// overload ID, falls through to the `default:` arm, and is charged 1.
//
// Measured against cel-go v0.31.0 with the profile environment, `s + s` where
// `s` holds 200,000 characters: 3 units parsed, 40,002 units checked. The same
// expression, the same bytes, a four-order-of-magnitude difference in what the
// budget was told it cost. `xs.map(i, s + s + s + s)` over 100 items costs
// 1,912 units parsed — 0.19% of the budget for 172 MiB of allocation — and
// 1,000,095 checked, which the limit refuses.
//
// So the missing bound was not missing from cel-go. It was unreachable, and the
// fix is to price the call by something that survives the absence of a checked
// AST.
//
// # What is priced, and why the result rather than the operands
//
// [byteCostEstimator] charges every call that *produces* a string or bytes for
// the size of what it produced. The estimator hook
// ([interpreter.ActualCostEstimator]) is handed the function name, the argument
// values and the result value, and unlike the overload ID all three are present
// whether or not anything type-checked.
//
// Pricing the result rather than the operands is what keeps this from being a
// second spelling of cel-go's cost table. For concatenation the two agree
// exactly and by construction — the result of `a + b` is `size(a)+size(b)`
// characters, so `ceil(size(result) * StringTraversalCostFactor)` *is* cel-go's
// `AddString` formula, arrived at without copying the switch that computes it.
// Where they differ, they differ in the direction that closes the hole: cel-go
// has no runtime cost estimator for the strings extension at all, so `repeat`,
// `join` and `replace` are charged 1 unit even on a checked AST, and those
// three are the sharpest amplifiers in the vocabulary. `"x".repeat(50000000)`
// is one call.
//
// The estimator returns nil for every call that does not produce a string or
// bytes, which leaves cel-go's own pricing in force for all of them. That is
// deliberate: a returned cost *replaces* cel-go's decision rather than adding
// to it (interpreter/runtimecost.go:269), so answering only where this system
// has something to add is the difference between augmenting a cost model and
// forking one. `_==_` on two strings, `matches`, `contains` — all produce
// scalars, all stay priced by cel-go's operand-size branches, none of them
// multiplies memory.
//
// Two costs this design accepts, stated rather than discovered later:
//
//   - Sizes are in code points, not bytes, because `types.String.Size` counts
//     runes (common/types/string.go:179). A worst-case UTF-8 string is
//     under-charged by 4x against its heap footprint. Matching cel-go's own
//     unit is worth more than the factor: a budget where half the entries mean
//     runes and half mean bytes is a budget nobody can reason about.
//   - A call that traverses a large input to produce a small string —
//     `body.substring(0, 1)` — is charged 1. It is O(n) in time and O(1) in
//     memory, cel-go charges it 1 today, and this file is about the resource
//     that multiplies.
type byteCostEstimator struct{}

// evaluationCostEstimator is the estimator every [Evaluator] installs. It holds
// no state, so one value serves every concurrent evaluation in the process.
var evaluationCostEstimator interpreter.ActualCostEstimator = byteCostEstimator{}

// CallCost charges a call for the bytes it produced, or declines to price it.
//
// A nil return means "no opinion", and cel-go then applies its own cost table
// unchanged. Every call whose result is not a string or bytes takes that path.
func (byteCostEstimator) CallCost(function, overloadID string, args []ref.Val, result ref.Val) *uint64 {
	switch result.Type() {
	case types.StringType, types.BytesType:
	default:
		return nil
	}

	sizer, ok := result.(traits.Sizer)
	if !ok {
		return nil
	}
	size, ok := sizer.Size().(types.Int)
	if !ok || size < 0 {
		return nil
	}

	// The same factor and the same rounding cel-go applies to the overloads it
	// does price by size, so a concatenation costs what cel-go would have
	// charged it had an overload ID been resolved.
	cost := uint64(math.Ceil(float64(size) * common.StringTraversalCostFactor))

	// Never cheaper than the 1 unit cel-go charges a call it treats as O(1):
	// pricing by size must not make a call free just because it produced few
	// bytes, or an expression could loop on short results forever.
	if cost < 1 {
		cost = 1
	}
	return &cost
}
