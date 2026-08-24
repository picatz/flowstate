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
// The estimator returns nil for every call producing neither a string, bytes,
// nor a list, which leaves cel-go's own pricing in force for all of them. That
// is deliberate: a returned cost *replaces* cel-go's decision rather than adding
// to it (interpreter/runtimecost.go:269), so answering only where this system
// has something to add is the difference between augmenting a cost model and
// forking one. `_==_` on two strings, `matches`, `contains` — all produce
// scalars, all stay priced by cel-go's operand-size branches, none of them
// multiplies memory.
//
// # Why a list is charged too, and charged for its arguments
//
// Pricing what a call *produced* bounds a multiplication spread across many
// calls and does nothing about one call that allocates all of it at once,
// because an estimator runs after the call it prices. Reported on PR #885 by
// Codex, with the expression that shows it:
// `lists.range(10000).map(i, body).join("")`. Ten thousand appends cost about
// ten thousand units, nothing against the budget, and then a single `join`
// materialises 2 GB. Measured under GOMEMLIMIT=1GiB, it was refused only after
// 11,882 MiB of allocation and 16.4 seconds — the budget answering well after
// the allocator had.
//
// What closes it is that those 2 GB have to be *accumulated* before they can be
// concatenated. A list is therefore charged for the bytes entering it rather
// than for its length, so the budget is spent across the comprehension and the
// refusal lands before the allocation the bytes would have funded — the same
// expression is now refused in 11 ms having allocated nothing measurable. See
// [accumulatedChars] for why charging the smaller argument charges each element
// exactly once.
//
// This is what makes the bound statable in one sentence, which is the property
// worth having: an evaluation may produce or accumulate roughly
// `DefaultCostLimit / StringTraversalCostFactor` characters — about ten
// million — however it spells the arithmetic.
//
// Three costs this design accepts, stated rather than discovered later:
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
//   - A list accumulating many references to *one* string is charged as though
//     it held that many copies, so `lists.range(10000).map(i, body)` is refused
//     even though it retains one body and a set of pointers. Nothing available
//     at the charge point distinguishes aliased bytes from distinct ones, and
//     the reason to charge anyway is that the next call is what decides:
//     `join` on that list is a 2 GB materialisation of a 5 MiB value. Charging
//     the accumulation is what makes the refusal land before it, and
//     over-charging aliases is the price of being early rather than sorry.
type byteCostEstimator struct{}

// evaluationCostEstimator is the estimator every [Evaluator] installs. It holds
// no state, so one value serves every concurrent evaluation in the process.
var evaluationCostEstimator interpreter.ActualCostEstimator = byteCostEstimator{}

// CallCost charges a call for the bytes it moved, or declines to price it.
//
// A nil return means "no opinion", and cel-go then applies its own cost table
// unchanged. Every call producing neither a string, bytes, nor a list takes
// that path.
func (byteCostEstimator) CallCost(function, overloadID string, args []ref.Val, result ref.Val) *uint64 {
	var chars int64

	// json_parse is the one call here priced by what it was *handed* rather than
	// by what it produced, and it has to be: it returns a map or a list of
	// decoded values, which the switch below declines to price, so cel-go's
	// default charged it 1 unit however many bytes it decoded. A workflow that
	// can put a response body into an activation could then decode it once per
	// iteration of a comprehension for about 13 units an iteration — the
	// unbounded-repetition shape this whole file exists to close, on the one
	// function whose work is most obviously linear in its input.
	//
	// Charging the input at the same factor as every other traversal keeps the
	// bound statable in the same sentence: an evaluation may decode roughly
	// DefaultCostLimit / StringTraversalCostFactor characters of JSON, about ten
	// million, however it spells the arithmetic. At the default budget that is
	// nine or ten passes over a full default-size HTTP response body, and it
	// refuses before the eleventh rather than after it.
	//
	// The residual, stated rather than left to be discovered: a decoded value is
	// larger in memory than the bytes it came from, sometimes by a lot, and
	// nothing charges the decoded representation itself. What is bounded here is
	// the input an evaluation may feed to the decoder, which is the multiplier
	// the workflow controls; the per-value size is bounded where the value was
	// admitted (a response body, a webhook payload, a specification), which is
	// where a limit an operator can raise belongs.
	if function == jsonParseFunction && len(args) == 1 {
		if chars = sizeOf(args[0]); chars < 0 {
			return nil
		}

		cost := uint64(math.Ceil(float64(chars) * common.StringTraversalCostFactor))
		if cost < 1 {
			cost = 1
		}

		return &cost
	}

	switch result.Type() {
	case types.StringType, types.BytesType:
		chars = sizeOf(result)
	case types.ListType:
		// See [accumulatedChars]: a list is charged for the bytes *entering*
		// it, which is what makes the charge land before the allocation that
		// the bytes eventually fund.
		chars = accumulatedChars(args)
	default:
		return nil
	}

	if chars < 0 {
		return nil
	}

	// The same factor and the same rounding cel-go applies to the overloads it
	// does price by size, so a concatenation costs what cel-go would have
	// charged it had an overload ID been resolved.
	cost := uint64(math.Ceil(float64(chars) * common.StringTraversalCostFactor))

	// Never cheaper than the 1 unit cel-go charges a call it treats as O(1):
	// pricing by size must not make a call free just because it moved few
	// bytes, or an expression could loop on short results forever.
	if cost < 1 {
		cost = 1
	}
	return &cost
}

// listInspectionLimit is how many elements of a list argument are examined to
// weigh the bytes entering a list.
//
// It exists so that weighing a list is O(1) rather than O(n): a list-producing
// call is made once per iteration of a comprehension, so an O(n) weigh-in would
// make the *accounting* quadratic — a denial of service built out of the
// mechanism meant to prevent one. Every list-append cel-go plans for a
// comprehension has a single-element list on one side, so a small limit is
// enough to see the element being added while never walking the accumulator.
const listInspectionLimit = 16

// accumulatedChars returns the characters a list-producing call is adding,
// which is the size of its smallest short list argument.
//
// # Why the smallest argument, and why this is not an approximation
//
// A comprehension accumulates with `acc + [element]` — measured, not assumed:
// cel-go plans `lists.range(50).map(i, body)` as fifty `_+_` calls whose
// arguments are two lists, one of which is the freshly built single-element
// list holding what the iteration produced. Charging the smaller side charges
// each element exactly once, at the append that introduces it, and never walks
// the accumulator.
//
// That makes the accounting inductive rather than approximate: every list is
// built by appends, so by the time a list holds N bytes, those N bytes have
// been charged across the N appends that put them there. A long accumulator is
// skipped not because walking it is unaffordable but because its contents were
// already paid for.
//
// Elements that are not strings or bytes weigh nothing here. A list of lists or
// a list of maps is charged when *its* strings are built, for the same reason,
// and descending into one would reintroduce the unbounded traversal this limit
// exists to prevent.
func accumulatedChars(args []ref.Val) int64 {
	smallest := int64(-1)

	for _, arg := range args {
		list, ok := arg.(traits.Lister)
		if !ok {
			continue
		}
		length := sizeOf(arg)
		if length < 0 || length > listInspectionLimit {
			// The accumulator. Its bytes were charged as they were appended.
			continue
		}

		var chars int64
		for i := int64(0); i < length; i++ {
			element := list.Get(types.Int(i))
			switch element.Type() {
			case types.StringType, types.BytesType:
				if n := sizeOf(element); n > 0 {
					chars += n
				}
			}
		}

		if smallest < 0 || chars < smallest {
			smallest = chars
		}
	}

	if smallest < 0 {
		return 0
	}
	return smallest
}

// sizeOf returns the size a value reports, or -1 when it does not report one.
func sizeOf(value ref.Val) int64 {
	sizer, ok := value.(traits.Sizer)
	if !ok {
		return -1
	}
	size, ok := sizer.Size().(types.Int)
	if !ok || size < 0 {
		return -1
	}
	return int64(size)
}
