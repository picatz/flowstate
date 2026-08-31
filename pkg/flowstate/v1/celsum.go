package flowstatev1

import (
	"github.com/google/cel-go/cel"
	"github.com/google/cel-go/common/ast"
	"github.com/google/cel-go/common/operators"
	"github.com/google/cel-go/common/types"
)

// The expression language had every list operation except the fold (#1304).
//
// `filter`, `map`, `sortBy` and the comprehensions reshape a list, and nothing
// could reduce one — so "total of `amount_cents` over the kept orders", the most
// ordinary aggregation in any billing or reporting workload, had exactly one
// spelling: a `loop:` node folding by index. That costs three ways at once. A
// pure O(n) computation becomes n engine iterations with durable history per
// iteration; the do-while/update ordering invites an off-by-one that produces a
// wrong total with nothing to catch it; and the boilerplate buries the one
// expression that means anything. examples/loop-accumulate exists to teach that
// workaround, which is the measure of the pressure.
//
// # Why a macro, and why that lets it join the current profile
//
// `sum` is a parse-time macro expanding to a standard CEL comprehension — the
// same family as `filter` and `map`, not the same as `sortBy`, whose expansion
// calls a helper function its library must also declare. The expansion below
// spells the fold out of vocabulary every CEL runtime has (`size`, indexing,
// `_+_`, `_?_:_`, `dyn`, a list literal), and that is what decides where this
// may land. A compiled specification carries the expansion rather than the
// name, so no stored run can observe whether the build evaluating it ever heard
// of `sum`, and a worker predating this macro still evaluates every spec that
// uses it — the properties the profile freeze exists to protect, kept without
// minting a new profile. The rule this instantiates is written at [profiles].
//
// # The expansion
//
//	xs.sum()
//
// becomes, with `acc` the parser's accumulator and `@__sum_elem__` the element:
//
//	__comprehension__(          // fold over xs
//	  iterRange: xs,
//	  accuInit:  [],
//	  loopStep:  size(acc) == 0 ? [@__sum_elem__] : [acc[0] + @__sum_elem__],
//	  result:    size(acc) == 0 ? dyn(0) : acc[0]
//	)
//
// The accumulator carries the running total inside a single-element list, and
// the first element seeds it, because CEL's `+` has no cross-type overloads and
// the language has no polymorphic zero: an accumulator initialised to int `0`
// would make `[1.5, 2.5].sum()` fail on `0 + 1.5`. Seeding from the list keeps
// an int sum an int, a double sum a double, a uint sum a uint, and a duration
// sum a duration, each added under `_+_`'s own checked arithmetic — an int
// total that would wrap is an evaluation error, not a silent negative, which is
// one of the failure modes the `loop:` spelling could not catch either.
//
// What `+` accepts, `sum` folds: `['a', 'b'].sum()` concatenates, exactly as
// the operator chain it expands to would. A list `+` cannot add — a string
// beside an int — fails with the operator's own no-such-overload error, naming
// the mismatch. An empty list sums to int `0`, the one case with no element to
// take a type from; `dyn(0)` rather than `0` so the type checker joins that
// branch with a typed element (`flow validate` on `[1.5].sum()` would otherwise
// refuse a working expression for mixing int and double across `?:`).
//
// # Cost
//
// Expanded before evaluation, the fold is charged per iteration by the same
// runtime cost tracking every comprehension is charged under, so a sum spends
// against [DefaultCostLimit] in proportion to the list it folds and a
// pathological one is refused mid-fold. Sums that accumulate bytes rather than
// numbers are additionally priced by [byteCostEstimator]: each step's `_+_`
// produces the running concatenation and is charged for its size, so a string
// sum costs what the equivalent chain of `+` costs and cannot outrun the
// budget's ten-million-character bound.
func sumLibrary() cel.EnvOption {
	return cel.Macros(cel.ReceiverMacro("sum", 0, expandSum))
}

// sumElementVar is the fold's element variable. Named in the parser's reserved
// `@`-prefixed space, like `sortBy`'s `@__sortBy_input__`, so it cannot shadow
// or capture any name an author can write.
const sumElementVar = "@__sum_elem__"

// expandSum rewrites `xs.sum()` into the comprehension documented on
// [sumLibrary]. Every node is built fresh — the factory assigns each an id, and
// a shared node would give two positions one identity.
func expandSum(mef cel.MacroExprFactory, target ast.Expr, _ []ast.Expr) (ast.Expr, *cel.Error) {
	// size(acc) == 0, the "nothing folded yet" test both the step and the
	// result branch on.
	empty := func() ast.Expr {
		return mef.NewCall(operators.Equals,
			mef.NewCall("size", mef.NewAccuIdent()),
			mef.NewLiteral(types.Int(0)))
	}
	// acc[0], the running total unwrapped from its carrier.
	total := func() ast.Expr {
		return mef.NewCall(operators.Index, mef.NewAccuIdent(), mef.NewLiteral(types.Int(0)))
	}

	step := mef.NewCall(operators.Conditional,
		empty(),
		mef.NewList(mef.NewIdent(sumElementVar)),
		mef.NewList(mef.NewCall(operators.Add, total(), mef.NewIdent(sumElementVar))))

	result := mef.NewCall(operators.Conditional,
		empty(),
		mef.NewCall("dyn", mef.NewLiteral(types.Int(0))),
		total())

	return mef.NewComprehension(target, sumElementVar, mef.AccuIdentName(),
		mef.NewList(), mef.NewLiteral(types.True), step, result), nil
}
