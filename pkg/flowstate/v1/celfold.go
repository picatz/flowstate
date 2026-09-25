package flowstatev1

import (
	"github.com/google/cel-go/cel"
	"github.com/google/cel-go/common/ast"
	"github.com/google/cel-go/common/operators"
	"github.com/google/cel-go/common/types"
	"github.com/google/cel-go/parser"
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
// Two macros close it, and they are one decision spelled at two levels. `sum`
// is the dominant case with nothing to declare: no variables, no seed, fold
// with `+`. `reduce` is the general form for a combiner that is not `+` — a
// product, a running maximum, a fold whose seed carries meaning — where the
// author names the accumulator and the element and writes the step. `sum` is
// exactly `reduce` with the naming and seeding questions answered, which is why
// they live in one file and one library entry.
//
// # Why macros, and why that lets them join the current profile
//
// Both are parse-time macros expanding to standard CEL comprehensions — the
// same family as `filter` and `map`, not the same as `sortBy`, whose expansion
// calls a helper function its library must also declare. The expansions below
// spell the fold out of vocabulary every CEL runtime has (`size`, indexing,
// `_+_`, `_?_:_`, `dyn`, a list literal — and for `reduce`, nothing beyond the
// comprehension node itself), and that is what decides where this may land. A
// compiled specification carries the expansion rather than the name, so no
// stored run can observe whether the build evaluating it ever heard of these
// macros, and a worker predating them still evaluates every spec that uses
// them — the properties the profile freeze exists to protect, kept without
// minting a new profile. The rule this instantiates is written at [profiles].
//
// # Only a list may be folded
//
// CEL's comprehension machinery iterates a map over its keys, while a fold's
// ordinary use is to combine values. Treating `m.sum()` as a sum of keys and
// making `reduce`'s one element variable mean a key would be surprising beside
// their list contract, so both macros require the author to select map values
// explicitly. Map traversal itself is canonicalized by celmap.go (#1359), but
// that does not answer which values an author intended to combine.
//
// So both expansions refuse a non-list receiver before iterating, inside the
// expansion itself — the guard has to be spelled in the same standard-CEL
// vocabulary as the rest, or the forward-compatibility argument above dies
// with it. Two tricks make that possible. The receiver is bound once through
// the same empty-range comprehension `cel.bind` and `sortBy` use, so the
// guard's two reads do not evaluate the receiver twice. And the type test is
// written `type(x) == type([])` rather than `type(x) == list`, because the
// bare identifier `list` would be collected by flowfile's type-check
// declarations (celcheck.go) and collide with the standard environment's own
// type identifier. The refusal itself is a key lookup on an empty map, whose
// error carries the one sentence an author needs: select the map's values
// explicitly — `m.map(k, k).sort().map(k, m[k]).sum()` also makes a chosen
// business order visible at the call site.
//
// # The `sum` expansion
//
//	xs.sum()
//
// becomes, with `acc` the parser's accumulator, `@__sum_input__` the bound
// receiver, and `@__sum_elem__` the element:
//
//	cel.bind(@__sum_input__, xs,
//	  __comprehension__(        // fold over the guarded receiver
//	    iterRange: type(@__sum_input__) == type([]) ? @__sum_input__ : {}['sum() folds a list…'],
//	    accuInit:  [],
//	    loopStep:  size(acc) == 0 ? [@__sum_elem__] : [acc[0] + @__sum_elem__],
//	    result:    size(acc) == 0 ? dyn(0) : acc[0]
//	  ))
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
// # The `reduce` expansion
//
//	xs.reduce(a, v, init, step)
//
// becomes the comprehension it names outright, under the same bind and guard:
// iterate the guarded xs as `v`, carry `a` seeded with `init`, evaluate `step`
// each iteration as the next `a`, answer the final `a`. The seed makes the
// empty-list answer the author's — `[]` folds to `init`, no `dyn(0)` needed —
// and the author-written step means no carrier list either: `reduce` is the
// comprehension machinery with none of `sum`'s compensations, because every
// question those compensations answer (what is zero, what is the combining
// operator) is answered in the call.
//
//	[2, 3, 4].reduce(p, v, 1, p * v)   // 24 — the fold sum cannot spell
//
// The two variable names are the macro's whole surface to get wrong, so the
// expander refuses what cannot work, by name, at compile time: a first or
// second argument that is not a bare identifier, the same name for both, or
// the parser's own accumulator name, which the expansion must be able to tell
// from the author's.
//
// # Cost
//
// Expanded before evaluation, both folds are charged per iteration by the same
// runtime cost tracking every comprehension is charged under, so a fold spends
// against [DefaultCostLimit] in proportion to the list it walks and a
// pathological one is refused mid-fold. Folds that accumulate bytes rather
// than numbers are additionally priced by [byteCostEstimator]: each step's
// `_+_` produces the running concatenation and is charged for its size, so a
// string fold costs what the equivalent chain of `+` costs and cannot outrun
// the budget's ten-million-character bound.
func foldLibrary() cel.EnvOption {
	return cel.Macros(
		cel.ReceiverMacro("sum", 0, expandSum),
		cel.ReceiverMacro("reduce", 4, expandReduce),
	)
}

// sumElementVar is `sum`'s element variable. Named in the parser's reserved
// `@`-prefixed space, like `sortBy`'s `@__sortBy_input__`, so it cannot shadow
// or capture any name an author can write.
const sumElementVar = "@__sum_elem__"

// bindReceiver wraps body so that the macro's receiver is evaluated once and
// read as inputVar — the empty-range comprehension `cel.bind` and `sortBy`
// expand to. Without it the guard's two reads of the receiver would evaluate
// it twice, doubling the work and the charged cost of whatever chain produced
// it.
func bindReceiver(mef cel.MacroExprFactory, inputVar string, target, body ast.Expr) ast.Expr {
	return mef.NewComprehension(mef.NewList(), "#unused", inputVar,
		target, mef.NewLiteral(types.False), mef.NewIdent(inputVar), body)
}

// listOnly returns the bound receiver where it is a list, and a refusal
// naming the explicit map-value spelling where it is not. See "Only a list may
// be folded" above for why the test avoids the bare identifier `list` and why
// the refusal is a key lookup.
func listOnly(mef cel.MacroExprFactory, inputVar, macroName string) ast.Expr {
	isList := mef.NewCall(operators.Equals,
		mef.NewCall("type", mef.NewIdent(inputVar)),
		mef.NewCall("type", mef.NewList()))

	refusal := mef.NewCall(operators.Index, mef.NewMap(), mef.NewLiteral(types.String(
		macroName+" folds a list, and this receiver is not one; select a map's values "+
			"explicitly — m.map(k, k).sort().map(k, m[k])")))

	return mef.NewCall(operators.Conditional, isList, mef.NewIdent(inputVar), refusal)
}

// expandSum rewrites `xs.sum()` into the comprehension documented on
// [foldLibrary]. Every node is built fresh — the factory assigns each an id,
// and a shared node would give two positions one identity.
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

	const inputVar = "@__sum_input__"
	fold := mef.NewComprehension(listOnly(mef, inputVar, "sum()"), sumElementVar,
		mef.AccuIdentName(), mef.NewList(), mef.NewLiteral(types.True), step, result)

	return bindReceiver(mef, inputVar, target, fold), nil
}

// expandReduce rewrites `xs.reduce(a, v, init, step)` into the comprehension
// documented on [foldLibrary].
func expandReduce(mef cel.MacroExprFactory, target ast.Expr, args []ast.Expr) (ast.Expr, *cel.Error) {
	accuVar, ok := identName(args[0])
	if !ok {
		return nil, mef.NewError(args[0].ID(),
			"reduce's first argument names the accumulator; write a bare identifier, as in xs.reduce(a, v, 0, a + v)")
	}
	iterVar, ok := identName(args[1])
	if !ok {
		return nil, mef.NewError(args[1].ID(),
			"reduce's second argument names the element; write a bare identifier, as in xs.reduce(a, v, 0, a + v)")
	}
	if accuVar == iterVar {
		return nil, mef.NewError(args[1].ID(),
			"reduce's accumulator and element must have different names; the step reads both")
	}
	// The parser's own accumulator names, under either convention it has
	// shipped. The expansion has to be able to tell the author's accumulator
	// from the machinery's, and the standard macros refuse the same collision
	// for their one bound variable.
	for _, reserved := range []string{mef.AccuIdentName(), parser.AccumulatorName} {
		if accuVar == reserved || iterVar == reserved {
			return nil, mef.NewError(args[0].ID(),
				"reduce cannot bind the name "+reserved+"; it is the comprehension machinery's own accumulator")
		}
	}

	const inputVar = "@__reduce_input__"
	fold := mef.NewComprehension(listOnly(mef, inputVar, "reduce()"), iterVar, accuVar,
		args[2], mef.NewLiteral(types.True), args[3], mef.NewIdent(accuVar))

	return bindReceiver(mef, inputVar, target, fold), nil
}

// identName returns the name a bare identifier expression carries.
func identName(e ast.Expr) (string, bool) {
	if e.Kind() != ast.IdentKind {
		return "", false
	}
	return e.AsIdent(), true
}
