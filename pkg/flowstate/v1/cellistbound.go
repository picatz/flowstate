package flowstatev1

import (
	"fmt"

	"github.com/google/cel-go/cel"
	"github.com/google/cel-go/common/operators"
	"github.com/google/cel-go/common/types"
	"github.com/google/cel-go/common/types/ref"
	"github.com/google/cel-go/common/types/traits"
	"github.com/google/cel-go/interpreter"
)

// #204 bounded list elements where a value *enters* an expression: a caller's
// submitted input and a task's result are each refused past [maxListElements],
// because comprehension wall-time is quadratic in element count while CEL's
// cost accounting of it is linear, so no [DefaultCostLimit] bounds the time.
// #1769 found the third origin: elements *manufactured inside* the expression.
// `lists.range(60000)` builds sixty thousand elements from a literal the author
// wrote, `.map(i, lists.range(100))` turns them into six million, and neither
// placement #204 chose ever sees a value — the list exists only between two
// calls of one evaluation. Measured at eb8172f: 3.3 s of CPU under the cost
// limit, and two such `value:` steps in one workflow task exceeded
// [WorkerDeadlockDetectionTimeout], so the durable driver's task panicked and
// was rescheduled forever while `flow run local` completed the same file.
//
// This file closes that origin with the same bound, in the evaluator, so both
// drivers, `flow test`, and the LSP's checker share it without any of them
// spelling a second number:
//
//   - [listRangeLibrary] replaces the lists extension's `lists.range` binding
//     with one that refuses n past [maxListElements] *before* allocating —
//     the only call in the vocabulary that manufactures elements from an
//     integer, and so the only one that can be refused from its argument.
//     cel-go's own `ListsMaxRangeSize` option would refuse at the same point,
//     but with its own sentence; the refusal here is worded like #204's, so an
//     author meets one explanation of one bound wherever they reach it.
//   - [boundListResults] checks every call's result as it is produced and
//     fails the evaluation the moment a list exceeds [maxListElements]. A
//     comprehension accumulates through a `_+_` call per element, `flatten`
//     and `+` are calls, so the check lands at the append or the call that
//     crossed the bound, and before the next call could multiply it. The
//     check is O(1) per call: a size read, never a walk.
//
// Neither replaces the cost budget. `lists.range` is also priced by the
// elements it produced ([byteCostEstimator]), so the *number* of bounded
// lists one evaluation may manufacture is bounded too — a hundred
// `lists.range(10000)` calls spend [DefaultCostLimit] — which is what keeps
// a comprehension from paying the element bound ten thousand times over.

// listsRangeFunction is the lists extension's range function, by the name it
// carries in a parsed AST.
const listsRangeFunction = "lists.range"

// listsRangeOverload is the extension's overload id for [listsRangeFunction],
// reused so the declaration below *replaces* the library's binding rather than
// adding an ambiguous second overload: cel-go merges a redeclared overload
// whose signature matches by taking the new binding (decls.FunctionDecl.AddOverload).
const listsRangeOverload = "lists_range"

// listRangeLibrary is the `lists.range` binding this system installs over the
// extension's — see the file comment. It must follow `ext.Lists` in the
// environment options so the merge replaces theirs with this one; a test pins
// that order by asserting the refusal is worded here.
func listRangeLibrary() cel.EnvOption {
	return cel.Function(listsRangeFunction,
		cel.Overload(listsRangeOverload,
			[]*cel.Type{cel.IntType}, cel.ListType(cel.IntType),
			cel.UnaryBinding(func(arg ref.Val) ref.Val {
				n, ok := arg.(types.Int)
				if !ok {
					return types.MaybeNoSuchOverloadErr(arg)
				}
				if n < 0 {
					return types.NewErr("lists.range: size must be non-negative, got %d", n)
				}
				if int64(n) > maxListElements {
					return types.WrapErr(expressionListBoundError(
						fmt.Sprintf("`lists.range(%d)` would build", n), int(n)))
				}
				elements := make([]ref.Val, 0, n)
				for i := types.Int(0); i < n; i++ {
					elements = append(elements, i)
				}
				return types.NewRefValList(types.DefaultTypeAdapter, elements)
			}),
		),
	)
}

// boundListResults is the [interpreter.InterpretableDecorator] that refuses a
// list result past [maxListElements] at the call that produced it. Installed
// by [Limits.programOptions] on every program this system builds.
func boundListResults(i interpreter.Interpretable) (interpreter.Interpretable, error) {
	call, ok := i.(interpreter.InterpretableCall)
	if !ok {
		return i, nil
	}
	// The canonical map-traversal wrapper (celmap.go) returns a comprehension's
	// list source unchanged; it builds nothing, and naming it in a refusal would
	// hand the author an internal spelling. A source past the bound is caught
	// at the comprehension's first append over it instead.
	if call.Function() == orderedMapFunction {
		return i, nil
	}
	return &boundedListCall{InterpretableCall: call}, nil
}

// boundedListCall wraps one call so its list results are size-checked. It
// keeps implementing [interpreter.InterpretableCall], so the cost observer and
// any later decorator still see the call's function and arguments through it.
type boundedListCall struct {
	interpreter.InterpretableCall
}

// Eval evaluates the call and refuses a list result past the bound.
func (c *boundedListCall) Eval(activation interpreter.Activation) ref.Val {
	return c.check(c.InterpretableCall.Eval(activation))
}

// Exec is Eval for the interpreter's frame-passing path, which is the one a
// comprehension's step runs on.
func (c *boundedListCall) Exec(frame *interpreter.ExecutionFrame) ref.Val {
	return c.check(c.InterpretableCall.Exec(frame))
}

func (c *boundedListCall) check(out ref.Val) ref.Val {
	list, ok := out.(traits.Lister)
	if !ok {
		return out
	}
	size, ok := list.Size().(types.Int)
	if !ok || int64(size) <= maxListElements {
		return out
	}

	// The evaluation ends here, the way the cost budget ends one: cel-go's
	// Eval recovers this error and returns it. Returned as an error *value*
	// instead, the refusal would not stop a comprehension — the fold carries
	// an errored accumulator through every element left in its source, and
	// the cost tracker's stack scan for each of those steps misses (the
	// short-circuited `+` never evaluates its right side), so a source of n
	// elements costs O(n²) after the refusal. A source is bounded where it
	// enters an expression, but a webhook body is bounded in bytes rather
	// than elements, and a megabyte of `[1,1,1,…]` mapped over would have
	// turned this bound into the very stall it exists to prevent.
	panic(interpreter.EvalCancelledError{
		Cause: interpreter.CostLimitExceeded,
		Message: expressionListBoundError(
			fmt.Sprintf("%s built", spelledFunction(c.Function())), int(size)).Error(),
	})
}

// spelledFunction names a call the way its author wrote it: an operator by
// its symbol, since nobody writes `_+_`, and anything else by its name.
func spelledFunction(function string) string {
	if symbol, ok := operators.FindReverse(function); ok {
		return "the `" + symbol + "` operator"
	}
	return "`" + function + "`"
}

// expressionListBoundError words the refusal for a list built inside an
// expression, in the shape [inputSideConstraintBoundError] uses for a value
// the caller sized: what was built, the bound, why the bound exists, and what
// to do instead. producer is the subject and verb — "`lists.range(60000)`
// would build", "the `+` operator built" — so the same sentence serves the
// refusal before an allocation and the one after it.
func expressionListBoundError(producer string, elements int) error {
	return fmt.Errorf(
		"%s a list of %d elements, over the %d one expression may hold in a list "+
			"(a comprehension, `flatten`, `+`, and `lists.range` all pay the same cost); "+
			"elements made inside an expression are not a cost this server bounds any other "+
			"way — narrow what the expression builds, page the work across multiple runs, "+
			"or have a step read the list from a reference instead of building it here",
		producer, elements, maxListElements)
}
