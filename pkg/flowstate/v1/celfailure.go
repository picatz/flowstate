package flowstatev1

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"strings"

	"github.com/google/cel-go/cel"
	"github.com/google/cel-go/common/ast"
	"github.com/google/cel-go/common/operators"
	"github.com/google/cel-go/common/types"
	"github.com/google/cel-go/common/types/ref"
	"github.com/google/cel-go/common/types/traits"
	"github.com/google/cel-go/parser"
	v1alpha1 "google.golang.org/genproto/googleapis/api/expr/v1alpha1"
)

// maxFailureNodes bounds how many nodes a failure report walks to find the one
// cel-go named. An expression is already bounded by the spec's size checks; this
// keeps the failure path, which runs once per failed expression, bounded on its
// own account (invariant 5: bound the work where it is spent).
const maxFailureNodes = 4096

// maxFailureCandidates bounds the names offered for a missing key.
const maxFailureCandidates = 8

// describeEvalFailure says what cel-go knew when it failed and the sentence
// dropped: which operator or function, what types its operands had, the
// subexpression, and, for a missing key under `steps`, the names that exist
// (#1551).
//
// cel-go labels an operator, function or selection error with the id of the AST
// node that failed ([types.Err.NodeID]), and the chain from an evaluation
// carries it. This looks that node up in the expression the program was built
// from and renders it; nothing here runs unless an evaluation has already failed,
// so the success path pays nothing.
//
// Operand types come from evaluating the failing node's operands again under the
// same activation and the same limits. The expression is pure, so the answer is
// the one the failed evaluation saw; an operand that cannot be evaluated is
// reported as "?" rather than hiding the rest. Only type names are reported, never
// values, so nothing a redaction set exists to hide is spelled here.
//
// Returns "" when there is nothing to add: no node id, a node it cannot find, or
// a failure that is not about an operation (a cost budget, a cancellation).
func (e *Evaluator) describeEvalFailure(ctx context.Context, env *cel.Env, parsed *v1alpha1.ParsedExpr, activation any, err error) string {
	var celErr *types.Err
	if !errors.As(err, &celErr) || celErr.NodeID() == 0 || parsed.GetExpr() == nil {
		return ""
	}

	// Only the two failures the sentence leaves unexplained: an operation with
	// no overload for its operands, and a selection of a key that is not there.
	// Others (`index out of bounds: 5`, a division by zero) already say what
	// went wrong in the words they have, and restating them would only
	// lengthen a sentence that was complete.
	if message := celErr.Error(); !strings.HasPrefix(message, "no such overload") && !strings.HasPrefix(message, "no such key") {
		return ""
	}

	node := findNode(parsed.GetExpr(), celErr.NodeID())
	if node == nil {
		return ""
	}

	switch kind := node.GetExprKind().(type) {
	case *v1alpha1.Expr_CallExpr:
		return e.describeCall(ctx, env, parsed, activation, node, kind.CallExpr)
	case *v1alpha1.Expr_SelectExpr:
		return e.describeSelect(ctx, env, parsed, activation, node, kind.SelectExpr)
	}

	return ""
}

// describeCall renders an operator or function failure: its name, the types it
// saw, and the subexpression.
func (e *Evaluator) describeCall(ctx context.Context, env *cel.Env, parsed *v1alpha1.ParsedExpr, activation any, node *v1alpha1.Expr, call *v1alpha1.Expr_Call) string {
	var operands []*v1alpha1.Expr
	if call.GetTarget() != nil {
		operands = append(operands, call.GetTarget())
	}
	operands = append(operands, call.GetArgs()...)

	names := make([]string, len(operands))
	for i, operand := range operands {
		names[i] = "?"
		if value, ok := e.evalOperand(ctx, env, parsed, activation, operand); ok {
			names[i] = value.Type().TypeName()
		}
	}

	what := fmt.Sprintf("function %q", call.GetFunction())
	if symbol, ok := operators.FindReverse(call.GetFunction()); ok && symbol != "" {
		what = fmt.Sprintf("operator %q", symbol)
	} else if call.GetFunction() == operators.Conditional {
		what = `operator "?:"`
	}

	return fmt.Sprintf("%s applied to (%s) in `%s`", what, strings.Join(names, ", "), unparseNode(parsed, node))
}

// describeSelect renders a missing-key failure: the selection, and, when it
// selects from something under `steps`, the names that do exist.
//
// Candidates are offered only for `steps` and `steps.<id>`: those names are the
// author's own step ids and declared outputs, which `flow validate` already
// prints. A map of data the run fetched is not listed, because its keys are the
// data's and not the author's.
func (e *Evaluator) describeSelect(ctx context.Context, env *cel.Env, parsed *v1alpha1.ParsedExpr, activation any, node *v1alpha1.Expr, selection *v1alpha1.Expr_Select) string {
	text := fmt.Sprintf("selecting %q in `%s`", selection.GetField(), unparseNode(parsed, node))

	if !underSteps(selection.GetOperand()) {
		return text
	}

	value, ok := e.evalOperand(ctx, env, parsed, activation, selection.GetOperand())
	if !ok {
		// cel-go labels the outermost selection of a chain, but the key that is
		// missing is the first one whose own operand does evaluate: for
		// `steps.nope.value` the node named is `.value`, and `nope` is the miss.
		// Walk inward to it.
		if inner := selection.GetOperand().GetSelectExpr(); inner != nil {
			return e.describeSelect(ctx, env, parsed, activation, selection.GetOperand(), inner)
		}

		return text
	}
	mapper, ok := value.(traits.Mapper)
	if !ok {
		return text
	}

	var names []string
	for it := mapper.Iterator(); it.HasNext() == types.True; {
		if name, ok := it.Next().Value().(string); ok {
			names = append(names, name)
		}
	}
	if len(names) == 0 {
		return text + "; there are none"
	}

	slices.Sort(names)
	more := ""
	if len(names) > maxFailureCandidates {
		more = fmt.Sprintf(" (and %d more)", len(names)-maxFailureCandidates)
		names = names[:maxFailureCandidates]
	}

	return fmt.Sprintf("%s; available: %s%s", text, strings.Join(names, ", "), more)
}

// evalOperand evaluates one operand of the failing node under the activation
// and the evaluator's limits.
func (e *Evaluator) evalOperand(ctx context.Context, env *cel.Env, parsed *v1alpha1.ParsedExpr, activation any, operand *v1alpha1.Expr) (value ref.Val, ok bool) {
	programEnv, err := e.extendedEnvFor(env)
	if err != nil {
		return nil, false
	}
	prg, err := programEnv.Program(cel.ParsedExprToAst(&v1alpha1.ParsedExpr{Expr: operand, SourceInfo: parsed.GetSourceInfo()}), e.limits.programOptions()...)
	if err != nil {
		return nil, false
	}
	out, _, err := prg.ContextEval(ctx, activation)
	if err != nil || out == nil {
		return nil, false
	}

	return out, true
}

// findNode returns the node with id, or nil. Iterative and bounded by
// [maxFailureNodes], so a pathological expression cannot make a failure report
// the expensive part of a failure.
func findNode(root *v1alpha1.Expr, id int64) *v1alpha1.Expr {
	stack := []*v1alpha1.Expr{root}
	for visited := 0; len(stack) > 0 && visited < maxFailureNodes; visited++ {
		node := stack[len(stack)-1]
		stack = stack[:len(stack)-1]

		if node.GetId() == id {
			return node
		}

		switch kind := node.GetExprKind().(type) {
		case *v1alpha1.Expr_CallExpr:
			stack = append(stack, kind.CallExpr.GetTarget())
			stack = append(stack, kind.CallExpr.GetArgs()...)
		case *v1alpha1.Expr_SelectExpr:
			stack = append(stack, kind.SelectExpr.GetOperand())
		case *v1alpha1.Expr_ListExpr:
			stack = append(stack, kind.ListExpr.GetElements()...)
		case *v1alpha1.Expr_StructExpr:
			for _, entry := range kind.StructExpr.GetEntries() {
				stack = append(stack, entry.GetValue(), entry.GetMapKey())
			}
		case *v1alpha1.Expr_ComprehensionExpr:
			c := kind.ComprehensionExpr
			stack = append(stack, c.GetIterRange(), c.GetAccuInit(), c.GetLoopCondition(), c.GetLoopStep(), c.GetResult())
		}

		// A nil child is the absence of one (a function with no target).
		stack = slices.DeleteFunc(stack, func(n *v1alpha1.Expr) bool { return n == nil })
	}

	return nil
}

// underSteps reports whether an expression is `steps` or a selection chain
// rooted at it: `steps`, `steps.id`, `steps.id.output`.
func underSteps(node *v1alpha1.Expr) bool {
	for node != nil {
		switch kind := node.GetExprKind().(type) {
		case *v1alpha1.Expr_IdentExpr:
			return kind.IdentExpr.GetName() == "steps"
		case *v1alpha1.Expr_SelectExpr:
			node = kind.SelectExpr.GetOperand()
		default:
			return false
		}
	}

	return false
}

// unparseNode renders a node back to expression text, or its function name when
// the unparser cannot.
func unparseNode(parsed *v1alpha1.ParsedExpr, node *v1alpha1.Expr) string {
	native, err := ast.ProtoToExpr(node)
	if err != nil {
		return "?"
	}
	info, err := ast.ProtoToSourceInfo(parsed.GetSourceInfo())
	if err != nil {
		return "?"
	}
	text, err := parser.Unparse(native, info)
	if err != nil {
		return "?"
	}

	return text
}
