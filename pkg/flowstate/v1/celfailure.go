package flowstatev1

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"strings"
	"unicode/utf8"

	"github.com/google/cel-go/cel"
	"github.com/google/cel-go/common/ast"
	"github.com/google/cel-go/common/operators"
	"github.com/google/cel-go/common/types"
	"github.com/google/cel-go/common/types/ref"
	"github.com/google/cel-go/common/types/traits"
	"github.com/google/cel-go/parser"
	"github.com/picatz/flowstate/internal/textbound"
	v1alpha1 "google.golang.org/genproto/googleapis/api/expr/v1alpha1"
)

// maxFailureNodes bounds how many nodes a failure report walks to find the one
// cel-go named. An expression is already bounded by the spec's size checks; this
// keeps the failure path, which runs once per failed expression, bounded on its
// own account (invariant 5: bound the work where it is spent).
const maxFailureNodes = 4096

// maxFailureCandidates bounds the names offered for a missing key.
const maxFailureCandidates = 8

// maxFailureSubexpr bounds the subexpression echoed into a failure sentence. An
// authored expression may approach the specification limit, and a tolerated
// failure records its text twice in durable state.
const maxFailureSubexpr = 256

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
func (e *Evaluator) describeEvalFailure(ctx context.Context, env *cel.Env, parsed *v1alpha1.ParsedExpr, activation any, err error) (string, *ExpressionFailure) {
	var celErr *types.Err
	if !errors.As(err, &celErr) || celErr.NodeID() == 0 || parsed.GetExpr() == nil {
		return "", nil
	}

	// Only the two failures the sentence leaves unexplained: an operation with
	// no overload for its operands, and a selection of a key that is not there.
	// Others (`index out of bounds: 5`, a division by zero) already say what
	// went wrong in the words they have, and restating them would only
	// lengthen a sentence that was complete.
	if message := celErr.Error(); !strings.HasPrefix(message, "no such overload") && !strings.HasPrefix(message, "no such key") {
		return "", nil
	}

	node, bound := findNode(parsed.GetExpr(), celErr.NodeID())
	work := newFailureWork(e.limits.Cost)
	if node == nil {
		return "", nil
	}

	var text string
	switch kind := node.GetExprKind().(type) {
	case *v1alpha1.Expr_CallExpr:
		text = e.describeCall(ctx, env, parsed, activation, work, bound, node, kind.CallExpr)
	case *v1alpha1.Expr_SelectExpr:
		text = e.describeSelect(ctx, env, parsed, activation, work, bound, node, kind.SelectExpr)
	}
	if text == "" {
		return "", nil
	}

	return text, work.detail
}

// describeCall renders an operator or function failure: its name, the types it
// saw, and the subexpression.
func (e *Evaluator) describeCall(ctx context.Context, env *cel.Env, parsed *v1alpha1.ParsedExpr, activation any, work *failureWork, bound []string, node *v1alpha1.Expr, call *v1alpha1.Expr_Call) string {
	var operands []*v1alpha1.Expr
	if call.GetTarget() != nil {
		operands = append(operands, call.GetTarget())
	}
	operands = append(operands, call.GetArgs()...)

	names := make([]string, len(operands))
	for i, operand := range operands {
		names[i] = "?"
		if value, ok := e.evalOperand(ctx, env, parsed, activation, work, bound, operand); ok {
			names[i] = value.Type().TypeName()
		}
	}

	what := fmt.Sprintf("function %q", call.GetFunction())
	if symbol, ok := operators.FindReverse(call.GetFunction()); ok && symbol != "" {
		what = fmt.Sprintf("operator %q", symbol)
	} else if call.GetFunction() == operators.Conditional {
		what = `operator "?:"`
	}

	subexpr := unparseNode(parsed, node)
	work.detail.Operator = textbound.Cut(operatorName(call.GetFunction()), maxFailureFieldBytes)
	work.detail.OperandTypes = names[:min(len(names), maxFailureOperands)]
	work.detail.Subexpression = subexpr
	work.detail.Offset = offsetOf(parsed, node)
	work.detail.Caret = callCaret(parsed, call, subexpr)

	return fmt.Sprintf("%s applied to (%s) in `%s`", what, strings.Join(names, ", "), subexpr)
}

// describeSelect renders a missing-key failure: the selection, and, when it
// selects from something under `steps`, the names that do exist.
//
// Candidates are offered only for `steps` and `steps.<id>`: those names are the
// author's own step ids and declared outputs, which `flow validate` already
// prints. A map of data the run fetched is not listed, because its keys are the
// data's and not the author's.
func (e *Evaluator) describeSelect(ctx context.Context, env *cel.Env, parsed *v1alpha1.ParsedExpr, activation any, work *failureWork, bound []string, node *v1alpha1.Expr, selection *v1alpha1.Expr_Select) string {
	subexpr := unparseNode(parsed, node)
	text := fmt.Sprintf("selecting %q in `%s`", selection.GetField(), subexpr)
	work.detail.Selected = textbound.Cut(selection.GetField(), maxFailureFieldBytes)
	work.detail.Subexpression = subexpr
	work.detail.Offset = offsetOf(parsed, node)
	work.detail.Caret = selectCaret(selection.GetField(), subexpr)

	if !stepsMapOperand(selection.GetOperand()) {
		return text
	}

	value, ok := e.evalOperand(ctx, env, parsed, activation, work, bound, selection.GetOperand())
	if !ok {
		// cel-go labels the outermost selection of a chain, but the key that is
		// missing is the first one whose own operand does evaluate: for
		// `steps.nope.value` the node named is `.value`, and `nope` is the miss.
		// Walk inward to it.
		if inner := selection.GetOperand().GetSelectExpr(); inner != nil {
			return e.describeSelect(ctx, env, parsed, activation, work, bound, selection.GetOperand(), inner)
		}

		return text
	}
	mapper, ok := value.(traits.Mapper)
	if !ok {
		return text
	}

	var names []string
	held := 0
	for it := mapper.Iterator(); it.HasNext() == types.True; {
		held++
		if name, ok := it.Next().Value().(string); ok && declaredNameShape(name) {
			names = append(names, name)
		}
	}
	if len(names) == 0 {
		if held > 0 {
			return text + "; none of its names can be shown"
		}

		return text + "; there are none"
	}

	slices.Sort(names)
	more := ""
	if len(names) > maxFailureCandidates {
		more = fmt.Sprintf(" (and %d more)", len(names)-maxFailureCandidates)
		names = names[:maxFailureCandidates]
	}
	work.detail.Candidates = slices.Clone(names)

	return fmt.Sprintf("%s; available: %s%s", text, strings.Join(names, ", "), more)
}

// maxFailureEvals bounds how many operands one failure re-evaluates: a call has
// at most a target and a few arguments, and a missing-key walk a few levels.
const maxFailureEvals = 8

// maxFailureNameLen bounds one listed candidate name.
const maxFailureNameLen = 128

// maxFailureFieldBytes and maxFailureOperands hold the structured account to the
// limits ExpressionFailure declares in service.proto.
const (
	maxFailureFieldBytes = 256
	maxFailureOperands   = 16
)

// failureWork is the one budget a failure's diagnostic work shares. Each operand
// re-evaluation draws on the same cost the failed evaluation was allowed, so
// describing a failure costs at most one more evaluation's worth in total, not
// one per operand. A zero cost limit (tests) is unlimited, as it is elsewhere,
// and the evaluation count still bounds the work.
type failureWork struct {
	remaining uint64
	limited   bool
	evals     int

	// detail is the structured account being built beside the sentence.
	detail *ExpressionFailure
}

func newFailureWork(cost uint64) *failureWork {
	return &failureWork{remaining: cost, limited: cost > 0, evals: maxFailureEvals, detail: &ExpressionFailure{}}
}

// take reserves one operand evaluation, or reports the budget spent.
func (w *failureWork) take() bool {
	if w.evals <= 0 || w.limited && w.remaining == 0 {
		return false
	}
	w.evals--

	return true
}

// limits returns l with its cost budget narrowed to what is left.
func (w *failureWork) limits(l Limits) Limits {
	if l.Cost > 0 {
		l.Cost = max(w.remaining, 1)
	}

	return l
}

// spend charges an evaluation's actual cost; an evaluation that did not report
// one (it failed before finishing) is charged what remained, so a cost-limit
// failure ends the diagnostic work.
func (w *failureWork) spend(details *cel.EvalDetails) {
	if !w.limited || w.remaining == 0 {
		return
	}
	if details == nil || details.ActualCost() == nil {
		w.remaining = 0

		return
	}
	w.remaining -= min(*details.ActualCost(), w.remaining)
	if w.remaining == 0 {
		w.evals = 0
	}
}

// evalOperand evaluates one operand of the failing node under the activation
// and the evaluator's limits.
func (e *Evaluator) evalOperand(ctx context.Context, env *cel.Env, parsed *v1alpha1.ParsedExpr, activation any, work *failureWork, bound []string, operand *v1alpha1.Expr) (value ref.Val, ok bool) {
	if readsAny(operand, bound) || !work.take() {
		return nil, false
	}
	programEnv, err := e.extendedEnvFor(env)
	if err != nil {
		return nil, false
	}
	prg, err := programEnv.Program(cel.ParsedExprToAst(&v1alpha1.ParsedExpr{Expr: operand, SourceInfo: parsed.GetSourceInfo()}), work.limits(e.limits).programOptions()...)
	if err != nil {
		return nil, false
	}
	out, details, err := prg.ContextEval(ctx, activation)
	work.spend(details)
	if err != nil || out == nil {
		return nil, false
	}

	return out, true
}

// findNode returns the node with id, and the comprehension variables bound at
// that point (`[..].map(x, ..)` binds x), or nil. Bounded by [maxFailureNodes], so
// a pathological expression cannot make a failure report the expensive part of
// a failure.
//
// The bound names matter because an operand is re-evaluated on its own, against
// the run's activation alone: an operand that reads a variable a comprehension
// bound would resolve it to whatever the activation holds under that name, and
// say something false. Such an operand is reported as `?` instead.
func findNode(root *v1alpha1.Expr, id int64) (*v1alpha1.Expr, []string) {
	visited := 0

	var walk func(node *v1alpha1.Expr, bound []string) (*v1alpha1.Expr, []string)
	walk = func(node *v1alpha1.Expr, bound []string) (*v1alpha1.Expr, []string) {
		if node == nil || visited >= maxFailureNodes {
			return nil, nil
		}
		visited++

		if node.GetId() == id {
			return node, bound
		}

		var children []*v1alpha1.Expr
		inner := bound

		switch kind := node.GetExprKind().(type) {
		case *v1alpha1.Expr_CallExpr:
			children = append(children, kind.CallExpr.GetTarget())
			children = append(children, kind.CallExpr.GetArgs()...)
		case *v1alpha1.Expr_SelectExpr:
			children = append(children, kind.SelectExpr.GetOperand())
		case *v1alpha1.Expr_ListExpr:
			children = append(children, kind.ListExpr.GetElements()...)
		case *v1alpha1.Expr_StructExpr:
			for _, entry := range kind.StructExpr.GetEntries() {
				children = append(children, entry.GetValue(), entry.GetMapKey())
			}
		case *v1alpha1.Expr_ComprehensionExpr:
			c := kind.ComprehensionExpr
			// The range is evaluated outside the comprehension's own names.
			if found, names := walk(c.GetIterRange(), bound); found != nil {
				return found, names
			}
			inner = append(slices.Clone(bound), c.GetIterVar(), c.GetIterVar2(), c.GetAccuVar())
			children = append(children, c.GetAccuInit(), c.GetLoopCondition(), c.GetLoopStep(), c.GetResult())
		}

		for _, child := range children {
			if found, names := walk(child, inner); found != nil {
				return found, names
			}
		}

		return nil, nil
	}

	return walk(root, nil)
}

// readsAny reports whether an expression names any of the identifiers.
func readsAny(node *v1alpha1.Expr, names []string) bool {
	if node == nil || len(names) == 0 {
		return false
	}

	switch kind := node.GetExprKind().(type) {
	case *v1alpha1.Expr_IdentExpr:
		return slices.Contains(names, kind.IdentExpr.GetName())
	case *v1alpha1.Expr_CallExpr:
		if readsAny(kind.CallExpr.GetTarget(), names) {
			return true
		}
		return slices.ContainsFunc(kind.CallExpr.GetArgs(), func(n *v1alpha1.Expr) bool { return readsAny(n, names) })
	case *v1alpha1.Expr_SelectExpr:
		return readsAny(kind.SelectExpr.GetOperand(), names)
	case *v1alpha1.Expr_ListExpr:
		return slices.ContainsFunc(kind.ListExpr.GetElements(), func(n *v1alpha1.Expr) bool { return readsAny(n, names) })
	case *v1alpha1.Expr_StructExpr:
		return slices.ContainsFunc(kind.StructExpr.GetEntries(), func(e *v1alpha1.Expr_CreateStruct_Entry) bool {
			return readsAny(e.GetValue(), names) || readsAny(e.GetMapKey(), names)
		})
	case *v1alpha1.Expr_ComprehensionExpr:
		c := kind.ComprehensionExpr
		return readsAny(c.GetIterRange(), names) || readsAny(c.GetAccuInit(), names) ||
			readsAny(c.GetLoopCondition(), names) || readsAny(c.GetLoopStep(), names) || readsAny(c.GetResult(), names)
	}

	return false
}

// stepsMapOperand reports whether an expression is `steps` itself or one
// selection of it, `steps.<id>`: the two maps whose keys are the author's own
// names (step ids, and the outputs a step declares).
//
// No deeper. `steps.fetch.value` is data a step produced, and its keys are the
// data's: an email address, an id, a token-shaped name. A failure text is
// recorded durably and shown to callers, so those are not listed.
func stepsMapOperand(node *v1alpha1.Expr) bool {
	if ident := node.GetIdentExpr(); ident != nil {
		return ident.GetName() == "steps"
	}

	selection := node.GetSelectExpr()

	return selection != nil && selection.GetOperand().GetIdentExpr().GetName() == "steps"
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

	return textbound.Truncate(text, maxFailureSubexpr)
}

// declaredNameShape reports whether name is spelled like something an author
// declares: a step id or an output name, which validation holds to identifiers.
// The names listed come from the run's own map, so this is the guard that keeps
// a key shaped like data (an address, a path, a token with punctuation) out of
// a durable sentence even if a step kind ever put one there. It is defense in
// depth, not the control: that is listing only `steps` and `steps.<id>`.
func declaredNameShape(name string) bool {
	return len(name) <= maxFailureNameLen && IsCELIdentifier(name)
}

// operatorName is the operator as an author writes it (`+`), or the function's
// own name when it has no operator spelling.
func operatorName(function string) string {
	if symbol, ok := operators.FindReverse(function); ok && symbol != "" {
		return symbol
	}
	if function == operators.Conditional {
		return "?:"
	}

	return function
}

// offsetOf is where node sits in the expression's text, or nil when the parsed
// expression carries no position for it.
func offsetOf(parsed *v1alpha1.ParsedExpr, node *v1alpha1.Expr) *int32 {
	offset, ok := parsed.GetSourceInfo().GetPositions()[node.GetId()]
	if !ok || offset < 0 {
		return nil
	}

	return &offset
}

// callCaret is the character index of a call's operator or name within subexpr,
// the text unparseNode made of the call, or nil when subexpr does not place it
// exactly.
//
// Exact rather than searched: the unparser parenthesizes by precedence, so the
// operands' own renderings are put back together and compared to subexpr, and a
// mismatch (parentheses added, text cut) yields no caret instead of one under a
// neighbouring character.
func callCaret(parsed *v1alpha1.ParsedExpr, call *v1alpha1.Expr_Call, subexpr string) *int32 {
	args := call.GetArgs()
	render := func(node *v1alpha1.Expr) string { return unparseNode(parsed, node) }

	var prefix string
	switch symbol, _ := operators.FindReverse(call.GetFunction()); {
	case call.GetFunction() == operators.Conditional && len(args) == 3:
		prefix = render(args[0])
		if subexpr != prefix+" ? "+render(args[1])+" : "+render(args[2]) {
			return nil
		}
		return caretAt(subexpr, len(prefix)+1)
	case call.GetFunction() == operators.Index && len(args) == 2:
		prefix = render(args[0])
		if subexpr != prefix+"["+render(args[1])+"]" {
			return nil
		}
		return caretAt(subexpr, len(prefix))
	case symbol != "" && len(args) == 2 && call.GetTarget() == nil:
		prefix = render(args[0])
		if subexpr != prefix+" "+symbol+" "+render(args[1]) {
			return nil
		}
		return caretAt(subexpr, len(prefix)+1)
	case call.GetTarget() != nil:
		prefix = render(call.GetTarget()) + "."
		if !strings.HasPrefix(subexpr, prefix+call.GetFunction()+"(") {
			return nil
		}
		return caretAt(subexpr, len(prefix))
	case symbol == "" && strings.HasPrefix(subexpr, call.GetFunction()+"("):
		return caretAt(subexpr, 0)
	}

	return nil
}

// selectCaret is the character index of the selected name within subexpr, or nil
// when subexpr (a cut or backtick-quoted rendering) does not end in `.field`.
func selectCaret(field, subexpr string) *int32 {
	if field == "" || !strings.HasSuffix(subexpr, "."+field) {
		return nil
	}

	return caretAt(subexpr, len(subexpr)-len(field))
}

// caretAt converts a byte index in text to a character index, or nil when the
// index is out of range.
func caretAt(text string, index int) *int32 {
	if index < 0 || index >= len(text) {
		return nil
	}
	column := int32(utf8.RuneCountInString(text[:index]))

	return &column
}

// Excerpt renders the failing subexpression with a caret under the operator or
// selected name, indented by indent, or "" when the failure carries no caret.
// It is built from the structured account alone, so every driver and every
// surface that holds an [ExpressionFailure] draws the same two lines.
func (f *ExpressionFailure) Excerpt(indent string) string {
	if f == nil || f.Caret == nil || f.GetSubexpression() == "" {
		return ""
	}
	// A response is data from a peer: a caret outside the text it points into
	// would panic strings.Repeat (negative) or allocate what the peer chose.
	if column := int(f.GetCaret()); column < 0 || column >= utf8.RuneCountInString(f.GetSubexpression()) {
		return ""
	}

	return indent + f.GetSubexpression() + "\n" + indent + strings.Repeat(" ", int(f.GetCaret())) + "^"
}
