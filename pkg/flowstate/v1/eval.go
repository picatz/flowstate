package flowstatev1

import (
	"context"
	"fmt"
	"math"
	"reflect"
	"strings"
	"time"

	"github.com/google/cel-go/cel"
	"github.com/google/cel-go/common/types"
	"github.com/google/cel-go/common/types/ref"
	"github.com/google/cel-go/interpreter"
	expr "google.golang.org/genproto/googleapis/api/expr/v1alpha1"
)

// TypeAdapter is the default type adapter used for CEL evaluation in Flowstate.
//
// In the future this might not just be the default one from the CEL library, but
// a custom one that provides additional functionality or type handling specific to Flowstate
// in the future.
var TypeAdapter = types.DefaultTypeAdapter

// Ensure StepsOutputActivation implements the interpreter.Activation interface.
var _ interpreter.Activation = (*StepsOutputActivation)(nil)

// maxActivationDepth bounds how deeply stored expressions may nest while being
// resolved. A stored expression is evaluated against this same activation, so a
// workflow whose step output references itself would otherwise recurse until the
// stack is exhausted.
const maxActivationDepth = 32

// StepsOutputActivation is a CEL activation that exposes the outputs of earlier
// workflow steps to an expression.
//
// A name is resolved as a step ID, optionally followed by an output name, so an
// expression may reference either a whole step's outputs or one named output of
// it. Selection deeper than that is left to CEL, which applies it to the
// returned value.
type StepsOutputActivation struct {
	// Prev holds the outputs of steps that have already run.
	Prev *Workflow_StepOutputs

	// AmbientVars holds the rooted `vars.<name>` namespace: the workflow's declared
	// vars plus any an enclosing scope added, an inner shadowing an outer.
	//
	// Answered as a whole root, the way step outputs are, so this needs no idea how
	// deep a reference goes. See [VarsRoot].
	AmbientVars map[string]ref.Val

	// Locals holds names bound *where the expression is written* — a loop's current
	// item, a step's own `vars:`, `now` inside a wait — resolved bare. They are
	// resolved before step outputs, so a loop body can refer to its iterator by name.
	//
	// Resolution stops at the local itself: an expression selecting into it, like
	// item.name, is resolved by returning the item and letting CEL apply the
	// selection — the same contract step outputs follow.
	Locals map[string]ref.Val

	// Ctx bounds evaluation of any stored expression encountered while
	// resolving a name. A context is held here, rather than passed in,
	// because ResolveName implements a fixed third-party interface that has
	// no place to thread one. Nil is treated as [context.Background].
	Ctx context.Context

	// Eval evaluates stored expressions. Nil uses [DefaultEvaluator].
	Eval *Evaluator

	// Profile is the workflow's language profile.
	//
	// Carried here because resolving a stored expression re-enters evaluation with
	// no scope in hand — see resolveValue, which builds a child activation rather
	// than threading a scope it does not have. Without it that inner evaluation
	// would fall back to whatever the build calls current, so one run could resolve
	// its outer expression against the profile it recorded and an expression nested
	// inside an output against a different one.
	Profile string

	// depth counts nested stored-expression evaluations, bounded by
	// maxActivationDepth.
	depth int
}

// evaluator returns the evaluator to use, defaulting to the shared one.
func (e *StepsOutputActivation) evaluator() *Evaluator {
	if e.Eval != nil {
		return e.Eval
	}
	return DefaultEvaluator()
}

// context returns the context to evaluate under, defaulting to a background
// context.
func (e *StepsOutputActivation) context() context.Context {
	if e.Ctx != nil {
		return e.Ctx
	}
	return context.Background()
}

// ResolveName resolves a step ID, or a step ID and output name joined by a dot,
// to a CEL value.
//
// Reporting failure as a missing name (rather than an error) is what CEL's
// activation contract allows, so an evaluation error while resolving a stored
// expression surfaces to the author as an unresolved reference.
func (e *StepsOutputActivation) ResolveName(name string) (any, bool) {
	// A name bound by enclosing control flow — a loop's iterator, `now` inside a
	// wait — wins over a step of the same id, and that is now a real precedence
	// rule rather than a case validation rules out.
	//
	// It used to be ruled out: an iterator was refused if it collided with a step
	// id, so whichever was checked first gave the same answer. Rooting deleted that
	// rule, because the two stopped sharing a namespace — a step is `steps.<id>`
	// and a binding is bare, so a loop over `item` beside a step called `item` is
	// an ordinary file now.
	//
	// Which leaves only the retired spelling to order against, and bindings have to
	// win it. A bare `item` is the live way to say the iterator and the legacy way
	// to say the step; resolving it to the step would break a correct loop in order
	// to keep answering a spelling this migration exists to retire.
	if v, ok := e.Locals[name]; ok {
		return v, true
	}

	// The rooted vars namespace, answered whole for the same reason [StepsRoot] is:
	// CEL resolves `vars.a.b` by asking for that, then `vars.a`, then `vars`, so
	// answering the shortest and letting CEL apply the rest means this needs no idea
	// how deep a reference goes.
	//
	// Asked before step outputs and after locals, which is a precedence that cannot
	// actually be observed — `vars` is a reserved name that no step may take, and a
	// local called `vars` would have to be a loop iterator named `vars`, which
	// validation refuses. Ordered explicitly anyway, because "unreachable" is a
	// property of today's rules and this is cheaper than rediscovering that.
	if name == VarsRoot {
		if len(e.AmbientVars) == 0 {
			// An empty root still resolves, to an empty map. Otherwise a workflow
			// with no vars makes `vars.missing` an *unresolved reference* rather
			// than a missing key, and the diagnostic sends the author looking for
			// the wrong mistake.
			return types.NewStringInterfaceMap(TypeAdapter, nil), true
		}

		entries := make(map[ref.Val]ref.Val, len(e.AmbientVars))
		for varName, v := range e.AmbientVars {
			entries[types.String(varName)] = v
		}

		return types.NewRefValMap(TypeAdapter, entries), true
	}

	// The root resolves whether or not anything has run, which is what makes it a
	// root rather than a name a particular moment happens to have.
	//
	// This used to sit below the guard on the next line, so a scope carrying no
	// outputs answered `steps` as *unbound* while answering `vars` as an empty map —
	// one root that is always there and one that appears once a step has finished.
	// `size(steps)` before the first step is zero, not a mistake.
	//
	// Checked before the step lookup only for the empty case: where there *are*
	// outputs, a step literally called `steps` still wins, which is the arm below and
	// the reason it is answered last. A spec compiled before this root existed may
	// contain one, and a worker evaluates the stored AST rather than re-parsing it.
	if len(e.Prev.GetStepValues()) == 0 {
		if name == StepsRoot {
			root, err := e.stepsMap()
			if err != nil {
				return nil, false
			}

			return root, true
		}

		return nil, false
	}

	stepName, outputName, hasOutput := strings.Cut(name, ".")

	outputs, hasVal := e.Prev.StepValues[stepName]
	if !hasVal {
		// Not a step. It may still be the root every step hangs from.
		//
		// Answered last so that nothing already resolvable changes meaning: a spec
		// compiled before this root existed may contain a step literally called
		// `steps`, and its own outputs still win. That matters because a worker
		// evaluates the *stored* AST out of RunState rather than re-parsing, so a
		// run started on an older build keeps resolving the way it always did —
		// invariant 10, which is why this arm exists at all.
		if name == StepsRoot {
			root, err := e.stepsMap()
			if err != nil {
				return nil, false
			}
			return root, true
		}
		return nil, false
	}
	if !hasOutput {
		// Return CEL-native values, not the protobuf message. CEL has no type
		// registered for Node.Outputs, so handing it back makes any expression
		// referencing a bare step ID fail with "unknown type" — including the
		// fallback CEL performs when a longer name does not resolve.
		vals, err := e.outputsToMap(outputs)
		if err != nil {
			return nil, false
		}
		return vals, true
	}

	// A name addresses at most a step and one of its outputs. Anything longer
	// must be reported as unresolved rather than answered with the output
	// value, because CEL resolves a qualified name by trying successively
	// shorter prefixes: claiming to resolve "step.output.field" would consume
	// the qualifiers CEL needs to apply itself, yielding the whole output where
	// the author asked for one field inside it.
	if strings.Contains(outputName, ".") {
		return nil, false
	}

	v, hasField := outputs.GetNamedValues()[outputName]
	if !hasField {
		return nil, false
	}

	rv, err := e.resolveValue(v)
	if err != nil {
		return nil, false
	}
	return rv, true
}

// StepsRoot is the name every step's outputs hang from in an expression, as
// `steps.<id>.<output>`.
//
// Rooting the ambient half of the namespace is what makes a collision between a
// step id and a locally bound name unrepresentable, rather than something a
// validation rule has to forbid. See docs/DSL.md.
const StepsRoot = "steps"

// VarsRoot is the name declared variables hang from in an expression, as
// `vars.<name>`.
//
// Rooted for the same reason [StepsRoot] is, and it buys the same thing: a var and a
// step of one name cannot collide, so there is no rule to write and none to get wrong.
// What is deliberately *not* rooted is a local — a loop iterator, `now` — because a
// local's binding is visible where the expression is written and `${item.name}` reads
// better than `${vars.item.name}`.
const VarsRoot = "vars"

// ResponseRoot is the name an http task's `expect:` and `outputs:` expressions reach the
// response through: `${response.status_code}`.
//
// A third root, for the third set of system-chosen names — after `steps.<id>` and the
// signal payload's `payload.*`. It is private to the task rather than ambient, which is
// why it is bound by the task and not by [Scope]: only the two inputs the task evaluates
// itself can see it, and a step's ordinary inputs cannot, because there is no response
// yet when those are resolved.
const ResponseRoot = "response"

// stepsMap returns every step's outputs as one CEL map, keyed by step id.
//
// The whole root is handed back rather than a prefix being parsed here, because
// CEL resolves a qualified name by trying successively shorter prefixes: given
// `steps.a.result` it asks for that, then `steps.a`, then `steps`. Answering the
// last one and letting CEL apply `.a.result` itself means this needs no idea how
// deep a reference goes — which is exactly the bug the bare form has to guard
// against by refusing any name with a second dot in it.
func (e *StepsOutputActivation) stepsMap() (ref.Val, error) {
	values := e.Prev.GetStepValues()
	entries := make(map[ref.Val]ref.Val, len(values))
	for id, outputs := range values {
		vals, err := e.outputsToMap(outputs)
		if err != nil {
			return nil, err
		}
		entries[types.String(id)] = vals
	}
	return types.NewRefValMap(TypeAdapter, entries), nil
}

// outputsToMap converts a step's outputs into a CEL map, resolving each value to
// a native CEL value so that CEL can apply its own selection and indexing.
func (e *StepsOutputActivation) outputsToMap(outputs *Node_Outputs) (ref.Val, error) {
	named := outputs.GetNamedValues()
	entries := make(map[ref.Val]ref.Val, len(named))
	for name, v := range named {
		rv, err := e.resolveValue(v)
		if err != nil {
			return nil, err
		}
		entries[types.String(name)] = rv
	}
	return types.NewRefValMap(TypeAdapter, entries), nil
}

// resolveValue converts a stored value into a CEL value, evaluating it first if
// it is an expression.
func (e *StepsOutputActivation) resolveValue(v *Value) (ref.Val, error) {
	switch v.GetKind().(type) {
	case *Value_Expr:
		if e.depth >= maxActivationDepth {
			return nil, fmt.Errorf("expression nesting exceeded %d levels", maxActivationDepth)
		}
		child := &StepsOutputActivation{
			Prev:    e.Prev,
			Ctx:     e.Ctx,
			Eval:    e.Eval,
			Profile: e.Profile,
			depth:   e.depth + 1,
		}
		return e.evaluator().EvalParsedBase(e.context(), e.Profile, v.GetExpr(), cel.Activation(child))
	case *Value_Literal:
		return cel.ValueToRefValue(TypeAdapter, v.GetLiteral())

	case *Value_SecretRef:
		// A secret reference is deliberately unresolvable here. Resolving it
		// would produce a value in workflow code, and anything a workflow
		// computes can end up in history — which is exactly what referencing a
		// secret instead of embedding it is meant to prevent. Only the activity
		// that needs the value resolves it, worker-side.
		return nil, fmt.Errorf("a secret reference cannot be read in an expression; "+
			"pass it to a task input that accepts one (%s:%s)",
			v.GetSecretRef().GetScheme(), v.GetSecretRef().GetName())

	default:
		return nil, fmt.Errorf("unsupported value kind %T", v.GetKind())
	}
}

func (e *StepsOutputActivation) Parent() interpreter.Activation {
	return nil
}

func NewLiteralList(vals ...any) *Value {
	literals := make([]*expr.Value, 0, len(vals))
	for _, v := range vals {
		// NewValue rather than NewLiteral: NewLiteral handles only scalars, so a
		// list of maps or of nested lists silently became a list of error values
		// whose literal is nil — which is how a loop's per-iteration results came
		// out empty.
		literals = append(literals, NewValue(v).GetLiteral())
	}
	return &Value{
		Kind: &Value_Literal{
			Literal: &expr.Value{
				Kind: &expr.Value_ListValue{
					ListValue: &expr.ListValue{
						Values: literals,
					},
				},
			},
		},
	}
}

func NewLiteralMap(m map[string]any) *Value {
	if m == nil {
		return &Value{Kind: &Value_Literal{Literal: &expr.Value{Kind: &expr.Value_NullValue{}}}}
	}

	entries := make([]*expr.MapValue_Entry, 0, len(m))
	for k, v := range m {
		entries = append(entries, &expr.MapValue_Entry{
			Key:   NewLiteral(k).GetLiteral(),
			Value: NewValue(v).GetLiteral(),
		})
	}

	return &Value{
		Kind: &Value_Literal{
			Literal: &expr.Value{
				Kind: &expr.Value_MapValue{
					MapValue: &expr.MapValue{Entries: entries},
				},
			},
		},
	}
}

func NewLiteral(val any) *Value {
	switch v := val.(type) {
	case string:
		return &Value{
			Kind: &Value_Literal{
				Literal: &expr.Value{
					Kind: &expr.Value_StringValue{
						StringValue: v,
					},
				},
			},
		}
	case int:
		return &Value{
			Kind: &Value_Literal{
				Literal: &expr.Value{
					Kind: &expr.Value_Int64Value{
						Int64Value: int64(v),
					},
				},
			},
		}
	case float64:
		return &Value{
			Kind: &Value_Literal{
				Literal: &expr.Value{
					Kind: &expr.Value_DoubleValue{
						DoubleValue: v,
					},
				},
			},
		}
	case float32:
		return &Value{
			Kind: &Value_Literal{
				Literal: &expr.Value{
					Kind: &expr.Value_DoubleValue{
						DoubleValue: float64(v),
					},
				},
			},
		}
	case int64:
		return &Value{
			Kind: &Value_Literal{
				Literal: &expr.Value{
					Kind: &expr.Value_Int64Value{
						Int64Value: v,
					},
				},
			},
		}
	case bool:
		return &Value{
			Kind: &Value_Literal{
				Literal: &expr.Value{
					Kind: &expr.Value_BoolValue{
						BoolValue: v,
					},
				},
			},
		}
	case *expr.Value:
		return &Value{
			Kind: &Value_Literal{
				Literal: v,
			},
		}
	case error:
		return &Value{
			Kind: &Value_Error_{
				Error: &Value_Error{
					Message: fmt.Errorf("flowstatev1: error value: %w", v).Error(),
					Code:    Value_Error_CODE_INTERNAL,
				},
			},
		}
	default:
		return &Value{
			Kind: &Value_Error_{
				Error: &Value_Error{
					Message: fmt.Sprintf("flowstatev1: unsupported type for new value: %T", v),
					Code:    Value_Error_CODE_INTERNAL,
				},
			},
		}
	}
}

func NewExpr(exprStr string) *Value {
	v, err := newValueExprWithErr(exprStr)
	if err != nil {
		return &Value{
			Kind: &Value_Error_{
				Error: &Value_Error{
					Message: fmt.Errorf("failed to create CEL expression: %w", err).Error(),
					Code:    Value_Error_CODE_INTERNAL,
				},
			},
		}
	}
	return v
}

func newValueExprWithErr(exprStr string) (*Value, error) {
	env, err := DefaultEvaluator().Env()
	if err != nil {
		return nil, fmt.Errorf("failed to create CEL environment: %w", err)
	}

	ast, issues := env.Parse(exprStr)
	if issues != nil && issues.Err() != nil {
		return nil, fmt.Errorf("failed to parse CEL expression: %w", issues.Err())
	}

	parsedExpr, err := cel.AstToParsedExpr(ast)
	if err != nil {
		return nil, fmt.Errorf("failed to convert AST to parsed expression: %w", err)
	}

	return &Value{
		Kind: &Value_Expr{
			Expr: parsedExpr,
		},
	}, nil
}

func NewValue(v any) *Value {
	if v == nil {
		return &Value{
			Kind: &Value_Literal{
				Literal: &expr.Value{
					Kind: &expr.Value_NullValue{},
				},
			},
		}
	}

	switch val := v.(type) {
	case *Value:
		return val
	case map[string]any:
		return NewLiteralMap(val)
	case string, int, float64, float32, int64, bool, *expr.Value:
		return NewLiteral(val)
	case []any:
		return NewLiteralList(val...)
	case error:
		return &Value{
			Kind: &Value_Error_{
				Error: &Value_Error{
					Message: fmt.Errorf("flowstatev1: error value: %w", val).Error(),
					Code:    Value_Error_CODE_INTERNAL,
				},
			},
		}
	default:
		// Handle other slice types using reflection
		rv := reflect.ValueOf(v)
		if rv.Kind() == reflect.Slice {
			// Convert slice to []any
			slice := make([]any, rv.Len())
			for i := 0; i < rv.Len(); i++ {
				slice[i] = rv.Index(i).Interface()
			}
			return NewLiteralList(slice...)
		}

		if rv.Kind() == reflect.Map && rv.Type().Key().Kind() == reflect.String {
			m := make(map[string]any, rv.Len())
			iter := rv.MapRange()
			for iter.Next() {
				m[iter.Key().String()] = iter.Value().Interface()
			}
			return NewLiteralMap(m)
		}

		return &Value{
			Kind: &Value_Error_{
				Error: &Value_Error{
					Message: fmt.Sprintf("flowstatev1: unsupported type for new value: %T", val),
					Code:    Value_Error_CODE_INTERNAL,
				},
			},
		}
	}
}

func NewNamedValues(inputValues map[string]any) map[string]*Value {
	if inputValues == nil {
		return nil
	}

	outputValues := make(map[string]*Value, len(inputValues))
	for name, val := range inputValues {
		outputValues[name] = NewValue(val)
	}
	return outputValues
}

func (v *Value) Error() error {
	if errKind := v.GetError(); errKind != nil {
		return fmt.Errorf("flowstatev1: value error: %s (code: %s)", errKind.Message, errKind.Code.String())
	}
	return nil
}

func Run(ctx context.Context, w *Workflow) (*Workflow_StepOutputs, error) {
	return eval(ctx, w)
}

func eval(ctx context.Context, w *Workflow) (*Workflow_StepOutputs, error) {
	if w == nil || len(w.Steps) == 0 {
		return nil, fmt.Errorf("workflow cannot be nil or empty")
	}

	stepOutputs := &Workflow_StepOutputs{
		StepValues: make(map[string]*Node_Outputs),
	}

	// Evaluated before any step, which is what makes them ambient: every step sees
	// the same set, so there is no ordering for an author to reason about.
	vars, err := EvalWorkflowVars(ctx, w)
	if err != nil {
		return nil, err
	}

	scope := NewScope(w.GetProfile(), stepOutputs)
	scope.AmbientVars = vars

	if err := runNodes(ctx, w.Steps, scope); err != nil {
		return nil, err
	}
	return stepOutputs, nil
}

// runNodes executes a list of nodes in order, writing their outputs into out.
//
// Conditions, timeouts, retries, continue-on-error, loops, and parallel branches
// behave the same here as they do under durable execution. Local runs exist to
// tell an author what will happen in production, so a local run that disagrees is
// worse than no local run at all.
//
// It is recursive because control flow nests: a loop body may contain a parallel
// block whose branches contain further loops.
func runNodes(ctx context.Context, nodes []*Node, scope *Scope) error {
	for _, node := range nodes {
		run, err := EvalConditionInScope(ctx, node.GetCondition(), scope)
		if err != nil {
			return fmt.Errorf("step %q: %w", node.GetId(), err)
		}
		if !run {
			continue
		}

		// The step's own `vars:`, bound for this node and its body only. Evaluated
		// after the condition deliberately: `if:` decides whether the step runs at
		// all, so a var it declares does not exist yet when the question is asked —
		// and a var whose expression would fail must not fail a step that is skipped.
		inner, err := EvalStepVars(ctx, node, scope)
		if err != nil {
			return fmt.Errorf("step %q: %w", node.GetId(), err)
		}

		outputs, err := runNode(ctx, node, inner)
		if err != nil {
			if !node.GetPolicy().GetContinueOnError() {
				return fmt.Errorf("step %q: %w", node.GetId(), err)
			}
			scope.Outputs.StepValues[node.GetId()] = &Node_Outputs{
				NamedValues: map[string]*Value{"error": NewLiteral(err.Error())},
			}
			continue
		}
		if outputs != nil {
			scope.Outputs.StepValues[node.GetId()] = outputs
		}
	}
	return nil
}

// runNode executes one node and returns the outputs it records.
func runNode(ctx context.Context, node *Node, scope *Scope) (*Node_Outputs, error) {
	switch n := node.Kind.(type) {
	case *Node_Task:
		return runStepWithPolicy(ctx, n.Task, node.GetPolicy(), scope)

	case *Node_ForEach:
		return runForEach(ctx, n.ForEach, scope)

	case *Node_Parallel:
		return nil, runParallel(ctx, n.Parallel, scope)

	case *Node_Wait:
		return runWait(ctx, node, n.Wait, scope)

	default:
		return nil, fmt.Errorf("unsupported node kind: %T", n)
	}
}

// runForEach runs a loop body once per item.
//
// Iterations run sequentially here regardless of MaxParallel. The durable driver
// honors it, but reproducing concurrency locally would only reorder side effects
// without reproducing anything an author can act on — and sequential execution
// makes a local run's output deterministic, which is what makes it useful for
// comparison.
func runForEach(ctx context.Context, loop *ForEach, scope *Scope) (*Node_Outputs, error) {
	items, err := ResolveItems(ctx, loop, scope)
	if err != nil {
		return nil, err
	}

	name := IteratorName(loop)
	iterations := make([]*Workflow_StepOutputs, 0, len(items))

	for i, item := range items {
		// Each iteration gets its own output scope, seeded with what was visible
		// before the loop. Body steps therefore cannot see a previous iteration's
		// outputs, which keeps an iteration's behavior independent of how many
		// ran before it.
		iterationOutputs := &Workflow_StepOutputs{StepValues: map[string]*Node_Outputs{}}
		for k, v := range scope.GetOutputs().GetStepValues() {
			iterationOutputs.StepValues[k] = v
		}

		// A local, not a var: the iterator is bound right where the body's
		// expressions are written, so it stays bare.
		iterationScope := scope.WithLocal(name, item)
		iterationScope.Outputs = iterationOutputs

		if err := runNodes(ctx, loop.GetBody(), iterationScope); err != nil {
			return nil, fmt.Errorf("iteration %d: %w", i, err)
		}

		iterations = append(iterations, onlyBodyOutputs(loop.GetBody(), iterationOutputs))
	}

	return LoopOutputs(iterations), nil
}

// onlyBodyOutputs narrows an iteration's scope to the outputs its own body
// produced, so a loop's results describe the loop rather than repeating whatever
// preceded it.
func onlyBodyOutputs(body []*Node, scope *Workflow_StepOutputs) *Workflow_StepOutputs {
	result := &Workflow_StepOutputs{StepValues: map[string]*Node_Outputs{}}
	for _, node := range body {
		if outputs, ok := scope.GetStepValues()[node.GetId()]; ok {
			result.StepValues[node.GetId()] = outputs
		}
	}
	return result
}

// runParallel runs each branch and merges their outputs.
//
// Branches run sequentially in the local driver for the same reason loop
// iterations do: determinism. Because branches may not depend on each other's
// outputs, running them in order produces the same result the durable driver
// reaches concurrently.
func runParallel(ctx context.Context, parallel *Parallel, scope *Scope) error {
	before := &Workflow_StepOutputs{StepValues: map[string]*Node_Outputs{}}
	for k, v := range scope.GetOutputs().GetStepValues() {
		before.StepValues[k] = v
	}

	for i, branch := range parallel.GetBranches() {
		// Every branch starts from the outputs that existed before the block, so
		// a branch cannot observe a sibling's work even though they run in order
		// here. A workflow that accidentally depended on that would behave
		// differently under concurrent execution.
		branchOutputs := &Workflow_StepOutputs{StepValues: map[string]*Node_Outputs{}}
		for k, v := range before.GetStepValues() {
			branchOutputs.StepValues[k] = v
		}

		// Derived rather than rebuilt. A hand-built Scope here is how the profile
		// went missing in the first place: it names the two fields somebody was
		// thinking about, and silently omits every other one the type grows.
		branchScope := scope.WithOutputs(branchOutputs)
		if err := runNodes(ctx, branch.GetSteps(), branchScope); err != nil {
			return fmt.Errorf("branch %d: %w", i, err)
		}

		for _, node := range branch.GetSteps() {
			if outputs, ok := branchOutputs.GetStepValues()[node.GetId()]; ok {
				scope.Outputs.StepValues[node.GetId()] = outputs
			}
		}
	}
	return nil
}

// EvalCondition reports whether a step's condition allows it to run.
//
// A nil condition means the step always runs. A condition that does not produce a
// boolean is an error rather than being coerced, so a mistake surfaces instead of
// being silently interpreted.
// The profile is empty because this signature has no workflow to read one from,
// which resolves to the first profile — see [ProfileLibraries]. Callers executing
// a real workflow reach [EvalConditionInScope] with a scope the engine built, and
// that scope carries the profile its spec recorded.
func EvalCondition(ctx context.Context, condition *Value, prev *Workflow_StepOutputs) (bool, error) {
	return EvalConditionInScope(ctx, condition, NewScope("", prev))
}

// EvalConditionInScope evaluates a condition against a scope, so a loop body can
// guard on its own item as well as on earlier steps' outputs.
func EvalConditionInScope(ctx context.Context, condition *Value, scope *Scope) (bool, error) {
	if condition == nil {
		return true, nil
	}

	ev := DefaultEvaluator()
	switch kind := condition.GetKind().(type) {
	case *Value_Literal:
		b, ok := kind.Literal.GetKind().(*expr.Value_BoolValue)
		if !ok {
			return false, fmt.Errorf("condition must be a boolean, got %s", literalKindName(kind.Literal))
		}
		return b.BoolValue, nil

	case *Value_Expr:
		out, err := ev.EvalParsedBase(ctx, scope.GetProfile(), kind.Expr, scope.Activation(ctx))
		if err != nil {
			return false, fmt.Errorf("evaluating condition: %w", err)
		}
		b, ok := out.Value().(bool)
		if !ok {
			return false, fmt.Errorf("condition must evaluate to a boolean, got %s", out.Type())
		}
		return b, nil

	default:
		return false, fmt.Errorf("unsupported condition kind %T", condition.GetKind())
	}
}

// runStepWithPolicy executes a task, applying the step's timeout and retrying
// failures the policy allows.
//
// Retries here are in-process and therefore not durable: a crash loses them,
// which is exactly the difference durable execution removes. They exist so that a
// local run reproduces the same observable outcome — a flaky dependency that
// succeeds on the second attempt succeeds in both places — rather than to make
// local execution reliable.
func runStepWithPolicy(ctx context.Context, task *Task, policy *StepPolicy, scope *Scope) (*Node_Outputs, error) {
	attempts := 1
	if retry := policy.GetRetry(); retry != nil && retry.GetMaxAttempts() > 0 {
		attempts = int(retry.GetMaxAttempts())
	}

	var err error
	for attempt := 1; ; attempt++ {
		var out *Node_Outputs
		out, err = runStepAttempt(ctx, task, policy, scope)
		if err == nil {
			return out, nil
		}

		// Only failures that could plausibly succeed on another attempt are
		// retried, matching how the durable driver classifies them.
		if attempt >= attempts || !ClassifyError(err).Retryable() {
			return nil, err
		}

		delay := retryDelay(policy.GetRetry(), attempt)
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-time.After(delay):
		}
	}
}

// runStepAttempt performs one attempt, bounded by the step's timeout.
func runStepAttempt(ctx context.Context, task *Task, policy *StepPolicy, scope *Scope) (*Node_Outputs, error) {
	if timeout := policy.GetTimeout().AsDuration(); timeout > 0 {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, timeout)
		defer cancel()
	}
	return task.EvalInScope(ctx, scope)
}

// retryDelay returns how long to wait before the next attempt.
func retryDelay(retry *RetryPolicy, attempt int) time.Duration {
	interval := retry.GetInitialInterval().AsDuration()
	if interval <= 0 {
		interval = time.Second
	}

	backoff := retry.GetBackoffCoefficient()
	if backoff < 1 {
		backoff = 2
	}

	delay := float64(interval) * math.Pow(backoff, float64(attempt-1))
	if max := retry.GetMaxInterval().AsDuration(); max > 0 && time.Duration(delay) > max {
		return max
	}
	return time.Duration(delay)
}

// Evaluates against the first profile, since this signature carries no workflow.
// The engine reaches this only for a task that does not need previous outputs,
// which is a task with no deferred inputs and so nothing profile-sensitive to
// evaluate; anything that does evaluate an expression arrives through
// [Task.EvalInScope] with the run's scope.
func (t *Task) Eval(ctx context.Context, prevStepOutputs *Workflow_StepOutputs) (*Node_Outputs, error) {
	return t.EvalInScope(ctx, NewScope("", prevStepOutputs))
}

// EvalInScope executes the task against a scope, which is how a loop body's
// inputs and a task's own expressions reference the current item.
//
// Expression inputs are resolved into a copy of the task before execution, which
// is the single place resolution happens for the local driver — the durable driver
// resolves the same way before scheduling an activity, so both agree on what a
// task receives.
func (t *Task) EvalInScope(ctx context.Context, scope *Scope) (*Node_Outputs, error) {
	if t == nil {
		return nil, fmt.Errorf("task cannot be nil")
	}
	if scope.BindsNames() {
		resolved, err := ResolveTaskInputs(ctx, t, scope)
		if err != nil {
			return nil, err
		}
		t = resolved
	}
	if t == nil {
		return nil, fmt.Errorf("task cannot be nil")
	}
	def, ok := LookupTask(t.Name)
	if !ok {
		return nil, NewTaskError(t.Name, ErrorKindUnknownTask, fmt.Errorf(
			"unknown task %q (available: %s)", t.Name, strings.Join(TaskNames(), ", ")))
	}
	return def.Fn(ctx, t.Inputs, scope)
}
