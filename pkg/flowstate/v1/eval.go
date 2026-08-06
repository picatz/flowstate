package flowstatev1

import (
	"context"
	"errors"
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

// maxActivationEvaluations bounds how many stored-expression evaluations one
// resolution may perform in total, shared across every nesting level.
//
// The depth bound above cannot do this job, and measuring proved it: a chain of
// stored expressions where each level references the one below it twice is 2^n
// evaluations at depth n — 71 seconds of CPU at depth 20, with twelve levels of
// headroom still under the depth limit. Each of those evaluations also carried
// its own fresh CEL cost budget, so cost accounting never saw the total either:
// the work was exponential and every meter that existed read near zero. A depth
// bound on a breadth explosion, which CLAUDE.md names as the exact mistake —
// bound the resource that grows, and here that is evaluations performed, not
// levels descended.
//
// The value is far above anything legitimate. Step outputs are stored as
// literals by every execution path today, so resolution performs zero stored
// evaluations for a real workflow; this exists so that the invariant is
// enforced rather than relied on, and a future path that stores an expression
// meets a limit instead of an afternoon of CPU.
const maxActivationEvaluations = 10_000

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

	// Inputs holds the rooted `inputs.<name>` namespace: the arguments the run was
	// started with, already checked against the workflow's declarations and
	// defaulted.
	//
	// Answered whole, exactly as [StepsOutputActivation.AmbientVars] is, so nothing
	// here needs an opinion about how deep a reference goes. See [InputsRoot].
	Inputs map[string]ref.Val

	// Locals holds names bound *where the expression is written* — a loop's current
	// item, a step's own `vars:`, `now` inside a wait — resolved bare. They are
	// resolved before step outputs, so a loop body can refer to its iterator by name.
	//
	// Resolution stops at the local itself: an expression selecting into it, like
	// item.name, is resolved by returning the item and letting CEL apply the
	// selection — the same contract step outputs follow.
	Locals map[string]ref.Val

	// RunIdentity is the attested identity of whoever started this run, answered
	// whole under [RunRoot] exactly as [StepsOutputActivation.Inputs] is: nil reads
	// as every field empty, which is correct both for a run that predates the field
	// and for a run the server attested anonymously — [RunLocal] is what tells
	// those apart from a run with no authenticated caller at all.
	RunIdentity *WorkloadIdentity

	// RunLocal marks an activation built by the local driver, which has no
	// authenticated caller at all. See [Scope.local].
	RunLocal bool

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

	// remaining is the evaluation budget shared by pointer with every child
	// activation, bounded by maxActivationEvaluations. Shared, because the
	// resource it bounds is the total across the whole resolution: a budget per
	// level is a depth bound wearing a different name. Nil until the first
	// stored expression needs it, so the common case — every output a literal —
	// allocates nothing.
	remaining *int
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

		return e.ambientRoot(name)
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
		return e.ambientRoot(name)
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

// ambientRoot answers a rooted namespace that is not `steps` and not `vars`, for
// the two positions in [StepsOutputActivation.ResolveName] where a name has been
// found not to be a step.
//
// Answered *after* the step lookup, which is the same placement [StepsRoot] gets
// and for the same reason: a specification compiled before a root existed may hold
// a step of that name, and a worker evaluates the stored AST out of `RunState`
// rather than re-parsing the file — so a run started on an older build keeps
// resolving the way it always did (invariant 10). The compiler refuses the id, so
// no new file can reach this precedence; it exists for the runs that predate the
// root and for nothing else.
func (e *StepsOutputActivation) ambientRoot(name string) (any, bool) {
	switch name {
	case InputsRoot:
		// An empty root still resolves, to an empty map, for the reason [VarsRoot]
		// does: a run started with no arguments should make `inputs.missing` a
		// missing key rather than an unresolved reference, so the diagnostic
		// describes the mistake the author made rather than sending them to look
		// for a root that is always there.
		if len(e.Inputs) == 0 {
			return types.NewStringInterfaceMap(TypeAdapter, nil), true
		}

		entries := make(map[ref.Val]ref.Val, len(e.Inputs))
		for inputName, v := range e.Inputs {
			entries[types.String(inputName)] = v
		}

		return types.NewRefValMap(TypeAdapter, entries), true

	case RunRoot:
		return runRootValue(e.RunIdentity, e.RunLocal), true

	default:
		return nil, false
	}
}

// InputsRoot is the name a run's arguments hang from in an expression, as
// `inputs.<name>`.
//
// The third rooted namespace, for the reason the first two have one (invariant 2):
// a root makes a collision with a step id or a var unrepresentable rather than a
// rule somebody has to write, and it turns a declared name into a field selection —
// so seventeen of CEL's twenty-one reserved words are legal input names for free.
// The four that are not are lexer tokens, which is the one thing a root cannot
// rescue; the compiler refuses those.
const InputsRoot = "inputs"

// RunRoot is the name the run's own starter identity hangs from in an expression,
// as `run.identity.<field>` and `run.local`.
//
// # Naming this over `caller`, `principal`, `requester`, `started_by`
//
// `run` reads least surprising against this codebase's own vocabulary — every
// comment that explains [WorkloadIdentity.subject] already calls it "the caller
// that requested the run" — and it is the word this project's own design notes
// already set aside for exactly this: docs/DSL.md lists `run.*` beside
// `steps.<id>.*`, `vars.*` and `inputs.*` as part of one naming model, and its
// own "Order of work" section records it as planned and unstarted. Landing
// identity under it completes that reservation rather than inventing a new one.
//
// `caller` was the next candidate and was rejected for being no clearer while
// giving up that alignment. `principal` is accurate IAM terminology this
// codebase's own docs never otherwise reach for. `requester` and `started_by`
// were rejected for a sharper reason: an approval-gate workflow is exactly the
// place a step might legitimately be named "requester" to look something up
// about one, and `requester` sits one letter from `inputs.requested_by` — the
// caller-supplied field this very root exists to be checked against — which is
// precisely the confusion a reader should never have to resolve. Nesting under
// `workload` (the name egress and secret policy already bind this exact
// [WorkloadIdentity] shape to, see auth/assume.go's attrWorkload) was rejected
// because that grammar's `workload` carries a richer, different shape —
// subject, namespace, deployment, workflow, run, step, on_behalf_of, claims —
// and reusing the word here would make it answer two different questions in
// two grammars a reader moves between.
//
// # Reserved without an edition boundary
//
// Adding `run` as a root makes it collide with a step of that id, exactly as
// `steps`, `vars` and `inputs` did when rooting landed — and that precedent is
// the reason this needs no edition bump. `cel:`, `echo:` and `printf:`
// retiring at v2026.2 were *reinterpretations*: the same spelling kept parsing
// and silently started meaning something else, which is what an edition
// boundary and a rewriter exist to make an explicit, versioned break instead of
// a silent one. This is not that. It is [flow validate] refusing one new step
// id, unconditionally, the same way it already refuses `steps`, `vars` and
// `inputs` — a check that runs regardless of a file's declared edition, because
// invariant 10 protects what a run already compiled and replays, never what a
// file sitting in a repository is allowed to name a step. No example in this
// repository names a step `run`, and the diagnostic ([shadowsRoot]) names the
// collision and what to do about it exactly as the other three already do.
const RunRoot = "run"

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

		// The budget is the bound that matters; the depth check above only keeps
		// the stack shallow. Initialized lazily at the root so a resolution that
		// meets no stored expression — every real workflow today — pays nothing.
		if e.remaining == nil {
			budget := maxActivationEvaluations
			e.remaining = &budget
		}
		if *e.remaining <= 0 {
			return nil, fmt.Errorf(
				"resolving stored expressions exceeded %d evaluations; a chain of "+
					"expressions that reference each other multiplies work at every level, "+
					"and this run's outputs reference each other too much to resolve",
				maxActivationEvaluations)
		}
		*e.remaining--

		child := &StepsOutputActivation{
			Prev:    e.Prev,
			Ctx:     e.Ctx,
			Eval:    e.Eval,
			Profile: e.Profile,
			depth:   e.depth + 1,
			// The same counter, not a copy: children spend the parent's budget,
			// which is what makes it a bound on the total.
			remaining: e.remaining,
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

// newLiteralFromUint64 converts an unsigned value to the Int64Value CEL
// uses, or an error value when it does not fit.
//
// Found by way of `flow test`'s stub `returns:` (#155): goccy/go-yaml decodes
// a plain non-negative YAML integer — `status: 200` — into a Go `uint64`
// rather than `int`, and [NewLiteral]'s switch had no case for any unsigned
// width at all. It did not error either — it fell to the default case, which
// *does* return an error value, but one recorded on the [Value] and never
// surfaced anywhere a caller who only wanted an integer would think to look:
// the key silently carried an error instead of 200, EvalRunOutputs found
// nothing there, and `steps.fetch.status` failed with "no such attribute"
// rather than with anything naming the real cause. A test file's stub
// returning any plain integer was broken from the moment stubs existed, and
// nothing caught it until the run-fails-silently defect this package's own
// [assertExpectation] fix (#155, P1-1) stopped absorbing the failure.
func newLiteralFromUint64(v uint64) *Value {
	if v > math.MaxInt64 {
		return &Value{
			Kind: &Value_Error_{
				Error: &Value_Error{
					Message: fmt.Sprintf("flowstatev1: %d overflows the int64 every CEL integer is represented as", v),
					Code:    Value_Error_CODE_INTERNAL,
				},
			},
		}
	}
	return NewLiteral(int64(v))
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
	case int8:
		return NewLiteral(int64(v))
	case int16:
		return NewLiteral(int64(v))
	case int32:
		return NewLiteral(int64(v))
	case uint:
		return newLiteralFromUint64(uint64(v))
	case uint8:
		return NewLiteral(int64(v))
	case uint16:
		return NewLiteral(int64(v))
	case uint32:
		return NewLiteral(int64(v))
	case uint64:
		return newLiteralFromUint64(v)
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

// newValueExprWithErr parses one expression into the form a specification carries.
//
// Parsed against the profile's environment, and that is the whole of this
// function's difficulty. It used to parse against a bare one — `Env()` with no
// libraries — on the reasoning that parsing does not need declarations, which is
// true of functions and false of macros.
//
// A macro is expanded by the *parser*, so a parser that has not been told about one
// does not expand it. `math.greatest(1, 2)` was stored as a receiver call on an
// identifier named `math`, and there is no such identifier and no such method, so
// nothing could ever evaluate it. Eighteen of the profile's macros were in that
// state: `cel.bind`, every two-variable comprehension (`transformList`,
// `transformMap`, `transformMapEntry`, and the three-argument `all`, `exists` and
// `existsOne`), `sortBy`, `math.least` and `math.greatest`, `optMap`, `optFlatMap`,
// `proto.getExt` and `proto.hasExt`.
//
// It failed in two different-looking ways, which is why it stayed hidden. A macro
// whose qualifier is a name — `math.greatest` — validated cleanly and died at run
// time with `no such attribute(s): math`. One that binds a variable —
// `[3,1,2].sortBy(v, v)` — left `v` as a bare identifier in the stored tree, so the
// reference walk reported `references unknown name "v"`: a false diagnostic naming
// the macro's own bound variable, about a function the docs list and `flow tasks`
// prints.
//
// The standard macros — `has`, `filter`, `map`, `all/2`, `exists/2` — always worked,
// because they are in cel-go's default environment rather than in a library. That is
// what made the failure look like something particular to a few functions instead of
// what it was.
func newValueExprWithErr(exprStr string) (*Value, error) {
	libs, err := ProfileLibraries(CurrentProfile)
	if err != nil {
		return nil, fmt.Errorf("failed to resolve profile libraries: %w", err)
	}

	// CurrentProfile rather than a profile carried in from the caller: this is
	// compilation, and a file compiled by this build is compiled in this build's
	// language. What a *spec* pins is which profile evaluates it later, which is a
	// different question and already answered by Workflow.profile.
	base, err := DefaultEvaluator().Env(libs...)
	if err != nil {
		return nil, fmt.Errorf("failed to create CEL environment: %w", err)
	}

	env, err := base.Extend(cel.EnableMacroCallTracking())
	if err != nil {
		return nil, fmt.Errorf("failed to enable macro call tracking: %w", err)
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
	case string, int, float64, float32, int64, bool, *expr.Value,
		int8, int16, int32, uint, uint8, uint16, uint32, uint64:
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
	return RunWithInputs(ctx, w, nil)
}

// RunWithInputs runs a workflow locally with the arguments a caller supplied.
//
// This is the local driver's submit boundary, and it enforces exactly what the
// server enforces at its own: the arguments are checked against the workflow's
// declarations and defaulted by [BindRunInputs], and the pair is weighed by
// [CheckSubmissionSize]. One function, two callers — a local run that accepted an
// undeclared input would be a rehearsal that says yes to a submission production
// refuses, which is the direction invariant 3 exists to prevent.
func RunWithInputs(ctx context.Context, w *Workflow, inputs map[string]*Value) (*Workflow_StepOutputs, error) {
	if w == nil || len(w.Steps) == 0 {
		return nil, fmt.Errorf("workflow cannot be nil or empty")
	}

	bound, err := BindRunInputs(w, inputs)
	if err != nil {
		return nil, err
	}
	if err := CheckSubmissionSize(w, bound); err != nil {
		return nil, err
	}

	return eval(ctx, w, bound)
}

func eval(ctx context.Context, w *Workflow, inputs map[string]*Value) (*Workflow_StepOutputs, error) {
	if w == nil || len(w.Steps) == 0 {
		return nil, fmt.Errorf("workflow cannot be nil or empty")
	}

	// Registered for the whole run, not per wait: a [VirtualClock] must not see
	// this goroutine as "gone" between two waits, or a second, unrelated
	// participant's own wait (`flow test`'s scripted signal delivery, running
	// concurrently) would see the clock as fully parked and advance out from
	// under it. See [EnterClock]. Registered through [EnterClockForWholeRun]
	// rather than [EnterClock] directly, so that when an outer caller already
	// holds the run's participant — `flow test` does, from before it starts
	// delivering scripted signals — this does not register a redundant second
	// one that would keep the clock from ever advancing on the run's own waits.
	leaveClock := EnterClockForWholeRun(ctx)
	defer leaveClock()

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

	// Bound for the whole run rather than per step: an argument is a fact about the
	// run, which is what makes it a root and not a binding.
	scope.Inputs = inputs

	// The local driver has no authenticated caller at all — no server sits in
	// front of it to attest anything — so Identity stays unset (every field
	// reads empty) and Local is true. This is the same honest answer
	// `LocalSignalSender` gives for a wait's `sender`, made for the run's own
	// starter identity: a local run must never look like an attested
	// production one, which is invariant 3's whole point.
	scope.Local = true

	if runtime, ok := ctx.Value(secretRuntimeKey{}).(TaskRuntime); ok && runtime.Step.Workflow == "" {
		runtime.Step.Workflow = w.GetName()
		ctx = ContextWithTaskRuntime(ctx, runtime)
	}

	// The compensations steps register as they succeed. Empty for a workflow with
	// no `undo:` anywhere, which is every workflow that predates the feature, and
	// then nothing below this line does anything.
	undo := NewUndoLog(nil)

	if err := runNodes(ctx, w.Steps, scope, undo, false, 0); err != nil {
		// The run cannot continue, so whatever already happened is taken back —
		// reverse order, every entry attempted, one summary appended to the failure.
		// [RunUndoLog] owns all three of those rules and the durable driver reaches
		// them through the same call.
		//
		// A cancellation compensates too, and it is the one case that cannot use the
		// context it arrived on. Every call made with a cancelled context fails
		// immediately, so compensating on `ctx` would attempt each entry, have each
		// refused by its own transport before it left the process, and report a run
		// that "could not undo" everything it had in fact never tried to. The scope
		// therefore has to survive the cancellation — [context.WithoutCancel] here,
		// `workflow.NewDisconnectedContext` in the durable driver — and be given a
		// deadline of its own, because an operator who asked a run to stop is
		// waiting for it. That deadline is [UndoBudget], read by both drivers.
		//
		// The cancellation itself is returned, wrapped: `errors.Is(err,
		// context.Canceled)` still answers yes, so a caller that distinguishes a
		// stopped run from a failed one keeps doing so.
		if ctx.Err() != nil && errors.Is(err, ctx.Err()) {
			return nil, UndoRunError(err, runUndoOnCancel(ctx, w, undo))
		}

		return nil, UndoRunError(err, RunUndoLog(undo, func(entry *PendingUndo) error {
			return runUndoTask(ctx, w.GetProfile(), entry)
		}))
	}

	// Evaluated once, after the last step, against the scope the run finished in —
	// the same moment and the same scope the durable driver uses. See
	// [EvalRunOutputs] and engine.Run, where the reason that moment is safe in
	// workflow code is written down.
	outputs, err := EvalRunOutputs(ctx, w, scope)
	if err != nil {
		return nil, err
	}
	stepOutputs.RunOutputs = outputs

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
//
// undo collects the compensations of steps that succeed, and nested is what tells
// this level whether it is one a compensation may be written at — see
// [CheckUndoPlacement], which refuses the nested case rather than silently
// dropping it.
func runNodes(ctx context.Context, nodes []*Node, scope *Scope, undo *UndoLog, nested bool, depth int) error {
	for _, node := range nodes {
		// Refused before the step runs rather than after it succeeds, so a workload
		// the engine cannot honour does not perform half of itself first.
		if err := CheckUndoPlacement(node, nested); err != nil {
			return fmt.Errorf("step %q: %w", node.GetId(), err)
		}

		nodeCtx := ctx
		if runtime, ok := ctx.Value(secretRuntimeKey{}).(TaskRuntime); ok {
			nodeCtx = ContextWithSecretStep(ctx, runtime.Step.Workflow, runtime.Step.Run, node.GetId())
		}
		run, err := EvalConditionInScope(nodeCtx, node.GetCondition(), scope)
		if err != nil {
			return fmt.Errorf("step %q: %w", node.GetId(), err)
		}
		if !run {
			continue
		}

		outputs, err := runNodeWithVars(nodeCtx, node, scope, undo, depth)
		if err != nil {
			// Cancellation is not a step failure, so `continue_on_error` does not
			// get to tolerate it — the durable driver says the same thing at the
			// same point, and for the same reason: that policy says "this task may
			// fail without stopping the workload", not "the workload may not be
			// stopped". Tolerated, a cancelled run would walk on through every
			// remaining step, fail each one instantly on the same dead context,
			// record each as a best-effort failure, and *succeed*.
			//
			// Asked of the run's context rather than of the error, because a
			// step's own `timeout:` also arrives here as a context error and that
			// one is an ordinary failure the policy exists to tolerate.
			if ctx.Err() != nil && errors.Is(err, ctx.Err()) {
				return fmt.Errorf("step %q: %w", node.GetId(), err)
			}
			if !node.GetPolicy().GetContinueOnError() {
				return fmt.Errorf("step %q: %w", node.GetId(), err)
			}
			// Recorded without the `step %q` position the propagating path adds:
			// the id is implied by the key this is recorded under, and repeating it
			// would make `${steps.<id>.error}` name its own step. The durable
			// driver draws the same line at the same place — see stepFailed.
			scope.Outputs.StepValues[node.GetId()] = FailedStepOutputs(StepErrorText(err))
			continue
		}
		if outputs != nil {
			scope.Outputs.StepValues[node.GetId()] = outputs
		}
	}
	return nil
}

// runNodeWithVars executes a node with its own `vars:` block bound.
//
// The block is evaluated here rather than in runNodes' loop body so that a
// failure evaluating it is a failure *of this node*, reaching the same
// `continue_on_error` check every other failure of this node reaches. It used to
// return straight out of runNodes, one statement above that check: a step whose
// `vars:` expression failed aborted the whole local run even where the author had
// said the step may fail, while the durable driver — which evaluates the same
// block inside its own runNodeWithVars, whose error flows to the same tolerance
// check — carried on and recorded the failure. Rehearsal was stricter than
// production, in the direction that makes a local run misleading.
//
// Evaluated after the condition deliberately: `if:` decides whether the step runs
// at all, so a var it declares does not exist yet when the question is asked —
// and a var whose expression would fail must not fail a step that is skipped.
func runNodeWithVars(ctx context.Context, node *Node, scope *Scope, undo *UndoLog, depth int) (*Node_Outputs, error) {
	inner, err := EvalStepVars(ctx, node, scope)
	if err != nil {
		return nil, err
	}

	outputs, err := runNode(ctx, node, inner, depth)
	if err != nil {
		return nil, err
	}

	// Registered here rather than in runNodes' loop for the same reason the vars
	// block is evaluated here: the inner scope is live at this point, so a
	// compensation can read the step's own bare `vars:` exactly as the step's inputs
	// could. One statement further out they are gone.
	//
	// A failure to resolve one is a failure of this step, reaching the same
	// `continue_on_error:` check every other failure of this step reaches — and, on
	// the path where it is not tolerated, ending the run before anything is built on
	// top of an effect that has no way back. The durable driver registers at the
	// identical point, in its own runNodeWithVars.
	entry, err := UndoRegistrationFor(ctx, node, inner, outputs)
	if err != nil {
		return nil, err
	}
	undo.Register(entry)

	return outputs, nil
}

// runUndoTask executes one registered compensation.
//
// Through [runStepWithPolicy] with no policy, which is what gives a compensation
// the same defaults an ordinary step with no `retry:` and no `timeout:` gets — the
// attempt count, the backoff and both timeouts, from the constants the durable
// driver reads for the same purpose. A compensation deserves at least the
// resilience of the step it is undoing, and giving it a *different* answer would
// be one more number written down twice.
//
// The scope is empty but for the profile, and that is not a shortcut. The task's
// inputs were resolved when the step succeeded, so there is nothing here left to
// evaluate against a run; what remains unresolved is only what a task evaluates
// against its own response, which needs no run scope in either driver.
func runUndoTask(ctx context.Context, profile string, entry *PendingUndo) error {
	scope := NewScope(profile, &Workflow_StepOutputs{StepValues: map[string]*Node_Outputs{}})
	_, err := runStepWithPolicy(ctx, entry.GetTask(), nil, scope)

	return err
}

// runUndoOnCancel takes back what a cancelled run did, within [UndoBudget].
//
// The context is stripped of the cancellation that brought the run here and given
// the budget as its whole deadline — not a deadline per compensation, because what
// is being bounded is how long a run keeps working after being told to stop, and
// that is a total.
//
// The budget is enforced twice over, and the two catch different things: the
// context stops a compensation that is *running* when it expires, and
// [RunUndoLogWithin] stops one that has not started, so that it is reported as
// never attempted rather than as having failed. Both rules are that function's,
// which is what keeps them identical in the durable driver.
func runUndoOnCancel(ctx context.Context, w *Workflow, undo *UndoLog) []UndoResult {
	if undo.Len() == 0 {
		return nil
	}

	uctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), UndoBudget)
	defer cancel()

	deadline, _ := uctx.Deadline()

	return RunUndoLogWithin(undo,
		func() time.Duration { return time.Until(deadline) },
		func(entry *PendingUndo, _ time.Duration) error {
			// The remaining budget is not passed on here: locally it is already
			// the context's own deadline, and the task honours that.
			return runUndoTask(uctx, w.GetProfile(), entry)
		})
}

// runNode executes one node and returns the outputs it records.
func runNode(ctx context.Context, node *Node, scope *Scope, depth int) (*Node_Outputs, error) {
	switch n := node.Kind.(type) {
	case *Node_Task:
		return runStepWithPolicy(ctx, n.Task, node.GetPolicy(), scope)

	case *Node_ForEach:
		return runForEach(ctx, n.ForEach, scope, depth)

	case *Node_Loop:
		return runLoop(ctx, n.Loop, scope, depth)

	case *Node_Parallel:
		return nil, runParallel(ctx, n.Parallel, scope, depth)

	case *Node_Wait:
		return runWait(ctx, node, n.Wait, scope)

	case *Node_Call:
		return runCall(ctx, n.Call, scope, depth+1)

	default:
		return nil, fmt.Errorf("unsupported node kind: %T", n)
	}
}

// runCall runs a called workflow and returns what it declared it answers with.
//
// The three rules a call has to obey are [CallScope], [CallOutputs] and
// [CheckCallDepth], all of which live in call.go because the durable driver obeys
// the same three. What is here is the one thing this driver does differently:
// running the callee's steps is a function call, where durably it is the same
// executor descending a level.
//
// The callee's own `vars:` are evaluated here, in process, exactly as the top
// level's are in [eval] — [EvalWorkflowVars] needs nothing this driver cannot
// give it inline, unlike the durable driver, which must reach it through an
// activity and then carry the answer across any Continue-As-New the call
// spans (see `Frame.call_vars`'s doc). A local run never continues as new, so
// there is nothing to carry here and evaluating it fresh every time this call
// is reached costs nothing durably meaningful.
//
// `nested` is true for the body, which is what stops a called workflow from
// carrying an `undo:` on one of its steps — the same refusal a loop body and a
// parallel branch get, for the same unresolved reason about ordering across a
// boundary. A nil undo log is passed for the same purpose: a compensation
// registered inside a call has nowhere agreed to go yet.
func runCall(ctx context.Context, call *Call, scope *Scope, depth int) (*Node_Outputs, error) {
	if err := CheckCallDepth(depth); err != nil {
		return nil, err
	}

	callee := call.GetWorkflow()

	arguments, err := ResolveCallArguments(ctx, call.GetArguments(), scope)
	if err != nil {
		return nil, err
	}

	// The explicit-profile form ([EvalVars]) rather than [EvalWorkflowVars],
	// which reads the profile off the workflow itself: a callee that names no
	// profile of its own inherits the caller's, per [CalleeProfile], and using
	// the workflow's bare (empty) field here would evaluate its vars against
	// [OriginalProfile] instead — a different vocabulary from the one its
	// steps, scoped moments later through the same [CalleeProfile] call
	// inside [CallScope], actually run under.
	vars, err := EvalVars(ctx, CalleeProfile(scope, callee), callee.GetVars())
	if err != nil {
		return nil, fmt.Errorf("calling %q: %w", callee.GetName(), err)
	}

	inner, err := CallScope(scope, callee, arguments, vars)
	if err != nil {
		return nil, err
	}

	if err := runNodes(ctx, callee.GetSteps(), inner, nil, true, depth); err != nil {
		// Named, because a failure inside a called workflow reported without
		// saying which one leaves a reader looking through the caller for a step
		// that is not there.
		return nil, fmt.Errorf("workflow %q: %w", callee.GetName(), err)
	}

	return CallOutputs(ctx, callee, inner)
}

// runForEach runs a loop body once per item.
//
// Iterations run sequentially here regardless of MaxParallel. The durable driver
// honors it, but reproducing concurrency locally would only reorder side effects
// without reproducing anything an author can act on — and sequential execution
// makes a local run's output deterministic, which is what makes it useful for
// comparison.
func runForEach(ctx context.Context, loop *ForEach, scope *Scope, depth int) (*Node_Outputs, error) {
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

		// nil log and nested: a compensation may not be written in a loop body, and
		// [CheckUndoPlacement] refuses one rather than this level quietly ignoring it.
		if err := runNodes(ctx, loop.GetBody(), iterationScope, nil, true, depth); err != nil {
			return nil, fmt.Errorf("iteration %d: %w", i, err)
		}

		iterations = append(iterations, onlyBodyOutputs(loop.GetBody(), iterationOutputs))
	}

	return LoopOutputs(iterations), nil
}

// runLoop runs a loop body repeatedly, carrying state, until its `until:` condition
// holds or its iteration ceiling is reached.
//
// Do-while: the body runs, then `until:` is evaluated against the scope it finished
// in — so `until:` may read the body's own outputs, which is the whole point, since
// the stop signal (a page reporting it was not truncated) is something a body step
// produces and nothing a pre-body check could see.
//
// The bound is the first thing checked each iteration, and reaching it fails the run
// with [LoopIterationLimitError] rather than returning what the loop has so far —
// the honest outcome for a loop that ran its whole budget without finishing. The
// durable driver checks the identical bound at the identical point; both read it
// through [LoopMaxIterations].
func runLoop(ctx context.Context, loop *Loop, scope *Scope, depth int) (*Node_Outputs, error) {
	name := loop.GetState()
	max := LoopMaxIterations(loop)

	// Evaluated once, before the first iteration, against the scope the loop sits in
	// — the state does not exist yet, which is why this is where it is defined.
	state, err := LoopInitialState(ctx, loop, scope)
	if err != nil {
		return nil, err
	}

	iterations := make([]*Workflow_StepOutputs, 0)

	for i := 0; ; i++ {
		if i >= max {
			// The budget is spent and `until:` never held. A distinct failure, not a
			// silent stop: the loop did not do what it was asked.
			return nil, LoopIterationLimitError(max)
		}

		// Each iteration starts from the outputs visible before the loop, so a body
		// step cannot see a previous iteration's outputs — the only value threaded
		// between iterations is the carried state, exactly as a `for_each`'s only
		// thread is its item.
		iterationOutputs := &Workflow_StepOutputs{StepValues: map[string]*Node_Outputs{}}
		for k, v := range scope.GetOutputs().GetStepValues() {
			iterationOutputs.StepValues[k] = v
		}

		// The carried state is bound bare, the same standing as a loop iterator, so a
		// body written for a loop that names one reads `${cursor}`. A loop that carries
		// nothing binds no name and this is a plain output-scope swap.
		iterationScope := scope
		if LoopCarriesState(loop) {
			iterationScope = scope.WithLocal(name, state)
		}
		iterationScope = iterationScope.WithOutputs(iterationOutputs)

		// nil log and nested: a compensation may not be written in a loop body, and
		// [CheckUndoPlacement] refuses one rather than this level quietly ignoring it —
		// the same refusal a `for_each` body gets.
		if err := runNodes(ctx, loop.GetBody(), iterationScope, nil, true, depth); err != nil {
			return nil, fmt.Errorf("iteration %d: %w", i, err)
		}

		iterations = append(iterations, onlyBodyOutputs(loop.GetBody(), iterationOutputs))

		// `until:` and `update:` both see the body's outputs and the current state, so
		// they are evaluated against the scope the body finished in.
		stop, err := EvalLoopUntil(ctx, loop, iterationScope)
		if err != nil {
			return nil, fmt.Errorf("iteration %d: %w", i, err)
		}
		if stop {
			return LoopStateOutputs(iterations, state), nil
		}

		next, err := LoopNextState(ctx, loop, iterationScope)
		if err != nil {
			return nil, fmt.Errorf("iteration %d: %w", i, err)
		}
		state = next
	}
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
func runParallel(ctx context.Context, parallel *Parallel, scope *Scope, depth int) error {
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
		if err := runNodes(ctx, branch.GetSteps(), branchScope, nil, true, depth); err != nil {
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
	// Resolved here, above the loop, because this is the position the durable
	// driver resolves at: in workflow code, before an activity is scheduled
	// (`engine/execute.go`'s runTask). Inputs are part of the *specification*, so
	// an expression that cannot be evaluated fails the same way every time — and
	// resolving inside the loop made that deterministic failure a retryable one.
	// A `for_each` body's `${item.missing}` was attempted five times over fifteen
	// seconds of backoff locally and failed instantly in production, which is the
	// rehearsal disagreeing about both the outcome's timing and the number of
	// times a dependency is touched on the way to it.
	//
	// Unconditional, where it used to happen only for a scope that bound names
	// (in [Task.EvalInScope]). That guard is why the same file recorded two
	// different sentences: at a top-level step, with nothing bound, the task
	// resolved its own inputs and reported a classified `task "log" failed
	// (InvalidInput): field "message": …`, while the durable driver — which does
	// not have the guard — reported `input "message": …`. Two sentences and two
	// error kinds for one mistake, so the one an author's `if:` compared depended
	// on where the workload ran.
	//
	// Inputs a task evaluates for itself are untouched by this and stay per
	// attempt under both drivers; see [ResolvableInputs].
	resolved, err := ResolveTaskInputs(ctx, task, scope)
	if err != nil {
		return nil, err
	}

	// The same number the durable driver uses, from the same constant. This was
	// `1` here and five there, so a step with no `retry:` behaved differently in
	// the place that exists to rehearse the other.
	attempts := RetryAttemptsFor(policy.GetRetry())

	// And the same two timeouts, through the same precedence. The local driver
	// applied a bound only where a step declared one, so a task that hangs hung
	// the whole run — the durable driver has never been able to do that, because
	// Temporal refuses an activity with no timeout at all.
	timeouts := StepTimeoutsFor(policy, StepTimeoutsFromContext(ctx))
	if timeouts.ScheduleToClose > 0 {
		// Derived here rather than around the whole run, so that exhausting it is
		// an ordinary step failure `continue_on_error:` may tolerate — runNodes
		// asks the *run's* context whether it is cancelled, and this one is not it.
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, timeouts.ScheduleToClose)
		defer cancel()
	}

	for attempt := 1; ; attempt++ {
		var out *Node_Outputs
		out, err = runStepAttempt(ctx, resolved, timeouts.StartToClose, scope)
		if err == nil {
			return out, nil
		}

		// Only failures that could plausibly succeed on another attempt are
		// retried, matching how the durable driver classifies them.
		if attempt >= attempts || !ClassifyError(err).Retryable() {
			return nil, err
		}

		// What the failure itself asked for wins over the policy's backoff, which
		// is what the durable driver does — `engine/activities.go` reads the same
		// value and hands it to Temporal as NextRetryDelay.
		//
		// Invisible until now for the reason the missing interval cap was: with one
		// attempt there was never a delay to get wrong. A server answering 503 with
		// `Retry-After: 30` would have been asked again after a second here and
		// after thirty in production — hammering a dependency that has just said it
		// is struggling, in the driver whose whole purpose is to rehearse the other.
		delay := RetryAfter(err)
		if delay <= 0 {
			delay = retryDelay(policy.GetRetry(), attempt)
		}
		// Through ctx's clock rather than time.After directly, for the same
		// reason a wait node is (see wait_local.go): a retry backoff is still
		// a duration this driver blocks on, and `flow test` needs a case
		// whose stub fails on the first attempt and succeeds on a later one
		// to run at test speed rather than spend the backoff for real.
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-ClockFromContext(ctx).After(delay):
		}
	}
}

// runStepAttempt performs one attempt, bounded by the per-attempt timeout.
//
// The bound is passed in rather than read from the step's policy, because a step
// that declares no `timeout:` still has one — [DefaultStartToCloseTimeout], the
// same bound Temporal applies to every attempt at every activity. This used to ask
// the policy directly and do nothing when it was silent, which is the whole of how
// a hung task could hang a local run forever while production failed it after two
// minutes.
func runStepAttempt(ctx context.Context, task *Task, timeout time.Duration, scope *Scope) (*Node_Outputs, error) {
	if timeout > 0 {
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
		interval = DefaultRetryInitialInterval
	}

	backoff := retry.GetBackoffCoefficient()
	if backoff < 1 {
		backoff = DefaultRetryBackoff
	}

	// Defaulted rather than left unbounded, which is what it was. With one attempt
	// there was never a second wait for the missing cap to matter; with the attempt
	// counts agreeing, a step's fourth retry would have waited eight seconds here
	// and four under Temporal.
	max := retry.GetMaxInterval().AsDuration()
	if max <= 0 {
		max = DefaultRetryMaxInterval
	}

	delay := float64(interval) * math.Pow(backoff, float64(attempt-1))
	if time.Duration(delay) > max {
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
// Expression inputs are resolved into a copy of the task before execution, for the
// callers that reach a task without having resolved them: the durable driver's
// activity, which receives a scope built on another machine, and direct callers of
// [Task.Eval]. A step running under the local driver arrives with its inputs
// already resolved — [runStepWithPolicy] does it above the retry loop, at the
// position the durable driver resolves at — so this is a second, idempotent pass
// over literals for that path.
//
// The `BindsNames` guard is why it cannot be the *only* resolution point for the
// local driver. A scope binding no names skips it, leaving the task to evaluate
// its own inputs, and a task reports that failure as a classified `task "log"
// failed (InvalidInput): field "message": …` where the durable driver reports
// `input "message": …`. Same file, same mistake, two sentences and two error
// kinds.
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
	// LookupTaskIn, not LookupTask: this is the one place a task's Fn is
	// actually called, so what runs must be decided by the registry *this run*
	// was given rather than by whatever the process-wide registry holds at this
	// instant. With no registry on the context — production, and every ordinary
	// local run — this is exactly LookupTask. See [NewContextWithRegistry].
	def, ok := LookupTaskIn(ctx, t.Name)
	if !ok {
		return nil, NewTaskError(t.Name, ErrorKindUnknownTask, fmt.Errorf(
			"unknown task %q (available: %s)", t.Name, strings.Join(TaskNamesIn(ctx), ", ")))
	}
	out, err := def.Fn(ctx, t.Inputs, scope)
	if err != nil || out == nil {
		return out, err
	}

	// Bounds what the task's own result carries, not what the caller
	// submitted — [checkInputListElementBound]/[CheckInputConstraints]
	// already close that half of #204 at [BindRunInputs]. Every task, built-in
	// or plugin, returns through this one return statement on both drivers
	// (the durable driver's activities and the local driver's
	// [runStepAttempt] both call EvalInScope rather than def.Fn directly), so
	// bounding it here is the one place that makes both drivers agree by
	// construction rather than by two call sites staying in sync. See
	// [checkTaskOutputElementBound]'s own doc for why the message it returns
	// is not returned as-is, and CLAUDE.md's "Both execution drivers must
	// agree" for why this lives at the shared choke point rather than in
	// either driver.
	//
	// Classified [ErrorKindLimitExceeded] — the same kind an oversized `http`
	// response body already gets — so it is non-retryable: the size of a
	// task's result does not change between attempts, so retrying spends a
	// worker's time to learn the same thing twice.
	if err := checkTaskOutputElementBound(t.Name, out); err != nil {
		return nil, NewTaskError(t.Name, ErrorKindLimitExceeded, err)
	}

	return out, nil
}
