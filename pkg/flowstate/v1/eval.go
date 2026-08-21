package flowstatev1

import (
	"context"
	"errors"
	"fmt"
	"maps"
	"math"
	"reflect"
	"slices"
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

	// RunAddress is the run's own address — `run.workflow_id` and `run.run_id` —
	// answered under [RunRoot] beside the identity, because both are facts about
	// the run rather than about the file. Nil reads as both fields empty, which
	// is correct only for a run that predates them; see [runRootValue].
	RunAddress *RunAddress

	// Trigger is how the run started, answered whole under [TriggerRoot] exactly
	// as [StepsOutputActivation.RunIdentity] is: nil reads as every field empty,
	// which is correct both for a run that predates the field and for a path that
	// records no trigger. See [TriggerContextValue].
	Trigger *TriggerContext

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
		return runRootValue(e.RunIdentity, e.RunLocal, e.RunAddress), true

	case TriggerRoot:
		// The fifth root, answered whole for the reason the four above are, and
		// answered *after* the step lookup for the reason they are: a
		// specification compiled before this root existed may hold a step of that
		// name, and a worker evaluates the stored AST out of `RunState` rather
		// than re-parsing the file, so a run started on an older build keeps
		// resolving the way it always did (invariant 10).
		return TriggerContextValue(e.Trigger), true

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

// RunRoot is the name the run's own facts hang from in an expression: its
// address, as `run.workflow_id` and `run.run_id`, and the identity that started
// it, as `run.identity.<field>` and `run.local`.
//
// # Additive by construction
//
// Adding a field here costs nothing already written. `run` is a root, so a name
// under it is a field selection rather than an identifier, and the four names a
// file can bind — a step id, a loop's `as:`, a step's own `vars:` key, `now` —
// cannot collide with one: the root itself is refused as any of them, and
// nothing below it is in anyone's namespace. That is the whole argument for
// having made this a root in the first place, and it is why `workflow_id` and
// `run_id` needed no edition boundary either — a file that did not reference
// them before means exactly what it meant.
//
// What is not free is the *set*: [runRootValue] renders these fields and no
// others, on every run, on both drivers, which is what lets
// [flowfile.unknownRunField] report an unknown one rather than staying silent.
// So a field added here has to be filled by both drivers with something honest,
// and the test for that is a shared case, not a driver's own.
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

	// Sorted, so the same map encodes as the same message every time. The
	// entries of an [expr.MapValue] are a repeated field, and a Go map's
	// iteration order is deliberately random — so an unsorted walk here made
	// every multi-key map literal this system records (a loop's per-iteration
	// `results` entries above all) a value that differed from one construction
	// to the next, unassertable by proto.Equal and unstable for no reason a
	// reader of the run record could see. CEL itself gives entry order no
	// meaning, so sorting changes nothing an expression can observe.
	entries := make([]*expr.MapValue_Entry, 0, len(m))
	for _, k := range slices.Sorted(maps.Keys(m)) {
		entries = append(entries, &expr.MapValue_Entry{
			Key:   NewLiteral(k).GetLiteral(),
			Value: NewValue(m[k]).GetLiteral(),
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

// LiteralToGo converts a resolved CEL literal into a plain Go value,
// recursively for a list or a map. It is the reverse of what [NewValue]
// performs when a Go value becomes a literal.
//
// This is the one spelling of that conversion: flowtest and embed both call
// it rather than each keeping their own copy of the switch, so a step's
// recorded value reads back the same way no matter which package reads it.
func LiteralToGo(v *expr.Value) (any, error) {
	switch kind := v.GetKind().(type) {
	case nil, *expr.Value_NullValue:
		return nil, nil
	case *expr.Value_StringValue:
		return kind.StringValue, nil
	case *expr.Value_Int64Value:
		return kind.Int64Value, nil
	case *expr.Value_Uint64Value:
		return kind.Uint64Value, nil
	case *expr.Value_DoubleValue:
		return kind.DoubleValue, nil
	case *expr.Value_BoolValue:
		return kind.BoolValue, nil
	case *expr.Value_BytesValue:
		return kind.BytesValue, nil
	case *expr.Value_ListValue:
		list := make([]any, 0, len(kind.ListValue.GetValues()))
		for i, element := range kind.ListValue.GetValues() {
			native, err := LiteralToGo(element)
			if err != nil {
				return nil, fmt.Errorf("element %d: %w", i, err)
			}
			list = append(list, native)
		}
		return list, nil
	case *expr.Value_MapValue:
		object := make(map[string]any, len(kind.MapValue.GetEntries()))
		for _, entry := range kind.MapValue.GetEntries() {
			// A Go map[string]any cannot hold an integer, unsigned, or
			// boolean CEL key, and GetStringValue would silently return ""
			// for every one of them, collapsing distinct entries into a
			// single object[""] and reporting success. Fail closed on a key
			// this target cannot represent rather than corrupt the result.
			key, ok := entry.GetKey().GetKind().(*expr.Value_StringValue)
			if !ok {
				return nil, fmt.Errorf("map key of type %T cannot be converted to a Go map key: only string keys are supported", entry.GetKey().GetKind())
			}
			name := key.StringValue
			native, err := LiteralToGo(entry.GetValue())
			if err != nil {
				return nil, fmt.Errorf("key %q: %w", name, err)
			}
			object[name] = native
		}
		return object, nil
	default:
		return nil, fmt.Errorf("a %T cannot be converted to a Go value", kind)
	}
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
//
// # A failed run still hands back what it did
//
// When the run fails, the error is accompanied by the *partial transcript*, see
// [PartialTranscript] for exactly what that contains and what it deliberately does
// not. A caller that treats a non-nil error as "no outputs" keeps working
// unchanged, because it never looks; a caller that wants to know what ran before
// the failure now can. The refusals *above* the run, an undeclared input, a
// submission past its size, still hand back nothing, because no step ran.
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

	// The local driver has no authenticated caller — no server sits in front of
	// it to attest anything — so Local is true, unconditionally and with no way
	// for a flag to turn it off. That is the field which says a local run is not
	// a production one, it is what `run.local` renders, and it is what every
	// reader of this scope can tell the two apart by.
	scope.Local = true

	// And the identity this rehearsal acts as, when its starter named one.
	//
	// Unset for the bare [Run] entry point and for `flow test`, which have no
	// starter to name; set by `flow run local` from --as-subject and its
	// siblings, the same identity that already reaches the secret-access
	// policy, the credential broker, the plugin caller, and
	// `distinct_from_starter:`. Three surfaces read this field — the
	// task-shape policy (#187), the egress policy's identity dimension
	// (#240), and `run.identity` — and leaving it empty while the other four
	// saw the rehearsal identity is what made one flag rehearse some of a
	// deployment's policy and silently no-op on the rest (#295): a rule keyed
	// on `identity.namespace` matched nothing here and matched in production,
	// so a local run *denied* what production permits.
	//
	// This does not make the rehearsal attested, and the mechanism that keeps
	// it from looking attested was never this field's emptiness: `Local`
	// above says so to anything reading the scope, and a minted credential's
	// subject carries auth's unforgeable `_local` component, on a path that
	// does not run through here at all. See [NewContextWithRehearsalIdentity]
	// for the whole argument.
	scope.Identity = RehearsalIdentityFromContext(ctx)

	// And the address a local run answers with, which is a sentinel and not an
	// empty string: a local run is not reachable by any name, so the honest
	// answer is one that says so rather than one that looks like a field nobody
	// filled in. See [LocalRunAddress].
	scope.Address = NewLocalRunAddress()

	// And how this run started, which is a manual start unless a caller said
	// otherwise: `flow run local` is a person at a keyboard, and `flow test` sets
	// a case's own so that a branch guarded on the trigger is exercisable without
	// one. See [TriggerFromContext].
	scope.Trigger = TriggerFromContext(ctx)

	// The declarations a wait reports itself policed against, recorded once for
	// the run and from the top-level workflow only: a delivery is authorized
	// against the root's `signals:`, so a callee's own would be the wrong answer
	// to report. A no-op where nobody installed a [PendingWaits] to watch with.
	ctx = contextWithWaitPolicies(ctx, w.GetSignals())

	if runtime, ok := ctx.Value(secretRuntimeKey{}).(TaskRuntime); ok && runtime.Step.Workflow == "" {
		runtime.Step.Workflow = w.GetName()
		ctx = ContextWithTaskRuntime(ctx, runtime)
	}

	// The compensations steps register as they succeed. Empty for a workflow with
	// no `undo:` anywhere, which is every workflow that predates the feature, and
	// then nothing below this line does anything.
	undo := NewUndoLog(nil)

	// How this run fails, wherever it fails: the compensations run, their
	// summary is appended, and a cancellation takes the one path that can still
	// perform them. One function because a run has two places it can fail —
	// during a step, and after the last one while computing its declared
	// outputs — and the difference between those is *when*, which is not
	// something the saga contract turns on. Written twice, the second copy is
	// where the cancellation arm goes missing.
	failRun := func(err error) (*Workflow_StepOutputs, error) {
		// A cancellation compensates too, and it is the one case that cannot use
		// the context it arrived on. Every call made with a cancelled context
		// fails immediately, so compensating on `ctx` would attempt each entry,
		// have each refused by its own transport before it left the process, and
		// report a run that "could not undo" everything it had in fact never
		// tried to. The scope therefore has to survive the cancellation —
		// [context.WithoutCancel] here, `workflow.NewDisconnectedContext` in the
		// durable driver — and be given a deadline of its own, because an
		// operator who asked a run to stop is waiting for it. That deadline is
		// [UndoBudget], read by both drivers.
		//
		// The cancellation itself is returned, wrapped: `errors.Is(err,
		// context.Canceled)` still answers yes, so a caller that distinguishes a
		// stopped run from a failed one keeps doing so.
		//
		// The transcript accompanies the failure, see [PartialTranscript], and
		// [RunWithInputs] for what a caller may read from it. Built from the same
		// `scope.Outputs` the successful path returns, so there is one accumulated
		// record per run rather than a second one assembled for failures.
		// Two decisions, and they are not the same question, which is the trap
		// in writing this as one condition. *Which context compensates* turns
		// only on whether this one is already cancelled — a dead context
		// refuses every entry whatever the run failed of. *What the run
		// reports* turns on whether the cancellation is the failure. Conflating
		// them compensates on the dead context whenever the failure is anything
		// but the cancellation itself: a run whose last step raced a `flow
		// cancel` and then produced an oversized transcript fails with a size
		// error that cannot wrap ctx.Err(), and every undo is refused by its
		// transport before it is attempted.
		if ctx.Err() != nil {
			results := runUndoOnCancel(ctx, w, undo)
			if errors.Is(err, ctx.Err()) {
				// withCancellationCause reads context.Cause(ctx) here, at the run's
				// own context — the one a caller like `flow run local` attaches a
				// signal's or a `flow cancel`'s reason to (see cmd/flow/main.go).
				// Whatever ran underneath already had its own chance to name a
				// narrower cause (a step's schedule-to-close budget, the
				// compensation budget); this is the fallback for the cases nothing
				// more specific was running, most visibly a run parked at a `wait:`
				// when the stop arrives.
				return PartialTranscript(stepOutputs), UndoRunError(withCancellationCause(ctx, err), results)
			}

			// Compensated on the surviving scope, but reported as what actually
			// went wrong: a run that failed on its own terms while a stop was
			// arriving did not fail *because* of the stop, and saying so would
			// lose the only account of why.
			return PartialTranscript(stepOutputs), UndoRunError(err, results)
		}

		return PartialTranscript(stepOutputs), UndoRunError(err, RunUndoLog(undo, func(entry *PendingUndo) error {
			return runUndoTask(ctx, w.GetProfile(), entry)
		}))
	}

	if err := runNodes(ctx, w.Steps, scope, undo, UndoScopeTopLevel, 0, nil); err != nil {
		// The run cannot continue, so whatever already happened is taken back —
		// reverse order, every entry attempted, one summary appended to the
		// failure, and a cancellation compensated on a context that survives it.
		// All four belong to [failRun] above, which the completion path below
		// reaches for the same reasons.
		return failRun(err)
	}

	// Evaluated once, after the last step, against the scope the run finished in —
	// the same moment and the same scope the durable driver uses. See
	// [EvalRunOutputs] and engine.Run, where the reason that moment is safe in
	// workflow code is written down.
	outputs, err := EvalRunOutputs(ctx, w, scope)
	if err == nil {
		stepOutputs.RunOutputs = outputs
		err = CheckRunResultSize(stepOutputs)
	}
	if err != nil {
		// A run that cannot produce its declared outputs — because an expression
		// failed, or because the transcript it computed is too large to record —
		// has not succeeded, and it gets the same accompanying transcript every
		// other failure gets: every step did run, which is precisely why the
		// outputs were reachable to evaluate.
		//
		// And it takes back what it did, through the very same call the step
		// failure above makes. A saga that reports FAILED with entries still
		// pending has left the world holding resources nobody will come back
		// for; that the failure arrived after the last step rather than during
		// one changes nothing about it. The durable driver reaches its own
		// [compensate] on this path for the same reason, which is what keeps
		// the two answering alike (invariant 3).
		//
		// Through the shared function rather than by spelling the undo call out
		// again, because the cancellation arm is precisely the half a second
		// spelling drops: evaluating the declared outputs takes `ctx`, so a run
		// cancelled while computing them arrives here with a cancelled context,
		// and compensating on that context would have every entry refused by its
		// own transport before it was ever attempted — a run reporting it could
		// not undo work it never tried to undo.
		return failRun(err)
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
//
// undo collects the compensations of steps that succeed, and placement is what
// tells this level whether — and how — a compensation written here may be
// honoured; see [CheckUndoPlacement] and [UndoScope], which refuse the shapes
// that cannot be, rather than silently dropping them.
//
// tolerated, when non-nil, collects the id of every step whose failure this
// level records and continues past. A loop passes a fresh set per iteration so
// [AttachIterationBinding] can key on the driver's own record of tolerance
// rather than on the shape of the outputs — the one place "this step failed and
// was tolerated" is a fact is the line below that records it. Everything that
// is not a loop body passes nil, which collects nothing.
func runNodes(ctx context.Context, nodes []*Node, scope *Scope, undo *UndoLog, placement UndoScope, depth int, tolerated map[string]struct{}) error {
	// The scope's outstanding async work, in the order it was started — which is
	// written order, since a scope starts a step where it is written. See
	// [asyncHeld] for why this driver holds a completed result rather than a
	// running one.
	var (
		started []string
		held    = map[string]*asyncHeld{}
	)

	// A scope leaving on a failure still owes whatever it started. When the
	// schedule held a step's work back ([SchedulePointAsyncLaunch]), returning
	// without running it would not be a different *order*, it would be a
	// different execution: work the file launched, that never happened, and a
	// compensation for it that never registered. So the work finishes here,
	// publishing nothing — the durable driver's asyncStep.wait is this same rule
	// at this same point, and says why in the same words: a scope that is failing
	// merges no outputs and raises no second failure.
	//
	// Nothing to do on the way out successfully, or on any path at all under
	// written order: the scope's end has already joined everything it started,
	// and an unheld step's work ran where it was written.
	defer func() {
		for _, id := range started {
			if outcome, outstanding := held[id]; outstanding && outcome.run != nil {
				_, _ = outcome.run()
			}
		}
	}()

	// join publishes one outstanding async step at the position that asked for
	// it: its outputs become visible here, or its failure is reported here,
	// through exactly the same [recordStepOutcome] a step written in order
	// reaches. Nothing about being async changes what a failure says or whether
	// `continue_on_error:` tolerates it — only where it is heard.
	join := func(id string) error {
		outcome, outstanding := held[id]
		if !outstanding {
			return nil
		}
		delete(held, id)
		started = slices.DeleteFunc(started, func(other string) bool { return other == id })

		// A step the schedule held back has not run yet; this is the moment it
		// does. Either way what follows is the same [recordStepOutcome] on the
		// same values, which is the point of the whole arrangement: where the
		// work happened must not be visible in what the join reports.
		outputs, err := outcome.outputs, outcome.err
		if outcome.run != nil {
			outputs, err = outcome.run()
		}

		return recordStepOutcome(ctx, outcome.node, outputs, err, scope, tolerated)
	}

	for _, node := range nodes {
		// Refused before the step runs rather than after it succeeds, so a workload
		// the engine cannot honour does not perform half of itself first.
		if err := CheckUndoPlacement(node, placement); err != nil {
			return fmt.Errorf("step %q: %w", node.GetId(), err)
		}
		// The same rule at the same point, for the same reason: a step marked
		// `async:` where this engine will not honour it must not run half of
		// itself first. `flow validate` refuses it earlier, with a position.
		if err := CheckAsyncPlacement(node, placement); err != nil {
			return fmt.Errorf("step %q: %w", node.GetId(), err)
		}

		// Every mention is a join, and every mention includes the ones evaluated
		// before the step runs — its `if:` first of all. So the joins happen here,
		// ahead of the condition, rather than around the work: a condition that
		// names an async step waits for it and then may still skip the step, which
		// is the honest outcome (the data decided the skip) and the one the
		// durable driver reaches at the identical point.
		for _, id := range AsyncJoinTargets(node, started, scope.GetOutputs()) {
			if err := join(id); err != nil {
				return err
			}
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

		if node.GetAsync() {
			if err := CheckAsyncWidth(len(started), node.GetId()); err != nil {
				return err
			}

			// The slot is taken *here*, where the step is written, and filled when
			// the step finishes. That is what keeps the unwind in reverse written
			// order once joins can happen in an order the text does not have — see
			// [UndoLog.Reserve]. Taken before the schedule gets a say, so a step
			// held back until its join still lands where it was written.
			slot := undo.Reserve()

			// Where the work happens is the schedule's choice (issue #477): at the
			// launch, which is what this driver always did, or at the join, which
			// is nearer what the durable driver does. Both are rehearsals of one
			// durable behaviour, and an author must not be able to tell which ran —
			// that claim is the schedule-equivalence property, and this is the
			// choice it is checked over.
			//
			// A held step's work runs against the outputs visible *at its launch*,
			// frozen here. Anything else would not be a schedule at all: a step
			// whose inputs saw later steps' outputs is a step that ran somewhere
			// else in the file. Freezing is cheap and exact because `async:` is a
			// task step only ([CheckAsyncPlacement]), so the work reads this scope
			// and never writes to it.
			if SchedulerFromContext(ctx).Interleave(SchedulePointAsyncLaunch, node.GetId()) {
				launched := scope.WithOutputs(cloneStepOutputs(scope.GetOutputs()))
				held[node.GetId()] = &asyncHeld{node: node, run: func() (*Node_Outputs, error) {
					return runNodeWithVars(nodeCtx, node, launched, undo, placement, depth, slot, tolerated)
				}}
				started = append(started, node.GetId())

				continue
			}

			outputs, err := runNodeWithVars(nodeCtx, node, scope, undo, placement, depth, slot, tolerated)
			held[node.GetId()] = &asyncHeld{node: node, outputs: outputs, err: err}
			started = append(started, node.GetId())

			continue
		}

		outputs, err := runNodeWithVars(nodeCtx, node, scope, undo, placement, depth, registerAtCompletion, tolerated)
		if err := recordStepOutcome(ctx, node, outputs, err, scope, tolerated); err != nil {
			return err
		}
	}

	// The scope's end joins everything it started, in written order. Not only on
	// the way out successfully: a scope that is failing has already returned
	// above, and what it leaves behind is nothing, because this driver's async
	// work is complete by the time it is held. The durable driver, whose work is
	// genuinely in flight, has to drain on both paths — see its runNodes.
	for _, id := range slices.Clone(started) {
		if err := join(id); err != nil {
			return err
		}
	}

	return nil
}

// asyncHeld is one async step's finished result, waiting for the position that
// joins it.
//
// This driver runs an async step's work where it is written, in order, and holds
// what it produced until a join asks for it — the same rehearsal `parallel:` gets
// here, whose branches also run in written order locally while running
// concurrently in production. What that buys is that everything an author can
// *see* is identical on both drivers: outputs appear at joins, a failure is heard
// at the join rather than where the step is written, and the compensations
// unwind in reverse written order. What it deliberately does not rehearse is
// latency, which is the one thing `async:` is for and the one thing a local
// rehearsal has never claimed to predict.
//
// A held step is one of two shapes, and never both: either the work already ran
// and outputs/err are what it produced, or the schedule held it back and run is
// the work itself, waiting for the join to call it once
// ([SchedulePointAsyncLaunch]).
type asyncHeld struct {
	node    *Node
	outputs *Node_Outputs
	err     error
	run     func() (*Node_Outputs, error)
}

// cloneStepOutputs copies the step-output map one level deep: a new map over the
// same [Node_Outputs] values, which are never written through once recorded.
//
// The copy that makes a held async step's launch position meaningful. The scope's
// own map is written to as later steps finish, so a closure holding the map
// itself would read whatever had accumulated by the time the join called it.
func cloneStepOutputs(outputs *Workflow_StepOutputs) *Workflow_StepOutputs {
	clone := &Workflow_StepOutputs{StepValues: make(map[string]*Node_Outputs, len(outputs.GetStepValues()))}
	for id, value := range outputs.GetStepValues() {
		clone.StepValues[id] = value
	}

	return clone
}

// registerAtCompletion is the slot value meaning "this step is not async, so
// register its compensation at the end of the log the moment it succeeds" — the
// sequential behaviour, where registration order and written order coincide.
const registerAtCompletion = -1

// recordStepOutcome applies one finished step's result to the scope, and reports
// the error the enclosing walk should propagate.
//
// One function for the step written in order and the async step heard at its
// join, because every decision in it — is this a cancellation, does
// `continue_on_error:` tolerate it, what is recorded under the step's id — must
// come out the same either way. The durable driver keeps the same two callers on
// one body for the same reason.
func recordStepOutcome(ctx context.Context, node *Node, outputs *Node_Outputs, err error, scope *Scope, tolerated map[string]struct{}) error {
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
			// Recorded on the way out, under the same key and in the same shape a
			// tolerated failure is recorded in, so that the [PartialTranscript] the
			// run hands back names the step it stopped on rather than ending one
			// step short of the truth. Nothing else can observe it: the run is over,
			// no later step evaluates against this scope, and the successful path
			// never reaches this line. The durable driver records at the identical
			// point, for the identical reason.
			scope.Outputs.StepValues[node.GetId()] = failureRecord(err)

			return fmt.Errorf("step %q: %w", node.GetId(), err)
		}
		// Recorded without the `step %q` position the propagating path adds:
		// the id is implied by the key this is recorded under, and repeating it
		// would make `${steps.<id>.error}` name its own step. The durable
		// driver draws the same line at the same place — see stepFailed.
		if tolerated != nil {
			tolerated[node.GetId()] = struct{}{}
		}
		scope.Outputs.StepValues[node.GetId()] = failureRecord(err)

		return nil
	}

	if outputs != nil {
		scope.Outputs.StepValues[node.GetId()] = outputs
	}

	return nil
}

// failureRecord shapes a step failure into the outputs recorded under that
// step's id: [FailedStepOutputs] for an ordinary failure, and the richer
// [StepFailureRecord.Record] when the failing step owns an account of its own —
// an exhausted loop's `results` ([LoopExhaustedError]), a failed switch's
// selection ([SwitchBodyError]).
//
// The account is recognised by direct type assertion, never through an unwrap
// chain, and that is the point rather than a shortcut: only the step that owns
// it raises the error bare, so only that step's own entry carries it. The same
// failure propagating out of a call or an enclosing for_each arrives here
// wrapped in a position (`workflow %q: …`, `iteration %d: …`) and records as the
// plain failure it is at that level — which is also exactly what the durable
// driver's failedAt does, reading the raw error at the one site it is raised and
// never copying the record into the wrappers it builds above it.
func failureRecord(err error) *Node_Outputs {
	text := StepErrorText(err)
	if account, ok := err.(StepFailureRecord); ok {
		return account.Record(text)
	}
	return FailedStepOutputs(text)
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
//
// slot is [registerAtCompletion] for a step written in order, and the position
// [UndoLog.Reserve] handed out for an async one. Either way the compensation is
// resolved here, at the moment the step succeeds and while its inner scope is
// live; what the slot decides is only *where in the log* it lands, which is what
// keeps the unwind in reverse written order when joins are out of order.
func runNodeWithVars(ctx context.Context, node *Node, scope *Scope, undo *UndoLog, placement UndoScope, depth int, slot int, tolerated map[string]struct{}) (*Node_Outputs, error) {
	inner, err := EvalStepVars(ctx, node, scope)
	if err != nil {
		return nil, err
	}

	outputs, err := runNode(ctx, node, inner, undo, placement, depth, tolerated)
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
	if slot == registerAtCompletion {
		undo.Register(entry)
	} else {
		undo.Fill(slot, entry)
	}

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
// The scope carries the profile and the run identity, and nothing more. The
// task's inputs were resolved when the step succeeded, so there is nothing here
// left to evaluate against a run; what remains unresolved is only what a task
// evaluates against its own response, which needs no other run scope in either
// driver.
//
// Identity is the exception, for the reason the run scope above carries it
// (#295): the task-shape policy reads `identity.namespace`, so a compensation
// dispatched with an empty identity matches no identity-keyed rule and runs
// where production would refuse it. The durable driver puts the run identity in
// the compensation's scope for exactly this reason; leaving it out here would
// make a local rehearsal permit what production denies, which is the divergence
// local runs exist to prevent.
func runUndoTask(ctx context.Context, profile string, entry *PendingUndo) error {
	scope := NewScope(profile, &Workflow_StepOutputs{StepValues: map[string]*Node_Outputs{}})
	scope.Identity = RehearsalIdentityFromContext(ctx)
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

	uctx, cancel := context.WithTimeoutCause(context.WithoutCancel(ctx), UndoBudget, ErrUndoBudgetExpired)
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
func runNode(ctx context.Context, node *Node, scope *Scope, undo *UndoLog, placement UndoScope, depth int, tolerated map[string]struct{}) (*Node_Outputs, error) {
	switch n := node.Kind.(type) {
	case *Node_Task:
		return runStepWithPolicy(ctx, n.Task, node.GetPolicy(), scope)

	case *Node_ForEach:
		// A wait inside the body reports this step as its nearest enclosing one,
		// unless the loop declares concurrency, which the durable driver runs
		// with no position at all, and this driver therefore reports with none
		// either even though it runs the iterations sequentially. See
		// [waitReporting.unpositioned].
		if n.ForEach.GetMaxParallel() > 1 {
			ctx = enterConcurrentWait(ctx)
		} else {
			ctx = pushWaitAncestor(ctx, node.GetId())
		}

		return runForEach(ctx, n.ForEach, scope, undo, depth)

	case *Node_Loop:
		// The body's placement composes with the scope this loop itself sits in
		// — see [UndoScope.IntoLoop] — rather than always being [UndoScopeLoop].
		// A `loop:` written inside a for_each body or a parallel branch is legal,
		// and must not become an escape hatch out of the concurrency refusal that
		// already applies there.
		return runLoop(pushWaitAncestor(ctx, node.GetId()), n.Loop, scope, undo, placement.IntoLoop(), depth)

	case *Node_Parallel:
		// Branches are concurrent work, whatever order this driver happens to
		// run them in, so a wait inside one reports no ancestry: the position
		// the durable driver refuses to claim.
		return nil, runParallel(enterConcurrentWait(ctx), n.Parallel, scope, undo, depth)

	case *Node_Wait:
		return runWait(ctx, node, n.Wait, scope)

	case *Node_Value:
		// Through the shared [EvalValueNode] rather than inline, because the
		// durable driver reaches the identical function from its own runNode: a
		// value is the one step whose whole observable behaviour is what the
		// expression evaluated to, so two spellings of it would be two answers
		// waiting to differ.
		return EvalValueNode(ctx, n.Value, scope)

	case *Node_Switch:
		// Sequential, exactly-one-body work, so a wait inside the taken body
		// reports this step as its nearest enclosing one, the way a sequential
		// loop's does. placement passes through unchanged: the body runs once,
		// in order, in the run's own scope, so an `undo:` there means exactly
		// what it would mean on the same step written under an `if:`.
		return runSwitch(pushWaitAncestor(ctx, node.GetId()), n.Switch, scope, undo, placement, depth, tolerated)

	case *Node_Call:
		// The callee's own placement composes with the scope this call itself
		// sits in — see [UndoScope.IntoCall] — rather than always being
		// [UndoScopeCall]. A call reached from inside a for_each body or a
		// parallel branch must not become an escape hatch out of the
		// concurrency refusal just because a call sits between the two.
		return runCall(pushWaitAncestor(ctx, node.GetId()), n.Call, scope, undo, placement.IntoCall(), depth+1)

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
// undo is the caller's own log, passed straight through rather than a fresh one:
// a call is sequential, compile-time-vendored control flow — the callee's steps
// run to completion, in order, before the step that follows the call does, on
// both drivers — so a compensation the callee registers belongs on the same
// run-level stack a top-level step's would, and undoes in the same reverse
// registration order across the boundary.
//
// placement is the callee's own body scope, already composed by the caller
// through [UndoScope.IntoCall] — not always [UndoScopeCall]. A call reached
// from the top level or from another call's body does run its callee at
// [UndoScopeCall], which [CheckUndoPlacement] honours for the reason above; a
// call reached from inside a `for_each` body, a `parallel` branch, or a
// `loop:` body carries that restriction straight through, because nothing
// about a call sitting in between changes why the enclosing scope refused
// `undo:` in the first place. [CheckUndoPlacement] still refuses `undo:` on
// the `call:` step itself regardless, since a call has no effect of its own —
// the compensation belongs on the callee's steps, not on the step that
// reaches them.
func runCall(ctx context.Context, call *Call, scope *Scope, undo *UndoLog, placement UndoScope, depth int) (*Node_Outputs, error) {
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

	if err := runNodes(ctx, callee.GetSteps(), inner, undo, placement, depth, nil); err != nil {
		// Named, because a failure inside a called workflow reported without
		// saying which one leaves a reader looking through the caller for a step
		// that is not there.
		return nil, fmt.Errorf("workflow %q: %w", callee.GetName(), err)
	}

	return CallOutputs(ctx, callee, inner)
}

// runSwitch dispatches on one value and runs the body [SelectSwitchCase] picks.
//
// The selection — one evaluation of the discriminant, first literal match in
// written order, default when none, an error when the discriminant cannot be
// computed — is entirely [SelectSwitchCase]'s, shared with the durable driver,
// so the two cannot disagree about which branch a value takes or what the
// record says. What is local here is only how the chosen body runs: a plain
// recursion into [runNodes] against the same scope, which is what merges the
// body's step outputs into the enclosing namespace the way a parallel branch's
// merge — exactly one body ran, so there is nothing to collide with.
//
// tolerated passes straight through rather than being reset to nil, and that is
// the same answer the durable driver gives: its runSwitch recurses on its *own*
// executor, so the body inherits whatever collector that executor carries. The
// reason both say it is that a switch body is not a nested scope the way a
// parallel branch or a callee is — its step outputs merge into the enclosing
// namespace, so a body step that fails and is tolerated is, from the enclosing
// walk's view, exactly as tolerated as a sibling written beside the switch.
//
// Nothing reads a switch-body id out of the set today: the only reader is
// [AttachIterationBinding], and what reaches it is filtered through
// [onlyBodyOutputs] to the loop body's *direct* ids, which a body step of a
// switch is not. So this is structure rather than an observable difference —
// written this way because the alternative is the two drivers threading one
// piece of state differently, which is the shape every disagreement found so
// far has had.
func runSwitch(ctx context.Context, sw *Switch, scope *Scope, undo *UndoLog, placement UndoScope, depth int, tolerated map[string]struct{}) (*Node_Outputs, error) {
	body, outputs, err := SelectSwitchCase(ctx, sw, scope)
	if err != nil {
		return nil, err
	}

	// enterAtomicBlock because the durable driver runs the taken body at
	// `susp + 1` — a switch is never a suspension position — so a for_each
	// written in a switch arm runs atomically there and is weighed here too
	// ([CheckAtomicBlockActivities]).
	if err := runNodes(enterAtomicBlock(ctx), body, scope, undo, placement, depth, tolerated); err != nil {
		// Wrapped so the selection survives the failure: recordStepOutcome
		// records this step through failureRecord, which reads the account off
		// [SwitchBodyError] the same way it reads an exhausted loop's. Without
		// it the switch's own entry holds the failure text alone, and the arm
		// that ran is absent from the record every reader of it consults. The
		// durable driver's runSwitch wraps at the identical point.
		return nil, &SwitchBodyError{Err: err, Selection: outputs}
	}

	return outputs, nil
}

// atomicBlockKey marks a context as being inside work the durable driver runs
// with no Continue-As-New seam: a `parallel:` branch, a loop body, or a
// `switch:` arm. It is this driver's mirror of the engine's suspend depth
// (`susp` in engine/execute.go): the engine increments that counter at exactly
// the descents that mark this context, so "is this for_each atomic in
// production" gets the same answer from both drivers. A context value rather
// than a parameter for the reason the wait-reporting markers are
// ([enterConcurrentWait]): the fact is monotone — nothing inside an atomic
// stretch un-enters it, calls included — and every descent already threads a
// context.
type atomicBlockKey struct{}

// enterAtomicBlock marks the context as suspension-opaque; see [atomicBlockKey].
func enterAtomicBlock(ctx context.Context) context.Context {
	return context.WithValue(ctx, atomicBlockKey{}, true)
}

// inAtomicBlock reports whether the durable driver would be unable to suspend
// at this position — the local half of the engine's `susp > 0`.
func inAtomicBlock(ctx context.Context) bool {
	v, _ := ctx.Value(atomicBlockKey{}).(bool)
	return v
}

// runForEach runs a loop body once per item.
//
// Iterations run sequentially here regardless of MaxParallel. The durable driver
// honors it, but reproducing concurrency locally would only reorder side effects
// without reproducing anything an author can act on — and sequential execution
// makes a local run's output deterministic, which is what makes it useful for
// comparison.
func runForEach(ctx context.Context, loop *ForEach, scope *Scope, undo *UndoLog, depth int) (*Node_Outputs, error) {
	items, err := ResolveItems(ctx, loop, scope)
	if err != nil {
		return nil, err
	}

	// The trip-count ceiling, applied at the one moment the length of the
	// resolved list is known and before any iteration has run. The durable
	// driver checks at exactly this point through the same
	// [CheckForEachItems], so a fan-out this rehearsal refuses is one
	// production refuses too, which is the whole reason a local run exists.
	if err := CheckForEachItems(items); err != nil {
		return nil, err
	}

	// A loop the durable driver would run as one atomic stretch of history —
	// one declaring concurrency, or one reached where suspension is already
	// illegal ([inAtomicBlock], the engine's `susp > 0`) — is weighed before
	// anything runs, through the same [CheckAtomicBlockActivities] at the same
	// point. This driver runs iterations sequentially and has no history to
	// protect; it refuses anyway, because a fan-out the rehearsal admits and
	// production refuses is the drivers disagreeing about what the file means,
	// [CheckForEachItems]'s exact reasoning one bound over.
	if loop.GetMaxParallel() > 1 || inAtomicBlock(ctx) {
		if err := CheckAtomicBlockActivities(len(items), loop.GetBody()); err != nil {
			return nil, err
		}
	}

	name := IteratorName(loop)
	iterations := make([]*Workflow_StepOutputs, 0, len(items))
	resultsBytes := 0

	// The first failing iteration by index, reported only after a concurrent
	// loop has run everything it launched. Sequential loops never set it: a
	// MaxParallel of one means later iterations were genuinely never started,
	// on either driver, so stopping at the failure is the honest account.
	var firstErr error

	// The body runs where the durable driver cannot suspend — its runForEach
	// passes `susp + 1` into every iteration — so a for_each nested in this
	// body is atomic there and must be weighed here too.
	bodyCtx := enterAtomicBlock(ctx)

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

		// Accumulate privately, then merge by iteration index. Completion time is
		// deliberately not part of the compensation ordering contract.
		//
		// toleratedSteps is the iteration's own record of which body steps
		// failed and were tolerated — the marker [AttachIterationBinding] keys
		// on below, so a successful step that merely *declares* an output
		// named `error` is never mistaken for a failure.
		iterationUndo := NewUndoLog(nil)
		toleratedSteps := map[string]struct{}{}
		if err := runNodes(bodyCtx, loop.GetBody(), iterationScope, iterationUndo, UndoScopeConcurrent, depth, toleratedSteps); err != nil {
			undo.Append(iterationUndo)
			if loop.GetMaxParallel() > 1 {
				// A concurrent fan-out launches every iteration before it can
				// know one failed, so the durable driver runs, joins, and
				// compensates all of them. This rehearsal owes an author the
				// same account: the remaining iterations run, their logs merge
				// in index order, and the first failure by index is still the
				// answer. Returning here instead would compensate less work
				// locally than production would do and take back.
				if firstErr == nil {
					firstErr = fmt.Errorf("iteration %d: %w", i, err)
				}
				continue
			}
			return nil, fmt.Errorf("iteration %d: %w", i, err)
		}
		undo.Append(iterationUndo)

		// The same byte bound the durable driver applies to a `for_each`'s
		// accumulating `results`, at the same point — right after an iteration's
		// outputs are known — so a local rehearsal fails identically to production
		// rather than accumulating silently past what a local process can hold.
		// This is #229's `loop:` fix reached through the shared [MaxLoopResultsBytes]
		// (see [AccumulateForEachResult]); the position wrap is spelled the way the
		// durable driver's stepFailed composes it, `"iteration %d: "`, so the
		// recorded sentence matches across drivers.
		// The iteration's item rides on any tolerated failure the body recorded
		// ([AttachIterationBinding], keyed on toleratedSteps — the walk's own
		// record, never the outputs' names), attached before the accumulate so
		// the byte bound weighs it — and at the identical point the durable
		// driver attaches it, in both its sequential and concurrent paths.
		var sizeErr error
		iterations, resultsBytes, sizeErr = AccumulateForEachResult(iterations, resultsBytes,
			AttachIterationBinding(onlyBodyOutputs(loop.GetBody(), iterationOutputs), item, toleratedSteps))
		if sizeErr != nil {
			return nil, fmt.Errorf("iteration %d: %w", i, sizeErr)
		}
	}

	if firstErr != nil {
		return nil, firstErr
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
//
// A second bound applies to what the loop accumulates rather than how many times
// it runs, per #229: `results` is bounded in bytes through [MaxLoopResultsBytes],
// enforced by [AccumulateLoopResult] at the identical point the durable driver's
// runLoop enforces it. A local run never suspends or resumes, so it has no
// Continue-As-New frame to suppress `results` from carrying across — the other
// half of #229's fix — and holds only this bound; see the package doc on
// [LoopResumeResults] for why suppression could not live here even if it did.
// undo is the run's own log, passed straight through rather than withheld: a
// loop's iterations are sequential here and on the durable driver, so a
// compensation a body step registers belongs on the same run-level stack a
// top-level step's does, and comes off newest-first — iteration 3's before
// iteration 2's. [UndoScope] argues that at length; #253 is where the refusal
// that used to sit here was retired.
//
// placement is the body's own scope, already composed by the caller through
// [UndoScope.IntoLoop], which is what keeps a loop inside a `for_each` from
// laundering the concurrency refusal.
func runLoop(ctx context.Context, loop *Loop, scope *Scope, undo *UndoLog, placement UndoScope, depth int) (*Node_Outputs, error) {
	name := loop.GetState()
	max := LoopMaxIterations(loop)

	// Evaluated once, before the first iteration, against the scope the loop sits in
	// — the state does not exist yet, which is why this is where it is defined.
	state, err := LoopInitialState(ctx, loop, scope)
	if err != nil {
		return nil, err
	}

	resultsBytes := 0
	iterations := make([]*Workflow_StepOutputs, 0)

	for i := 0; ; i++ {
		if i >= max {
			// The budget is spent and `until:` never held. A distinct failure, not a
			// silent stop: the loop did not do what it was asked. The error carries
			// the iterations that ran, so the recorded entry can say which of them
			// failed and that nothing past the budget was ever attempted — see
			// [LoopExhaustedError]. Never truncated here: a local run has no resume
			// to have dropped history across.
			return nil, LoopExhausted(iterations, max, false)
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

		// The run's own log and the composed placement: a body step's `undo:` is
		// registered onto the same stack a top-level step's is, once per iteration.
		// Where the composed placement is still [UndoScopeConcurrent] — a loop
		// inside a for_each body — [CheckUndoPlacement] refuses it rather than this
		// level quietly ignoring it.
		//
		// toleratedSteps collects which body steps failed and were tolerated —
		// the marker the attach below keys on, per iteration, so a successful
		// step that merely declares an output named `error` is never mistaken
		// for a failure.
		toleratedSteps := map[string]struct{}{}
		// enterAtomicBlock because the durable driver's runLoop passes
		// `susp + 1` into every iteration: a for_each written in a loop body
		// runs atomically inside that iteration there, so it is weighed here
		// too ([CheckAtomicBlockActivities]).
		if err := runNodes(enterAtomicBlock(ctx), loop.GetBody(), iterationScope, undo, placement, depth, toleratedSteps); err != nil {
			return nil, fmt.Errorf("iteration %d: %w", i, err)
		}

		// The carried state the iteration ran with rides on any tolerated
		// failure the body recorded ([AttachIterationBinding]) — nil for a loop
		// that binds nothing, which attaches nothing. Before the accumulate so
		// the byte bound weighs it, matching the durable driver's point.
		var sizeErr error
		iterations, resultsBytes, sizeErr = AccumulateLoopResult(iterations, resultsBytes,
			AttachIterationBinding(onlyBodyOutputs(loop.GetBody(), iterationOutputs), state, toleratedSteps))
		if sizeErr != nil {
			return nil, fmt.Errorf("iteration %d: %w", i, sizeErr)
		}

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
// Branches run one at a time in the local driver for the same reason loop
// iterations do: determinism. Because branches may not depend on each other's
// outputs, running them one at a time produces the same result the durable driver
// reaches concurrently — and *which* one goes first is the schedule's choice
// ([SchedulePointParallelBranches]), which by default is declaration order, the
// order this driver has always taken.
//
// Everything an author can see is computed in declaration order regardless of
// the order the branches ran in: the merge below, the first failure reported, and
// the order each branch's private undo log joins the enclosing one. That last one
// used to follow execution order, which was the same thing while execution order
// was declaration order; under a schedule that is free to differ it is not, and a
// compensation log ordered by who finished first is exactly the completion order
// #418 promises is never observable.
func runParallel(ctx context.Context, parallel *Parallel, scope *Scope, undo *UndoLog, depth int) error {
	before := cloneStepOutputs(scope.GetOutputs())

	// Each branch's outputs, merged only at the join and only when every branch
	// succeeded: the durable driver merges nothing into the enclosing scope on
	// a failed parallel, so a tolerated failure must not leave a sibling's
	// partial outputs visible here either.
	branchResults := make([]*Workflow_StepOutputs, len(parallel.GetBranches()))

	// Kept per branch and consulted in declaration order once every branch has
	// run, rather than recorded as the branches finish.
	branchErrs := make([]error, len(parallel.GetBranches()))
	branchUndos := make([]*UndoLog, len(parallel.GetBranches()))

	for _, i := range ScheduleOrder(SchedulerFromContext(ctx), SchedulePointParallelBranches, len(parallel.GetBranches())) {
		branch := parallel.GetBranches()[i]
		// Every branch starts from the outputs that existed before the block, so
		// a branch cannot observe a sibling's work even though they run one at a
		// time here, in whatever order the schedule chose. A workflow that
		// accidentally depended on that would behave differently under concurrent
		// execution.
		branchOutputs := cloneStepOutputs(before)

		// Derived rather than rebuilt. A hand-built Scope here is how the profile
		// went missing in the first place: it names the two fields somebody was
		// thinking about, and silently omits every other one the type grows.
		branchScope := scope.WithOutputs(branchOutputs)
		branchUndo := NewUndoLog(nil)
		branchUndos[i] = branchUndo
		// enterAtomicBlock because the durable driver runs a branch at
		// `susp + 1`: a for_each written inside a `parallel:` branch runs
		// atomically there whatever its `max_parallel:` says, so it is
		// weighed here too ([CheckAtomicBlockActivities]).
		if err := runNodes(enterAtomicBlock(ctx), branch.GetSteps(), branchScope, branchUndo, UndoScopeConcurrent, depth, nil); err != nil {
			// Branches are concurrent by declaration: the durable driver has
			// launched every one of them before it can learn that any failed,
			// then joins, merges every private log, and reports the first
			// failure by branch index. This rehearsal does the same, or a
			// failing first branch would hide both the work and the
			// compensations of a second branch production runs regardless.
			branchErrs[i] = fmt.Errorf("branch %d: %w", i, err)

			continue
		}
		branchResults[i] = branchOutputs
	}

	// In declaration order, whatever order the branches ran in: this is the order
	// the compensations unwind in, and it is a property of the file.
	for _, branchUndo := range branchUndos {
		undo.Append(branchUndo)
	}

	// The first failing branch by declaration index, reported only after every
	// branch has run, because that is what the durable driver's join reports.
	for _, err := range branchErrs {
		if err != nil {
			return err
		}
	}

	// Merge at the join, in declaration order, exactly as the durable driver
	// does after its channel drain: the merged result is the same regardless of
	// the order branches completed in, and nothing merges on failure.
	for i, branch := range parallel.GetBranches() {
		for _, node := range branch.GetSteps() {
			if outputs, ok := branchResults[i].GetStepValues()[node.GetId()]; ok {
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

	// The deployment's task-shape policy (#187), consulted once for the whole
	// dispatch — above the retry loop below, not inside it, because a
	// dispatch's task name and identity do not change between retries of the
	// same step, and a denial must produce none of a retry's side effects
	// either. Placed after inputs resolve and before any attempt runs: inputs
	// resolution never touches a secret reference (see [ResolveTaskInputs]
	// and eval.go's own note on [Value_SecretRef]), so a denied dispatch here
	// has still resolved no credential — the deployment-side echo of
	// invariant 7 the design record for #187 states. The durable driver
	// checks at the identical position, once per activity entry
	// (`engine/activities.go`), which is what keeps the two drivers agreeing
	// about which dispatches are denied.
	// scope.GetLocal() is true for a rehearsal run through any local-driver
	// entry point; it changes nothing about the decision above, only
	// whether a resulting denial's message says so — see [CheckTaskPolicy]'s
	// own doc.
	if err := CheckTaskPolicy(ctx, resolved.GetName(), scope.GetIdentity(), scope.GetLocal()); err != nil {
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
		//
		// Given a cause: this budget and the per-attempt timeout in
		// [runStepAttempt] both end a step with the same bare
		// context.DeadlineExceeded, and today nothing an operator reads says
		// which one it was — "this one attempt ran long" and "the step ran long
		// across every retry" are different facts about the same failure. The
		// per-attempt context stays uncaused, so [withCancellationCause] can tell
		// them apart by whether a cause is present at all.
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeoutCause(ctx, timeouts.ScheduleToClose,
			fmt.Errorf("schedule-to-close timeout of %s reached", timeouts.ScheduleToClose))
		defer cancel()
	}

	for attempt := 1; ; attempt++ {
		var out *Node_Outputs
		out, err = runStepAttempt(ctx, resolved, timeouts.StartToClose, scope)
		if err == nil {
			return out, nil
		}
		err = withCancellationCause(ctx, err)

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
			return nil, withCancellationCause(ctx, ctx.Err())
		case <-ClockFromContext(ctx).After(delay):
		}
	}
}

// withCancellationCause enriches err with the reason ctx (or the timeout
// nearest to it) was given for stopping, when that reason is more specific
// than the bare sentinel err already carries.
//
// [context.WithTimeoutCause] and [context.WithCancelCause] let a context name
// why it stops, but a caller several layers below only ever sees
// context.Canceled or context.DeadlineExceeded through ctx.Err() — the two
// sentinels every std-library caller already checks with errors.Is, and the
// only ones a task's own transport error is likely to wrap. [context.Cause]
// is where the reason actually is, and reading it here is what turns "context
// deadline exceeded" into "schedule-to-close timeout of 5m0s reached" without
// disturbing errors.Is(err, context.DeadlineExceeded) for a caller checking
// the ordinary way: err is wrapped with %w, so the chain still contains the
// original sentinel.
//
// A cause that is itself just [context.Canceled] or [context.DeadlineExceeded]
// — the default [context.WithCancelCause] leaves when nobody named a reason —
// is not appended; wrapping "context canceled" in "context canceled" is noise,
// not a diagnostic. Nor is a cause appended to an err that is not itself a
// cancellation: a step that failed for its own reason while an unrelated outer
// deadline happened to be running is not that deadline's business.
//
// Idempotent by construction, which matters because this is called at more than
// one layer on the same error as it propagates: [runStepWithPolicy] calls it
// where a step attempt or its retry backoff observes cancellation, and eval's
// run-level fallback calls it again on whatever came back, in case nothing more
// specific was running (a run parked at a `wait:` when the stop arrives). Both
// calls read from context.Cause of a context that, absent a narrower budget in
// between, names the same reason — so without a check somewhere the fallback
// would append it a second time, rendering "context canceled: maintenance:
// maintenance" instead of naming the reason once. [WithCause], which this
// calls to do the actual appending, is where that check lives:
// [causeEnrichedError] marks an error already enriched, and appending to one is
// a no-op rather than a second suffix.
func withCancellationCause(ctx context.Context, err error) error {
	if err == nil {
		return err
	}
	if !errors.Is(err, context.Canceled) && !errors.Is(err, context.DeadlineExceeded) {
		return err
	}

	cause := context.Cause(ctx)
	if cause == nil || errors.Is(cause, context.Canceled) || errors.Is(cause, context.DeadlineExceeded) {
		return err
	}

	return WithCause(err, cause)
}

// WithCause appends a fixed, named cause to err, in the same shape and with the
// same idempotence [withCancellationCause] gives a context-derived one — see
// [causeEnrichedError].
//
// Exported for the durable driver, which meets the equivalent of a cancellation
// cause without a context to read one from: Temporal reports a compensation
// activity cut off by [UndoBudget] as its own TimeoutError, wrapped in an
// envelope this package's [StepErrorText] already knows how to shed (see
// engine.recordedStepError), but with nothing resembling [context.Cause]
// underneath for [withCancellationCause] to read. `engine.runUndoTask` calls
// this directly once it has recognized that shape, naming the same
// [ErrUndoBudgetExpired] the local driver's [runUndoOnCancel] attaches through
// a context — one cause, spelled once here, read by both drivers, rather than
// the durable driver inventing its own sentence for the same fact.
func WithCause(err error, cause error) error {
	if err == nil || cause == nil {
		return err
	}

	var already *causeEnrichedError
	if errors.As(err, &already) {
		return err
	}

	return &causeEnrichedError{err: err, cause: cause}
}

// causeEnrichedError is what [withCancellationCause] and [WithCause] return
// once a cause has been appended, and what both check for to avoid appending
// a second one.
//
// A distinct type rather than a plain fmt.Errorf-built error, because
// idempotence needs something to test for: a string check against the
// rendered text would risk matching a cause's own wording that legitimately
// recurs, where errors.As against this type can only ever match an error this
// package itself produced.
//
// Renders identically to the fmt.Errorf("%w: %s", err, cause) this replaced —
// same text, same errors.Is behavior through Unwrap — so no caller or test
// observes the difference except the doubling this exists to prevent.
type causeEnrichedError struct {
	err   error
	cause error
}

func (e *causeEnrichedError) Error() string {
	return fmt.Sprintf("%s: %s", e.err, e.cause)
}

func (e *causeEnrichedError) Unwrap() error {
	return e.err
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

	// And what it weighs (#787). The element bound above caps what a later
	// expression pays to walk the result; this caps what the substrate is
	// asked to store it as — on the durable driver the outputs returned here
	// are an activity's result, refused past Temporal's blob limit, and a
	// refusal at completion retries into a misdiagnosed ScheduleToClose
	// timeout. The admission bounds upstream both admit more than that limit:
	// a plugin response cap of 4 MiB, and the http task's default outputs
	// carrying a parsed body twice. Same classification, same non-retryability
	// reasoning: the size of a result does not change between attempts.
	if err := CheckTaskOutputSize(out); err != nil {
		return nil, NewTaskError(t.Name, ErrorKindLimitExceeded, err)
	}

	return out, nil
}
