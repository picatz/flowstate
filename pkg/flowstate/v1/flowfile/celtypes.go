package flowfile

import (
	"maps"
	"slices"
	"strings"

	"github.com/google/cel-go/cel"
	"github.com/google/cel-go/common/types"
	expr "google.golang.org/genproto/googleapis/api/expr/v1alpha1"

	v1 "github.com/picatz/flowstate/pkg/flowstate/v1"
)

// What the file already says about the type of a name.
//
// [checkExpressionTypes] assumes every name an expression mentions is `dyn`, and
// that stays true of everything the file does not state: a response body, a
// plugin's output, a loop's result. But a Flowfile states more than it used to be
// credited for, and an expression reading any of it was never judged:
//
//	inputs:
//	  port: {type: int}
//	steps:
//	  - id: s
//	    value: ${"abc"}
//	  - id: n
//	    value: ${inputs.port.startsWith("8") && steps.s.value + 1 > 0}
//
// `inputs.port` is an int by declaration and `steps.s.value` is a string by the
// checker's own answer for the expression that produced it, and both reached the
// next expression as `dyn`, so a file that cannot run said `ok` (#1634).
//
// A [typeTable] is that knowledge, and nothing else is added to the checker: the
// same environment, with a few more declarations in it. Each is a qualified
// variable — `inputs.port`, `steps.s.value` — which is how cel-go reads a dotted
// name it has a declaration for, so the root (`inputs`, `steps`) stays `dyn` and
// every other selection through it is exactly as silent as before.
//
// # Where it stays quiet, on purpose
//
//   - A type is declared only for a name the file states. An input it does not
//     declare, or a step it does not contain, is the reference walk's to report
//     in the sentence written for it ([validateInputRefs]); guessing a type for
//     one would report the same mistake twice.
//   - A step id that appears more than once is `dyn`. An id is unique within a
//     visibility domain and not within a file (two sibling loops may each have
//     a body step called `page`), and this check carries no scope, so it cannot
//     tell which one a reference means. Staying silent is the one answer that
//     cannot be wrong.
//   - Only a `value:` step's `value` is typed, because it is the one output whose
//     type is the checker's own answer. A task's outputs come from descriptors
//     and an author-shaped `outputs:` from their expressions, both later slices.
//
// The checker's answer for a `value:` step is exact. A value round-trips through
// the run document as the CEL type it had (a timestamp, a duration, bytes and a
// uint all do), so the type of `steps.s.value` at run time is the type the
// expression checked to; where the checker could not decide (a free type
// parameter, `dyn`) the table says `dyn`.
type typeTable struct {
	// inputs are the declared inputs' types by name. A declaration with no type
	// is absent rather than `dyn`: it promised nothing.
	inputs map[string]*cel.Type

	// values are the `value:` steps whose id is unique in the workflow, by id.
	values map[string]*valueStep
}

// A valueStep is one `value:` step and what is known of its type.
type valueStep struct {
	value *v1.Value

	// typed is set once resolved is true; resolving is true while the type is being
	// computed, which is how a reference cycle (a step whose value reads itself, or
	// two that read each other) ends as `dyn` rather than as unbounded recursion.
	// A cycle is the reference walk's to report as the forward reference it is.
	typed     *cel.Type
	resolved  bool
	resolving bool
}

// newTypeTable reads a workflow's declarations and `value:` steps.
func newTypeTable(wf *v1.Workflow) *typeTable {
	table := &typeTable{
		inputs: map[string]*cel.Type{},
		values: map[string]*valueStep{},
	}

	for _, declaration := range wf.GetDeclaredInputs() {
		if declared := declaration.DeclaredType(); declared != nil {
			table.inputs[declaration.GetName()] = v1.CELType(declared)
		}
	}

	seen := map[string]int{}
	v1.WalkNodes(wf.GetSteps(), v1.Walk{Node: func(node *v1.Node) {
		seen[node.GetId()]++
		if value, ok := node.GetKind().(*v1.Node_Value); ok {
			table.values[node.GetId()] = &valueStep{value: value.Value}
		}
	}})
	for id, count := range seen {
		if count > 1 {
			delete(table.values, id)
		}
	}

	return table
}

// leavesFor returns the typed declarations an expression can use: for each
// `inputs.<name>` and `steps.<id>.value` it selects that the file states a type
// for, the qualified name and the type.
//
// Nil-safe, and nil when there is nothing to declare, so a caller without a table
// (a subtree checked on its own) gets the `dyn` environment it always had.
func (t *typeTable) leavesFor(parsed *expr.ParsedExpr) map[string]*cel.Type {
	if t == nil || parsed == nil {
		return nil
	}

	rooted, _, inputs, _, _, _, _ := referencedIdentifiers(parsed)

	var leaves map[string]*cel.Type
	add := func(name string, typed *cel.Type) {
		if leaves == nil {
			leaves = map[string]*cel.Type{}
		}
		leaves[name] = typed
	}

	for _, name := range inputs {
		if typed, ok := t.inputs[name]; ok {
			add(v1.InputsRoot+"."+name, typed)
		}
	}
	for _, ref := range rooted {
		if ref.Output != "value" {
			continue
		}
		if typed := t.valueType(ref.ID); typed != nil {
			add(v1.StepsRoot+"."+ref.ID+".value", typed)
		}
	}

	return leaves
}

// valueType is the type of `steps.<id>.value`, or nil where it is not known: not a
// `value:` step, not a unique id, or an answer of `dyn`.
func (t *typeTable) valueType(id string) *cel.Type {
	step, ok := t.values[id]
	if !ok {
		return nil
	}
	if step.resolved {
		return step.typed
	}
	if step.resolving {
		return nil
	}

	step.resolving = true
	step.typed = t.inferValue(step.value)
	step.resolving, step.resolved = false, true

	return step.typed
}

// inferValue is the type a `value:` holds, nil when it is `dyn`.
func (t *typeTable) inferValue(value *v1.Value) *cel.Type {
	switch kind := value.GetKind().(type) {
	case *v1.Value_Literal:
		return knownType(literalCELType(kind.Literal))

	case *v1.Value_Expr:
		checked, ok := checkedType(t, kind.Expr)
		if !ok {
			return nil
		}

		return knownType(looseContainers(checked))

	case *v1.Value_Structure_:
		// A structure is a list or a mapping and nothing else; what it holds may be a
		// secret reference, which is not a type to infer.
		if kind.Structure.GetList() != nil {
			return cel.ListType(cel.DynType)
		}

		return cel.MapType(cel.StringType, cel.DynType)

	default:
		return nil
	}
}

// checkedType runs the checker over one expression in the environment the table
// gives it and returns the type it decided, normalised: a free type parameter
// (`[]` is `list(_T)`) is `dyn`, since nothing about the file pins it down.
//
// False when the expression does not check, which [checkExpressionTypes] reports
// in its own sentence.
func checkedType(t *typeTable, parsed *expr.ParsedExpr) (*cel.Type, bool) {
	env, err := envDeclaring(referencedNames(parsed.GetExpr()), t.leavesFor(parsed))
	if err != nil {
		return nil, false
	}

	checked, issues := env.Check(cel.ParsedExprToAst(parsed))
	if issues != nil && issues.Err() != nil {
		return nil, false
	}

	return normalizeType(checked.OutputType()), true
}

// normalizeType replaces what a checker leaves open with `dyn`, throughout.
func normalizeType(t *cel.Type) *cel.Type {
	if t == nil {
		return cel.DynType
	}

	switch t.Kind() {
	case types.TypeParamKind, types.ErrorKind:
		return cel.DynType
	case types.ListKind:
		if params := t.Parameters(); len(params) == 1 {
			return cel.ListType(normalizeType(params[0]))
		}
	case types.MapKind:
		if params := t.Parameters(); len(params) == 2 {
			return cel.MapType(normalizeType(params[0]), normalizeType(params[1]))
		}
	}

	return t
}

// looseContainers keeps what a `value:` is known to be and drops what it merely
// happened to contain: a list is `list(dyn)` and a map `map(string, dyn)`, and a
// type with no stored form (`optional`, an opaque type) is not known at all.
//
// An element type inferred from a literal is a statement about that literal and
// not about every value a reader may meet through it. `{"volume": 7}` checks to
// `map(string, int)`, and `steps.prefs.value.?muted.orValue(false)` is a read of
// a key the literal never had, which the run resolves to its `false` and the
// checker, holding the inferred value type, refuses as `optional(int).orValue(bool)`
// (examples/expressions reads exactly that). Element typing is the author's to
// declare, once a declaration can say it (#1640), and until then the container
// is the whole claim.
func looseContainers(t *cel.Type) *cel.Type {
	switch t.Kind() {
	case types.ListKind:
		return cel.ListType(cel.DynType)
	case types.MapKind:
		if params := t.Parameters(); len(params) == 2 && params[0].Kind() == types.StringKind {
			return cel.MapType(cel.StringType, cel.DynType)
		}

		return cel.MapType(cel.DynType, cel.DynType)
	case types.OpaqueKind:
		return cel.DynType
	default:
		return t
	}
}

// knownType is nil for `dyn`, which is how this file says "not known": a
// declaration of `dyn` would be the environment it already has.
func knownType(t *cel.Type) *cel.Type {
	if t == nil || t.Kind() == types.DynKind {
		return nil
	}

	return t
}

// literalCELType is the type of a literal value.
//
// A map is keyed by strings when every key is one, and a list says nothing about
// its elements: both are `dyn`-valued because a literal's entries are not
// required to agree, and a type that claimed they do would refuse files that run.
// A `uint` literal is `dyn`: the parser stores every number a YAML integer can be
// as signed, so one that arrives unsigned is out of the signed range and a
// boundary decision (#1432), not something to type here.
func literalCELType(literal *expr.Value) *cel.Type {
	switch kind := literal.GetKind().(type) {
	case *expr.Value_StringValue:
		return cel.StringType
	case *expr.Value_Int64Value:
		return cel.IntType
	case *expr.Value_DoubleValue:
		return cel.DoubleType
	case *expr.Value_BoolValue:
		return cel.BoolType
	case *expr.Value_BytesValue:
		return cel.BytesType
	case *expr.Value_NullValue:
		return cel.NullType
	case *expr.Value_ListValue:
		return cel.ListType(cel.DynType)
	case *expr.Value_MapValue:
		for _, entry := range kind.MapValue.GetEntries() {
			if _, ok := entry.GetKey().GetKind().(*expr.Value_StringValue); !ok {
				return cel.DynType
			}
		}

		return cel.MapType(cel.StringType, cel.DynType)
	default:
		return cel.DynType
	}
}

// cacheKeyFor is the cache key of an environment: the names declared `dyn`, and
// each typed leaf with its type, in a stable order.
func cacheKeyFor(names []string, leaves map[string]*cel.Type) string {
	var key strings.Builder
	key.WriteString(strings.Join(names, "\x00"))
	for _, name := range slices.Sorted(maps.Keys(leaves)) {
		key.WriteString("\x01" + name + "=" + leaves[name].String())
	}

	return key.String()
}
