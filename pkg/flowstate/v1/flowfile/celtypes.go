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
//   - Only a top-level `value:` step is typed, and only for a position written
//     after it. A reference to a later step, or to one inside a loop body, is the
//     reference walk's to report, and typing it would add a second, spurious
//     complaint about the same reference.
//   - A name is typed only where its own definition states one
//     ([v1.NamedOutput.Type]): a `value:` step's `value`, which is the checker's
//     own answer; a task's declared field, which is what the run stores for it; a
//     called workflow's declared output, which both drivers enforce; a wait's
//     `timed_out` and `count`. A shaped `outputs:` computes its names from
//     expressions, and a loop's, a switch's and a response body's say nothing
//     further, so they stay `dyn`.
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

	// inputTypes are the same inputs' types as declared, which a record's fields
	// are read from, and records resolves the names those types carry. See
	// [typeTable.fieldPaths].
	inputTypes map[string]*v1.Type
	records    v1.TypeTable

	// values are the top-level `value:` steps whose id is unique in the workflow,
	// by id.
	values map[string]*valueStep

	// outputs are the top-level task, call and wait steps whose id is unique in the
	// workflow, by id, with the type each of their named outputs holds.
	outputs map[string]*stepOutputs

	// owner is the index, among the workflow's top-level steps, of the top-level
	// step that holds each step id (itself, for a top-level one). It is how a
	// position is placed in written order: it may read a value step only when
	// that step comes before the one it sits in.
	owner map[string]int

	// steps is how many top-level steps there are, which is the position of an
	// output: evaluated after every step, it sees them all.
	steps int

	// functions are the file's declared functions, which an expression that calls
	// one is checked against as written: the expanded tree holds the body over
	// untyped arguments, so only the call can say a string went where an int was
	// declared. Nil when the file declares none.
	functions *v1.FunctionSet
}

// A valueStep is one `value:` step and what is known of its type.
type valueStep struct {
	value *v1.Value

	// index is the step's place among the top-level steps.
	index int

	// typed is set once resolved is true. Computing it reads only steps before this
	// one (see [typeTable.leavesFor]), so it cannot meet itself: a step that reads
	// itself or a later one is a forward reference, which the reference walk reports
	// in its own sentence and this does not type.
	typed    *cel.Type
	resolved bool
}

// stepOutputs are the typed outputs of one task, call or wait step.
type stepOutputs struct {
	// types are the CEL type of each output name the step's definition types.
	types map[string]*cel.Type

	// index is the step's place among the top-level steps.
	index int
}

// newTypeTable reads a workflow's declarations and `value:` steps.
func newTypeTable(wf *v1.Workflow) *typeTable {
	table := &typeTable{
		inputs:     map[string]*cel.Type{},
		inputTypes: map[string]*v1.Type{},
		records:    v1.TypesOf(wf),
		values:     map[string]*valueStep{},
		outputs:    map[string]*stepOutputs{},
		owner:      map[string]int{},
	}

	for _, declaration := range wf.GetDeclaredInputs() {
		if declared := declaration.DeclaredType(); declared != nil {
			table.inputs[declaration.GetName()] = v1.CELType(declared)
			table.inputTypes[declaration.GetName()] = declared
		}
	}

	table.steps = len(wf.GetSteps())

	if len(wf.GetDeclaredFunctions()) > 0 {
		// The set the compiler built, rebuilt from what the spec carries; a
		// definition that did not check is absent, and a call to it was refused
		// when the file compiled.
		table.functions, _ = v1.NewFunctionSet(wf.GetProfile(), wf.GetDeclaredFunctions())
	}

	seen := map[string]int{}
	for index, top := range wf.GetSteps() {
		v1.WalkNodes([]*v1.Node{top}, v1.Walk{Node: func(node *v1.Node) {
			seen[node.GetId()]++
			table.owner[node.GetId()] = index
		}})
		if value, ok := top.GetKind().(*v1.Node_Value); ok {
			table.values[top.GetId()] = &valueStep{value: value.Value, index: index}
		} else if typed := typedOutputs(top); len(typed) > 0 {
			table.outputs[top.GetId()] = &stepOutputs{types: typed, index: index}
		}
	}
	for id, count := range seen {
		if count > 1 {
			delete(table.values, id)
			delete(table.outputs, id)
		}
	}

	return table
}

// typedOutputs are the output names of a task, call or wait step that its own
// definition gives a type, by [v1.OutputNames]: the one answer to what a step
// exposes, so this reads no descriptor or declaration of its own. Nil for any
// other kind, and for a step whose names the file cannot know.
func typedOutputs(node *v1.Node) map[string]*cel.Type {
	switch node.GetKind().(type) {
	case *v1.Node_Task, *v1.Node_Call, *v1.Node_Wait:
	default:
		return nil
	}

	names, ok := v1.OutputNames(node, nil)
	if !ok {
		return nil
	}

	var typed map[string]*cel.Type
	for _, named := range names {
		if named.Name == "" || v1.IsDyn(named.Type) {
			continue
		}
		if known := knownType(v1.CELType(named.Type)); known != nil {
			if typed == nil {
				typed = map[string]*cel.Type{}
			}
			typed[named.Name] = known
		}
	}

	return typed
}

// leavesFor returns the typed declarations an expression can use: for each
// `inputs.<name>` and `steps.<id>.<name>` it selects that the file states a type
// for, the qualified name and the type.
//
// before is how many top-level steps are visible from where the expression is
// written: a `value:` step is typed only when its index is below it, which is the
// written order a run evaluates in. A position the table cannot place, and every
// step nested in a body, sees none, so a forward or out-of-scope reference stays
// `dyn` and is reported once, by the reference walk, rather than twice.
//
// Nil-safe, and nil when there is nothing to declare, so a caller without a table
// (a subtree checked on its own) gets the `dyn` environment it always had.
func (t *typeTable) leavesFor(parsed *expr.ParsedExpr, before int) map[string]*cel.Type {
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
	for _, path := range t.fieldPaths(parsed) {
		for i, field := range path.fields {
			if field == nil {
				break
			}
			add(v1.InputsRoot+"."+strings.Join(path.names[:i+2], "."), v1.CELType(field.DeclaredType()))
		}
	}
	for _, ref := range rooted {
		if typed := t.outputType(ref.ID, ref.Output, before); typed != nil {
			add(v1.StepsRoot+"."+ref.ID+"."+ref.Output, typed)
		}
	}

	return leaves
}

// outputType is the type of `steps.<id>.<output>`, or nil where it is not known.
func (t *typeTable) outputType(id, output string, before int) *cel.Type {
	if _, isValue := t.values[id]; isValue {
		// Only a `value:` step's own `value` is the checker's answer; a task or call
		// may declare an output of any name, `value` included, so it is the step's
		// kind and not the name that picks the table.
		if output != v1.ValueOutput {
			return nil
		}

		return t.valueType(id, before)
	}
	if step, ok := t.outputs[id]; ok && step.index < before {
		return step.types[output]
	}

	return nil
}

// valueType is the type of `steps.<id>.value`, or nil where it is not known: not a
// top-level `value:` step, not a unique id, not before the position asking, or an
// answer of `dyn`.
func (t *typeTable) valueType(id string, before int) *cel.Type {
	step, ok := t.values[id]
	if !ok || step.index >= before {
		return nil
	}
	if !step.resolved {
		step.typed = t.inferValue(step.value, step.index)
		step.resolved = true
	}

	return step.typed
}

// before is the number of top-level steps visible to a site: those ahead of the
// top-level step it belongs to, all of them for a declared output, none for a
// position outside the steps (a `vars:`, a trigger, a default).
func (t *typeTable) before(site v1.ValueSite) int {
	if t == nil {
		return 0
	}
	if site.Step == "" {
		if site.Slot == v1.SlotDeclaredOutput {
			return t.steps
		}

		return 0
	}
	if index, ok := t.owner[site.Step]; ok {
		return index
	}

	return 0
}

// inferValue is the type a `value:` holds, nil when it is `dyn`.
func (t *typeTable) inferValue(value *v1.Value, before int) *cel.Type {
	switch kind := value.GetKind().(type) {
	case *v1.Value_Literal:
		return knownType(literalCELType(kind.Literal))

	case *v1.Value_Expr:
		checked, ok := checkedType(t, kind.Expr, before)
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
func checkedType(t *typeTable, parsed *expr.ParsedExpr, before int) (*cel.Type, bool) {
	env, err := envDeclaring(referencedNames(parsed.GetExpr()), t.leavesFor(parsed, before))
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
