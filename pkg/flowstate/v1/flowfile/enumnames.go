package flowfile

import (
	"fmt"
	"maps"
	"math"
	"strconv"
	"strings"

	expr "google.golang.org/genproto/googleapis/api/expr/v1alpha1"
	"google.golang.org/protobuf/reflect/protoreflect"

	v1 "github.com/picatz/flowstate/pkg/flowstate/v1"
	"github.com/picatz/flowstate/pkg/flowstate/v1/nearest"
)

// Enum names, written where the run holds a number.
//
// A plugin's answer holds a Protobuf enum as its number, and the schema names
// it: `answer.calibration == 2` is CALIBRATION_SELF_REPORTED. An author may write
// the name, `answer.calibration == CALIBRATION_SELF_REPORTED`, for every enum
// value the outputs of the file's own tasks can hold ([v1.OutputEnumsOf]).
//
// The name is resolved here, when the file compiles, and the expression the
// specification evaluates holds the integer literal. Nothing at run time knows a
// name: both drivers evaluate the same `== 2` a hand-written file would, so they
// agree by construction and a worker needs no descriptor to run the workflow.
//
// The author's spelling is kept for writing back. Each replaced node is recorded
// in the expression's source info as a macro call whose expansion is the name,
// which is the record CEL's unparser already consults to write a macro as the
// author wrote it. So `flow fmt` and Marshal print `CALIBRATION_SELF_REPORTED`,
// and evaluation never reads the record. A macro's own recorded body is left as
// written, which is where its names already are.
//
// A name an author bound (a step var, a loop's iterator or state, a function's
// parameter, a macro's variable) is theirs and is left alone.

// loweredName is one identifier replaced by its number.
type loweredName struct {
	id   int64
	name string
}

// lowerEnumNames replaces each enum value name in wf's expressions with its number.
func lowerEnumNames(wf *v1.Workflow) {
	enums := v1.OutputEnumsOf(wf, nil)
	if enums.Empty() {
		return
	}

	bound := boundNamesByStep(wf.GetSteps())

	// A function's body is not walked: it sees only its parameters, and a name in
	// it is refused when the function compiles.
	v1.WalkWorkflow(wf, v1.Walk{Value: func(site v1.ValueSite) {
		parsed := site.Value.GetExpr()
		if parsed == nil {
			return
		}

		// Per site: what is bound where this expression is written, and the
		// language's own names, which an enum value of the same name never takes.
		shadowed := bound[site.Step]
		var lowered []loweredName
		lowerEnumIdents(parsed.GetExpr(), enums, shadowed, &lowered)
		recordLowered(parsed, lowered)
	}})
}

// reservedForEnums reports a name the language owns, which an enum value of that
// name must never replace: a root (`steps`, `inputs`, ...), `now`, and a word CEL
// reserves.
func reservedForEnums(name string) bool {
	return v1.IsDeclarationRoot(name) || name == v1.NowIdentifier || IsCELReservedIdentifier(name)
}

// boundNamesByStep are the bare names an author has bound where each step's
// expressions are written: the step's own `vars:`, and the iterator or state of
// every enclosing `for_each` and `loop:`, which scope to the body. Keyed by step
// id; a workflow-level position (id "") binds none. Two steps sharing an id
// (siblings in different loops) share the union, which can only withhold a
// lowering, never apply one over a binding.
func boundNamesByStep(steps []*v1.Node) map[string]map[string]bool {
	out := map[string]map[string]bool{}

	var visit func(nodes []*v1.Node, enclosing map[string]bool)
	visit = func(nodes []*v1.Node, enclosing map[string]bool) {
		for _, node := range nodes {
			own := maps.Clone(enclosing)
			if own == nil {
				own = map[string]bool{}
			}
			for name := range node.GetVars() {
				own[name] = true
			}

			// The iterator and the state belong to the body, and are taken for the
			// step's own positions too (`items:` is evaluated outside the loop, so
			// this can only withhold a lowering there).
			inner := maps.Clone(own)
			if each := node.GetForEach(); each != nil {
				own[v1.IteratorName(each)], inner[v1.IteratorName(each)] = true, true
			}
			if loop := node.GetLoop(); loop != nil && loop.GetState() != "" {
				own[loop.GetState()], inner[loop.GetState()] = true, true
			}

			if prior, ok := out[node.GetId()]; ok {
				maps.Copy(prior, own)
			} else {
				out[node.GetId()] = own
			}

			for _, children := range childSteps(node) {
				visit(children, inner)
			}
		}
	}
	visit(steps, nil)

	return out
}

// childSteps are the lists of steps nested directly under node.
func childSteps(node *v1.Node) [][]*v1.Node {
	var out [][]*v1.Node
	out = append(out, node.GetForEach().GetBody(), node.GetLoop().GetBody())
	for _, branch := range node.GetParallel().GetBranches() {
		out = append(out, branch.GetSteps())
	}
	for _, c := range node.GetSwitch().GetCases() {
		out = append(out, c.GetSteps())
	}
	out = append(out, node.GetSwitch().GetDefault().GetSteps())

	return out
}

// recordLowered writes down the names an expression was written with, as macro
// calls over the nodes that now hold their numbers.
func recordLowered(parsed *expr.ParsedExpr, lowered []loweredName) {
	if len(lowered) == 0 {
		return
	}

	if parsed.SourceInfo == nil {
		parsed.SourceInfo = &expr.SourceInfo{}
	}
	info := parsed.SourceInfo
	if info.MacroCalls == nil {
		info.MacroCalls = map[int64]*expr.Expr{}
	}

	// The recorded identifier needs an id of its own: the unparser looks every
	// node it visits up in the macro calls, and a record that shared its node's id
	// would be looked up again as its own expansion.
	next := int64(0)
	note := func(e *expr.Expr) { next = max(next, e.GetId()) }
	walkExprTree(parsed.GetExpr(), note)
	for id, call := range info.MacroCalls {
		next = max(next, id)
		walkExprTree(call, note)
	}

	for _, l := range lowered {
		next++
		info.MacroCalls[l.id] = &expr.Expr{
			Id:       next,
			ExprKind: &expr.Expr_IdentExpr{IdentExpr: &expr.Expr_Ident{Name: l.name}},
		}
	}
}

// lowerEnumIdents rewrites e in place, noting what it replaced. A comprehension's
// own variables shadow a value of the same name inside it.
func lowerEnumIdents(e *expr.Expr, enums *v1.OutputEnums, shadowed map[string]bool, into *[]loweredName) {
	if e == nil {
		return
	}

	switch kind := e.GetExprKind().(type) {
	case *expr.Expr_IdentExpr:
		name := kind.IdentExpr.GetName()
		if shadowed[name] || reservedForEnums(name) {
			return
		}
		if value, ok := enums.Value(name); ok {
			*into = append(*into, loweredName{id: e.GetId(), name: name})
			e.ExprKind = &expr.Expr_ConstExpr{ConstExpr: &expr.Constant{
				ConstantKind: &expr.Constant_Int64Value{Int64Value: value.Number},
			}}
		}
	case *expr.Expr_SelectExpr:
		lowerEnumIdents(kind.SelectExpr.GetOperand(), enums, shadowed, into)
	case *expr.Expr_CallExpr:
		lowerEnumIdents(kind.CallExpr.GetTarget(), enums, shadowed, into)
		for _, arg := range kind.CallExpr.GetArgs() {
			lowerEnumIdents(arg, enums, shadowed, into)
		}
	case *expr.Expr_ListExpr:
		for _, element := range kind.ListExpr.GetElements() {
			lowerEnumIdents(element, enums, shadowed, into)
		}
	case *expr.Expr_StructExpr:
		for _, entry := range kind.StructExpr.GetEntries() {
			lowerEnumIdents(entry.GetMapKey(), enums, shadowed, into)
			lowerEnumIdents(entry.GetValue(), enums, shadowed, into)
		}
	case *expr.Expr_ComprehensionExpr:
		c := kind.ComprehensionExpr
		lowerEnumIdents(c.GetIterRange(), enums, shadowed, into)
		lowerEnumIdents(c.GetAccuInit(), enums, shadowed, into)

		inner := make(map[string]bool, len(shadowed)+3)
		for name := range shadowed {
			inner[name] = true
		}
		inner[c.GetIterVar()], inner[c.GetIterVar2()], inner[c.GetAccuVar()] = true, true, true

		lowerEnumIdents(c.GetLoopCondition(), enums, inner, into)
		lowerEnumIdents(c.GetLoopStep(), enums, inner, into)
		lowerEnumIdents(c.GetResult(), enums, inner, into)
	}
}

// walkExprTree calls visit on e and everything under it.
func walkExprTree(e *expr.Expr, visit func(*expr.Expr)) {
	if e == nil {
		return
	}
	visit(e)

	switch kind := e.GetExprKind().(type) {
	case *expr.Expr_SelectExpr:
		walkExprTree(kind.SelectExpr.GetOperand(), visit)
	case *expr.Expr_CallExpr:
		walkExprTree(kind.CallExpr.GetTarget(), visit)
		for _, arg := range kind.CallExpr.GetArgs() {
			walkExprTree(arg, visit)
		}
	case *expr.Expr_ListExpr:
		for _, element := range kind.ListExpr.GetElements() {
			walkExprTree(element, visit)
		}
	case *expr.Expr_StructExpr:
		for _, entry := range kind.StructExpr.GetEntries() {
			walkExprTree(entry.GetMapKey(), visit)
			walkExprTree(entry.GetValue(), visit)
		}
	case *expr.Expr_ComprehensionExpr:
		c := kind.ComprehensionExpr
		walkExprTree(c.GetIterRange(), visit)
		walkExprTree(c.GetAccuInit(), visit)
		walkExprTree(c.GetLoopCondition(), visit)
		walkExprTree(c.GetLoopStep(), visit)
		walkExprTree(c.GetResult(), visit)
	}
}

// enumOrigins answers which task's outputs an operand reads, so a comparison is
// judged only against an enum that task's answer holds.
type enumOrigins struct {
	steps     map[string]*v1.Node
	iterators map[string]*expr.Expr
	perStep   map[string]*v1.OutputEnums
}

func newEnumOrigins(wf *v1.Workflow) *enumOrigins {
	o := &enumOrigins{
		steps:     map[string]*v1.Node{},
		iterators: map[string]*expr.Expr{},
		perStep:   map[string]*v1.OutputEnums{},
	}

	seen := map[string]int{}
	ambiguous := map[string]bool{}
	v1.WalkNodes(wf.GetSteps(), v1.Walk{Node: func(node *v1.Node) {
		seen[node.GetId()]++
		o.steps[node.GetId()] = node

		if each := node.GetForEach(); each != nil {
			name := v1.IteratorName(each)
			if _, again := o.iterators[name]; again || each.GetItems().GetExpr() == nil {
				ambiguous[name] = true
			}
			o.iterators[name] = each.GetItems().GetExpr().GetExpr()
		}
	}})
	for id, count := range seen {
		if count > 1 {
			delete(o.steps, id)
		}
	}
	for name := range ambiguous {
		delete(o.iterators, name)
	}

	return o
}

// rangesIn are the lists the comprehensions in e iterate, by variable name; a
// name two comprehensions bind to different lists is dropped.
func rangesIn(e *expr.Expr) map[string]*expr.Expr {
	ranges := map[string]*expr.Expr{}
	dropped := map[string]bool{}
	walkExprTree(e, func(node *expr.Expr) {
		c := node.GetComprehensionExpr()
		if c == nil || c.GetIterVar() == "" {
			return
		}
		if _, again := ranges[c.GetIterVar()]; again {
			dropped[c.GetIterVar()] = true
		}
		ranges[c.GetIterVar()] = c.GetIterRange()
	})
	for name := range dropped {
		delete(ranges, name)
	}

	return ranges
}

// maxOriginDepth bounds how many `value:` steps are followed back to a task.
const maxOriginDepth = 4

// of is the enums of the task whose output e reads, false when e is not
// provably a read of one: rooted at `steps.<id>` for a task step, through a
// `value:` step that is itself such a read, through indexing, a comprehension's
// list, or the iterator of a `for_each` over one. Anything rooted elsewhere (an
// input, a var, a declared type) is not.
func (o *enumOrigins) of(e *expr.Expr, ranges map[string]*expr.Expr, depth int) (*v1.OutputEnums, bool) {
	if depth > maxOriginDepth {
		return nil, false
	}

	switch kind := e.GetExprKind().(type) {
	case *expr.Expr_SelectExpr:
		if operand := kind.SelectExpr.GetOperand().GetIdentExpr(); operand.GetName() == v1.StepsRoot {
			return o.step(kind.SelectExpr.GetField(), depth)
		}

		return o.of(kind.SelectExpr.GetOperand(), ranges, depth)
	case *expr.Expr_IdentExpr:
		if list, ok := ranges[kind.IdentExpr.GetName()]; ok {
			return o.of(list, ranges, depth+1)
		}
		if list, ok := o.iterators[kind.IdentExpr.GetName()]; ok {
			return o.of(list, ranges, depth+1)
		}
	case *expr.Expr_CallExpr:
		if kind.CallExpr.GetFunction() == "_[_]" && len(kind.CallExpr.GetArgs()) == 2 {
			return o.of(kind.CallExpr.GetArgs()[0], ranges, depth)
		}
	case *expr.Expr_ComprehensionExpr:
		return o.of(kind.ComprehensionExpr.GetIterRange(), ranges, depth)
	}

	return nil, false
}

func (o *enumOrigins) step(id string, depth int) (*v1.OutputEnums, bool) {
	node, ok := o.steps[id]
	if !ok {
		return nil, false
	}

	switch node.GetKind().(type) {
	case *v1.Node_Task:
		enums, cached := o.perStep[id]
		if !cached {
			enums = v1.OutputEnumsOf(&v1.Workflow{Steps: []*v1.Node{node}}, nil)
			o.perStep[id] = enums
		}

		return enums, !enums.Empty()
	case *v1.Node_Value:
		root := node.GetValue().GetExpr().GetExpr()

		return o.of(root, rangesIn(root), depth+1)
	}

	return nil, false
}

// enumComparisonErrors reports a comparison of an enum-valued output field with
// something that can never be one of its values: a string, which is what a name
// looks like and what the run never holds, or a number the enum does not define.
//
// Judged by the field's name against the enums of the one task the operand
// provably reads ([enumOrigins]), because the checker does not type what is
// inside a task's answer (`answers.filter(...)[0]` is `dyn`). An operand rooted
// at an input, a var or a declared type is never judged, whatever it is called.
// Within the task, the field is refused only when every output field of that name
// is the same enum.
func enumComparisonErrors(wf *v1.Workflow) Diagnostics {
	if v1.OutputEnumsOf(wf, nil).Empty() {
		return nil
	}
	origins := newEnumOrigins(wf)

	var ds Diagnostics
	v1.WalkWorkflow(wf, v1.Walk{Value: func(site v1.ValueSite) {
		parsed := site.Value.GetExpr()
		if parsed == nil {
			return
		}
		ranges := rangesIn(parsed.GetExpr())

		walkExprTree(parsed.GetExpr(), func(node *expr.Expr) {
			call := node.GetCallExpr()
			if call == nil || len(call.GetArgs()) != 2 || (call.GetFunction() != "_==_" && call.GetFunction() != "_!=_") {
				return
			}

			for i := range 2 {
				selected, literal := call.GetArgs()[i].GetSelectExpr(), call.GetArgs()[1-i].GetConstExpr()
				if selected == nil || literal == nil {
					continue
				}
				enums, ok := origins.of(selected.GetOperand(), ranges, 0)
				if !ok {
					continue
				}
				enum, ok := enums.FieldEnum(selected.GetField())
				if !ok {
					continue
				}
				if message, wrong := enumLiteralMessage(selected.GetField(), enum, literal); wrong {
					ds = append(ds, Diagnostic{
						Step:    site.Step,
						Field:   site.Field(),
						Message: message,
						Code:    v1.DiagnosticCodeTypeMismatch,
					})
				}
			}
		})
	}})

	return ds
}

// enumLiteralMessage says why literal cannot be a value of enum, and what to write.
func enumLiteralMessage(field string, enum protoreflect.EnumDescriptor, literal *expr.Constant) (string, bool) {
	values := enum.Values()
	names := make([]string, 0, values.Len())
	for i := range values.Len() {
		names = append(names, string(values.Get(i).Name()))
	}

	switch kind := literal.GetConstantKind().(type) {
	case *expr.Constant_StringValue:
		message := fmt.Sprintf(
			"`%s` is the enum %s, a number at run time, and %q is a string, so this is never true; "+
				"compare it with a value name, one of %s",
			field, enum.Name(), kind.StringValue, strings.Join(names, ", "))
		if suggestion, ok := nearestEnumName(kind.StringValue, names); ok {
			message += fmt.Sprintf("; did you mean %s?", suggestion)
		}

		return message, true

	case *expr.Constant_Int64Value:
		return enumNumberMessage(field, enum, names, strconv.FormatInt(kind.Int64Value, 10), kind.Int64Value, true)

	case *expr.Constant_Uint64Value:
		// CEL compares a uint with an int by value, so `2u` is the number 2.
		return enumNumberMessage(field, enum, names, strconv.FormatUint(kind.Uint64Value, 10)+"u",
			int64(min(kind.Uint64Value, math.MaxInt64)), kind.Uint64Value <= math.MaxInt64)

	case *expr.Constant_DoubleValue:
		// And a double by value too: `2.0` is the number 2, and `2.5` is none.
		integral := kind.DoubleValue == math.Trunc(kind.DoubleValue) &&
			kind.DoubleValue >= math.MinInt32 && kind.DoubleValue <= math.MaxInt32

		return enumNumberMessage(field, enum, names, strconv.FormatFloat(kind.DoubleValue, 'g', -1, 64), int64(kind.DoubleValue), integral)
	}

	return "", false
}

// enumNumberMessage refuses a number the enum does not define. exact is false
// for a literal that is no integer in range, which no value can equal.
func enumNumberMessage(field string, enum protoreflect.EnumDescriptor, names []string, written string, number int64, exact bool) (string, bool) {
	if exact && number >= math.MinInt32 && number <= math.MaxInt32 &&
		enum.Values().ByNumber(protoreflect.EnumNumber(number)) != nil {
		return "", false
	}

	return fmt.Sprintf(
		"%s is not a value of the enum %s that `%s` holds; its values are %s",
		written, enum.Name(), field, strings.Join(names, ", ")), true
}

// nearestEnumName is the value name an author's string most likely meant: the
// one that ends in what they typed (`self_reported` for CALIBRATION_SELF_REPORTED,
// a value name without its enum's prefix), when there is exactly one, and
// otherwise the nearest by [nearest.Name]'s rule.
func nearestEnumName(typed string, names []string) (string, bool) {
	upper := strings.ToUpper(typed)

	var suffixed []string
	for _, name := range names {
		if name == upper || strings.HasSuffix(name, "_"+upper) {
			suffixed = append(suffixed, name)
		}
	}
	if len(suffixed) == 1 {
		return suffixed[0], true
	}

	return nearest.Name(upper, names)
}

// unknownEnumHint adds to an unknown-name message what the file's enum names
// offer: the near miss, or the fact that a name two enums define differently
// has no unambiguous spelling.
func unknownEnumHint(wf *v1.Workflow, name string) string {
	enums := v1.OutputEnumsOf(wf, nil)
	if enums.Ambiguous(name) {
		return fmt.Sprintf("; `%s` is a value of more than one enum among this file's tasks, so no spelling of it is unambiguous", name)
	}
	if suggestion, ok := nearest.Name(name, enums.Names()); ok {
		return fmt.Sprintf("; did you mean the enum value `%s`?", suggestion)
	}

	return ""
}
