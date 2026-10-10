package flowfile

import (
	"fmt"
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
// The name is resolved here, when the file compiles, and replaced by the integer
// literal before the specification exists. Nothing at run time knows a name:
// both drivers evaluate the same `== 2` a hand-written file would, so they agree
// by construction and a worker needs no descriptor to run the workflow. The
// price is that a specification decompiled from the compiled form shows the
// number; the Flowfile keeps the name.
//
// A name an author bound (a step var, a loop's iterator or state, a function's
// parameter, a macro's variable) is theirs and is left alone.

// lowerEnumNames replaces each enum value name in wf's expressions with its number.
func lowerEnumNames(wf *v1.Workflow) {
	enums := v1.OutputEnumsOf(wf, nil)
	if len(enums.Names()) == 0 {
		return
	}

	bound := map[string]bool{v1.NowIdentifier: true}
	for _, declared := range wf.GetDeclaredFunctions() {
		for _, parameter := range declared.GetParameters() {
			bound[parameter.GetName()] = true
		}
	}
	v1.WalkNodes(wf.GetSteps(), v1.Walk{Node: func(node *v1.Node) {
		for name := range node.GetVars() {
			bound[name] = true
		}
		if each := node.GetForEach(); each != nil {
			bound[v1.IteratorName(each)] = true
		}
		if loop := node.GetLoop(); loop != nil && loop.GetState() != "" {
			bound[loop.GetState()] = true
		}
	}})

	v1.WalkWorkflow(wf, v1.Walk{Value: func(site v1.ValueSite) {
		parsed := site.Value.GetExpr()
		if parsed == nil {
			return
		}

		lowerEnumIdents(parsed.GetExpr(), enums, bound)

		// A macro call is the form an expression is written back in, and the form
		// a declared function's call is checked as: it holds the author's names too.
		macroVariables := map[string]bool{}
		collectMacroVariables(parsed.GetExpr(), macroVariables)
		for name := range bound {
			macroVariables[name] = true
		}
		for _, call := range parsed.GetSourceInfo().GetMacroCalls() {
			lowerEnumIdents(call, enums, macroVariables)
		}
	}})
}

// collectMacroVariables records the variables the comprehensions in e bind.
func collectMacroVariables(e *expr.Expr, into map[string]bool) {
	walkExprTree(e, func(node *expr.Expr) {
		if c := node.GetComprehensionExpr(); c != nil {
			into[c.GetIterVar()] = true
			into[c.GetIterVar2()] = true
			into[c.GetAccuVar()] = true
		}
	})
}

// lowerEnumIdents rewrites e in place. A comprehension's own variables shadow a
// value of the same name inside it.
func lowerEnumIdents(e *expr.Expr, enums *v1.OutputEnums, shadowed map[string]bool) {
	if e == nil {
		return
	}

	switch kind := e.GetExprKind().(type) {
	case *expr.Expr_IdentExpr:
		name := kind.IdentExpr.GetName()
		if shadowed[name] {
			return
		}
		if value, ok := enums.Value(name); ok {
			e.ExprKind = &expr.Expr_ConstExpr{ConstExpr: &expr.Constant{
				ConstantKind: &expr.Constant_Int64Value{Int64Value: value.Number},
			}}
		}
	case *expr.Expr_SelectExpr:
		lowerEnumIdents(kind.SelectExpr.GetOperand(), enums, shadowed)
	case *expr.Expr_CallExpr:
		lowerEnumIdents(kind.CallExpr.GetTarget(), enums, shadowed)
		for _, arg := range kind.CallExpr.GetArgs() {
			lowerEnumIdents(arg, enums, shadowed)
		}
	case *expr.Expr_ListExpr:
		for _, element := range kind.ListExpr.GetElements() {
			lowerEnumIdents(element, enums, shadowed)
		}
	case *expr.Expr_StructExpr:
		for _, entry := range kind.StructExpr.GetEntries() {
			lowerEnumIdents(entry.GetMapKey(), enums, shadowed)
			lowerEnumIdents(entry.GetValue(), enums, shadowed)
		}
	case *expr.Expr_ComprehensionExpr:
		c := kind.ComprehensionExpr
		lowerEnumIdents(c.GetIterRange(), enums, shadowed)
		lowerEnumIdents(c.GetAccuInit(), enums, shadowed)

		inner := make(map[string]bool, len(shadowed)+3)
		for name := range shadowed {
			inner[name] = true
		}
		inner[c.GetIterVar()], inner[c.GetIterVar2()], inner[c.GetAccuVar()] = true, true, true

		lowerEnumIdents(c.GetLoopCondition(), enums, inner)
		lowerEnumIdents(c.GetLoopStep(), enums, inner)
		lowerEnumIdents(c.GetResult(), enums, inner)
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

// enumComparisonErrors reports a comparison of an enum-valued output field with
// something that can never be one of its values: a string, which is what a name
// looks like and what the run never holds, or a number the enum does not define.
//
// Judged by the field's name, because the checker does not type what is inside a
// task's answer (`answers.filter(...)[0]` is `dyn`): `x.calibration == "a"` is
// refused when every output field called `calibration` among the file's own tasks
// is the same enum, and left alone when any is not, so a field some other task
// types differently is never misjudged.
func enumComparisonErrors(wf *v1.Workflow) Diagnostics {
	enums := v1.OutputEnumsOf(wf, nil)
	if enums.Empty() {
		return nil
	}

	var ds Diagnostics
	v1.WalkWorkflow(wf, v1.Walk{Value: func(site v1.ValueSite) {
		parsed := site.Value.GetExpr()
		if parsed == nil {
			return
		}

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
		if kind.Int64Value >= math.MinInt32 && kind.Int64Value <= math.MaxInt32 &&
			values.ByNumber(protoreflect.EnumNumber(kind.Int64Value)) != nil {
			return "", false
		}

		return fmt.Sprintf(
			"%s is not a value of the enum %s that `%s` holds; its values are %s",
			strconv.FormatInt(kind.Int64Value, 10), enum.Name(), field, strings.Join(names, ", ")), true
	}

	return "", false
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
