package flowfile

import (
	"fmt"

	expr "google.golang.org/genproto/googleapis/api/expr/v1alpha1"
	"google.golang.org/protobuf/reflect/protoreflect"

	v1 "github.com/picatz/flowstate/pkg/flowstate/v1"
	"github.com/picatz/flowstate/pkg/flowstate/v1/nearest"
)

// What a message inside a task's answer holds, as an expression reads it.
//
// A plugin's answer stores a nested message as a map of its field names, and the
// task's output descriptor names those fields. The type table cannot say so
// (`answers` is `list(dyn)`, and so is `answers.filter(...)[0]`), which let
// `steps.decide.answers[0].chioce` validate and fail hours into a durable run. The
// descriptor is the one source of truth already used for typed outputs and enum
// names, so a read of a field the message does not have is refused here with the
// fields it does.
//
// Judged only when the operand is provably such a message, from a chain written in
// the expression itself: `steps.<id>` of a task step whose descriptor is known,
// field selection into a message-typed field, indexing a repeated one, and
// `filter(...)`, which keeps the elements. A comprehension's own variable is the
// element of its range while the range is such a list. Never judged: a bare name
// that is not a comprehension's own, anything `map` built, a map-valued or
// dynamic-valued field, a `value:` step, and a task whose outputs are shaped by
// the file. A missed diagnostic costs less than refusing a valid file.

// outputElement is what an expression provably holds from a task's descriptor.
type outputElement struct {
	// message is the message type: the answer itself for the root, or what a
	// message-typed field holds.
	message protoreflect.MessageDescriptor

	// list marks a repeated field of message.
	list bool

	// root marks the task's whole answer (`steps.<id>`), whose unknown names the
	// reference checks already report.
	root bool
}

// outputElementErrors reports a read of a field a message inside a task's answer
// does not have.
func outputElementErrors(wf *v1.Workflow) Diagnostics {
	origins := newEnumOrigins(wf)

	var ds Diagnostics
	v1.WalkWorkflow(wf, v1.Walk{Value: func(site v1.ValueSite) {
		parsed := site.Value.GetExpr()
		if parsed == nil {
			return
		}
		origins.checkElements(parsed.GetExpr(), nil, func(message string) {
			if len(ds) < maxFieldErrors {
				ds = append(ds, Diagnostic{
					Step: site.Step, Field: site.Field(), Message: message,
					Code: v1.DiagnosticCodeUnresolvedReference,
				})
			}
		})
	}})

	return ds
}

// checkElements walks e with the comprehension variables in scope known to be
// elements, and reports each select on a message that lacks the field.
func (o *enumOrigins) checkElements(e *expr.Expr, scope map[string]outputElement, report func(string)) {
	if e == nil {
		return
	}

	switch kind := e.GetExprKind().(type) {
	case *expr.Expr_SelectExpr:
		sel := kind.SelectExpr
		if element, ok := o.element(sel.GetOperand(), scope, 0); ok && !element.root && !element.list {
			if message, missing := missingFieldMessage(element.message, sel.GetField()); missing {
				report(message)
			}
		}
		o.checkElements(sel.GetOperand(), scope, report)
	case *expr.Expr_CallExpr:
		if operand, name, ok := optionalSelect(kind.CallExpr); ok {
			if element, found := o.element(operand, scope, 0); found && !element.root && !element.list {
				if message, missing := missingFieldMessage(element.message, name); missing {
					report(message)
				}
			}
		}
		o.checkElements(kind.CallExpr.GetTarget(), scope, report)
		for _, arg := range kind.CallExpr.GetArgs() {
			o.checkElements(arg, scope, report)
		}
	case *expr.Expr_ListExpr:
		for _, element := range kind.ListExpr.GetElements() {
			o.checkElements(element, scope, report)
		}
	case *expr.Expr_StructExpr:
		for _, entry := range kind.StructExpr.GetEntries() {
			o.checkElements(entry.GetMapKey(), scope, report)
			o.checkElements(entry.GetValue(), scope, report)
		}
	case *expr.Expr_ComprehensionExpr:
		c := kind.ComprehensionExpr
		o.checkElements(c.GetIterRange(), scope, report)
		o.checkElements(c.GetAccuInit(), scope, report)

		// The comprehension's names shadow whatever the outer scope held; the
		// iteration variable is an element only of a single-variable macro over a
		// list of messages.
		inner := make(map[string]outputElement, len(scope)+1)
		for name, element := range scope {
			inner[name] = element
		}
		delete(inner, c.GetIterVar())
		delete(inner, c.GetIterVar2())
		delete(inner, c.GetAccuVar())
		if c.GetIterVar2() == "" {
			if element, ok := o.element(c.GetIterRange(), scope, 0); ok && element.list {
				inner[c.GetIterVar()] = outputElement{message: element.message}
			}
		}

		o.checkElements(c.GetLoopCondition(), inner, report)
		o.checkElements(c.GetLoopStep(), inner, report)
		o.checkElements(c.GetResult(), inner, report)
	}
}

// element is what e provably holds, false when it is not provably a message or a
// list of them from a task's descriptor.
func (o *enumOrigins) element(e *expr.Expr, scope map[string]outputElement, depth int) (outputElement, bool) {
	if depth > maxOriginDepth*8 {
		return outputElement{}, false
	}

	switch kind := e.GetExprKind().(type) {
	case *expr.Expr_IdentExpr:
		element, ok := scope[kind.IdentExpr.GetName()]

		return element, ok
	case *expr.Expr_SelectExpr:
		sel := kind.SelectExpr
		if sel.GetTestOnly() {
			return outputElement{}, false
		}
		if sel.GetOperand().GetIdentExpr().GetName() == v1.StepsRoot {
			return o.taskAnswer(sel.GetField())
		}

		operand, ok := o.element(sel.GetOperand(), scope, depth+1)
		if !ok || operand.list {
			return outputElement{}, false
		}

		return fieldElement(operand, sel.GetField())
	case *expr.Expr_CallExpr:
		call := kind.CallExpr
		if operand, name, ok := optionalSelect(call); ok {
			// `x.?field` continues the chain like `x.field`.
			parent, found := o.element(operand, scope, depth+1)
			if !found || parent.list {
				return outputElement{}, false
			}

			return fieldElement(parent, name)
		}
		if call.GetFunction() != "_[_]" || len(call.GetArgs()) != 2 {
			return outputElement{}, false
		}
		list, ok := o.element(call.GetArgs()[0], scope, depth+1)
		if !ok || !list.list {
			return outputElement{}, false
		}

		return outputElement{message: list.message}, true
	case *expr.Expr_ComprehensionExpr:
		if !keepsElements(kind.ComprehensionExpr) {
			return outputElement{}, false
		}
		list, ok := o.element(kind.ComprehensionExpr.GetIterRange(), scope, depth+1)

		return list, ok && list.list
	}

	return outputElement{}, false
}

// fieldElement is the message a message-typed field of operand holds.
func fieldElement(operand outputElement, name string) (outputElement, bool) {
	field := operand.message.Fields().ByName(protoreflect.Name(name))
	if field == nil || field.IsMap() || !messageEncodedAsMap(field) {
		return outputElement{}, false
	}

	return outputElement{message: field.Message(), list: field.IsList()}, true
}

// optionalSelect is the operand and field name of `operand.?name`, which CEL
// represents as the `_?._` call with the name as a string literal.
func optionalSelect(call *expr.Expr_Call) (*expr.Expr, string, bool) {
	if call.GetFunction() != "_?._" || len(call.GetArgs()) != 2 {
		return nil, "", false
	}
	name, ok := call.GetArgs()[1].GetConstExpr().GetConstantKind().(*expr.Constant_StringValue)
	if !ok {
		return nil, "", false
	}

	return call.GetArgs()[0], name.StringValue, true
}

// messageEncodedAsMap reports that a message-typed field is stored as a map of its
// field names: not a well-known type or a dynamic value, which store something
// else.
func messageEncodedAsMap(field protoreflect.FieldDescriptor) bool {
	if field.Kind() != protoreflect.MessageKind {
		return false
	}
	message := field.Message()

	return !v1.IsDynamicValueMessage(message.FullName()) && message.ParentFile().Package() != "google.protobuf"
}

// taskAnswer is the answer of the task step with this id, when its descriptor
// names it in full: not a step the file repeats the id of, not a step whose
// outputs the file shapes, and not a task registered without a descriptor.
func (o *enumOrigins) taskAnswer(id string) (outputElement, bool) {
	node, ok := o.steps[id]
	if !ok || node.GetTask() == nil {
		return outputElement{}, false
	}
	def, found := v1.LookupTask(node.GetTask().GetName())
	if !found || def.Outputs == nil {
		return outputElement{}, false
	}
	if _, shaped := node.GetTask().GetInputs()[v1.ShapingInput]; shaped && def.ShapesOutputs {
		return outputElement{}, false
	}

	return outputElement{message: def.Outputs, root: true}, true
}

// missingFieldMessage says what a message has instead of field.
func missingFieldMessage(message protoreflect.MessageDescriptor, field string) (string, bool) {
	fields := message.Fields()
	if fields.ByName(protoreflect.Name(field)) != nil {
		return "", false
	}

	known := make([]string, 0, fields.Len())
	for i := range fields.Len() {
		known = append(known, string(fields.Get(i).Name()))
	}

	text := fmt.Sprintf("`%s` is not a field of %s, which this value holds; its fields are %s",
		echo(field), message.Name(), quoteAll(known[:min(len(known), maxEchoedFieldList)]))
	if len(known) > maxEchoedFieldList {
		text += fmt.Sprintf(", and %d more", len(known)-maxEchoedFieldList)
	}
	if len(field) <= maxEchoedName {
		if suggestion, ok := nearest.Name(field, known); ok {
			text += fmt.Sprintf("; did you mean `%s`?", suggestion)
		}
	}

	return text, true
}
