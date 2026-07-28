package flowfile

import (
	"fmt"
	"strings"

	v1 "github.com/picatz/flowstate/pkg/flowstate/v1"
	expr "google.golang.org/genproto/googleapis/api/expr/v1alpha1"
	"google.golang.org/protobuf/reflect/protoreflect"
)

// Inputs are checked against the schema the task declares, which is the only way
// a mistake in one can be reported before the step runs.
//
// The descriptor is the source of truth. A task's inputs are a generated message
// — `Task.Echo.Inputs`, `Task.HTTP.Inputs` — and [flowstatev1.TaskDef] carries its
// descriptor, so what a task accepts, which of those are required, and what type
// each holds are all read from the schema rather than listed here. Adding a task
// or a field to one therefore needs no change in this file, which is the property
// that keeps the checks from drifting from what the engine will actually accept.
//
// Two things bound what can be checked at compile time, and both are the
// difference between a useful diagnostic and a false one:
//
//   - Only a literal has a knowable type. An expression's type depends on step
//     outputs, which do not exist yet and are not themselves typed, so an
//     expression input is not type-checked at all.
//   - Some tasks accept input names their schema does not declare. The cel task
//     binds every unrecognized input as a variable, so checking names against its
//     descriptor would report each one.
//
// A missing descriptor, an unknown task, or anything else this cannot decide
// produces no diagnostic. That matters more here than coverage does: a validator
// that reports a mistake the author did not make teaches them to stop reading it.

// validateTaskLibraries reports a CEL extension library this build does not have.
//
// A misspelled library used to compile cleanly and fail at run time, which is the
// "a misspelled key must be reported" rule with the failure moved as late as it can
// go: the workflow starts, the step is scheduled, an activity runs, and only then
// does anyone learn that `stirngs` is not a library. The names are a closed set the
// registry knows, so there is no reason for that to be a run-time answer.
//
// It also closes the front door on a resource bound. The evaluator caches an
// environment per library set, and until recently cached the failures too — so
// every distinct unknown name became a permanent entry in a process-wide map. That
// is fixed where it belongs, in the evaluator; this is the half that means an
// author never arrives there by accident.
func validateTaskLibraries(stepID string, task *v1.Task) Diagnostics {
	var ds Diagnostics

	libs, present := task.GetInputs()["libs"]
	if !present {
		return ds
	}

	// Only a literal list can be checked. An expression producing the list is
	// resolved at run time against a scope this validator cannot see, and
	// reporting it would be a false diagnostic — which this package holds to be
	// worse than a missing one.
	list := libs.GetLiteral().GetListValue()
	if list == nil {
		return ds
	}

	known := make(map[string]bool)
	for _, name := range v1.ExtensionLibraries() {
		known[name] = true
	}

	for _, value := range list.GetValues() {
		name := value.GetStringValue()
		if name == "" || known[strings.ToLower(strings.TrimSpace(name))] {
			continue
		}

		ds = append(ds, Diagnostic{
			Step:  stepID,
			Field: "libs",
			// The element, not just the field: a surface with positions can then
			// underline the name that is wrong rather than the whole list.
			Value: name,
			Message: fmt.Sprintf("unknown CEL extension library %q; available libraries are %s",
				name, strings.Join(v1.ExtensionLibraries(), ", ")),
		})
	}

	return ds
}

// validateTaskInputs reports what the task's own schema says is wrong with its
// inputs: a name it does not declare, a required one left out, and a literal
// whose type the field cannot hold.
func validateTaskInputs(stepID string, task *v1.Task) Diagnostics {
	def, known := v1.LookupTask(task.GetName())
	if !known || def.Inputs == nil {
		// An unknown task is reported on its own, and a task whose shape is not
		// expressed as a message — a plugin declaring no input descriptor — has
		// nothing to check against.
		return nil
	}

	var ds Diagnostics
	fields := def.Inputs.Fields()

	// Inputs the task evaluates itself are passed through untouched, so their
	// shape is the task's business rather than the schema's.
	_, deferred := v1.ResolvableInputs(task.GetName(), task.GetInputs())

	// misspelled records what a typo was probably meant to be, so that one
	// mistake is not also reported as the required input it left unset. Writing
	// `mesage: hi` is one problem, not two.
	misspelled := make(map[string]bool)

	if !acceptsUndeclaredInputs(def) {
		declared := fieldNames(def.Inputs)
		for _, name := range sortedInputNames(task.GetInputs()) {
			if findField(def.Inputs, name) != nil {
				continue
			}

			// The diagnostic already names the step and the input, so the
			// message says what is wrong with it rather than repeating both.
			message := fmt.Sprintf("task %q has no such input", def.Name)
			if suggestion, ok := nearest(name, declared); ok {
				misspelled[suggestion] = true
				message += fmt.Sprintf("; did you mean %q?", suggestion)
			} else if len(declared) > 0 {
				message += fmt.Sprintf("; it accepts %s", strings.Join(declared, ", "))
			}
			ds = append(ds, Diagnostic{Step: stepID, Field: name, Message: message})
		}
	}

	for i := range fields.Len() {
		field := fields.Get(i)
		name := string(field.Name())

		if !requiredField(field) || misspelled[name] {
			continue
		}
		if _, present := task.GetInputs()[name]; present {
			continue
		}
		// The field is named in the message rather than in Field, because there is
		// nothing in the source to point at — the input was never written. Its type
		// is named too, since the next thing the author needs is what to write.
		ds = append(ds, Diagnostic{
			Step: stepID,
			Message: fmt.Sprintf("task %q requires input %q (%s)",
				def.Name, name, inputTypePhrase(field)),
		})
	}

	for _, name := range sortedInputNames(task.GetInputs()) {
		field := findField(def.Inputs, name)
		if field == nil {
			continue
		}
		if _, isDeferred := deferred[name]; isDeferred {
			continue
		}
		literal := task.GetInputs()[name].GetLiteral()
		if literal == nil {
			// An expression or a secret reference. Neither has a type this could
			// check, and guessing at one is how a correct workflow gets reported.
			continue
		}
		if message := literalMismatch(field, literal); message != "" {
			ds = append(ds, Diagnostic{Step: stepID, Field: name, Message: message})
		}
	}

	return ds
}

// literalMismatch reports why a field cannot hold a literal, or empty when it can
// or when this cannot tell.
//
// Every rule here answers "could the engine possibly accept this?", never "is this
// what the author meant?". A number written for a floating-point field is accepted
// because the engine accepts it; only a value the field could never hold is
// reported.
func literalMismatch(field protoreflect.FieldDescriptor, literal *expr.Value) string {
	switch {
	case field.IsMap():
		if literal.GetMapValue() == nil {
			return fmt.Sprintf("expected a mapping, but this is %s", literalKind(literal))
		}
		return ""

	case field.IsList():
		if literal.GetListValue() == nil {
			return fmt.Sprintf("expected a list, but this is %s", literalKind(literal))
		}
		return ""

	case field.Kind() == protoreflect.MessageKind, field.Kind() == protoreflect.GroupKind:
		// A singular message field — a Value, a Duration — accepts shapes this
		// cannot usefully narrow, and the task converts it.
		return ""
	}

	if holds(field.Kind(), literal) {
		return ""
	}
	return fmt.Sprintf("expected %s, but this is %s", kindPhrase(field.Kind()), literalKind(literal))
}

// holds reports whether a scalar field could accept a literal.
//
// It errs toward accepting: a kind combination not listed here is treated as
// acceptable rather than reported, because the cost of being wrong is a false
// diagnostic on a workflow that runs.
func holds(kind protoreflect.Kind, literal *expr.Value) bool {
	switch literal.GetKind().(type) {
	case *expr.Value_StringValue:
		return kind == protoreflect.StringKind || kind == protoreflect.BytesKind
	case *expr.Value_BytesValue:
		return kind == protoreflect.BytesKind || kind == protoreflect.StringKind
	case *expr.Value_BoolValue:
		return kind == protoreflect.BoolKind
	case *expr.Value_Int64Value, *expr.Value_Uint64Value:
		// A whole number is also how a Flowfile writes a floating-point value:
		// `backoff: 2` is a double, and reporting it would be wrong.
		return isNumeric(kind)
	case *expr.Value_DoubleValue:
		return kind == protoreflect.DoubleKind || kind == protoreflect.FloatKind
	case *expr.Value_ListValue, *expr.Value_MapValue:
		// A structure where a scalar belongs. The field is singular and scalar,
		// having reached here, so nothing could accept this.
		return false
	default:
		// Null, an enum, a type value: not worth a rule each, and silence is the
		// safe answer.
		return true
	}
}

// isNumeric reports whether a field kind holds a number.
func isNumeric(kind protoreflect.Kind) bool {
	switch kind {
	case protoreflect.Int32Kind, protoreflect.Int64Kind,
		protoreflect.Sint32Kind, protoreflect.Sint64Kind,
		protoreflect.Sfixed32Kind, protoreflect.Sfixed64Kind,
		protoreflect.Uint32Kind, protoreflect.Uint64Kind,
		protoreflect.Fixed32Kind, protoreflect.Fixed64Kind,
		protoreflect.FloatKind, protoreflect.DoubleKind:
		return true
	default:
		return false
	}
}

// inputTypePhrase names what an input has to be, for a message telling an author
// what to write where they wrote nothing.
func inputTypePhrase(field protoreflect.FieldDescriptor) string {
	switch {
	case field.IsMap():
		return "a mapping"
	case field.IsList():
		return fmt.Sprintf("a list of %s", kindPhrase(field.Kind()))
	case field.Kind() == protoreflect.MessageKind, field.Kind() == protoreflect.GroupKind:
		return "a value"
	default:
		return kindPhrase(field.Kind())
	}
}

// kindPhrase names a field's type the way a Flowfile author would, since the
// protobuf spelling of it is not what they wrote.
func kindPhrase(kind protoreflect.Kind) string {
	switch kind {
	case protoreflect.StringKind:
		return "a string"
	case protoreflect.BytesKind:
		return "a string of bytes"
	case protoreflect.BoolKind:
		return "true or false"
	case protoreflect.FloatKind, protoreflect.DoubleKind:
		return "a number"
	case protoreflect.EnumKind:
		return "one of the declared values"
	default:
		if isNumeric(kind) {
			return "a whole number"
		}
		return "a different type"
	}
}

// acceptsUndeclaredInputs reports whether a task takes input names its schema does
// not declare.
//
// The cel task does: every input it does not recognize becomes a variable its
// expression can reference. That is task behavior the registry does not declare, so
// it is inferred from the shape that makes it possible — a `vars` mapping — which is
// a guess standing in for a [flowstatev1.TaskDef] field that should say so outright.
func acceptsUndeclaredInputs(def v1.TaskDef) bool {
	field := findField(def.Inputs, "vars")
	return field != nil && field.IsMap()
}

// findField returns the field a task declares under the given input name.
func findField(md protoreflect.MessageDescriptor, name string) protoreflect.FieldDescriptor {
	if md == nil {
		return nil
	}
	return md.Fields().ByName(protoreflect.Name(name))
}

// fieldNames returns the input names a task declares, in schema order, which is the
// order a person reading the schema would list them in.
func fieldNames(md protoreflect.MessageDescriptor) []string {
	if md == nil {
		return nil
	}
	fields := md.Fields()
	names := make([]string, 0, fields.Len())
	for i := range fields.Len() {
		names = append(names, string(fields.Get(i).Name()))
	}
	return names
}

// requiredField reports whether the schema says an input must be given.
//
// This is read from the field's protovalidate rules rather than from a list here,
// so that marking a field required in the schema is all it takes.
func requiredField(field protoreflect.FieldDescriptor) bool {
	return v1.RequiredInput(field)
}
