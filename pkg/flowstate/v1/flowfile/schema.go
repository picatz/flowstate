package flowfile

import (
	"errors"
	"fmt"
	"strings"

	v1 "github.com/picatz/flowstate/pkg/flowstate/v1"
	expr "google.golang.org/genproto/googleapis/api/expr/v1alpha1"
	"google.golang.org/protobuf/reflect/protoreflect"
	"google.golang.org/protobuf/types/dynamicpb"
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
//   - Only a literal has a knowable type, and only a literal has a knowable
//     *value*. An expression's depends on step outputs, which do not exist yet and
//     are not themselves typed, so an expression input is neither type-checked nor
//     rule-checked.
//   - An input the task evaluates itself is passed through untouched, so its shape
//     is the task's business rather than the schema's.
//
// A missing descriptor, an unknown task, or anything else this cannot decide
// produces no diagnostic. That matters more here than coverage does: a validator
// that reports a mistake the author did not make teaches them to stop reading it.

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

	// checkable names the inputs whose literal the field can hold, which is the set
	// the schema's own rules are worth running over. Collected on the way rather
	// than derived after, since the loop below already answers the question.
	checkable := make(map[string]bool)

	// misspelled records what a typo was probably meant to be, so that one
	// mistake is not also reported as the required input it left unset. Writing
	// `mesage: hi` is one problem, not two.
	misspelled := make(map[string]bool)

	// Every input a task takes is one its schema declares.
	//
	// This used to be gated: one task bound every unrecognised key as a *variable*
	// its expression could name, so an undeclared input there was legitimate rather
	// than a mistake, and the check had to be skipped for it. That task retired at
	// edition v2026.2, and with it the only shape in which an unknown key meant
	// something. An unknown key is a misspelling again, everywhere.
	declared := fieldNames(def.Inputs)
	for _, name := range sortedInputNames(task.GetInputs()) {
		if findField(def.Inputs, name) != nil || misspelled[name] {
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
		// An input that has to be written as an expression is checked before the
		// deferred skip, and that ordering is the whole point.
		//
		// These two sets overlap almost entirely — an input the task evaluates is
		// usually one that has to be an expression — so a check placed after the
		// skip would never run on the inputs it exists for. That is how `expect:`
		// came to accept a mapping: every other check here declines on a deferred
		// input, correctly, because their shape is the task's business. Whether a
		// value carries a fence is not the task's business, because it is decided
		// by the parser before the task sees anything.
		if v1.MustBeExpression(def.Name, name) {
			if task.GetInputs()[name].GetExpr() == nil {
				ds = append(ds, Diagnostic{
					Step:  stepID,
					Field: name,
					Message: fmt.Sprintf(
						"task %q evaluates input %q as an expression, so it has to be written as one: "+
							"wrap the value in ${...}", def.Name, name),
				})
			}
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

			continue
		}
		// The field can hold the value. Whether the *schema* accepts it is a
		// separate question, and one the author would otherwise meet at run time.
		checkable[name] = true
	}

	return append(ds, violatedRules(stepID, task, def, checkable)...)
}

// violatedRules reports what the schema's own rules say about the inputs written as
// literals — a method that is not a method, a URL that is not a URL, a map with more
// entries than the field allows.
//
// Asked of protovalidate rather than of a table here, for the reason the rest of this
// file reads descriptors: a rule added to the schema is enforced by `flow validate`
// the day it is added, and a rule this package had never heard of is enforced too.
// Rendering the rules by hand — which the language server does, for hover — would be
// a second reading of the schema to keep in step, and the two would drift in the
// direction of the validator being wrong about a working file.
//
// # Only what the author wrote, and only what is knowable
//
// The message this builds holds the literal inputs and nothing else, so it is
// missing every field supplied by an expression. Violations about a field being
// absent are therefore about *this message* rather than about the file, and are
// dropped — a required input genuinely left out is reported above, from the source,
// where there is a position to name.
//
// checkable names the inputs whose literal the field can hold. One that cannot is
// already reported, and running rules over a value the field rejected would answer a
// question nobody asked with a second diagnostic about the same line.
func violatedRules(stepID string, task *v1.Task, def v1.TaskDef, checkable map[string]bool) Diagnostics {
	if len(checkable) == 0 || def.Inputs == nil {
		return nil
	}

	var ds Diagnostics
	for _, name := range sortedInputNames(task.GetInputs()) {
		if !checkable[name] {
			continue
		}
		if violations := violatedRulesFor(stepID, name, task.GetInputs()[name], def); len(violations) > 0 {
			ds = append(ds, violations...)

			continue
		}
		// What the task itself knows, asked last because it is the narrowest of
		// the three questions and the only one that presumes the other two were
		// answered.
		//
		// The order is not a preference. `url: not a uri at all` is refused by the
		// schema's own `uri` rule, and the egress policy — handed a string it
		// cannot parse as a URL — reports that it has no scheme and suggests
		// writing `https://not a uri at all`. Asking the task first produced
		// exactly that, which is a worse answer to a question the schema had
		// already answered well. So: does the field accept this shape, then does
		// the schema's rule accept this value, then will this build do anything
		// with it.
		if err := v1.CheckLiteralInput(def.Name, name, task.GetInputs()[name]); err != nil {
			ds = append(ds, Diagnostic{Step: stepID, Field: name, Message: err.Error()})
		}
	}

	return ds
}

// violatedRulesFor checks one input, in a message holding only that input.
//
// One field at a time rather than all of them together, because a conversion that
// fails takes the whole message with it. An `http` step written with both
// `method: FETCH` and `headers: {X-Count: 5}` reported *neither*: the header's value
// is a number where the field holds strings, the conversion refused it, and the
// method's perfectly visible mistake went with it. One bad input silencing the check
// for the others is worse than not having the check, because the file now looks
// examined.
//
// The cost of the split is that a rule spanning two fields could not fire, since
// neither message would hold both. No task input declares one today and
// TestNoTaskInputsMessageDeclaresACrossFieldRule fails when one does — a limitation
// that is checked rather than remembered.
func violatedRulesFor(stepID, name string, value *v1.Value, def v1.TaskDef) Diagnostics {
	inputs := dynamicpb.NewMessage(def.Inputs)
	if err := v1.PopulateLiterals(inputs, map[string]*v1.Value{name: value}); err != nil {
		// The field's own type accepted the shape and the conversion refused
		// something inside it — a map's value, a list's element. [literalMismatch]
		// cannot see that far: it asks whether a map is a map, not what is in one.
		//
		// So this is a real mistake in the file rather than a disagreement between
		// checks, and reporting it is the only way an author hears about it before
		// the run. The message says which key, which is the part they need.
		return Diagnostics{{
			Step:    stepID,
			Field:   name,
			Message: fmt.Sprintf("task %q does not accept it: %s", def.Name, trimFieldPrefix(err.Error(), name)),
		}}
	}

	var invalid *v1.ValidationError
	if err := v1.Validate(inputs); !errors.As(err, &invalid) {
		// Either it validated, or the validator itself is unavailable. A rule that
		// will not compile is a defect in the schema, and refusing the author's file
		// for it would be this package blaming them for it.
		return nil
	}

	var ds Diagnostics
	for _, violation := range invalid.Violations {
		// Only this field. The message holds one input, so every *other* required
		// field is absent from it and reported as missing — which is a fact about
		// this message rather than about the file. The real case is reported from
		// the source above, where there is a position to name.
		if violationField(violation) != name {
			continue
		}

		ds = append(ds, Diagnostic{
			Step: stepID,
			// The diagnostic's own prefix already names the step and the input, so
			// the message says what the schema objected to and nothing else.
			Field:   name,
			Message: fmt.Sprintf("task %q does not accept it: %s", def.Name, violation.Message),
		})
	}

	return ds
}

// violationField returns the input a violation is about, dropping any path beneath
// it: protovalidate addresses a map entry as `headers[X-Count]` and a list element as
// `args[0]`, and the input is the part before either.
func violationField(violation v1.Violation) string {
	name := violation.Field
	if at := strings.IndexAny(name, ".["); at >= 0 {
		name = name[:at]
	}

	return name
}

// trimFieldPrefix drops the conversion error's own `field "x": ` opening, which the
// diagnostic has already said.
func trimFieldPrefix(message, name string) string {
	return strings.TrimPrefix(message, fmt.Sprintf("field %q: ", name))
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

	case field.Kind() == protoreflect.EnumKind:
		// The one field kind where the *set* of acceptable values is known in full
		// and short enough to print, so this can say what to write instead of only
		// what is wrong. Checked here rather than left to the engine because the
		// engine's answer arrives after the run has started.
		choices := strings.Join(v1.EnumValueNames(field.Enum()), ", ")
		written, isString := literal.GetKind().(*expr.Value_StringValue)
		if !isString {
			return fmt.Sprintf("expected one of %s, but this is %s", choices, literalKind(literal))
		}
		if _, known := v1.EnumValueNumber(field.Enum(), written.StringValue); !known {
			message := fmt.Sprintf("%q is not one of %s", written.StringValue, choices)
			if suggestion, ok := nearestChoice(written.StringValue, v1.EnumValueNames(field.Enum())); ok {
				message += fmt.Sprintf("; did you mean %q?", suggestion)
			}
			return message
		}
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

// varsKey is the `vars:` key, at every position the grammar has one.
//
// A table of retired task *inputs* used to sit here, alongside a hoist that emptied
// one task's `vars:` mapping into the inputs around it and the two predicates that
// decided when to do so. All four served the same retired task, and all four are
// gone with it. The surviving shape for retiring a spelling is `retiredStepKeys` in
// parse.go, which retires a step key rather than an input; an input needs the same
// treatment again the day one is retired, and not before.
const varsKey = "vars"

// The other keys that introduce a bare binding, which is what makes a name inside
// them something other than a step id.
//
// Named here beside `vars:` because they are read together: [boundBareNames] has to
// know all of them, and a rewriter that knew two of three corrupts a working file
// in the one case it did not know about.
const (
	// forEachKey opens a loop, whose `as:` names the item.
	forEachKey = "for_each"

	// forEachAsKey is where that name is written, when it is written at all. A
	// loop with no `as:` still binds one, under [v1.DefaultIterator].
	forEachAsKey = "as"

	// forEachItemsKey and forEachStepsKey are the two keys that make a mapping a
	// loop's, which is how the default iterator is recognised.
	forEachItemsKey = "items"
	forEachStepsKey = "steps"

	// conditionKey is the step's `if:`, evaluated before the step's own `vars:`
	// exist and therefore unable to see them.
	conditionKey = "if"

	// waitUntilKey opens the one expression that sees `now`.
	waitUntilKey = "wait_until"

	// nowBinding is the clock, bound bare and only inside a wait.
	nowBinding = "now"

	// loopUntilKey and loopUpdateKey are the two loop expressions that see the
	// carried state (evaluated after the body each iteration); loopInitKey does not,
	// because it defines the state before the loop begins. A loop is recognised by
	// carrying `until:` and `steps:`, which `for_each` never does.
	loopUntilKey  = "until"
	loopUpdateKey = "update"
)

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

// nearestChoice suggests a value from a small closed set, more willingly than [nearest]
// does.
//
// [nearest] is tuned for a name typed against a large open vocabulary — step ids, task
// inputs — where a loose match suggests something the author never heard of. A set of
// three words is the opposite situation: everything in it is on the author's screen in
// the same message, so a wrong suggestion costs a glance and a missing one costs a
// lookup.
//
// The extra rule is a shared prefix in either direction, which is what an author's
// mistakes here actually look like: `warning` for `warn`, `err` for `error`. Both are
// three edits away and neither is a typo — they are the *other* common spelling of the
// same word, which no edit-distance threshold tight enough to be useful will ever
// reach.
func nearestChoice(got string, choices []string) (string, bool) {
	if suggestion, ok := nearest(got, choices); ok {
		return suggestion, true
	}

	lower := strings.ToLower(got)
	for _, choice := range choices {
		if strings.HasPrefix(lower, choice) || strings.HasPrefix(choice, lower) {
			return choice, true
		}
	}

	return "", false
}
