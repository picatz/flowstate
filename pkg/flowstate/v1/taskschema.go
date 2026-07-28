package flowstatev1

import (
	"fmt"
	"slices"

	validate "buf.build/gen/go/bufbuild/protovalidate/protocolbuffers/go/buf/validate"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protoreflect"
)

// Reading a task's shape out of its schema, in one place.
//
// A [TaskDef] carries the descriptors of its input and output messages, and that
// is deliberately the only definition of what a task takes: the engine validates
// against it, `flow validate` reports against it, the editor completes from it,
// and `flow tasks` prints it. What that leaves is a handful of questions each of
// those has to ask — is this field required, what would an author call its type —
// and those were being answered separately.
//
// Two implementations of "required" existed, in flowfile and in the language
// server, agreeing today and written differently enough to stop agreeing later.
// One of them checked `HasMinItems() && GetMinItems() > 0` and the other only
// `GetMinItems() > 0`. Nothing turns on the difference right now, which is exactly
// the condition under which a copy survives long enough to matter.
//
// "What type is this" existed once, in the language server, which is why `flow
// tasks` could tell you a task exists and not what it takes.

// dynamicValueMessages are the schema messages that hold whatever an expression
// produced, rather than a fixed type.
//
// Naming the concrete message would be true and useless: an author writing a
// value there needs to know the shape is unconstrained, not which wrapper the
// engine stores it in.
var dynamicValueMessages = []protoreflect.FullName{
	"flowstate.v1.Value",
	"google.api.expr.v1alpha1.Value",
	"google.protobuf.Value",
}

// FieldRules returns the protovalidate rules attached to a field, or nil.
func FieldRules(fd protoreflect.FieldDescriptor) *validate.FieldRules {
	if fd == nil {
		return nil
	}
	rules, _ := proto.GetExtension(fd.Options(), validate.E_Field).(*validate.FieldRules)
	return rules
}

// RequiredInput reports whether the schema marks a field as one the task cannot
// run without.
//
// Read from protovalidate rather than from a list, so a field that becomes
// required in the schema is required everywhere at once — in the engine that
// rejects the run, in the diagnostic that explains it, and in the completion that
// offers it first.
//
// Two spellings mean the same thing and the schema uses both: `required` on a
// singular field, and `min_items: 1` on a repeated one, where "required" would be
// satisfied by an empty list.
func RequiredInput(fd protoreflect.FieldDescriptor) bool {
	rules := FieldRules(fd)
	if rules == nil {
		return false
	}
	if rules.GetRequired() {
		return true
	}

	repeated := rules.GetRepeated()

	return repeated != nil && repeated.GetMinItems() > 0
}

// InputTypeName names a field's type the way an author would say it.
//
// The DSL's vocabulary, not Protobuf's: an author writes YAML and thinks in
// `string`, `list[string]`, `map[string, string]`. Reporting `TYPE_STRING` or
// `repeated .flowstate.v1.Value` would be accurate about the schema and useless
// about the file being written.
func InputTypeName(fd protoreflect.FieldDescriptor) string {
	if fd == nil {
		return "unknown"
	}

	switch {
	case fd.IsMap():
		return fmt.Sprintf("map[%s, %s]", scalarTypeName(fd.MapKey()), scalarTypeName(fd.MapValue()))
	case fd.IsList():
		return fmt.Sprintf("list[%s]", scalarTypeName(fd))
	default:
		return scalarTypeName(fd)
	}
}

// scalarTypeName names the type of a single value of the field's element type.
func scalarTypeName(fd protoreflect.FieldDescriptor) string {
	switch fd.Kind() {
	case protoreflect.StringKind:
		return "string"
	case protoreflect.BoolKind:
		return "bool"
	case protoreflect.BytesKind:
		return "bytes"
	case protoreflect.Int32Kind, protoreflect.Int64Kind, protoreflect.Sint32Kind,
		protoreflect.Sint64Kind, protoreflect.Sfixed32Kind, protoreflect.Sfixed64Kind:
		return "int"
	case protoreflect.Uint32Kind, protoreflect.Uint64Kind, protoreflect.Fixed32Kind,
		protoreflect.Fixed64Kind:
		return "uint"
	case protoreflect.FloatKind, protoreflect.DoubleKind:
		return "double"
	case protoreflect.EnumKind:
		return string(fd.Enum().Name())
	case protoreflect.MessageKind, protoreflect.GroupKind:
		if slices.Contains(dynamicValueMessages, fd.Message().FullName()) {
			// A CEL value: the concrete type is whatever the expression produces,
			// which is exactly what "any" tells the author.
			return "any"
		}
		return string(fd.Message().Name())
	default:
		return fd.Kind().String()
	}
}

// An InputField is one input a task accepts, described the way an author needs it.
type InputField struct {
	// Name is what the input is called in a Flowfile.
	Name string

	// Type is the DSL's name for what it holds.
	Type string

	// Required reports whether the task cannot run without it.
	Required bool

	// Deferred reports whether the task evaluates this input's expression itself,
	// against a scope the workflow does not have. The http task's `outputs` is
	// the example: it names response variables that exist only after the request.
	Deferred bool
}

// Inputs describes what a task accepts, required fields first.
//
// Required first because that is the order somebody needs them in: the inputs
// without which nothing works, and then the ones that tune it. Within each group
// the schema's own field order is kept, which is the order the person who defined
// the message chose to explain it in.
func Inputs(def TaskDef) []InputField {
	return describeFields(def.Inputs, def.DeferredInputs)
}

// Outputs describes what a task produces.
//
// Required is not reported: an output the task always sets is not a thing the
// author has to supply, so the distinction says nothing here. Whether a field is
// present after a step ran is a question about that run.
func Outputs(def TaskDef) []InputField {
	fields := describeFields(def.Outputs, nil)
	for i := range fields {
		fields[i].Required = false
	}
	return fields
}

// describeFields walks a message descriptor into the author's vocabulary.
func describeFields(md protoreflect.MessageDescriptor, deferred []string) []InputField {
	if md == nil {
		return nil
	}

	fields := md.Fields()
	out := make([]InputField, 0, fields.Len())
	for i := range fields.Len() {
		fd := fields.Get(i)
		name := string(fd.Name())
		out = append(out, InputField{
			Name:     name,
			Type:     InputTypeName(fd),
			Required: RequiredInput(fd),
			Deferred: slices.Contains(deferred, name),
		})
	}

	slices.SortStableFunc(out, func(a, b InputField) int {
		switch {
		case a.Required == b.Required:
			return 0
		case a.Required:
			return -1
		default:
			return 1
		}
	})

	return out
}
