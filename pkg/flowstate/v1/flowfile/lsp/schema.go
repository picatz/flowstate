package lsp

import (
	"fmt"
	"slices"
	"strings"

	"buf.build/gen/go/bufbuild/protovalidate/protocolbuffers/go/buf/validate"
	v1 "github.com/picatz/flowstate/pkg/flowstate/v1"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protoreflect"
)

// Everything an editor shows about a task — its inputs, their types, which are
// required, what values they accept — is read from the Protobuf descriptors on
// the task's registry entry. There is no table here to fall out of date, and
// nothing this file can say that the engine would not also enforce, because both
// read the same schema.

// dynamicValueMessages are the messages that carry a CEL value, and so fields whose
// type a Flowfile author sees as "whatever the expression yields".
//
// There are two because the schema uses both: its own wrapper, and CEL's, which the
// http task's parsed `json` output is. Neither name means anything in the DSL — an
// author has no `Value` type to reason about — so both render as `any`.
var dynamicValueMessages = []protoreflect.FullName{
	"flowstate.v1.Value",
	"google.api.expr.v1alpha1.Value",
}

// fieldNames returns the input or output names a message declares, in field
// number order, which is the order the schema author chose.
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

// findField returns the descriptor of a named field, or nil.
func findField(md protoreflect.MessageDescriptor, name string) protoreflect.FieldDescriptor {
	if md == nil {
		return nil
	}
	return md.Fields().ByName(protoreflect.Name(name))
}

// typeName renders a field's type the way a Flowfile author writes values, not
// the way Protobuf names them: a repeated string is a list, and a field carrying
// a CEL value is dynamic.
func typeName(fd protoreflect.FieldDescriptor) string {
	return v1.InputTypeName(fd)
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
			// A CEL value: the concrete type is whatever the expression
			// produces, which is exactly what "any" tells the author.
			return "any"
		}
		return string(fd.Message().Name())
	default:
		return fd.Kind().String()
	}
}

// fieldRules returns the protovalidate rules attached to a field, or nil.
func fieldRules(fd protoreflect.FieldDescriptor) *validate.FieldRules {
	if fd == nil {
		return nil
	}
	rules, _ := proto.GetExtension(fd.Options(), validate.E_Field).(*validate.FieldRules)
	return rules
}

// required reports whether the schema marks a field as one the task cannot run
// without.
//
// This is read from protovalidate rather than from a list here, so that a field
// that becomes required in the schema immediately becomes required in the editor.
func required(fd protoreflect.FieldDescriptor) bool {
	return v1.RequiredInput(fd)
}

// constraints renders a field's protovalidate rules as short phrases, so hover
// can tell an author that a method must match a pattern before the engine
// rejects one that does not.
//
// Only the rules the Flowfile schema actually uses are rendered. A rule with no
// case here is silently omitted rather than printed in Protobuf spelling, since
// a half-translated constraint reads as a bug.
func constraints(fd protoreflect.FieldDescriptor) []string {
	rules := fieldRules(fd)
	if rules == nil {
		return nil
	}

	var out []string
	if s := rules.GetString(); s != nil {
		if s.HasLen() {
			out = append(out, fmt.Sprintf("exactly %d characters", s.GetLen()))
		}
		if s.HasMinLen() && s.GetMinLen() > 0 {
			out = append(out, fmt.Sprintf("at least %d characters", s.GetMinLen()))
		}
		if s.HasMaxLen() {
			out = append(out, fmt.Sprintf("at most %d characters", s.GetMaxLen()))
		}
		if s.HasPattern() {
			out = append(out, fmt.Sprintf("matches %s", s.GetPattern()))
		}
		if s.HasUri() && s.GetUri() {
			out = append(out, "must be an absolute URI")
		}
		if in := s.GetIn(); len(in) > 0 {
			out = append(out, "one of "+strings.Join(in, ", "))
		}
	}
	if i := rules.GetInt32(); i != nil {
		if i.HasGte() {
			out = append(out, fmt.Sprintf("at least %d", i.GetGte()))
		}
		if i.HasGt() {
			out = append(out, fmt.Sprintf("greater than %d", i.GetGt()))
		}
		if i.HasLte() {
			out = append(out, fmt.Sprintf("at most %d", i.GetLte()))
		}
		if i.HasLt() {
			out = append(out, fmt.Sprintf("less than %d", i.GetLt()))
		}
	}
	if r := rules.GetRepeated(); r != nil {
		if r.HasMinItems() && r.GetMinItems() > 0 {
			out = append(out, fmt.Sprintf("at least %d item(s)", r.GetMinItems()))
		}
		if r.HasMaxItems() {
			out = append(out, fmt.Sprintf("at most %d item(s)", r.GetMaxItems()))
		}
		if r.GetUnique() {
			out = append(out, "items must be unique")
		}
	}
	return out
}

// The schema's own comments would be the natural source of per-field prose, but
// protoc-gen-go strips SourceCodeInfo from the descriptors it embeds, so
// fd.ParentFile().SourceLocations() is empty at run time and there is nothing to
// read. Hover therefore reports the type, whether the field is required, and the
// protovalidate rules — all of which are present in the descriptor — rather than
// prose that would silently always be blank. See the note in the accompanying
// report about emitting source info so the comments become available.

// signature renders a task's full input and output shape as a fenced block, the
// form editors display best in hover.
func signature(def v1.TaskDef) string {
	var b strings.Builder
	b.WriteString("```\n")
	b.WriteString(def.Name)
	b.WriteString("\n")
	writeFields(&b, "inputs", def.Inputs, true)
	writeFields(&b, "outputs", def.Outputs, false)
	b.WriteString("```")
	return b.String()
}

// writeFields renders one side of a task's signature, aligning the columns so a
// task with several inputs stays readable.
func writeFields(b *strings.Builder, label string, md protoreflect.MessageDescriptor, showRequired bool) {
	if md == nil {
		fmt.Fprintf(b, "\n%s:\n  (not described by the schema)\n", label)
		return
	}
	fields := md.Fields()
	if fields.Len() == 0 {
		fmt.Fprintf(b, "\n%s:\n  (none)\n", label)
		return
	}

	width := 0
	for i := range fields.Len() {
		width = max(width, len(fields.Get(i).Name()))
	}

	fmt.Fprintf(b, "\n%s:\n", label)
	for i := range fields.Len() {
		fd := fields.Get(i)
		fmt.Fprintf(b, "  %-*s  %s", width, fd.Name(), typeName(fd))
		if showRequired && required(fd) {
			b.WriteString("  (required)")
		}
		b.WriteString("\n")
	}
}
