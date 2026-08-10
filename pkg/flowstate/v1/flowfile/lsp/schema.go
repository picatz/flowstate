package lsp

import (
	"fmt"
	"strings"

	v1 "github.com/picatz/flowstate/pkg/flowstate/v1"
	"github.com/picatz/flowstate/pkg/flowstate/v1/protodoc"
	"google.golang.org/protobuf/reflect/protoreflect"
)

// Everything an editor shows about a task (its inputs, their types, which are
// required, what values they accept, and what each one is *for*) is read from the
// Protobuf descriptors on the task's registry entry, or from the schema's own
// comments through [protodoc]. There is no table here to fall out of date, and
// nothing this file can say that the engine would not also enforce, because both
// read the same schema.

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
// One derivation, not a second one: [v1.FieldConstraints] is what `flow tasks`
// prints and what the generated reference tabulates, and this package reads it
// so an editor cannot describe a bound in different words from the terminal.
// The copy that used to live here did exactly that, spelling one field's bound
// "at least 3 characters, at most 6 characters" where every other surface said
// "3 to 6 characters".
func constraints(fd protoreflect.FieldDescriptor) []string {
	return v1.FieldConstraints(fd)
}

// fieldDoc returns the schema's own prose for a field, or empty when nothing
// this build carries describes it.
//
// Read through [protodoc] rather than off the descriptor the registry holds,
// because protoc strips SourceCodeInfo from what a .pb.go embeds: the linked-in
// descriptor carries the shape and none of the prose, which is why this package
// used to say the comments were unreachable and write its own. A descriptor that
// does carry its own source info is asked first, so a plugin shipping one
// documents its inputs the same way a built-in task does.
//
// Empty is a real answer and the caller must render nothing rather than a gap: a
// task whose message this build's schema does not describe (a plugin's, an
// embedder's) has no sentence to inherit, and inventing one would describe a
// field nobody wrote a description for.
func fieldDoc(fd protoreflect.FieldDescriptor) string {
	if fd == nil {
		return ""
	}
	if doc, ok := protodoc.CommentOf(fd); ok {
		return doc
	}
	doc, _ := protodoc.Comment(fd.FullName())

	return doc
}

// schemaSentence returns the opening sentence of a named schema symbol's
// comment, or empty when this build's schema does not declare it.
//
// One sentence rather than the whole comment, for the positions where a surface
// writes the rest itself. The wait's result names are the example: the schema
// describes `payload` and `sender` as the pair a signal delivery carries, and
// everything else an author needs there is true only inside a shaping (the name
// is bound bare; a later step reads it only if the shaping re-exposed it), which
// is this package's to say and not the schema's.
//
// A name that resolves to nothing answers empty rather than guessing, and the
// symbols the hovers here name are pinned by the presence walk in protodoc, so
// an empty answer means the schema moved rather than that the prose was
// optional.
func schemaSentence(name protoreflect.FullName) string {
	comment, ok := protodoc.Comment(name)
	if !ok {
		return ""
	}

	return protodoc.FirstSentence(comment)
}

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
