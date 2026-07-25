package lsp

import (
	"fmt"
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

// dynamicValueMessage is the schema message that carries a CEL value, and so a
// field whose type the Flowfile author sees as "whatever the expression yields".
const dynamicValueMessage protoreflect.FullName = "flowstate.v1.Value"

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
		if fd.Message().FullName() == dynamicValueMessage {
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
	rules := fieldRules(fd)
	if rules == nil {
		return false
	}
	if rules.GetRequired() {
		return true
	}
	// min_items: 1 on a repeated field is required by another name, and the
	// schema uses both spellings.
	if r := rules.GetRepeated(); r != nil && r.HasMinItems() && r.GetMinItems() > 0 {
		return true
	}
	return false
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

// closestName returns the candidate closest to name, or the empty string when
// none is close enough to be worth suggesting.
//
// The threshold is deliberately tight. Suggesting "message" for "url" would be
// noise, while suggesting it for "mesage" is the whole point of the check.
func closestName(name string, candidates []string) string {
	best, bestDist := "", 0
	limit := max(len(name)/3, 1)
	for _, c := range candidates {
		d := editDistance(strings.ToLower(name), strings.ToLower(c))
		if d > limit {
			continue
		}
		if best == "" || d < bestDist {
			best, bestDist = c, d
		}
	}
	return best
}

// editDistance returns the edit distance between a and b, counting a transposition
// of two adjacent characters as one edit.
//
// Plain Levenshtein charges two for a transposition, which puts "ulr" two away from
// "url" and so past the suggestion threshold for any short name — and transposing
// two characters is the most common way to misspell one. Distances are over code
// points, so an accented character costs the same as an ASCII one.
func editDistance(a, b string) int {
	ar, br := []rune(a), []rune(b)

	// Rows for i-2, i-1, and i; the i-2 row is what makes transposition cost one.
	prev2 := make([]int, len(br)+1)
	prev := make([]int, len(br)+1)
	curr := make([]int, len(br)+1)
	for j := range prev {
		prev[j] = j
	}

	for i := 1; i <= len(ar); i++ {
		curr[0] = i
		for j := 1; j <= len(br); j++ {
			cost := 1
			if ar[i-1] == br[j-1] {
				cost = 0
			}
			curr[j] = min(prev[j]+1, curr[j-1]+1, prev[j-1]+cost)
			if i > 1 && j > 1 && ar[i-1] == br[j-2] && ar[i-2] == br[j-1] {
				curr[j] = min(curr[j], prev2[j-2]+1)
			}
		}
		prev2, prev, curr = prev, curr, prev2
	}
	return prev[len(br)]
}
