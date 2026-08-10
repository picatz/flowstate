// Package protodoc reads the prose the schema already carries.
//
// The comments in proto/flowstate/v1/flowstate.proto describe the same things
// several Go surfaces describe again in their own words: the MCP tool table,
// LSP hover, the generated reference. A sentence written twice is a sentence
// that can disagree with itself, and the copy beside the code is the one that
// goes stale when the schema moves. This package makes the schema's own
// comments readable at run time so those surfaces can inherit them instead.
//
// The one technical fact that shapes the design: runtime descriptors compiled
// into generated code carry no comments. protoc strips SourceCodeInfo from what
// a .pb.go embeds, so protoreflect over the linked-in registry finds shape and
// no prose. The prose therefore travels in a separate artifact,
// flowstate.descriptorset.binpb, built by the same pinned buf toolchain that
// writes the .pb.go and held by the same git diff --exit-code pin, so it cannot
// drift from the schema it describes.
//
// Everything here fails closed. An unknown name, a corrupt artifact, a
// descriptor with no comment: the answer is the empty string and false. Nothing
// here panics, and nothing here reports a comment it did not find.
package protodoc

import (
	_ "embed"
	"strings"
	"sync"
	"unicode"

	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protodesc"
	"google.golang.org/protobuf/reflect/protoreflect"
	"google.golang.org/protobuf/reflect/protoregistry"
	"google.golang.org/protobuf/types/descriptorpb"
)

// rawDescriptorSet is the FileDescriptorSet for this repository's schema, built
// with source info retained.
//
// Built by `buf build --exclude-imports`, which is the whole of the artifact's
// contract: imports are excluded because the prose this package serves is the
// prose this repository wrote, and carrying googleapis and protovalidate along
// would double the bytes to describe files nobody asks this package about. The
// cost of that choice is that the set does not link on its own, which is why
// Files below allows unresolvable references rather than failing on them.
//
//go:embed flowstate.descriptorset.binpb
var rawDescriptorSet []byte

// files is parsed once, on first use, and never mutated afterwards.
var files = sync.OnceValues(func() (*protoregistry.Files, error) {
	set := &descriptorpb.FileDescriptorSet{}
	if err := proto.Unmarshal(rawDescriptorSet, set); err != nil {
		return nil, err
	}
	// AllowUnresolvable, because the set deliberately excludes its imports: a
	// field typed google.protobuf.Struct resolves to a placeholder rather than
	// failing the whole registry. A placeholder still has a name, which is all
	// a caller walking for prose needs, and IsPlaceholder tells a caller that
	// asked for more.
	return protodesc.FileOptions{AllowUnresolvable: true}.NewFiles(set)
})

// Files returns the schema's descriptors, with comments attached.
//
// Callers that need to walk the schema (a presence check over a service, a
// generator over a message) take it from here rather than from
// protoregistry.GlobalFiles, because only these descriptors carry
// SourceLocations. Returns an error rather than a nil registry if the embedded
// artifact cannot be read, so a caller cannot mistake "no schema" for "no
// prose".
func Files() (*protoregistry.Files, error) {
	return files()
}

// Comment returns the normalized leading comment for a schema element, and
// whether one was found.
//
// The name is a protobuf full name: a message (flowstate.v1.RunRequest), a
// field (flowstate.v1.RunRequest.workflow), an enum value, a service, or a
// method (flowstate.v1.WorkflowService.Signal). A name this schema does not
// declare, or one that declares no leading comment, returns "" and false. Those
// two cases are deliberately indistinguishable: a caller that wants prose has
// nothing to say either way, and a caller that wants to know whether a symbol
// exists should ask Files.
func Comment(name protoreflect.FullName) (string, bool) {
	reg, err := files()
	if err != nil {
		return "", false
	}
	desc, err := reg.FindDescriptorByName(name)
	if err != nil || desc == nil {
		return "", false
	}
	return CommentOf(desc)
}

// CommentOf returns the normalized leading comment for a descriptor a caller
// already holds, and whether one was found.
//
// The descriptor must come from Files. One from protoregistry.GlobalFiles
// carries no SourceCodeInfo, so this answers false for it rather than pretending
// the symbol has no documentation.
func CommentOf(desc protoreflect.Descriptor) (string, bool) {
	if desc == nil {
		return "", false
	}
	file := desc.ParentFile()
	if file == nil {
		return "", false
	}
	loc := file.SourceLocations().ByDescriptor(desc)
	return normalize(loc.LeadingComments)
}

// Method returns the normalized leading comment for one RPC, addressed the way
// a caller with a service and a method name in hand already holds it.
//
// This is Comment(service + "." + method) with the concatenation done once, in
// one place, because the surfaces that need it (an MCP tool table keyed by
// method name, a reference generator walking a service) all have the two halves
// separately and would otherwise each spell the join themselves.
func Method(service protoreflect.FullName, method protoreflect.Name) (string, bool) {
	if service == "" || method == "" {
		return "", false
	}
	return Comment(service.Append(method))
}

// FirstSentence returns the first sentence of a comment, for the one-line
// contexts that cannot show a paragraph: a tool list, a completion item, a
// column in a table.
//
// It ends at the first period that ends a sentence, which is not every period:
// "e.g." and "i.e." and a single initial are not sentence ends, and a period
// inside a backticked span is part of the span. If no sentence end is found the
// whole first paragraph is returned, because a caller asking for one line is
// better served by a long one than by nothing.
func FirstSentence(comment string) string {
	para, _, _ := strings.Cut(strings.TrimSpace(comment), "\n\n")
	para = strings.TrimSpace(strings.ReplaceAll(para, "\n", " "))
	if para == "" {
		return ""
	}

	inCode := false
	for i, r := range para {
		switch {
		case r == '`':
			inCode = !inCode
		case r == '.' && !inCode:
			if !endsSentence(para, i) {
				continue
			}
			return para[:i+1]
		}
	}
	return para
}

// endsSentence reports whether the period at index i in s closes a sentence.
func endsSentence(s string, i int) bool {
	// A period followed by more text only ends a sentence when whitespace
	// follows it. "1.5" and "flowstate.v1" are not sentence ends.
	rest := s[i+1:]
	if rest != "" {
		r := []rune(rest)[0]
		if !unicode.IsSpace(r) {
			return false
		}
	}
	// The abbreviations that actually appear in prose of this kind, plus a
	// single capital letter, which is an initial rather than a sentence.
	before := s[:i]
	for _, abbrev := range []string{"e.g", "i.e", "etc", "vs", "cf", "Mr", "Ms", "Dr", "No"} {
		if strings.HasSuffix(before, abbrev) {
			return false
		}
	}
	if word := lastWord(before); len([]rune(word)) == 1 {
		r := []rune(word)[0]
		if unicode.IsUpper(r) {
			return false
		}
	}
	return true
}

func lastWord(s string) string {
	if i := strings.LastIndexFunc(s, unicode.IsSpace); i >= 0 {
		return s[i+1:]
	}
	return s
}

// normalize turns a raw leading comment into prose.
//
// Raw comments arrive as protoc hands them over: every line already has its //
// removed and a single leading space left behind, and the whole block ends in a
// newline. What is left to do is take that space off, keep paragraphs apart,
// unwrap the hard line breaks inside a paragraph so a consumer can wrap the
// text itself, and translate the schema's [Symbol] links into something a
// terminal or a JSON field can show.
func normalize(raw string) (string, bool) {
	if strings.TrimSpace(raw) == "" {
		return "", false
	}

	lines := strings.Split(strings.TrimRight(raw, "\n"), "\n")
	for i, line := range lines {
		lines[i] = strings.TrimRight(strings.TrimPrefix(line, " "), " \t")
	}

	var out strings.Builder
	pendingBlank := false
	for i, line := range lines {
		if strings.TrimSpace(line) == "" {
			if out.Len() > 0 {
				pendingBlank = true
			}
			continue
		}
		switch {
		case out.Len() == 0:
			// first line of the comment
		case pendingBlank:
			out.WriteString("\n\n")
		case isStructural(line) || isStructural(lines[i-1]):
			// A bullet, a numbered item or an indented block is structure the
			// author chose, so its line break is meaning rather than wrapping.
			out.WriteString("\n")
		default:
			out.WriteString(" ")
		}
		pendingBlank = false
		out.WriteString(line)
	}

	text := translateLinks(out.String())
	if strings.TrimSpace(text) == "" {
		return "", false
	}
	return text, true
}

// isStructural reports whether a line's break is the author's structure rather
// than a wrap point: a list item, or an indented block such as an example.
func isStructural(line string) bool {
	if strings.HasPrefix(line, " ") || strings.HasPrefix(line, "\t") {
		return true
	}
	trimmed := strings.TrimLeft(line, " \t")
	for _, prefix := range []string{"- ", "* ", "+ ", "> ", "| "} {
		if strings.HasPrefix(trimmed, prefix) {
			return true
		}
	}
	// "1. ", "2) " and so on.
	digits := 0
	for digits < len(trimmed) && trimmed[digits] >= '0' && trimmed[digits] <= '9' {
		digits++
	}
	if digits > 0 && digits+1 < len(trimmed) &&
		(trimmed[digits] == '.' || trimmed[digits] == ')') && trimmed[digits+1] == ' ' {
		return true
	}
	return false
}

// translateLinks rewrites godoc-style [Symbol] links as backticked names.
//
// The schema writes [ValidationReport] and [SignalWithStartRequest.workflow]
// because those render as links on pkg.go.dev, where the generated Go types
// carry these same comments. Everywhere else they read as stray brackets, so
// they become `ValidationReport` here: a name a terminal, a JSON description
// and a Markdown table all render identically.
//
// Only bracketed text shaped like a symbol is touched. Prose that genuinely
// brackets something ("[sic]", "[1]") keeps its brackets, because rewriting it
// would be inventing a link the author did not write.
func translateLinks(s string) string {
	var out strings.Builder
	out.Grow(len(s))
	// Brackets inside an existing code span are that span's own text, never a
	// link: the schema writes `list[string]` as a type an author spells, and
	// translating its inner "[string]" would nest backticks and break the span.
	// Parity over backticks decides which side of that boundary a bracket is on.
	inCode := false
	for i := 0; i < len(s); {
		c := s[i]
		if c == '`' {
			inCode = !inCode
			out.WriteByte(c)
			i++
			continue
		}
		if c != '[' || inCode {
			out.WriteByte(c)
			i++
			continue
		}
		close := strings.IndexByte(s[i:], ']')
		if close < 0 {
			out.WriteString(s[i:])
			break
		}
		close += i
		inner := s[i+1 : close]
		if isSymbol(inner) {
			out.WriteString("`")
			out.WriteString(inner)
			out.WriteString("`")
		} else {
			// Copied verbatim, so any backtick inside still toggles the span
			// state the next bracket is judged against.
			out.WriteString(s[i : close+1])
			inCode = (inCode != (strings.Count(inner, "`")%2 == 1))
		}
		i = close + 1
	}
	return out.String()
}

// isSymbol reports whether text between brackets names a protobuf symbol:
// dot-separated identifiers, nothing else.
func isSymbol(s string) bool {
	if s == "" {
		return false
	}
	for _, part := range strings.Split(s, ".") {
		if part == "" {
			return false
		}
		for i, r := range part {
			switch {
			case r == '_':
			case unicode.IsLetter(r):
			case unicode.IsDigit(r) && i > 0:
			default:
				return false
			}
		}
	}
	return true
}
