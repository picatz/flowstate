// Package protodoc reads the prose the schema already carries.
//
// The comments in proto/flowstate/v1/*.proto describe the same things
// several Go surfaces describe again in their own words: the MCP tool table,
// LSP hover, the generated reference. A sentence written twice is a sentence
// that can disagree with itself, and the copy beside the code is the one that
// goes stale when the schema moves. This package makes the schema's own
// comments readable at run time so those surfaces can inherit them instead.
//
// The one technical fact that shapes the design: runtime descriptors compiled
// into generated code carry no comments. protoc-gen-go strips SourceCodeInfo
// from what a .pb.go embeds, so protoreflect over the linked-in registry finds
// shape and no prose. The prose is therefore generated separately, by
// protoc-gen-flowstate-doc in the same `buf generate` run that writes the .pb.go:
// the flowstate_*.doc.pb.go files beside this one register every file's leading
// comments with [protodocimpl] at init. They are held by the same git diff
// --exit-code pin as the types, so they cannot drift from the schema they
// describe, and a comment change reviews as a text diff.
//
// The generated files live in this package rather than beside the .pb.go, so a
// binary pays for the schema's prose only when it imports this package.
//
// Everything here fails closed. An unknown name, an ambiguous one, a descriptor
// with no comment: the answer is the empty string and false. Nothing here
// panics, and nothing here reports a comment it did not find.
package protodoc

import (
	"strings"
	"unicode"

	"github.com/picatz/flowstate/pkg/flowstate/v1/protodoc/protodocimpl"
	"google.golang.org/protobuf/reflect/protoreflect"
)

// Comment returns the normalized leading comment for a schema element, and
// whether one was found.
//
// The name is a protobuf full name: a message (flowstate.v1.RunRequest), a
// field (flowstate.v1.RunRequest.workflow), an enum value, a service, or a
// method (flowstate.v1.WorkflowService.Signal). A name no linked schema
// documents returns "" and false. The cases are deliberately
// indistinguishable: a caller that wants prose has nothing to say either way,
// and a caller that wants to know whether a symbol exists should ask
// protoregistry.GlobalFiles.
func Comment(name protoreflect.FullName) (string, bool) {
	leading, _, ok := protodocimpl.Lookup(name)
	if !ok {
		return "", false
	}
	return normalize(leading)
}

// CommentOf returns the normalized leading comment for a descriptor a caller
// already holds, and whether one was found.
//
// The descriptor's own source info is asked first, so a descriptor that
// arrived carrying comments (a plugin's task input, reconstructed from the
// bytes its manifest sent) is described in its author's words. Otherwise the
// generated comments are asked, which is what answers for a descriptor from
// protoregistry.GlobalFiles; they answer only when they were generated from
// the file the descriptor was declared in, so a same-named declaration from
// some other schema is not given this one's prose.
func CommentOf(desc protoreflect.Descriptor) (string, bool) {
	if desc == nil {
		return "", false
	}
	file := desc.ParentFile()
	if file == nil {
		return "", false
	}
	if text, ok := normalize(file.SourceLocations().ByDescriptor(desc).LeadingComments); ok {
		return text, true
	}
	leading, path, ok := protodocimpl.Lookup(desc.FullName())
	if !ok || path != file.Path() {
		return "", false
	}
	return normalize(leading)
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
// The schema writes [ValidationReport] because it renders as a link on
// pkg.go.dev, where the generated Go types carry these same comments; a field
// is written `SignalWithStartRequest.workflow` instead, since the Go field is
// spelled differently and a bracketed schema spelling would never resolve.
// Everywhere but pkg.go.dev a link reads as stray brackets, so it becomes
// `ValidationReport` here: a name a terminal, a JSON description and a
// Markdown table all render identically.
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
	for part := range strings.SplitSeq(s, ".") {
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
