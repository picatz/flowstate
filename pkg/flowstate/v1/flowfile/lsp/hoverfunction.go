package lsp

import (
	"cmp"
	"fmt"
	"slices"
	"strings"
	"sync"

	lsp "github.com/sourcegraph/go-lsp"

	v1 "github.com/picatz/flowstate/pkg/flowstate/v1"
	"github.com/picatz/flowstate/pkg/flowstate/v1/celcomplete"
)

// Completion offers the profile's functions and hover said nothing about one.
//
// So an author could be shown `sortBy` while typing, accept it, and have no way to
// ask what it is — which is the half of discovery that matters after the first
// time. Hover is where somebody asks about code they are *reading*, including code
// they did not write.

// functionIndex is the profile's functions, keyed for lookup by written name.
var functionIndex = sync.OnceValue(func() (out struct {
	byName     map[string]v1.LibraryFunction
	namespaces map[string]bool
},
) {
	out.byName = map[string]v1.LibraryFunction{}
	out.namespaces = map[string]bool{}

	for _, fn := range v1.ProfileFunctions(v1.CurrentProfile) {
		out.byName[fn.Name] = fn
		if qualifier, _, ok := strings.Cut(fn.Name, "."); ok {
			out.namespaces[qualifier] = true
		}
	}

	return out
})

// hoverFunction describes the profile function under the cursor.
//
// The fallback, never the first answer. A resolved reference wins: `value` is a
// function in the optional library and is also a perfectly ordinary step output, so
// `${steps.web.value}` has to describe the output. The caller only reaches this once
// the reference lookups have declined, and never for a `steps.`-rooted reference at
// all — nothing inside one is a call.
func hoverFunction(doc *document, v *value, f fence, cursor int) *lsp.Hover {
	if h := hoverDeclaredFunction(doc, v, f, cursor); h != nil {
		return h
	}

	fn, span, ok := functionAt(f.source, cursor)
	if !ok {
		return nil
	}

	rng := v.fenceSpanOrWhole(doc.index, f, span[0], span[1])

	if fn.Name == "" {
		// A namespace rather than a function: the cursor is on `math` in
		// `math.abs(x)`, or on one written alone.
		return markdownHover(fmt.Sprintf(
			"**`%s`** — a namespace of functions.\n\nWritten `%s.<name>(...)`. "+
				"`flow tasks` lists what is in it, and every other name the profile provides.",
			fn.Library, fn.Library), rng)
	}

	if fn.Macro {
		written := "It is written on something"
		if fn.Example != "" {
			// The call form, which the name is not: cel-go reports `greatest` for
			// something written `math.greatest(1, 2)`.
			written = fmt.Sprintf("Written `%s`. It goes on something", fn.Example)
		}

		return markdownHover(fmt.Sprintf(
			"**`%s`** — a macro from the `%s` library.\n\n%s%s"+
				" (a value or a namespace) and is expanded when the file *compiles*, so what a "+
				"run carries is the expansion rather than this spelling. That is why a macro's "+
				"meaning is frozen by the spec where a function's is resolved by whichever worker "+
				"evaluates the run.",
			fn.Name, fn.Library, functionDescription(fn), written), rng)
	}

	return markdownHover(fmt.Sprintf(
		"**`%s`** — from the `%s` library.\n\n%s%s"+
			"Available to every expression in the file: an `if:`, a `vars:` value, a task input, "+
			"a loop's `items:`, a `wait_until:`. One profile, one dialect.",
		fn.Name, fn.Library, functionSignatureBlock(fn), functionDescription(fn)), rng)
}

// functionSignatureBlock renders a function's call forms as a code block, one
// overload per line, followed by a paragraph break; empty when it has none.
//
// First, because it answers "how do I write this": argument order, arity,
// types, and whether it goes on a namespace or a value.
func functionSignatureBlock(fn v1.LibraryFunction) string {
	if len(fn.Signature) == 0 {
		return ""
	}

	return "```cel\n" + strings.Join(fn.Signature, "\n") + "\n```\n\n"
}

// functionDescription is the declaration's own description followed by a
// paragraph break, or empty when the declaration carries none.
func functionDescription(fn v1.LibraryFunction) string {
	if fn.Description == "" {
		return ""
	}

	return fn.Description + "\n\n"
}

// functionAt returns the function named at the cursor, and the span of the name as
// the author wrote it.
//
// A zero Name with a Library set means the cursor is on a *namespace* rather than a
// function — `math` in `math.abs(x)`.
//
// Text over the parsed expression, matching [referenceAt], because the same thing
// makes both work: an expression that does not parse is exactly when somebody is
// mid-edit and most wants to know what a name is.
func functionAt(src string, cursor int) (v1.LibraryFunction, [2]int, bool) {
	var none v1.LibraryFunction

	if insideStringLiteral(src, cursor) {
		// `${'upperAscii'}` is a string that happens to spell a function. The lookup
		// below is lexical — it knows names, not syntax — so without this it reports
		// the strings library for a piece of text, which is a confident answer about
		// something that is not there at all.
		return none, [2]int{}, false
	}

	segments, at, ok := segmentAt(src, cursor)
	if !ok {
		return none, [2]int{}, false
	}

	index := functionIndex()

	// A qualified name wins over a bare one, and the difference is real: `replace`
	// is a function in the strings library *and* the tail of `regex.replace` in
	// another. Describing the bare one where the author wrote the qualified one
	// would name the wrong library and the wrong behaviour.
	if at > 0 {
		if fn, found := index.byName[segments[at-1].text+"."+segments[at].text]; found {
			return fn, [2]int{segments[at-1].start, segments[at].end}, true
		}
	}
	if at+1 < len(segments) {
		if fn, found := index.byName[segments[at].text+"."+segments[at+1].text]; found {
			return fn, [2]int{segments[at].start, segments[at+1].end}, true
		}
	}

	if fn, found := index.byName[segments[at].text]; found {
		return fn, [2]int{segments[at].start, segments[at].end}, true
	}

	if index.namespaces[segments[at].text] {
		// Reported as a library with no function name, which is how the caller
		// tells a namespace from a function without a second return value that
		// would be ignored everywhere else.
		return v1.LibraryFunction{Library: segments[at].text},
			[2]int{segments[at].start, segments[at].end}, true
	}

	return none, [2]int{}, false
}

// A segment is one dot-separated part of the word under the cursor, and where it
// sits in the expression.
type segment struct {
	text  string
	start int
	end   int
}

// segmentAt splits the word around the cursor and says which part the cursor is in.
//
// The word is taken the way [referenceAt] takes it, so the two agree about where a
// name begins and ends — a hover that highlighted a different span from the one the
// reference lookup considered would underline something the text beside it is not
// about.
func segmentAt(src string, cursor int) ([]segment, int, bool) {
	if cursor < 0 || cursor > len(src) {
		return nil, 0, false
	}

	isWord := func(c byte) bool {
		return c == '_' || c == '.' ||
			(c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z') || (c >= '0' && c <= '9')
	}

	start := min(cursor, len(src))
	for start > 0 && isWord(src[start-1]) {
		start--
	}
	end := min(cursor, len(src))
	for end < len(src) && isWord(src[end]) {
		end++
	}
	if start == end {
		return nil, 0, false
	}

	var (
		segments []segment
		at       = -1
		from     = start
	)
	for i := start; i <= end; i++ {
		if i < end && src[i] != '.' {
			continue
		}
		if text := src[from:i]; text != "" {
			segments = append(segments, segment{text: text, start: from, end: i})
			// The cursor sits in this part when it is anywhere from its first
			// character to just past its last — the trailing edge included, since
			// an editor reports a cursor between two characters and hovering the
			// end of a name is hovering the name.
			if cursor >= from && cursor <= i {
				at = len(segments) - 1
			}
		}
		from = i + 1
	}

	if at < 0 || len(segments) == 0 {
		return nil, 0, false
	}

	return segments, at, true
}

// insideStringLiteral reports whether the cursor sits within a quoted literal.
//
// Scanned rather than parsed, for the reason the rest of this file is: hover has to
// work on an expression that does not compile, because that is when somebody is
// asking. What it needs to know is narrow enough to be answerable lexically — CEL's
// string literals are single- or double-quoted with backslash escapes, and a quote
// inside the other kind is ordinary text.
//
// Raw strings (`r'...'`) are not treated specially. The prefix is outside the quotes
// either way, so the region this finds is the same one.
func insideStringLiteral(src string, cursor int) bool {
	if cursor < 0 || cursor > len(src) {
		return false
	}

	var quote byte
	for i := 0; i < len(src) && i < cursor; i++ {
		c := src[i]
		switch {
		case quote != 0 && c == '\\':
			// An escape consumes the next character, so a `\'` does not close the
			// literal it is inside.
			i++
		case quote != 0 && c == quote:
			quote = 0
		case quote == 0 && (c == '\'' || c == '"'):
			quote = c
		}
	}

	return quote != 0
}

// hoverDeclaredFunction describes a call to one of the file's own `functions:`
// under the cursor: its signature as the definition declares it, and its
// description.
//
// Before the profile's functions, which is safe rather than a precedence call: a
// declared name may not be one the profile already has, so the two can never be
// the same word. What the author needs at a call is what the definition asks of
// them (which arguments, of which types, and what comes back), and that is
// otherwise one scroll away in a block they have stopped looking at.
func hoverDeclaredFunction(doc *document, v *value, f fence, cursor int) *lsp.Hover {
	if insideStringLiteral(f.source, cursor) {
		return nil
	}
	segments, _, ok := segmentAt(f.source, cursor)
	if !ok || len(segments) < 1 || len(segments) > 2 {
		return nil
	}

	// A module's function is called through its alias, `ids.isUuid(x)`: the two
	// segments are one name, and the cursor on either describes it.
	name, first, last := segments[0].text, segments[0], segments[len(segments)-1]
	if len(segments) == 2 {
		name += "." + segments[1].text
	}
	if !strings.HasPrefix(f.source[last.end:], "(") {
		return nil
	}

	wf := compiledWorkflow(doc)
	i := slices.IndexFunc(wf.GetDeclaredFunctions(), func(d *v1.FunctionDeclaration) bool { return d.GetName() == name })
	if i < 0 {
		return nil
	}

	return markdownHover(declaredFunctionDoc(wf.GetDeclaredFunctions()[i]),
		v.fenceSpanOrWhole(doc.index, f, first.start, last.end))
}

// declaredFunctionDoc is the hover text for a declared function.
func declaredFunctionDoc(d *v1.FunctionDeclaration) string {
	var b strings.Builder
	if alias, _, carried := v1.SplitQualified(d.GetName()); carried {
		fmt.Fprintf(&b, "**`%s`** — a function declared by the module `%s`.", declaredFunctionSignature(d), alias)
	} else {
		fmt.Fprintf(&b, "**`%s`** — a function declared in this file.", declaredFunctionSignature(d))
	}
	if d.Description != nil {
		fmt.Fprintf(&b, "\n\n%s", d.GetDescription())
	}
	b.WriteString("\n\nA call is replaced by the body when the file compiles, so a run executes plain CEL.")

	return b.String()
}

// declaredFunctionCandidates offers the file's own `functions:` wherever an
// expression is written, with the signature as the detail and the description as
// the prose, which is what hover says about the same name.
//
// Skipped without compiling when the file has no `functions:` key at the top
// level, which is nearly every file: completion runs on every keystroke, and
// compiling a document to learn there is nothing to offer is the cost this check
// exists to avoid.
func declaredFunctionCandidates(doc *document, pos lsp.Position) []celcomplete.Candidate {
	if !declaresKey(doc.text, "functions") && !declaresKey(doc.text, "use") {
		return nil
	}

	wf := compiledWorkflow(doc)
	if wf == nil {
		// An expression being typed rarely compiles, and a document that does not
		// compile declares nothing, so the fence under the cursor is compiled as
		// `null`: the declarations are the part of the file the author is not editing.
		wf = compiledText(doc, withoutFenceAt(doc, pos))
	}
	out := make([]celcomplete.Candidate, 0, len(wf.GetDeclaredFunctions()))
	for _, d := range wf.GetDeclaredFunctions() {
		alias, bare, carried := v1.SplitQualified(d.GetName())
		candidate := celcomplete.Candidate{
			Name:   cmp.Or(bare, d.GetName()),
			Kind:   celcomplete.KindFunction,
			Detail: declaredFunctionSignature(d),
			Docs:   declaredFunctionDoc(d),
		}
		if !carried {
			out = append(out, candidate)

			continue
		}

		// A module's functions are written through its alias, so they are offered
		// after the alias and its dot, the way a library's are after `math.`.
		i := slices.IndexFunc(out, func(c celcomplete.Candidate) bool {
			return c.Kind == celcomplete.KindNamespace && c.Name == alias
		})
		if i < 0 {
			out = append(out, celcomplete.Candidate{
				Name:   alias,
				Detail: "module",
				Docs:   "A module this file uses. Its functions are written " + alias + ".<name>(...); type the dot to see them.",
				Insert: alias + ".",
				Kind:   celcomplete.KindNamespace,
			})
			i = len(out) - 1
		}
		out[i].Members = append(out[i].Members, candidate)
	}

	return out
}

// withoutFenceAt is the document text with the `${...}` around pos replaced by
// `${null}`, or the text unchanged when pos is not inside one on its line.
func withoutFenceAt(doc *document, pos lsp.Position) string {
	at := doc.index.offsetOfPosition(pos)
	if at < 0 || at > len(doc.text) {
		return doc.text
	}
	open := strings.LastIndex(doc.text[:at], "${")
	if open < 0 || strings.Contains(doc.text[open:at], "\n") {
		return doc.text
	}
	end := len(doc.text)
	if i := strings.IndexAny(doc.text[at:], "}\n"); i >= 0 {
		end = at + i
		if doc.text[end] == '}' {
			end++
		}
	}

	return doc.text[:open] + "${null}" + doc.text[end:]
}

// declaresKey reports whether text has key as a top-level key, without parsing it.
func declaresKey(text, key string) bool {
	return strings.HasPrefix(text, key+":") || strings.Contains(text, "\n"+key+":")
}

// declaredFunctionSignature is `name(param: type, ...) -> type`.
func declaredFunctionSignature(d *v1.FunctionDeclaration) string {
	params := make([]string, 0, len(d.GetParameters()))
	for _, p := range d.GetParameters() {
		params = append(params, p.GetName()+": "+v1.TypeString(p.GetType()))
	}

	return fmt.Sprintf("%s(%s) -> %s", d.GetName(), strings.Join(params, ", "), v1.TypeString(d.GetResult()))
}
