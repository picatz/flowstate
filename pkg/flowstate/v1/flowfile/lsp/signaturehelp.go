package lsp

import (
	"slices"
	"strings"

	v1 "github.com/picatz/flowstate/pkg/flowstate/v1"
	"github.com/sourcegraph/go-lsp"
)

// Signature help answers "what does this call want next?" while the author is
// still inside the parentheses. Hover answers the same question for a name
// already written; this is the half that matters mid-edit, when the call is not
// finished and the argument order is exactly what has been forgotten.
//
// The answer is read from the same two places hover reads: the profile's
// functions and the file's own `functions:`. A macro has no signature (its call
// form is an example, not an overload list), so it answers nothing rather than a
// guess.

// signatureHelpAt returns the call form for the innermost call enclosing pos,
// with the argument the cursor is in marked, or nil when pos is not inside the
// arguments of a function this server can describe.
func signatureHelpAt(doc *document, pos lsp.Position) *lsp.SignatureHelp {
	pos = clampPosition(pos)

	if doc.isTestDocument() || !doc.speaksFlowfile() || doc.parsed == nil {
		return nil
	}

	var help *lsp.SignatureHelp
	visit := func(_ *parsedStep, _ loopScope, v *value) {
		if help != nil {
			return
		}
		f, cursor, ok := v.fenceAt(doc.index, pos)
		if !ok {
			return
		}
		call, ok := enclosingCall(f.source, cursor)
		if !ok {
			return
		}
		help = signatureHelpFor(doc, f, call)
	}

	forEachExpression(doc, visit)

	// A declared function's `body:` is the one position forEachExpression does
	// not model, because nothing there resolves a step reference.
	for _, e := range doc.parsed.entries {
		if e.key == "functions" {
			walkValues(e.value, func(v *value) { visit(nil, loopScopeNone, v) })
		}
	}

	return help
}

// call is a function call the cursor is inside the arguments of.
type call struct {
	// name is the dotted word written before the opening parenthesis: `math.abs`
	// for a namespaced function, `xs.join` for a member call.
	name string

	// arg is the zero-based index of the argument the cursor is in.
	arg int
}

// enclosingCall finds the innermost call whose parentheses are open at cursor.
//
// Lexical, like [functionAt], for the same reason: the author is mid-edit, so the
// expression usually does not parse. Strings and `//` comments are skipped so a
// parenthesis or comma inside text does not count, and only brackets of the same
// call separate its arguments — a comma inside `[1, 2]` or a nested call belongs
// to that inner construct.
func enclosingCall(src string, cursor int) (call, bool) {
	if cursor < 0 || cursor > len(src) {
		return call{}, false
	}

	type frame struct {
		open  int  // offset of the bracket
		isArg bool // a call's parenthesis, as opposed to a grouping or literal bracket
		arg   int  // commas seen at this level
	}
	var stack []frame

	for i := 0; i < cursor; {
		switch c := src[i]; {
		case c == '"' || c == '\'':
			raw := i > 0 && (src[i-1] == 'r' || src[i-1] == 'R')
			end := stringEnd(src, i, raw)
			closed := stringClosed(src, i, end, raw)
			if end > cursor || (end == cursor && !closed) {
				return call{}, false // the cursor is inside a string
			}
			i = end
		case c == '/' && strings.HasPrefix(src[i:], "//"):
			nl := strings.IndexByte(src[i:], '\n')
			if nl < 0 || i+nl >= cursor {
				return call{}, false // the cursor is inside a comment
			}
			i += nl + 1
		case c == '(' || c == '[' || c == '{':
			stack = append(stack, frame{open: i, isArg: c == '(' && calleeBefore(src, i) != ""})
			i++
		case c == ')' || c == ']' || c == '}':
			if len(stack) > 0 {
				stack = stack[:len(stack)-1]
			}
			i++
		case c == ',':
			if len(stack) > 0 {
				stack[len(stack)-1].arg++
			}
			i++
		default:
			i++
		}
	}

	// The innermost *call*, not the innermost bracket: the cursor in `f([1, 2` is
	// still in f's first argument, and a grouping `(a + b)` belongs to whatever
	// call encloses it. Commas counted in a literal bracket were counted on that
	// bracket's own frame, so the call's argument index is its own frame's.
	for _, fr := range slices.Backward(stack) { // list and map literals are skipped outward
		if fr.isArg {
			return call{name: calleeBefore(src, fr.open), arg: fr.arg}, true
		}
	}

	return call{}, false
}

// stringClosed reports whether the literal opening at i ends at end with its own
// closing quotes, as opposed to the end of the source or of the line, which is
// where [stringEnd] stops for one that was never closed.
func stringClosed(src string, i, end int, raw bool) bool {
	q := src[i]
	quotes := string([]byte{q})
	opening := 1
	if strings.HasPrefix(src[i:], strings.Repeat(quotes, 3)) {
		quotes, opening = strings.Repeat(quotes, 3), 3
	}
	if end-i < 2*opening || !strings.HasSuffix(src[:end], quotes) {
		return false
	}
	if raw {
		return true
	}
	// An escaped final quote does not close it.
	backslashes := 0
	for j := end - len(quotes) - 1; j > i && src[j] == '\\'; j-- {
		backslashes++
	}

	return backslashes%2 == 0
}

// calleeBefore returns the dotted identifier ending just before src[at], skipping
// spaces. It is empty when what precedes is not a name — a grouping parenthesis
// after an operator, say.
func calleeBefore(src string, at int) string {
	end := at
	for end > 0 && (src[end-1] == ' ' || src[end-1] == '\t') {
		end--
	}
	start := end
	for start > 0 && isWordByte(src[start-1]) {
		start--
	}
	word := strings.Trim(src[start:end], ".")
	if word == "" || (word[0] >= '0' && word[0] <= '9') {
		return ""
	}

	return word
}

func isWordByte(c byte) bool {
	return c == '_' || c == '.' ||
		(c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z') || (c >= '0' && c <= '9')
}

// signatureHelpFor describes the function a call names, or nil.
func signatureHelpFor(doc *document, f fence, c call) *lsp.SignatureHelp {
	var (
		name       string
		signatures []string
		docs       string
	)

	index := functionIndex()
	last := c.name[strings.LastIndexByte(c.name, '.')+1:]

	switch fn, ok := lookupFunction(index.byName, c.name, last); {
	case ok:
		if fn.Macro || len(fn.Signature) == 0 {
			return nil
		}
		name, signatures, docs = fn.Name, fn.Signature, fn.Description
	default:
		if strings.Contains(c.name, ".") {
			return nil // a declared function is never namespaced
		}
		d, ok := declaredFunction(doc, f, c.name)
		if !ok {
			return nil
		}
		name, signatures, docs = d.GetName(), []string{declaredFunctionSignature(d)}, d.GetDescription()
	}

	out := &lsp.SignatureHelp{ActiveParameter: c.arg}
	active := -1
	for i, sig := range signatures {
		info := lsp.SignatureInformation{Label: sig, Documentation: docs}
		for _, p := range signatureParameters(sig, name) {
			info.Parameters = append(info.Parameters, lsp.ParameterInformation{Label: p})
		}
		out.Signatures = append(out.Signatures, info)
		// The first overload that has an argument where the cursor is: with the
		// cursor on the third argument, an overload taking two cannot be the one
		// being written.
		if active < 0 && c.arg < len(info.Parameters) {
			active = i
		}
	}
	out.ActiveSignature = max(active, 0)

	return out
}

// lookupFunction resolves the word before a call to a profile function. The full
// dotted word wins, so `regex.replace(` is not read as the strings library's
// `replace`; failing that the last segment names a member call on a value.
func lookupFunction(byName map[string]v1.LibraryFunction, word, last string) (v1.LibraryFunction, bool) {
	if fn, ok := byName[word]; ok {
		return fn, true
	}
	if word != last {
		fn, ok := byName[last]
		return fn, ok
	}

	return v1.LibraryFunction{}, false
}

// declaredFunction finds a function the file's own `functions:` declares.
//
// A call being typed is usually a syntax error, and a document that does not
// compile declares nothing, so the fence under the cursor is compiled as `null`:
// the declarations are the part of the file the author is not editing.
func declaredFunction(doc *document, f fence, name string) (*v1.FunctionDeclaration, bool) {
	if !slices.ContainsFunc(doc.parsed.entries, func(e *entry) bool { return e.key == "functions" }) {
		return nil, false
	}

	wf := compiledWorkflow(doc)
	if wf == nil {
		start, end := doc.index.offsetOfPosition(f.rng.Start), doc.index.offsetOfPosition(f.rng.End)
		if start < 0 || end < start || end > len(doc.text) {
			return nil, false
		}
		wf = compiledText(doc, doc.text[:start]+"${null}"+doc.text[end:])
	}

	decls := wf.GetDeclaredFunctions()
	i := slices.IndexFunc(decls, func(d *v1.FunctionDeclaration) bool { return d.GetName() == name })
	if i < 0 {
		return nil, false
	}

	return decls[i], true
}

// signatureParameters splits the argument list of a signature string into one
// label per parameter. Signatures read `string.replace(string, string) -> string`
// or `name(a: int) -> int`; the receiver of a member function can itself contain
// parentheses (`list(string).join()`), so the list is located by the function's
// own name rather than the first parenthesis.
func signatureParameters(sig, name string) []string {
	last := name[strings.LastIndexByte(name, '.')+1:]

	open := strings.Index(sig, "."+last+"(")
	if open >= 0 {
		open += len(last) + 1
	} else if open = strings.Index(sig, last+"("); open >= 0 {
		open += len(last)
	} else {
		return nil
	}

	var (
		params []string
		depth  int
		from   = open + 1
	)
	for i := open; i < len(sig); i++ {
		switch sig[i] {
		case '(', '[', '{', '<':
			depth++
		case ']', '}', '>':
			depth--
		case ')':
			depth--
			if depth == 0 {
				if p := strings.TrimSpace(sig[from:i]); p != "" {
					params = append(params, p)
				}
				return params
			}
		case ',':
			if depth == 1 {
				params = append(params, strings.TrimSpace(sig[from:i]))
				from = i + 1
			}
		}
	}

	return nil
}
