package lsp

import (
	"cmp"
	"slices"
	"strings"

	v1 "github.com/picatz/flowstate/pkg/flowstate/v1"
	"github.com/sourcegraph/go-lsp"
)

// Semantic tokens colour the CEL inside a Flowfile.
//
// A Flowfile is YAML, so an editor's own grammar paints every `${...}` as one
// string-coloured run (#1874). This server already knows where each expression
// starts and ends — the fence rule is the compiler's — so it says what each part
// of one is, once, and every editor that speaks LSP shows the same thing.
//
// The pass is lexical on purpose. An expression is being typed more often than it
// is finished, and a parse that refuses `inputs.` would paint nothing exactly when
// the author is looking at it. What a name *is* — a root, a function, a member —
// follows from the characters around it, so a half-written expression still gets
// the colours of the part that is written.

// The legend. The order is the wire contract: a token carries an index into
// [semanticTokenTypes], and a client reads it against the legend this server
// advertised. Standard LSP names only, so a theme that knows nothing about
// Flowstate still colours them.
const (
	tokVariable = iota
	tokProperty
	tokFunction
	tokMethod
	tokKeyword
	tokString
	tokNumber
	tokOperator
	tokComment
)

var semanticTokenTypes = []string{
	tokVariable: "variable",
	tokProperty: "property",
	tokFunction: "function",
	tokMethod:   "method",
	tokKeyword:  "keyword",
	tokString:   "string",
	tokNumber:   "number",
	tokOperator: "operator",
	tokComment:  "comment",
}

// modDefaultLibrary is the bit for the one modifier in the legend: a name the
// engine itself binds, which is what distinguishes `inputs` from an iterator the
// author named.
const modDefaultLibrary = 1 << 0

var semanticTokenModifiers = []string{"defaultLibrary"}

// engineRoots are the names an expression can read that the engine, rather than
// the author, binds. `item`, a loop's `as:` name and the like are bindings an
// author chose and stay plain variables.
var engineRoots = map[string]bool{
	v1.InputsRoot:   true,
	v1.VarsRoot:     true,
	v1.StepsRoot:    true,
	v1.RunRoot:      true,
	v1.EventRoot:    true,
	v1.TriggerRoot:  true,
	v1.ResponseRoot: true,
	"this":          true,
	"now":           true,
	"sender":        true,
}

// bareCELKeys are the keys whose scalar is CEL written without a fence, because
// the compiler reads it as a literal expression ([docs/LANGUAGE.md] lists them).
var bareCELKeys = map[string]bool{"must": true}

// declarationRoots are the top-level keys whose subtree declares typed values,
// and dataKeys the keys inside one whose value is data rather than declaration.
var (
	declarationRoots = map[string]bool{"inputs": true, "outputs": true, "types": true}
	dataKeys         = map[string]bool{"default": true, "example": true, "values": true}
)

// semanticTokensProvider is the `semanticTokensProvider` capability: the legend,
// and full-document answers only. There is no range or delta form — the answer is
// bounded by the document, which is bounded by [maxDocumentBytes], and a stale
// delta would be a wrong colour under working code.
type semanticTokensProvider struct {
	Legend semanticTokensLegend `json:"legend"`
	Full   bool                 `json:"full"`
}

type semanticTokensLegend struct {
	TokenTypes     []string `json:"tokenTypes"`
	TokenModifiers []string `json:"tokenModifiers"`
}

// semanticTokensResult is the `textDocument/semanticTokens/full` answer.
type semanticTokensResult struct {
	Data []uint32 `json:"data"`
}

// semanticTokensParams carries the one thing the request asks.
type semanticTokensParams struct {
	TextDocument lsp.TextDocumentIdentifier `json:"textDocument"`
}

// celToken is one lexed piece of an expression, as byte offsets into its source.
type celToken struct {
	start, end int
	kind       int
	mods       uint32
}

// lexCEL splits src into the pieces worth colouring. It never fails: whatever it
// does not recognise is left uncoloured, and an unterminated string runs to the
// end of the source so a quote being typed is already one.
func lexCEL(src string) []celToken {
	var out []celToken
	afterDot := false // the previous significant token was a member selection

	// A comprehension macro binds its first argument for the length of its call,
	// and the binding shadows an engine root: `[1].exists(inputs, inputs > 0)` is
	// valid, and both names there are the author's. depth counts open
	// parentheses, and each binding records the depth it was opened at.
	type binding struct {
		name  string
		depth int
	}
	var bound []binding
	depth := 0
	shadowed := func(name string) bool {
		return slices.ContainsFunc(bound, func(b binding) bool { return b.name == name })
	}

	for i := 0; i < len(src); {
		c := src[i]
		switch {
		case c == ' ' || c == '\t' || c == '\n' || c == '\r':
			i++

		case c == '/' && strings.HasPrefix(src[i:], "//"):
			end := len(src)
			if nl := strings.IndexByte(src[i:], '\n'); nl >= 0 {
				end = i + nl
			}
			out = append(out, celToken{i, end, tokComment, 0})
			i = end

		case isIdentStart(c):
			j := i + 1
			for j < len(src) && isIdentPart(src[j]) {
				j++
			}
			// A string prefix: r, b, rb, br in either case, directly before a quote.
			if j < len(src) && (src[j] == '"' || src[j] == '\'') && isStringPrefix(src[i:j]) {
				end := stringEnd(src, j, strings.ContainsAny(src[i:j], "rR"))
				out = append(out, celToken{i, end, tokString, 0})
				i, afterDot = end, false
				continue
			}
			word := src[i:j]
			call := j < len(src) && nextSignificant(src, j) == '('
			tok := celToken{start: i, end: j}
			switch {
			case afterDot && call:
				tok.kind = tokMethod
				if bindsFirstArgument[word] {
					// Past maxBindings an unclosed macro stops shadowing: every
					// root is checked against the whole stack, so an uncapped one
					// makes a document of repeated `.all(x,` quadratic to lex.
					if name, ok := firstArgument(src, j); ok && len(bound) < maxBindings {
						// The parenthesis that opens the call is not counted yet:
						// the binding lives one level deeper than it.
						bound = append(bound, binding{name, depth + 1})
					}
				}
			case afterDot:
				tok.kind = tokProperty
			case call:
				tok.kind = tokFunction
			case word == "true" || word == "false" || word == "null" || word == "in":
				tok.kind = tokKeyword
			default:
				tok.kind = tokVariable
				if engineRoots[word] && !shadowed(word) {
					tok.mods = modDefaultLibrary
				}
			}
			out = append(out, tok)
			i, afterDot = j, false

		case c == '"' || c == '\'':
			end := stringEnd(src, i, false)
			out = append(out, celToken{i, end, tokString, 0})
			i, afterDot = end, false

		case isDigit(c) || (c == '.' && i+1 < len(src) && isDigit(src[i+1]) && !afterDot):
			end := numberEnd(src, i)
			out = append(out, celToken{i, end, tokNumber, 0})
			i, afterDot = end, false

		case c == '.':
			afterDot = true
			i++
			// `.?` is the optional field selection: one operator, then a member.
			if i < len(src) && src[i] == '?' {
				out = append(out, celToken{i - 1, i + 1, tokOperator, 0})
				i++
			}

		case strings.IndexByte("+-*/%!<>=?:&|", c) >= 0:
			j := i + 1
			if j < len(src) && slices.Contains(twoByteOperators, src[i:j+1]) {
				j++
			}
			out = append(out, celToken{i, j, tokOperator, 0})
			i, afterDot = j, false

		default:
			// A bracket, comma or something the lexer does not know: not a name,
			// so a selection opened before it selects nothing.
			switch c {
			case '(':
				depth++
			case ')':
				for len(bound) > 0 && bound[len(bound)-1].depth >= depth {
					bound = bound[:len(bound)-1]
				}
				depth = max(depth-1, 0)
			}
			i++
			afterDot = false
		}
	}

	return out
}

// maxBindings bounds how many comprehension variables lexCEL tracks at once. A
// real expression nests a handful of macros; the cap only exists so an
// author-controlled document cannot make the shadowing check quadratic.
const maxBindings = 64

var twoByteOperators = []string{"&&", "||", "==", "!=", "<=", ">="}

// bindsFirstArgument are the macros whose first argument names a variable bound
// in the rest of the call.
var bindsFirstArgument = map[string]bool{
	"all": true, "exists": true, "exists_one": true, "existsOne": true,
	"map": true, "filter": true, "bind": true,
	"transformList": true, "transformMap": true, "transformMapEntry": true,
}

// firstArgument returns the identifier that opens the argument list starting at
// the parenthesis at or after i, when one is followed by a comma.
func firstArgument(src string, i int) (string, bool) {
	for ; i < len(src) && src[i] != '('; i++ {
		if src[i] != ' ' && src[i] != '\t' && src[i] != '\n' {
			return "", false
		}
	}
	i++
	for i < len(src) && (src[i] == ' ' || src[i] == '\t' || src[i] == '\n') {
		i++
	}
	start := i
	if i >= len(src) || !isIdentStart(src[i]) {
		return "", false
	}
	for i < len(src) && isIdentPart(src[i]) {
		i++
	}
	name := src[start:i]
	if nextSignificant(src, i) != ',' {
		return "", false
	}
	return name, true
}

func isIdentStart(c byte) bool {
	return c == '_' || (c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z')
}
func isIdentPart(c byte) bool { return isIdentStart(c) || isDigit(c) }
func isDigit(c byte) bool     { return c >= '0' && c <= '9' }

func isStringPrefix(s string) bool {
	switch strings.ToLower(s) {
	case "r", "b", "rb", "br":
		return true
	}
	return false
}

// nextSignificant returns the first byte at or after i that is not whitespace, or
// zero at the end of the source.
func nextSignificant(src string, i int) byte {
	for ; i < len(src); i++ {
		switch src[i] {
		case ' ', '\t', '\n', '\r':
		default:
			return src[i]
		}
	}
	return 0
}

// stringEnd returns the offset just past the string literal opening at i, which
// is the end of the source when it is never closed. A triple-quoted string closes
// only on its own three quotes. A backslash escapes the next byte except in a raw
// string, where it is an ordinary character: `r"\"` is a complete literal.
func stringEnd(src string, i int, raw bool) int {
	q := src[i]
	triple := strings.HasPrefix(src[i:], string([]byte{q, q, q}))
	j := i + 1
	if triple {
		j = i + 3
	}
	for j < len(src) {
		switch {
		case src[j] == '\\' && !raw:
			j += 2
		case triple && strings.HasPrefix(src[j:], string([]byte{q, q, q})):
			return j + 3
		case !triple && src[j] == q:
			return j + 1
		case !triple && src[j] == '\n':
			return j // an unclosed single-line string ends with its line
		default:
			j++
		}
	}
	return len(src)
}

// numberEnd returns the offset just past the numeric literal starting at i.
func numberEnd(src string, i int) int {
	j := i
	if strings.HasPrefix(src[j:], "0x") || strings.HasPrefix(src[j:], "0X") {
		j += 2
		for j < len(src) && (isDigit(src[j]) || strings.IndexByte("abcdefABCDEF", src[j]) >= 0) {
			j++
		}
	} else {
		for j < len(src) && isDigit(src[j]) {
			j++
		}
		if j+1 < len(src) && src[j] == '.' && isDigit(src[j+1]) {
			j++
			for j < len(src) && isDigit(src[j]) {
				j++
			}
		} else if j < len(src) && src[j] == '.' && j == i {
			// A leading dot: `.5`.
			j++
			for j < len(src) && isDigit(src[j]) {
				j++
			}
		}
		if j < len(src) && (src[j] == 'e' || src[j] == 'E') {
			k := j + 1
			if k < len(src) && (src[k] == '+' || src[k] == '-') {
				k++
			}
			if k < len(src) && isDigit(src[k]) {
				for k < len(src) && isDigit(src[k]) {
					k++
				}
				j = k
			}
		}
	}
	if j < len(src) && (src[j] == 'u' || src[j] == 'U') {
		j++
	}
	return j
}

// An absoluteToken is a token placed in the document, before delta encoding.
type absoluteToken struct {
	line, char, length int
	kind               int
	mods               uint32
}

// semanticTokens answers `textDocument/semanticTokens/full`.
//
// A document that is not a workflow, does not parse, or holds an expression the
// position mapping cannot place gets fewer tokens, never wrong ones: an
// expression whose bytes cannot be tied to document positions (a folded block
// scalar, whose line breaks were rewritten) is left to the editor's own grammar.
func semanticTokens(doc *document) semanticTokensResult {
	empty := semanticTokensResult{Data: []uint32{}}
	if doc.isTestDocument() || !doc.speaksFlowfile() || doc.parsed == nil {
		return empty
	}

	var tokens []absoluteToken
	place := func(src string, rng spanMapper) {
		for _, t := range lexCEL(src) {
			r, ok := rng(t.start, t.end)
			if !ok || r.Start.Line != r.End.Line || r.End.Character <= r.Start.Character {
				continue
			}
			tokens = append(tokens, absoluteToken{
				line: r.Start.Line, char: r.Start.Character,
				length: r.End.Character - r.Start.Character,
				kind:   t.kind, mods: t.mods,
			})
		}
	}

	// decl is true beneath the three places a `must:` is a declaration's
	// predicate. Anywhere else the key is a name a map happens to use — a var
	// called `must`, a key in a task input — and its value is as literal as the
	// compiler reads it.
	var walk func(v *value, key string, decl bool)
	walk = func(v *value, key string, decl bool) {
		if v == nil {
			return
		}
		switch v.kind {
		case kindScalar:
			if decl && bareCELKeys[key] && len(v.fences) == 0 && v.text != "" {
				place(v.text, v.textMapper(doc.index))
			}
			for _, f := range v.fences {
				place(f.source, v.fenceMapper(doc.index, f))
			}
		case kindSequence:
			for _, item := range v.items {
				walk(item, "", decl)
			}
		case kindMapping:
			for _, e := range v.entries {
				// A declaration's own values are data: an input's default or
				// example may be a map with a key called `must`.
				walk(e.value, e.key, decl && !dataKeys[e.key])
			}
		}
	}
	for _, e := range doc.parsed.entries {
		walk(e.value, e.key, declarationRoots[e.key])
	}
	if len(tokens) == 0 {
		return empty
	}

	slices.SortFunc(tokens, func(a, b absoluteToken) int {
		return cmp.Or(cmp.Compare(a.line, b.line), cmp.Compare(a.char, b.char))
	})

	data := make([]uint32, 0, len(tokens)*5)
	prevLine, prevChar := 0, 0
	for _, t := range tokens {
		dLine := t.line - prevLine
		dChar := t.char
		if dLine == 0 {
			dChar = t.char - prevChar
			if dChar < 0 { // an overlap; the earlier token wins
				continue
			}
		}
		data = append(data, uint32(dLine), uint32(dChar), uint32(t.length), uint32(t.kind), t.mods)
		prevLine, prevChar = t.line, t.char
	}

	return semanticTokensResult{Data: data}
}
