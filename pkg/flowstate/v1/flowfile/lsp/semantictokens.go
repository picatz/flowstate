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
				end := stringEnd(src, j)
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
			case afterDot:
				tok.kind = tokProperty
			case call:
				tok.kind = tokFunction
			case word == "true" || word == "false" || word == "null" || word == "in":
				tok.kind = tokKeyword
			default:
				tok.kind = tokVariable
				if engineRoots[word] {
					tok.mods = modDefaultLibrary
				}
			}
			out = append(out, tok)
			i, afterDot = j, false

		case c == '"' || c == '\'':
			end := stringEnd(src, i)
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
			i++
			afterDot = false
		}
	}

	return out
}

var twoByteOperators = []string{"&&", "||", "==", "!=", "<=", ">="}

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
// only on its own three quotes; a raw string's backslash escapes nothing, but
// treating it as an escape here only ever shortens a string that is already
// malformed in the way a raw one would not be, so one rule serves both.
func stringEnd(src string, i int) int {
	q := src[i]
	triple := strings.HasPrefix(src[i:], string([]byte{q, q, q}))
	j := i + 1
	if triple {
		j = i + 3
	}
	for j < len(src) {
		switch {
		case src[j] == '\\':
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

	var walk func(v *value, key string)
	walk = func(v *value, key string) {
		if v == nil {
			return
		}
		switch v.kind {
		case kindScalar:
			if bareCELKeys[key] && len(v.fences) == 0 && v.text != "" {
				place(v.text, v.textMapper(doc.index))
			}
			for _, f := range v.fences {
				place(f.source, v.fenceMapper(doc.index, f))
			}
		case kindSequence:
			for _, item := range v.items {
				walk(item, "")
			}
		case kindMapping:
			for _, e := range v.entries {
				walk(e.value, e.key)
			}
		}
	}
	for _, e := range doc.parsed.entries {
		walk(e.value, e.key)
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
