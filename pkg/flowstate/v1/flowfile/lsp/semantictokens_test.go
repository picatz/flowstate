package lsp

import (
	"fmt"
	"strings"
	"testing"
	"unicode/utf16"

	"github.com/sourcegraph/go-lsp"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// lexed renders what the lexer found as `text:kind` pairs, which reads as the
// claim being made.
func lexed(src string) []string {
	var out []string
	for _, t := range lexCEL(src) {
		out = append(out, fmt.Sprintf("%s:%s", src[t.start:t.end], semanticTokenTypes[t.kind]))
	}
	return out
}

func TestLexCELClassifiesByRole(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		src  string
		want []string
	}{
		{"a root", "inputs.name", []string{"inputs:variable", "name:property"}},
		{"a call and a method", "size(x) + x.trim()", []string{
			"size:function", "x:variable", "+:operator", "x:variable", "trim:method"}},
		{"keywords", "true && x in [null]", []string{
			"true:keyword", "&&:operator", "x:variable", "in:keyword", "null:keyword"}},
		{"strings and numbers", `"a" + 'b' + 1.5e3 + 0xff + 7u`, []string{
			`"a":string`, "+:operator", "'b':string", "+:operator",
			"1.5e3:number", "+:operator", "0xff:number", "+:operator", "7u:number"}},
		{"raw and bytes prefixes", `r'^v?\d+$' + b"x"`, []string{
			`r'^v?\d+$':string`, "+:operator", `b"x":string`}},
		{"an escaped quote stays inside", `"a\"b" + c`, []string{`"a\"b":string`, "+:operator", "c:variable"}},
		{"optional selection", "a.?b", []string{"a:variable", ".?:operator", "b:property"}},
		{"a number then a member is not a float", "1.size()", []string{"1:number", "size:method"}},
		{"a comment", "x // why\n+ y", []string{"x:variable", "// why:comment", "+:operator", "y:variable"}},
		{"a quote being typed is already a string", `inputs.a == "ab`, []string{
			"inputs:variable", "a:property", "==:operator", `"ab:string`}},
		{"a raw string ends at its quote even after a backslash", `r"\" + x`, []string{
			`r"\":string`, "+:operator", "x:variable"}},
		{"a cooked string skips an escaped backslash pair", `"\\" + x`, []string{
			`"\\":string`, "+:operator", "x:variable"}},
		{"nothing", "", nil},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			assert.Equal(t, tt.want, lexed(tt.src))
		})
	}
}

func TestLexCELMarksOnlyEngineRootsAsDefaultLibrary(t *testing.T) {
	t.Parallel()

	got := lexCEL("inputs + item")
	require.Len(t, got, 3)
	assert.Equal(t, uint32(modDefaultLibrary), got[0].mods)
	assert.Zero(t, got[2].mods)
}

// decodeTokens turns the delta-encoded answer back into `line:char text kind`
// strings against the document, reading columns as UTF-16 the way a client does.
func decodeTokens(t *testing.T, text string, data []uint32) []string {
	t.Helper()
	require.Zero(t, len(data)%5)
	lines := strings.Split(text, "\n")
	var out []string
	line, char := 0, 0
	for i := 0; i < len(data); i += 5 {
		if data[i] != 0 {
			line += int(data[i])
			char = 0
		}
		char += int(data[i+1])
		units := utf16.Encode([]rune(lines[line]))
		require.LessOrEqual(t, char+int(data[i+2]), len(units), "token runs past its line")
		word := string(utf16.Decode(units[char : char+int(data[i+2])]))
		kind := semanticTokenTypes[data[i+3]]
		if data[i+4]&modDefaultLibrary != 0 {
			kind += "+lib"
		}
		out = append(out, fmt.Sprintf("%d:%d %s %s", line, char, word, kind))
	}
	return out
}

func (c *client) semanticTokens(uri string) semanticTokensResult {
	c.t.Helper()
	var result semanticTokensResult
	require.NoError(c.t, c.conn.Call(c.t.Context(), "textDocument/semanticTokens/full", semanticTokensParams{
		TextDocument: lsp.TextDocumentIdentifier{URI: lsp.DocumentURI(uri)},
	}, &result))
	return result
}

func TestSemanticTokensColourFencesAndBareCEL(t *testing.T) {
	t.Parallel()

	const src = `name: tokens
inputs:
  n:
    type: int
    must: this > 0
steps:
  - id: say
    log:
      message: "héllo ${inputs.n} and ${size(steps.say)}"
`
	c := newClient(t)
	c.initialize()
	c.open("file:///tokens.yaml", src)

	got := decodeTokens(t, src, c.semanticTokens("file:///tokens.yaml").Data)
	assert.Equal(t, []string{
		"4:10 this variable+lib",
		"4:15 > operator",
		"4:17 0 number",
		// "é" is two bytes and one UTF-16 unit; columns are the latter.
		"8:24 inputs variable+lib",
		"8:31 n property",
		"8:40 size function",
		"8:45 steps variable+lib",
		"8:51 say property",
	}, got)
}

func TestSemanticTokensLeaveWhatTheyCannotPlaceToTheGrammar(t *testing.T) {
	t.Parallel()

	const folded = `name: folded
steps:
  - id: say
    log:
      message: >-
        one ${inputs.n}
        two
`
	c := newClient(t)
	c.initialize()
	c.open("file:///folded.yaml", folded)
	assert.Empty(t, c.semanticTokens("file:///folded.yaml").Data,
		"a folded scalar's line breaks were rewritten, so no byte of it is a document position")

	// A document that does not parse has no model to place anything against.
	c.open("file:///broken.yaml", "name: [unclosed\n")
	assert.Empty(t, c.semanticTokens("file:///broken.yaml").Data)

	// A test file speaks another language; none of this applies.
	c.open("file:///x.test.yaml", "tests:\n  - name: a\n    expect: ${steps.a}\n")
	assert.Empty(t, c.semanticTokens("file:///x.test.yaml").Data)

	// Not a document the server has been told about.
	assert.Empty(t, c.semanticTokens("file:///never-opened.yaml").Data)
}

func TestSemanticTokensKeepLiteralsLiteral(t *testing.T) {
	t.Parallel()

	// `must:` is bare CEL; `description:` that merely looks like one is prose.
	const src = `name: literal
inputs:
  n:
    type: string
    description: this > 0
    must: this != ""
steps:
  - id: say
    log:
      message: plain text, no fence
`
	c := newClient(t)
	c.initialize()
	c.open("file:///lit.yaml", src)
	assert.Equal(t, []string{
		"5:10 this variable+lib",
		"5:15 != operator",
		`5:18 "" string`,
	}, decodeTokens(t, src, c.semanticTokens("file:///lit.yaml").Data))
}

func TestLexCELLetsAComprehensionShadowARoot(t *testing.T) {
	t.Parallel()

	mods := func(src string) map[string][]uint32 {
		got := map[string][]uint32{}
		for _, tok := range lexCEL(src) {
			if tok.kind == tokVariable {
				name := src[tok.start:tok.end]
				got[name] = append(got[name], tok.mods)
			}
		}
		return got
	}

	// The receiver and the use after the call are the engine's; both names inside
	// the macro are the author's.
	got := mods("[inputs].exists(inputs, inputs == 1) && inputs.n > 0")
	assert.Equal(t, []uint32{modDefaultLibrary, 0, 0, modDefaultLibrary}, got["inputs"],
		"the binding shadows the root for the length of the call and no longer")

	// A binding does not leak across a sibling call, and a nested one is scoped.
	got = mods("xs.all(vars, vars > 1) && vars.a == 1 && ys.map(i, zs.filter(steps, steps > i))")
	assert.Equal(t, []uint32{0, 0, modDefaultLibrary}, got["vars"])
	assert.Equal(t, []uint32{0, 0}, got["steps"])

	// cel.bind binds its first argument too.
	got = mods("cel.bind(run, 1, run + 1)")
	assert.Equal(t, []uint32{0, 0}, got["run"])

	// A call that merely shares a macro's name but whose first argument is not
	// followed by a comma binds nothing.
	got = mods("xs.map(inputs)")
	assert.Equal(t, []uint32{modDefaultLibrary}, got["inputs"])
}

func TestSemanticTokensTakeMustOnlyFromDeclarations(t *testing.T) {
	t.Parallel()

	// `must:` is a predicate under inputs, outputs and types, and an ordinary key
	// everywhere else: a var, a task input, an input's default or example.
	const src = `name: where
inputs:
  n:
    type: int
    must: this > 0
    default: 1
  rec:
    type: map
    default:
      must: plain words
vars:
  must: plain text
steps:
  - id: say
    log:
      message: hi
      must: not an expression
`
	c := newClient(t)
	c.initialize()
	c.open("file:///where.yaml", src)
	assert.Equal(t, []string{
		"4:10 this variable+lib",
		"4:15 > operator",
		"4:17 0 number",
	}, decodeTokens(t, src, c.semanticTokens("file:///where.yaml").Data))
}

// TestLexCELBoundsTheBindingsItTracks pins the work limit on shadowing: every
// root is checked against the open bindings, so an author-controlled run of
// unclosed macros must not grow that stack without bound. Past maxBindings a
// further macro binds nothing, which is observable without timing anything.
func TestLexCELBoundsTheBindingsItTracks(t *testing.T) {
	t.Parallel()

	rootMods := func(src string) uint32 {
		var last uint32
		for _, tok := range lexCEL(src) {
			if tok.kind == tokVariable && src[tok.start:tok.end] == "inputs" {
				last = tok.mods
			}
		}
		return last
	}

	within := strings.Repeat("a.all(x, ", maxBindings-1) + "a.all(inputs, inputs)"
	assert.Zero(t, rootMods(within), "a binding inside the cap still shadows the root")

	past := strings.Repeat("a.all(x, ", maxBindings) + "a.all(inputs, inputs)"
	assert.Equal(t, uint32(modDefaultLibrary), rootMods(past),
		"a binding past the cap is not tracked, so the work stays bounded")
}
