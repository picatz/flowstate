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
