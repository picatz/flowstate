package lsp

import (
	"strings"
	"testing"

	"github.com/sourcegraph/go-lsp"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// hoverSource is used by the hover tests. Positions are given as the text to point
// at rather than as coordinates, so the tests stay readable and stay correct when
// the source is edited.
const hoverSource = `name: hover
steps:
  - id: web
    task:
      name: http
      inputs:
        method: GET
        url: https://example.com
  - id: shout
    task:
      name: echo
      inputs:
        message: ${web.body}
  - id: parsed
    task:
      name: cel
      inputs:
        libs: [json]
        expr: json_parse(web.body)
`

func TestHover(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		// at is the source text to place the cursor on; the cursor goes on its
		// first character unless offset moves it.
		at     string
		offset int
		// want are substrings the hover content must contain.
		want []string
		// notWant are substrings it must not contain.
		notWant []string
		// none asserts that there is no hover at all.
		none bool
	}{
		{
			name: "task name shows summary and full signature",
			at:   "http\n",
			want: []string{
				"task `http`",
				"Perform an HTTP request",
				"url", "string", "(required)",
				"status_code", "int",
				"headers", "map[string, string]",
				// Derived from the task definition, not from a list here.
				"evaluates `outputs` itself",
				"${step.status_code}",
			},
		},
		{
			name: "required input shows its type and constraints",
			at:   "url:",
			want: []string{"`url`", "`string`", "required", "must be an absolute URI"},
		},
		{
			name: "optional input says so",
			at:   "method:",
			want: []string{"`method`", "`string`", "optional", "matches"},
		},
		{
			name: "expression reference names the producing step and output type",
			at:   "${web.body}",
			// The cursor sits on the `web` identifier inside the expression.
			offset: 2,
			want:   []string{"`web.body`", "`string`", "step `web`", "`http` task"},
		},
		{
			name:   "expression reference on the output name",
			at:     "${web.body}",
			offset: 6,
			want:   []string{"`web.body`", "`string`"},
		},
		{
			name: "cel library describes itself and what it provides",
			at:   "json]",
			want: []string{"CEL library `json`", "json_parse", "libs: [json]"},
		},
		{
			name: "step id summarizes the step",
			at:   "web\n",
			want: []string{"step `web`", "step 1", "`http` task", "${web.body}"},
		},
		{
			name: "nothing to say about a plain literal value",
			at:   "GET",
			none: true,
		},
		{
			name: "nothing to say about the steps key",
			at:   "steps:",
			none: true,
		},
	}

	c := newClient(t)
	c.initialize()
	const uri = "file:///hover.yaml"
	c.open(uri, hoverSource)

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			pos := positionOf(t, hoverSource, tt.at, tt.offset)
			got := c.hover(uri, pos.Line, pos.Character)

			if tt.none {
				assert.Nil(t, got, "expected no hover, got %q", hoverText(got))
				return
			}
			require.NotNil(t, got, "expected hover content at %v", pos)
			require.NotNil(t, got.Range, "hover must report the range it describes")
			text := hoverText(got)
			for _, want := range tt.want {
				assert.Contains(t, text, want)
			}
			for _, notWant := range tt.notWant {
				assert.NotContains(t, text, notWant)
			}
		})
	}
}

// TestHoverStaysQuietOnUnresolvableReference checks that hover says nothing about a
// reference the workflow cannot resolve. The diagnostics already explain it, and a
// popup describing a step that does not exist would contradict them.
func TestHoverStaysQuietOnUnresolvableReference(t *testing.T) {
	t.Parallel()

	const src = `name: quiet
steps:
  - id: a
    task:
      name: echo
      inputs:
        message: ${later.result}
  - id: later
    task:
      name: echo
      inputs:
        message: hi
`
	c := newClient(t)
	c.initialize()
	c.open("file:///quiet.yaml", src)

	pos := positionOf(t, src, "${later.result}", 3)
	assert.Nil(t, c.hover("file:///quiet.yaml", pos.Line, pos.Character))
}

// TestHoverOnQuestionableReferences checks what hover says about a reference that
// resolves to a step but not to anything that step produces. These are the cases
// where saying the wrong thing would actively mislead.
func TestHoverOnQuestionableReferences(t *testing.T) {
	t.Parallel()

	c := newClient(t)
	c.initialize()

	t.Run("an output the task does not declare", func(t *testing.T) {
		const src = `name: wrong-output
steps:
  - id: web
    task:
      name: http
      inputs:
        url: https://example.com
  - id: out
    task:
      name: echo
      inputs:
        message: ${web.stdout}
`
		c.open("file:///wrong-output.yaml", src)
		pos := positionOf(t, src, "${web.stdout}", 8)
		got := c.hover("file:///wrong-output.yaml", pos.Line, pos.Character)
		require.NotNil(t, got)
		// It names what the task does produce rather than inventing a type for
		// an output that does not exist.
		assert.Contains(t, hoverText(got), "does not declare an output named `stdout`")
		assert.Contains(t, hoverText(got), "status_code")
	})

	t.Run("a step whose task is not registered", func(t *testing.T) {
		const src = `name: unknown-task
steps:
  - id: mystery
    task:
      name: shell
      inputs:
        command: ls
  - id: out
    task:
      name: echo
      inputs:
        message: ${mystery.stdout}
`
		c.open("file:///unknown-producer.yaml", src)
		pos := positionOf(t, src, "${mystery.stdout}", 3)
		got := c.hover("file:///unknown-producer.yaml", pos.Line, pos.Character)
		require.NotNil(t, got)
		assert.Contains(t, hoverText(got), "whose task `shell` is not registered")
	})

	t.Run("a bare step reference with no output", func(t *testing.T) {
		const src = `name: bare
steps:
  - id: web
    task:
      name: http
      inputs:
        url: https://example.com
  - id: out
    task:
      name: cel
      inputs:
        expr: "1"
        vars:
          v: ${web}
`
		c.open("file:///bare.yaml", src)
		pos := positionOf(t, src, "${web}", 3)
		got := c.hover("file:///bare.yaml", pos.Line, pos.Character)
		require.NotNil(t, got)
		assert.Contains(t, hoverText(got), "step 1")
		// A bare reference lists what is available rather than guessing.
		assert.Contains(t, hoverText(got), "Outputs:")
		assert.Contains(t, hoverText(got), "body")
	})
}

// TestHoverOnSecretReferences covers the one thing an author cannot see from the
// text: what a ${secret('scheme:name')} marker actually names, and why it may only
// be a whole task input.
func TestHoverOnSecretReferences(t *testing.T) {
	t.Parallel()

	c := newClient(t)
	c.initialize()

	t.Run("a valid reference", func(t *testing.T) {
		const src = `name: secrets
steps:
  - id: a
    task:
      name: echo
      inputs:
        message: ${secret('env:API_KEY')}
`
		const uri = "file:///secret-ok.yaml"
		require.Empty(t, messages(c.open(uri, src).Diagnostics))

		pos := positionOf(t, src, "secret('env:API_KEY')", 3)
		got := c.hover(uri, pos.Line, pos.Character)
		require.NotNil(t, got)
		text := hoverText(got)
		assert.Contains(t, text, "env:API_KEY")
		assert.Contains(t, text, "resolved by whichever provider this deployment registers")
		assert.Contains(t, text, "never enters workflow history")
		// The scheme is described, never a concrete backend: which provider serves
		// a scheme is a deployment's choice, made worker-side.
		assert.NotContains(t, text, "environment variable")

		require.NotNil(t, got.Range)
		assert.Equal(t, "secret('env:API_KEY')", textInRange(src, *got.Range))
	})

	t.Run("a reference with no scheme says why", func(t *testing.T) {
		const src = `name: secrets
steps:
  - id: a
    task:
      name: echo
      inputs:
        message: ${secret('API_KEY')}
`
		const uri = "file:///secret-bad.yaml"
		// The compiler reports it too; hover must agree rather than describe it as
		// if it worked.
		require.NotEmpty(t, messages(c.open(uri, src).Diagnostics))

		pos := positionOf(t, src, "secret('API_KEY')", 3)
		got := c.hover(uri, pos.Line, pos.Character)
		require.NotNil(t, got)
		assert.Contains(t, hoverText(got), "not usable as written")
		assert.Contains(t, hoverText(got), "has no provider")
	})

	t.Run("an opaque name is not interpreted", func(t *testing.T) {
		const src = `name: secrets
steps:
  - id: a
    task:
      name: echo
      inputs:
        message: ${secret('vault:prod/api#token')}
`
		const uri = "file:///secret-vault.yaml"
		c.open(uri, src)

		pos := positionOf(t, src, "vault:prod/api#token", 2)
		got := c.hover(uri, pos.Line, pos.Character)
		require.NotNil(t, got)
		// Everything after the first colon belongs to the provider, so it is
		// reported verbatim rather than parsed into parts the DSL does not define.
		assert.Contains(t, hoverText(got), "prod/api#token")
		assert.Contains(t, hoverText(got), "`vault`")
	})
}

func TestSecretRefAt(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		src     string
		cursor  int
		wantRef string
		wantErr bool
	}{
		{name: "cursor on the marker", src: `secret('env:A')`, cursor: 2, wantRef: "env:A"},
		{name: "cursor in the reference", src: `secret('env:A')`, cursor: 10, wantRef: "env:A"},
		{name: "cursor at the end", src: `secret('env:A')`, cursor: 15, wantRef: "env:A"},
		{name: "double quotes", src: `secret("env:A")`, cursor: 3, wantRef: "env:A"},
		{name: "not a secret call", src: `step.output`, cursor: 3, wantErr: true},
		{name: "cursor outside the call", src: `x + secret('env:A')`, cursor: 0, wantErr: true},
		{name: "unterminated quote", src: `secret('env:A`, cursor: 3, wantErr: true},
		{name: "no closing paren", src: `secret('env:A'`, cursor: 3, wantErr: true},
		{name: "empty", src: ``, cursor: 0, wantErr: true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ref, span, err := secretRefAt(tt.src, tt.cursor)
			if tt.wantErr {
				assert.Error(t, err)
				return
			}
			require.NoError(t, err)
			assert.Equal(t, tt.wantRef, ref)
			assert.Equal(t, tt.src[span[0]:span[1]], tt.src[span[0]:span[1]])
			assert.Contains(t, tt.src[span[0]:span[1]], tt.wantRef)
		})
	}
}

// TestHoverUsesUTF16Columns is the position-correctness test. The reference sits
// after an astral-plane character, so a server counting bytes or code points
// instead of UTF-16 code units would resolve the cursor to the wrong text.
func TestHoverUsesUTF16Columns(t *testing.T) {
	t.Parallel()

	const src = "name: ünïcödé\n" +
		"steps:\n" +
		"  - id: first\n" +
		"    task:\n" +
		"      name: echo\n" +
		"      inputs:\n" +
		"        message: \"héllo wörld\"\n" +
		"  - id: second\n" +
		"    task:\n" +
		"      name: http\n" +
		"      inputs:\n" +
		"        url: https://example.com\n" +
		"        headers:\n" +
		"          X-🙂-Trace: ${first.result}\n"

	// The three unit systems genuinely disagree on this line, which is the point.
	line := "          X-🙂-Trace: ${first.result}"
	dollar := strings.Index(line, "$")
	require.Equal(t, 24, dollar, "byte column")
	require.Equal(t, 21, len([]rune(line[:dollar])), "code point column")
	require.Equal(t, 22, utf16Len(line[:dollar]), "UTF-16 column")

	c := newClient(t)
	c.initialize()
	c.open("file:///unicode.yaml", src)

	// Point at the `first` identifier inside the expression: two UTF-16 units
	// past the `$`.
	got := c.hover("file:///unicode.yaml", 13, 22+2)
	require.NotNil(t, got, "hover must resolve a reference that follows non-ASCII text")
	assert.Contains(t, hoverText(got), "`first.result`")

	// The range it reports must come back in UTF-16 units too, covering exactly
	// `first.result`.
	require.NotNil(t, got.Range)
	assert.Equal(t, "first.result", textInRange(src, *got.Range))
}

// positionOf returns the position of the needle in src, advanced by offset UTF-16
// code units.
func positionOf(t *testing.T, src, needle string, offset int) lsp.Position {
	t.Helper()
	at := strings.Index(src, needle)
	require.GreaterOrEqual(t, at, 0, "test source does not contain %q", needle)
	ix := newLineIndex(src)
	pos := ix.positionOfOffset(at)
	pos.Character += offset
	return pos
}
