package lsp

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// sigIn places a log step whose message is expr, with the cursor at the `§`
// marker, and asks for signature help there.
func sigIn(t *testing.T, expr string, header string) (activeLabel string, active int, param string, ok bool) {
	t.Helper()
	before, after, found := strings.Cut(expr, "§")
	require.True(t, found, "the expression needs a § cursor marker")

	src := "edition: v2026.4\nname: sig\n" + header + "steps:\n  - id: a\n    log:\n      message: ${"
	cursor := len(src) + len(before)
	src += before + after + "}\n"

	doc := refsDoc(t, src)
	help := signatureHelpAt(doc, doc.index.positionOfOffset(cursor))
	if help == nil {
		return "", 0, "", false
	}
	sig := help.Signatures[help.ActiveSignature]
	if help.ActiveParameter < len(sig.Parameters) {
		param = sig.Parameters[help.ActiveParameter].Label
	}

	return sig.Label, help.ActiveParameter, param, true
}

func TestSignatureHelp(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		expr      string
		header    string
		none      bool
		label     string // substring of the active signature's label
		wantArg   int
		wantParam string
	}{
		{name: "namespaced, first argument", expr: `math.abs(§)`, label: "math.abs(", wantParam: "double"},
		{name: "second argument after a comma", expr: `"a".replace("x", §)`, label: "string.replace(string, string)", wantArg: 1, wantParam: "string"},
		{name: "the overload with a third argument", expr: `"a".replace("x", "y", §)`, label: "string.replace(string, string, int)", wantArg: 2, wantParam: "int"},
		{name: "qualified name beats the bare one", expr: `regex.replace("a", "b", §)`, label: "regex.replace(", wantArg: 2, wantParam: "string"},
		{name: "method on a value", expr: `["a"].join(§)`, label: "join(string)", wantParam: "string"},
		{name: "nested call answers for the inner one", expr: `"a".replace(math.abs(§), "y")`, label: "math.abs(", wantParam: "double"},
		{name: "after the inner call closes, the outer is back", expr: `"a".replace(math.abs(1), §)`, label: "string.replace(", wantArg: 1, wantParam: "string"},
		{name: "commas in a list literal are not arguments", expr: `"a".replace([1, 2, 3].join(","), §)`, label: "string.replace(", wantArg: 1},
		{name: "comma in a list argument", expr: `"a".replace(["x", §`, label: "string.replace(", wantArg: 0},
		{name: "commas in strings are not arguments", expr: `"a".replace("x,y,z", §)`, label: "string.replace(", wantArg: 1},
		{name: "parens in strings are not calls", expr: `math.abs(")", §)`, label: "math.abs(", wantArg: 1},
		{name: "grouping inside an argument", expr: `math.abs((1 + 2), §)`, label: "math.abs(", wantArg: 1},
		{name: "unfinished expression", expr: `"a".replace("x", §`, label: "string.replace(", wantArg: 1},
		{name: "a macro has no signature", expr: `[1].map(x, §)`, none: true},
		{name: "an unknown name has none", expr: `nosuchfn(§)`, none: true},
		{name: "outside any call", expr: `§1 + 2`, none: true},
		{name: "after the call closed", expr: `math.abs(1)§`, none: true},
		{name: "an unclosed string at the cursor", expr: `math.abs("abc§`, none: true},
		{name: "an escaped final quote does not close", expr: `math.abs("abc\"§`, none: true},
		{name: "after a closed string", expr: `math.abs("abc"§`, label: "math.abs(", wantParam: "double"},
		{name: "inside a string", expr: `math.abs("§")`, none: true},
		{name: "grouping is not a call", expr: `(1 + §2)`, none: true},
		{
			name: "a quoted functions key", header: "\"functions\":\n  slugify:\n    params:\n      title: string\n    returns: string\n    body: ${title.trim()}\n",
			expr: `slugify(§"x")`, label: "slugify(title: string)", wantParam: "title: string",
		},
		{
			name: "a declared function", header: "functions:\n  slugify:\n    params:\n      title: string\n      sep: string\n    returns: string\n    body: ${title.trim()}\n",
			expr: `slugify("x", §)`, label: "slugify(title: string, sep: string) -> string", wantArg: 1, wantParam: "sep: string",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			label, arg, param, ok := sigIn(t, tt.expr, tt.header)
			if tt.none {
				assert.False(t, ok, "expected no signature help, got %q", label)
				return
			}
			require.True(t, ok, "expected signature help")
			assert.Contains(t, label, tt.label)
			assert.Equal(t, tt.wantArg, arg)
			if tt.wantParam != "" {
				assert.Equal(t, tt.wantParam, param)
			}
		})
	}
}

func TestSignatureParametersSplitsOnlyTopLevelCommas(t *testing.T) {
	t.Parallel()

	assert.Equal(t, []string{"map(string, int)", "list(dyn)"},
		signatureParameters("x.f(map(string, int), list(dyn)) -> int", "f"))
	assert.Equal(t, []string(nil), signatureParameters("list(string).join() -> string", "join"))
	assert.Equal(t, []string{"string"}, signatureParameters("list(string).join(string) -> string", "join"))
	assert.Nil(t, signatureParameters("no call form", "f"))
}

func TestEnclosingCallSkipsComments(t *testing.T) {
	t.Parallel()

	_, ok := enclosingCall("math.abs(1 // , (\n", len("math.abs(1 // , (\n"))
	assert.True(t, ok, "a comment's parenthesis must not open a call")

	c, _ := enclosingCall("f(1 // ,,\n, ", len("f(1 // ,,\n, "))
	assert.Equal(t, 1, c.arg, "commas in a comment are not arguments")

	_, ok = enclosingCall("f(1 // inside", len("f(1 // inside"))
	assert.False(t, ok, "the cursor in a comment is not in a call")
}

func TestSignatureHelpInVarsAndFunctionBodies(t *testing.T) {
	t.Parallel()

	for name, src := range map[string]string{
		"vars":          "edition: v2026.4\nname: v\nvars:\n  n: ${math.abs(§1)}\nsteps:\n  - id: a\n    log:\n      message: x\n",
		"function body": "edition: v2026.4\nname: v\nfunctions:\n  f:\n    params:\n      x: int\n    returns: int\n    body: ${math.abs(§x)}\nsteps:\n  - id: a\n    log:\n      message: x\n",
	} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			before, after, _ := strings.Cut(src, "§")
			doc := refsDoc(t, before+after)
			help := signatureHelpAt(doc, doc.index.positionOfOffset(len(before)))
			require.NotNil(t, help)
			assert.Contains(t, help.Signatures[0].Label, "math.abs(")
		})
	}
}
