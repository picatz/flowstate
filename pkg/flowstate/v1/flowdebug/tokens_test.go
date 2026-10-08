package flowdebug

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
)

type run struct {
	kind TokenKind
	text string
}

func tokens(text string) []run {
	var runs []run
	for kind, part := range ValueTokens(text) {
		runs = append(runs, run{kind, part})
	}

	return runs
}

func TestValueTokensClassifyACompactValue(t *testing.T) {
	assert.Equal(t, []run{
		{TokenPlain, "{"},
		{TokenKey, `"a"`}, {TokenPlain, ":"},
		{TokenLiteral, "1"},
		{TokenPlain, ","},
		{TokenKey, `"b"`}, {TokenPlain, ":"},
		{TokenString, `"x\"y"`},
		{TokenPlain, ","},
		{TokenKey, `"c"`}, {TokenPlain, ":"},
		{TokenRedacted, `"[redacted]"`},
		{TokenPlain, ","},
		{TokenKey, `"d"`}, {TokenPlain, ":"},
		{TokenLiteral, "null"},
		{TokenPlain, "}"},
	}, tokens(`{"a":1,"b":"x\"y","c":"[redacted]","d":null}`))
}

func TestValueTokensClassifyATree(t *testing.T) {
	assert.Equal(t, []run{
		{TokenKey, "auth:"}, {TokenPlain, "\n"},
		{TokenPlain, "  "}, {TokenKey, "token:"}, {TokenPlain, " "}, {TokenRedacted, `"[redacted]"`}, {TokenPlain, "\n"},
		{TokenPlain, "  "}, {TokenKey, "[0]"}, {TokenPlain, " "}, {TokenLiteral, "true"}, {TokenPlain, "\n"},
		{TokenPlain, "  "}, {TokenElision, "… 3 more items"}, {TokenPlain, "\n"},
		{TokenKey, "deep:"}, {TokenPlain, " "}, {TokenElision, "{… 4 keys}"}, {TokenPlain, "\n"},
	}, tokens("auth:\n  token: \"[redacted]\"\n  [0] true\n  … 3 more items\ndeep: {… 4 keys}\n"))
}

// A string is a string wherever it sits: data that looks like a key, a marker
// or an elision does not get that treatment.
func TestValueTokensDoNotTrustTheContentOfAString(t *testing.T) {
	for _, in := range []string{`"a: b"`, `"… 9 more keys"`, `"[0] true"`, `"x[redacted]x"`} {
		got := tokens(in)
		assert.Equal(t, []run{{TokenString, in}}, got, in)
	}
}

// The runs are the text, whatever the text is: styling can never change bytes.
func TestValueTokensConcatenateToTheirInput(t *testing.T) {
	for _, in := range []string{
		"", "\n", "plain words", `"unterminated`, "{[", "[x] y", "key:value:more", "1e5 -3.2 tru nul",
		"\x1b[31m red", "日本語: 値", "a\n\n  b:\n", `"é\"`, "… only", "{… ", "[…",
	} {
		var b strings.Builder
		for _, r := range tokens(in) {
			b.WriteString(r.text)
		}
		assert.Equal(t, in, b.String(), "input %q", in)
	}
}

func FuzzValueTokensAreLossless(f *testing.F) {
	for _, seed := range []string{`{"a":[1,"x"]}`, "k:\n  [0] 1\n  … 2 more items\n", `"\`, "\xff\xfe"} {
		f.Add(seed)
	}
	f.Fuzz(func(t *testing.T, in string) {
		var b strings.Builder
		for _, r := range tokens(in) {
			if r.text == "" {
				t.Fatalf("empty run for %q", in)
			}
			b.WriteString(r.text)
		}
		if b.String() != in {
			t.Fatalf("runs %q differ from %q", b.String(), in)
		}
	})
}
