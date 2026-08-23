package auth

import (
	"strings"
	"testing"
)

// TestQuotedStringRefusesWhatCannotBeAQuotedString is the header-injection
// direction, and it is the reason this renderer exists as a function rather
// than as concatenation.
//
// picatz/flowstate#999 built the same helper escaping "\" and '"' only, which
// Copilot found there: RFC 9110's qdtext excludes every control character
// except HTAB and quoted-pair cannot rescue one, so a CR or LF in a value
// escaped that way is emitted raw — response splitting in a header a caller's
// own token failure produced. Refusing the parameter is the fail-closed
// answer, so the assertion is that the byte does not appear *and* that the
// rest of the challenge is still well-formed.
func TestQuotedStringRefusesWhatCannotBeAQuotedString(t *testing.T) {
	t.Parallel()

	for _, value := range []string{
		"https://flowstate.example.com/\r\nX-Injected: yes",
		"https://flowstate.example.com/\r",
		"https://flowstate.example.com/\n",
		"https://flowstate.example.com/\x00",
		"https://flowstate.example.com/\x7f",
		"",
	} {
		if quoted, ok := quotedString(value); ok {
			t.Errorf("quotedString(%q) = %q, true; want refused", value, quoted)
		}

		challenge := bearerChallenge("invalid_token", value)
		if strings.ContainsAny(challenge, "\r\n") {
			t.Errorf("bearerChallenge with %q rendered %q, which ends the header early", value, challenge)
		}
		if challenge != `Bearer error="invalid_token"` {
			t.Errorf("bearerChallenge with %q = %q; want the bare challenge, with the parameter dropped",
				value, challenge)
		}
	}

	// HTAB is the one control character a quoted-string may hold, so refusing
	// it would be a bound wider than the grammar rather than the grammar.
	if _, ok := quotedString("a\tb"); !ok {
		t.Error(`quotedString("a\tb") refused HTAB, which RFC 9110 qdtext permits`)
	}
}

// TestQuotedStringEscapesQuotedPairs pins the ordinary half: a value carrying
// a quote or a backslash must not end the string early either.
func TestQuotedStringEscapesQuotedPairs(t *testing.T) {
	t.Parallel()

	for _, test := range []struct{ in, want string }{
		{`plain`, `"plain"`},
		{`say "hi"`, `"say \"hi\""`},
		{`back\slash`, `"back\\slash"`},
		{`"; injected="`, `"\"; injected=\""`},
	} {
		got, ok := quotedString(test.in)
		if !ok || got != test.want {
			t.Errorf("quotedString(%q) = %q, %v; want %q, true", test.in, got, ok, test.want)
		}
	}
}

// TestBearerChallengeShape pins what the header reads as, including the two
// parameters this deployment deliberately does not send.
func TestBearerChallengeShape(t *testing.T) {
	t.Parallel()

	bare := bearerChallenge("invalid_token", "")
	if bare != `Bearer error="invalid_token"` {
		t.Errorf("bearerChallenge without a document = %q", bare)
	}

	full := bearerChallenge("invalid_token", "https://flowstate.example.com/.well-known/oauth-protected-resource/mcp")
	want := `Bearer error="invalid_token", resource_metadata="https://flowstate.example.com/.well-known/oauth-protected-resource/mcp"`
	if full != want {
		t.Errorf("bearerChallenge = %q; want %q", full, want)
	}

	for _, parameter := range []string{"scope=", "realm=", "DPoP"} {
		if strings.Contains(full, parameter) {
			t.Errorf("challenge %q carries %q, which this deployment has not defined", full, parameter)
		}
	}

	// A challenge with nothing to say is still a challenge: RFC 6750 makes
	// every parameter optional, and a bare scheme is what a caller gets rather
	// than an empty header.
	if empty := bearerChallenge("", ""); empty != "Bearer" {
		t.Errorf("bearerChallenge with no parameters = %q; want %q", empty, "Bearer")
	}
}
