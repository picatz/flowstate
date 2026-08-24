package auth

import "strings"

// The `WWW-Authenticate` challenge a 401 from this package carries.
//
// Folded forward from picatz/flowstate#999, which built it beside a scope
// vocabulary and was closed for that half's sake (the vocabulary and its
// enforcement land together or not at all — #567's D1, and #1014). What
// survives is the construction itself, which #1007 needs for a reason that
// has nothing to do with scopes: once the Connect RPC surface binds tokens to
// its own resource, the document a challenge points at is a decision rather
// than a field, and a decision wants one place to be made in.
//
// Two parameters, both RFC-defined, and no third:
//
//   - "error", RFC 6750 section 3.1, always "invalid_token" here. Every path
//     that reaches a challenge is a token this deployment would not accept,
//     and the distinctions RFC 6750 draws between its codes are exactly the
//     trust-policy detail [publicReason] exists to withhold.
//   - "resource_metadata", RFC 9728 section 5.1, naming the protected-resource
//     document — when there is one *and it describes this surface*; see
//     [Authenticator.challengeMetadataURL].
//
// No "scope", and the reason has moved on from the one this comment used to
// give. #567's D1 is answered — the vocabulary exists, in the schema, and
// [ProtectedResource] now publishes it as "scopes_supported" — but a "scope"
// parameter on a challenge says which scope *this request* needed, and that
// is an answer only a per-action enforcement point can give. Nothing here
// consults a token's scopes yet (docs/MCP_AUTHORIZATION.md's "authorization
// here is coarse"), so naming one would tell a caller to go acquire a scope
// this deployment will never look at. It lands with the enforcement or not at
// all, which is the same shape #999 was closed for. No "realm": RFC 6750
// makes it optional and it would say nothing a caller can act on. And no DPoP
// scheme, which #999 carried a
// parameter for: nothing in this tree issues a DPoP-bound token, and a
// parameter no caller can reach is a configuration surface advertising a
// decision nobody has made.

// bearerChallenge renders the RFC 6750 `WWW-Authenticate` value for a rejected
// request: the "Bearer" scheme, an "error" parameter, and RFC 9728's
// "resource_metadata" when metadataURL is non-empty.
//
// Both parameter values are rendered as RFC 9110 quoted-strings by
// [quotedString], which is why this is a function rather than the four lines
// of concatenation it replaced: a value that cannot be expressed as one is
// dropped rather than emitted, so no input can end this header early and
// begin another.
func bearerChallenge(errorCode, metadataURL string) string {
	params := make([]string, 0, 2)

	if quoted, ok := quotedString(errorCode); ok {
		params = append(params, "error="+quoted)
	}

	if quoted, ok := quotedString(metadataURL); ok {
		params = append(params, "resource_metadata="+quoted)
	}

	if len(params) == 0 {
		return "Bearer"
	}

	return "Bearer " + strings.Join(params, ", ")
}

// quotedString renders s as an RFC 9110 section 5.6.4 quoted-string, reporting
// false for the empty string and for any value that cannot be one.
//
// The escaping half is ordinary: a backslash and a double quote each become a
// quoted-pair, so a value carrying either does not end the string early.
//
// The refusing half is the one worth stating. RFC 9110's qdtext excludes every
// control character except HTAB, and quoted-pair cannot rescue one — so a
// value holding CR or LF has no quoted-string spelling at all, and a renderer
// that escaped only `\` and `"` (as #999's did, which Copilot found there)
// would emit those bytes raw into a header value: response splitting, in a
// header a caller's own token failure produced. Refusing the parameter
// outright is the fail-closed answer and loses nothing real, because a value
// that cannot be written down was never going to be read back.
//
// Nothing reaching this today can carry one: the only non-constant caller
// passes [ProtectedResource.MetadataURL], built from a [url.URL] that
// net/url's own parser already refuses control characters in, and both are
// fixed at start-up from configuration rather than taken from a request. That
// is a property of the two call sites, though, not of this function, and the
// next caller does not inherit it.
func quotedString(s string) (string, bool) {
	if s == "" {
		return "", false
	}

	for i := range len(s) {
		// Byte-wise on purpose: obs-text (%x80-FF) is permitted, and every
		// byte of a multi-byte UTF-8 rune falls in that range, so a range-over-
		// string decoding runes would ask the question of the wrong unit.
		if c := s[i]; (c < 0x20 && c != '\t') || c == 0x7f {
			return "", false
		}
	}

	return `"` + strings.NewReplacer(`\`, `\\`, `"`, `\"`).Replace(s) + `"`, true
}
