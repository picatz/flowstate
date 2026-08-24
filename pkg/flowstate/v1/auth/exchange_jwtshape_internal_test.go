package auth

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
)

// TestJWTShapedKnowsBothCompactSerializations pins jwtShaped to RFC 7519's
// actual grammar rather than to the JWS half of it: an encrypted JWT is five
// segments, a direct-encryption JWE carries an empty encrypted-key segment,
// and an unsecured JWT carries an empty signature — all of them tokens the
// authorization server would accept, so all of them tokens this shape check
// must not refuse. The refusals are the opposite direction: opaque blobs,
// missing load-bearing segments, wrong segment counts, and a token past the
// bound every other door here already applies.
func TestJWTShapedKnowsBothCompactSerializations(t *testing.T) {
	for name, test := range map[string]struct {
		token string
		want  bool
	}{
		"a signed JWT":                         {"aGVhZGVy.Y2xhaW1z.c2ln", true},
		"an unsecured JWT has an empty sig":    {"aGVhZGVy.Y2xhaW1z.", true},
		"an encrypted JWT is five segments":    {"aGVhZGVy.a2V5.aXY.Y2lwaGVydGV4dA.dGFn", true},
		"direct encryption has an empty key":   {"aGVhZGVy..aXY.Y2lwaGVydGV4dA.dGFn", true},
		"an opaque blob is not a JWT":          {"an-opaque-session-blob", false},
		"a JWS with no claims is not a JWT":    {"aGVhZGVy..c2ln", false},
		"a JWE with no ciphertext is not one":  {"aGVhZGVy.a2V5.aXY..dGFn", false},
		"four segments belong to neither form": {"a.b.c.d", false},
		"a non-base64url byte is refused":      {"aGVhZGVy.Y2xh aW1z.c2ln", false},
		"a token past the parse bound":         {strings.Repeat("a", maxTokenBytes+1), false},
		"a dot-heavy blob is refused, bounded": {strings.Repeat("a.", 1000) + "a", false},
		"an empty token is not a JWT":          {"", false},
	} {
		t.Run(name, func(t *testing.T) {
			assert.Equal(t, test.want, jwtShaped(test.token))
		})
	}
}
