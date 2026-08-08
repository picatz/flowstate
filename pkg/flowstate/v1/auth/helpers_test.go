package auth_test

import (
	"crypto"
	"crypto/x509"
	"encoding/base64"
	"encoding/json"
	"strings"
	"testing"

	"github.com/picatz/jose/pkg/header"
	"github.com/picatz/jose/pkg/jwa"
	"github.com/picatz/jose/pkg/jwt"
	"github.com/stretchr/testify/require"

	"github.com/picatz/flowstate/pkg/flowstate/v1/authtest"
)

// The tests in this package never reach the network: every issuer is an
// [authtest.Issuer] in this process, and every key is generated here.
//
// What is left in this file is the half of a token that no issuer mints. An
// [authtest.Issuer] can be made to misbehave in every way an identity provider
// can, but it cannot sign with a symmetric algorithm or with none at all,
// because no provider's key set contains such a key. Those tokens are an
// attacker's, so they are built here, next to the tests proving they are
// refused.

// newTestIssuer starts a stand-in identity provider that is stopped when the
// test ends. It is not newIssuer, which builds the [auth.Issuer] this package
// exports: this one is somebody else's provider, whose tokens arrive here.
//
// The authtest package takes no testing.TB, so that it can be used from an
// example, a benchmark, or a program that is not a test at all. Registering the
// cleanup is this package's business.
func newTestIssuer(t *testing.T, options ...authtest.IssuerOption) *authtest.Issuer {
	t.Helper()

	issuer := authtest.NewIssuer(options...)
	t.Cleanup(func() { _ = issuer.Close() })

	return issuer
}

// hmacToken returns a token signed with an HMAC secret, the shape of an
// algorithm confusion attempt when the secret is an issuer's public key.
func hmacToken(t *testing.T, keyID string, secret []byte, claims map[string]any) string {
	t.Helper()

	set := jwt.ClaimsSet{}
	for name, value := range claims {
		set[name] = value
	}

	token, err := jwt.New(
		header.Parameters{
			header.Type:      jwt.Type,
			header.Algorithm: jwa.HS256,
			header.KeyID:     keyID,
		},
		set,
		secret,
	)
	require.NoError(t, err)

	return token.String()
}

// publicKeyBytes returns a key's DER encoding, which is what an attacker
// attempting algorithm confusion uses as an HMAC secret: it is the exact byte
// string the issuer publishes.
func publicKeyBytes(t *testing.T, key crypto.PublicKey) []byte {
	t.Helper()
	der, err := x509.MarshalPKIXPublicKey(key)
	require.NoError(t, err)
	return der
}

// noneToken returns an unsigned token, with the empty signature segment that a
// real "alg": "none" token carries.
func noneToken(t *testing.T, claims map[string]any) string {
	t.Helper()

	encodedHeader, err := header.Parameters{
		header.Type:      jwt.Type,
		header.Algorithm: jwa.None,
	}.Base64URLString()
	require.NoError(t, err)

	encodedClaims, err := json.Marshal(claims)
	require.NoError(t, err)

	return encodedHeader + "." + base64.RawURLEncoding.EncodeToString(encodedClaims) + "."
}

// tamperSignature alters a token's signature, leaving a well-formed token whose
// signature cannot verify.
//
// The first character of the signature is changed rather than the last, because
// the trailing base64url character of a signature carries padding bits that
// decode to nothing: changing it can leave the signature bytes identical.
func tamperSignature(t *testing.T, token string) string {
	t.Helper()

	dot := strings.LastIndex(token, ".")
	require.Greater(t, dot, 0, "token has no signature segment")

	signature := token[dot+1:]
	require.NotEmpty(t, signature, "token has an empty signature")

	replacement := "A"
	if strings.HasPrefix(signature, "A") {
		replacement = "B"
	}

	return token[:dot+1] + replacement + signature[1:]
}

// dropSignature removes a token's signature segment entirely, leaving two
// segments where a JWT must have three.
func dropSignature(t *testing.T, token string) string {
	t.Helper()

	dot := strings.LastIndex(token, ".")
	require.Greater(t, dot, 0, "token has no signature segment")

	return token[:dot]
}
