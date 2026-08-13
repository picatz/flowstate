package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/picatz/jose/pkg/jwt"
	"github.com/stretchr/testify/require"
)

func timeAt(unixSeconds int64) time.Time {
	return time.Unix(unixSeconds, 0)
}

func runJWTSignInto(t *testing.T, args ...string) (stdout, stderr string, err error) {
	t.Helper()

	var out, errOut bytes.Buffer

	cmd := newJWTSignCommand()
	for i := 0; i+1 < len(args); i += 2 {
		require.NoError(t, cmd.Flags().Set(args[i], args[i+1]))
	}
	cmd.SetOut(&out)
	cmd.SetErr(&errOut)
	cmd.SetContext(t.Context())

	err = runJWTSign(cmd, nil)

	return out.String(), errOut.String(), err
}

func runJWTInspectInto(t *testing.T, token string, args ...string) (stdout, stderr string, err error) {
	t.Helper()

	var out, errOut bytes.Buffer

	cmd := newJWTInspectCommand()
	for i := 0; i+1 < len(args); i += 2 {
		require.NoError(t, cmd.Flags().Set(args[i], args[i+1]))
	}
	cmd.SetOut(&out)
	cmd.SetErr(&errOut)
	cmd.SetContext(t.Context())

	err = runJWTInspect(cmd, []string{token})

	return out.String(), errOut.String(), err
}

// generateTestKey generates a signing key at dir/id.pem and returns its path.
func generateTestKey(t *testing.T, dir, id string) string {
	t.Helper()

	path := filepath.Join(dir, id+".pem")
	_, _, err := runKeysGenerateInto(t, "out", path)
	require.NoError(t, err)

	return path
}

func TestJWTSignThenInspectRoundTripsTheClaims(t *testing.T) {
	keyPath := generateTestKey(t, t.TempDir(), "2026-08")

	token, _, err := runJWTSignInto(t,
		"key", keyPath,
		"issuer", "https://flowstate.internal",
		"subject", "worker-1",
		"audience", "flowstate-worker",
		"claim", "namespace=team-a",
	)
	require.NoError(t, err)
	token = strings.TrimSpace(token)
	require.NotEmpty(t, token)

	stdout, _, err := runJWTInspectInto(t, token)
	require.NoError(t, err)

	var result map[string]any
	require.NoError(t, json.Unmarshal([]byte(stdout), &result))

	header := result["header"].(map[string]any)
	require.Equal(t, "2026-08", header["kid"])
	require.Equal(t, "ES256", header["alg"])

	claims := result["claims"].(map[string]any)
	require.Equal(t, "https://flowstate.internal", claims["iss"])
	require.Equal(t, "worker-1", claims["sub"])
	require.Equal(t, "flowstate-worker", claims["aud"])
	require.Equal(t, "team-a", claims["namespace"])
	require.Equal(t, false, result["expired"])
}

// TestJWTInspectAnnotatesExpiryAndIssuedAtWithReadableTime pins
// picatz/flowstate#395's second item: `exp`/`iat` are read by a person
// debugging "why won't this verify", and the most common answer, expiry, comes
// back as bare epoch seconds. The claims themselves stay the raw wire values
// (a machine reading `.claims.exp` sees exactly what the token carries); the
// annotation is additive, beside them.
//
// Mutation-proven: removing the ExpiresAt/IssuedAt assignments in
// runJWTInspect, or reverting annotateClaimTime to return "", makes this fail.
func TestJWTInspectAnnotatesExpiryAndIssuedAtWithReadableTime(t *testing.T) {
	keyPath := generateTestKey(t, t.TempDir(), "2026-08")

	token, _, err := runJWTSignInto(t,
		"key", keyPath, "issuer", "i", "subject", "s", "audience", "a", "ttl", "5m",
	)
	require.NoError(t, err)
	token = strings.TrimSpace(token)

	stdout, _, err := runJWTInspectInto(t, token)
	require.NoError(t, err)

	var result map[string]any
	require.NoError(t, json.Unmarshal([]byte(stdout), &result))

	claims := result["claims"].(map[string]any)
	expEpoch, ok := claims["exp"].(float64)
	require.True(t, ok, "the raw exp claim must stay a bare number, verbatim")

	expiresAt, ok := result["expiresAt"].(string)
	require.True(t, ok, "expected an expiresAt annotation beside the raw claim")
	require.Contains(t, expiresAt, fmt.Sprintf("%d", int64(expEpoch)),
		"the annotation must still carry the epoch value, not replace it")
	require.Contains(t, expiresAt, "in ", "a token minted just now should read as expiring in the future")

	issuedAt, ok := result["issuedAt"].(string)
	require.True(t, ok, "expected an issuedAt annotation beside the raw claim")
	require.Contains(t, issuedAt, "ago", "a token minted just now should read as issued in the past")
}

// TestJWTInspectOmitsTimeAnnotationsWhenAbsent covers a token minted outside
// `flow jwt sign` (or hand-built for a test) that carries no exp/iat: the
// annotation fields must not appear at all, rather than reporting a guess about
// a claim the token never made.
func TestJWTInspectOmitsTimeAnnotationsWhenAbsent(t *testing.T) {
	claims := jwt.ClaimsSet{jwt.Subject: "s"}
	now := time.Now()

	require.Empty(t, annotateClaimTime(claims, jwt.ExpirationTime, now))
	require.Empty(t, annotateClaimTime(claims, jwt.IssuedAt, now))
}

func TestJWTInspectWithTheSigningKeyVerifiesTheSignature(t *testing.T) {
	dir := t.TempDir()
	keyPath := generateTestKey(t, dir, "2026-08")
	otherKeyPath := generateTestKey(t, dir, "2026-09")

	token, _, err := runJWTSignInto(t,
		"key", keyPath,
		"issuer", "i", "subject", "s", "audience", "a",
	)
	require.NoError(t, err)
	token = strings.TrimSpace(token)

	stdout, _, err := runJWTInspectInto(t, token, "key", keyPath)
	require.NoError(t, err)
	var result map[string]any
	require.NoError(t, json.Unmarshal([]byte(stdout), &result))
	require.Equal(t, true, result["signatureValid"])

	// Verifying against a different key must report false, not an error — the
	// question "was this signed by X" has a false answer, not a failure.
	stdout, _, err = runJWTInspectInto(t, token, "key", otherKeyPath)
	require.NoError(t, err)
	require.NoError(t, json.Unmarshal([]byte(stdout), &result))
	require.Equal(t, false, result["signatureValid"])
}

// TestJWTInspectVerifiesByTheTokensOwnKeyIDNotTheKeyFileName is the regression
// case for a bug Codex found in review: verification keyed the lookup map by
// the key *file's* name rather than the token's "kid" header, so a token
// signed with `flow jwt sign --id` set to something other than the file name
// reported signatureValid=false even though the right key had signed it.
func TestJWTInspectVerifiesByTheTokensOwnKeyIDNotTheKeyFileName(t *testing.T) {
	keyPath := generateTestKey(t, t.TempDir(), "2026-08")

	token, _, err := runJWTSignInto(t,
		"key", keyPath, "id", "a-completely-different-kid",
		"issuer", "i", "subject", "s", "audience", "a",
	)
	require.NoError(t, err)
	token = strings.TrimSpace(token)

	stdout, _, err := runJWTInspectInto(t, token, "key", keyPath)
	require.NoError(t, err)

	var result map[string]any
	require.NoError(t, json.Unmarshal([]byte(stdout), &result))
	require.Equal(t, "a-completely-different-kid", result["header"].(map[string]any)["kid"])
	require.Equal(t, true, result["signatureValid"],
		"the key that signed the token must verify it regardless of what its file happens to be named")
}

func TestJWTSignRefusesATTLOverTheCap(t *testing.T) {
	keyPath := generateTestKey(t, t.TempDir(), "2026-08")

	_, _, err := runJWTSignInto(t,
		"key", keyPath, "issuer", "i", "subject", "s", "audience", "a",
		"ttl", "2h",
	)
	require.Error(t, err)
	require.ErrorContains(t, err, "cap")
}

func TestJWTSignRefusesANonPositiveTTL(t *testing.T) {
	keyPath := generateTestKey(t, t.TempDir(), "2026-08")

	_, _, err := runJWTSignInto(t,
		"key", keyPath, "issuer", "i", "subject", "s", "audience", "a",
		"ttl", "0s",
	)
	require.Error(t, err)
}

func TestJWTSignRefusesAClaimThatCollidesWithARegisteredFlag(t *testing.T) {
	keyPath := generateTestKey(t, t.TempDir(), "2026-08")

	_, _, err := runJWTSignInto(t,
		"key", keyPath, "issuer", "i", "subject", "s", "audience", "a",
		"claim", "sub=someone-else",
	)
	require.Error(t, err)
	require.ErrorContains(t, err, "sub")
}

func TestJWTSignRefusesAMalformedClaim(t *testing.T) {
	keyPath := generateTestKey(t, t.TempDir(), "2026-08")

	_, _, err := runJWTSignInto(t,
		"key", keyPath, "issuer", "i", "subject", "s", "audience", "a",
		"claim", "no-equals-sign",
	)
	require.Error(t, err)
}

func TestJWTInspectOnGarbageDoesNotEchoTheInput(t *testing.T) {
	garbage := "not-a-jwt-at-all-but-maybe-a-leaked-secret-abc123XYZ"

	_, _, err := runJWTInspectInto(t, garbage)
	require.Error(t, err)
	require.NotContains(t, err.Error(), garbage,
		"an unparseable token might be a live credential; the error must not repeat it")
}

func TestTokenExpired(t *testing.T) {
	claims := jwt.ClaimsSet{"exp": int64(100)}
	expired, ok := tokenExpired(claims, timeAt(200))
	require.True(t, ok)
	require.True(t, expired)

	notExpired, ok := tokenExpired(claims, timeAt(50))
	require.True(t, ok)
	require.False(t, notExpired)

	// A token parsed from the wire round-trips its "exp" claim through JSON,
	// which decodes numbers as float64. That must be handled the same as
	// int64, or every real inspected token would silently lose expiry.
	floatClaims := jwt.ClaimsSet{"exp": float64(100)}
	expiredFloat, ok := tokenExpired(floatClaims, timeAt(200))
	require.True(t, ok)
	require.True(t, expiredFloat)

	_, ok = tokenExpired(jwt.ClaimsSet{}, timeAt(0))
	require.False(t, ok, "no exp claim means nothing to judge")

	_, ok = tokenExpired(jwt.ClaimsSet{"exp": "not a number"}, timeAt(0))
	require.False(t, ok, "an exp claim of the wrong type is unjudgeable, not an error")
}

func TestNewJWTCommandWiresBothSubcommands(t *testing.T) {
	cmd := newJWTCommand()

	names := map[string]bool{}
	for _, sub := range cmd.Commands() {
		names[sub.Name()] = true
	}
	require.True(t, names["sign"])
	require.True(t, names["inspect"])
}
