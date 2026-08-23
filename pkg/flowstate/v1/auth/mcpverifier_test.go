package auth_test

import (
	"errors"
	"fmt"
	"net/http/httptest"
	"testing"
	"time"

	mcpauth "github.com/modelcontextprotocol/go-sdk/auth"
	"github.com/stretchr/testify/require"

	"github.com/picatz/flowstate/pkg/flowstate/v1/auth"
	"github.com/picatz/flowstate/pkg/flowstate/v1/authtest"
)

// The negative direction, first and mostly. CLAUDE.md's rule for a boundary is
// to test that A cannot reach B rather than that A can reach A, and every test
// below except the one that proves a good token is admitted is a token that
// must not be.
//
// The shared shape of all of them: an [mcpauth.TokenVerifier] returning a
// non-nil [mcpauth.TokenInfo] is an authenticated caller as far as
// [mcpauth.RequireBearerToken] is concerned — it reads the error only to pick
// a status code — so every refusal is asserted as *both* an error and a nil
// TokenInfo. A test that checked only the error would pass against the exact
// bypass this adapter exists to avoid.

// mcpResource is the resource identifier the surface under test is.
const mcpResource = "https://flowstate.example.com/mcp"

// mcpOtherResource is a second resource the same issuer is also trusted to
// mint for. It exists so that "the audience is checked" can be distinguished
// from "the trust policy's audience list is checked": a token carrying this
// passes [auth.OIDCVerifier] and must still be refused here.
const mcpOtherResource = "https://flowstate.example.com/api"

// mcpTestVerifier builds an issuer trusted for both resources above, and the
// verifier that trusts it.
func mcpTestVerifier(t *testing.T) (*authtest.Issuer, auth.Verifier) {
	t.Helper()

	issuer := authtest.NewIssuer()
	t.Cleanup(func() { _ = issuer.Close() })

	verifier, err := auth.NewOIDCVerifier(auth.Policy{
		Issuers: []auth.TrustedIssuer{{
			Name:   "agent-idp",
			Issuer: issuer.URL(),
			// Two audiences on one entry, which is the configuration that
			// makes the adapter's own audience check load-bearing rather than
			// redundant: the verifier admits a token for either, and only this
			// surface knows which one it is.
			Audiences: []string{mcpResource, mcpOtherResource},
		}},
	})
	require.NoError(t, err)

	return issuer, verifier
}

// requireRefused asserts a refusal in both of the ways a refusal has to hold.
func requireRefused(t *testing.T, info *mcpauth.TokenInfo, err error) {
	t.Helper()

	require.Error(t, err)
	require.Nil(t, info, "a non-nil TokenInfo is an authenticated caller whatever the error says")
	require.True(t, errors.Is(err, mcpauth.ErrInvalidToken),
		"a refusal must unwrap to ErrInvalidToken so the middleware answers 401 rather than 500: %v", err)
}

// TestMCPTokenVerifierAdmitsAToken is the one positive case, and it is here to
// keep every negative one honest: without it, a verifier that refused
// everything would pass the whole rest of this file.
func TestMCPTokenVerifierAdmitsAToken(t *testing.T) {
	t.Parallel()

	issuer, verifier := mcpTestVerifier(t)
	token := issuer.MintToken(nil, authtest.WithSubject("agent"), authtest.WithAudience(mcpResource))

	info, err := auth.MCPTokenVerifier(verifier, mcpResource)(t.Context(), token, httptest.NewRequest("POST", "/mcp", nil))

	require.NoError(t, err)
	require.NotNil(t, info)
	principal, ok := auth.MCPPrincipal(info)
	require.True(t, ok)
	require.Equal(t, auth.MCPSessionUserID(principal), info.UserID,
		"UserID is what the SDK pins a session to, and it must be derived from the verified principal")
	require.False(t, info.Expiration.IsZero(),
		"the middleware refuses a TokenInfo with no expiration unless AllowMissingExpiration is set")
	require.Empty(t, info.Scopes,
		"#567's D1 is deferred by omission: this surface names no scope anywhere")
	require.Equal(t, "agent", principal.Subject)
	require.Equal(t, mcpResource, principal.Audience[0])
}

// TestMCPTokenVerifierRefusesAWrongAudienceToken is the audience check every
// MCP resource server MUST perform (RFC 8707 section 2): a token addressed to
// somebody else, signed by an issuer this deployment does trust.
func TestMCPTokenVerifierRefusesAWrongAudienceToken(t *testing.T) {
	t.Parallel()

	issuer, verifier := mcpTestVerifier(t)
	token := issuer.WrongAudienceToken("https://elsewhere.example.com/api", nil, authtest.WithSubject("agent"))

	info, err := auth.MCPTokenVerifier(verifier, mcpResource)(t.Context(), token, nil)

	requireRefused(t, info, err)
}

// TestMCPTokenVerifierRefusesATokenForAnotherResourceOfThisDeployment is the
// cross-resource case, and the reason this adapter checks the audience at all
// when [auth.OIDCVerifier] already does.
//
// The token below is valid by the trust policy: the issuer is trusted, and
// mcpOtherResource is on that entry's Audiences list, so Verify admits it.
// What must not follow is that it is spendable *here*. A deployment whose
// trust entry lists its Connect RPC audience beside its MCP resource would
// otherwise let a token minted for one be replayed at the other, which is
// precisely the RFC 8707 binding this surface is required to hold.
func TestMCPTokenVerifierRefusesATokenForAnotherResourceOfThisDeployment(t *testing.T) {
	t.Parallel()

	issuer, verifier := mcpTestVerifier(t)
	token := issuer.MintToken(nil, authtest.WithSubject("agent"), authtest.WithAudience(mcpOtherResource))

	// The premise: the trust policy really does admit this token. Without
	// this assertion the test below could be passing because the token was
	// bad in some other way, and would keep passing if the adapter's own
	// audience check were deleted.
	principal, err := verifier.Verify(t.Context(), token)
	require.NoError(t, err, "the trust policy must admit this token, or the test proves nothing about this surface")
	require.True(t, principal.HasAudience(mcpOtherResource))

	info, err := auth.MCPTokenVerifier(verifier, mcpResource)(t.Context(), token, nil)

	requireRefused(t, info, err)
}

// TestMCPTokenVerifierRefusesAnUntrustedIssuersToken: a correctly signed,
// correctly addressed, unexpired token from an identity provider the policy
// does not name.
func TestMCPTokenVerifierRefusesAnUntrustedIssuersToken(t *testing.T) {
	t.Parallel()

	_, verifier := mcpTestVerifier(t)

	token, foreign := authtest.WrongIssuerToken(nil, []authtest.TokenOption{
		authtest.WithSubject("agent"),
		authtest.WithAudience(mcpResource),
	})
	t.Cleanup(func() { _ = foreign.Close() })

	info, err := auth.MCPTokenVerifier(verifier, mcpResource)(t.Context(), token, nil)

	requireRefused(t, info, err)
}

// TestMCPTokenVerifierRefusesADelegationClaim is #567's D2 deferral, stated
// fail-closed: a token carrying RFC 8693 "act" is refused rather than admitted
// as the bare subject it names.
//
// Everything else about this token is correct — trusted issuer, this
// resource's audience, unexpired — so the refusal can only be the claim.
func TestMCPTokenVerifierRefusesADelegationClaim(t *testing.T) {
	t.Parallel()

	issuer, verifier := mcpTestVerifier(t)
	token := issuer.MintToken(nil,
		authtest.WithSubject("alice@example.com"),
		authtest.WithAudience(mcpResource),
		authtest.WithDelegation(map[string]any{"sub": "agent:deploy-bot"}))

	info, err := auth.MCPTokenVerifier(verifier, mcpResource)(t.Context(), token, nil)

	requireRefused(t, info, err)
	require.ErrorContains(t, err, `"act"`,
		"the refusal must name the claim it refused, so an operator can tell it from an audience failure")
}

// TestMCPTokenVerifierRefusesAMayActClaim is the other half of D2's refusal.
// "may_act" is a distinct claim from "act" — permission to delegate rather
// than a record that delegation happened — and a check that caught only one
// of them would let the other through.
func TestMCPTokenVerifierRefusesAMayActClaim(t *testing.T) {
	t.Parallel()

	issuer, verifier := mcpTestVerifier(t)
	token := issuer.MintToken(nil,
		authtest.WithSubject("alice@example.com"),
		authtest.WithAudience(mcpResource),
		authtest.WithMayAct(map[string]any{"sub": "agent:deploy-bot"}))

	info, err := auth.MCPTokenVerifier(verifier, mcpResource)(t.Context(), token, nil)

	requireRefused(t, info, err)
	require.ErrorContains(t, err, `"may_act"`)
}

// TestMCPTokenVerifierRefusesAnExpiredToken: the lifetime check the underlying
// verifier performs, asserted through this adapter so that a future refactor
// that stopped consulting the verifier's error would fail here.
func TestMCPTokenVerifierRefusesAnExpiredToken(t *testing.T) {
	t.Parallel()

	issuer, verifier := mcpTestVerifier(t)
	token := issuer.MintToken(nil,
		authtest.WithSubject("agent"),
		authtest.WithAudience(mcpResource),
		authtest.Expired())

	info, err := auth.MCPTokenVerifier(verifier, mcpResource)(t.Context(), token, nil)

	requireRefused(t, info, err)
}

// TestMCPTokenVerifierRefusesAnAnonymousPrincipal covers the one verifier in
// this package that admits everybody. `flow mcp serve` refuses
// --insecure-no-auth so this should be unreachable from the command, and the
// adapter refuses it anyway: an anonymous caller would give every session the
// same UserID, which silently turns the SDK's session pinning off for the
// whole surface.
func TestMCPTokenVerifierRefusesAnAnonymousPrincipal(t *testing.T) {
	t.Parallel()

	info, err := auth.MCPTokenVerifier(auth.InsecureAnonymousVerifier(), mcpResource)(t.Context(), "anything", nil)

	requireRefused(t, info, err)
}

// TestMCPTokenVerifierRefusesWhenUnconfigured: a nil verifier and an empty
// resource are both programming errors, and both must fail closed rather than
// admitting a caller nothing checked.
func TestMCPTokenVerifierRefusesWhenUnconfigured(t *testing.T) {
	t.Parallel()

	issuer, verifier := mcpTestVerifier(t)
	token := issuer.MintToken(nil, authtest.WithSubject("agent"), authtest.WithAudience(mcpResource))

	t.Run("no verifier", func(t *testing.T) {
		info, err := auth.MCPTokenVerifier(nil, mcpResource)(t.Context(), token, nil)
		requireRefused(t, info, err)
	})

	t.Run("no resource", func(t *testing.T) {
		info, err := auth.MCPTokenVerifier(verifier, "")(t.Context(), token, nil)
		requireRefused(t, info, err)
	})
}

// TestMCPTokenVerifierRefusalsNameNoTokenMaterial is invariant 7's containment
// discipline applied to this adapter's error text, which the SDK writes
// verbatim into the 401 body a caller reads: no part of the token, and nothing
// about the deployment's configuration, may appear in it.
//
// The token minted here is deliberately one with a recognisable subject and a
// recognisable audience, so that a refusal echoing either would be caught.
func TestMCPTokenVerifierRefusalsNameNoTokenMaterial(t *testing.T) {
	t.Parallel()

	issuer, verifier := mcpTestVerifier(t)

	const secretSubject = "subject-that-must-not-be-echoed"
	const secretAudience = "https://audience-that-must-not-be-echoed.example.com"

	token := issuer.WrongAudienceToken(secretAudience, nil, authtest.WithSubject(secretSubject))

	info, err := auth.MCPTokenVerifier(verifier, mcpResource)(t.Context(), token, nil)
	requireRefused(t, info, err)

	// Every containment shape invariant 7 names, because a value that stays
	// out of %v can still appear under %+v or %#v, and the middleware renders
	// the error with %s (http.Error of err.Error()).
	for name, rendered := range map[string]string{
		"%v":  fmt.Sprintf("%v", err),
		"%+v": fmt.Sprintf("%+v", err),
		"%#v": fmt.Sprintf("%#v", err),
		"%s":  fmt.Sprintf("%s", err),
	} {
		require.NotContains(t, rendered, token, "the raw token appeared under "+name)
		require.NotContains(t, rendered, secretSubject, "the token's subject appeared under "+name)
		require.NotContains(t, rendered, secretAudience, "the token's audience appeared under "+name)
		require.NotContains(t, rendered, mcpResource,
			"this deployment's resource identifier appeared under "+name+"; a client reads it from the "+
				"RFC 9728 document, not from a failure")
		require.NotContains(t, rendered, issuer.URL(), "the trusted issuer appeared under "+name)
	}
}

// TestMCPSessionUserIDDistinguishesSubjectsAcrossIssuers pins the property the
// session pin depends on: two callers that share a subject at different
// issuers, or share an issuer with different subjects, must never collide.
func TestMCPSessionUserIDDistinguishesSubjectsAcrossIssuers(t *testing.T) {
	t.Parallel()

	a := auth.MCPSessionUserID(auth.Principal{Issuer: "https://one.example.com", Subject: "runner"})
	b := auth.MCPSessionUserID(auth.Principal{Issuer: "https://two.example.com", Subject: "runner"})
	c := auth.MCPSessionUserID(auth.Principal{Issuer: "https://one.example.com", Subject: "other"})

	require.NotEqual(t, a, b)
	require.NotEqual(t, a, c)
}

// TestMCPSessionUserIDIsUnambiguous is the ambiguous-encoding defect CLAUDE.md
// records against the env secret provider, checked on this boundary before it
// can be shipped again — reported by Codex on picatz/flowstate#807.
//
// Each pair below is two genuinely different principals whose (issuer,
// subject) fields, concatenated with any single separator, produce one string.
// The first pair is the one Codex named, with "|"; the rest are the same trick
// with every other plausible separator, because the point is not that "|" was
// the wrong character — it is that no character is the right one, since every
// character legal in an issuer is legal in a subject. A collision here is the
// SDK treating two principals as one session owner, which is the session
// hijacking [auth.MCPSessionUserID] exists to prevent, arriving through the
// encoding rather than through a missing check.
func TestMCPSessionUserIDIsUnambiguous(t *testing.T) {
	t.Parallel()

	for separator, pair := range map[string][2]auth.Principal{
		"|": {
			{Issuer: "https://idp.example/a", Subject: "b|victim"},
			{Issuer: "https://idp.example/a|b", Subject: "victim"},
		},
		"#": {
			{Issuer: "https://idp.example/a", Subject: "b#victim"},
			{Issuer: "https://idp.example/a#b", Subject: "victim"},
		},
		"/": {
			{Issuer: "https://idp.example/a", Subject: "b/victim"},
			{Issuer: "https://idp.example/a/b", Subject: "victim"},
		},
		"the empty string": {
			{Issuer: "https://idp.example/ab", Subject: "victim"},
			{Issuer: "https://idp.example/a", Subject: "bvictim"},
		},
	} {
		require.NotEqual(t,
			auth.MCPSessionUserID(pair[0]), auth.MCPSessionUserID(pair[1]),
			"two principals collide under a %s-joined encoding: %+v and %+v", separator, pair[0], pair[1])
	}
}

// TestMCPSessionUserIDCarriesNoIdentityInTheClear: the SDK holds this value
// for a session's lifetime and logs around it, so no part of the principal may
// be readable in it. A property of the digest rather than of the caller, and
// worth pinning because the obvious "simplification" back to a joined string
// would silently undo it along with the collision-freedom above.
func TestMCPSessionUserIDCarriesNoIdentityInTheClear(t *testing.T) {
	t.Parallel()

	const issuer = "https://idp.example.com/tenant"
	const subject = "alice@example.com"

	id := auth.MCPSessionUserID(auth.Principal{Issuer: issuer, Subject: subject})

	require.NotContains(t, id, issuer)
	require.NotContains(t, id, subject)
	require.Len(t, id, 64, "a SHA-256 digest, hex encoded")
}

// TestMCPTokenVerifierIsDeterministicOnAClock is the deterministic-clock
// rehearsal authtest's [authtest.WithClock] exists for: a foreign issuer and
// the trusted one sharing one clock means an untrusted-issuer refusal cannot
// be a lifetime refusal wearing its clothes.
func TestMCPTokenVerifierIsDeterministicOnAClock(t *testing.T) {
	t.Parallel()

	clock := authtest.NewClock(time.Date(2026, 8, 20, 12, 0, 0, 0, time.UTC))

	issuer := authtest.NewIssuer(authtest.WithClock(clock.Now))
	t.Cleanup(func() { _ = issuer.Close() })

	verifier, err := auth.NewOIDCVerifier(auth.Policy{
		Issuers: []auth.TrustedIssuer{{Name: "idp", Issuer: issuer.URL(), Audiences: []string{mcpResource}}},
	}, auth.WithClock(clock.Now))
	require.NoError(t, err)

	token, foreign := authtest.WrongIssuerToken(nil,
		[]authtest.TokenOption{authtest.WithSubject("agent"), authtest.WithAudience(mcpResource)},
		authtest.WithClock(clock.Now))
	t.Cleanup(func() { _ = foreign.Close() })

	info, err := auth.MCPTokenVerifier(verifier, mcpResource)(t.Context(), token, nil)
	requireRefused(t, info, err)
	require.ErrorIs(t, err, mcpauth.ErrInvalidToken)
}

// TestNewProtectedResourceRefusesAQuery is RFC 8707 section 2's other
// component rule, enforced rather than tolerated — reported by Codex on
// picatz/flowstate#807.
//
// A resource identifier's path is what a serving surface mounts itself at,
// and [http.ServeMux] does not distinguish requests by query. So a resource
// carrying one would be served at the bare path and at every other query
// alike: an identifier whose distinguishing part nothing distinguishes on,
// answering at URIs it does not name and requiring in no token's audience.
func TestNewProtectedResourceRefusesAQuery(t *testing.T) {
	t.Parallel()

	policy := &auth.Policy{Issuers: []auth.TrustedIssuer{{
		Name:      "idp",
		Issuer:    "https://idp.example.com",
		Audiences: []string{"https://flowstate.example.com/mcp?tenant=a"},
	}}}

	_, err := auth.NewProtectedResource(auth.ProtectedResourceConfig{
		Resource:             "https://flowstate.example.com/mcp?tenant=a",
		AuthorizationServers: []string{"https://idp.example.com"},
	}, policy)

	require.Error(t, err)
	require.ErrorContains(t, err, "query",
		"the diagnostic must name what is wrong with the identifier, not merely refuse it")

	// The same identifier without the query is accepted, so the refusal is
	// about the query and not about something else in the URI.
	policy.Issuers[0].Audiences = []string{"https://flowstate.example.com/mcp"}
	_, err = auth.NewProtectedResource(auth.ProtectedResourceConfig{
		Resource:             "https://flowstate.example.com/mcp",
		AuthorizationServers: []string{"https://idp.example.com"},
	}, policy)
	require.NoError(t, err)
}
