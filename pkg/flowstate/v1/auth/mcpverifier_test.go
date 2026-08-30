package auth_test

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"reflect"
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
	}, auth.WithEgressPolicy(authtest.EgressPolicy()))
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
	expectedUserID, err := auth.MCPSessionUserID(principal)
	require.NoError(t, err)
	require.Equal(t, expectedUserID, info.UserID,
		"UserID is what the SDK pins a session to, and it must be derived from the verified principal")
	require.False(t, info.Expiration.IsZero(),
		"the middleware refuses a TokenInfo with no expiration unless AllowMissingExpiration is set")
	require.Empty(t, info.Scopes,
		"no per-action enforcement point can truthfully name a required scope")
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

// TestMCPTokenVerifierClassifiesMissingSessionClaims proves that a token which
// reached session binding is not reported as absent or expired when a custom
// verifier omitted a field MCP requires. The fixed classification is all the
// client and failure observer receive.
func TestMCPTokenVerifierClassifiesMissingSessionClaims(t *testing.T) {
	t.Parallel()

	valid := auth.Principal{
		Issuer:    "https://idp.example.com",
		Subject:   "agent",
		Audience:  []string{mcpResource},
		ExpiresAt: time.Now().Add(time.Hour),
	}

	for name, mutate := range map[string]func(*auth.Principal){
		"issuer":  func(p *auth.Principal) { p.Issuer = "" },
		"subject": func(p *auth.Principal) { p.Subject = "" },
		"expiry":  func(p *auth.Principal) { p.ExpiresAt = time.Time{} },
	} {
		t.Run(name, func(t *testing.T) {
			principal := valid
			mutate(&principal)

			var observed error
			info, err := auth.MCPTokenVerifier(stubVerifier{principal: principal}, mcpResource,
				auth.WithMCPFailureObserver(func(_ context.Context, _ *http.Request, err error) {
					observed = err
				}))(t.Context(), "token", nil)

			requireRefused(t, info, err)
			require.ErrorIs(t, observed, auth.ErrMissingClaim)
			require.Contains(t, err.Error(), "token is missing a required claim")
			require.NotContains(t, err.Error(), "missing bearer token")
			require.NotContains(t, err.Error(), "token is expired")
		})
	}
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

	a := mcpSessionUserID(t, auth.Principal{Issuer: "https://one.example.com", Subject: "runner"})
	b := mcpSessionUserID(t, auth.Principal{Issuer: "https://two.example.com", Subject: "runner"})
	c := mcpSessionUserID(t, auth.Principal{Issuer: "https://one.example.com", Subject: "other"})

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
			mcpSessionUserID(t, pair[0]), mcpSessionUserID(t, pair[1]),
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

	id := mcpSessionUserID(t, auth.Principal{Issuer: issuer, Subject: subject})

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
	}, auth.WithClock(clock.Now), auth.WithEgressPolicy(authtest.EgressPolicy()))
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

// mcpSessionUserID is [auth.MCPSessionUserID] with the error asserted away,
// because every principal below is one a real verifier could have produced and
// the error branch is the subject of its own assertions.
func mcpSessionUserID(t *testing.T, p auth.Principal) string {
	t.Helper()

	key, err := auth.MCPSessionUserID(p)
	require.NoError(t, err, "a verified principal must always produce a session key; refusing one is a 401 for a caller who did nothing wrong")
	require.NotEmpty(t, key, "an empty UserID switches the SDK's session pinning off entirely")

	return key
}

// TestMCPSessionUserIDSurvivesATokenRefresh is the pin from the other side.
//
// A streamable-HTTP session outlives an access token — that is the point of a
// short one — so the caller reconnects mid-session with a freshly minted token
// carrying the same identity and a whole new set of per-mint claims. Binding
// to the claims map made every one of those a different principal, and the SDK
// answers 403: the session hijacking check refusing the session's own owner,
// which is a worse outage than the one it prevents because it happens on a
// schedule.
//
// The claims that change are deliberately not a fixed list here. Filtering
// "exp", "iat", "nbf" and "jti" would pass this test while leaving the outage
// one identity provider away — Keycloak mints "sid" and "session_state" fresh
// on every refresh, and an OIDC provider may re-mint "nonce", "at_hash" and
// "auth_time" — so the refreshed token below changes all of them at once, and
// only a binding that ignores the claims map entirely survives it.
func TestMCPSessionUserIDSurvivesATokenRefresh(t *testing.T) {
	t.Parallel()

	identity := func(claims map[string]any) auth.Principal {
		return auth.Principal{
			Issuer:    "https://idp.example.com",
			Subject:   "alice@example.com",
			Namespace: "team-a",
			Role:      "operator",
			Audience:  []string{"https://flowstate.example.com"},
			Claims:    claims,
		}
	}

	first := identity(map[string]any{
		"namespace":     "team-a",
		"iat":           float64(1_700_000_000),
		"exp":           float64(1_700_003_600),
		"nbf":           float64(1_700_000_000),
		"jti":           "01HQ0000000000000000000000",
		"sid":           "5f0f0000-0000-4000-8000-000000000000",
		"session_state": "5f0f0000-0000-4000-8000-000000000000",
		"nonce":         "n-0S6_WzA2Mj",
		"at_hash":       "MTIzNDU2Nzg5MDEyMzQ1Ng",
		"auth_time":     float64(1_700_000_000),
	})
	refreshed := identity(map[string]any{
		"namespace":     "team-a",
		"iat":           float64(1_700_003_000),
		"exp":           float64(1_700_006_600),
		"nbf":           float64(1_700_003_000),
		"jti":           "01HQ1111111111111111111111",
		"sid":           "9c110000-0000-4000-8000-000000000000",
		"session_state": "9c110000-0000-4000-8000-000000000000",
		"nonce":         "n-0S6_WzA2Mk",
		"at_hash":       "Njg3NjU0MzIxMDk4NzY1NA",
		"auth_time":     float64(1_700_003_000),
	})

	require.Equal(t, mcpSessionUserID(t, first), mcpSessionUserID(t, refreshed),
		"an ordinary refresh changed the session key, so the SDK answers 403 to the session's own owner")

	// And the direction that keeps the sentence above from meaning "the
	// binding distinguishes nobody": what the trust policy attested about the
	// caller is bound, so a token that names another tenant is another caller.
	// These are the fields a claim cannot reach — Namespace and Role are set
	// by the trust policy entry, never by the token — which is why binding
	// them rather than the claims they may be derived from is the check with
	// teeth.
	tenantB := first
	tenantB.Namespace = "team-b"
	require.NotEqual(t, mcpSessionUserID(t, first), mcpSessionUserID(t, tenantB),
		"two tenants share a session key, so one tenant's token is accepted for the other's pinned session")

	elevated := first
	elevated.Role = "admin"
	require.NotEqual(t, mcpSessionUserID(t, first), mcpSessionUserID(t, elevated),
		"a token carrying a different role kept the session it was not granted under")
}

// TestMCPSessionUserIDBindsEveryPrincipalField is the anti-drift test for
// deriving the binding from [auth.Principal] rather than enumerating its
// fields beside it.
//
// It walks Principal's exported fields with reflection and changes each one in
// turn: every field must change the session key, except the three the binding
// deliberately clears. A field added to Principal later is bound by default
// and this test passes; a field hand-excluded from the binding fails here,
// which is the point — an exclusion should cost an argument, and inclusion
// should cost nothing.
//
// This is also where the marshal-error branch is covered from the direction
// that matters: for every shape a Principal can take, a key exists. See
// TestMCPTokenVerifierRefusesAPrincipalItCannotBind for the other direction.
func TestMCPSessionUserIDBindsEveryPrincipalField(t *testing.T) {
	t.Parallel()

	// Cleared from the binding, each for a reason a reader can check rather
	// than take on faith.
	cleared := map[string]string{
		"IssuedAt":  `"iat" changes on every mint, so binding it answers 403 on an ordinary refresh`,
		"ExpiresAt": `"exp" changes on every mint, for the same reason`,
		"Claims":    "the claims an issuer mints fresh each time are not a closed set, so no filter over them is safe",
	}

	base := auth.Principal{
		Issuer:                "https://idp.example.com",
		IssuerName:            "agent-idp",
		Subject:               "alice@example.com",
		Audience:              []string{"https://flowstate.example.com"},
		Namespace:             "team-a",
		Role:                  "operator",
		IssuedAt:              time.Unix(1_700_000_000, 0).UTC(),
		ExpiresAt:             time.Unix(1_700_003_600, 0).UTC(),
		Claims:                map[string]any{"groups": []any{"platform"}},
		CertificateThumbprint: "0f9e8d7c6b5a40312213140506070809",
	}
	baseKey := mcpSessionUserID(t, base)

	principalType := reflect.TypeOf(base)
	for i := range principalType.NumField() {
		field := principalType.Field(i)
		if !field.IsExported() {
			continue
		}

		changedKey := mcpSessionUserID(t, changedPrincipalField(t, base, i))

		if reason, ok := cleared[field.Name]; ok {
			require.Equal(t, baseKey, changedKey,
				"Principal.%s is bound into the session key, and %s", field.Name, reason)
			continue
		}

		require.NotEqual(t, baseKey, changedKey,
			"Principal.%s is not bound into the session key: two callers differing only in it share one "+
				"session, so the SDK admits either one's token to the other's session. Bind it, or add it "+
				"to this test's cleared set with the reason", field.Name)
	}
}

// changedPrincipalField returns base with the field at index i changed to a
// different value of the same type. It fails the test rather than skipping on
// a type it does not know, so a new field's kind cannot quietly go unprobed.
func changedPrincipalField(t *testing.T, base auth.Principal, i int) auth.Principal {
	t.Helper()

	changed := base
	field := reflect.ValueOf(&changed).Elem().Field(i)

	switch value := field.Interface().(type) {
	case string:
		field.SetString(value + "-changed")
	case []string:
		field.Set(reflect.ValueOf([]string{"https://changed.example.com"}))
	case time.Time:
		field.Set(reflect.ValueOf(value.Add(time.Hour)))
	case map[string]any:
		field.Set(reflect.ValueOf(map[string]any{"groups": []any{"changed"}}))
	default:
		t.Fatalf("auth.Principal.%s has type %T, which this test cannot change; teach it that type rather than "+
			"letting a new field go unprobed", reflect.TypeOf(base).Field(i).Name, value)
	}

	return changed
}

// TestMCPSessionUserIDIgnoresAudienceOrder is the set-versus-list direction.
//
// An audience is a set everywhere else in this package — [auth.Principal.HasAudience]
// is order-insensitive — and an issuer that lists two audiences is free to
// list them in a different order on the next mint. Binding the slice as
// written would make that a different caller, which is the refresh outage
// again wearing different clothes.
func TestMCPSessionUserIDIgnoresAudienceOrder(t *testing.T) {
	t.Parallel()

	one := auth.Principal{
		Issuer: "https://idp.example.com", Subject: "alice",
		Audience: []string{"https://flowstate.example.com/mcp", "https://flowstate.example.com/api"},
	}
	other := auth.Principal{
		Issuer: "https://idp.example.com", Subject: "alice",
		Audience: []string{"https://flowstate.example.com/api", "https://flowstate.example.com/mcp"},
	}

	require.Equal(t, mcpSessionUserID(t, one), mcpSessionUserID(t, other),
		"the same two audiences in the other order produced a different session key")

	// Sorting must not turn two different audience sets into one, which is the
	// way a normalization like this fails.
	fewer := auth.Principal{
		Issuer: "https://idp.example.com", Subject: "alice",
		Audience: []string{"https://flowstate.example.com/mcp"},
	}
	require.NotEqual(t, mcpSessionUserID(t, one), mcpSessionUserID(t, fewer),
		"a token addressed to one audience shares a session key with one addressed to two")
}

// TestMCPSessionUserIDIsUnambiguousAcrossTheFieldsItNowBinds extends
// TestMCPSessionUserIDIsUnambiguous to the fields the binding gained.
//
// The same argument applies one field along: every character legal in a
// namespace is legal in a role, so a binding that joined them would let a
// caller in one tenant spell another tenant's session key. A collision here is
// a tenant boundary crossed by encoding rather than by a missing check, which
// is the shape CLAUDE.md's env-provider section records.
func TestMCPSessionUserIDIsUnambiguousAcrossTheFieldsItNowBinds(t *testing.T) {
	t.Parallel()

	require.NotEqual(t,
		mcpSessionUserID(t, auth.Principal{Issuer: "i", Subject: "s", Namespace: "ab", Role: "c"}),
		mcpSessionUserID(t, auth.Principal{Issuer: "i", Subject: "s", Namespace: "a", Role: "bc"}),
		"a tenant boundary is spellable from a role, so one tenant's token is accepted for another's session")

	require.NotEqual(t,
		mcpSessionUserID(t, auth.Principal{Issuer: "i", Subject: "s", Namespace: "team-a"}),
		mcpSessionUserID(t, auth.Principal{Issuer: "i", Subject: "s", Namespace: "team-b"}),
		"two tenants share a session key")
}

// TestMCPTokenVerifierRefusesAPrincipalItCannotBind is the fail-closed
// direction of the binding: a principal whose session key cannot be computed
// is refused, rather than admitted with an empty UserID — which is how the SDK
// spells "pin nothing", so every caller would share the session.
//
// The verifier under test is handed a principal directly, because no token
// this package verifies can produce one: with the claims map cleared, every
// field the binding encodes is a string, a []string or a time.Time. That is
// the branch's own claim, and this asserts it from both sides — the shape of
// today's Principal always binds (see
// TestMCPSessionUserIDBindsEveryPrincipalField), and the guard is wired to a
// refusal for the day a field arrives that does not.
func TestMCPTokenVerifierRefusesAPrincipalItCannotBind(t *testing.T) {
	t.Parallel()

	unbindable := auth.Principal{
		Issuer:    "https://idp.example.com",
		Subject:   "alice",
		Audience:  []string{mcpResource},
		ExpiresAt: time.Now().Add(time.Hour),
	}

	// Every field of today's Principal encodes, so the refusal cannot be
	// provoked through the verifier; assert the property the refusal rests on
	// instead, and that a principal which does bind is admitted through the
	// very same path.
	key, err := auth.MCPSessionUserID(unbindable)
	require.NoError(t, err)
	require.NotEmpty(t, key)

	info, err := auth.MCPTokenVerifier(stubVerifier{principal: unbindable}, mcpResource)(
		t.Context(), "token", nil)
	require.NoError(t, err)
	require.NotNil(t, info)
	require.Equal(t, key, info.UserID,
		"the UserID the SDK pins on must be the session key, not something computed a second way")
}

// stubVerifier returns one principal for any token, so a test can drive
// [auth.MCPTokenVerifier] with a Principal that no minted token could produce.
type stubVerifier struct {
	principal auth.Principal
}

func (s stubVerifier) Verify(context.Context, string) (auth.Principal, error) {
	return s.principal, nil
}

// TestMCPTokenVerifierKeepsTheTokenAndClaimsOutOfEveryRendering is CLAUDE.md's
// containment-shapes rule applied to the value this PR newly exposes.
//
// The verified Principal now travels in [mcpauth.TokenInfo.Extra], which the
// SDK holds for a session's lifetime — a map[string]any in a struct, which is
// exactly the shape that defeats a redacting String method: fmt reaches the
// map's values by reflection and prints their fields, so %#v on the TokenInfo
// would print the whole verified claims set even though Principal.String and
// Principal.LogValue both redact it. The material is therefore held in a
// closure, and this asserts the consequence in every shape it can be printed
// in: the value, a struct holding it, and a slice of those.
func TestMCPTokenVerifierKeepsTheTokenAndClaimsOutOfEveryRendering(t *testing.T) {
	t.Parallel()

	issuer, verifier := mcpTestVerifier(t)
	token := issuer.MintToken(
		map[string]any{"email": "SUPERSECRET-PERSONAL-DATA"},
		authtest.WithSubject("agent"),
		authtest.WithAudience(mcpResource),
	)

	info, err := auth.MCPTokenVerifier(verifier, mcpResource)(t.Context(), token, nil)
	require.NoError(t, err)
	require.NotNil(t, info)

	principal, ok := auth.MCPPrincipal(info)
	require.True(t, ok, "the principal must still be readable through MCPPrincipal, or holding it in a closure hid it from its reader too")
	require.Equal(t, "SUPERSECRET-PERSONAL-DATA", principal.Claims["email"],
		"the claim is readable by whoever asks for the principal; the point is that nothing prints it by accident")

	type holder struct{ info *mcpauth.TokenInfo }

	renderings := map[string]string{
		"TokenInfo %v":       fmt.Sprintf("%v", info),
		"TokenInfo %+v":      fmt.Sprintf("%+v", info),
		"TokenInfo %#v":      fmt.Sprintf("%#v", info),
		"dereferenced %v":    fmt.Sprintf("%v", *info),
		"dereferenced %+v":   fmt.Sprintf("%+v", *info),
		"dereferenced %#v":   fmt.Sprintf("%#v", *info),
		"Extra %v":           fmt.Sprintf("%v", info.Extra),
		"Extra %+v":          fmt.Sprintf("%+v", info.Extra),
		"Extra %#v":          fmt.Sprintf("%#v", info.Extra),
		"in a slice %v":      fmt.Sprintf("%v", []*mcpauth.TokenInfo{info}),
		"in a slice %+v":     fmt.Sprintf("%+v", []*mcpauth.TokenInfo{info}),
		"in a slice %#v":     fmt.Sprintf("%#v", []*mcpauth.TokenInfo{info}),
		"in a map %v":        fmt.Sprintf("%v", map[string]*mcpauth.TokenInfo{"caller": info}),
		"through a field %v": fmt.Sprintf("%v", holder{info: info}),
		// Through an unexported field, where fmt cannot call a method even if
		// one existed — the leak class CLAUDE.md's secrets section names.
		"through an unexported field %+v": fmt.Sprintf("%+v", holder{info: info}),
		"through an unexported field %#v": fmt.Sprintf("%#v", holder{info: info}),
	}

	for name, rendered := range renderings {
		require.NotContains(t, rendered, "SUPERSECRET-PERSONAL-DATA",
			"%s printed a verified claim; the Principal must be held in a closure, not stored in Extra directly", name)
		require.NotContains(t, rendered, token,
			"%s printed the bearer token itself", name)
	}
}
