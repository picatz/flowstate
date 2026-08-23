package auth

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"net/http"

	"github.com/picatz/jose/pkg/jwt"

	mcpauth "github.com/modelcontextprotocol/go-sdk/auth"
)

// The bridge between this package's [Verifier] — the one every authenticated
// surface in this repository already goes through — and the callback the MCP
// Go SDK's bearer-token middleware wants. picatz/flowstate#558's slice two:
// a second verifier is the thing this file exists to avoid.

// MCPTokenVerifier adapts v — which already performs every check OAuth 2.1
// section 5.2 asks a resource server for: signature against a discovered key
// set, an "alg" allowlist, "exp"/"iat"/"nbf", an exact "iss" match against a
// trusted issuer, and "aud" against that entry's accepted audiences — to the
// [mcpauth.TokenVerifier] callback [mcpauth.RequireBearerToken] takes.
//
// resource is the canonical URI from [ProtectedResourceConfig.Resource], and
// checking it here is not redundant with the audience check v already
// performs. A [TrustedIssuer] entry lists every audience that issuer may mint
// for, and a deployment whose entry accepts several — its Connect RPC
// audience and its MCP resource, say — would otherwise let a token minted for
// one be spent at the other. RFC 8707 section 2 and the MCP specification
// both require the narrower check: the token must name *this* resource.
// So the entry's list admits the token to the deployment, and this admits it
// to this surface.
//
// # Every error path returns a nil TokenInfo
//
// [mcpauth.RequireBearerToken] treats any non-nil TokenInfo as an
// authenticated caller — it inspects the error only to choose a status code —
// so a partially verified TokenInfo returned beside an error is an
// authentication bypass. Every return below is therefore either
// (nil, non-nil) or (non-nil, nil), and never anything else.
//
// # What the returned TokenInfo carries, and what it deliberately does not
//
//   - UserID is [MCPSessionUserID] of the verified principal, which is what
//     pins a streamable-HTTP session to whoever opened it: the SDK compares
//     this field on every subsequent request against the one the session was
//     created with (mcp/streamable.go's lookupSession) and answers 403 on a
//     mismatch, so a session opened by one principal refuses another's token.
//     See that function for why it is a digest rather than two strings joined.
//   - Expiration is the token's own "exp". A token without one is refused:
//     the middleware would refuse it too unless AllowMissingExpiration were
//     set, and this says so in the verifier where the reason is legible.
//   - Scopes is deliberately nil, and no caller of this sets
//     [mcpauth.RequireBearerTokenOptions.Scopes]. #567's D1 defers the
//     scope vocabulary by omission: a challenge that named a scope would name
//     a spelling that has to migrate the day that decision lands.
//   - Extra carries the verified [Principal], and nothing else. This is S7b
//     arriving: the local MCP path holds a *server.FlowstateServer, which
//     reads [PrincipalFromContext], so the principal now has a reader rather
//     than travelling to a surface that ignores it. It is the Principal and
//     not the raw claims map for the reason the previous note gave — a
//     token's own contents reaching a log or a tool result by accident — and
//     it never carries the bearer token itself. Read it through
//     [MCPPrincipal] rather than by key.
//
// The error text returned on refusal is written into the 401 body by the
// middleware, so it is drawn from [PublicReason] and names nothing the caller
// did not already have: never the configured resource, never an issuer,
// never any part of the token.
func MCPTokenVerifier(v Verifier, resource string) mcpauth.TokenVerifier {
	return func(ctx context.Context, token string, _ *http.Request) (*mcpauth.TokenInfo, error) {
		// Both are programming errors rather than caller errors, and both are
		// refusals anyway: a surface wired without a verifier or without a
		// resource to bind to has no way to tell one caller from another, and
		// the fail-closed answer to "I cannot decide" is no.
		if v == nil {
			return nil, fmt.Errorf("%w: this surface has no token verifier configured", mcpauth.ErrInvalidToken)
		}
		if resource == "" {
			return nil, fmt.Errorf("%w: this surface has no protected resource configured", mcpauth.ErrInvalidToken)
		}

		principal, err := v.Verify(ctx, token)
		if err != nil {
			return nil, fmt.Errorf("%w: %s", mcpauth.ErrInvalidToken, PublicReason(err))
		}

		// [InsecureAnonymousVerifier] admits every caller as the anonymous
		// principal, which is a development affordance for a loopback Connect
		// listener and never an identity. It cannot pin a session either —
		// every caller would share one UserID — so admitting it here would
		// silently turn session pinning off for the whole surface. cmd/flow
		// refuses --insecure-no-auth on this command as well; this is the same
		// refusal in the package that would otherwise have to be trusted to
		// have made it.
		if principal.IsAnonymous() {
			return nil, fmt.Errorf("%w: anonymous access is not available on this surface", mcpauth.ErrInvalidToken)
		}

		if !principal.HasAudience(resource) {
			// Named without the resource: a caller holding a token for some
			// other service learns that the audience was wrong, and does not
			// learn this deployment's resource identifier from a failure — it
			// is published in the RFC 9728 document the challenge points at,
			// which is where a client is meant to read it.
			return nil, fmt.Errorf("%w: the token's audience does not name this resource", mcpauth.ErrInvalidToken)
		}

		// Redundant behind an [OIDCVerifier], which refuses a delegated token
		// before it ever returns a Principal (see delegation.go), and kept
		// anyway: this adapter accepts any [Verifier], and a surface that
		// lost the refusal by being handed a different one would be the
		// asymmetry this call exists to have removed, pointing the other way.
		if err := refuseDelegationClaims(principal.Claims); err != nil {
			return nil, fmt.Errorf("%w: %s", mcpauth.ErrInvalidToken, PublicReason(err))
		}

		// A Principal with no issuer or no subject cannot produce a session
		// key that distinguishes callers, and an empty UserID switches the
		// SDK's session pinning off entirely (mcp/streamable.go pins only
		// when the recorded userID is non-empty). Refusing is the only answer
		// that keeps the pin honest.
		if principal.Issuer == "" || principal.Subject == "" {
			return nil, fmt.Errorf("%w: the token names no issuer and subject to bind a session to", mcpauth.ErrInvalidToken)
		}

		if principal.ExpiresAt.IsZero() {
			return nil, fmt.Errorf("%w: the token carries no expiry", mcpauth.ErrInvalidToken)
		}

		return &mcpauth.TokenInfo{
			Expiration: principal.ExpiresAt,
			UserID:     MCPSessionUserID(principal),
			Extra:      map[string]any{mcpPrincipalKey: principal},
		}, nil
	}
}

const mcpPrincipalKey = "flowstate.auth.principal"

// MCPPrincipal returns the verified Principal carried by MCPTokenVerifier.
// It never reads or exposes the bearer token.
func MCPPrincipal(info *mcpauth.TokenInfo) (Principal, bool) {
	if info == nil || info.Extra == nil {
		return Principal{}, false
	}
	p, ok := info.Extra[mcpPrincipalKey].(Principal)
	return p, ok && !p.IsZero()
}

// MCPSessionUserID is the value [MCPTokenVerifier] puts in
// [mcpauth.TokenInfo.UserID]: the opaque key a streamable-HTTP session is
// pinned to. Exported so a test can assert the pin without reproducing the
// spelling, which is the one way two copies of it could disagree.
//
// Both the issuer and the subject go into it, because a subject is only unique
// within its issuer — the same reasoning [Principal.ID] gives. The
// authorization-relevant claims go in with them and the ones that change on an
// ordinary re-mint do not; see [bindableClaims] for which and why.
//
// # Why this is a digest of a length-prefixed encoding rather than a joined string
//
// The obvious spelling, issuer + separator + subject, is the ambiguous-encoding
// defect CLAUDE.md already records against the env secret provider, on a new
// boundary. Every character legal in an issuer is legal in a subject, so no
// separator makes the pair unambiguous: with "|", the principal
// (https://idp.example/a, "b|victim") and the principal
// (https://idp.example/a|b, "victim") produce one identical key. A subject is
// an arbitrary string the issuer chooses, and an issuer identifier is a URL
// whose path may carry the separator too, so both halves of that collision are
// reachable — and what it buys is the SDK treating two different principals as
// one session owner, which is the session hijacking this field exists to
// prevent, arriving through the encoding rather than through a missing check.
// Reported by Codex on picatz/flowstate#807.
//
// Length-prefixing each field makes the encoding injective: two different
// (issuer, subject) pairs cannot produce one string, because the prefix says
// where the first field ends before its contents are read. The digest over
// that is a second, independent property rather than the fix — the SDK holds
// this value for a session's lifetime and logs around it, and a fixed-width
// opaque key means no part of a caller's identity is sitting in the
// transport's state waiting to be printed. It is compared byte for byte and
// never parsed, so nothing is lost by it being unreadable.
func MCPSessionUserID(p Principal) string {
	sum := sha256.New()
	binding := struct {
		Issuer, Subject, Namespace, Role, CertificateThumbprint string
		Audience                                                []string
		Claims                                                  map[string]any
	}{p.Issuer, p.Subject, p.Namespace, p.Role, p.CertificateThumbprint, p.Audience,
		bindableClaims(p.Claims)}
	encoded, err := json.Marshal(binding)
	if err != nil {
		// Verified token claims are JSON values, so this is unreachable for a
		// token this package minted or verified. A custom Verifier returning
		// something else still gets a binding that distinguishes principals:
		// the issuer and subject go in explicitly, because hashing only the
		// error and the claims map's *type* would give two different callers
		// the same session key — one principal's token accepted for another's
		// session, which is the opposite of fail-closed.
		fmt.Fprintf(sum, "invalid:%d:%s%d:%s:%T:%v",
			len(p.Issuer), p.Issuer, len(p.Subject), p.Subject, p.Claims, err)
	} else {
		sum.Write(encoded)
	}

	return hex.EncodeToString(sum.Sum(nil))
}

// bindableClaims is p.Claims without the ones that change on an ordinary
// refresh.
//
// A session is pinned to who the caller is, not to which token they presented.
// "exp", "iat", "nbf" and "jti" differ on every mint, so binding to them would
// make a routine refresh look like a different principal and answer 403 — the
// failure this session pin exists to prevent, arriving from the other side.
// Everything else is authorization-relevant and stays: a token whose namespace
// or role changed really is a different caller for this purpose.
func bindableClaims(claims map[string]any) map[string]any {
	if len(claims) == 0 {
		return nil
	}

	bindable := make(map[string]any, len(claims))
	for name, value := range claims {
		switch name {
		case jwt.ExpirationTime, jwt.IssuedAt, jwt.NotBefore, jwt.JWTID:
			continue
		}
		bindable[name] = value
	}

	return bindable
}
