package auth

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"net/http"

	mcpauth "github.com/modelcontextprotocol/go-sdk/auth"
)

// The bridge between this package's [Verifier] — the one every authenticated
// surface in this repository already goes through — and the callback the MCP
// Go SDK's bearer-token middleware wants. picatz/flowstate#558's slice two:
// a second verifier is the thing this file exists to avoid.

// Claim names an inbound token may carry that this deployment refuses rather
// than interprets.
//
// Both are RFC 8693 (OAuth 2.0 Token Exchange) delegation claims: "act"
// records that the bearer is an agent exercising some other party's
// authority, and "may_act" records that the token's subject permits some
// other party to do so in a future token. Flowstate has nowhere to map
// either yet — a trust policy cannot express which claim carries a
// delegator, and #567's D2 leaves that naming decision open — so a token
// carrying one is refused outright.
//
// Refused, specifically, rather than stripped. Admitting the request as the
// bare subject named by "sub" would let it proceed while the audit record
// said nothing was delegated: the token itself claims an agent is acting for
// somebody, and a deployment that cannot yet read that claim must not answer
// as though it were never made. That is #567's S7a amendment, "deferred
// fail-closed", and it is the confused-deputy shape stated one layer earlier
// than the mapping that would otherwise get it wrong.
//
// These are deliberately not [ClaimOnBehalfOf]: that one is a claim
// Flowstate's own issuer *mints* into an assertion it is vouching for, where
// these two are claims an external identity provider mints into a token
// Flowstate only verifies. See pkg/flowstate/v1/authtest/negative.go, whose
// WithDelegation and WithMayAct mint exactly the tokens this refuses.
const (
	// ClaimActor is RFC 8693 section 4.1's "act".
	ClaimActor = "act"

	// ClaimMayAct is RFC 8693 section 4.4's "may_act".
	ClaimMayAct = "may_act"
)

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
//   - Extra is deliberately nil. It travels to tool handlers through the
//     request context, and nothing on this surface authorizes per claim yet
//     (that is S7b); carrying a verified claims map into a surface that does
//     not read it is a place for a token's own contents to reach a log or a
//     tool result by accident.
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

		if claim, refused := refusedDelegationClaim(principal.Claims); refused {
			return nil, fmt.Errorf("%w: the token carries a %q delegation claim, which this deployment "+
				"does not interpret and will not ignore", mcpauth.ErrInvalidToken, claim)
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
		}, nil
	}
}

// MCPSessionUserID is the value [MCPTokenVerifier] puts in
// [mcpauth.TokenInfo.UserID]: the opaque key a streamable-HTTP session is
// pinned to. Exported so a test can assert the pin without reproducing the
// spelling, which is the one way two copies of it could disagree.
//
// Both the issuer and the subject go into it, because a subject is only unique
// within its issuer — the same reasoning [Principal.ID] gives.
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
	fmt.Fprintf(sum, "%d:%s%d:%s", len(p.Issuer), p.Issuer, len(p.Subject), p.Subject)

	return hex.EncodeToString(sum.Sum(nil))
}

// refusedDelegationClaim reports the first delegation claim present in a
// verified claims set, if any. See [ClaimActor] for why presence alone is
// enough — the value is never read, because there is nothing here yet that
// could read it correctly.
func refusedDelegationClaim(claims map[string]any) (string, bool) {
	// Checked in a fixed order rather than by ranging the map, so a token
	// carrying both is always refused by naming the same one and a test can
	// assert the message.
	for _, name := range []string{ClaimActor, ClaimMayAct} {
		if _, present := claims[name]; present {
			return name, true
		}
	}

	return "", false
}
