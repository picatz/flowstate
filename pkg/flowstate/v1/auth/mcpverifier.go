package auth

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"net/http"
	"slices"
	"time"

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
//
//   - Expiration is the token's own "exp". A token without one is refused:
//     the middleware would refuse it too unless AllowMissingExpiration were
//     set, and this says so in the verifier where the reason is legible.
//
//   - Scopes is deliberately nil, and no caller of this sets
//     [mcpauth.RequireBearerTokenOptions.Scopes]. The action vocabulary now
//     exists, but no per-action enforcement point can truthfully name which
//     scope this request requires.
//
//   - Extra carries the verified [Principal], and nothing else. It is the
//     Principal and not the raw claims map for the reason the previous note
//     gave — a token's own contents reaching a log or a tool result by
//     accident — and it never carries the bearer token itself. Read it
//     through [MCPPrincipal] rather than by key, and note that the value
//     under that key is a closure rather than the Principal: a map[string]any
//     is printed field by field by %#v, which reaches past
//     [Principal.String] and [Principal.LogValue] into the claims those two
//     redact. CLAUDE.md's containment rule — hold the material in a closure,
//     because reflection cannot reach a captured variable — applies to
//     anything that lands in this map.
//
//     Nothing on the one surface this verifier is wired to reads it yet.
//     `flow mcp serve` serves Validate, Compile, GetCatalog,
//     flowstate_test and flowstate_debug (cmd/flow/mcpserve.go's
//     mcpServeTools), and none of those consults [PrincipalFromContext]: the three RPCs answer from the
//     request alone (server/validate.go), and only the run-creating RPCs this
//     surface deliberately does not serve reach FlowstateServer.identityFor.
//     So the carry is inert today, and deliberately so — cmd/flow's MCP
//     handlers install it on the handler context (withMCPPrincipal) so that
//     the *first* reader is a reader rather than a plumbing change, which is
//     the half of S7b that can land before there is anything to authorize.
//
// The error text returned on refusal is written into the 401 body by the
// middleware, so it is drawn from [PublicReason] and names nothing the caller
// did not already have: never the configured resource, never an issuer,
// never any part of the token.
func MCPTokenVerifier(v Verifier, resource string, opts ...MCPTokenVerifierOption) mcpauth.TokenVerifier {
	var settings mcpTokenVerifierSettings
	for _, opt := range opts {
		opt(&settings)
	}

	return func(ctx context.Context, token string, req *http.Request) (*mcpauth.TokenInfo, error) {
		refuse := func(err error) (*mcpauth.TokenInfo, error) {
			if settings.observe != nil {
				settings.observe(ctx, req, err)
			}
			return nil, fmt.Errorf("%w: %s", mcpauth.ErrInvalidToken, PublicReason(err))
		}

		// Both are programming errors rather than caller errors, and both are
		// refusals anyway: a surface wired without a verifier or without a
		// resource to bind to has no way to tell one caller from another, and
		// the fail-closed answer to "I cannot decide" is no.
		if v == nil {
			return refuse(fmt.Errorf("%w: this surface has no token verifier configured", ErrInvalidPolicy))
		}
		if resource == "" {
			return refuse(fmt.Errorf("%w: this surface has no protected resource configured", ErrInvalidPolicy))
		}

		principal, err := admitBearer(ctx, v, token, resource, true)
		if err != nil {
			return refuse(err)
		}

		// A Principal with no issuer or no subject cannot produce a session
		// key that distinguishes callers, and an empty UserID switches the
		// SDK's session pinning off entirely (mcp/streamable.go pins only
		// when the recorded userID is non-empty). Refusing is the only answer
		// that keeps the pin honest.
		if principal.Issuer == "" || principal.Subject == "" {
			return refuse(fmt.Errorf("%w: the token names no issuer and subject to bind a session to", ErrMissingClaim))
		}

		if principal.ExpiresAt.IsZero() {
			return refuse(fmt.Errorf("%w: the token carries no expiry", ErrMissingClaim))
		}

		userID, err := MCPSessionUserID(principal)
		if err != nil {
			// Fail closed. A binding that cannot be encoded is a binding that
			// cannot distinguish this caller from another, and the SDK treats
			// an empty UserID as "do not pin this session at all" — so the
			// only answer that keeps the pin honest is to refuse. The reason
			// is generic on purpose: err names a field of the token.
			return refuse(fmt.Errorf("%w: this token cannot be bound to a session", ErrInvalidPolicy))
		}

		return &mcpauth.TokenInfo{
			Expiration: principal.ExpiresAt,
			UserID:     userID,
			Extra:      map[string]any{mcpPrincipalKey: heldPrincipal(func() Principal { return principal })},
		}, nil
	}
}

// MCPTokenVerifierOption configures the MCP transport adapter without changing
// the admission checks it shares with Connect.
type MCPTokenVerifierOption func(*mcpTokenVerifierSettings)

type mcpTokenVerifierSettings struct {
	observe func(context.Context, *http.Request, error)
}

// WithMCPFailureObserver registers a function called with the internal reason
// each MCP bearer request was rejected. The caller still receives only
// [PublicReason]. The observer runs on the request path and must not inspect or
// log the Authorization header, which carries the bearer token.
func WithMCPFailureObserver(observe func(context.Context, *http.Request, error)) MCPTokenVerifierOption {
	return func(settings *mcpTokenVerifierSettings) {
		settings.observe = observe
	}
}

const mcpPrincipalKey = "flowstate.auth.principal"

// heldPrincipal is how the verified Principal travels in
// [mcpauth.TokenInfo.Extra]: as a closure over it rather than as the value.
//
// Extra is a map[string]any the SDK holds for a session's lifetime, and %#v on
// a map prints every value it holds field by field — past [Principal.String]
// and [Principal.LogValue], which exist precisely to keep the verified claims
// out of a log line. fmt prints a func as an address and no verb reaches
// inside one, which is the same containment CLAUDE.md's secrets section
// prescribes for credential material, applied to a claims set that may carry
// personal data.
type heldPrincipal func() Principal

// MCPPrincipal returns the verified Principal carried by MCPTokenVerifier.
// It never reads or exposes the bearer token.
func MCPPrincipal(info *mcpauth.TokenInfo) (Principal, bool) {
	if info == nil || info.Extra == nil {
		return Principal{}, false
	}
	held, ok := info.Extra[mcpPrincipalKey].(heldPrincipal)
	if !ok {
		return Principal{}, false
	}
	p := held()
	return p, !p.IsZero()
}

// MCPSessionUserID is the value [MCPTokenVerifier] puts in
// [mcpauth.TokenInfo.UserID]: the opaque key a streamable-HTTP session is
// pinned to. Exported so a test can assert the pin without reproducing the
// spelling, which is the one way two copies of it could disagree.
//
// Both the issuer and the subject go into it, because a subject is only unique
// within its issuer — the same reasoning [Principal.ID] gives. So does every
// other field the trust policy attested — namespace, role, audience, the
// issuer entry's name, a certificate thumbprint — because a token that names a
// different tenant is a different caller for this purpose.
//
// # The binding is derived from the Principal, not enumerated beside it
//
// It is [Principal]'s own JSON encoding, with the two timestamps and the
// claims map cleared. Writing the fields out here instead would be a second
// declaration of "the identity fields that matter", beside Principal's json
// tags, [Principal.ID] and the operator-named claim allowlist
// [IdentityFromPrincipal] takes — and a field added to Principal later would
// be silently absent from the binding, with no compile error and no failing
// test. Deriving it means the default for a new field is to be bound;
// excluding one is an edit to this function that
// TestMCPSessionUserIDBindsEveryPrincipalField makes someone justify.
//
// # What is cleared, and the cost of clearing it
//
// IssuedAt and ExpiresAt change on every mint. A streamable-HTTP session
// outlives an access token — that is the point of a short one — so binding to
// them would answer 403 to the session's own owner on an ordinary refresh:
// the hijacking check firing at whoever it exists to protect, on a schedule.
//
// Claims are cleared for the same reason, taken one step further. Filtering
// the *known* per-mint claims ("exp", "iat", "nbf", "jti") is a denylist, and
// the claims an issuer mints fresh each time are not a closed set: Keycloak's
// "sid" and "session_state", "nonce", "at_hash" and "auth_time" are all real
// and none of them would be caught, so the refresh outage returns the first
// time a deployment changes identity provider. The claims are therefore out
// entirely, and the cost is stated rather than hidden: two tokens that differ
// only in an issuer-specific claim — a "groups" or "scope" list, say — share a
// session key. That is deliberate. Session identity is *who the caller is*,
// which is what [Principal.ID] already means; it is not an authorization
// decision. Every request on a pinned session carries its own bearer token and
// is verified afresh by the middleware before it reaches a handler
// (go-sdk@v1.7.0 mcp/streamable.go's lookupSession runs after
// RequireBearerToken, and the handler reads the *current* request's
// Principal), so a re-mint that drops a claim loses whatever that claim
// granted on its very next call, session or no session.
//
// Audience is sorted, because it is a set everywhere else in this package
// ([Principal.HasAudience] is order-insensitive) and an issuer is free to list
// one in a different order next time.
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
// JSON encoding is injective for the same reason: each field is a quoted,
// escaped string under its own key, so the encoder says where one field ends
// before the next is read and no value can spell its way into a neighbour. The
// digest over that is a second, independent property rather than the fix — the
// SDK holds this value for a session's lifetime and logs around it, and a
// fixed-width opaque key means no part of a caller's identity is sitting in
// the transport's state waiting to be printed. It is compared byte for byte
// and never parsed, so nothing is lost by it being unreadable.
//
// The error is unreachable for a Principal as it is shaped today: with Claims
// cleared, every remaining field is a string, a []string or a time.Time, none
// of which json.Marshal can refuse. It is returned rather than swallowed so
// that a field added later which *can* be refused fails closed at the one
// caller — [MCPTokenVerifier] answers 401 — instead of every such caller
// quietly sharing one session key.
func MCPSessionUserID(p Principal) (string, error) {
	binding := p
	binding.IssuedAt = time.Time{}
	binding.ExpiresAt = time.Time{}
	binding.Claims = nil
	binding.Audience = slices.Sorted(slices.Values(p.Audience))

	encoded, err := json.Marshal(binding)
	if err != nil {
		return "", fmt.Errorf("encoding the session binding: %w", err)
	}

	sum := sha256.Sum256(encoded)
	return hex.EncodeToString(sum[:]), nil
}
