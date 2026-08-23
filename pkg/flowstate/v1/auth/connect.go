package auth

import (
	"context"
	"errors"
	"fmt"
	"net/http"

	"connectrpc.com/authn"
	"connectrpc.com/connect"
)

// Authenticator authenticates Connect RPC requests from the bearer token in
// their Authorization header.
//
// It is written to be used with [authn.NewMiddleware], which runs before a
// request body is decompressed or unmarshaled, so an unauthenticated caller
// cannot reach the RPC handlers or the protobuf decoder at all:
//
//	authenticator := auth.NewAuthenticator(verifier)
//	middleware := authn.NewMiddleware(authenticator.Authenticate)
//	server := &http.Server{Handler: middleware.Wrap(mux)}
//
// Handlers recover the caller with [PrincipalFromContext].
//
// An Authenticator fails closed. Every path that is not a successfully verified
// token, including a missing header, a header that is not a bearer token, an
// unparseable token, an untrusted issuer, and a verifier that was never
// configured, returns a [connect.CodeUnauthenticated] error. The zero
// Authenticator rejects everything.
//
// A client certificate is consulted only when [WithPeerVerifier] configures
// one, and only through [PeerVerifier] — never as a second [Verifier], and
// never read from the request except through the chain crypto/tls has already
// verified. See mtls.go's package doc for why that is the seam and not a
// widened [Verifier] signature.
type Authenticator struct {
	verifier                     Verifier
	peerVerifier                 PeerVerifier
	observe                      func(context.Context, *http.Request, error)
	protectedResource            string
	protectedResourceMetadataURL string
	expectedResource             string
}

// An AuthenticatorOption configures an [Authenticator].
type AuthenticatorOption func(*Authenticator)

// WithFailureObserver registers a function called with the reason each request
// was rejected. Authentication failures are otherwise invisible to the server,
// because the error returned to the caller deliberately says very little.
//
// The observer runs on the request path, so it should be cheap and must not
// block. It is given the request for context such as the peer address and
// procedure; it must not log the request's Authorization header, which holds the
// caller's token.
func WithFailureObserver(observe func(ctx context.Context, req *http.Request, err error)) AuthenticatorOption {
	return func(a *Authenticator) {
		if observe != nil {
			a.observe = observe
		}
	}
}

// WithPeerVerifier registers a [PeerVerifier] consulted for a request whose
// client certificate crypto/tls has already verified — see
// [http.Request.TLS.VerifiedChains]. Its absence (the default) means a client
// certificate, however the listener's tls.Config treated it at the connection
// level, is never turned into a Principal: mTLS behaves purely as a transport
// fence, and a caller still needs a bearer token this Authenticator's
// [Verifier] accepts.
//
// A nil PeerVerifier, or a request with no verified chain, is treated as "no
// certificate was in play" and falls back to the bearer-token path unchanged.
func WithPeerVerifier(p PeerVerifier) AuthenticatorOption {
	return func(a *Authenticator) {
		a.peerVerifier = p
	}
}

// WithProtectedResource makes a 401 challenge this Authenticator issues carry
// a "resource_metadata" parameter naming pr's RFC 9728 metadata URL, per the
// MCP specification's requirement that the challenge point a client at the
// document before it holds any credential.
//
// The URL always comes from pr, which is itself built from configuration —
// never from the request this Authenticator is rejecting. A forged Host
// header on the request cannot change what this challenge advertises.
//
// A nil pr, or the option omitted entirely, leaves the challenge exactly as
// it reads without this option: "Bearer error=\"invalid_token\"" and nothing
// more, byte-identical to every deployment that has not configured a
// protected resource.
//
// # Why the resource is kept beside its URL
//
// pr's own resource identifier is recorded as well as the document's URL, so
// that [Authenticator.challengeMetadataURL] can tell whether the document
// describes the surface doing the rejecting. A challenge is an instruction —
// a client that reads "fetch this document" fetches it, asks its authorization
// server for the audience the document names, and comes back with that token
// — so pointing at a document for some *other* resource does not merely fail
// to help, it sends a client to mint precisely the token this surface has
// been configured to refuse. That is reachable from the configuration
// docs/DEPLOYMENT.md recommends: `flow server --rpc-resource .../rpc
// --protected-resource .../mcp` publishes the MCP document from the process
// whose Connect RPC surface admits only the RPC audience. Reported by Codex
// on picatz/flowstate#1007.
func WithProtectedResource(pr *ProtectedResource) AuthenticatorOption {
	return func(a *Authenticator) {
		a.protectedResource = pr.Resource()
		a.protectedResourceMetadataURL = pr.MetadataURL()
	}
}

// WithExpectedResource narrows what a verified token may be spent on here: the
// token's "aud" must name resource, the canonical resource URI (RFC 8707
// section 2) this RPC surface identifies as, or the request is refused.
//
// It is the check [MCPTokenVerifier] has always performed against
// [ProtectedResource.Resource], available to the Connect surface for the first
// time. The two surfaces are otherwise the same deployment behind the same
// trust policy, and a [TrustedIssuer] entry lists every audience that issuer
// may mint for — so a deployment whose entry accepts both its RPC audience and
// its MCP resource lets a token minted for one be spent at the other. The
// entry's list admits a token to the deployment; this admits it to this
// surface.
//
// # Why this is opt-in, and what has since flipped it
//
// Unset (the default) is unnarrowed: every deployment that does not configure
// one builds the exact same Authenticator this constructor always built, and
// no token that verifies today starts failing. That is the cost, stated
// plainly — an RPC surface is narrowed only when an operator says so, and
// until then the trust policy's audience list is the whole of the check.
//
// It was the right default because of what was true when it landed: nothing
// minted an RPC token carrying this deployment's resource URI.
// `--protected-resource` is optional on `flow server` (required only on
// `flow mcp serve`, whose surface *is* the resource), the RFC 9728 document a
// deployment may serve is advertisement rather than enforcement, and clients
// ask their authorization server for whatever audience they were configured
// with. Narrowing by default would have refused every one of those tokens on
// upgrade, which is a fail-closed posture bought by an outage.
//
// What that paragraph said would flip it has happened, and this is the record
// of it rather than a prediction: `flow server --rpc-resource` (see
// cmd/flow/rpcresource.go) turns it on, and having grown the flag the command
// went straight to requiring it — a deployment that has a bearer issuer to
// bind to and has not named its RPC audience is refused at start-up, with
// `--allow-issuer-wide-audiences` restoring the old behaviour for one
// migration window. The flag is named after the thing it configures rather
// than after the check it performs, per #890.
//
// So the empty value stays meaningful, and now carries three cases rather
// than "not yet": that migration window, a surface with no bearer path to
// narrow at all (`flow server --insecure-no-auth`, `flow server --dev`, and a
// trust policy of nothing but kind: mtls entries — see [AdmitsBearerTokens]),
// and any caller outside cmd/flow that has not made the decision. An empty
// resource is therefore ignored rather than refused, so a caller threading
// through an unset configuration value gets the unnarrowed default rather
// than a surface that refuses everyone. Which of those a given deployment is
// in, is decided by the command; this package stays out of command-line
// policy.
//
// Only bearer tokens are narrowed. A client certificate carries no audience,
// so a request authenticated by [WithPeerVerifier] alone is unaffected; a
// request carrying both is refused unless the token also names this resource,
// which is the same "both must verify and agree" rule that path already has.
func WithExpectedResource(resource string) AuthenticatorOption {
	return func(a *Authenticator) {
		a.expectedResource = resource
	}
}

// NewAuthenticator returns an Authenticator that authenticates requests with the
// given verifier.
//
// A nil verifier yields an Authenticator that rejects every request, because the
// alternative, admitting everyone when authentication was left unconfigured, is
// how a service ends up unauthenticated in production.
func NewAuthenticator(verifier Verifier, opts ...AuthenticatorOption) *Authenticator {
	authenticator := &Authenticator{verifier: verifier}

	for _, opt := range opts {
		opt(authenticator)
	}

	return authenticator
}

// Authenticate verifies the request's bearer token and returns the
// authenticated [Principal], which [authn.Middleware] attaches to the request
// context.
//
// Its signature matches [authn.AuthFunc], so it can be passed directly to
// [authn.NewMiddleware]. It is safe for concurrent use.
//
// The returned error is always a [connect.CodeUnauthenticated] error whose
// message is a short, fixed reason such as "token is expired". The full cause,
// which may name the trust policy's expected audiences or claim values, goes to
// the [WithFailureObserver] callback instead of to the caller, so an
// unauthenticated caller cannot probe the configuration.
//
// # Client certificates
//
// When [WithPeerVerifier] configured one and req carries a verified client
// certificate chain, that chain — never a raw, unverified certificate — is
// handed to the [PeerVerifier]. A verification failure there denies the
// request outright; it never falls back to the bearer token, because a
// certificate that failed policy is a caller this Authenticator has already
// formed an opinion about, not a caller with no opinion formed. A request
// carrying both a verified certificate and a bearer token is accepted only
// when both verify and agree on the same principal — see [ErrAmbiguousIdentity].
func (a *Authenticator) Authenticate(ctx context.Context, req *http.Request) (any, error) {
	// Substituted here rather than in the constructor so that the zero
	// Authenticator rejects requests too.
	verifier := a.verifier
	if verifier == nil {
		verifier = unconfiguredVerifier{}
	}

	// An absent or non-bearer Authorization header yields the empty token,
	// which every Verifier rejects. Requests with no credentials therefore take
	// the same path as requests with bad ones.
	rawToken, _ := authn.BearerToken(req)

	tokenPrincipal, tokenErr := verifier.Verify(ctx, rawToken)
	if tokenErr == nil && tokenPrincipal.IsZero() {
		// A Verifier that vouches for nobody has not authenticated anyone,
		// whatever it returned. Reaching a handler with an identity that reads as
		// unauthenticated is worse than a rejection.
		tokenErr = fmt.Errorf("%w: verifier returned no identity", ErrNoToken)
	}

	// Refused here, on the Principal the Verifier returned, and not only inside
	// [OIDCVerifier.Verify]. That is the seam where the repository's own
	// verifier performs this refusal, but an Authenticator holds *any*
	// [Verifier] — a custom implementation that returns a non-zero Principal
	// carrying a delegation claim would otherwise be admitted here while the
	// MCP surface refused the same Principal (mcpverifier.go runs the same
	// helper for exactly this reason). Checking it at the surface as well as in
	// the default verifier is what keeps the two surfaces symmetric for every
	// Verifier rather than only for [OIDCVerifier], which is the whole point of
	// the change this belongs to. Recorded as a token failure, not returned, so
	// the mTLS paths below treat it as any other invalid token.
	if tokenErr == nil {
		if err := refuseDelegationClaims(tokenPrincipal.Claims); err != nil {
			tokenErr = err
		}
	}

	// Recorded as a token failure rather than returned here, so that the mTLS
	// paths below treat a token for the wrong resource exactly as they treat
	// any other invalid token: no fallback to the certificate, and no
	// precedence rule invented for this one check. See [WithExpectedResource]
	// for why an unset resource narrows nothing.
	if tokenErr == nil && a.expectedResource != "" && !tokenPrincipal.HasAudience(a.expectedResource) {
		// The resource is not named in the error: a caller holding a token for
		// some other service learns its audience was wrong, and does not learn
		// this deployment's resource identifier from a failure — that is
		// published in the RFC 9728 document the challenge points at, which is
		// where a client is meant to read it.
		tokenErr = fmt.Errorf("%w: the token's audience does not name this resource", ErrInvalidAudience)
	}

	// req.TLS.VerifiedChains is set by crypto/tls itself, only once the peer's
	// certificate has been verified against the listener's ClientCAs — nothing
	// here re-verifies it or reads req.TLS.PeerCertificates instead. No peer
	// verifier configured, or no verified chain on this connection, is "no
	// certificate is in play", and the bearer-token outcome above stands
	// unchanged — this is what keeps every deployment that never turns mTLS on
	// seeing no behavior change at all.
	if a.peerVerifier == nil || req.TLS == nil || len(req.TLS.VerifiedChains) == 0 {
		if tokenErr != nil {
			if a.observe != nil {
				a.observe(ctx, req, tokenErr)
			}
			return nil, a.unauthenticated(tokenErr)
		}
		return tokenPrincipal, nil
	}

	peerPrincipal, peerErr := a.peerVerifier.VerifyPeer(ctx, req.TLS.VerifiedChains)
	if peerErr == nil && peerPrincipal.IsZero() {
		peerErr = fmt.Errorf("%w: peer verifier returned no identity", ErrNoToken)
	}
	if peerErr != nil {
		if a.observe != nil {
			a.observe(ctx, req, peerErr)
		}
		return nil, a.unauthenticated(peerErr)
	}

	if rawToken != "" {
		// A bearer token arrived alongside a verified client certificate. Per
		// CLAUDE.md's "fail closed": an ambiguous identity on a control plane
		// that mints workload assertions is refused rather than resolved by a
		// precedence rule, whichever the token or the certificate names an
		// invalid principal or the two disagree.
		if tokenErr != nil {
			if a.observe != nil {
				a.observe(ctx, req, tokenErr)
			}
			return nil, a.unauthenticated(tokenErr)
		}
		if tokenPrincipal.ID() != peerPrincipal.ID() {
			err := fmt.Errorf("%w: certificate names %q, token names %q",
				ErrAmbiguousIdentity, peerPrincipal.ID(), tokenPrincipal.ID())
			if a.observe != nil {
				a.observe(ctx, req, err)
			}
			return nil, a.unauthenticated(err)
		}
	}

	return peerPrincipal, nil
}

// unauthenticated renders a verification failure as the error a caller sees: the
// RFC 6750 challenge that tells a client its token is the problem, and a short
// reason that describes the failure without describing the trust policy.
//
// The challenge itself is rendered by [bearerChallenge], which is also where
// the parameters this deployment deliberately does not send are written down.
func (a *Authenticator) unauthenticated(cause error) error {
	err := connect.NewError(connect.CodeUnauthenticated, errors.New(publicReason(cause)))
	err.Meta().Set("WWW-Authenticate", bearerChallenge("invalid_token", a.challengeMetadataURL()))
	return err
}

// challengeMetadataURL is the RFC 9728 document this surface's challenge may
// name: the one [WithProtectedResource] configured, or nothing when that
// document describes a resource this surface would not accept a token for.
//
// The two options are independent, and a deployment is expected to set both
// with *different* values — that is the whole point of picatz/flowstate#1007,
// and what docs/DEPLOYMENT.md now recommends. Naming the document anyway
// would make the challenge an instruction to go and mint the one audience
// [WithExpectedResource] has been told to refuse, and a discovery-driven
// client that followed it would loop: fetch, mint, 401, fetch. Silence is the
// honest answer — RFC 9728 section 5.1 makes the parameter optional, and a
// caller reading a bare challenge learns "your token is not accepted here"
// and no falsehood.
//
// It costs discovery on a surface that has narrowed itself, and that cost is
// paid deliberately rather than papered over: publishing a *second* RFC 9728
// document, for the RPC resource, is the thing that would buy it back, and
// that is a route to mount, a collision to check against the three
// serverHandler already registers, and a decision about a document whose
// resource path ("/rpc") is not where Connect RPC actually answers ("/", plus
// each procedure). It is a slice, not a line, and it is left as one.
//
// An unnarrowed surface (expectedResource empty — the migration window, the
// insecure and dev servers, a certificate-only policy) keeps the document it
// always named: it still accepts every audience its trust policy lists, so
// the one the document advertises is among them and the instruction is good.
func (a *Authenticator) challengeMetadataURL() string {
	if a.expectedResource != "" && a.expectedResource != a.protectedResource {
		return ""
	}

	return a.protectedResourceMetadataURL
}

// PrincipalFromContext returns the [Principal] that [Authenticator.Authenticate]
// attached to a request context, reporting false when the request was not
// authenticated.
//
// Handlers should treat a false result as a bug rather than as an anonymous
// caller: the middleware rejects unauthenticated requests before they reach a
// handler, so reaching one without a Principal means the middleware was not
// installed.
func PrincipalFromContext(ctx context.Context) (Principal, bool) {
	principal, ok := authn.GetInfo(ctx).(Principal)
	return principal, ok
}

// ContextWithPrincipal returns a context carrying the given [Principal], as if
// it had been authenticated by an [Authenticator]. It is for tests and for
// callers that authenticate outside of Connect middleware.
func ContextWithPrincipal(ctx context.Context, principal Principal) context.Context {
	return authn.SetInfo(ctx, principal)
}

// InsecureAnonymousVerifier returns a [Verifier] that authenticates every
// request as [AnonymousPrincipal] without a token, without a signature, and
// without a trust policy.
//
// It exists so that running Flowstate locally, without an identity provider,
// stays one explicit flag away. It is never reached by accident: it has to be
// constructed and passed to [NewAuthenticator] by name, an unconfigured or zero
// [Authenticator] rejects every request instead, and the callers it admits are
// recognizable with [Principal.IsAnonymous] so authorization can refuse them
// anything privileged.
//
// # Warning
//
// A server using this has no authentication whatsoever. Anyone who can reach the
// port can start and inspect workflows. Never enable it on a network anyone else
// can reach.
func InsecureAnonymousVerifier() Verifier {
	return insecureAnonymousVerifier{}
}

// insecureAnonymousVerifier admits everyone. See [InsecureAnonymousVerifier].
type insecureAnonymousVerifier struct{}

// Verify returns the anonymous principal, ignoring the token entirely.
func (insecureAnonymousVerifier) Verify(context.Context, string) (Principal, error) {
	return AnonymousPrincipal(), nil
}

// unconfiguredVerifier stands in for a missing [Verifier] and rejects
// everything.
type unconfiguredVerifier struct{}

// Verify always fails.
func (unconfiguredVerifier) Verify(context.Context, string) (Principal, error) {
	return Principal{}, fmt.Errorf("%w: no token verifier is configured", ErrInvalidPolicy)
}
