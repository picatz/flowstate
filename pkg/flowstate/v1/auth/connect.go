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
	verifier     Verifier
	peerVerifier PeerVerifier
	observe      func(context.Context, *http.Request, error)
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
			return nil, unauthenticated(tokenErr)
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
		return nil, unauthenticated(peerErr)
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
			return nil, unauthenticated(tokenErr)
		}
		if tokenPrincipal.ID() != peerPrincipal.ID() {
			err := fmt.Errorf("%w: certificate names %q, token names %q",
				ErrAmbiguousIdentity, peerPrincipal.ID(), tokenPrincipal.ID())
			if a.observe != nil {
				a.observe(ctx, req, err)
			}
			return nil, unauthenticated(err)
		}
	}

	return peerPrincipal, nil
}

// unauthenticated renders a verification failure as the error a caller sees: the
// RFC 6750 challenge that tells a client its token is the problem, and a short
// reason that describes the failure without describing the trust policy.
func unauthenticated(cause error) error {
	err := connect.NewError(connect.CodeUnauthenticated, errors.New(publicReason(cause)))
	err.Meta().Set("WWW-Authenticate", `Bearer error="invalid_token"`)
	return err
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
