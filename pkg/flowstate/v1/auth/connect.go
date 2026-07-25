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
type Authenticator struct {
	verifier Verifier
	observe  func(context.Context, *http.Request, error)
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

	principal, err := verifier.Verify(ctx, rawToken)
	if err == nil && principal.IsZero() {
		// A Verifier that vouches for nobody has not authenticated anyone,
		// whatever it returned. Reaching a handler with an identity that reads as
		// unauthenticated is worse than a rejection.
		err = fmt.Errorf("%w: verifier returned no identity", ErrNoToken)
	}
	if err != nil {
		if a.observe != nil {
			a.observe(ctx, req, err)
		}
		return nil, unauthenticated(err)
	}

	return principal, nil
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
