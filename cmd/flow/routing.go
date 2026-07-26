package main

import (
	"net/http"

	"connectrpc.com/authn"
	"github.com/picatz/flowstate/pkg/flowstate/v1/auth"
)

// serverHandler routes the server's HTTP surface, deciding what authentication
// applies where.
//
// # Why the identity documents sit outside authentication
//
// A relying party fetches the discovery document and the key set *before* it holds
// any credential to present. That is the whole point of them, and they contain only
// public keys — there is nothing there to protect.
//
// Serving them from inside the authenticated mux is the usual reason a working
// federation setup silently stops verifying. The peer asks for the key set, receives
// a 401 where it expected JSON, caches nothing, and every assertion Flowstate issues
// starts being rejected. The symptom points at signing, so that is where someone
// looks, and the cause is a route.
//
// # Why the wrapping is per-route rather than global
//
// The default route is the wrapped one, so a route registered later is authenticated
// unless someone deliberately adds it here. That keeps the fail-closed property: the
// mistake this shape allows is a public endpoint accidentally requiring a credential,
// which is loud, rather than a private one accidentally not requiring one, which is
// silent.
//
// A deployment that does not federate outward has no broker, and then nothing is
// mounted unauthenticated at all.
func serverHandler(verifier auth.Verifier, broker *auth.Broker, rpc http.Handler) http.Handler {
	authenticated := authn.NewMiddleware(auth.NewAuthenticator(verifier).Authenticate)

	mux := http.NewServeMux()
	mux.Handle("/", authenticated.Wrap(rpc))

	if broker != nil {
		issuer := broker.Issuer()
		mux.Handle(auth.DiscoveryPath, issuer.Handler())
		mux.Handle(issuer.JWKSPath(), issuer.Handler())
	}

	return mux
}
