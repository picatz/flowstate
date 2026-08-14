package main

import (
	"context"
	"log/slog"
	"net/http"
	"net/http/pprof"

	"connectrpc.com/authn"
	"github.com/picatz/flowstate/pkg/flowstate/v1/auth"
	"github.com/picatz/flowstate/pkg/flowstate/v1/server"
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
//
// # Why rejections are logged here
//
// The error an unauthenticated caller receives deliberately says very little, so
// without the failure observer a rejection is invisible to the server too — a
// misconfigured CI job and a probe both look like silence. What is logged is the
// classified reason from [auth.PublicReason], never the raw error: the full
// cause can carry the wrapped text of a parse failure, and the token itself is
// in the request's Authorization header, which is exactly why the observer logs
// fields it chooses rather than the request.
//
// # Why the webhook receiver sits outside authentication too
//
// A webhook sender holds no Flowstate credential and never will: a payments
// provider POSTs to a URL and signs the body with a shared key. That signature
// *is* the authentication, checked by the receiver against the key the trigger's
// `verify:` names, and a delivery that does not verify is refused — so the route
// is unauthenticated in the sense that no bearer token is required and in no other
// sense. Wrapping it in the authenticator would make a webhook impossible to
// deliver to rather than making it safer.
//
// It is mounted only when a deployment asked for it (--webhook). A deployment that
// did not has no such route at all, which is the fail-closed default this file's
// per-route wrapping exists to keep.
func serverHandler(
	logger *slog.Logger, verifier auth.Verifier, broker *auth.Broker,
	rpc http.Handler, webhooks *server.WebhookReceiver,
) http.Handler {
	authenticated := authn.NewMiddleware(auth.NewAuthenticator(verifier,
		auth.WithFailureObserver(func(ctx context.Context, req *http.Request, err error) {
			logger.WarnContext(ctx, "rejected unauthenticated request",
				"procedure", req.URL.Path,
				"peer", req.RemoteAddr,
				"reason", auth.PublicReason(err))
		}),
	).Authenticate)

	mux := http.NewServeMux()
	mux.Handle("/", authenticated.Wrap(rpc))

	// Liveness, deliberately unauthenticated and deliberately empty-handed. A
	// load balancer or an orchestrator probes before it holds any credential —
	// the same reason the discovery documents below sit outside the mux — and
	// what it needs is a status code, not information: no version, no config,
	// no dependency states, because an unauthenticated endpoint that describes
	// the deployment is reconnaissance served on request. GET and HEAD only,
	// mirroring the identity documents' handler.
	mux.HandleFunc("/healthz", healthzHandler())

	// Typed rather than an [http.Handler], so that "this deployment configured
	// no webhooks" is a nil pointer this can see: a nil handler in an interface
	// is a non-nil interface, and mounting one would serve the route and panic on
	// the first delivery.
	if webhooks != nil {
		mux.Handle(server.WebhookPathPrefix, webhooks)
	}

	if broker != nil {
		issuer := broker.Issuer()
		mux.Handle(auth.DiscoveryPath, issuer.Handler())
		mux.Handle(issuer.JWKSPath(), issuer.Handler())
	}

	return mux
}

// healthzHandler answers a liveness probe with a status code and nothing
// else, GET and HEAD only. Shared between the public mux above and the
// internal one below: the check itself is one fact ("this process can
// answer HTTP"), and duplicating the route is a judgement call explained on
// [internalHandler] — the two must still agree on what it answers.
func healthzHandler() http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet && r.Method != http.MethodHead {
			w.WriteHeader(http.StatusMethodNotAllowed)
			return
		}
		w.WriteHeader(http.StatusOK)
	}
}

// internalHandler routes the internal listener (cmd/flow/internallistener.go):
// health and pprof, on a socket separate from the public one.
//
// # Why pprof is not on the public mux
//
// pprof's profile and trace endpoints can read this process's running
// goroutines and a slice of its memory — reconnaissance considerably more
// useful than the empty body [healthzHandler] deliberately limits itself to.
// The public listener answers the internet, or at least whatever network
// reaches it; pprof belongs on a socket an operator binds to somewhere
// private instead, which is the internal listener's whole reason to exist as
// a second bind rather than more routes here.
//
// No metrics endpoint yet: this deployment's telemetry is OTLP-push only
// (see telemetry.go), and standing up a Prometheus-shaped `/metrics` scrape
// target well means adding a registry and an exporter this tree does not
// carry today — a second telemetry pipeline to keep in sync with the OTLP
// one, not a route. Left for the slice that adds it deliberately rather than
// folded in here because the socket happened to be free.
//
// # Why /healthz is duplicated rather than moved
//
// The public route predates this listener and existing infrastructure — a
// load balancer, a readiness probe already pointed at the public port —
// depends on it answering there; moving it would be a breaking change this
// slice does not need to make. Duplicating it costs nothing (the same
// contentless handler) and gives an operator who wants their prober off the
// public listener entirely somewhere to point it. See serverHandler's own
// doc for why the public copy stays unauthenticated and empty-handed.
func internalHandler(logger *slog.Logger) http.Handler {
	mux := http.NewServeMux()

	mux.HandleFunc("/healthz", healthzHandler())

	// Wired by hand rather than importing net/http/pprof for its
	// init-time registration onto http.DefaultServeMux: that global is
	// reachable from any package that happens to import pprof transitively,
	// which is exactly the kind of route nobody deliberately mounted that
	// this file's own comments warn about elsewhere. This mux, and no other.
	mux.HandleFunc("/debug/pprof/", pprof.Index)
	mux.HandleFunc("/debug/pprof/cmdline", pprof.Cmdline)
	mux.HandleFunc("/debug/pprof/profile", pprof.Profile)
	mux.HandleFunc("/debug/pprof/symbol", pprof.Symbol)
	mux.HandleFunc("/debug/pprof/trace", pprof.Trace)

	return mux
}
