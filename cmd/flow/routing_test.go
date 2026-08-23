package main

import (
	"bytes"
	"context"
	"crypto/ed25519"
	"encoding/json"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/spf13/cobra"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/baggage"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/trace"

	v1 "github.com/picatz/flowstate/pkg/flowstate/v1"
	"github.com/picatz/flowstate/pkg/flowstate/v1/auth"
	"github.com/picatz/flowstate/pkg/flowstate/v1/secrets"
)

// TestServerHandlerContainsInboundBaggage exercises the public handler rather
// than the filter in isolation, and pins two separate claims: *what* survives
// (nothing of the caller's baggage; all of its trace context) and *where* the
// removal happens (before the authenticator looks at the request, which is
// what makes it before everything else too).
//
// The ordering half is the one that is easy to write vacuously. Asserting that
// the context the authenticator runs under carries no baggage proves nothing,
// because nothing on the authentication path ever extracts baggage into a
// context — that assertion stays green with this whole feature deleted. What
// the filter actually controls is the header, and [http.Header] is a map: the
// wrapper mutates the very map the authenticator, the RPC route and every
// unauthenticated route below then read. So the verifier snapshots that live
// map at the moment it is called, and the snapshot is empty only if the
// removal already happened.
func TestServerHandlerContainsInboundBaggage(t *testing.T) {
	isolateTelemetry(t)
	t.Setenv("OTEL_EXPORTER_OTLP_ENDPOINT", "http://telemetry.invalid")
	otel.SetTextMapPropagator(propagation.NewCompositeTextMapPropagator(
		propagation.TraceContext{}, propagation.Baggage{}))

	type seen struct {
		header http.Header
		bag    baggage.Baggage
		span   trace.SpanContext
	}
	// inFlight is the live header map of the request currently being served,
	// handed over by request() below. Cloning it inside the verifier is what
	// dates the observation: whatever the authenticator sees, this sees.
	var inFlight http.Header
	var authenticated, instrumented, handled []seen
	verifier := recordingVerifier{record: func(ctx context.Context) {
		authenticated = append(authenticated, seen{
			header: inFlight.Clone(),
			bag:    baggage.FromContext(ctx),
			span:   trace.SpanContextFromContext(ctx),
		})
	}}
	rpc := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		ctx := otel.GetTextMapPropagator().Extract(r.Context(), propagation.HeaderCarrier(r.Header))
		instrumented = append(instrumented, seen{header: r.Header.Clone(), bag: baggage.FromContext(ctx), span: trace.SpanContextFromContext(ctx)})
		r = r.WithContext(ctx)
		handled = append(handled, seen{header: r.Header.Clone(), bag: baggage.FromContext(r.Context()), span: trace.SpanContextFromContext(r.Context())})
		w.WriteHeader(http.StatusNoContent)
	})
	handler := serverHandler(discardLogger(), verifier, nil, testBroker(t), rpc, nil, nil)

	const traceparent = "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01"
	request := func(path string, lines ...string) *httptest.ResponseRecorder {
		t.Helper()
		req := httptest.NewRequest(http.MethodGet, path, nil)
		req.Header.Set("aUtHoRiZaTiOn", "Bearer accepted")
		req.Header.Set("tRaCePaReNt", traceparent)
		for i, line := range lines {
			name := "Baggage"
			if i%2 == 1 {
				name = "bAgGaGe"
			}
			req.Header.Add(name, line)
		}
		inFlight = req.Header
		rec := httptest.NewRecorder()
		handler.ServeHTTP(rec, req)
		return rec
	}

	// Every member here is a shape the earlier allowlist would have kept or
	// half-kept: a bare value under a name that looks like this repository's,
	// the same name carrying a member *property* (`;token=...`, which a
	// whole-Member allowlist forwards verbatim), a credential-looking key, and
	// a tenant a caller must never get to assert.
	request("/flowstate.v1.WorkflowService/Run",
		"flowstate.workflow.name=orders;token=sk-live-abcdef,unknown=drop,authorization=secret",
		"flowstate.run.id=run-7,flowstate.tenant=forged")

	require.Len(t, authenticated, 1)
	require.Empty(t, authenticated[0].header.Values("Baggage"),
		"the authenticator observed the caller's baggage, so the removal is not before authentication")
	require.Equal(t, traceparent, authenticated[0].header.Get("Traceparent"),
		"trace context was removed along with baggage")

	require.Len(t, handled, 1)
	require.Empty(t, handled[0].header.Values("Baggage"), "a Baggage header crossed the RPC boundary")
	require.Empty(t, handled[0].bag.Members(), "caller baggage reached the RPC context")
	require.Empty(t, instrumented[0].header.Values("Baggage"))
	require.NotContains(t, strings.Join(instrumented[0].header.Values("Baggage"), ","), "sk-live-abcdef")
	require.Equal(t, traceparent, instrumented[0].header.Get("Traceparent"))
	require.True(t, handled[0].span.IsValid(), "removing baggage damaged trace context")

	// Shapes a parsing filter had to reason about and this one does not: a
	// header that is not baggage at all, a member count past any plausible
	// bound, and a value large enough that joining it before deciding would be
	// the allocation the bound exists to avoid. All three are one answer here,
	// which is the point — they are listed so a future filter that starts
	// parsing again cannot quietly lose them.
	for name, lines := range map[string][]string{
		"malformed": {"flowstate.workflow.name=orders", "%not-baggage"},
		"member count spread across header lines": {
			"flowstate.workflow.name=orders,k0=v,k1=v,k2=v,k3=v,k4=v,k5=v,k6=v,k7=v",
			"k8=v,k9=v,k10=v,k11=v,k12=v,k13=v,k14=v,k15=v",
		},
		"oversized value spread across header lines": {
			"flowstate.workflow.name=orders", "unknown=" + strings.Repeat("x", 8192),
		},
		"properties on an otherwise plausible key": {
			"flowstate.task.name=charge;secret=sk-live-abcdef;scope=admin",
		},
	} {
		t.Run(name, func(t *testing.T) {
			before := len(handled)
			request("/flowstate.v1.WorkflowService/Run", lines...)
			require.Len(t, handled, before+1)
			require.Empty(t, handled[before].header.Values("Baggage"))
			require.Empty(t, handled[before].bag.Members())
			require.True(t, handled[before].span.IsValid())
		})
	}

	// The unauthenticated routes are the other half of "before authentication":
	// they are not wrapped by the authenticator at all, so a filter mounted
	// inside that wrap would leave them reading the caller's header. They must
	// still answer — removing a header is not allowed to break a liveness probe
	// or a relying party's fetch of the key set.
	for _, path := range []string{"/healthz", auth.DiscoveryPath, testBroker(t).Issuer().JWKSPath()} {
		resp := request(path, "flowstate.workflow.name=orders")
		require.Equal(t, http.StatusOK, resp.Code, "%s stopped answering", path)
	}
}

// TestFilterServerBaggageRemovesEverySpelling is the unit-level complement:
// repeated lines, mixed case, and a member carrying properties are one header
// by the time [filterServerBaggage] runs, and none of it survives — while a
// request carrying no baggage at all is left exactly as it arrived.
func TestFilterServerBaggageRemovesEverySpelling(t *testing.T) {
	t.Parallel()

	header := http.Header{}
	header.Set("Traceparent", "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01")
	header.Add("Baggage", "flowstate.workflow.name=orders;token=sk-live-abcdef")
	header.Add("bAgGaGe", "unknown=drop")
	header.Add("BAGGAGE", "authorization=secret")

	filterServerBaggage(header)

	require.Empty(t, header.Values("Baggage"))
	require.Empty(t, header.Values("bAgGaGe"))
	require.NotContains(t, header.Get("Traceparent"), "sk-live")
	require.Len(t, header, 1, "filtering touched a header that is not baggage: %v", header)

	untouched := http.Header{"Traceparent": []string{"tp"}, "Authorization": []string{"Bearer x"}}
	filterServerBaggage(untouched)
	require.Equal(t, http.Header{"Traceparent": []string{"tp"}, "Authorization": []string{"Bearer x"}}, untouched)
}

type recordingVerifier struct{ record func(context.Context) }

func (v recordingVerifier) Verify(ctx context.Context, _ string) (auth.Principal, error) {
	v.record(ctx)
	return auth.Principal{Subject: "routing-test"}, nil
}

func TestServerHandlerLeavesPropagationAloneWhenTelemetryIsDisabled(t *testing.T) {
	isolateTelemetry(t)
	telemetryOff(t)

	var got http.Header
	handler := serverHandler(discardLogger(), recordingVerifier{record: func(context.Context) {}}, nil, nil,
		http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			got = r.Header.Clone()
			w.WriteHeader(http.StatusNoContent)
		}), nil, nil)
	req := httptest.NewRequest(http.MethodGet, "/flowstate.v1.WorkflowService/Get", nil)
	req.Header.Set("Authorization", "Bearer accepted")
	req.Header.Add("bAgGaGe", "unknown=still-present")
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)

	require.Equal(t, http.StatusNoContent, rec.Code)
	require.Equal(t, "unknown=still-present", got.Get("Baggage"), "telemetry-off changed the prior request surface")
	require.Empty(t, rec.Header().Values("Traceparent"))
	require.Empty(t, rec.Header().Values("Baggage"), "telemetry-off added propagation response headers")
}

// TestIdentityDocumentsAreReachableWithoutCredentials is the regression guard for
// the mistake this routing exists to prevent.
//
// A relying party fetches the discovery document and the key set before it holds any
// credential. If they end up behind authentication, federation stops working and the
// symptom — rejected assertions — points at signing rather than at a route. So this
// asserts the property directly: the documents answer with no credential, and the API
// still refuses one.
func TestIdentityDocumentsAreReachableWithoutCredentials(t *testing.T) {
	t.Parallel()

	broker := testBroker(t)

	// A verifier that refuses everything, so an authenticated route answering at
	// all would mean the middleware was not applied.
	handler := serverHandler(discardLogger(), refusingVerifier{}, nil, broker, http.HandlerFunc(
		func(w http.ResponseWriter, r *http.Request) {
			w.WriteHeader(http.StatusOK)
			_, _ = w.Write([]byte(`{"api":"reached"}`))
		},
	), nil, nil)

	server := httptest.NewServer(handler)
	defer server.Close()

	t.Run("discovery answers without a credential", func(t *testing.T) {
		response, err := server.Client().Get(server.URL + auth.DiscoveryPath)
		require.NoError(t, err)
		defer response.Body.Close()

		require.Equal(t, http.StatusOK, response.StatusCode,
			"the discovery document must be reachable unauthenticated; a relying party "+
				"fetches it before it has any credential to present")

		var document map[string]any
		require.NoError(t, json.NewDecoder(response.Body).Decode(&document))
		require.NotEmpty(t, document["issuer"], "discovery document should name its issuer")
		require.NotEmpty(t, document["jwks_uri"], "discovery document should name its key set")
	})

	t.Run("the key set answers without a credential", func(t *testing.T) {
		response, err := server.Client().Get(server.URL + broker.Issuer().JWKSPath())
		require.NoError(t, err)
		defer response.Body.Close()

		require.Equal(t, http.StatusOK, response.StatusCode,
			"the key set must be reachable unauthenticated; it contains only public keys")

		var document map[string]any
		require.NoError(t, json.NewDecoder(response.Body).Decode(&document))
		require.Contains(t, document, "keys")
	})

	t.Run("the API still refuses an unauthenticated caller", func(t *testing.T) {
		// The other half of the property. Without this, a handler that simply
		// applied no authentication anywhere would pass the two tests above.
		response, err := server.Client().Get(server.URL + "/flowstate.v1.WorkflowService/Run")
		require.NoError(t, err)
		defer response.Body.Close()

		require.NotEqual(t, http.StatusOK, response.StatusCode,
			"the API must not answer an unauthenticated caller; if it does, the "+
				"middleware is not wrapping the default route")
	})
}

// TestNoUnauthenticatedRoutesWithoutFederation checks that a deployment which does
// not federate outward exposes nothing unauthenticated at all.
func TestNoUnauthenticatedRoutesWithoutFederation(t *testing.T) {
	t.Parallel()

	handler := serverHandler(discardLogger(), refusingVerifier{}, nil, nil, http.HandlerFunc(
		func(w http.ResponseWriter, r *http.Request) { w.WriteHeader(http.StatusOK) },
	), nil, nil)

	server := httptest.NewServer(handler)
	defer server.Close()

	for _, path := range []string{auth.DiscoveryPath, auth.DefaultJWKSPath, "/"} {
		response, err := server.Client().Get(server.URL + path)
		require.NoError(t, err)
		response.Body.Close()

		require.NotEqual(t, http.StatusOK, response.StatusCode,
			"%s answered without a credential on a deployment that issues no assertions", path)
	}
}

// refusingVerifier rejects every credential, so any route that answers proves the
// authentication middleware was not applied to it.
type refusingVerifier struct{}

func (refusingVerifier) Verify(context.Context, string) (auth.Principal, error) {
	return auth.Principal{}, auth.ErrMalformedToken
}

// testBroker builds a broker with a throwaway signing key.
func testBroker(t *testing.T) *auth.Broker {
	t.Helper()

	_, private, err := ed25519.GenerateKey(nil)
	require.NoError(t, err)

	key, err := auth.NewSigningKey("test-key", private)
	require.NoError(t, err)

	issuer, err := auth.NewIssuer("https://flowstate.test", key)
	require.NoError(t, err)

	broker, err := auth.NewBroker(issuer)
	require.NoError(t, err)

	return broker
}

// TestHealthzAnswersWithoutCredentialsAndWithoutInformation pins both halves of
// the liveness route: a prober holding no credential gets its status code, and
// gets nothing else — an unauthenticated endpoint that described the deployment
// would be reconnaissance served on request.
func TestHealthzAnswersWithoutCredentialsAndWithoutInformation(t *testing.T) {
	t.Parallel()

	handler := serverHandler(discardLogger(), refusingVerifier{}, nil, nil, http.HandlerFunc(
		func(w http.ResponseWriter, r *http.Request) {
			t.Error("a health probe reached the RPC handler")
		}), nil, nil)

	server := httptest.NewServer(handler)
	defer server.Close()

	resp, err := http.Get(server.URL + "/healthz")
	require.NoError(t, err)
	defer resp.Body.Close()

	require.Equal(t, http.StatusOK, resp.StatusCode,
		"a load balancer probing before it holds any credential was refused")

	var body [64]byte
	n, _ := resp.Body.Read(body[:])
	require.Zero(t, n, "the health endpoint answered with content: %q", body[:n])

	// The method discipline the identity documents' handler keeps.
	post, err := http.Post(server.URL+"/healthz", "text/plain", nil)
	require.NoError(t, err)
	defer post.Body.Close()
	require.Equal(t, http.StatusMethodNotAllowed, post.StatusCode)
}

// discardLogger is a logger for tests that assert on behavior rather than on
// what was said about it.
func discardLogger() *slog.Logger {
	return slog.New(slog.DiscardHandler)
}

// TestARejectionIsLoggedWithoutTheToken pins the two halves of the failure
// observer at once: that a rejection is visible to the operator at all — the
// caller's error deliberately says almost nothing, so before the observer a
// misconfigured CI job and a probe were both silence — and that what becomes
// visible is the classified reason, never anything from the request's
// Authorization header.
func TestARejectionIsLoggedWithoutTheToken(t *testing.T) {
	t.Parallel()

	var buf bytes.Buffer
	logger := slog.New(slog.NewTextHandler(&buf, nil))

	handler := serverHandler(logger, refusingVerifier{}, nil, nil, http.HandlerFunc(
		func(w http.ResponseWriter, r *http.Request) {
			t.Error("a rejected request reached the RPC handler")
		}), nil, nil)

	const token = "not-a-real-credential-but-must-not-appear"

	req := httptest.NewRequest(http.MethodPost, "/flowstate.v1.WorkflowService/Run", nil)
	req.Header.Set("Authorization", "Bearer "+token)

	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)

	require.Equal(t, http.StatusUnauthorized, rec.Code)

	logged := buf.String()
	require.NotEmpty(t, logged, "a rejection produced no log line, which is the silence the observer exists to end")
	require.Contains(t, logged, "/flowstate.v1.WorkflowService/Run",
		"the log does not say which procedure was called")
	require.NotContains(t, logged, token,
		"the Authorization header's value reached the log")
}

// TestTheServerTakesTheIdentityFlags is the wiring check, in the same shape the
// plugin flags have: a flag this file reads that the command does not declare
// would read its zero value forever, and the failure would look like a policy
// that silently carries no claims.
func TestTheServerTakesTheIdentityFlags(t *testing.T) {
	t.Parallel()

	var server *cobra.Command
	for _, c := range newRootCommand().Commands() {
		if c.Name() == "server" {
			server = c

			break
		}
	}
	require.NotNil(t, server, "there is no server command")

	for _, name := range []string{
		"identity-claim", "deployment-name", "auth-policy", "identity-key",

		// The receiver's own surface, and the secret flags it cannot resolve a
		// `verify:` key without. A --webhook the command did not declare would
		// read its zero value forever, and the deployment would look like one
		// that simply serves no webhooks.
		"webhook", "secret-env", "secret-dir",

		// The public listener's TLS configuration and the internal listener's
		// address (cmd/flow/tls.go, cmd/flow/internallistener.go).
		"tls-cert-file", "tls-key-file", "tls-min-version", "internal-listen",
	} {
		require.NotNil(t, server.Flags().Lookup(name),
			"`flow server` does not take --%s, so a deployment cannot configure it", name)
	}
}

// TestPublicMuxDoesNotServePprof pins the complement of
// TestInternalListenerServesHealthAndPprofButNotTheRPCSurface: pprof can read
// this process's memory and running goroutines, and belongs only on the
// internal listener (routing.go's [internalHandler]). A request for it on the
// public mux must fall through to the authenticated default route like any
// other unrecognized path, not answer directly.
func TestPublicMuxDoesNotServePprof(t *testing.T) {
	t.Parallel()

	handler := serverHandler(discardLogger(), refusingVerifier{}, nil, nil, http.HandlerFunc(
		func(w http.ResponseWriter, r *http.Request) { w.WriteHeader(http.StatusOK) }), nil, nil)

	server := httptest.NewServer(handler)
	defer server.Close()

	resp, err := server.Client().Get(server.URL + "/debug/pprof/")
	require.NoError(t, err)
	defer resp.Body.Close()

	require.NotEqual(t, http.StatusOK, resp.StatusCode,
		"the public listener must not serve pprof; it fell through to the RPC handler "+
			"where the authenticator would have refused it, so an OK here means pprof is reachable")
}

// TestTheWebhookRouteIsMountedOnlyWhenConfigured pins both halves of the one
// route this server serves without a bearer token.
//
// A sender presents a signature over the body rather than a credential, so
// wrapping the receiver in the authenticator would make a webhook undeliverable.
// The other half is the fail-closed one: a deployment that configured no webhooks
// must have no such route, and a request there meets the authenticated default
// like anything else.
func TestTheWebhookRouteIsMountedOnlyWhenConfigured(t *testing.T) {
	t.Parallel()

	receiver, err := mustNewFlowstateServer(t, nil).NewWebhookReceiver(t.Context(), "",
		[]*v1.Workflow{{
			Name:    "order-webhook",
			Profile: v1.CurrentProfile,
			Triggers: &v1.Triggers{Webhooks: []*v1.WebhookTrigger{{
				Name: "storefront",
				Verify: map[string]*v1.Value{
					v1.WebhookSchemeHMACSHA256: {Kind: &v1.Value_SecretRef{
						SecretRef: &v1.SecretRef{Scheme: "env", Name: "K"},
					}},
				},
				IdempotencyKey: v1.NewExpr(`event.body.id`),
			}}},
			Steps: []*v1.Node{{Id: "record", Kind: &v1.Node_Value{Value: v1.NewLiteral("ok")}}},
		}}, staticStore(t))
	require.NoError(t, err)

	served := httptest.NewServer(serverHandler(discardLogger(), refusingVerifier{}, nil, nil,
		http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			t.Error("a delivery reached the RPC handler")
		}), receiver, nil))
	defer served.Close()

	resp, err := served.Client().Post(served.URL+"/webhooks/order-webhook/storefront",
		"application/json", bytes.NewReader([]byte(`{"id":"evt_1"}`)))
	require.NoError(t, err)
	defer resp.Body.Close()

	// The receiver answered — refusing the unsigned delivery, which is its job —
	// rather than the authenticator refusing the request for want of a token.
	require.Equal(t, http.StatusNotFound, resp.StatusCode,
		"a delivery was answered by something other than the receiver")

	unconfigured := httptest.NewServer(serverHandler(discardLogger(), refusingVerifier{}, nil, nil,
		http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { w.WriteHeader(http.StatusOK) }), nil, nil))
	defer unconfigured.Close()

	absent, err := unconfigured.Client().Post(unconfigured.URL+"/webhooks/order-webhook/storefront",
		"application/json", bytes.NewReader([]byte(`{"id":"evt_1"}`)))
	require.NoError(t, err)
	defer absent.Body.Close()

	require.Equal(t, http.StatusUnauthorized, absent.StatusCode,
		"a deployment that configured no webhooks served the webhook route anyway")
}

// TestTheServerTakesTheProtectedResourceFlags is the wiring check for
// picatz/flowstate#558's RFC 9728 slice, the same shape as
// TestTheServerTakesTheIdentityFlags above.
func TestTheServerTakesTheProtectedResourceFlags(t *testing.T) {
	t.Parallel()

	var server *cobra.Command
	for _, c := range newRootCommand().Commands() {
		if c.Name() == "server" {
			server = c

			break
		}
	}
	require.NotNil(t, server, "there is no server command")

	for _, name := range []string{"protected-resource", "authorization-server"} {
		require.NotNil(t, server.Flags().Lookup(name),
			"`flow server` does not take --%s, so a deployment cannot configure it", name)
	}
}

// TestProtectedResourceRouteMountedOnlyWhenConfigured pins both halves of
// #558's decision: unconfigured means the route does not exist at all — a
// 404, not an empty document — and configured means it serves the exact
// document [resolveProtectedResource] built.
func TestProtectedResourceRouteMountedOnlyWhenConfigured(t *testing.T) {
	t.Parallel()

	policy := &auth.Policy{Issuers: []auth.TrustedIssuer{
		{Name: "as", Issuer: "https://trusted.example.com", Audiences: []string{"https://flowstate.example.com/mcp"}},
	}}

	pr, err := resolveProtectedResource(protectedResourceFlags{
		resource:             "https://flowstate.example.com/mcp",
		authorizationServers: []string{"https://trusted.example.com"},
	}, policy)
	require.NoError(t, err)
	require.NotNil(t, pr)

	configured := httptest.NewServer(serverHandler(discardLogger(), refusingVerifier{}, nil, nil,
		http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { w.WriteHeader(http.StatusOK) }),
		nil, pr))
	defer configured.Close()

	resp, err := configured.Client().Get(configured.URL + pr.Path())
	require.NoError(t, err)
	defer resp.Body.Close()
	require.Equal(t, http.StatusOK, resp.StatusCode)

	var doc map[string]any
	require.NoError(t, json.NewDecoder(resp.Body).Decode(&doc))
	require.Equal(t, "https://flowstate.example.com/mcp", doc["resource"])
	require.NotContains(t, doc, "scopes_supported")

	unconfigured := httptest.NewServer(serverHandler(discardLogger(), refusingVerifier{}, nil, nil,
		http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { w.WriteHeader(http.StatusOK) }),
		nil, nil))
	defer unconfigured.Close()

	// No route is mounted at all: the mux's "/" pattern is what answers, and
	// that is the authenticated default route — so a GET with no credential
	// meets the authenticator, exactly as an unconfigured --webhook does (see
	// TestTheWebhookRouteIsMountedOnlyWhenConfigured). What must NOT happen is
	// the request reaching an RFC 9728 handler that serves an empty document.
	absent, err := unconfigured.Client().Get(unconfigured.URL + pr.Path())
	require.NoError(t, err)
	defer absent.Body.Close()
	require.Equal(t, http.StatusUnauthorized, absent.StatusCode,
		"an unconfigured deployment must have no such route: this path meets the authenticated "+
			"default instead of an RFC 9728 handler serving an empty document")
}

// TestProtectedResourceChallengeMatchesServedDocument is the end-to-end check
// that the 401 challenge points a caller at wherever the document is
// actually reachable — not merely at some URL that happens to look right.
func TestProtectedResourceChallengeMatchesServedDocument(t *testing.T) {
	t.Parallel()

	policy := &auth.Policy{Issuers: []auth.TrustedIssuer{
		{Name: "as", Issuer: "https://trusted.example.com", Audiences: []string{"https://flowstate.example.com/mcp"}},
	}}

	pr, err := resolveProtectedResource(protectedResourceFlags{
		resource:             "https://flowstate.example.com/mcp",
		authorizationServers: []string{"https://trusted.example.com"},
	}, policy)
	require.NoError(t, err)

	server := httptest.NewServer(serverHandler(discardLogger(), refusingVerifier{}, nil, nil,
		http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { w.WriteHeader(http.StatusOK) }),
		nil, pr))
	defer server.Close()

	resp, err := server.Client().Post(server.URL+"/flowstate.v1.WorkflowService/Run",
		"application/json", strings.NewReader("{}"))
	require.NoError(t, err)
	defer resp.Body.Close()
	require.Equal(t, http.StatusUnauthorized, resp.StatusCode)

	challenge := resp.Header.Get("WWW-Authenticate")
	require.Contains(t, challenge, `resource_metadata="`+pr.MetadataURL()+`"`)

	// The URL the challenge names is not merely well-formed; it is fetched
	// from this same server (rewritten onto the test server's own address,
	// since pr.MetadataURL() names the configured production host) and must
	// answer with a document RFC 9728 accepts.
	docResp, err := server.Client().Get(server.URL + pr.Path())
	require.NoError(t, err)
	defer docResp.Body.Close()
	require.Equal(t, http.StatusOK, docResp.StatusCode)

	var doc map[string]any
	require.NoError(t, json.NewDecoder(docResp.Body).Decode(&doc))
	require.Equal(t, "https://flowstate.example.com/mcp", doc["resource"])
	require.Equal(t, []any{"https://trusted.example.com"}, doc["authorization_servers"])
}

// TestProtectedResourceChallengeUnaffectedByForgedHost is the #1 named risk:
// a caller forging the Host header on its own rejected request must not be
// able to steer the challenge, or the document it points at, toward
// anywhere but the configured resource.
func TestProtectedResourceChallengeUnaffectedByForgedHost(t *testing.T) {
	t.Parallel()

	policy := &auth.Policy{Issuers: []auth.TrustedIssuer{
		{Name: "as", Issuer: "https://trusted.example.com", Audiences: []string{"https://flowstate.example.com/mcp"}},
	}}

	pr, err := resolveProtectedResource(protectedResourceFlags{
		resource:             "https://flowstate.example.com/mcp",
		authorizationServers: []string{"https://trusted.example.com"},
	}, policy)
	require.NoError(t, err)

	server := httptest.NewServer(serverHandler(discardLogger(), refusingVerifier{}, nil, nil,
		http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { w.WriteHeader(http.StatusOK) }),
		nil, pr))
	defer server.Close()

	req, err := http.NewRequest(http.MethodPost, server.URL+"/flowstate.v1.WorkflowService/Run",
		strings.NewReader("{}"))
	require.NoError(t, err)
	req.Host = "attacker.example.com"
	req.Header.Set("X-Forwarded-Host", "attacker.example.com")

	resp, err := server.Client().Do(req)
	require.NoError(t, err)
	defer resp.Body.Close()

	challenge := resp.Header.Get("WWW-Authenticate")
	require.Equal(t, `Bearer error="invalid_token", resource_metadata="`+pr.MetadataURL()+`"`, challenge)
	require.NotContains(t, challenge, "attacker.example.com")
}

// TestResolveProtectedResourceRefusesUntrustedAuthorizationServer is
// cmd/flow's wiring layer over auth.NewProtectedResource's own fail-closed
// check, pinned here so a regression that skipped passing policy through
// would be caught at this seam too.
func TestResolveProtectedResourceRefusesUntrustedAuthorizationServer(t *testing.T) {
	t.Parallel()

	policy := &auth.Policy{Issuers: []auth.TrustedIssuer{
		{Name: "as", Issuer: "https://trusted.example.com", Audiences: []string{"https://flowstate.example.com/mcp"}},
	}}

	_, err := resolveProtectedResource(protectedResourceFlags{
		resource:             "https://flowstate.example.com/mcp",
		authorizationServers: []string{"https://rogue.example.com"},
	}, policy)

	require.Error(t, err)
	require.ErrorContains(t, err, "rogue.example.com")
}

// TestResolveProtectedResourceRejectsAuthorizationServerWithoutResource pins
// the flag-pairing check: --authorization-server names something to
// advertise it *for*, and without --protected-resource there is nothing.
func TestResolveProtectedResourceRejectsAuthorizationServerWithoutResource(t *testing.T) {
	t.Parallel()

	_, err := resolveProtectedResource(protectedResourceFlags{
		authorizationServers: []string{"https://trusted.example.com"},
	}, nil)

	require.Error(t, err)
}

// TestResolveProtectedResourceUnconfiguredIsNil pins the "absence is the
// whole answer" default: no flags given, no error, and nothing to mount.
func TestResolveProtectedResourceUnconfiguredIsNil(t *testing.T) {
	t.Parallel()

	pr, err := resolveProtectedResource(protectedResourceFlags{}, nil)
	require.NoError(t, err)
	require.Nil(t, pr)
}

// TestCheckProtectedResourceRouteCollisionRefusesJWKSPathCollision pins a
// review finding: federation's own `jwks_path` is operator-configurable, so
// a resource whose computed metadata path lands on it is reachable from
// ordinary configuration, not just a pathological resource URI. Left
// unchecked, serverHandler's second mux.Handle call panics at start-up
// (http.ServeMux refuses a duplicate pattern) instead of the two flags
// failing with a diagnosis.
func TestCheckProtectedResourceRouteCollisionRefusesJWKSPathCollision(t *testing.T) {
	t.Parallel()

	_, private, err := ed25519.GenerateKey(nil)
	require.NoError(t, err)
	key, err := auth.NewSigningKey("test-key", private)
	require.NoError(t, err)

	const collidingPath = auth.ProtectedResourceMetadataPath + "/mcp"

	issuer, err := auth.NewIssuer("https://flowstate.test", key, auth.WithJWKSPath(collidingPath))
	require.NoError(t, err)
	broker, err := auth.NewBroker(issuer)
	require.NoError(t, err)

	policy := &auth.Policy{Issuers: []auth.TrustedIssuer{
		{Name: "as", Issuer: "https://trusted.example.com", Audiences: []string{"https://flowstate.example.com/mcp"}},
	}}
	pr, err := resolveProtectedResource(protectedResourceFlags{
		resource:             "https://flowstate.example.com/mcp",
		authorizationServers: []string{"https://trusted.example.com"},
	}, policy)
	require.NoError(t, err)
	require.Equal(t, collidingPath, pr.Path(), "test setup: the two paths must actually collide")

	err = checkProtectedResourceRouteCollision(pr, broker)
	require.Error(t, err)
	require.ErrorContains(t, err, collidingPath)

	// And the positive control: what this check exists to prevent actually
	// panics serverHandler if the check is skipped.
	require.Panics(t, func() {
		serverHandler(discardLogger(), refusingVerifier{}, nil, broker,
			http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {}), nil, pr)
	}, "a colliding route should panic serverHandler's mux.Handle, which is exactly what the check must catch first")
}

// TestCheckProtectedResourceRouteCollisionAllowsTheOrdinaryCase pins the
// negative direction: a resource path that does not collide with anything
// passes with no error, and the same values TestTheServerTakesTheProtectedResourceFlags
// and friends already use are unaffected by this check's addition.
func TestCheckProtectedResourceRouteCollisionAllowsTheOrdinaryCase(t *testing.T) {
	t.Parallel()

	broker := testBroker(t)

	policy := &auth.Policy{Issuers: []auth.TrustedIssuer{
		{Name: "as", Issuer: "https://trusted.example.com", Audiences: []string{"https://flowstate.example.com/mcp"}},
	}}
	pr, err := resolveProtectedResource(protectedResourceFlags{
		resource:             "https://flowstate.example.com/mcp",
		authorizationServers: []string{"https://trusted.example.com"},
	}, policy)
	require.NoError(t, err)

	require.NoError(t, checkProtectedResourceRouteCollision(pr, broker))
	require.NoError(t, checkProtectedResourceRouteCollision(pr, nil))
	require.NoError(t, checkProtectedResourceRouteCollision(nil, broker))
}

// staticProvider resolves any reference to a fixed value, standing in for
// whatever backend a deployment configured.
type staticProvider struct{}

func (staticProvider) Scheme() string { return "env" }

func (staticProvider) Resolve(_ context.Context, req secrets.Request) (secrets.Secret, error) {
	return secrets.NewSecret(req.Ref, "whsec_routing_test"), nil
}

// staticStore is the secret machinery a receiver is handed here. The receiver
// scopes it to its own namespace itself, which is why it takes the store.
func staticStore(t *testing.T) *secrets.Store {
	t.Helper()

	store, err := secrets.NewStore(staticProvider{})
	require.NoError(t, err)

	return store
}
