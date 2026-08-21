package netpolicy

import (
	"context"
	"fmt"
	"net"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"

	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/baggage"
	"go.opentelemetry.io/otel/propagation"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"
	"go.opentelemetry.io/otel/trace"
)

// What these tests are for.
//
// A span leaves this process for a collector, is indexed there, and is read by
// people and systems with no relationship to the run that produced it — so the
// containment rule that governs workflow history governs a span attribute at
// least as hard. The request this round tripper describes is the one place in
// the tree where a credential is most likely to be sitting in the URL: in
// userinfo, in a webhook path segment, or in a query parameter.
//
// So the assertions below are written in the direction that can fail. Asserting
// that server.address is the host cannot catch the bug this instrumentation
// would have; rendering every recorded span through the %v family and requiring
// that a distinctive secret appears in none of them can.

// theURLSecret values are distinctive enough that a substring search cannot
// match one by accident, and each sits in a different part of a URL that a real
// credential really does get written into.
const (
	theQuerySecret    = "s3cr3t-query-token-that-must-never-be-exported"
	thePathSecret     = "s3cr3t-path-token-that-must-never-be-exported"
	theUserinfoSecret = "s3cr3t-userinfo-password-that-must-never-be-exported"
)

// recordSpans installs a recording tracer provider for the duration of a test
// and returns the recorder. Mirrors engine/tracing_test.go's helper of the same
// name, and restores the previous provider for the same reason: this binary is
// shared with every other test in the package.
func recordSpans(t *testing.T) *tracetest.SpanRecorder {
	t.Helper()

	recorder := tracetest.NewSpanRecorder()
	provider := sdktrace.NewTracerProvider(sdktrace.WithSpanProcessor(recorder))

	previous := otel.GetTracerProvider()
	otel.SetTracerProvider(provider)

	t.Cleanup(func() {
		otel.SetTracerProvider(previous)
		_ = provider.Shutdown(context.Background())
	})

	return recorder
}

// renderedSpans renders every recorded span through the %v family, over the
// batch, over each span individually, and over a struct holding one — the
// containment shapes CLAUDE.md names, since `fmt` reaching a value through an
// unexported field prints the fields rather than calling any accessor.
func renderedSpans(recorder *tracetest.SpanRecorder) []string {
	stubs := tracetest.SpanStubsFromReadOnlySpans(recorder.Ended())

	type wrapper struct {
		one   tracetest.SpanStub
		batch []tracetest.SpanStub
	}

	rendered := []string{
		fmt.Sprintf("%v", stubs),
		fmt.Sprintf("%+v", stubs),
		fmt.Sprintf("%#v", stubs),
	}

	if len(stubs) > 0 {
		w := wrapper{one: stubs[0], batch: stubs}
		rendered = append(rendered,
			fmt.Sprintf("%v", w), fmt.Sprintf("%+v", w), fmt.Sprintf("%#v", w))
	}

	for _, stub := range stubs {
		rendered = append(rendered,
			fmt.Sprintf("%v", stub),
			fmt.Sprintf("%+v", stub),
			fmt.Sprintf("%#v", stub),
			stub.Name,
			stub.Status.Description,
		)

		for _, attr := range stub.Attributes {
			rendered = append(rendered, string(attr.Key), attr.Value.String(),
				fmt.Sprintf("%v", attr), fmt.Sprintf("%+v", attr), fmt.Sprintf("%#v", attr))
		}

		for _, event := range stub.Events {
			rendered = append(rendered, event.Name, fmt.Sprintf("%+v", event), fmt.Sprintf("%#v", event))
		}

		for _, link := range stub.Links {
			rendered = append(rendered, fmt.Sprintf("%+v", link), fmt.Sprintf("%#v", link))
		}
	}

	return rendered
}

// requireNoMaterialInSpans is the assertion itself.
func requireNoMaterialInSpans(t *testing.T, recorder *tracetest.SpanRecorder, material string) {
	t.Helper()

	for _, rendered := range renderedSpans(recorder) {
		require.NotContains(t, rendered, material,
			"credential material reached a span, which is exported to a collector")
	}
}

// headerCapture records the headers of every request a test server received.
//
// Guarded, because the handler runs on the server's goroutine and the
// assertions run on the test's.
type headerCapture struct {
	mu      sync.Mutex
	headers []http.Header
}

func (c *headerCapture) record(h http.Header) {
	c.mu.Lock()
	defer c.mu.Unlock()

	c.headers = append(c.headers, h.Clone())
}

func (c *headerCapture) only(t *testing.T) http.Header {
	t.Helper()

	c.mu.Lock()
	defer c.mu.Unlock()

	require.Len(t, c.headers, 1, "want exactly one request to have reached the server")

	return c.headers[0]
}

// capturingServer starts a loopback server that records what it was sent.
func capturingServer(t *testing.T, capture *headerCapture) *httptest.Server {
	t.Helper()

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		capture.record(r.Header)
		fmt.Fprint(w, "ok")
	}))
	t.Cleanup(server.Close)

	return server
}

// spanStub returns the single recorded span, failing if there is not exactly
// one.
func spanStub(t *testing.T, recorder *tracetest.SpanRecorder) tracetest.SpanStub {
	t.Helper()

	stubs := tracetest.SpanStubsFromReadOnlySpans(recorder.Ended())
	require.Len(t, stubs, 1, "want exactly one recorded span")

	return stubs[0]
}

// attributesOf flattens a span's attributes for assertion.
func attributesOf(stub tracetest.SpanStub) map[string]string {
	attrs := make(map[string]string, len(stub.Attributes))
	for _, attr := range stub.Attributes {
		attrs[string(attr.Key)] = attr.Value.String()
	}

	return attrs
}

// TestClientSpanNamesTheCallAndNotItsContent is the containment assertion, and
// it is written in the direction that can fail: the request carries a
// recognizable credential in each of the three places a URL can hide one, and
// none of them may appear anywhere in the exported span.
func TestClientSpanNamesTheCallAndNotItsContent(t *testing.T) {
	recorder := recordSpans(t)

	var capture headerCapture
	server := capturingServer(t, &capture)

	policy, err := New(WithAllowLoopback())
	require.NoError(t, err)

	// The three hiding places, in one request: userinfo, a path segment (the
	// shape a chat webhook URL takes), and a query parameter (the shape the http
	// task's own schema comment warns about at HTTPTaskDef).
	target := fmt.Sprintf("http://someone:%s@%s/services/%s?token=%s",
		theUserinfoSecret, server.Listener.Addr().String(), thePathSecret, theQuerySecret)

	resp, err := get(t, policy, target)
	require.NoError(t, err)
	require.Equal(t, http.StatusOK, resp.StatusCode)

	stub := spanStub(t, recorder)

	require.Equal(t, "GET", stub.Name, "a client span is named for its method")
	require.Equal(t, trace.SpanKindClient, stub.SpanKind)

	host, port, err := net.SplitHostPort(server.Listener.Addr().String())
	require.NoError(t, err)

	// The whole attribute set, asserted as a set rather than key by key: a test
	// that checks the attributes it knows about cannot notice the one somebody
	// adds later, and the one somebody adds later is how a URL gets exported.
	require.Equal(t, map[string]string{
		"http.request.method":       "GET",
		"url.scheme":                "http",
		"server.address":            host,
		"server.port":               port,
		"http.response.status_code": "200",
	}, attributesOf(stub))

	requireNoMaterialInSpans(t, recorder, theUserinfoSecret)
	requireNoMaterialInSpans(t, recorder, thePathSecret)
	requireNoMaterialInSpans(t, recorder, theQuerySecret)
}

// TestClientSpanPropagatesTraceContextToThePeer asserts the header arrived,
// decoded, and names the span this side opened.
//
// The equality is the point. A test asserting only that a traceparent header is
// present passes against a hard-coded string; requiring the peer's extracted
// span context to name the recorded span's trace and span ids is what makes it
// a propagation test.
func TestClientSpanPropagatesTraceContextToThePeer(t *testing.T) {
	recorder := recordSpans(t)

	var capture headerCapture
	server := capturingServer(t, &capture)

	policy, err := New(WithAllowLoopback())
	require.NoError(t, err)

	resp, err := get(t, policy, server.URL)
	require.NoError(t, err)
	require.Equal(t, http.StatusOK, resp.StatusCode)

	received := capture.only(t)
	require.NotEmpty(t, received.Get("traceparent"), "the peer got no trace context to hang its own span on")

	// Extracted the way the receiving service would extract it, rather than
	// string-compared: what matters is that a peer's own instrumentation reads a
	// usable parent out of it.
	extracted := trace.SpanContextFromContext(propagation.TraceContext{}.Extract(
		context.Background(), propagation.HeaderCarrier(received)))

	require.True(t, extracted.IsValid(), "the traceparent the peer received does not decode")
	require.True(t, extracted.TraceID().IsValid(), "the peer received a zero trace id")

	stub := spanStub(t, recorder)
	require.Equal(t, stub.SpanContext.TraceID(), extracted.TraceID(),
		"the peer's parent trace is not the trace this side recorded")
	require.Equal(t, stub.SpanContext.SpanID(), extracted.SpanID(),
		"the peer's parent span is not the span this side opened")
}

// TestClientSpanDoesNotForwardBaggageToThePeer is the second containment
// direction, and the reason this file injects [propagation.TraceContext]
// explicitly rather than reaching for the globally registered propagator.
//
// The global one is a composite that also carries baggage, and baggage is
// caller-controlled data that this process may have put anything into. The peer
// is whatever host a workflow named. The plugin boundary already filters
// baggage down to two bounded members for a plugin the worker launched itself;
// an arbitrary third party gets less than that, not more.
func TestClientSpanDoesNotForwardBaggageToThePeer(t *testing.T) {
	recordSpans(t)

	var capture headerCapture
	server := capturingServer(t, &capture)

	policy, err := New(WithAllowLoopback())
	require.NoError(t, err)

	member, err := baggage.NewMember("tenant", theQuerySecret)
	require.NoError(t, err)
	bag, err := baggage.New(member)
	require.NoError(t, err)

	ctx := baggage.ContextWithBaggage(t.Context(), bag)

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, server.URL, nil)
	require.NoError(t, err)

	resp, err := policy.Client().Do(req)
	require.NoError(t, err)
	t.Cleanup(func() { resp.Body.Close() })

	received := capture.only(t)
	require.NotEmpty(t, received.Get("traceparent"), "trace context still has to reach the peer")
	require.Empty(t, received.Get("baggage"), "baggage was forwarded to an external peer")
	for _, values := range received {
		for _, value := range values {
			require.NotContains(t, value, theQuerySecret,
				"caller-controlled baggage reached an external peer")
		}
	}
}

// TestDeniedRequestIsTracedWithoutSayingWhatItRefused covers the outcome an
// operator most wants to find and the one most likely to leak: a policy denial
// names the target it refused, and that target is a URL.
func TestDeniedRequestIsTracedWithoutSayingWhatItRefused(t *testing.T) {
	recorder := recordSpans(t)

	policy, err := New()
	require.NoError(t, err)

	target := fmt.Sprintf("ftp://example.com/services/%s?token=%s", thePathSecret, theQuerySecret)

	_, err = get(t, policy, target)
	requireDenied(t, err, ReasonScheme, "ftp")

	// The error really does carry the material, which is what makes recording it
	// on the span a live hazard rather than a hypothetical one.
	require.Contains(t, err.Error(), theQuerySecret,
		"this test is meaningless if the denial does not name the target")

	stub := spanStub(t, recorder)
	require.Equal(t, "Error", stub.Status.Code.String(), "a refused request must mark its span")
	require.Empty(t, stub.Events, "no exception event, because an exception event carries the message")
	require.Equal(t, "*netpolicy.DenyError", attributesOf(stub)["error.type"],
		"the classification, not the sentence")

	requireNoMaterialInSpans(t, recorder, thePathSecret)
	requireNoMaterialInSpans(t, recorder, theQuerySecret)
}

// TestErrorStatusOnAFailingPeer keeps the other error direction honest: a 4xx
// or 5xx is an error for a client span, and the description is the fact of
// failure rather than anything the peer said.
func TestErrorStatusOnAFailingPeer(t *testing.T) {
	recorder := recordSpans(t)

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Error(w, theQuerySecret, http.StatusInternalServerError)
	}))
	t.Cleanup(server.Close)

	policy, err := New(WithAllowLoopback())
	require.NoError(t, err)

	resp, err := get(t, policy, server.URL)
	require.NoError(t, err)
	require.Equal(t, http.StatusInternalServerError, resp.StatusCode)

	stub := spanStub(t, recorder)
	require.Equal(t, "Error", stub.Status.Code.String())
	require.Equal(t, "500", attributesOf(stub)["error.type"])
	require.Equal(t, "500", attributesOf(stub)["http.response.status_code"])

	requireNoMaterialInSpans(t, recorder, theQuerySecret)
}

// TestNoSpanAndNoHeaderWithoutATracerProvider keeps the unconfigured case
// literally silent, the way engine's TestNoSpansWithoutATracerProvider does:
// with no provider installed there is no span to propagate, so nothing is
// written onto the outbound request either.
func TestNoSpanAndNoHeaderWithoutATracerProvider(t *testing.T) {
	var capture headerCapture
	server := capturingServer(t, &capture)

	policy, err := New(WithAllowLoopback())
	require.NoError(t, err)

	resp, err := get(t, policy, server.URL)
	require.NoError(t, err)
	require.Equal(t, http.StatusOK, resp.StatusCode)

	received := capture.only(t)
	require.Empty(t, received.Get("traceparent"),
		"a no-op tracer produced an invalid span context, which must inject nothing")
	require.Empty(t, received.Get("tracestate"))
}

// Test_spanNameAndMethod pins the two rules semantic conventions state for a
// client span's name and method attribute, including the one that matters for
// containment: an unrecognized method is reported as _OTHER and its own
// spelling — a string the caller chose — is never recorded.
func Test_spanNameAndMethod(t *testing.T) {
	for _, tc := range []struct {
		method string
		name   string
		attr   string
	}{
		{method: "GET", name: "GET", attr: "GET"},
		{method: "get", name: "GET", attr: "GET"},
		{method: "", name: "GET", attr: "GET"},
		{method: "POST", name: "POST", attr: "POST"},
		{method: "DELETE", name: "DELETE", attr: "DELETE"},
		{method: theQuerySecret, name: "HTTP", attr: "_OTHER"},
	} {
		t.Run(tc.method, func(t *testing.T) {
			name, attr := spanNameAndMethod(tc.method)
			require.Equal(t, tc.name, name)
			require.Equal(t, tc.attr, attr.Value.String())
		})
	}
}
