package conformance

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/propagation"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"
	"go.opentelemetry.io/otel/trace"
	"google.golang.org/protobuf/proto"

	v1 "github.com/picatz/flowstate/pkg/flowstate/v1"
)

// Shared cases for #523's gap 2: the outbound span and W3C trace context the
// http task's request carries, run against both execution drivers —
// `flowstatev1_test.TestRunWorkflowHTTPSpan` locally and
// `engine.TestRunWorkflowHTTPSpan` durably.
//
// # Why a run rather than the round tripper's own tests
//
// `netpolicy`'s tests already prove the round tripper opens the span, injects
// the header and says nothing it should not. What they cannot see is whether a
// *run* reaches it: the span is opened on whatever context the task hands the
// client, and the two drivers build that context by completely different routes
// — one calls the task function directly, the other arrives inside a Temporal
// activity. A driver that lost the context, or that ran the task through some
// other client, would fail here and nowhere else.
//
// This slice narrowed the drivers' disagreement about tracing without closing
// it, and gap 3 has since closed the rest of it: the local driver opens
// `flowstate.task/<name>` too (see taskspan.go), so the client span asserted
// below now hangs off a task span under *either* driver rather than being a
// root locally and a child durably. That nesting is asserted here as well —
// it is the cheapest possible evidence that the two drivers' spans are one
// vocabulary and not two, since it can only hold if the local driver's span
// context is the one the task's client actually runs on.

// HTTPSpanQuerySecret is the credential-shaped value this case hides in the
// request's query string, distinctive enough that a substring search cannot
// match it by accident.
//
// A query string is where a real token most often ends up: the http task's own
// schema comment says so where it excludes `query` from NestedSecretInputs — "a
// query string is written to access logs, kept in browser history, and forwarded
// in a Referer header". An exported span is one more such destination, and this
// is the case that would catch it becoming one.
const HTTPSpanQuerySecret = "s3cr3t-run-query-token-that-must-never-be-exported"

// TracedHTTPServer is a loopback server that records the trace context of the
// request it was sent.
type TracedHTTPServer struct {
	// URL is the base URL of the server.
	URL string

	mu     sync.Mutex
	header http.Header
}

// NewTracedHTTPServer starts a server recording what a run's request carried,
// and registers an http task permitting loopback for the test.
func NewTracedHTTPServer(tb testing.TB) *TracedHTTPServer {
	tb.Helper()

	traced := &TracedHTTPServer{}

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		traced.mu.Lock()
		traced.header = r.Header.Clone()
		traced.mu.Unlock()

		w.Header().Set("Content-Type", "text/plain")
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("reached"))
	}))
	tb.Cleanup(srv.Close)

	traced.URL = srv.URL

	allowLoopback(tb)

	return traced
}

// ReceivedSpanContext returns the span context the peer extracted from the
// request it was sent, decoded the way a receiving service's own
// instrumentation would decode it.
//
// Extracted rather than string-compared on purpose: what a propagation test has
// to establish is that the peer can hang its own span on what arrived, not that
// some header was present.
func (s *TracedHTTPServer) ReceivedSpanContext(tb testing.TB) trace.SpanContext {
	tb.Helper()

	s.mu.Lock()
	defer s.mu.Unlock()

	if s.header == nil {
		tb.Fatalf("no request reached the peer, so there is no trace context to read")
	}

	return trace.SpanContextFromContext(propagation.TraceContext{}.Extract(
		context.Background(), propagation.HeaderCarrier(s.header)))
}

// RecordSpans installs a recording tracer provider for the duration of a test
// and returns the recorder.
//
// The global provider, because that is where the round tripper's spans go — the
// same place `engine`'s task span and otelconnect's spans go — and restored
// afterwards, since these run in binaries shared with every other test in their
// packages.
func RecordSpans(tb testing.TB) *tracetest.SpanRecorder {
	tb.Helper()

	recorder := tracetest.NewSpanRecorder()
	provider := sdktrace.NewTracerProvider(sdktrace.WithSpanProcessor(recorder))

	previous := otel.GetTracerProvider()
	otel.SetTracerProvider(provider)

	tb.Cleanup(func() {
		otel.SetTracerProvider(previous)
		_ = provider.Shutdown(context.Background())
	})

	return recorder
}

// HTTPSpanWorkflow returns a one-step workflow whose request hides
// [HTTPSpanQuerySecret] in its query string.
func HTTPSpanWorkflow(baseURL string) *v1.Workflow {
	return &v1.Workflow{
		Name:    "http-span",
		Profile: v1.CurrentProfile,
		Steps: []*v1.Node{{
			Id: "reach",
			Kind: &v1.Node_Task{Task: &v1.Task{
				Name: "http",
				Inputs: map[string]*v1.Value{
					"method":  v1.NewLiteral(http.MethodGet),
					"url":     v1.NewLiteral(baseURL + "/call?token=" + HTTPSpanQuerySecret),
					"outputs": v1.NewExpr(`{"said": response.body}`),
				},
			}},
		}},
	}
}

// HTTPSpanExpectedOutputs is what a [HTTPSpanWorkflow] run produces.
func HTTPSpanExpectedOutputs() *v1.Workflow_StepOutputs {
	return &v1.Workflow_StepOutputs{StepValues: map[string]*v1.Node_Outputs{
		"reach": said("reached"),
	}}
}

// AssertHTTPSpan is the shared assertion both drivers make.
//
// Three claims, and the order is the order they can fail in. The request
// reached the peer carrying trace context that decodes; the trace it names is
// the one this side actually recorded, span for span; and no part of the URL's
// credential appears anywhere in any exported span.
func AssertHTTPSpan(tb testing.TB, server *TracedHTTPServer, recorder *tracetest.SpanRecorder, outputs *v1.Workflow_StepOutputs, err error) {
	tb.Helper()

	if err != nil {
		tb.Fatalf("the run failed: %v", err)
	}
	if want := HTTPSpanExpectedOutputs(); !proto.Equal(want, outputs) {
		tb.Fatalf("the run produced %v, want %v", outputs, want)
	}

	received := server.ReceivedSpanContext(tb)
	if !received.IsValid() {
		tb.Fatalf("the peer received no usable trace context; a service on the other side has nothing to hang its span on")
	}

	// The equality is what makes this a propagation test rather than a header
	// test: the peer's parent has to be the span this side opened for the call.
	stub, found := clientSpanFor(recorder, received)
	if !found {
		tb.Fatalf("the peer's parent span %s is not among the spans this side recorded: %v",
			received.SpanID(), spanNames(recorder))
	}
	if stub.SpanContext.TraceID() != received.TraceID() {
		tb.Fatalf("the peer's trace %s is not the trace this side recorded, %s",
			received.TraceID(), stub.SpanContext.TraceID())
	}
	if stub.SpanKind != trace.SpanKindClient {
		tb.Fatalf("the span covering an outbound call is %s, want a client span", stub.SpanKind)
	}

	// And where it sits: under the task span for the step that made the call,
	// on both drivers. A client span rooted at the top of a trace is what the
	// local driver produced before #523's gap 3 — the call was traced, and
	// nothing said which step made it.
	if parent := parentSpanName(recorder, stub); parent != v1.TaskSpanName("http") {
		tb.Fatalf("the outbound call's span sits under %q, want %q — nothing in the trace says which step made the call",
			parent, v1.TaskSpanName("http"))
	}

	// And the containment, in the direction that can fail: rendered through the
	// %v family over every span, not checked against the one attribute somebody
	// remembered.
	for _, rendered := range RenderedSpans(recorder) {
		if strings.Contains(rendered, HTTPSpanQuerySecret) {
			tb.Fatalf("a credential from the request URL reached a span, which is exported to a collector")
		}
	}
}

// clientSpanFor returns the recorded span the peer named as its parent.
func clientSpanFor(recorder *tracetest.SpanRecorder, received trace.SpanContext) (tracetest.SpanStub, bool) {
	for _, stub := range tracetest.SpanStubsFromReadOnlySpans(recorder.Ended()) {
		if stub.SpanContext.SpanID() == received.SpanID() {
			return stub, true
		}
	}

	return tracetest.SpanStub{}, false
}

// parentSpanName names the recorded span one level above the given one, or the
// empty string where it is a root or its parent was not recorded here.
func parentSpanName(recorder *tracetest.SpanRecorder, stub tracetest.SpanStub) string {
	for _, candidate := range tracetest.SpanStubsFromReadOnlySpans(recorder.Ended()) {
		if candidate.SpanContext.SpanID() == stub.Parent.SpanID() {
			return candidate.Name
		}
	}

	return ""
}

// spanNames lists what was recorded, for a failure message that says what
// happened instead.
func spanNames(recorder *tracetest.SpanRecorder) []string {
	var names []string
	for _, span := range recorder.Ended() {
		names = append(names, span.Name())
	}

	return names
}

// RenderedSpans renders every recorded span through the containment shapes
// CLAUDE.md names rather than through the containment value: the four verbs
// `%v`, `%+v`, `%#v` and `%s`, over the batch, over each span, and over a
// struct holding those through an *unexported* field — which is the whole
// point, because `fmt` cannot call a method on a value it reaches that way and
// prints the fields instead. A redacting String() protects a value printed
// directly and does nothing one level down.
//
// The `%s` shapes are over [spanText] rather than over [tracetest.SpanStub],
// which is not a decision about coverage: a SpanStub is mostly ints and
// timestamps, so `go vet` rejects the verb against it and what it would print
// is `%!s(int=0)` beside the strings the other three verbs already printed.
// spanText is the string-shaped part of the same span, reached through
// unexported fields, which is where `%s` means something.
func RenderedSpans(recorder *tracetest.SpanRecorder) []string {
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
			fmt.Sprintf("%v", w), fmt.Sprintf("%+v", w), fmt.Sprintf("%#v", w),
			fmt.Sprintf("%v", []wrapper{w}), fmt.Sprintf("%+v", []wrapper{w}),
			fmt.Sprintf("%#v", []wrapper{w}))
	}

	texts := spanTexts(stubs)
	rendered = append(rendered,
		fmt.Sprintf("%v", texts), fmt.Sprintf("%+v", texts),
		fmt.Sprintf("%#v", texts), fmt.Sprintf("%s", texts))

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
	}

	for _, text := range texts {
		rendered = append(rendered,
			fmt.Sprintf("%v", text), fmt.Sprintf("%+v", text),
			fmt.Sprintf("%#v", text), fmt.Sprintf("%s", text))
	}

	return rendered
}

// spanText is everything a span says in words, held through unexported fields.
//
// Unexported deliberately: this is the arrangement a redacting formatter cannot
// survive, so it is the arrangement the containment assertions have to check.
type spanText struct {
	name        string
	description string
	attributes  []string
	events      []string
}

// spanTexts reduces recorded spans to their [spanText].
func spanTexts(stubs []tracetest.SpanStub) []spanText {
	texts := make([]spanText, 0, len(stubs))
	for _, stub := range stubs {
		text := spanText{name: stub.Name, description: stub.Status.Description}
		for _, attr := range stub.Attributes {
			text.attributes = append(text.attributes, string(attr.Key)+"="+attr.Value.String())
		}
		for _, event := range stub.Events {
			text.events = append(text.events, event.Name)
			for _, attr := range event.Attributes {
				text.events = append(text.events, string(attr.Key)+"="+attr.Value.String())
			}
		}
		texts = append(texts, text)
	}

	return texts
}
