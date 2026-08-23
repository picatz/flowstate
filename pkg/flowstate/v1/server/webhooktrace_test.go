package server_test

import (
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel/baggage"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"
	"go.opentelemetry.io/otel/trace"
	"go.temporal.io/sdk/client"
	"go.temporal.io/sdk/contrib/opentelemetry"
	"go.temporal.io/sdk/interceptor"
	"go.temporal.io/sdk/worker"
	"google.golang.org/protobuf/encoding/prototext"

	v1 "github.com/picatz/flowstate/pkg/flowstate/v1"
	"github.com/picatz/flowstate/pkg/flowstate/v1/internal/conformance"
	"github.com/picatz/flowstate/pkg/flowstate/v1/server"
)

// #523's gap 1, measured rather than wired: a delivery carrying a `traceparent`
// produced a run with no relationship of any kind to the sender's trace, and
// what these assert is the relationship that now exists and — just as
// deliberately — the one that does not.
//
// The distinction is the whole point. A *link* says the delivery caused the run.
// A *parent* would say the run is part of the sender's trace, which would hand a
// sender that holds nothing but a signing key the ability to name our trace ids
// and would file a week-long run under a 200ms client span. `webhooktrace.go`
// carries the argument; these tests are what keep it true.

// senderTraceContext is the span context a sender puts on the wire, and the
// header carrying it.
//
// Remote and sampled, because that is what an arriving `traceparent` produces
// once parsed — a test that built a local one would be asserting against a
// value this code never sees.
func senderTraceContext(tb testing.TB) (trace.SpanContext, string) {
	tb.Helper()

	sender := trace.NewSpanContext(trace.SpanContextConfig{
		TraceID:    trace.TraceID{0x0a, 0xf7, 0x65, 0x19, 0x16, 0xcd, 0x43, 0xdd, 0x84, 0x48, 0xeb, 0x21, 0x1c, 0x80, 0x31, 0x9c},
		SpanID:     trace.SpanID{0xb7, 0xad, 0x6b, 0x71, 0x69, 0x20, 0x33, 0x31},
		TraceFlags: trace.FlagsSampled,
		Remote:     true,
	})

	header := http.Header{}
	propagation.TraceContext{}.Inject(
		trace.ContextWithSpanContext(tb.Context(), sender), propagation.HeaderCarrier(header))

	traceparent := header.Get("traceparent")
	require.NotEmpty(tb, traceparent, "the propagator wrote no header, so this test would assert nothing")

	return sender, traceparent
}

// tracedWebhookDeployment is the deployment these tests deliver into: a client
// and a worker both carrying the SDK's tracing interceptor, and a receiver over
// them.
//
// The interceptors matter to the claim rather than to the plumbing. Without the
// client's, `ExecuteWorkflow` injects nothing and the run starts in a trace of
// its own — which is exactly the "the link stops at the receiver" failure the
// second half of [TestADeliveryLinksTheSendersTrace] is written to catch.
func tracedWebhookDeployment(t *testing.T) (*server.WebhookReceiver, client.Client) {
	t.Helper()

	_, namespace := newTemporalNamespace(t)

	tracing, err := opentelemetry.NewTracingInterceptor(opentelemetry.TracerOptions{})
	require.NoError(t, err)

	temporal, err := client.Dial(client.Options{
		HostPort:     devServer.FrontendHostPort(),
		Namespace:    namespace,
		Logger:       newTestingLogger(t),
		Interceptors: []interceptor.ClientInterceptor{tracing},
	})
	require.NoError(t, err)
	t.Cleanup(temporal.Close)

	startWorkerWithOptions(t, temporal, worker.Options{
		Interceptors: []interceptor.WorkerInterceptor{tracing},
	})

	receiver, err := mustNew(t, temporal).NewWebhookReceiver(t.Context(),
		"", []*v1.Workflow{orderWebhookWorkflow()}, keyStore(t, webhookSecret))
	require.NoError(t, err)

	return receiver, temporal
}

// deliverWithHeaders is [deliver] for the tests that have something to say about
// a header the sender chose.
func deliverWithHeaders(t *testing.T, handler http.Handler, body string, headers map[string]string) *http.Response {
	t.Helper()

	req := httptest.NewRequest(http.MethodPost, "/webhooks/order-webhook/storefront", strings.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set(v1.WebhookSignatureHeader, signed(body))
	for name, value := range headers {
		req.Header.Set(name, value)
	}

	recorder := httptest.NewRecorder()
	handler.ServeHTTP(recorder, req)

	return recorder.Result()
}

// deliverySpan finds the one span an accepted delivery opened.
func deliverySpan(t *testing.T, recorder *tracetest.SpanRecorder) tracetest.SpanStub {
	t.Helper()

	const want = "flowstate.webhook/order-webhook/storefront"

	var (
		found tracetest.SpanStub
		count int
	)
	for _, stub := range tracetest.SpanStubsFromReadOnlySpans(recorder.Ended()) {
		if stub.Name == want {
			found, count = stub, count+1
		}
	}
	require.Equal(t, 1, count, "want exactly one %s span, got %d", want, count)

	return found
}

// TestADeliveryLinksTheSendersTrace is gap 1's positive half, in both directions
// that can be wrong: the sender's trace is *linked*, and it is not the parent.
func TestADeliveryLinksTheSendersTrace(t *testing.T) {
	// No t.Parallel: [conformance.RecordSpans] swaps the global tracer provider,
	// and it is installed before the interceptors below read it.
	recorder := conformance.RecordSpans(t)

	sender, traceparent := senderTraceContext(t)

	receiver, temporal := tracedWebhookDeployment(t)

	body := deliveryBody("evt_traced")
	resp := deliverWithHeaders(t, receiver, body, map[string]string{"traceparent": traceparent})
	require.Equal(t, http.StatusAccepted, resp.StatusCode, "a genuine delivery carrying a traceparent was not accepted")

	accepted := readAccepted(t, resp)

	// The run finished, so every span it opened has ended and been recorded.
	var outputs v1.Workflow_StepOutputs
	require.NoError(t, temporal.GetWorkflow(t.Context(), accepted.WorkflowID, accepted.RunID).
		Get(t.Context(), &outputs))

	delivery := deliverySpan(t, recorder)

	// Linked, with the sender's own ids.
	require.Len(t, delivery.Links, 1, "the delivery span carries no link to the trace its sender named")
	require.Equal(t, sender.TraceID(), delivery.Links[0].SpanContext.TraceID(),
		"the link names a different trace from the one the sender sent")
	require.Equal(t, sender.SpanID(), delivery.Links[0].SpanContext.SpanID(),
		"the link names a different span from the one the sender sent")

	// And not parented, which is the half that would be wrong in the way nobody
	// notices until a week-long run is filed under somebody else's HTTP call.
	require.False(t, delivery.Parent.IsValid(),
		"the delivery span has a parent, so an unauthenticated sender's header decided where our trace hangs")
	require.NotEqual(t, sender.TraceID(), delivery.SpanContext.TraceID(),
		"the delivery is in the sender's trace, so the sender chose our trace id")
	require.Equal(t, trace.SpanKindServer, delivery.SpanKind)

	// The linkage reaches the run, which is the part wiring alone does not
	// prove: the delivery span's context is what starts the workflow, so the
	// run's own span — and every task span under it — is in this trace.
	var workflowSpans int
	for _, stub := range tracetest.SpanStubsFromReadOnlySpans(recorder.Ended()) {
		if stub.Name != "RunWorkflow:Run" {
			continue
		}
		workflowSpans++

		require.Equal(t, delivery.SpanContext.TraceID(), stub.SpanContext.TraceID(),
			"the run the delivery started is in a different trace from the delivery, so the link stops at the receiver")
	}
	require.NotZero(t, workflowSpans,
		"no workflow span was recorded, so this test would pass on a deployment that started no run")

	// The delivery span says which delivery it was, by the digest the memo
	// carries and never by the idempotency key that digest names.
	var named bool
	for _, attr := range delivery.Attributes {
		if string(attr.Key) == v1.SpanAttributeDeliveryID {
			named = true
			require.Equal(t, accepted.DeliveryID, attr.Value.AsString())
		}
		require.NotContains(t, attr.Value.String(), "evt_traced",
			"the raw idempotency key reached a span attribute")
	}
	require.True(t, named, "the delivery span does not say which delivery it covered")

	// And the header itself is never written down anywhere: it is
	// attacker-chosen text, and the parsed ids in the link are the whole of what
	// a link needs.
	for _, attr := range delivery.Attributes {
		require.NotContains(t, attr.Value.String(), traceparent,
			"the raw traceparent header was recorded as an attribute")
	}
}

// TestADeliveryWithoutUsableTraceContextIsLinkedToNothing is the fail-open half:
// telemetry is not a policy surface.
//
// A missing header is the ordinary case — most senders carry no tracing at all —
// and a malformed one is what a broken or hostile sender produces. Neither may
// refuse a correctly signed delivery, or a sender could turn a header typo into
// an outage, and neither may produce a link, because a link to nothing is a
// claim about a trace that does not exist.
func TestADeliveryWithoutUsableTraceContextIsLinkedToNothing(t *testing.T) {
	_, valid := senderTraceContext(t)

	for _, test := range []struct {
		name   string
		event  string
		header map[string]string
	}{
		{name: "absent", event: "evt_untraced"},
		{name: "empty", event: "evt_empty", header: map[string]string{"traceparent": ""}},
		{name: "not a traceparent", event: "evt_garbage", header: map[string]string{"traceparent": "not-a-traceparent"}},
		{
			name:   "zero ids",
			event:  "evt_zero",
			header: map[string]string{"traceparent": "00-00000000000000000000000000000000-0000000000000000-01"},
		},
		{
			name:   "unsupported version",
			event:  "evt_version",
			header: map[string]string{"traceparent": "ff-" + valid[3:]},
		},
		{
			name:   "truncated",
			event:  "evt_short",
			header: map[string]string{"traceparent": valid[:20]},
		},
		{
			// A sender is not bounded by politeness, and the propagator is the
			// only thing that parses this: a value far past any legal length
			// must be refused by it rather than reach anything else.
			name:   "far too long",
			event:  "evt_long",
			header: map[string]string{"traceparent": valid + strings.Repeat("0", 4096)},
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			recorder := conformance.RecordSpans(t)

			receiver, _ := tracedWebhookDeployment(t)

			resp := deliverWithHeaders(t, receiver, deliveryBody(test.event), test.header)
			require.Equal(t, http.StatusAccepted, resp.StatusCode,
				"a correctly signed delivery was refused over its trace header, so telemetry became a policy surface")

			require.Empty(t, deliverySpan(t, recorder).Links,
				"an unusable traceparent produced a link, so the trace claims a relationship to a trace nobody named")
		})
	}
}

// The distinctive values a hostile sender puts in each peer-controlled channel,
// each a substring no legitimate part of a run would contain, so a plain
// `Contains` over spans and history is decisive.
const (
	leakedTracestate = "s3cr3t-tracestate-vendor-value"
	leakedBaggage    = "s3cr3tbaggagevalue" // baggage values are token-restricted; keep it alnum.
	echoedHeader     = "carried-into-the-run-legitimately"
)

// headerEchoWebhookWorkflow maps one inbound header into a declared input, so a
// run's RunState genuinely carries a value drawn from `event.headers` — which is
// what makes the containment test's history scan a test of the delivery-facing
// header map and not only of the tracing-header channel beside it.
func headerEchoWebhookWorkflow() *v1.Workflow {
	return &v1.Workflow{
		Name:    "order-webhook",
		Profile: v1.CurrentProfile,
		Triggers: &v1.Triggers{Webhooks: []*v1.WebhookTrigger{{
			Name: "storefront",
			Verify: map[string]*v1.Value{
				v1.WebhookSchemeHMACSHA256: {Kind: &v1.Value_SecretRef{
					SecretRef: &v1.SecretRef{Scheme: "env", Name: "STOREFRONT_WEBHOOK_SECRET"},
				}},
			},
			IdempotencyKey: v1.NewExpr(`event.body.id`),
			Arguments: map[string]*v1.Value{
				// A non-trace header, so a passing test also proves the strip does
				// not over-reach: an ordinary header a workflow reads still arrives.
				"seen": v1.NewExpr(`event.headers["x-custom"]`),

				// And the trace header, mapped exactly as a Flowfile that wanted
				// it would — the leak finding 3 is about. Guarded with `in` so the
				// binding does not error when the header is (correctly) absent: it
				// binds to the raw value when the strip is defeated and to "" when
				// it holds, which is what makes the mutation observable in RunState.
				"trace_seen": v1.NewExpr(`"traceparent" in event.headers ? event.headers["traceparent"] : ""`),
			},
		}}},
		DeclaredInputs: []*v1.InputDeclaration{
			{Name: "seen", Type: v1.InputDeclaration_TYPE_STRING, Required: true},
			{Name: "trace_seen", Type: v1.InputDeclaration_TYPE_STRING, Required: true},
		},
		Steps: []*v1.Node{{
			Id:   "record",
			Kind: &v1.Node_Value{Value: v1.NewExpr(`inputs.seen`)},
		}, {
			Id:   "record_trace",
			Kind: &v1.Node_Value{Value: v1.NewExpr(`inputs.trace_seen`)},
		}},
	}
}

// historyText renders a run's whole history as searchable text.
//
// prototext rather than protojson, because a `bytes` field — which is what a
// Temporal Payload's data and a propagation header value both are — renders as a
// C-escaped string with printable ASCII left literal, so a leaked
// `traceparent`/`tracestate`/baggage value appears verbatim, while protojson
// would base64 it and hide exactly what is under test.
func historyText(t *testing.T, temporal client.Client, workflowID string) string {
	t.Helper()

	events, err := historyOf(t.Context(), temporal, workflowID)
	require.NoError(t, err)

	var b strings.Builder
	for _, event := range events {
		b.WriteString(prototext.Format(event))
	}

	return b.String()
}

// TestADeliveryDoesNotCarryPeerTraceMetadataIntoTheRun is #903's containment
// fix, in the shape CLAUDE.md names: a delivery carrying a raw `traceparent`, a
// vendor `tracestate`, and inbound baggage (as OTel middleware in front of the
// receiver would leave it in the request context) must land *none* of those
// peer-chosen values in the exported link, in any span, or anywhere in workflow
// history — and the trace-ID link must still work.
//
// Each of the three findings has its own leak channel, and this asserts all
// three are closed at once:
//
//   - tracestate rides in the [trace.SpanContext] the propagator returns, and
//     [trace.WithLinks] would export it to the collector;
//   - baggage rides in the context and Temporal's interceptor would serialize it
//     into the workflow header, which is written to history;
//   - the raw trace headers ride in `event.headers` and a Flowfile mapping one
//     into an input would write it to RunState.
func TestADeliveryDoesNotCarryPeerTraceMetadataIntoTheRun(t *testing.T) {
	// No t.Parallel: [conformance.RecordSpans] swaps the global tracer provider.
	recorder := conformance.RecordSpans(t)

	sender, traceparent := senderTraceContext(t)

	_, namespace := newTemporalNamespace(t)

	tracing, err := opentelemetry.NewTracingInterceptor(opentelemetry.TracerOptions{})
	require.NoError(t, err)

	temporal, err := client.Dial(client.Options{
		HostPort:     devServer.FrontendHostPort(),
		Namespace:    namespace,
		Logger:       newTestingLogger(t),
		Interceptors: []interceptor.ClientInterceptor{tracing},
	})
	require.NoError(t, err)
	t.Cleanup(temporal.Close)

	startWorkerWithOptions(t, temporal, worker.Options{
		Interceptors: []interceptor.WorkerInterceptor{tracing},
	})

	receiver, err := mustNew(t, temporal).NewWebhookReceiver(t.Context(),
		"", []*v1.Workflow{headerEchoWebhookWorkflow()}, keyStore(t, webhookSecret))
	require.NoError(t, err)

	// Inbound baggage, placed in the request context the way an OTel HTTP
	// middleware fronting this receiver would — the case [trace.WithNewRoot]
	// alone does not cover, because it resets the span parent and not the
	// baggage riding beside it.
	member, err := baggage.NewMember("leaked", leakedBaggage)
	require.NoError(t, err)
	bag, err := baggage.New(member)
	require.NoError(t, err)

	body := deliveryBody("evt_poisoned")
	req := httptest.NewRequest(http.MethodPost, "/webhooks/order-webhook/storefront", strings.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set(v1.WebhookSignatureHeader, signed(body))
	req.Header.Set("traceparent", traceparent)
	req.Header.Set("tracestate", "acme="+leakedTracestate)
	req.Header.Set("x-custom", echoedHeader)
	req = req.WithContext(baggage.ContextWithBaggage(req.Context(), bag))

	rec := httptest.NewRecorder()
	receiver.ServeHTTP(rec, req)
	resp := rec.Result()
	require.Equal(t, http.StatusAccepted, resp.StatusCode, "a genuine, correctly signed delivery was refused")

	accepted := readAccepted(t, resp)

	var outputs v1.Workflow_StepOutputs
	require.NoError(t, temporal.GetWorkflow(t.Context(), accepted.WorkflowID, accepted.RunID).
		Get(t.Context(), &outputs))

	// The strip did not over-reach: the ordinary header the workflow maps arrived
	// and reached the run's output.
	require.Equal(t, echoedHeader,
		outputs.GetStepValues()["record"].GetNamedValues()[v1.ValueOutput].GetLiteral().GetStringValue(),
		"a non-trace header did not survive into the run, so the strip removed more than trace context")

	// And the trace header a Flowfile deliberately mapped resolved to nothing,
	// because it was gone from `event.headers` before binding — the direct
	// RunState-facing form of finding 3.
	require.Empty(t,
		outputs.GetStepValues()["record_trace"].GetNamedValues()[v1.ValueOutput].GetLiteral().GetStringValue(),
		"a Flowfile mapping event.headers[\"traceparent\"] resolved to the raw header, so it entered RunState")

	// The link still works: it names the sender's trace.
	delivery := deliverySpan(t, recorder)
	require.Len(t, delivery.Links, 1, "the delivery span carries no link to its sender's trace")
	require.Equal(t, sender.TraceID(), delivery.Links[0].SpanContext.TraceID(),
		"the link no longer names the sender's trace, so the containment fix broke the link it protects")

	// Finding 1: the link carries none of the sender's tracestate.
	require.Zero(t, delivery.Links[0].SpanContext.TraceState().Len(),
		"the link exported the sender's tracestate to the collector")

	// Nothing peer-chosen reached any exported span, in any rendering.
	for _, rendered := range renderedSpanShapesOf(recorder) {
		require.NotContains(t, rendered, leakedTracestate, "a sender's tracestate reached an exported span")
		require.NotContains(t, rendered, leakedBaggage, "a sender's baggage reached an exported span")
		require.NotContains(t, rendered, traceparent, "the raw traceparent header reached an exported span")
	}

	// Findings 2 and 3: nothing peer-chosen reached workflow history — not
	// through the tracing header (baggage, tracestate) and not through
	// `event.headers` into RunState (the raw traceparent).
	history := historyText(t, temporal, accepted.WorkflowID)
	require.NotContains(t, history, leakedTracestate, "a sender's tracestate was written to workflow history")
	require.NotContains(t, history, leakedBaggage, "a sender's baggage was written to workflow history")
	require.NotContains(t, history, traceparent, "the raw traceparent header was written to workflow history")
	require.NotContains(t, history, "tracestate", "a tracestate header key reached workflow history")

	// And the history was actually read, so the assertions above are not vacuous.
	require.NotEmpty(t, history, "no history was read, so the containment assertions proved nothing")
}

// renderedSpanShapesOf renders every recorded span through the %v family, over
// the batch, each span, and a struct holding one — the containment shape
// CLAUDE.md's invariant 7 names, so a value hidden behind an unexported field
// still surfaces from inside a wrapper.
func renderedSpanShapesOf(recorder *tracetest.SpanRecorder) []string {
	stubs := tracetest.SpanStubsFromReadOnlySpans(recorder.Ended())

	type wrapper struct {
		one   tracetest.SpanStub
		batch tracetest.SpanStubs
	}

	rendered := []string{
		fmt.Sprintf("%v", stubs), fmt.Sprintf("%+v", stubs), fmt.Sprintf("%#v", stubs),
	}
	for _, stub := range stubs {
		w := wrapper{one: stub, batch: stubs}
		rendered = append(rendered,
			fmt.Sprintf("%v", stub), fmt.Sprintf("%+v", stub), fmt.Sprintf("%#v", stub),
			fmt.Sprintf("%v", w), fmt.Sprintf("%+v", w), fmt.Sprintf("%#v", w))
	}

	return rendered
}
