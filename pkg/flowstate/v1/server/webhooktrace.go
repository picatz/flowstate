package server

import (
	"context"
	"net/http"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/trace"

	v1 "github.com/picatz/flowstate/pkg/flowstate/v1"
)

// The trigger boundary's trace, which is #523's gap 1: a delivery arrived
// carrying a `traceparent`, that header was flattened into `event.headers` as
// data, and nothing connected "some pipeline called us" to "this run did the
// work". The run that resulted was an orphan root.
//
// # A link, and not a parent
//
// The inbound span context becomes a [trace.Link] on the delivery's own span,
// never its parent, and the delivery span is explicitly a new root. Three
// reasons, and any one of them decides it:
//
//   - **Lifetime.** A webhook sender's span ends when its HTTP call returns, in
//     milliseconds. The run the delivery starts may last hours, days, or cross
//     Continue-As-New for a week. Hanging that under a 200ms client span is a
//     trace no backend renders and no operator reads, and it is the case OTel's
//     own guidance names for links.
//   - **Trust.** The sender proved it holds a signing key. It did not thereby
//     become part of this deployment's trace graph, and a parent relationship
//     lets it *choose* our trace ids — every run it triggers, filed under a trace
//     id it picked, joinable by anything else it decides to put there.
//   - **Ownership.** A run's trace is this system's story about its own work. The
//     delivery is a cause, and "caused by" is exactly what a link says.
//
// # Where the extraction happens, and where it must not
//
// Only after [v1.VerifyWebhookDelivery] has passed, on the same side of that line
// as body decoding, for the reason the decoy-HMAC path above it exists: before
// verification an unauthenticated caller controls every byte, and extracting
// there would let anyone spam trace ids into a backend or graft themselves onto
// a trace graph without holding anything at all.
//
// # What is read, and what is never written down
//
// [propagation.TraceContext] alone parses the header, and its output is the only
// thing kept: a trace id, a span id and flags. The raw header value never
// becomes an attribute, an event or a log field — it is attacker-chosen text, and
// the parsed ids are the whole of what a link needs.
//
// Deliberately not the globally registered propagator, which is a composite
// carrying baggage too. Baggage from a sender is arbitrary caller-controlled
// key/value data that would ride the context into `ExecuteWorkflow` and be
// written to workflow history — durable and broadly readable, invariant 8's
// subject. `netpolicy`'s round tripper makes the same call in the outbound
// direction and `plugin/telemetry.go` makes the narrower version of it for a
// plugin this worker launched itself; an external sender gets less trust than
// that, not more.
//
// # Failing open on telemetry, closed on policy
//
// A missing `traceparent` is the ordinary case — most senders have no tracing at
// all — and a malformed one is what a broken or hostile sender produces. Both
// yield a span with no link and a delivery that proceeds exactly as before.
// Telemetry is not a policy surface: refusing a genuine, correctly signed
// delivery because its trace header was garbage would let any sender turn a
// header typo into an outage.

// webhookTracerName is the instrumentation scope the delivery span is attributed
// to, spelled the way `netpolicy` and `pkg/flowstate/v1` spell theirs: the import
// path of the package the instrumentation lives in.
const webhookTracerName = "github.com/picatz/flowstate/pkg/flowstate/v1/server"

// webhookDeliverySpanName names the span covering one accepted delivery.
//
// Both halves of the address, because a workflow may declare several webhooks
// and the question an operator asks is which one fired.
func webhookDeliverySpanName(workflow, trigger string) string {
	return "flowstate.webhook/" + workflow + "/" + trigger
}

// startDeliverySpan opens the span covering the acceptance of one verified
// delivery, linked to the sender's trace where the sender carried one.
//
// The returned context is what starts the run: with Temporal's client tracing
// interceptor installed, `ExecuteWorkflow` injects this span's context into the
// workflow's headers, so the run's own `RunWorkflow:Run` span — and every task
// span beneath it — lands in this trace. That is what carries the linkage
// durably: the link is recorded once, at the boundary that learned it, and the
// run is in the trace that holds it rather than carrying a copy of it forward.
//
// The provider is read per call for the reason [v1.StartTaskSpan] gives: a
// receiver is built once and may outlive the moment telemetry is configured.
func (r *WebhookReceiver) startDeliverySpan(ctx context.Context, route *webhookRoute, header http.Header) (context.Context, trace.Span) {
	options := []trace.SpanStartOption{
		// A server span: this covers handling an inbound request, which is work
		// somebody else asked for.
		trace.WithSpanKind(trace.SpanKindServer),

		// And a new root, stated rather than relied upon. Nothing extracts trace
		// context ahead of this handler today — the receiver is mounted bare, by
		// design, since a sender is unauthenticated — but a deployment that puts
		// an instrumented proxy or middleware in front of it would otherwise
		// hand the sender the parent relationship this whole file exists to
		// refuse, silently and from three files away.
		trace.WithNewRoot(),

		trace.WithAttributes(
			attribute.String(v1.SpanAttributeWorkflowName, route.workflow.GetName()),
			attribute.String(v1.SpanAttributeTriggerName, route.trigger.GetName()),
		),
	}

	// Extracted into a context of its own, so the sender's span context is read
	// and nothing else about it can reach the run: only the [trace.Link] below
	// crosses.
	if sender := trace.SpanContextFromContext(
		propagation.TraceContext{}.Extract(context.Background(), propagation.HeaderCarrier(header)),
	); sender.IsValid() {
		options = append(options, trace.WithLinks(trace.Link{SpanContext: sender}))
	}

	return otel.GetTracerProvider().Tracer(webhookTracerName).Start(ctx,
		webhookDeliverySpanName(route.workflow.GetName(), route.trigger.GetName()), options...)
}

// recordDeliveryOutcome says how the acceptance ended, in classifications only.
//
// The delivery id and never the idempotency key it names — the digest is the
// value a memo already carries, and the key is frequently a signature header.
// The error's own text is never recorded, for the reason
// [v1.RecordTaskOutcome] states: a start failure quotes whatever the cluster or
// the binding said, and a span goes to a collector that is not tenant-scoped.
func recordDeliveryOutcome(span trace.Span, accepted AcceptedDelivery, err error) {
	if !span.IsRecording() {
		return
	}

	if err != nil {
		span.SetStatus(codes.Error, "the delivery did not start a run")

		return
	}

	span.SetAttributes(
		attribute.String(v1.SpanAttributeDeliveryID, accepted.DeliveryID),
		attribute.Bool(v1.SpanAttributeDeliveryJoined, accepted.Joined),
	)
}
