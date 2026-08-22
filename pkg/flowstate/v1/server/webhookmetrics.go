package server

import (
	"context"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"

	"github.com/picatz/flowstate/pkg/flowstate/v1/metricschema"
)

// The webhook receiver's half of #526: what happened to each delivery.
//
// # The question this answers, and why a refusal counter alone does not
//
// #526 puts it plainly: an operator cannot tell "delivery rate dropped because
// the sender stopped" from "delivery rate dropped because we are refusing
// everything" without reading logs. Both of those look like zero accepted
// deliveries. Only the ratio separates them, so acceptance and refusal are one
// instrument with one attribute distinguishing them, rather than a refusals
// counter that reads the same at three in the morning whether a bound is being
// hit or nothing is arriving at all.
//
// # Why the reason is a fixed enumeration
//
// [WebhookReceiver] already says why it refused, in a log line, in a sentence
// that can name the path, the sender's own error, or the reason a payload did
// not bind — text an outsider influences. None of that may become a label, and
// the receiver's *classification* of the refusal is a different thing: a closed
// set written in [metricschema], one member per refusal site in webhook.go,
// which a sender cannot extend by sending anything. See
// [metricschema.WebhookRefusal] for the distinction against
// [metricschema.PolicySurface], which refuses the reason-shaped thing it sounds
// like.
//
// # Not counted here
//
// Which webhook was addressed. The trigger and workflow names would be bounded
// by configuration and are on every one of these log lines already, but a
// refusal decided before routing — an unrouted path, an oversized body — has no
// webhook to name, so the label would be present on some series and absent from
// others for reasons that have nothing to do with the operator's question. #526
// slice 3's residue, stated rather than smuggled in.
//
// The delivery id is not counted here and never will be: it is minted by the
// sender, one per delivery, which is precisely the class [metricschema] refuses
// by name.

// webhookMeterName is the instrumentation scope for the receiver's own
// measurements.
const webhookMeterName = "github.com/picatz/flowstate/pkg/flowstate/v1/server"

// recordWebhookDelivery counts one delivery under what the receiver did with it.
//
// The provider is read per call, for the reason
// `pkg/flowstate/v1/taskmetrics.go` gives at length: a receiver is built while a
// process assembles itself, and an instrument created before `startTelemetry`
// ran would hold the no-op provider for the life of the server. With nothing
// configured this is the no-op provider and the Add below does nothing at all.
//
// reason is empty for anything that was not refused, and a
// [metricschema.WebhookRefusal] member otherwise; the schema drops an
// empty value, so an accepted delivery carries the outcome alone rather than a
// blank reason that would read as a refusal nobody classified.
func recordWebhookDelivery(ctx context.Context, outcome, reason string) {
	meter := otel.GetMeterProvider().Meter(webhookMeterName)

	deliveries, _ := meter.Int64Counter(metricschema.InstrumentWebhookDeliveries,
		metric.WithDescription("webhook deliveries, by what the receiver did with them"))

	deliveries.Add(ctx, 1, metricschema.WithAttributes(
		attribute.String(metricschema.WebhookOutcome, outcome),
		attribute.String(metricschema.WebhookRefusal, reason),
	))
}

// recordWebhookRefusal counts one refused delivery under the receiver's own
// classification of the refusal.
func recordWebhookRefusal(ctx context.Context, reason string) {
	recordWebhookDelivery(ctx, metricschema.WebhookOutcomeRefused, reason)
}
