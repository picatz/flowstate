package main

import (
	"context"
	"strings"

	"go.opentelemetry.io/otel/baggage"
	"go.opentelemetry.io/otel/propagation"
)

// These are deliberately tighter than the wire-format limits. Baggage crosses a
// trust boundary and is copied to every downstream request, so accepting the W3C
// maximum as an application policy would turn a small request into durable,
// repeatedly propagated attacker-controlled state.
const (
	maxTelemetryBaggageMembers      = 32
	maxTelemetryBaggageKeyBytes     = 128
	maxTelemetryBaggageValueBytes   = 1024
	maxTelemetryBaggageEncodedBytes = 4096
)

// telemetryPolicy applies Flowstate's baggage policy on both extraction and
// injection. Trace context is left to the wrapped W3C propagator.
type telemetryPolicy struct {
	next propagation.TextMapPropagator
}

func (p telemetryPolicy) Inject(ctx context.Context, carrier propagation.TextMapCarrier) {
	p.next.Inject(ctx, carrier)
	carrier.Set("baggage", p.filterBaggage(carrier.Get("baggage")))
}

func (p telemetryPolicy) Extract(ctx context.Context, carrier propagation.TextMapCarrier) context.Context {
	return p.next.Extract(ctx, telemetryPolicyCarrier{TextMapCarrier: carrier, policy: p})
}

func (p telemetryPolicy) Fields() []string { return p.next.Fields() }

type telemetryPolicyCarrier struct {
	propagation.TextMapCarrier
	policy telemetryPolicy
}

func (c telemetryPolicyCarrier) Get(key string) string {
	value := c.TextMapCarrier.Get(key)
	if strings.EqualFold(key, "baggage") {
		return c.policy.filterBaggage(value)
	}
	return value
}

// Values preserves multi-header extraction without allowing the wrapped
// propagator to see an unsanitized alternate baggage header.
func (c telemetryPolicyCarrier) Values(key string) []string {
	if !strings.EqualFold(key, "baggage") {
		if values, ok := c.TextMapCarrier.(propagation.ValuesGetter); ok {
			return values.Values(key)
		}
		return []string{c.TextMapCarrier.Get(key)}
	}
	if values, ok := c.TextMapCarrier.(propagation.ValuesGetter); ok {
		return []string{c.policy.filterBaggage(strings.Join(values.Values(key), ","))}
	}
	return []string{c.policy.filterBaggage(c.TextMapCarrier.Get(key))}
}

// filterBaggage parses each member in isolation, applies policy to its decoded
// key and value, and then reconstructs it. Member properties are intentionally
// discarded: Flowstate has no property vocabulary, so retaining arbitrary
// metadata would create a second, ungoverned key/value channel.
func (telemetryPolicy) filterBaggage(header string) string {
	retained := make([]string, 0, min(strings.Count(header, ",")+1, maxTelemetryBaggageMembers))
	encodedBytes := 0
	for raw := range strings.SplitSeq(header, ",") {
		if len(retained) == maxTelemetryBaggageMembers {
			break
		}

		parsed, err := baggage.Parse(raw)
		if err != nil || parsed.Len() != 1 {
			continue
		}
		member := parsed.Members()[0]
		if len(member.Key()) > maxTelemetryBaggageKeyBytes ||
			len(member.Value()) > maxTelemetryBaggageValueBytes ||
			strings.Contains(member.Key(), "%") ||
			sensitiveBaggageKey(member.Key()) {
			continue
		}

		// Do not append member.String(): that would put its original properties
		// back on the wire after only its primary key and value were checked.
		sanitized, err := baggage.NewMemberRaw(member.Key(), member.Value())
		if err != nil {
			continue
		}
		encoded := sanitized.String()
		separator := 0
		if len(retained) != 0 {
			separator = 1
		}
		if encodedBytes+separator+len(encoded) > maxTelemetryBaggageEncodedBytes {
			continue
		}
		retained = append(retained, encoded)
		encodedBytes += separator + len(encoded)
	}
	return strings.Join(retained, ",")
}

func sensitiveBaggageKey(key string) bool {
	normalized := strings.NewReplacer("-", "", "_", "", ".", "").Replace(strings.ToLower(key))
	for _, sensitive := range []string{"authorization", "cookie", "password", "passwd", "secret", "token", "apikey", "privatekey", "credential"} {
		if strings.Contains(normalized, sensitive) {
			return true
		}
	}
	return false
}
