package main

import (
	"fmt"
	"os"
	"strconv"
	"strings"

	"go.opentelemetry.io/otel/baggage"
	"go.opentelemetry.io/otel/sdk/trace"
)

// telemetryPolicy is process-local deployment policy. It is deliberately not a
// protobuf: none of it belongs in a workload or Temporal history. Export
// transport, resource and sampler settings retain their standard OTEL_* names;
// only Flowstate trust and domain decisions are named here.
type telemetryPolicy struct {
	allowedBaggage  map[string]struct{}
	attributeAllow  map[string]struct{}
	redact          map[string]struct{}
	maxKeys         int
	maxKeyLen       int
	maxValueLen     int
	maxEncodedBytes int
	maxFieldLen     int
	executionDetail string
	auditSink       string
	sampler         trace.Sampler
}

func loadTelemetryPolicy() (telemetryPolicy, error) {
	p := telemetryPolicy{
		allowedBaggage: csvSet(os.Getenv("FLOWSTATE_TELEMETRY_BAGGAGE_ALLOWED_KEYS")),
		attributeAllow: csvSet(os.Getenv("FLOWSTATE_TELEMETRY_ATTRIBUTE_ALLOWLIST")),
		redact:         csvSet(valueOr(os.Getenv("FLOWSTATE_TELEMETRY_REDACT_KEYS"), "authorization,cookie,set-cookie,password,secret,token")),
		maxKeys:        16, maxKeyLen: 64, maxValueLen: 256, maxEncodedBytes: 4096, maxFieldLen: 1024,
		executionDetail: valueOr(os.Getenv("FLOWSTATE_TELEMETRY_EXECUTION_EVENT_DETAIL"), "status"),
		auditSink:       valueOr(os.Getenv("FLOWSTATE_TELEMETRY_AUDIT_SINK"), "stderr"),
	}
	var err error
	for _, item := range []struct {
		name, raw string
		dst       *int
	}{
		{"FLOWSTATE_TELEMETRY_BAGGAGE_MAX_KEYS", os.Getenv("FLOWSTATE_TELEMETRY_BAGGAGE_MAX_KEYS"), &p.maxKeys},
		{"FLOWSTATE_TELEMETRY_BAGGAGE_MAX_KEY_LENGTH", os.Getenv("FLOWSTATE_TELEMETRY_BAGGAGE_MAX_KEY_LENGTH"), &p.maxKeyLen},
		{"FLOWSTATE_TELEMETRY_BAGGAGE_MAX_VALUE_LENGTH", os.Getenv("FLOWSTATE_TELEMETRY_BAGGAGE_MAX_VALUE_LENGTH"), &p.maxValueLen},
		{"FLOWSTATE_TELEMETRY_BAGGAGE_MAX_ENCODED_BYTES", os.Getenv("FLOWSTATE_TELEMETRY_BAGGAGE_MAX_ENCODED_BYTES"), &p.maxEncodedBytes},
		{"FLOWSTATE_TELEMETRY_FIELD_MAX_LENGTH", os.Getenv("FLOWSTATE_TELEMETRY_FIELD_MAX_LENGTH"), &p.maxFieldLen},
	} {
		if *item.dst, err = positiveEnv(item.name, item.raw, *item.dst); err != nil {
			return telemetryPolicy{}, err
		}
	}
	if !oneOf(p.executionDetail, "none", "status", "full") {
		return telemetryPolicy{}, fmt.Errorf("FLOWSTATE_TELEMETRY_EXECUTION_EVENT_DETAIL must be none, status, or full")
	}
	if !oneOf(p.auditSink, "stderr", "otlp", "both", "none") {
		return telemetryPolicy{}, fmt.Errorf("FLOWSTATE_TELEMETRY_AUDIT_SINK must be stderr, otlp, both, or none")
	}
	p.sampler, err = samplerFromEnv()
	if err != nil {
		return telemetryPolicy{}, err
	}
	return p, nil
}

func samplerFromEnv() (trace.Sampler, error) {
	name := valueOr(os.Getenv("OTEL_TRACES_SAMPLER"), "parentbased_always_on")
	ratio := func() (trace.Sampler, error) {
		v, err := strconv.ParseFloat(valueOr(os.Getenv("OTEL_TRACES_SAMPLER_ARG"), "1"), 64)
		if err != nil || v < 0 || v > 1 {
			return nil, fmt.Errorf("OTEL_TRACES_SAMPLER_ARG must be a number from 0 to 1")
		}
		return trace.TraceIDRatioBased(v), nil
	}
	switch name {
	case "always_on":
		return trace.AlwaysSample(), nil
	case "always_off":
		return trace.NeverSample(), nil
	case "traceidratio":
		return ratio()
	case "parentbased_always_on":
		return trace.ParentBased(trace.AlwaysSample()), nil
	case "parentbased_always_off":
		return trace.ParentBased(trace.NeverSample()), nil
	case "parentbased_traceidratio":
		r, err := ratio()
		if err != nil {
			return nil, err
		}
		return trace.ParentBased(r), nil
	default:
		return nil, fmt.Errorf("unsupported OTEL_TRACES_SAMPLER %q", name)
	}
}

func positiveEnv(name, raw string, fallback int) (int, error) {
	if raw == "" {
		return fallback, nil
	}
	v, err := strconv.Atoi(raw)
	if err != nil || v <= 0 {
		return 0, fmt.Errorf("%s must be a positive integer", name)
	}
	return v, nil
}
func csvSet(raw string) map[string]struct{} {
	out := map[string]struct{}{}
	for _, s := range strings.Split(raw, ",") {
		if s = strings.TrimSpace(strings.ToLower(s)); s != "" {
			out[s] = struct{}{}
		}
	}
	return out
}
func valueOr(v, fallback string) string {
	if v == "" {
		return fallback
	}
	return v
}
func oneOf(v string, values ...string) bool {
	for _, x := range values {
		if v == x {
			return true
		}
	}
	return false
}

// filterBaggage is fail-closed: unknown and conventionally sensitive keys are
// dropped, and every resource controlled by a peer has an independent bound.
func (p telemetryPolicy) filterBaggage(in baggage.Baggage) baggage.Baggage {
	members := make([]baggage.Member, 0, min(in.Len(), p.maxKeys))
	total := 0
	for _, member := range in.Members() {
		key, value := strings.ToLower(member.Key()), member.Value()
		if len(members) == p.maxKeys || len(key) > p.maxKeyLen || len(value) > p.maxValueLen {
			continue
		}
		if _, ok := p.allowedBaggage[key]; !ok {
			continue
		}
		if _, sensitive := p.redact[key]; sensitive {
			continue
		}
		// Member.String is the actual encoded representation, including any
		// properties, so the byte budget cannot be bypassed with metadata that
		// the key/value-only accounting forgot.
		size := len(member.String())
		if len(members) > 0 {
			size++
		}
		if total+size > p.maxEncodedBytes {
			continue
		}
		members, total = append(members, member), total+size
	}
	out, _ := baggage.New(members...)
	return out
}
