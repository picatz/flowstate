# Telemetry policy

Flowstate uses the OpenTelemetry SDK configuration contract. Set
`OTEL_EXPORTER_OTLP_ENDPOINT` (or a signal-specific endpoint) to enable export,
and use the standard `OTEL_SERVICE_NAME`, `OTEL_RESOURCE_ATTRIBUTES`, exporter
headers, TLS, compression and timeout variables. Traces use
`OTEL_TRACES_SAMPLER`; the recommended head sampler is
`parentbased_traceidratio` with `OTEL_TRACES_SAMPLER_ARG` set to the root ratio.
Parent-based sampling keeps the Connect → Temporal → activity → plugin decision
continuous. Metrics are recorded directly by their instruments and never
derived from exported/sampleable spans.

## Flowstate policy variables

These settings are domain policy rather than substitutes for standard OTel
configuration. They apply identically to the server, worker, CLI client, local
runner and plugin host.

| Variable | Default | Meaning |
| --- | --- | --- |
| `FLOWSTATE_TELEMETRY_BAGGAGE_ALLOWED_KEYS` | empty | Comma-separated keys which may cross a trust boundary. Unknown keys are denied. |
| `FLOWSTATE_TELEMETRY_BAGGAGE_MAX_KEYS` | `16` | Maximum retained members. |
| `FLOWSTATE_TELEMETRY_BAGGAGE_MAX_KEY_LENGTH` | `64` | Maximum key bytes. |
| `FLOWSTATE_TELEMETRY_BAGGAGE_MAX_VALUE_LENGTH` | `256` | Maximum value bytes. |
| `FLOWSTATE_TELEMETRY_BAGGAGE_MAX_ENCODED_BYTES` | `4096` | Maximum retained encoded baggage bytes. |
| `FLOWSTATE_TELEMETRY_ATTRIBUTE_ALLOWLIST` | empty | Domain attributes permitted on telemetry. Tenant attributes must be explicitly listed and must come from authenticated identity, never inbound baggage. |
| `FLOWSTATE_TELEMETRY_REDACT_KEYS` | common credential names | Case-insensitive keys whose values are removed. |
| `FLOWSTATE_TELEMETRY_FIELD_MAX_LENGTH` | `1024` | Shared value limit for logs, span attributes, execution events and audit records; truncate with an explicit `truncated=true` marker. |
| `FLOWSTATE_TELEMETRY_EXECUTION_EVENT_DETAIL` | `status` | `none`, `status`, or `full`; full remains subject to allowlisting and redaction. |
| `FLOWSTATE_TELEMETRY_AUDIT_SINK` | `stderr` | `stderr`, `otlp`, `both`, or `none`. Audit delivery is an independent sink decision. |

Inbound baggage is filtered in the HTTP router before authentication and RPC
instrumentation sees it, then filtered again by the propagator before Temporal
or plugin headers are written. Sensitive and unknown keys are dropped by
default. Key count, key/value length, and total encoded size are independent
bounds. The same vocabulary applies to every signal: delete denied/sensitive
fields; truncate permitted oversized values at the configured byte limit and
attach a truncation marker. Never promote caller-supplied `tenant`, namespace,
authorization, cookies, tokens, secrets, workflow inputs, or task outputs.

Audit records are not traces. Head or tail sampling **must not** gate their
creation or delivery; an OTLP audit destination uses the logs pipeline and its
own retry/durability policy. Likewise, do not compute counters from sampled
span arrival—export Flowstate and Temporal SDK metrics independently.

## Collector production pipeline

Use `memory_limiter` first and `batch` last in every pipeline. Between them use
`attributes`/`resource` processors to delete credentials, raw inputs/outputs and
unapproved tenant fields. Route authenticated tenant attributes with the
`routing` connector (or separate pipelines), not with untrusted baggage. Use
multiple exporters in a pipeline, or a routing connector, for operational and
compliance destinations; give the audit pipeline its own queue and retry policy.

Head sampling cannot see the outcome of a run. For traces, place the
Collector's `tail_sampling` processor after memory limiting and configure
policies for:

* error status and failure attributes;
* latency above the deployment's slow-run threshold;
* the bounded `flowstate.policy.result=refused` attribute;
* an allowlist of authenticated `flowstate.tenant.id` values; and
* a probabilistic fallback for ordinary traffic.

Size `decision_wait`, `num_traces`, and `expected_new_traces_per_sec` explicitly;
tail sampling buffers whole traces and is therefore a memory policy. All spans
of a trace must reach the same tail-sampling Collector (load-balance by trace
ID). Do not put metrics or audit logs through `tail_sampling`. After sampling,
batch and export to every trace destination. The example Collector configuration
demonstrates the required memory limiter, batching, and independent signal
pipelines; production deployments should add the deletion, routing, tail
sampling, queued retry, and multi-export stages described above.
