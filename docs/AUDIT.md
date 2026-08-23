# Security audit records

Audit records are a security control, not a derivative of tracing. Flowstate
loads `FLOWSTATE_AUDIT_DESTINATION` during command startup, before command
dispatch and regardless of `OTEL_EXPORTER_OTLP_*`. Its values are `stderr`,
`otlp`, `both`, and `none` (the default). `FLOWSTATE_AUDIT_REQUIRED=true`
refuses `none` and makes an OTLP export/flush failure fail the action or process
closed. Invalid values always prevent startup.

`stderr` writes one bounded JSON object per record. A write failure is returned
to the auditable action. `otlp` uses the OpenTelemetry **logs** protocol and a
dedicated logger provider, batch processor, export queue, and shutdown; it does
not share the ordinary-log pipeline. `both` requires both selected destinations
to accept a record. `none` explicitly discards records and is invalid when audit
is required. In a non-required OTLP deployment asynchronous delivery is
best-effort: collector failure does not stop workload execution. Required OTLP
deployments force the audit queue after each action, and refuse the action when
that fails.

Audit calls live at auditable actions and authorization refusals. They must not
be added to a span processor, sampler callback, or trace-export completion path:
sampling is allowed to discard traces and is never allowed to discard audit
evidence.

The record schema contains only time, action, outcome, subject, namespace,
resource, and reason. Each caller-controlled string is scrubbed through the
same secret scrubber used by other signals and is bounded to 256 bytes before a
sink sees it. Payloads, tokens, workflow inputs, and arbitrary attributes do not
belong in an audit record.
