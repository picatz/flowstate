// Package audit writes the record of an authorization decision to a sink that
// is complete rather than sampled.
//
// # Why this is not telemetry
//
// Traces and metrics are sampled, best-effort and optional by design:
// cmd/flow's initTelemetry builds nothing at all unless an operator asked for
// it, and that is invariant 8 working as intended. "Who signalled which run,
// and was it refused" is a different kind of record. It must be complete
// rather than sampled, it must survive an operator who configured no
// collector, and a deployment must be able to say that an action which cannot
// be recorded does not happen. None of those three is true of a signal that
// travels the telemetry path, which is why nothing here reads the global
// logger provider: unset, that provider is a no-op that discards, which is
// correct for logs and exactly wrong for an audit trail.
//
// # The shape
//
// A [Recorder] holds the sinks and the deployment's policy about them. Each
// [Emitter] gets every record. In required mode an emitter's failure is the
// caller's failure — that is the whole of the fail-closed claim, and it is why
// [Emitter.Emit] returns an error where the OpenTelemetry log API's own Emit
// does not.
//
// A [sdklog.BatchProcessor] cannot back a required sink: it is asynchronous,
// so under it a "required" emitter proves nothing at the decision point — the
// export it would have failed at happens after the request has already been
// answered. [NewSyncProcessor] is the synchronous path required mode needs.
// Batch remains fine for a deployment that has not asked for required.
//
// # The record
//
// [flowstatev1.AuditRecord] and the reasoning behind every field of it live in
// proto/flowstate/v1/audit.proto. The short version, because it is what stops
// a well-meant addition here: the record is redacted structurally rather than
// scrubbed. It has no field a payload, an error message or a specification
// could be placed in, so there is nothing in it for a scrubber to be
// best-effort about. MCP arguments, prompts, session and request identifiers,
// and tool results are absent for the same reason. Do not add free text; add a
// closed code to AuditDenyCode instead.
//
// # The boundary
//
// A record may travel to a sink. It may never travel into Temporal: nothing
// here or anywhere else writes an AuditRecord into a workflow payload, a memo,
// a search attribute or a signal. Workflow history is durable and broadly
// readable, and an audit trail inside the substrate it audits is not one.
package audit
