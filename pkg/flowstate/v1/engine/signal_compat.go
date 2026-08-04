package engine

import (
	commonpb "go.temporal.io/api/common/v1"
	"go.temporal.io/sdk/converter"
	"go.temporal.io/sdk/workflow"

	v1 "github.com/picatz/flowstate/pkg/flowstate/v1"
)

// withSignalDeliveryCompat wraps ctx's data converter so a signal channel can
// decode either the current wire shape ([v1.SignalDelivery]) or the shape
// every signal used before #194 (a bare [v1.Node_Outputs]), without ever
// confusing one for the other.
//
// # Why this exists
//
// Invariant 10 requires RunState to stay readable across an interpreter
// upgrade because one version writes it and a different one reads it back at
// Continue-As-New. A Temporal signal walks the identical seam between a
// different pair of processes: the *server* writes the wire bytes and the
// *worker* currently running this workflow's interpreter reads them back, and
// the two are independently deployed. A rolling deploy needs both directions
// to work — a new server signalling a workflow still pinned to an old worker,
// which only ever knew Node_Outputs, and an old server (or a signal already
// sitting in an execution's history from before this field existed)
// signalling a workflow a new worker picked up, which expects SignalDelivery.
//
// Simply changing the decode target's Go type from Node_Outputs to
// SignalDelivery is not the additive change invariant 10 asks for. Temporal's
// default converter encodes a proto message as JSON and, by default, rejects
// any field the target message does not declare — so decoding one shape's
// bytes into the other's type does not leave the unknown field at its zero
// value the way RunState's own field additions do; it fails the whole decode.
// And a failed signal decode is not loud: channelImpl.Receive treats it as a
// corrupted signal, logs it, and keeps waiting (see the SDK's
// internal_workflow.go) — the run does not error, it just never sees that
// approval. An in-flight approval would be silently lost or, worse, a gate
// would silently consume an empty one.
//
// # The fix: try both, in a fixed order, using strictness itself as the proof
//
// The two shapes share no field name at all — Node_Outputs has only
// "namedValues"; SignalDelivery has only "payload" and "sender" — so the exact
// strictness that makes guessing wrong fail loudly is what makes trying both
// sound rather than a heuristic: a well-formed, non-empty encoding of one can
// never successfully decode as the other, because the decoder for the wrong
// type always meets a field it does not recognize and errors on it (see
// TestSignalCompatDiscriminationIsSound). There is exactly one case where both
// decodes succeed — an empty payload, `{}` — and both readings agree with each
// other: no payload, no sender (see TestSignalCompatEmptyPayloadIsHarmlesslyAmbiguous).
//
// This is deterministic and replay-safe for the identical reason CEL
// evaluation inline in workflow code is (CLAUDE.md, "Workflow-side code is
// pure and frozen"): it is a pure function of bytes already recorded in
// history, with no clock, no randomness, and no I/O — the same bytes decode
// the same way on every replay, on every worker.
func withSignalDeliveryCompat(ctx workflow.Context) workflow.Context {
	return workflow.WithDataConverter(ctx, &signalDeliveryCompatConverter{
		DataConverter: converter.GetDefaultDataConverter(),
	})
}

// signalDeliveryCompatConverter delegates everything to the default converter
// except decoding into a *v1.SignalDelivery, which is the one call site the
// compatibility fallback applies to.
type signalDeliveryCompatConverter struct {
	converter.DataConverter
}

// FromPayloads must be overridden explicitly rather than left to embedding:
// the default converter's own FromPayloads calls its *own* FromPayload
// internally, not whatever a wrapper around it overrides — Go does not dispatch
// through an embedded interface the way virtual methods do in other languages.
// Relying on embedding alone would silently skip the fallback on every signal,
// the actual call path (see decodeArg in the SDK's internal/encode_args.go),
// while firing correctly for a direct FromPayload call nothing here ever makes.
func (c *signalDeliveryCompatConverter) FromPayloads(payloads *commonpb.Payloads, valuePtrs ...interface{}) error {
	if payloads == nil {
		return nil
	}
	items := payloads.GetPayloads()
	for i, valuePtr := range valuePtrs {
		if i >= len(items) {
			break
		}
		if err := c.FromPayload(items[i], valuePtr); err != nil {
			return err
		}
	}
	return nil
}

// FromPayload is where the fallback lives.
func (c *signalDeliveryCompatConverter) FromPayload(payload *commonpb.Payload, valuePtr interface{}) error {
	delivery, ok := valuePtr.(*v1.SignalDelivery)
	if !ok {
		return c.DataConverter.FromPayload(payload, valuePtr)
	}

	// The current shape, tried first: what every up-to-date server sends.
	if err := c.DataConverter.FromPayload(payload, delivery); err == nil {
		return nil
	}

	// Falls back to the shape every signal used before #194. Sender is left
	// nil rather than an empty-but-present SignalSender — nil is what
	// signalSenderValue (wait.go) renders identically to
	// [v1.LocalSignalSender]'s "nothing here was attested" case, which is
	// exactly the honest answer: this delivery carries no attestation at all,
	// and must never be confused with an attested-but-anonymous one.
	var legacy v1.Node_Outputs
	if err := c.DataConverter.FromPayload(payload, &legacy); err != nil {
		// Neither shape decodes: a genuinely corrupted signal, exactly the
		// failure this fallback did not change.
		return err
	}

	*delivery = v1.SignalDelivery{Payload: &legacy}
	return nil
}
