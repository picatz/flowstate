package engine

import (
	"context"
	"errors"
	"fmt"

	commonpb "go.temporal.io/api/common/v1"
	"go.temporal.io/sdk/converter"
	"go.temporal.io/sdk/workflow"

	v1 "github.com/picatz/flowstate/pkg/flowstate/v1"
	"github.com/picatz/flowstate/pkg/flowstate/v1/payloadcodec"
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
//
// # The converter it wraps
//
// The worker's own, dc, not [converter.GetDefaultDataConverter]. The wrapper
// replaces the converter the SDK put on the context, so wrapping the default one
// silently drops whatever the worker was actually configured with, which, on a
// deployment with a payload codec, means handing ciphertext to a converter that
// cannot read it and losing every signal. See engine/codec.go.
func withSignalDeliveryCompat(ctx workflow.Context, dc converter.DataConverter) workflow.Context {
	return workflow.WithDataConverter(ctx, &signalDeliveryCompatConverter{
		DataConverter: orDefaultConverter(dc),
	})
}

// signalDeliveryCompatConverter delegates everything to the default converter
// except decoding into a *v1.SignalDelivery, which is the one call site the
// compatibility fallback applies to.
type signalDeliveryCompatConverter struct {
	converter.DataConverter
}

// WithWorkflowContext forwards the SDK's per-context binding to the wrapped
// converter. Embedding the interface does not promote it, and without it the
// converter [payloadcodec.Config.DataConverter] returns never learns which
// workflow it decodes for, so its pause of the deadlock detector during a
// key provider call is a no-op (go.temporal.io/sdk@v1.48.0
// internal/internal_workflow.go getDataConverterFromWorkflowContext).
func (c *signalDeliveryCompatConverter) WithWorkflowContext(ctx workflow.Context) converter.DataConverter {
	if aware, ok := c.DataConverter.(workflow.ContextAware); ok {
		return &signalDeliveryCompatConverter{DataConverter: aware.WithWorkflowContext(ctx)}
	}
	return c
}

// WithContext is [signalDeliveryCompatConverter.WithWorkflowContext]'s other
// half of [workflow.ContextAware], for a binding made outside a workflow.
func (c *signalDeliveryCompatConverter) WithContext(ctx context.Context) converter.DataConverter {
	if aware, ok := c.DataConverter.(workflow.ContextAware); ok {
		return &signalDeliveryCompatConverter{DataConverter: aware.WithContext(ctx)}
	}
	return c
}

// FromPayloads must be overridden explicitly rather than left to embedding:
// the default converter's own FromPayloads calls its *own* FromPayload
// internally, not whatever a wrapper around it overrides — Go does not dispatch
// through an embedded interface the way virtual methods do in other languages.
// Relying on embedding alone would silently skip the fallback on every signal,
// the actual call path (see decodeArg in the SDK's internal/encode_args.go),
// while firing correctly for a direct FromPayload call nothing here ever makes.
func (c *signalDeliveryCompatConverter) FromPayloads(payloads *commonpb.Payloads, valuePtrs ...any) error {
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

// FromPayload is where the fallback lives, and where a payload this process
// cannot read is kept from being taken for a corrupt one.
func (c *signalDeliveryCompatConverter) FromPayload(payload *commonpb.Payload, valuePtr any) error {
	delivery, ok := valuePtr.(*v1.SignalDelivery)
	if !ok {
		err := c.DataConverter.FromPayload(payload, valuePtr)
		failIfUnreadableHere(err)
		return err
	}

	// The current shape, tried first: what every up-to-date server sends.
	err := c.DataConverter.FromPayload(payload, delivery)
	if err == nil {
		return nil
	}
	failIfUnreadableHere(err)

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

// failIfUnreadableHere fails the run when err says the payload could not be
// decoded by this process rather than that it is wrong: a key provider that
// did not answer, or a key or envelope version this worker lacks.
//
// Returned, such an error is taken for the payload's own. A signal channel
// drops a signal it cannot decode as corrupt, losing an approval; an activity
// or child result that fails to decode fails its step, which runs
// `continue_on_error:` or `undo:` for a step that succeeded, and a replay on a
// worker that can read it then takes the other branch. Either way the run's
// decisions would depend on which worker read it and whether a provider
// answered, not on its history. Workflow code cannot make the task retry
// instead (every panic meets the worker's panic policy), so the run fails,
// loudly and recoverably, with the payload still in its history: see
// panicpolicy.go.
func failIfUnreadableHere(err error) {
	if errors.Is(err, payloadcodec.ErrUnavailable) || errors.Is(err, payloadcodec.ErrNotReadableHere) {
		panic(fmt.Errorf("decoding a payload in workflow code: %w", err))
	}
}
