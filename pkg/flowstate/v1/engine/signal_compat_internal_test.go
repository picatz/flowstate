package engine

import (
	"testing"

	"github.com/stretchr/testify/require"
	"go.temporal.io/sdk/converter"

	v1 "github.com/picatz/flowstate/pkg/flowstate/v1"
)

// newCompatConverter is the same wrapper withSignalDeliveryCompat installs on
// a workflow's context, without needing a workflow context to build one.
func newCompatConverter() *signalDeliveryCompatConverter {
	return &signalDeliveryCompatConverter{DataConverter: converter.GetDefaultDataConverter()}
}

// TestSignalCompatDiscriminationIsSound is the property the whole fallback
// depends on: a well-formed, non-empty encoding of one shape must never
// successfully decode as the other. If it could, the fallback in FromPayload
// would be a heuristic rather than a proof — exactly what #199's review asked
// this design not to be.
//
// Checked directly against the *default* converter, not the compat wrapper,
// because this is a fact about the wire encoding itself (Node_Outputs and
// SignalDelivery share no field name), not about the fallback logic built on
// top of it.
func TestSignalCompatDiscriminationIsSound(t *testing.T) {
	t.Parallel()

	dc := converter.GetDefaultDataConverter()

	t.Run("a non-empty legacy payload does not decode as the current shape", func(t *testing.T) {
		legacy := &v1.Node_Outputs{NamedValues: map[string]*v1.Value{
			"approved": v1.NewLiteral(true),
		}}
		payload, err := dc.ToPayload(legacy)
		require.NoError(t, err)

		var asDelivery v1.SignalDelivery
		err = dc.FromPayload(payload, &asDelivery)
		require.Error(t, err,
			"a Node_Outputs payload decoded as SignalDelivery without error, which means the "+
				"two shapes can be confused for one another")
	})

	t.Run("a non-empty current-shape delivery does not decode as the legacy shape", func(t *testing.T) {
		delivery := &v1.SignalDelivery{
			Payload: &v1.Node_Outputs{NamedValues: map[string]*v1.Value{"approved": v1.NewLiteral(true)}},
			Sender:  &v1.SignalSender{Identity: &v1.WorkloadIdentity{Subject: "someone@example.com"}},
		}
		payload, err := dc.ToPayload(delivery)
		require.NoError(t, err)

		var asLegacy v1.Node_Outputs
		err = dc.FromPayload(payload, &asLegacy)
		require.Error(t, err,
			"a SignalDelivery payload decoded as Node_Outputs without error, which means the "+
				"two shapes can be confused for one another")
	})

	t.Run("a sender-only delivery still does not decode as the legacy shape", func(t *testing.T) {
		// The narrowest current-shape message: an empty payload, so this
		// isolates that "sender" alone, with no "payload" content, is still
		// enough to make the legacy decode fail — the discrimination does not
		// depend on the payload half being present.
		delivery := &v1.SignalDelivery{
			Sender: &v1.SignalSender{Identity: &v1.WorkloadIdentity{Subject: "someone@example.com"}},
		}
		payload, err := dc.ToPayload(delivery)
		require.NoError(t, err)

		var asLegacy v1.Node_Outputs
		err = dc.FromPayload(payload, &asLegacy)
		require.Error(t, err)
	})
}

// TestSignalCompatEmptyPayloadIsHarmlesslyAmbiguous documents the one case
// where both candidate decodes succeed — an empty JSON object has no field to
// conflict with either schema — and proves the ambiguity does not matter: both
// readings mean the same thing, an empty payload with nothing attested.
func TestSignalCompatEmptyPayloadIsHarmlesslyAmbiguous(t *testing.T) {
	t.Parallel()

	dc := converter.GetDefaultDataConverter()

	empty := &v1.Node_Outputs{}
	payload, err := dc.ToPayload(empty)
	require.NoError(t, err)

	var asDelivery v1.SignalDelivery
	require.NoError(t, dc.FromPayload(payload, &asDelivery),
		"an empty legacy payload is expected to decode cleanly as the current shape too")
	require.Nil(t, asDelivery.GetPayload().GetNamedValues())
	require.Nil(t, asDelivery.GetSender())

	// And the compat wrapper's own fallback, exercised end to end, agrees:
	// whichever branch actually fired, the result is indistinguishable.
	var viaCompat v1.SignalDelivery
	require.NoError(t, newCompatConverter().FromPayload(payload, &viaCompat))
	require.Empty(t, viaCompat.GetPayload().GetNamedValues())
	require.Nil(t, viaCompat.GetSender())
}

// TestSignalCompatConverterDecodesTheLegacyShape is the forward-compatibility
// direction: an old server (or a signal already sitting in an execution's
// history from before #194) sends a bare Node_Outputs, and a new worker must
// still receive its payload intact rather than dropping the signal as
// corrupted.
func TestSignalCompatConverterDecodesTheLegacyShape(t *testing.T) {
	t.Parallel()

	legacy := &v1.Node_Outputs{NamedValues: map[string]*v1.Value{
		"approved": v1.NewLiteral(true),
		"by":       v1.NewLiteral("someone@example.com"),
	}}
	payload, err := converter.GetDefaultDataConverter().ToPayload(legacy)
	require.NoError(t, err)

	var delivery v1.SignalDelivery
	require.NoError(t, newCompatConverter().FromPayload(payload, &delivery),
		"a bare Node_Outputs payload was rejected instead of falling back")

	require.True(t, delivery.GetPayload().GetNamedValues()["approved"].GetLiteral().GetBoolValue(),
		"the legacy payload's content was lost in the fallback")
	require.Equal(t, "someone@example.com",
		delivery.GetPayload().GetNamedValues()["by"].GetLiteral().GetStringValue())

	// Honest about having nothing to attest: never an empty-but-present
	// SignalSender that could be mistaken for an attested-but-anonymous one.
	require.Nil(t, delivery.GetSender(),
		"a legacy signal produced a non-nil sender, which signalSenderValue would render "+
			"identically to a real attestation")
}

// TestSignalCompatConverterDecodesTheCurrentShape is the same fallback's
// other, more common branch: an up-to-date server's SignalDelivery decodes
// directly, without ever reaching the legacy attempt.
func TestSignalCompatConverterDecodesTheCurrentShape(t *testing.T) {
	t.Parallel()

	want := &v1.SignalDelivery{
		Payload: &v1.Node_Outputs{NamedValues: map[string]*v1.Value{"approved": v1.NewLiteral(true)}},
		Sender: &v1.SignalSender{
			Identity: &v1.WorkloadIdentity{Subject: "real-caller@example.com", Namespace: "team-a"},
		},
	}
	payload, err := converter.GetDefaultDataConverter().ToPayload(want)
	require.NoError(t, err)

	var got v1.SignalDelivery
	require.NoError(t, newCompatConverter().FromPayload(payload, &got))

	require.True(t, got.GetPayload().GetNamedValues()["approved"].GetLiteral().GetBoolValue())
	require.Equal(t, "real-caller@example.com", got.GetSender().GetIdentity().GetSubject())
	require.Equal(t, "team-a", got.GetSender().GetIdentity().GetNamespace())
}

// TestSignalCompatConverterFallsThroughForEverythingElse checks that the
// wrapper only special-cases *v1.SignalDelivery — every other decode target
// (RunState, an activity argument, a plain int) must behave exactly as the
// default converter alone would, since this wrapper is installed on the whole
// workflow context, not just the one signal channel it exists for.
func TestSignalCompatConverterFallsThroughForEverythingElse(t *testing.T) {
	t.Parallel()

	dc := converter.GetDefaultDataConverter()
	compat := newCompatConverter()

	state := &v1.RunState{Workflow: &v1.Workflow{Name: "passthrough"}}
	payload, err := dc.ToPayload(state)
	require.NoError(t, err)

	var viaDefault, viaCompat v1.RunState
	require.NoError(t, dc.FromPayload(payload, &viaDefault))
	require.NoError(t, compat.FromPayload(payload, &viaCompat))
	require.Equal(t, viaDefault.GetWorkflow().GetName(), viaCompat.GetWorkflow().GetName())

	intPayload, err := dc.ToPayload(42)
	require.NoError(t, err)
	var n int
	require.NoError(t, compat.FromPayload(intPayload, &n))
	require.Equal(t, 42, n)
}
