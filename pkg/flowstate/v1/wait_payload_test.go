package flowstatev1_test

import (
	"testing"

	"github.com/google/go-cmp/cmp"
	"github.com/stretchr/testify/require"
	expr "google.golang.org/genproto/googleapis/api/expr/v1alpha1"
	"google.golang.org/protobuf/testing/protocmp"

	v1 "github.com/picatz/flowstate/pkg/flowstate/v1"
)

// entries reads a wait's payload mapping as a plain map.
func entries(t *testing.T, outputs *v1.Node_Outputs) map[string]*expr.Value {
	t.Helper()

	mapping := outputs.GetNamedValues()[v1.PayloadOutput].GetLiteral().GetMapValue()
	require.NotNil(t, mapping, "a signal wait produced no payload mapping")

	out := make(map[string]*expr.Value, len(mapping.GetEntries()))
	for _, entry := range mapping.GetEntries() {
		out[entry.GetKey().GetStringValue()] = entry.GetValue()
	}
	return out
}

// TestASenderCannotNameAnythingOutsideItsPayload is the property, stated in the
// direction that matters.
//
// A signal's payload used to become the step's outputs directly, so whoever sent
// the signal chose names in the namespace every later expression resolves
// against. `timed_out` survived that only because it was written last — a defence
// that holds for exactly the names somebody thought to write down, and this
// engine will grow more wait outputs.
//
// The sender here tries the reserved name, the payload root itself, and an
// ordinary one. None of them may appear anywhere but inside `payload`.
func TestASenderCannotNameAnythingOutsideItsPayload(t *testing.T) {
	t.Parallel()

	hostile := &v1.Node_Outputs{NamedValues: map[string]*v1.Value{
		v1.TimedOutOutput: v1.NewLiteral(false),
		v1.PayloadOutput:  v1.NewLiteral("shadowed"),
		"approved":        v1.NewLiteral(true),
	}}

	outputs := v1.SignalOutputs(hostile, true)

	// The engine's own answer, unchanged by anything the sender said.
	require.True(t, outputs.GetNamedValues()[v1.TimedOutOutput].GetLiteral().GetBoolValue(),
		"a sender overrode the wait's own account of how it ended")

	// Exactly two names at the top: what the engine observed, and the one root
	// everything else lives under. A sender adding a third would be a sender
	// choosing an identifier that later expressions resolve.
	require.Len(t, outputs.GetNamedValues(), 2,
		"a sender's key reached the step's own output namespace: %v", outputs.GetNamedValues())

	// Everything the sender sent is still readable — rooted, not discarded.
	payload := entries(t, outputs)
	require.True(t, payload["approved"].GetBoolValue())
	require.Equal(t, "shadowed", payload[v1.PayloadOutput].GetStringValue(),
		"a sender may say `payload` inside its payload; it is only a name there")
	require.False(t, payload[v1.TimedOutOutput].GetBoolValue(),
		"a sender may say `timed_out` inside its payload without it meaning anything")
}

// TestATimerWaitHasNoPayload keeps the two kinds of wait honest about themselves.
//
// A sleep has no sender, so offering an empty payload would invite
// `${pause.payload.x}` on a step where one can never arrive. The answer to that
// should be a diagnostic, not an empty map.
func TestATimerWaitHasNoPayload(t *testing.T) {
	t.Parallel()

	outputs := v1.TimerOutputs(false)

	require.NotContains(t, outputs.GetNamedValues(), v1.PayloadOutput,
		"a timer wait offers a payload nothing can ever fill")
	require.Len(t, outputs.GetNamedValues(), 1)
}

// TestASignalThatTimedOutStillHasAPayloadToLookIn is why the root is always
// present.
//
// `has(gate.payload.approved)` has to be answerable after a timeout, rather than
// failing on a missing `payload` — an author guarding on `timed_out` first is the
// documented pattern, and the guard should not be load-bearing for whether the
// expression parses at all.
func TestASignalThatTimedOutStillHasAPayloadToLookIn(t *testing.T) {
	t.Parallel()

	outputs := v1.SignalOutputs(nil, true)

	require.Contains(t, outputs.GetNamedValues(), v1.PayloadOutput)
	require.Empty(t, entries(t, outputs))
}

// TestAPayloadEncodesTheSameWayTwice covers a determinism input nobody would
// look for.
//
// This value is serialized into the run's state and carried across every
// Continue-As-New, and it is built from a protobuf map, whose iteration order is
// deliberately unspecified. Two encodings of one payload differing would be a
// difference in persisted workflow state with no cause a reader could see.
func TestAPayloadEncodesTheSameWayTwice(t *testing.T) {
	t.Parallel()

	sent := &v1.Node_Outputs{NamedValues: map[string]*v1.Value{
		"zulu": v1.NewLiteral(1), "alpha": v1.NewLiteral(2),
		"mike": v1.NewLiteral(3), "delta": v1.NewLiteral(4),
	}}

	first := v1.SignalOutputs(sent, false)
	for range 20 {
		require.Empty(t, cmp.Diff(first, v1.SignalOutputs(sent, false), protocmp.Transform()),
			"the same payload encoded two different ways")
	}
}
