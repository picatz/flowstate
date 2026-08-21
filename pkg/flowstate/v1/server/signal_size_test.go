package server_test

import (
	"strings"
	"testing"

	"connectrpc.com/connect"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"

	v1 "github.com/picatz/flowstate/pkg/flowstate/v1"
)

// payloadOfSize builds a payload whose encoded size is exactly the given number
// of bytes, by measuring and trimming rather than by predicting varint widths.
func payloadOfSize(t *testing.T, target int) *v1.Node_Outputs {
	t.Helper()

	pad := target
	for range 8 {
		payload := &v1.Node_Outputs{NamedValues: map[string]*v1.Value{
			"blob": v1.NewLiteral(strings.Repeat("x", pad)),
		}}
		size := proto.Size(payload)
		if size == target {
			return payload
		}
		pad -= size - target
		require.Positive(t, pad, "the target size is smaller than the message's own framing")
	}
	t.Fatal("could not converge on the target payload size")
	return nil
}

// TestSignalPayloadBoundIsReached is the boundary in both directions, on the
// check itself: at the limit passes, one byte over names both numbers. The
// `<=` alone would also be satisfied by a bound nothing reaches.
func TestSignalPayloadBoundIsReached(t *testing.T) {
	t.Parallel()

	atLimit := payloadOfSize(t, v1.MaxSignalPayloadBytes)
	require.NoError(t, v1.CheckSignalPayloadSize(atLimit),
		"a payload at exactly the limit must be deliverable, or the limit is a lie by one")

	over := payloadOfSize(t, v1.MaxSignalPayloadBytes+1)
	err := v1.CheckSignalPayloadSize(over)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "over the", "the refusal should say what happened")
	assert.Contains(t, err.Error(), "send a reference", "the refusal should say what to do instead")

	assert.NoError(t, v1.CheckSignalPayloadSize(nil),
		"an absent payload is an empty one, and empty is the smallest thing there is")
}

// TestSignalRefusesAnOversizedPayloadAtTheDoor drives the real handlers. The
// nil client is the proof that matters: the refusal has to happen before any
// round trip to Temporal, and a regression that moved the check after the
// tenancy lookup would panic here before it confused anybody in production.
func TestSignalRefusesAnOversizedPayloadAtTheDoor(t *testing.T) {
	t.Parallel()

	s := mustNew(t, nil)
	over := payloadOfSize(t, v1.MaxSignalPayloadBytes+1)

	_, err := s.Signal(t.Context(), connect.NewRequest(&v1.SignalRequest{
		WorkflowId: "some-run",
		Name:       "approval",
		Payload:    over,
	}))
	require.Error(t, err)
	assert.Equal(t, connect.CodeInvalidArgument, connect.CodeOf(err),
		"an oversized payload is the caller's to fix, which is what InvalidArgument says")
	assert.Contains(t, err.Error(), "over the", "the refusal should carry the diagnosis")

	_, err = s.SignalWithStart(t.Context(), connect.NewRequest(&v1.SignalWithStartRequest{
		EntityKey: "order-1",
		Workflow: &v1.Workflow{
			Name: "entity",
			Steps: []*v1.Node{{
				Id: "gate",
				Kind: &v1.Node_Wait{Wait: &v1.Wait{
					Kind: &v1.Wait_Signal{Signal: &v1.Signal{Name: "mutate"}},
				}},
			}},
		},
		Name:    "mutate",
		Payload: over,
	}))
	require.Error(t, err)
	assert.Equal(t, connect.CodeInvalidArgument, connect.CodeOf(err),
		"one door with a bound and one without is no bound at all")
	assert.Contains(t, err.Error(), "over the")
}
