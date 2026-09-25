package server_test

import (
	"testing"

	"connectrpc.com/connect"
	"github.com/stretchr/testify/require"

	v1 "github.com/picatz/flowstate/pkg/flowstate/v1"
	"github.com/picatz/flowstate/pkg/flowstate/v1/internal/conformance"
)

// TestSignalPayloadDepthCasesAtTheDoors is the durable half of the #1770
// bound: the shared cases through the real Signal and SignalWithStart
// handlers against a running entity, so a payload at the bound is proved
// deliverable and one past it refused as the caller's problem. The local
// half is wait_local_depth_test.go.
func TestSignalPayloadDepthCasesAtTheDoors(t *testing.T) {
	t.Parallel()

	temporal, _ := newTemporalNamespace(t)
	s := mustNew(t, temporal)

	// The entity exists before either door is asked, so that Signal has a run
	// to address and SignalWithStart takes its already-running branch: both
	// doors then reach the same run, and a refusal is the door's, not "no such
	// run". No worker is started because nothing here needs the run to make
	// progress — the claim is about admission.
	started, err := s.SignalWithStart(t.Context(), connect.NewRequest(&v1.SignalWithStartRequest{
		EntityKey: "depth-bound",
		Workflow:  entityWorkflow(nil),
		Name:      "update",
		Payload:   updatePayload(1, false),
	}))
	require.NoError(t, err)
	require.True(t, started.Msg.GetCreated())

	// A refusal is reported as InvalidArgument — the payload is the caller's
	// to fix — checked inside the door so the shared assertion sees the code
	// beside the sentence.
	callersProblem := func(err error) error {
		if err != nil {
			require.Equal(t, connect.CodeInvalidArgument, connect.CodeOf(err),
				"refused as something other than the caller's problem: %v", err)
		}
		return err
	}

	t.Run("Signal", func(t *testing.T) {
		conformance.AssertSignalPayloadDepthCases(t, func(payload *v1.Node_Outputs) error {
			_, err := s.Signal(t.Context(), connect.NewRequest(&v1.SignalRequest{
				WorkflowId: started.Msg.GetWorkflowId(),
				Name:       "update",
				Payload:    payload,
			}))
			return callersProblem(err)
		})
	})

	t.Run("SignalWithStart", func(t *testing.T) {
		conformance.AssertSignalPayloadDepthCases(t, func(payload *v1.Node_Outputs) error {
			_, err := s.SignalWithStart(t.Context(), connect.NewRequest(&v1.SignalWithStartRequest{
				EntityKey: "depth-bound",
				Workflow:  entityWorkflow(nil),
				Name:      "update",
				Payload:   payload,
			}))
			return callersProblem(err)
		})
	})
}

// TestSignalRefusesADeepPayloadBeforeAnyRoundTrip is signal_size_test.go's
// nil-client proof for the other dimension: the refusal has to happen before
// the tenancy lookup, and a regression that moved it after would panic here
// on the nil client rather than confuse anybody in production.
func TestSignalRefusesADeepPayloadBeforeAnyRoundTrip(t *testing.T) {
	t.Parallel()

	s := mustNew(t, nil)

	for _, c := range conformance.SignalPayloadDepthCases() {
		if c.Refusal == "" {
			continue
		}

		t.Run(c.Name, func(t *testing.T) {
			_, err := s.Signal(t.Context(), connect.NewRequest(&v1.SignalRequest{
				WorkflowId: "some-run",
				Name:       "approval",
				Payload:    c.Payload,
			}))
			require.Error(t, err)
			require.Equal(t, connect.CodeInvalidArgument, connect.CodeOf(err))
			require.Contains(t, err.Error(), c.Refusal)

			_, err = s.SignalWithStart(t.Context(), connect.NewRequest(&v1.SignalWithStartRequest{
				EntityKey: "order-1",
				Workflow:  entityWorkflow(nil),
				Name:      "update",
				Payload:   c.Payload,
			}))
			require.Error(t, err)
			require.Equal(t, connect.CodeInvalidArgument, connect.CodeOf(err),
				"one door with a bound and one without is no bound at all")
			require.Contains(t, err.Error(), c.Refusal)
		})
	}
}
