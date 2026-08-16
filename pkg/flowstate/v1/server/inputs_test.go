package server_test

import (
	"testing"
	"time"

	"connectrpc.com/connect"
	"github.com/stretchr/testify/require"

	v1 "github.com/picatz/flowstate/pkg/flowstate/v1"
	"github.com/picatz/flowstate/pkg/flowstate/v1/internal/conformance"
	"github.com/picatz/flowstate/pkg/flowstate/v1/server"
)

// The submit boundary, from the outside.
//
// The shared corpus already holds both drivers to one answer about which
// submissions are refused; what these add is that the *handler* asks — that the
// rule is enforced by the component it belongs to rather than by whoever wired the
// server up. `Run` learned that lesson once already with specification validation,
// which held only because the CLI happened to install an interceptor.

// TestRunRefusesInputsThatDoNotMatchTheDeclarations runs the shared refusal corpus
// against the RPC handler.
//
// Nothing is started, which is the claim: a run that would be wrong is refused
// while the caller is still there to be told, rather than three steps in with two
// requests already sent. Reported as InvalidArgument, because the request is the
// problem and CodeInternal would tell a caller to retry something that can never
// succeed.
func TestRunRefusesInputsThatDoNotMatchTheDeclarations(t *testing.T) {
	t.Parallel()

	temporal, _ := newTemporalNamespace(t)
	flowstate := server.New(temporal)

	for _, refusal := range conformance.InputRefusalCases() {
		t.Run(refusal.Name, func(t *testing.T) {
			_, err := flowstate.Run(t.Context(), connect.NewRequest(&v1.RunRequest{
				Workflow: refusal.Workflow,
				Inputs:   refusal.Inputs,
			}))

			require.Error(t, err, "the submission was accepted")
			require.Equal(t, connect.CodeInvalidArgument, connect.CodeOf(err),
				"reported as something other than the caller's problem: %v", err)
			require.Contains(t, err.Error(), refusal.Contains,
				"the refusal does not say which rule refused: %v", err)
		})
	}
}

// TestAFinishedRunReportsItsDeclaredOutputs is the whole feature, end to end,
// through the surface a caller actually holds.
//
// A run is submitted with arguments, executes on a real worker, and is asked what
// it produced — and the answer is the two values the workflow said it would report
// rather than the transcript of every step, which is the distinction the outputs
// block exists to draw. The default fills in for the argument that was not sent,
// once, at submit.
func TestAFinishedRunReportsItsDeclaredOutputs(t *testing.T) {
	t.Parallel()

	temporal, _ := newTemporalNamespace(t)
	startWorker(t, temporal)
	flowstate := server.New(temporal)

	spec := &v1.Workflow{
		Name:    "answers",
		Profile: v1.CurrentProfile,
		DeclaredInputs: []*v1.InputDeclaration{
			{Name: "region", Type: v1.InputDeclaration_TYPE_STRING, Required: true},
			{Name: "retries", Type: v1.InputDeclaration_TYPE_INT, Default: v1.NewLiteral(int64(3))},
		},
		DeclaredOutputs: []*v1.OutputDeclaration{
			{Name: "where", Value: v1.NewExpr("inputs.region")},
			{Name: "attempts", Value: v1.NewExpr("inputs.retries")},
		},
		Steps: []*v1.Node{{
			Id: "note",
			Kind: &v1.Node_Task{Task: &v1.Task{
				Name:   "log",
				Inputs: map[string]*v1.Value{"message": v1.NewExpr(`"deploying to " + inputs.region`)},
			}},
		}},
	}

	started, err := flowstate.Run(t.Context(), connect.NewRequest(&v1.RunRequest{
		Workflow: spec,
		Inputs:   map[string]*v1.Value{"region": v1.NewLiteral("eu-west-1")},
	}))
	require.NoError(t, err)

	var final *connect.Response[v1.GetResponse]
	require.Eventually(t, func() bool {
		resp, err := flowstate.Get(t.Context(), connect.NewRequest(&v1.GetRequest{
			WorkflowId: started.Msg.GetWorkflowId(),
		}))
		if err != nil || resp.Msg.GetStatus() != v1.RunResponse_STATUS_COMPLETED {
			return false
		}
		final = resp

		return true
	}, 30*time.Second, 100*time.Millisecond, "the run did not finish")

	values := final.Msg.GetRunOutputs().GetValues()
	require.Equal(t, "eu-west-1", values["where"].GetLiteral().GetStringValue(),
		"the run did not report the argument it was started with")
	require.Equal(t, int64(3), values["attempts"].GetLiteral().GetInt64Value(),
		"the default was not applied at submit, or did not reach the outputs")

	// The transcript is still there beside the answer, and is still the transcript:
	// two different questions, two different fields, neither standing in for the
	// other.
	require.Contains(t, final.Msg.GetOutputs().GetStepValues(), "note")
}

// TestAWorkflowWithNoDeclaredOutputsReportsNothing pins the other direction, which
// is every workflow written before this existed.
//
// Unset rather than an empty message: "this workflow promises nothing" and "this
// run produced an empty result" are the same fact here, and the schema deliberately
// has no way to tell them apart — so a caller reading a completed run must not
// start seeing an empty object where it used to see nothing.
func TestAWorkflowWithNoDeclaredOutputsReportsNothing(t *testing.T) {
	t.Parallel()

	temporal, _ := newTemporalNamespace(t)
	startWorker(t, temporal)
	flowstate := server.New(temporal)

	started, err := flowstate.Run(t.Context(), connect.NewRequest(&v1.RunRequest{
		Workflow: &v1.Workflow{
			Name:    "promises-nothing",
			Profile: v1.CurrentProfile,
			Steps: []*v1.Node{{
				Id: "note",
				Kind: &v1.Node_Task{Task: &v1.Task{
					Name:   "log",
					Inputs: map[string]*v1.Value{"message": v1.NewLiteral("hello")},
				}},
			}},
		},
	}))
	require.NoError(t, err)

	require.Eventually(t, func() bool {
		resp, err := flowstate.Get(t.Context(), connect.NewRequest(&v1.GetRequest{
			WorkflowId: started.Msg.GetWorkflowId(),
		}))
		if err != nil || resp.Msg.GetStatus() != v1.RunResponse_STATUS_COMPLETED {
			return false
		}

		require.Nil(t, resp.Msg.GetRunOutputs(),
			"a workflow that declares no outputs reported a result")

		return true
	}, 30*time.Second, 100*time.Millisecond, "the run did not finish")
}
