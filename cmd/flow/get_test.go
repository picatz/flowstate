package main

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"connectrpc.com/connect"
	"github.com/spf13/cobra"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/types/known/timestamppb"

	v1 "github.com/picatz/flowstate/pkg/flowstate/v1"
)

// Get implements [flowstatev1connect.WorkflowServiceHandler].
//
// Defined on the same fake the signal tests use, so one stand-in server covers
// every verb the CLI has.
func (f *fakeWorkflowService) Get(_ context.Context, req *connect.Request[v1.GetRequest]) (*connect.Response[v1.GetResponse], error) {
	f.gotGet = req.Msg
	if f.onGet != nil {
		f.onGet()
	}
	if f.getErr != nil {
		return nil, f.getErr
	}
	return connect.NewResponse(f.getResponse), nil
}

// getCommand builds the command runGet expects, with the flags it declares.
//
// Declared here rather than reset, because they are no longer package variables: a
// flag lives in the FlagSet of the command that declared it, so a fresh command is a
// fresh set of flags and there is nothing to leak into the next test.
func getCommand(t *testing.T) (*cobra.Command, *strings.Builder, *strings.Builder) {
	t.Helper()

	var out, errOut strings.Builder
	cmd := &cobra.Command{}
	cmd.Flags().String("run-id", "", "")
	addOutputFlag(cmd)
	addServerFlags(cmd)
	cmd.SetContext(t.Context())
	cmd.SetOut(&out)
	cmd.SetErr(&errOut)

	return cmd, &out, &errOut
}

// TestGetSeparatesOutputsFromStatus is the property that makes `flow get x | jq`
// work: a workload's data goes to stdout and nothing else does.
func TestGetSeparatesOutputsFromStatus(t *testing.T) {
	fake := &fakeWorkflowService{
		getResponse: &v1.GetResponse{
			WorkflowId: "flowstate-workflow-3f7c",
			RunId:      "0198f1e2-0000-7000-8000-000000000000",
			Status:     v1.RunResponse_STATUS_COMPLETED,
			Kind: &v1.GetResponse_Outputs{
				Outputs: &v1.Workflow_StepOutputs{
					StepValues: map[string]*v1.Node_Outputs{
						"greet": {NamedValues: map[string]*v1.Value{"result": v1.NewLiteral("hello")}},
					},
				},
			},
		},
	}
	serveFake(t, fake)
	cmd, out, errOut := getCommand(t)

	require.NoError(t, runGet(cmd, []string{"flowstate-workflow-3f7c"}))

	require.Equal(t, "flowstate-workflow-3f7c", fake.gotGet.GetWorkflowId())

	// Everything on stdout has to parse as the outputs, or a pipe into jq breaks.
	require.Contains(t, out.String(), `"greet"`)
	require.NotContains(t, out.String(), "COMPLETED",
		"the status was written to stdout, which corrupts anything piping the outputs")

	require.Contains(t, errOut.String(), "COMPLETED")
	require.Contains(t, errOut.String(), "flowstate-workflow-3f7c")
}

// TestGetReportsWhatTheRunAnswered is the other half of a finished run: the
// transcript says what each step produced, and `run_outputs` says what the workflow
// promised to report.
//
// Both directions matter here. The values reach a person on stderr, named, without
// their having to read the document — and they do not reach stdout, where a second
// copy would break the one property this command is built around.
func TestGetReportsWhatTheRunAnswered(t *testing.T) {
	fake := &fakeWorkflowService{
		getResponse: &v1.GetResponse{
			WorkflowId: "flowstate-workflow-3f7c",
			RunId:      "0198f1e2-0000-7000-8000-000000000000",
			Status:     v1.RunResponse_STATUS_COMPLETED,
			Kind: &v1.GetResponse_Outputs{
				Outputs: &v1.Workflow_StepOutputs{
					StepValues: map[string]*v1.Node_Outputs{
						"deploy": {NamedValues: map[string]*v1.Value{"status_code": v1.NewLiteral(200)}},
					},
				},
			},
			RunOutputs: &v1.RunOutputs{Values: map[string]*v1.Value{
				"url":          v1.NewLiteral("https://example.com/build/12"),
				"hosts_placed": v1.NewLiteral(3),
			}},
		},
	}
	serveFake(t, fake)
	cmd, out, errOut := getCommand(t)

	require.NoError(t, runGet(cmd, []string{"flowstate-workflow-3f7c"}))

	require.Contains(t, errOut.String(), "outputs", "the run's declared outputs were not named")
	require.Contains(t, errOut.String(), "url")
	require.Contains(t, errOut.String(), "https://example.com/build/12",
		"a string output was not written as itself, so it cannot be copied off the terminal")
	require.Contains(t, errOut.String(), "hosts_placed")

	require.NotContains(t, out.String(), "https://example.com/build/12",
		"the human section was written to stdout, where the answer document lives")
	require.Contains(t, out.String(), `"deploy"`,
		"the transcript stopped being written when the answer started being")
}

// TestGetOnARunningRunProducesNoOutputs checks the honest answer to "what did it
// produce" while it is still producing it.
func TestGetOnARunningRunProducesNoOutputs(t *testing.T) {
	fake := &fakeWorkflowService{
		getResponse: &v1.GetResponse{
			WorkflowId: "flowstate-workflow-3f7c",
			RunId:      "0198f1e2-0000-7000-8000-000000000000",
			Status:     v1.RunResponse_STATUS_RUNNING,
		},
	}
	serveFake(t, fake)
	cmd, out, errOut := getCommand(t)

	require.NoError(t, runGet(cmd, []string{"flowstate-workflow-3f7c"}))

	require.Empty(t, out.String(), "a run still going wrote outputs it does not have")
	require.Contains(t, errOut.String(), "RUNNING")
}

// TestGetReportsAFailedRunAsAFailure checks that `flow get id && ...` behaves the
// way a shell reader expects.
func TestGetReportsAFailedRunAsAFailure(t *testing.T) {
	fake := &fakeWorkflowService{
		getResponse: &v1.GetResponse{
			WorkflowId: "flowstate-workflow-3f7c",
			RunId:      "0198f1e2-0000-7000-8000-000000000000",
			Status:     v1.RunResponse_STATUS_FAILED,
			Kind: &v1.GetResponse_Error{
				Error: &v1.RunResponse_Error{Message: "step \"deploy\" failed"},
			},
		},
	}
	serveFake(t, fake)
	cmd, _, _ := getCommand(t)

	err := runGet(cmd, []string{"flowstate-workflow-3f7c"})
	require.Error(t, err, "a failed run was reported as a success")
	require.ErrorContains(t, err, "failed")
}

// TestGetRefusesARunIDThatIsNotAUUIDBeforeSending checks the schema's rule runs
// before the round trip.
//
// GetRequest constrains run_id to a UUID, unlike SignalRequest, so this is the
// one place a run id can be wrong in a way worth catching early.
func TestGetRefusesARunIDThatIsNotAUUIDBeforeSending(t *testing.T) {
	fake := &fakeWorkflowService{}
	serveFake(t, fake)
	cmd, _, _ := getCommand(t)

	require.NoError(t, cmd.Flags().Set("run-id", "the-latest-one"))

	err := runGet(cmd, []string{"flowstate-workflow-3f7c"})
	require.Error(t, err, "a run id the schema forbids was accepted")
	require.Nil(t, fake.gotGet, "an invalid run id was sent anyway")
}

// TestGetWithoutARunIDLeavesItAbsent checks that unset means "whichever attempt is
// current" rather than an empty string the schema would refuse for not being a
// UUID.
func TestGetWithoutARunIDLeavesItAbsent(t *testing.T) {
	fake := &fakeWorkflowService{
		getResponse: &v1.GetResponse{
			WorkflowId: "flowstate-workflow-3f7c",
			Status:     v1.RunResponse_STATUS_RUNNING,
		},
	}
	serveFake(t, fake)
	cmd, _, _ := getCommand(t)

	require.NoError(t, runGet(cmd, []string{"flowstate-workflow-3f7c"}))
	require.Nil(t, fake.gotGet.RunId, "an empty run id was sent instead of none at all")
}

// TestGetOnAnUnaddressableRunNamesEveryCause checks that reading gets the same
// three-cause explanation signalling does, named for what was attempted.
func TestGetOnAnUnaddressableRunNamesEveryCause(t *testing.T) {
	fake := &fakeWorkflowService{
		getErr: connect.NewError(connect.CodeNotFound, errors.New(`no such run`)),
	}
	serveFake(t, fake)
	cmd, _, _ := getCommand(t)

	err := runGet(cmd, []string{"flowstate-workflow-3f7c"})
	require.ErrorContains(t, err, "check the id")
	require.ErrorContains(t, err, "tenant")
	require.ErrorContains(t, err, "retention")
}

// TestAPendingActivityLineSaysWhatItIsDoing is the render half of heartbeat phases.
//
// The phase travels a long way to be seen — a task reports it, a ticker heartbeats
// it, Temporal stores it, the server projects it out of a Describe response — and
// every one of those steps can be right while the last one is missing, in which
// case the feature is present, tested, and invisible. That is the failure the house
// rule about reachability describes, and this is the assertion that closes it for
// the two surfaces a person actually reads.
//
// Both `flow get` and `flow watch` render through this function, which is why there
// is one test rather than two: the two surfaces cannot drift because there is only
// one renderer.
func TestAPendingActivityLineSaysWhatItIsDoing(t *testing.T) {
	t.Parallel()

	now := time.Now()

	lines := pendingActivityLines([]*v1.PendingActivity{
		{Attempt: 1, Phase: "reading the response"},
	}, now)

	require.Equal(t, []string{"retrying, attempt 1, reading the response"}, lines)

	// A phase is an aside about the attempt running now; a failure and a countdown
	// describe attempts that are over. So the phase goes last, and the order is
	// asserted rather than the presence of each part — a line that reported what a
	// finished attempt is doing would read as a contradiction.
	lines = pendingActivityLines([]*v1.PendingActivity{{
		Attempt:                  3,
		LastFailure:              "connection refused",
		NextAttemptScheduledTime: timestamppb.New(now.Add(4 * time.Second)),
		Phase:                    "requesting",
	}}, now)

	require.Equal(t,
		[]string{"retrying, attempt 3: connection refused (next attempt in 4s), requesting"},
		lines)
}

// TestAPendingActivityWithNoPhaseSaysNothingAboutIt is the negative direction, and
// the one that matters most for honesty.
//
// A phase is absent in three situations that are not the same: the attempt has not
// reported yet, it is waiting to be retried and so nothing is running to have a
// phase, or the worker predates the field. None of them is "the step is doing
// nothing", so a renderer that printed a word for the empty phase would be
// inventing a fact about a workload — which is the failure this repository treats
// as worse than saying nothing at all.
func TestAPendingActivityWithNoPhaseSaysNothingAboutIt(t *testing.T) {
	t.Parallel()

	lines := pendingActivityLines([]*v1.PendingActivity{
		{Attempt: 2, LastFailure: "boom"},
	}, time.Now())

	require.Equal(t, []string{"retrying, attempt 2: boom"}, lines)
}
