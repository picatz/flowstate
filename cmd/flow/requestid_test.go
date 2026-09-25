package main

import (
	"strings"
	"testing"
	"uuid"

	"github.com/spf13/cobra"
	"github.com/stretchr/testify/require"

	v1 "github.com/picatz/flowstate/pkg/flowstate/v1"
)

// `flow run`'s half of [v1.RunRequest.request_id]: every invocation carries a
// key, a caller may supply their own, and a reuse is said to the person.

func runCommandForTest(t *testing.T, fake *fakeWorkflowService) (cmd *cobra.Command, out, errOut *strings.Builder) {
	t.Helper()

	serveFake(t, fake)
	cmd, out, errOut = watchCommandForTest(t)
	cmd.Flags().String("request-id", "", "")
	require.NoError(t, cmd.Flags().Set("interval", "1ms"))

	return cmd, out, errOut
}

func startedThenFinished() *fakeWorkflowService {
	return &fakeWorkflowService{
		runResponse: &v1.RunResponse{
			WorkflowId: "flowstate-request-3f7c",
			RunId:      "0198f1e2-0000-7000-8000-000000000000",
			Status:     v1.RunResponse_STATUS_RUNNING,
		},
		getResponse: response(v1.RunResponse_STATUS_COMPLETED, "hello"),
	}
}

func TestRunSendsAFreshRequestIDPerInvocation(t *testing.T) {
	first := startedThenFinished()
	cmd, _, _ := runCommandForTest(t, first)
	require.NoError(t, runWorkflow(cmd, []string{"../../examples/hello-world/workflow.yaml"}))

	require.NotNil(t, first.gotRun)
	require.NotNil(t, first.gotRun.RequestId, "a run submitted without a key is a run that cannot be retried safely")
	firstID, err := uuid.Parse(first.gotRun.GetRequestId())
	require.NoError(t, err, "the generated key is a UUID: %q", first.gotRun.GetRequestId())

	second := startedThenFinished()
	cmd, _, _ = runCommandForTest(t, second)
	require.NoError(t, runWorkflow(cmd, []string{"../../examples/hello-world/workflow.yaml"}))
	require.NotEqual(t, firstID.String(), second.gotRun.GetRequestId(),
		"two invocations are two submissions; only a retry of one reuses its key")
}

func TestRunPassesACallerSuppliedRequestID(t *testing.T) {
	fake := startedThenFinished()
	cmd, _, _ := runCommandForTest(t, fake)
	require.NoError(t, cmd.Flags().Set("request-id", "ci-4711-attempt-2"))

	require.NoError(t, runWorkflow(cmd, []string{"../../examples/hello-world/workflow.yaml"}))
	require.Equal(t, "ci-4711-attempt-2", fake.gotRun.GetRequestId(), "--request-id was dropped")
}

func TestRunSaysWhenTheRunWasReused(t *testing.T) {
	fake := startedThenFinished()
	fake.runResponse.Reused = true
	cmd, _, errOut := runCommandForTest(t, fake)

	require.NoError(t, runWorkflow(cmd, []string{"../../examples/hello-world/workflow.yaml"}))
	require.Contains(t, errOut.String(), "already started workflow",
		"a retry answered with the earlier attempt's run must say so; the person came back to learn exactly that")
	require.Contains(t, errOut.String(), "flow watch flowstate-request-3f7c")
}
