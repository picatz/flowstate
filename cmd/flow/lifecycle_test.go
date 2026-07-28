package main

import (
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

// A capability is not done until somebody can reach it, so these cover the path a
// person takes — flags, through validation, to an RPC a server receives — rather
// than only the request builders.

// lifecycleCommand builds the command the lifecycle verbs expect, with the
// process-wide flags reset and restored.
func lifecycleCommand(t *testing.T) (*cobra.Command, *strings.Builder, *strings.Builder) {
	t.Helper()

	previous := struct {
		cancelRunID, terminateRunID, terminateWhy, listPageToken string
		listPageSize                                             int32
		listAll                                                  bool
	}{cancelRunID, terminateRunID, terminateWhy, listPageToken, listPageSize, listAll}

	t.Cleanup(func() {
		cancelRunID, terminateRunID, terminateWhy = previous.cancelRunID, previous.terminateRunID, previous.terminateWhy
		listPageToken, listPageSize, listAll = previous.listPageToken, previous.listPageSize, previous.listAll
	})

	cancelRunID, terminateRunID, terminateWhy = "", "", ""
	listPageToken, listPageSize, listAll = "", 0, false

	var out, errOut strings.Builder
	cmd := &cobra.Command{}
	cmd.SetContext(t.Context())
	cmd.SetOut(&out)
	cmd.SetErr(&errOut)

	return cmd, &out, &errOut
}

func TestCancelReachesTheServer(t *testing.T) {
	fake := &fakeWorkflowService{}
	serveFake(t, fake)
	cmd, _, errOut := lifecycleCommand(t)

	cancelRunID = "run-1"

	require.NoError(t, runCancel(cmd, []string{"deploy-abc123"}))

	require.NotNil(t, fake.gotCancel, "nothing reached the server")
	require.Equal(t, "deploy-abc123", fake.gotCancel.GetWorkflowId())
	require.Equal(t, "run-1", fake.gotCancel.GetRunId(), "--run-id was dropped")

	// Cancellation is a request, not a result. Reporting it as "cancelled" would
	// claim something not yet true, and a script would build on the claim.
	require.NotContains(t, strings.ToLower(errOut.String()), "cancelled ",
		"a cooperative request was reported as a completed one")
	require.Contains(t, errOut.String(), "cleanup",
		"the message does not say the run is still finishing")
}

func TestTerminateReachesTheServerWithItsReason(t *testing.T) {
	fake := &fakeWorkflowService{}
	serveFake(t, fake)
	cmd, _, _ := lifecycleCommand(t)

	terminateWhy = "stuck on a dependency that is never coming back"

	require.NoError(t, runTerminate(cmd, []string{"deploy-abc123"}))

	require.NotNil(t, fake.gotTerminate, "nothing reached the server")
	require.Equal(t, "deploy-abc123", fake.gotTerminate.GetWorkflowId())

	// A terminated run leaves no account of itself, so dropping the reason loses
	// the only explanation anyone will ever find.
	require.Equal(t, "stuck on a dependency that is never coming back",
		fake.gotTerminate.GetReason(), "--reason was dropped")
}

func TestListRendersRunsAndSaysWhenMoreRemain(t *testing.T) {
	fake := &fakeWorkflowService{
		listResponses: []*v1.ListResponse{{
			Runs: []*v1.RunSummary{
				{
					WorkflowId: "run-running",
					Status:     v1.RunResponse_STATUS_RUNNING,
					StartTime:  timestamppb.New(mustTime(t, "2026-07-01T09:00:00Z")),
				},
				{
					WorkflowId: "run-done",
					Status:     v1.RunResponse_STATUS_COMPLETED,
					StartTime:  timestamppb.New(mustTime(t, "2026-07-01T08:00:00Z")),
					CloseTime:  timestamppb.New(mustTime(t, "2026-07-01T08:30:00Z")),
				},
			},
			NextPageToken: "more",
		}},
	}
	serveFake(t, fake)
	cmd, out, errOut := lifecycleCommand(t)

	require.NoError(t, runList(cmd, nil))

	require.Contains(t, out.String(), "run-running")
	require.Contains(t, out.String(), "run-done")
	require.Contains(t, out.String(), "2026-07-01T08:30:00Z", "a finished run lost its close time")

	// A run still going has no close time. Rendering the zero instant would
	// report it as having finished in 1970.
	require.NotContains(t, out.String(), "1970", "an unfinished run was given a close time")

	// The listing is a bounded scan, so a page that came back with a token still
	// set means runs remain. A caller that stops here misses their own runs, so
	// this has to be said rather than implied.
	require.Contains(t, errOut.String(), "more runs remain")
	require.Contains(t, errOut.String(), "--all")
}

// A short page is not the end of the listing, so --all keeps asking until the
// token is empty. This is the flag that makes the bounded scan usable rather than
// a trap.
func TestListAllWalksEveryPage(t *testing.T) {
	fake := &fakeWorkflowService{
		listResponses: []*v1.ListResponse{
			// Deliberately empty with a token set: a scan can spend its whole
			// budget on runs belonging to somebody else and find none of yours.
			{NextPageToken: "page-2"},
			{Runs: []*v1.RunSummary{{WorkflowId: "run-late", Status: v1.RunResponse_STATUS_RUNNING}}},
		},
	}
	serveFake(t, fake)
	cmd, out, errOut := lifecycleCommand(t)

	listAll = true
	require.NoError(t, runList(cmd, nil))

	require.Equal(t, 2, fake.listCalls, "--all stopped before the listing was exhausted")
	require.Contains(t, out.String(), "run-late",
		"a run found after an empty page was never reported")
	require.NotContains(t, errOut.String(), "more runs remain",
		"an exhausted listing still claimed there was more")

	// The second request has to continue where the first stopped, or --all is an
	// infinite loop over page one.
	require.Equal(t, "page-2", fake.lastListToken, "the page token was not carried forward")
}

func TestListRefusalsExplainThemselves(t *testing.T) {
	fake := &fakeWorkflowService{
		listErr: connect.NewError(connect.CodePermissionDenied, errors.New("no")),
	}
	serveFake(t, fake)
	cmd, _, _ := lifecycleCommand(t)

	err := runList(cmd, nil)
	require.Error(t, err)

	// A listing names no run, so "check the id" would be answering a question
	// nobody asked.
	require.Contains(t, err.Error(), "listing runs")
	require.NotContains(t, err.Error(), "check the id")
}

// mustTime parses a fixed instant for a fixture.
func mustTime(t *testing.T, value string) time.Time {
	t.Helper()

	parsed, err := time.Parse(time.RFC3339, value)
	require.NoError(t, err)

	return parsed
}

// `--all` keeps asking until the server says it is done, which makes its
// termination the server's decision rather than the CLI's.
//
// A server that answers with a token it has already issued would otherwise loop
// here forever. That is not obviously a fault from the outside — a hanging `flow
// list` looks like a large listing — so it is reported rather than endured.
func TestListAllStopsIfTheTokenDoesNotAdvance(t *testing.T) {
	fake := &fakeWorkflowService{
		// The same token every time, as a buggy or hostile server would.
		listResponses: []*v1.ListResponse{
			{Runs: []*v1.RunSummary{{WorkflowId: "run-1", Status: v1.RunResponse_STATUS_RUNNING}}, NextPageToken: "stuck"},
			{Runs: []*v1.RunSummary{{WorkflowId: "run-2", Status: v1.RunResponse_STATUS_RUNNING}}, NextPageToken: "stuck"},
			{Runs: []*v1.RunSummary{{WorkflowId: "run-3", Status: v1.RunResponse_STATUS_RUNNING}}, NextPageToken: "stuck"},
		},
	}
	serveFake(t, fake)
	cmd, out, _ := lifecycleCommand(t)

	listAll = true

	err := runList(cmd, nil)
	require.Error(t, err, "a non-advancing page token was followed indefinitely")
	require.Contains(t, err.Error(), "same page token twice")

	// It stopped, and it did not throw away what it had already been told.
	require.Contains(t, out.String(), "run-1")
	require.Less(t, fake.listCalls, 4, "the CLI kept asking after the token stopped moving")
}

// Cancel and Terminate are refused before anything is addressed, so a malformed
// request is a bad request rather than a lookup against a real run.
func TestStopVerbsRejectAMalformedRequest(t *testing.T) {
	for _, test := range []struct {
		name string
		run  func(*cobra.Command) error
	}{
		{"cancel with no workflow id", func(c *cobra.Command) error { return runCancel(c, []string{""}) }},
		{"terminate with no workflow id", func(c *cobra.Command) error { return runTerminate(c, []string{""}) }},
	} {
		t.Run(test.name, func(t *testing.T) {
			fake := &fakeWorkflowService{}
			serveFake(t, fake)
			cmd, _, _ := lifecycleCommand(t)

			require.Error(t, test.run(cmd))

			// Nothing left the process. A request the schema refuses should not
			// become a call the server has to answer.
			require.Nil(t, fake.gotCancel, "a malformed cancel still reached the server")
			require.Nil(t, fake.gotTerminate, "a malformed terminate still reached the server")
		})
	}
}
