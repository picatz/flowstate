package main

import (
	"encoding/json"
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
//
// `--output` is no longer among them: it is declared on the command by
// [addOutputFlag] and read back off it, so a test asks for a format the way a caller
// does rather than by assigning to a package variable. The rest still are, and this
// helper is what keeps them from leaking between tests until they follow.
func lifecycleCommand(t *testing.T) (*cobra.Command, *strings.Builder, *strings.Builder) {
	t.Helper()

	var out, errOut strings.Builder
	cmd := &cobra.Command{}
	cmd.Flags().String("run-id", "", "")
	cmd.Flags().String("reason", "", "")
	cmd.Flags().String("page-token", "", "")
	cmd.Flags().Int32("page-size", 0, "")
	cmd.Flags().Bool("all", false, "")
	addOutputFlag(cmd)
	cmd.SetContext(t.Context())
	cmd.SetOut(&out)
	cmd.SetErr(&errOut)

	return cmd, &out, &errOut
}

func TestCancelReachesTheServer(t *testing.T) {
	fake := &fakeWorkflowService{}
	serveFake(t, fake)
	cmd, _, errOut := lifecycleCommand(t)

	require.NoError(t, cmd.Flags().Set("run-id", "run-1"))

	require.NoError(t, runCancel(cmd, []string{"deploy-abc123"}))

	require.NotNil(t, fake.gotCancel, "nothing reached the server")
	require.Equal(t, "deploy-abc123", fake.gotCancel.GetWorkflowId())
	require.Equal(t, "run-1", fake.gotCancel.GetRunId(), "--run-id was dropped")

	// Cancellation is a request, not a result. Reporting it as "cancelled" would
	// claim something not yet true, and a script would build on the claim.
	// Neither spelling of the past tense, since claiming the run *is* cancelled is
	// the misreport this guards — and the repo's own vocabulary is the single-l
	// STATUS_CANCELED, so matching only "cancelled" would miss it.
	require.NotRegexp(t, `(?i)cancell?ed`, errOut.String(),
		"a cooperative request was reported as a completed one")
	require.Regexp(t, `^asked `, errOut.String(),
		"the message does not read as a request")
	require.Contains(t, errOut.String(), "cleanup",
		"the message does not say the run is still finishing")
}

func TestTerminateReachesTheServerWithItsReason(t *testing.T) {
	fake := &fakeWorkflowService{}
	serveFake(t, fake)
	cmd, _, _ := lifecycleCommand(t)

	require.NoError(t, cmd.Flags().Set("reason", "stuck on a dependency that is never coming back"))

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

	// The header first, because everything below asserts a position in a row and
	// a position means nothing without the column it belongs to.
	require.Equal(t,
		[]string{"WORKFLOW_ID", "STATUS", "STARTED", "FINISHED"},
		tableRow(t, out.String(), "WORKFLOW_ID"),
		"the columns are not the ones the rows are checked against")

	// A finished run: every field, in order, on its own line. The close time is
	// the field most easily rendered in the wrong column, and the status is the
	// one nothing used to check at all.
	require.Equal(t,
		[]string{"run-done", "COMPLETED", "2026-07-01T08:00:00Z", "2026-07-01T08:30:00Z"},
		tableRow(t, out.String(), "run-done"))

	// A run still going has no close time, so it renders a placeholder. Rendering
	// the zero instant instead would report it as having finished in 1970.
	require.Equal(t,
		[]string{"run-running", "RUNNING", "2026-07-01T09:00:00Z", "-"},
		tableRow(t, out.String(), "run-running"))

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

	require.NoError(t, cmd.Flags().Set("all", "true"))
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
	// The refusal branch specifically: "listing runs" alone also matches the
	// generic default, so deleting the refusal case entirely would still pass.
	require.Contains(t, err.Error(), "refused while listing runs")
	require.NotContains(t, err.Error(), "check the id")
}

// tableRow returns the whitespace-separated fields of the row that starts with
// the given first column.
//
// A row is a record, so it is checked as one. Asserting substrings against the
// whole buffer is what let three separate rendering bugs through here: `Contains`
// is satisfied by a value appearing anywhere, so swapping the STARTED and
// FINISHED columns passed, giving an unfinished run a close time passed as long as
// some other row had one, and the STATUS column was never asserted at all —
// `statusLabel` returning "" for every status passed every `TestList*`.
//
// Fields rather than the raw line because the separator is a tabwriter's padding,
// which is a rendering detail and not the contract; the order and the content are.
func tableRow(t *testing.T, rendered, first string) []string {
	t.Helper()

	for _, line := range strings.Split(rendered, "\n") {
		if fields := strings.Fields(line); len(fields) > 0 && fields[0] == first {
			return fields
		}
	}

	t.Fatalf("no row beginning %q in the listing:\n%s", first, rendered)

	return nil
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

	require.NoError(t, cmd.Flags().Set("all", "true"))

	err := runList(cmd, nil)
	require.Error(t, err, "a non-advancing page token was followed indefinitely")
	require.Contains(t, err.Error(), "same page token twice")

	// It stopped, and it did not throw away what it had already been told.
	require.Contains(t, out.String(), "run-1")
	require.Equal(t, 2, fake.listCalls,
		"the CLI kept asking after the token stopped moving")
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

// A page failing partway through `--all` must not discard the pages that
// succeeded.
//
// The table is buffered until flushed, so returning the error directly loses
// rows that were retrieved and formatted correctly — and a caller who sees an
// error with no output cannot tell whether it failed on the first page or the
// fortieth.
func TestListAllKeepsRowsWhenALaterPageFails(t *testing.T) {
	fake := &fakeWorkflowService{
		listResponses: []*v1.ListResponse{
			{Runs: []*v1.RunSummary{{WorkflowId: "run-early", Status: v1.RunResponse_STATUS_RUNNING}}, NextPageToken: "page-2"},
		},
		// The second call has no canned response and fails instead.
		listErrAfter: 1,
	}
	serveFake(t, fake)
	cmd, out, _ := lifecycleCommand(t)

	require.NoError(t, cmd.Flags().Set("all", "true"))

	err := runList(cmd, nil)
	require.Error(t, err, "a failed page was reported as success")

	require.Contains(t, out.String(), "run-early",
		"a page that succeeded was thrown away because a later one failed")
}

// The machine formats are a contract with whatever is parsing them — a script, a
// CI job, an agent driving the CLI as a tool — so they are asserted as one.
//
// Every assertion here goes through encoding/json rather than looking for
// substrings, because a consumer will address a field by name and a test that
// matches text would pass on output no `jq` expression could read.

// TestListJSONIsOneDocumentAConsumerCanIndex covers `flow list -o json | jq`.
func TestListJSONIsOneDocumentAConsumerCanIndex(t *testing.T) {
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

	require.NoError(t, cmd.Flags().Set("output", string(FormatJSON)))

	require.NoError(t, runList(cmd, nil))

	var listing struct {
		Runs []struct {
			WorkflowID string `json:"workflowId"`
			Status     string `json:"status"`
			StartTime  string `json:"startTime"`
			CloseTime  string `json:"closeTime"`
		} `json:"runs"`
		NextPageToken string `json:"nextPageToken"`
	}
	require.NoError(t, json.Unmarshal([]byte(out.String()), &listing),
		"the answer is not one JSON document, so `flow list -o json | jq` cannot read it")

	require.Len(t, listing.Runs, 2)

	// The field names are the schema's, not names invented here, which is what
	// makes a consumer's expression survive a change to this file.
	require.Equal(t, "run-running", listing.Runs[0].WorkflowID)
	require.Equal(t, "STATUS_RUNNING", listing.Runs[0].Status,
		"a status came back as something other than its schema name")
	require.Equal(t, "run-done", listing.Runs[1].WorkflowID)
	require.Equal(t, "STATUS_COMPLETED", listing.Runs[1].Status)
	require.Equal(t, "2026-07-01T08:30:00Z", listing.Runs[1].CloseTime)

	// The token reaches a program in the answer rather than only in prose it
	// would have to parse. A listing that stopped early without saying so in a
	// form the reader can act on is how a caller silently misses their own runs.
	require.Equal(t, "more", listing.NextPageToken,
		"a bounded listing did not tell a machine reader there was more")

	// And the prose is withheld, because it is addressed to a person who is not
	// the one reading this.
	require.NotContains(t, errOut.String(), "more runs remain",
		"a machine format still emitted advice meant for a human")
}

// TestListJSONLIsOneRunPerLine covers the streaming shape.
func TestListJSONLIsOneRunPerLine(t *testing.T) {
	fake := &fakeWorkflowService{
		listResponses: []*v1.ListResponse{
			{
				Runs:          []*v1.RunSummary{{WorkflowId: "run-1", Status: v1.RunResponse_STATUS_RUNNING}},
				NextPageToken: "page-2",
			},
			{Runs: []*v1.RunSummary{{WorkflowId: "run-2", Status: v1.RunResponse_STATUS_COMPLETED}}},
		},
	}
	serveFake(t, fake)
	cmd, out, _ := lifecycleCommand(t)

	require.NoError(t, cmd.Flags().Set("output", string(FormatJSONL)))
	require.NoError(t, cmd.Flags().Set("all", "true"))

	require.NoError(t, runList(cmd, nil))

	lines := strings.Split(strings.TrimSpace(out.String()), "\n")
	require.Len(t, lines, 2, "the streaming form did not write one run per line")

	// Each line stands alone, which is the property that lets a reader consume the
	// first run without waiting for the last.
	for i, want := range []string{"run-1", "run-2"} {
		var run struct {
			WorkflowID string `json:"workflowId"`
		}
		require.NoError(t, json.Unmarshal([]byte(lines[i]), &run), "line %d is not a document on its own", i+1)
		require.Equal(t, want, run.WorkflowID)
	}
}

// TestListFailingBeforeAnyRunWritesNothingToStdout is the defect a header hides.
//
// A bare header on stdout is indistinguishable, to anything parsing it, from a
// listing that succeeded and found nothing — so a script sees "you have no runs"
// where the truth is "the server refused you".
func TestListFailingBeforeAnyRunWritesNothingToStdout(t *testing.T) {
	fake := &fakeWorkflowService{
		listErr: connect.NewError(connect.CodePermissionDenied, errors.New("no")),
	}
	serveFake(t, fake)
	cmd, out, _ := lifecycleCommand(t)

	require.Error(t, runList(cmd, nil))

	require.Empty(t, out.String(),
		"a listing that returned nothing still wrote to stdout, which reads as an empty listing")
}

// TestListRefusesAFormatItDoesNotHave checks that a mistyped --output is answered
// rather than quietly ignored.
func TestListRefusesAFormatItDoesNotHave(t *testing.T) {
	fake := &fakeWorkflowService{}
	serveFake(t, fake)
	cmd, _, _ := lifecycleCommand(t)

	require.NoError(t, cmd.Flags().Set("output", "yaml"))

	err := runList(cmd, nil)
	require.Error(t, err, "an unknown --output was accepted and something else was rendered")

	// The message says what is accepted, since that is the next question.
	require.Contains(t, err.Error(), "yaml")
	require.Contains(t, err.Error(), "text")
	require.Contains(t, err.Error(), "jsonl")

	require.Equal(t, 0, fake.listCalls,
		"a request that could not be rendered was still sent")
}
