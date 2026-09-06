package server

import (
	"context"
	"testing"

	"connectrpc.com/connect"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	"go.temporal.io/api/workflow/v1"
	"go.temporal.io/api/workflowservice/v1"
	"go.temporal.io/sdk/mocks"

	v1 "github.com/picatz/flowstate/pkg/flowstate/v1"
)

// A filter that errors on a run leaves that run out and says how many (#1689).
// It used to fail the whole listing on the first run it could not answer for,
// which turned `labels["team"] == "x"` — a correct question about a tenant
// where most runs carry no `team` — into an InvalidArgument.

func filteredListing(t *testing.T, executions []*workflow.WorkflowExecutionInfo, filter string) *v1.ListResponse {
	t.Helper()

	temporal := &mocks.Client{}
	temporal.On("ListWorkflow", mock.Anything, mock.Anything).Return(
		&workflowservice.ListWorkflowExecutionsResponse{Executions: executions}, nil)

	response, err := mustNew(t, temporal).List(t.Context(), connect.NewRequest(&v1.ListRequest{Filter: filter}))
	require.NoError(t, err, "a filter that errors on a run failed the listing")

	return response.Msg
}

// TestAFilterThatErrorsOnSomeRunsLeavesThemOutAndCounts: the labelled run is
// kept, the unlabelled ones are left out, the count says so, and no diagnostic
// is raised for a filter that was right about the run it was right about.
func TestAFilterThatErrorsOnSomeRunsLeavesThemOutAndCounts(t *testing.T) {
	t.Parallel()

	executions := []*workflow.WorkflowExecutionInfo{
		labelledRun(t, "fulfilled", "", map[string]string{"team": "fulfillment"}),
		labelledRun(t, "bare-1", "", nil),
		labelledRun(t, "bare-2", "", nil),
		labelledRun(t, "other-team", "", map[string]string{"team": "payments"}),
	}

	page := filteredListing(t, executions, `labels["team"] == "fulfillment"`)

	require.Len(t, page.GetRuns(), 1)
	require.Equal(t, "fulfilled", page.GetRuns()[0].GetWorkflowId())
	require.Equal(t, uint32(2), page.GetExcludedByError(), "the runs the filter could not be evaluated over were not counted")
	require.Empty(t, page.GetFilterDiagnostic(), "a filter right about some runs was reported as wrong")
}

// TestAFilterThatErrorsOnEveryRunIsExplainedOnce: the shape a typo takes. Every
// run is left out, and the page carries one diagnostic naming the error and the
// optional spelling, rather than an empty page indistinguishable from "nothing
// matched".
func TestAFilterThatErrorsOnEveryRunIsExplainedOnce(t *testing.T) {
	t.Parallel()

	executions := []*workflow.WorkflowExecutionInfo{
		labelledRun(t, "bare-1", "", nil),
		labelledRun(t, "bare-2", "", map[string]string{"team": "payments"}),
	}

	page := filteredListing(t, executions, `labels["teamm"] == "payments"`)

	require.Empty(t, page.GetRuns())
	require.Equal(t, uint32(2), page.GetExcludedByError())
	require.Contains(t, page.GetFilterDiagnostic(), "no such key: teamm")
	require.Contains(t, page.GetFilterDiagnostic(), `labels.?teamm.orValue("")`)
}

// TestTheOptionalSpellingAsksTheServerTheSameQuestion: the documented spelling
// keeps the labelled run and leaves nothing out, so a caller who writes what
// the help teaches sees neither a count nor a diagnostic.
func TestTheOptionalSpellingAsksTheServerTheSameQuestion(t *testing.T) {
	t.Parallel()

	executions := []*workflow.WorkflowExecutionInfo{
		labelledRun(t, "fulfilled", "", map[string]string{"team": "fulfillment"}),
		labelledRun(t, "bare", "", nil),
	}

	page := filteredListing(t, executions, `labels.?team.orValue("") == "fulfillment"`)

	require.Len(t, page.GetRuns(), 1)
	require.Equal(t, "fulfilled", page.GetRuns()[0].GetWorkflowId())
	require.Zero(t, page.GetExcludedByError())
	require.Empty(t, page.GetFilterDiagnostic())
}

// TestATypeErrorIsStillRefusedBeforeAnyRunIsRead: a filter that cannot compile
// is the caller's mistake and is refused as one, not evaluated per run.
func TestATypeErrorIsStillRefusedBeforeAnyRunIsRead(t *testing.T) {
	t.Parallel()

	temporal := &mocks.Client{}
	_, err := mustNew(t, temporal).List(t.Context(), connect.NewRequest(&v1.ListRequest{Filter: `status == 1`}))
	require.Error(t, err)
	require.Equal(t, connect.CodeInvalidArgument, connect.CodeOf(err))
	temporal.AssertNotCalled(t, "ListWorkflow", mock.Anything, mock.Anything)
}

var _ = context.Background
