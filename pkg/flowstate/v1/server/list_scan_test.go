package server

import (
	"context"
	"fmt"
	"strconv"
	"testing"

	"connectrpc.com/connect"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	common "go.temporal.io/api/common/v1"
	"go.temporal.io/api/workflow/v1"
	"go.temporal.io/api/workflowservice/v1"
	"go.temporal.io/sdk/converter"
	"go.temporal.io/sdk/mocks"

	v1types "github.com/picatz/flowstate/pkg/flowstate/v1"
)

// The scan is the part of List that can be made expensive by someone else.
//
// Because the tenant is a memo, the server reads executions and keeps its
// caller's, so the work a request does is set by how many runs are in the
// namespace rather than by how many the caller asked for. In a namespace shared
// by several tenants — or one where another tenant is simply busier — a request
// for ten runs can walk past a great many that are not the caller's.
//
// Left unbounded that is a request whose cost a caller does not choose and cannot
// see: `flow list` against a namespace holding a hundred thousand runs would walk
// all hundred thousand to report none. So this test builds exactly that namespace
// — an endless listing containing nothing the caller owns — and pins that the
// server stops, reports nothing, and says there is more.

// otherTenantsRun is an execution belonging to somebody else.
func otherTenantsRun(t *testing.T, id string) *workflow.WorkflowExecutionInfo {
	t.Helper()

	payload, err := converter.GetDefaultDataConverter().ToPayload("somebody-else")
	require.NoError(t, err)

	return &workflow.WorkflowExecutionInfo{
		Execution: &common.WorkflowExecution{WorkflowId: id},
		Memo:      &common.Memo{Fields: map[string]*common.Payload{namespaceMemoKey: payload}},
	}
}

func TestListStopsScanningAndSaysSo(t *testing.T) {
	t.Parallel()

	temporal := &mocks.Client{}

	// A namespace that never runs out and holds nothing of the caller's: every
	// page is full, every page belongs to another tenant, and there is always a
	// next page. Without a bound this listing does not terminate.
	scanned := 0
	temporal.On("ListWorkflow", mock.Anything, mock.Anything).Return(
		func(_ context.Context, request *workflowservice.ListWorkflowExecutionsRequest) *workflowservice.ListWorkflowExecutionsResponse {
			executions := make([]*workflow.WorkflowExecutionInfo, 0, request.GetPageSize())
			for range int(request.GetPageSize()) {
				executions = append(executions, otherTenantsRun(t, "not-yours"))
			}
			scanned += len(executions)

			return &workflowservice.ListWorkflowExecutionsResponse{
				Executions:    executions,
				NextPageToken: []byte("there is always more"),
			}
		},
		nil,
	)

	server := New(temporal)

	response, err := server.List(t.Context(), connect.NewRequest(&v1types.ListRequest{PageSize: 10}))
	require.NoError(t, err)

	// The whole point: asking for ten did not walk the namespace.
	require.LessOrEqual(t, scanned, maxListScan,
		"a request for a small page read more executions than the scan bound allows")

	// And it spent the budget rather than giving up early, which the bound above
	// cannot distinguish on its own: a listing that stopped after one batch would
	// satisfy it while quietly reporting far less than it could have found. Both
	// directions matter, because under-scanning hides the caller's own runs just
	// as effectively as over-scanning costs the server.
	require.Equal(t, maxListScan, scanned,
		"the listing stopped short of its scan budget with matches still to look for")

	require.Empty(t, response.Msg.GetRuns(), "runs belonging to another tenant were returned")

	// And having stopped early, it must say so. A caller that reads an empty page
	// as the end of the listing is a caller who silently misses their own runs —
	// which is the failure this token exists to prevent.
	require.NotEmpty(t, response.Msg.GetNextPageToken(),
		"a listing that stopped on its scan bound reported itself as complete")
}

// A page that fills stops there, rather than always spending the whole scan
// budget.
func TestListStopsOnceThePageIsFull(t *testing.T) {
	t.Parallel()

	temporal := &mocks.Client{}

	scanned := 0
	temporal.On("ListWorkflow", mock.Anything, mock.Anything).Return(
		func(_ context.Context, request *workflowservice.ListWorkflowExecutionsRequest) *workflowservice.ListWorkflowExecutionsResponse {
			executions := make([]*workflow.WorkflowExecutionInfo, 0, request.GetPageSize())
			for range int(request.GetPageSize()) {
				// Owned by the caller: New with no namespace option resolves the
				// empty tenant, which a run with no recorded tenant belongs to.
				executions = append(executions, &workflow.WorkflowExecutionInfo{
					Execution: &common.WorkflowExecution{WorkflowId: "mine"},
				})
			}
			scanned += len(executions)

			return &workflowservice.ListWorkflowExecutionsResponse{
				Executions:    executions,
				NextPageToken: []byte("more"),
			}
		},
		nil,
	)

	response, err := New(temporal).List(t.Context(), connect.NewRequest(&v1types.ListRequest{PageSize: 5}))
	require.NoError(t, err)

	require.Len(t, response.Msg.GetRuns(), 5, "the page was not filled to what was asked for")
	require.LessOrEqual(t, scanned, listBatchSize,
		"a page that filled on the first batch kept reading anyway")
}

// Walking the pages must reach every run, and that is not implied by any single
// page being correct.
//
// Temporal's page token addresses a whole batch. If a page fills partway through
// one and the cursor is advanced anyway, the rest of that batch sits behind a
// cursor that has moved past it: those runs belong to the caller, and they are
// gone from every later page rather than merely delayed. Nothing about the
// response says so — each page looks complete, the token looks healthy, and the
// listing simply ends up short.
//
// Which is why this walks to exhaustion and checks the set. Asserting that one
// page holds the right number of runs would pass with half the namespace missing.
func TestListPagingReachesEveryRun(t *testing.T) {
	t.Parallel()

	// More runs than one page holds, and deliberately not a multiple of the page
	// size, so the last page is partial too.
	const total, pageSize = 23, 5

	all := make([]*workflow.WorkflowExecutionInfo, 0, total)
	for i := range total {
		all = append(all, &workflow.WorkflowExecutionInfo{
			Execution: &common.WorkflowExecution{WorkflowId: fmt.Sprintf("run-%02d", i)},
		})
	}

	// A namespace that pages the way Temporal does: the token is an opaque
	// position, and a request returns the executions after it.
	temporal := &mocks.Client{}
	temporal.On("ListWorkflow", mock.Anything, mock.Anything).Return(
		func(_ context.Context, request *workflowservice.ListWorkflowExecutionsRequest) *workflowservice.ListWorkflowExecutionsResponse {
			offset := 0
			if token := request.GetNextPageToken(); len(token) > 0 {
				parsed, err := strconv.Atoi(string(token))
				require.NoError(t, err)
				offset = parsed
			}

			end := min(offset+int(request.GetPageSize()), len(all))

			resp := &workflowservice.ListWorkflowExecutionsResponse{Executions: all[offset:end]}
			if end < len(all) {
				resp.NextPageToken = []byte(strconv.Itoa(end))
			}

			return resp
		},
		nil,
	)

	server := New(temporal)

	seen := map[string]int{}
	token := ""
	pages := 0

	for {
		response, err := server.List(t.Context(), connect.NewRequest(&v1types.ListRequest{
			PageSize:  pageSize,
			PageToken: token,
		}))
		require.NoError(t, err)

		for _, run := range response.Msg.GetRuns() {
			seen[run.GetWorkflowId()]++
		}

		pages++
		require.Less(t, pages, 50, "the listing never terminated")

		token = response.Msg.GetNextPageToken()
		if token == "" {
			break
		}
	}

	require.Len(t, seen, total, "walking every page did not reach every run")
	for _, execution := range all {
		id := execution.GetExecution().GetWorkflowId()
		require.Equal(t, 1, seen[id], "run %q was skipped or returned twice", id)
	}
}

// A peer that answers with nothing, forever, must not spin the server.
//
// Both loop guards — the page filling and the scan budget — only advance when
// executions come back. Temporal's visibility store can legitimately return an
// empty page carrying a next-page token, so a listing that only bounds
// executions read does not bound the requests it makes: nothing terminates.
//
// The mock is finite so a regression fails rather than hangs, but the shape is
// the unbounded one.
func TestListStopsWhenAPeerReturnsNothingForever(t *testing.T) {
	t.Parallel()

	const patience = 10_000

	calls := 0
	temporal := &mocks.Client{}
	temporal.On("ListWorkflow", mock.Anything, mock.Anything).Return(
		func(_ context.Context, _ *workflowservice.ListWorkflowExecutionsRequest) *workflowservice.ListWorkflowExecutionsResponse {
			calls++
			if calls > patience {
				return &workflowservice.ListWorkflowExecutionsResponse{}
			}
			// Nothing to show, and always more to come.
			return &workflowservice.ListWorkflowExecutionsResponse{
				NextPageToken: []byte(strconv.Itoa(calls)),
			}
		},
		nil,
	)

	response, err := New(temporal).List(t.Context(), connect.NewRequest(&v1types.ListRequest{PageSize: 10}))
	require.NoError(t, err)

	require.Less(t, calls, patience,
		"the listing kept asking a peer that never returns anything; nothing bounds the request count")
	require.Empty(t, response.Msg.GetRuns())
	require.NotEmpty(t, response.Msg.GetNextPageToken(),
		"a listing that gave up early must say there is more")
}

// Paging and filtering have to be right *together*, which neither test above
// establishes.
//
// TestListPagingReachesEveryRun walks a namespace where everything is the
// caller's, so the filter never fires mid-batch. TestListReturnsOnlyTheCallersRuns
// filters, but reads a single page. The bug this file already carries a fix for
// lived exactly in the join of the two: a page filling partway through a batch.
// Filtering changes where in a batch that happens, so it is the case most able to
// put the cursor and the page out of step.
func TestListPagingReachesEveryRunAmongOtherTenants(t *testing.T) {
	t.Parallel()

	// Interleaved rather than grouped, so batches straddle the boundary between
	// whose runs are whose instead of aligning neatly with it.
	const total, pageSize = 60, 4

	all := make([]*workflow.WorkflowExecutionInfo, 0, total)
	mine := map[string]bool{}
	for i := range total {
		id := fmt.Sprintf("run-%02d", i)
		if i%3 == 0 {
			all = append(all, &workflow.WorkflowExecutionInfo{
				Execution: &common.WorkflowExecution{WorkflowId: id},
			})
			mine[id] = true
			continue
		}
		all = append(all, otherTenantsRun(t, id))
	}

	temporal := &mocks.Client{}
	temporal.On("ListWorkflow", mock.Anything, mock.Anything).Return(
		func(_ context.Context, request *workflowservice.ListWorkflowExecutionsRequest) *workflowservice.ListWorkflowExecutionsResponse {
			offset := 0
			if token := request.GetNextPageToken(); len(token) > 0 {
				parsed, err := strconv.Atoi(string(token))
				require.NoError(t, err)
				offset = parsed
			}

			end := min(offset+int(request.GetPageSize()), len(all))

			resp := &workflowservice.ListWorkflowExecutionsResponse{Executions: all[offset:end]}
			if end < len(all) {
				resp.NextPageToken = []byte(strconv.Itoa(end))
			}
			return resp
		},
		nil,
	)

	server := New(temporal)

	seen := map[string]int{}
	token := ""
	for pages := 0; ; pages++ {
		require.Less(t, pages, 100, "the listing never terminated")

		response, err := server.List(t.Context(), connect.NewRequest(&v1types.ListRequest{
			PageSize:  pageSize,
			PageToken: token,
		}))
		require.NoError(t, err)

		require.LessOrEqual(t, len(response.Msg.GetRuns()), pageSize,
			"a page came back larger than it was asked for")

		for _, run := range response.Msg.GetRuns() {
			seen[run.GetWorkflowId()]++
		}

		token = response.Msg.GetNextPageToken()
		if token == "" {
			break
		}
	}

	// Every one of the caller's runs, exactly once.
	require.Len(t, seen, len(mine), "walking every page did not reach every run the caller owns")
	for id := range mine {
		require.Equal(t, 1, seen[id], "run %q was skipped or returned twice", id)
	}

	// And none of anybody else's, which paging must not quietly reintroduce.
	for id := range seen {
		require.True(t, mine[id], "a listing returned another tenant's run %q", id)
	}
}

// An empty namespace ends the listing rather than reporting more to come.
func TestListOnAnEmptyNamespaceIsDone(t *testing.T) {
	t.Parallel()

	temporal := &mocks.Client{}
	temporal.On("ListWorkflow", mock.Anything, mock.Anything).Return(
		&workflowservice.ListWorkflowExecutionsResponse{}, nil)

	response, err := New(temporal).List(t.Context(), connect.NewRequest(&v1types.ListRequest{}))
	require.NoError(t, err)

	require.Empty(t, response.Msg.GetRuns())
	require.Empty(t, response.Msg.GetNextPageToken(),
		"an exhausted listing asked the caller to keep going")
}

// What a caller asks for bounds the page, and what they may ask for is bounded
// in turn — a page size is a request for work, so it is not simply trusted.
//
// The ceiling is enforced twice, and the two cover different ways in: the schema
// refuses an oversized ask outright, which is what any caller going through the
// RPC meets, and List clamps for itself so the bound still holds on a path that
// did not validate. Only the first is reachable from here — the second is asserted
// by construction rather than by a test that would only be re-checking `min`.
func TestListPageSizeIsDefaultedAndBounded(t *testing.T) {
	t.Parallel()

	// A namespace with more runs than any of these ask for, so the page size is
	// what decides the answer's length rather than the supply.
	newServer := func() *FlowstateServer {
		temporal := &mocks.Client{}
		temporal.On("ListWorkflow", mock.Anything, mock.Anything).Return(
			func(_ context.Context, request *workflowservice.ListWorkflowExecutionsRequest) *workflowservice.ListWorkflowExecutionsResponse {
				executions := make([]*workflow.WorkflowExecutionInfo, 0, request.GetPageSize())
				for range int(request.GetPageSize()) {
					executions = append(executions, &workflow.WorkflowExecutionInfo{
						Execution: &common.WorkflowExecution{WorkflowId: "mine"},
					})
				}
				return &workflowservice.ListWorkflowExecutionsResponse{
					Executions:    executions,
					NextPageToken: []byte("more"),
				}
			},
			nil,
		)
		return New(temporal)
	}

	t.Run("unset takes the default", func(t *testing.T) {
		t.Parallel()

		response, err := newServer().List(t.Context(), connect.NewRequest(&v1types.ListRequest{}))
		require.NoError(t, err)
		require.Len(t, response.Msg.GetRuns(), defaultListPageSize)
	})

	t.Run("a modest ask is honored", func(t *testing.T) {
		t.Parallel()

		response, err := newServer().List(t.Context(), connect.NewRequest(&v1types.ListRequest{PageSize: 3}))
		require.NoError(t, err)
		require.Len(t, response.Msg.GetRuns(), 3)
	})

	t.Run("an ask above the ceiling is refused, not quietly shrunk", func(t *testing.T) {
		t.Parallel()

		// Refused rather than clamped, because silently returning a thousand
		// when someone asked for ten thousand reads as "that is all there was".
		_, err := newServer().List(t.Context(), connect.NewRequest(&v1types.ListRequest{
			PageSize: maxListPageSize + 1,
		}))
		require.Error(t, err)
		require.Equal(t, connect.CodeInvalidArgument, connect.CodeOf(err))
	})

}
