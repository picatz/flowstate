package server

import (
	"context"
	"fmt"
	"strconv"
	"testing"

	"buf.build/gen/go/bufbuild/protovalidate/protocolbuffers/go/buf/validate"
	"connectrpc.com/connect"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	common "go.temporal.io/api/common/v1"
	enums "go.temporal.io/api/enums/v1"
	"go.temporal.io/api/workflow/v1"
	"go.temporal.io/api/workflowservice/v1"
	"go.temporal.io/sdk/converter"
	"go.temporal.io/sdk/mocks"
	"google.golang.org/protobuf/proto"

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

	// Exactly the page's capacity, not merely "no more than a batch". Asserting
	// against listBatchSize (100) is satisfied by reading a hundred to return
	// five — which is precisely the mid-batch bug this file exists to prevent, so
	// the looser bound could not fail on the one thing it was written for.
	require.Equal(t, 5, scanned,
		"the listing read more executions than the page had room for, which is how a "+
			"cursor ends up advanced past runs that were never returned")
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

	require.LessOrEqual(t, calls, maxListRequests,
		"the listing kept asking a peer that never returns anything; the request count is not "+
			"bounded by maxListRequests")
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

// The page-size ceiling is written twice — once in the schema, once here — and
// the two must not drift.
//
// Which one wins decides the behavior a caller sees, and the failure is silent
// either way. If the schema's ceiling rose above this one, an ask the schema
// accepts would be quietly shrunk here, and a caller who asked for five thousand
// and received a thousand has no way to tell that from a namespace holding a
// thousand runs — the exact "that is all there was" reading the refusal exists to
// prevent. If it fell below, this clamp would be unreachable and the second line
// of defence would be gone without anything failing.
//
// So the constraint is read from the descriptor rather than restated, and a
// change to either side has to be a change to both.
func TestPageSizeCeilingMatchesTheSchema(t *testing.T) {
	t.Parallel()

	field := (&v1types.ListRequest{}).ProtoReflect().Descriptor().Fields().ByName("page_size")
	require.NotNil(t, field, "page_size is gone from the schema")

	// NotNil rather than the type assertion's ok: GetExtension returns a typed-nil
	// pointer when the field carries no rules at all, so `ok` is true even then
	// and asserting on it could never fail.
	rules, _ := proto.GetExtension(field.Options(), validate.E_Field).(*validate.FieldRules)
	require.NotNil(t, rules, "page_size carries no validation rules")

	require.Equal(t, int32(maxListPageSize), rules.GetInt32().GetLte(),
		"the schema's page-size ceiling and maxListPageSize disagree, so an ask the schema "+
			"accepts would be silently shrunk here (or this clamp is unreachable)")
}

// A workload that continued as new is several executions sharing one workflow id,
// and this engine reaches that state by design: a run that exhausts its step
// budget continues as new.
//
// Listing them all would show one workload once per segment, most of them closed,
// and the more work it had done the more of the page it would take. Worse, the
// status Temporal records on a prior segment has no mapping of its own, so before
// this each of those rows read UNSPECIFIED.
func TestListShowsAContinuedWorkloadOnce(t *testing.T) {
	t.Parallel()

	// One workload: two segments it has already left, and the one it is in.
	segments := []*workflow.WorkflowExecutionInfo{
		{
			Execution: &common.WorkflowExecution{WorkflowId: "long-runner", RunId: "run-1"},
			Status:    enums.WORKFLOW_EXECUTION_STATUS_CONTINUED_AS_NEW,
		},
		{
			Execution: &common.WorkflowExecution{WorkflowId: "long-runner", RunId: "run-2"},
			Status:    enums.WORKFLOW_EXECUTION_STATUS_CONTINUED_AS_NEW,
		},
		{
			Execution: &common.WorkflowExecution{WorkflowId: "long-runner", RunId: "run-3"},
			Status:    enums.WORKFLOW_EXECUTION_STATUS_RUNNING,
		},
	}

	temporal := &mocks.Client{}
	temporal.On("ListWorkflow", mock.Anything, mock.Anything).Return(
		&workflowservice.ListWorkflowExecutionsResponse{Executions: segments}, nil)

	response, err := New(temporal).List(t.Context(), connect.NewRequest(&v1types.ListRequest{}))
	require.NoError(t, err)

	require.Len(t, response.Msg.GetRuns(), 1,
		"a workload that continued as new was listed once per segment")
	require.Equal(t, "long-runner", response.Msg.GetRuns()[0].GetWorkflowId())
	require.Equal(t, "run-3", response.Msg.GetRuns()[0].GetRunId(),
		"the listing named a segment the workload has already left")
	require.Equal(t, v1types.RunResponse_STATUS_RUNNING, response.Msg.GetRuns()[0].GetStatus())
}

// And asked about directly, a segment reports the workload's state rather than
// falling through to UNSPECIFIED — which Get rejects as an unknown status, so
// asking about an earlier segment by run id used to return an internal error.
func TestContinuedAsNewReadsAsRunning(t *testing.T) {
	t.Parallel()

	require.Equal(t, v1types.RunResponse_STATUS_RUNNING,
		runStatus(enums.WORKFLOW_EXECUTION_STATUS_CONTINUED_AS_NEW),
		"a continued-as-new segment has no status of its own")
}

// TestListPagingSurvivesANamespaceThatGrowsUnderIt pins the property that makes this
// cursor safe, and that a faster one would give away.
//
// # The optimisation this refuses
//
// Every request asks Temporal for at most the page's remaining capacity, so a caller
// using a small page size makes a round trip per few executions: `page_size=1` against
// a namespace full of somebody else's runs walks it one execution at a time. The
// obvious fix is a composite page token — Temporal's own cursor plus a count of how far
// into the batch it returned we had got — so the server could always ask for a full
// batch and remember where it stopped inside one.
//
// It cannot be done, and the reason is what this test holds down. Temporal's page token
// is a *key*: it addresses a position in a sort order, so a run appearing after it was
// issued does not move it. A count is not a key. Resuming "the batch that token C
// returned, skipping the first k" assumes that re-asking C returns the same batch — and
// a visibility store is ordered newest-first, so anything started in between arrives at
// the *front*. The skip of k then lands k runs too late, and the ones it steps over are
// runs the caller owns, gone from every later page.
//
// So a batch is either consumed entirely or not advanced past, which is why the request
// is bounded by the page's remaining room. The cost is round trips; the thing bought is
// that no run can be skipped.
//
// # What this models
//
// A namespace that behaves the way Temporal's does — a keyed cursor over a
// newest-first order — and that *grows while the walk is in progress*. Every run
// present when the walk started must be reached exactly once. The new arrivals may or
// may not appear, which is the honest guarantee for a listing of a live system and is
// deliberately not asserted.
func TestListPagingSurvivesANamespaceThatGrowsUnderIt(t *testing.T) {
	t.Parallel()

	// A small page against a much larger namespace, so the walk takes many pages and
	// there are many boundaries for a run to fall through.
	const existing, pageSize = 23, 3

	// Newest first, which is the order a visibility store answers in. Keys descend, so
	// a newly started run gets a key above every existing one and lands at the front.
	ordered := make([]*workflow.WorkflowExecutionInfo, 0, existing)
	for i := existing - 1; i >= 0; i-- {
		ordered = append(ordered, &workflow.WorkflowExecutionInfo{
			Execution: &common.WorkflowExecution{WorkflowId: fmt.Sprintf("run-%02d", i)},
		})
	}

	// The cursor is the id to resume *after*, which is what makes it a key rather than
	// an offset: inserting at the front does not move it.
	requests := 0
	temporal := &mocks.Client{}
	temporal.On("ListWorkflow", mock.Anything, mock.Anything).Return(
		func(_ context.Context, request *workflowservice.ListWorkflowExecutionsRequest) *workflowservice.ListWorkflowExecutionsResponse {
			from := 0
			if token := request.GetNextPageToken(); len(token) > 0 {
				for i, execution := range ordered {
					if execution.GetExecution().GetWorkflowId() == string(token) {
						from = i + 1

						break
					}
				}
			}

			end := min(from+int(request.GetPageSize()), len(ordered))
			page := ordered[from:end]

			resp := &workflowservice.ListWorkflowExecutionsResponse{Executions: page}
			if end < len(ordered) && len(page) > 0 {
				resp.NextPageToken = []byte(page[len(page)-1].GetExecution().GetWorkflowId())
			}

			// A run starts between requests, at the front, exactly where an
			// offset-based cursor would be shifted by it.
			requests++
			if requests%2 == 0 {
				ordered = append([]*workflow.WorkflowExecutionInfo{{
					Execution: &common.WorkflowExecution{
						WorkflowId: fmt.Sprintf("arrived-%02d", requests),
					},
				}}, ordered...)
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
		require.Less(t, pages, 60, "the listing never terminated")

		token = response.Msg.GetNextPageToken()
		if token == "" {
			break
		}
	}

	for i := range existing {
		id := fmt.Sprintf("run-%02d", i)
		require.Equal(t, 1, seen[id],
			"run %q was skipped or returned twice while the namespace grew under the walk", id)
	}
}

// TestListPagingReachesEveryMatchingRun is the join, which is where the last
// paging bug lived and where this one would live too.
//
// Paging was tested. Filtering is tested. The defect this guards against sits
// exactly between them: a filter decides *where in a batch a page fills*, so a
// cursor advanced past the whole batch when the page filled early leaves the
// unexamined remainder behind a position that has already moved past it. Runs the
// caller owns and the filter matches, absent from every later page rather than
// delayed — and the listing reporting itself complete rather than short.
//
// So this walks to exhaustion and checks the *set*: every matching run reached,
// exactly once, and nothing that does not match. A page-shaped assertion cannot
// see a cursor that skips, which is the whole lesson from last time.
//
// The filter deliberately keeps a minority — one run in three — because that is
// what makes pages fill from the middle of a batch rather than at its edge.
func TestListPagingReachesEveryMatchingRun(t *testing.T) {
	t.Parallel()

	const total, pageSize = 23, 5

	all := make([]*workflow.WorkflowExecutionInfo, 0, total)
	wanted := map[string]bool{}
	for i := range total {
		id := fmt.Sprintf("run-%02d", i)

		// Every third run failed; the rest completed.
		status := enums.WORKFLOW_EXECUTION_STATUS_COMPLETED
		if i%3 == 0 {
			status = enums.WORKFLOW_EXECUTION_STATUS_FAILED
			wanted[id] = true
		}

		all = append(all, &workflow.WorkflowExecutionInfo{
			Execution: &common.WorkflowExecution{WorkflowId: id},
			Status:    status,
		})
	}

	require.NotEmpty(t, wanted)
	require.Less(t, len(wanted), total, "the filter must exclude some runs or this proves nothing")

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
			Filter:    `status == "FAILED"`,
		}))
		require.NoError(t, err)

		for _, run := range response.Msg.GetRuns() {
			seen[run.GetWorkflowId()]++
		}

		pages++
		require.Less(t, pages, 50, "the filtered listing never terminated")

		token = response.Msg.GetNextPageToken()
		if token == "" {
			break
		}
	}

	require.Len(t, seen, len(wanted),
		"walking every page did not reach every matching run: a filter that empties part "+
			"of a batch is exactly what moves a cursor past runs nobody looked at")
	for id := range wanted {
		require.Equal(t, 1, seen[id], "matching run %q was skipped or returned twice", id)
	}
	for id := range seen {
		require.True(t, wanted[id], "run %q came back and does not match the filter", id)
	}
}

// TestAFilterTheServerCannotCompileIsRefused is the backstop for a caller that is
// not the CLI.
//
// `flow list --filter` compiles the expression before it makes a request, which is
// where an author meets the mistake. That is an ergonomic, not a boundary: the RPC
// is public and a caller can send anything, so the server compiles it too and
// refuses with InvalidArgument rather than treating an uncompilable filter as one
// that matches nothing.
func TestAFilterTheServerCannotCompileIsRefused(t *testing.T) {
	t.Parallel()

	temporal := &mocks.Client{}
	server := New(temporal)

	for _, filter := range []string{
		`status ==`,          // not parseable
		`stauts == "FAILED"`, // not a name a run has
		`status == "FAILD"`,  // not a status a run has
		`workflow_id`,        // not a condition
	} {
		_, err := server.List(t.Context(), connect.NewRequest(&v1types.ListRequest{Filter: filter}))
		require.Error(t, err, "a filter the server cannot compile was accepted: %s", filter)
		require.Equal(t, connect.CodeInvalidArgument, connect.CodeOf(err),
			"a caller's malformed filter was reported as a server fault: %s", filter)
	}

	// And nothing was asked of Temporal, because a request that cannot be answered
	// should not cost a round trip.
	temporal.AssertNotCalled(t, "ListWorkflow", mock.Anything, mock.Anything)
}
