package server

import (
	"context"
	"encoding/base64"
	"fmt"

	"connectrpc.com/connect"
	enums "go.temporal.io/api/enums/v1"
	"go.temporal.io/api/workflow/v1"
	"go.temporal.io/api/workflowservice/v1"

	v1 "github.com/picatz/flowstate/pkg/flowstate/v1"
)

// Listing a tenant's runs is a scan, and the scan is what has to be bounded.
//
// The tenant a run belongs to is recorded as a memo, decided when Run started it
// — a memo rather than a search attribute because a memo needs no registration in
// the Temporal cluster, and requiring an operator to register attributes before
// the engine works would break the promise that a first run needs nothing but
// `temporal server start-dev`. That choice is what holds the tenancy invariant
// with no setup, and it applies here unchanged.
//
// Its consequence is that Temporal cannot do the filtering: a memo is not
// queryable. So the server reads executions and keeps the ones that are the
// caller's, which means the number of runs examined is not the number returned.
// In a namespace holding several tenants, finding ten of yours can mean reading
// far more than ten — and left unbounded, a caller asking for a small page in a
// large shared namespace would make the server walk the whole namespace. The
// bound is therefore on executions *read*, not on runs returned.
//
// A page can consequently come back short, or empty, with runs still to find.
// That is reported rather than hidden: a next page token is set whenever the scan
// stopped early, and only an absent token means the listing is finished. The
// alternative — looping until the page is full — is the unbounded scan wearing a
// bounded page's clothing.
const (
	// defaultListPageSize is how many runs come back when a caller does not say.
	defaultListPageSize = 50

	// maxListPageSize bounds what a caller may ask for in one page.
	maxListPageSize = 1000

	// maxListScan bounds how many executions one request may read, whatever it
	// finds among them. This is the number that stops a shared namespace from
	// turning a small request into a large one.
	maxListScan = 1000

	// listBatchSize is how many executions are read per call to Temporal. Smaller
	// than maxListScan so a request that fills its page early stops early rather
	// than always paying for the whole budget.
	listBatchSize = 100

	// maxListRequests bounds how many times one listing may call Temporal.
	//
	// A second bound is needed because the first one does not cover this: both
	// the page and the scan budget only advance when executions come back, and
	// how many come back is the peer's choice, not ours. A visibility store may
	// answer with an empty page and a next-page token — Temporal's legitimately
	// does — and on a peer that answers that way every time, a listing bounded
	// only by executions read never terminates at all.
	//
	// Equal to maxListScan on purpose. A request that returns anything spends at
	// least one execution of the scan budget, so a peer making progress reaches
	// that limit first and never this one; the two can only come apart for a peer
	// that is returning nothing, which is exactly the case this exists for.
	maxListRequests = maxListScan
)

// List returns a page of the runs belonging to the caller's tenant.
func (s *FlowstateServer) List(ctx context.Context, req *connect.Request[v1.ListRequest]) (*connect.Response[v1.ListResponse], error) {
	if err := v1.Validate(req.Msg); err != nil {
		return nil, connect.NewError(connect.CodeInvalidArgument, err)
	}

	caller := s.identityFor(ctx).GetNamespace()

	// The caller's own namespace decides which Temporal namespace is listed at
	// all, exactly as it decides which runs are addressable. Where a deployment
	// maps namespaces, another tenant's runs are not merely filtered out here —
	// they were never in the listing.
	temporal, err := s.clientFor(caller)
	if err != nil {
		return nil, err
	}

	pageSize := int(req.Msg.GetPageSize())
	switch {
	case pageSize <= 0:
		pageSize = defaultListPageSize
	case pageSize > maxListPageSize:
		pageSize = maxListPageSize
	}

	// A page token is something a caller sends, so it is parsed rather than
	// trusted. It cannot widen what the caller sees regardless of its contents:
	// it is a position in a listing the namespace above already narrowed, and
	// every execution it reaches is still checked against the caller's tenant.
	cursor, err := decodePageToken(req.Msg.GetPageToken())
	if err != nil {
		return nil, connect.NewError(connect.CodeInvalidArgument, err)
	}

	runs := make([]*v1.RunSummary, 0, pageSize)
	scanned := 0
	requests := 0

	for len(runs) < pageSize && scanned < maxListScan && requests < maxListRequests {
		requests++

		// Never ask for more executions than the page has room for.
		//
		// This is what keeps the cursor honest. Temporal's page token addresses a
		// whole batch, so it can only be advanced once every execution in that
		// batch has been considered. Asking for a hundred and stopping after
		// filling the page on the fiftieth would leave fifty executions behind a
		// cursor that has already moved past them — runs the caller owns, gone
		// from every subsequent page, unreachable even by walking to the end.
		//
		// Bounding the request by the remaining capacity makes that unrepresentable
		// rather than merely avoided: a batch can then only fill the page on its
		// final execution, which is exactly when advancing past it is right.
		batch := min(listBatchSize, pageSize-len(runs), maxListScan-scanned)

		// Namespace is left unset so the SDK fills it from the client that was
		// selected above, keeping the listing in the namespace the caller resolved
		// to rather than one named here.
		resp, err := temporal.ListWorkflow(ctx, &workflowservice.ListWorkflowExecutionsRequest{
			PageSize:      int32(batch),
			NextPageToken: cursor,
		})
		if err != nil {
			return nil, connect.NewError(connect.CodeInternal, fmt.Errorf("listing runs: %w", err))
		}

		scanned += len(resp.GetExecutions())

		for _, execution := range resp.GetExecutions() {
			if !ownedBy(caller, execution.GetMemo()) {
				continue
			}
			runs = append(runs, summarize(execution))
			if len(runs) == pageSize {
				break
			}
		}

		cursor = resp.GetNextPageToken()
		if len(cursor) == 0 {
			// The namespace is exhausted, which is the one case where there is
			// genuinely nothing more to ask for.
			break
		}
	}

	return connect.NewResponse(&v1.ListResponse{
		Runs: runs,
		// Set whenever Temporal has more to give, including when this page came
		// back short because the scan budget ran out first. A caller that stops on
		// a short page would silently miss runs it owns.
		NextPageToken: encodePageToken(cursor),
	}), nil
}

// summarize reduces an execution to what a listing reports.
//
// Not its outputs. A listing says which workloads exist and what they are doing;
// reading what one produced is Get, one run at a time and one authorization
// decision at a time. A list that carried outputs would make "show me my runs"
// the cheapest way to read every workload's data at once.
func summarize(execution *workflow.WorkflowExecutionInfo) *v1.RunSummary {
	summary := &v1.RunSummary{
		WorkflowId: execution.GetExecution().GetWorkflowId(),
		RunId:      execution.GetExecution().GetRunId(),
		Status:     runStatus(execution.GetStatus()),
		StartTime:  execution.GetStartTime(),
	}

	// Left unset while a run is still going, rather than reported as the zero
	// time, so "has not finished" and "finished at the epoch" stay distinct.
	if execution.GetStatus() != enums.WORKFLOW_EXECUTION_STATUS_RUNNING {
		summary.CloseTime = execution.GetCloseTime()
	}

	return summary
}

// decodePageToken parses the opaque cursor a caller returns.
func decodePageToken(token string) ([]byte, error) {
	if token == "" {
		return nil, nil
	}

	cursor, err := base64.RawURLEncoding.DecodeString(token)
	if err != nil {
		return nil, fmt.Errorf("page token is not a token this server issued")
	}

	return cursor, nil
}

// encodePageToken renders a cursor for a caller to hand back.
func encodePageToken(cursor []byte) string {
	if len(cursor) == 0 {
		return ""
	}
	return base64.RawURLEncoding.EncodeToString(cursor)
}
