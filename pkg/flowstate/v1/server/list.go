package server

import (
	"context"
	"encoding/base64"
	"errors"
	"fmt"

	"connectrpc.com/connect"
	enums "go.temporal.io/api/enums/v1"
	"go.temporal.io/api/workflow/v1"
	"go.temporal.io/api/workflowservice/v1"

	v1 "github.com/picatz/flowstate/pkg/flowstate/v1"
	"google.golang.org/protobuf/types/known/timestamppb"
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

	// listQuery scopes a listing to workflows this engine started.
	//
	// The type name is the Go function Temporal registers, `engine.Run`.
	listQuery = `WorkflowType = 'Run'`

	// maxListRequests bounds how many times one listing may call Temporal.
	//
	// A second bound is needed because the first one does not cover this: both
	// the page and the scan budget only advance when executions come back, and
	// how many come back is the peer's choice, not ours. A visibility store may
	// answer with an empty page and a next-page token — Temporal's legitimately
	// does — and on a peer that answers that way every time, a listing bounded
	// only by executions read never terminates at all.
	//
	// It also bounds a second thing, which is why it is not simply maxListScan.
	// Each request asks for at most the page's remaining capacity, so a caller
	// asking for a *small* page reads few executions per round trip: page_size=1
	// against a namespace holding none of the caller's runs would otherwise spend
	// the whole scan budget one execution at a time, a thousand sequential calls
	// to Temporal for one request. Bounding round trips caps that at a hundred,
	// and the listing says there is more rather than pretending it finished — so
	// the work still gets done, across calls the caller asked for.
	maxListRequests = 100
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

	// Compiled once for the request, not once per run: compiling parses and
	// type-checks, which costs more than the whole listing if repeated per
	// execution. A malformed filter is the caller's mistake and is reported as
	// such — `flow list --filter` compiles it before sending for the same reason,
	// so this is the backstop for a caller that is not the CLI.
	filter, err := v1.NewRunFilter(req.Msg.GetFilter())
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

			// Scoped to this engine's own workflows.
			//
			// A Temporal namespace is not necessarily Flowstate's alone, and the
			// tenant check cannot tell "a Flowstate run from before tenants were
			// recorded" from "not a Flowstate run at all" — both arrive with no
			// memo, and both therefore read as belonging to the default tenant.
			// Unscoped, a listing would enumerate whatever else shares the
			// namespace, and every id it returned would then be a live argument to
			// `flow cancel` and `flow terminate`.
			//
			// WorkflowType is one of Temporal's own default search attributes, so
			// this needs no registration and keeps the promise that a first run
			// wants nothing but `temporal server start-dev`. It also stops another
			// application's executions from spending this listing's scan budget.
			Query: listQuery,
		})
		if err != nil {
			// A page token comes from the caller, and Temporal reports one it
			// cannot deserialize as an ordinary error. Reported as InvalidArgument
			// rather than Internal, because a caller's malformed input is not a
			// server fault — and relaying the message would hand back Temporal's
			// own text, which names namespaces this deployment does not otherwise
			// disclose.
			if len(cursor) > 0 {
				return nil, connect.NewError(connect.CodeInvalidArgument,
					errors.New("page token is not a token this server issued"))
			}
			return nil, connect.NewError(connect.CodeInternal, fmt.Errorf("listing runs: %w", err))
		}

		scanned += len(resp.GetExecutions())

		for _, execution := range resp.GetExecutions() {
			if !s.ownedBy(caller, execution.GetMemo()) {
				continue
			}

			// A workload that continued as new is several executions sharing one
			// workflow id, and a listing is about workloads. Left in, a long
			// workload would appear once per segment — the same id repeated, most
			// of them closed — and the more work it had done the more of the page
			// it would occupy. The current segment carries the workload's real
			// status, so the earlier ones are skipped rather than deduplicated
			// afterwards, which would need the whole listing in hand to do.
			if execution.GetStatus() == enums.WORKFLOW_EXECUTION_STATUS_CONTINUED_AS_NEW {
				continue
			}

			run := s.summarize(execution)

			// Applied after the tenant check and after the continued-as-new skip,
			// which is the only correct order. A filter is the caller's question
			// about their own runs; running it first would evaluate an expression
			// against executions the caller is not entitled to see, and would let a
			// filter that errors on another tenant's data fail this caller's
			// listing.
			//
			// An error stops the listing rather than skipping the run. Nearly every
			// error a filter can raise is a property of the expression rather than
			// of the run — an unguarded `close_time` comparison errors on exactly
			// the runs still going — so skipping would answer "nothing matched" to a
			// question that was never asked correctly.
			matched, err := filter.Match(ctx, run)
			if err != nil {
				return nil, connect.NewError(connect.CodeInvalidArgument, err)
			}
			if !matched {
				continue
			}

			runs = append(runs, run)
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
func (s *FlowstateServer) summarize(execution *workflow.WorkflowExecutionInfo) *v1.RunSummary {
	start, close := runTimes(execution)

	return &v1.RunSummary{
		WorkflowId: execution.GetExecution().GetWorkflowId(),
		RunId:      execution.GetExecution().GetRunId(),
		Status:     runStatus(execution.GetStatus()),
		StartTime:  start,
		CloseTime:  close,
		Name:       s.workflowNameOf(execution),
	}
}

// workflowNameOf reads the workflow's own declared name off a run's memo,
// and reports the empty string when there is none to read.
//
// The memo, not the search attribute — deliberately, and this is the fix for
// a real bug the search-attribute-only version had. `flow list --filter`
// composes with the tenant check unconditionally, on every deployment
// whether or not [EnsureSearchAttributesRegistered] ever succeeded; a
// `name` comparison sourced from the search attribute would silently match
// nothing on a deployment where registration failed or was never attempted
// — a filter with nothing wrong with it, indistinguishable from one with a
// typo. [workflowNameMemoKey] has no such dependency: it is written by
// [workflowNameMemoEntry] on every run, exactly as [namespaceMemoKey] is,
// so a listing already paying for the memo it reads for tenancy reads this
// at the same cost. The search attribute this deployment may additionally
// carry is index-only, for tools that query Temporal's visibility store
// directly — this server never reads it back.
//
// Absence is still not an error, and still covers a real case: a run
// started before this memo key existed. That run predates the feature
// entirely, so "no name available" is the honest answer, and it is what
// [RunFilter]'s `name` comparison sees — a bare `name == "..."` never
// matches such a run, and `name == ""` does.
//
// Decoded with this server's own data converter, which is the one the client
// that wrote the memo encodes with, matching the encoding side in
// [workflowNameMemoEntry] exactly. A second decoder that guessed at the
// payload's encoding would be the shared-encoder lesson violated in the other
// direction, and naming the SDK default here rather than the configured
// converter would be the same mistake with a payload codec configured. See
// [WithDataConverter].
func (s *FlowstateServer) workflowNameOf(execution *workflow.WorkflowExecutionInfo) string {
	payload, ok := execution.GetMemo().GetFields()[workflowNameMemoKey]
	if !ok {
		return ""
	}

	var name string
	if err := s.dataConverter.FromPayload(payload, &name); err != nil {
		// A payload under this key that does not decode as a string is not this
		// deployment's doing — see [workflowNameOf]'s own doc — so the honest
		// answer is "no name available", the same as when the key is absent,
		// rather than failing a listing over a memo field this run's writer
		// used for something else entirely.
		return ""
	}

	return name
}

// runTimes returns when a run began and when it finished.
//
// Split out of [summarize] so a listing and a Get answer it the same way, which is
// the rule runStatus is written to for the same reason: two mappings of one Temporal
// response eventually disagree, and a run reported as started at one time by `flow
// list` and another by `flow get` is a bug nobody can reproduce.
//
// The close time is left unset while a run is still going, rather than reported as
// the zero time, so "has not finished" and "finished at the epoch" stay distinct.
func runTimes(execution *workflow.WorkflowExecutionInfo) (start, close *timestamppb.Timestamp) {
	if execution.GetStatus() == enums.WORKFLOW_EXECUTION_STATUS_RUNNING {
		return execution.GetStartTime(), nil
	}

	return execution.GetStartTime(), execution.GetCloseTime()
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
