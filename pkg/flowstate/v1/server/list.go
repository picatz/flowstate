package server

import (
	"context"
	"crypto/hmac"
	"crypto/rand"
	"crypto/sha256"
	"encoding/base64"
	"encoding/binary"
	"errors"
	"fmt"
	"time"

	"connectrpc.com/connect"
	enums "go.temporal.io/api/enums/v1"
	"go.temporal.io/api/workflow/v1"
	"go.temporal.io/api/workflowservice/v1"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"

	v1 "github.com/picatz/flowstate/pkg/flowstate/v1"
	"github.com/picatz/flowstate/pkg/flowstate/v1/engine"
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

	// flowstateRunWorkflowType is the Temporal workflow type this engine owns.
	//
	// The name is the Go function Temporal registers, `engine.Run`. Listing and
	// direct addressing both read this value so neither can expose an execution
	// the other rejects.
	flowstateRunWorkflowType = "Run"

	// listQuery scopes a listing to workflows this engine started.
	listQuery = `WorkflowType = '` + flowstateRunWorkflowType + `'`

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

	// listTokenLifetime is how long a page token stays usable after it was
	// issued.
	//
	// A day is plenty for a listing, and the bound does two things a signature
	// alone does not. A stored cursor cannot resume a listing whose visibility
	// has since changed — runs past their retention are gone, and a position
	// from before that is not one the caller would get by starting over. And
	// a token that leaks is usable for a day rather than for the life of the
	// process: it names a position in one tenant's listing, which is worth
	// little, but it is worth little for a bounded time rather than an
	// unbounded one.
	//
	// Not a bound across tenant remaps: the key is per process and the
	// namespace mapping is fixed at construction, so a remap restarts the
	// process and the new key refuses every earlier token on its own.
	listTokenLifetime = 24 * time.Hour

	// listTokenKeySize is the HMAC-SHA256 key length, which is also the length
	// of the authentication code a token carries after its cursor.
	listTokenKeySize = sha256.Size

	// maxListPositionBytes bounds the visibility store's own page token, which
	// is the one part of a cursor whose size another party chooses.
	//
	// ListRequest.page_token is bounded at 4096 characters by the schema, and
	// a token this server issues has to come back through that bound: a token
	// too long to validate is a listing the caller can start but never
	// continue, refused on the second page by the server's own validator. The
	// SDK documents no bound on the position, so one is set here, and
	// [issuePageToken] refuses to issue rather than issue something oversize.
	// The rest of a cursor is bounded already — a namespace at
	// [auth.MaxNamespaceLen], a digest at [sha256.Size], a timestamp — so with
	// this the whole token is, and TestAWorstCaseTokenFitsTheSchema pins that
	// the sum fits the schema's limit rather than assuming it.
	//
	// Two kilobytes is more than an order of magnitude above what either of
	// Temporal's visibility stores emits (a SQL store's token is three fields
	// of JSON, ~140 bytes; the dev server's measured under 200), which leaves
	// room for a store this server has not met without leaving the bound
	// decorative.
	maxListPositionBytes = 2048
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
		return nil, s.auditDeny(ctx, "List", v1.AuditResourceKind_AUDIT_RESOURCE_KIND_NAMESPACE, caller,
			v1.AuditDenyCode_AUDIT_DENY_CODE_NAMESPACE_UNROUTABLE, err)
	}

	// A listing addresses a tenant rather than a run: what is decided here is
	// that this caller may read their own namespace, which is the decision
	// every run the listing goes on to filter is already inside.
	if err := s.auditAllow(ctx, "List", v1.AuditResourceKind_AUDIT_RESOURCE_KIND_NAMESPACE, caller); err != nil {
		return nil, err
	}

	pageSize := int(req.Msg.GetPageSize())
	switch {
	case pageSize <= 0:
		pageSize = defaultListPageSize
	case pageSize > maxListPageSize:
		pageSize = maxListPageSize
	}

	// The token binds the query as well as the position, so a cursor issued
	// for one question is refused for another rather than quietly naming a
	// page that need not exist under it. The effective page size is what is
	// digested, so a caller who let the default apply and one who spelled it
	// out are asking the same question and may exchange tokens.
	query := listQueryDigest(req.Msg.GetFilter(), pageSize)

	// A page token is something a caller sends, so it is authenticated rather
	// than trusted: only a token this process issued, to this tenant, for this
	// query, and recently, is a position at all. Even one that passes cannot
	// widen what the caller sees — it is a position in a listing the namespace
	// above already narrowed, and every execution it reaches is still checked
	// against the caller's tenant.
	cursor, err := s.openPageToken(req.Msg.GetPageToken(), caller, query, time.Now())
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

	// The filter's account of this page: how many of the caller's runs it was
	// asked about, how many it could not answer for, and the first reason.
	var (
		evaluated, excluded int
		firstErr            error
	)

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
			// A position Temporal refuses is still the caller's to start over
			// from, and the sentence is still ours. The authentication code
			// proves this server issued the token; it does not prove Temporal
			// still accepts the position inside it — a visibility store swapped
			// under a running server, within the day a token lives, leaves a
			// signed position that names nothing. Reported as InvalidArgument
			// with a sentence of our own rather than by relaying Temporal's,
			// which can name namespaces this deployment does not otherwise
			// disclose. Only a first page, which handed Temporal no position at
			// all, is an error that cannot be the token's.
			if len(cursor) > 0 {
				return nil, connect.NewError(connect.CodeInvalidArgument,
					errors.New("the page token names a position this listing no longer has; start the listing again"))
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
			// An error is that run not matching, never a failed request. This used
			// to stop the listing, on the argument that an error is a property of
			// the expression rather than of the run — true of a type error, which
			// compilation refuses above, and false of a map index: `labels["team"]
			// == "x"` is a correct expression that errors on exactly the runs the
			// caller wants excluded, and failing the whole listing over the first
			// unlabelled run answered a correct question with an error (#1689).
			// Excluding is the fail-closed reading — a run the filter cannot answer
			// for is not one it said yes about — and the count says how many were
			// left out. A filter wrong about *every* run it met is still told so,
			// once, through the diagnostic below.
			evaluated++
			matched, err := filter.Match(ctx, run)
			if err != nil {
				// A request that was cancelled or timed out is not a run the
				// filter could not answer for: the evaluation was interrupted,
				// and a page reporting that as runs left out would hide the
				// client's own deadline behind a successful answer.
				if ctx.Err() != nil {
					return nil, connect.NewError(contextCode(ctx.Err()), ctx.Err())
				}
				excluded++
				if firstErr == nil {
					firstErr = err
				}
				continue
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

	// Set whenever Temporal has more to give, including when this page came
	// back short because the scan budget ran out first. A caller that stops on
	// a short page would silently miss runs it owns.
	next, err := s.issuePageToken(cursor, caller, query, time.Now())
	if err != nil {
		return nil, connect.NewError(connect.CodeInternal, fmt.Errorf("issuing the next page token: %w", err))
	}

	response := &v1.ListResponse{
		Runs:            runs,
		NextPageToken:   next,
		ExcludedByError: uint32(excluded),
	}
	// Said once, and only when the filter answered for none of the runs it
	// met: that is what a typo looks like, where a filter wrong about some runs
	// is the ordinary case the exclusion count already reports.
	if excluded > 0 && excluded == evaluated {
		response.FilterDiagnostic = filter.Diagnostic(firstErr)
	}

	return connect.NewResponse(response), nil
}

// contextCode is the Connect code for a request that ended before the server
// did: the caller's deadline, or the caller going away.
func contextCode(err error) connect.Code {
	if errors.Is(err, context.DeadlineExceeded) {
		return connect.CodeDeadlineExceeded
	}
	return connect.CodeCanceled
}

// summarize reduces an execution to what a listing reports.
//
// Not its outputs. A listing says which workloads exist and what they are doing;
// reading what one produced is Get, one run at a time and one authorization
// decision at a time. A list that carried outputs would make "show me my runs"
// the cheapest way to read every workload's data at once.
func (s *FlowstateServer) summarize(execution *workflow.WorkflowExecutionInfo) *v1.RunSummary {
	start, close := runTimes(execution)
	chain := s.chainOf(execution, start)

	return &v1.RunSummary{
		WorkflowId:       execution.GetExecution().GetWorkflowId(),
		RunId:            execution.GetExecution().GetRunId(),
		Status:           runStatus(execution.GetStatus()),
		StartTime:        chain.started,
		SegmentStartTime: start,
		Segments:         chain.segments,
		CloseTime:        close,
		Name:             s.workflowNameOf(execution),

		// All three read off what the listing already has in hand: the memo it
		// fetched for the tenant check, and the versioning info Temporal returns
		// with every execution. No second call per run — which is the property
		// that makes selecting on them at ten thousand runs cost the listing and
		// nothing more.
		Labels:        s.labelsOf(execution),
		Starter:       s.starterOf(execution),
		WorkerVersion: workerVersionOf(execution),
	}
}

// runChain is what a run's memo says about the Continue-As-New chain it
// belongs to: when the workload began, and how many segments it has run as.
type runChain struct {
	// started is the workload's start — the memo's, when a continued segment
	// wrote one, and otherwise the execution's own, which is the workload's
	// for a run that never continued and the best a reader can do for one
	// whose first segment predates the memo.
	started *timestamppb.Timestamp

	// segments is the memo's count, and zero when it wrote none: one segment,
	// or an older chain that cannot say.
	segments uint32
}

// chainOf reads the chain a continued segment recorded in its memo
// ([engine.WorkloadStartedMemoKey], [engine.SegmentsMemoKey]) off what the
// listing already has in hand, so a workload that continued as new is
// reported from where it began (#1690).
//
// The two fields are written together and are read together: a memo that
// carries one without the other, or one that will not decode, reads as no
// chain — the segment's own start and a count of zero — rather than as half
// of one, so a caller never sees a count beside a start it does not belong
// to. Never a failed listing, for [labelsOf]'s reason.
func (s *FlowstateServer) chainOf(execution *workflow.WorkflowExecutionInfo, segmentStart *timestamppb.Timestamp) runChain {
	fields := execution.GetMemo().GetFields()

	countPayload, ok := fields[engine.SegmentsMemoKey]
	if !ok {
		return runChain{started: segmentStart}
	}
	startPayload, ok := fields[engine.WorkloadStartedMemoKey]
	if !ok {
		return runChain{started: segmentStart}
	}

	var segments uint32
	if err := s.dataConverter.FromPayload(countPayload, &segments); err != nil {
		return runChain{started: segmentStart}
	}
	var started string
	if err := s.dataConverter.FromPayload(startPayload, &started); err != nil {
		return runChain{started: segmentStart}
	}
	at, err := time.Parse(time.RFC3339Nano, started)
	if err != nil {
		return runChain{started: segmentStart}
	}

	return runChain{started: timestamppb.New(at), segments: segments}
}

// labelsOf reads the workflow's declared labels off a run's memo, and reports
// nothing when there are none to read.
//
// [workflowNameOf]'s reasoning throughout, including what absence means: a run
// of a workflow that declared no labels and a run started before
// [labelsMemoKey] existed both read as carrying none, which is the same answer
// to the only question a `labels` filter asks. A payload under this key that
// does not decode as a string map is not this deployment's doing either, and
// gets that same answer rather than failing a listing.
//
// Read through the server's configured data converter, matching
// [labelsMemoEntry]'s encoding side exactly — see [workflowNameOf] for why
// naming the SDK default here instead would break precisely the deployments
// that configure a payload codec.
func (s *FlowstateServer) labelsOf(execution *workflow.WorkflowExecutionInfo) map[string]string {
	payload, ok := execution.GetMemo().GetFields()[labelsMemoKey]
	if !ok {
		return nil
	}

	var labels map[string]string
	if err := s.dataConverter.FromPayload(payload, &labels); err != nil {
		return nil
	}

	return labels
}

// starterOf reads who started a run off its memo, for a listing.
//
// Answers exactly what [FlowstateServer.reportedStarter] answers a Get with, by
// calling the same [FlowstateServer.memoStarter] and applying the same two
// rules — unreadable or absent is empty, and the qualified form of two empty
// strings (an unauthenticated submission, only possible in development) is
// empty as well, because handing a reader a bare separator invites a comparison
// that can only ever be wrong. Two readers of one memo field are what would
// eventually disagree; this is the second call site of the one reader, not a
// second reader.
func (s *FlowstateServer) starterOf(execution *workflow.WorkflowExecutionInfo) string {
	starter, ok, err := s.memoStarter(execution.GetMemo())
	if err != nil || !ok {
		return ""
	}

	if starter == v1.QualifiedSubject("", "") {
		return ""
	}

	return starter
}

// workerVersionOf reports the Worker Deployment version a run is pinned to, as
// `deployment-name.build-id`, and the empty string when it is pinned to none.
//
// Not a memo, unlike everything else a summary carries, and [v1.RunSummary]'s
// own field doc says why: this is a fact about where the run is executing rather
// than about its submission, and it legitimately changes at Continue-As-New.
// Temporal already answers it on every execution a listing reads, so reading it
// here costs nothing.
//
// Prefers the structured `deployment_version` and falls back to the deprecated
// `version` string, which is the same `name.build-id` spelling — a server too
// old to send the structured form still sends that one, and dropping to empty
// there would report "not versioned" about a run that is.
func workerVersionOf(execution *workflow.WorkflowExecutionInfo) string {
	versioning := execution.GetVersioningInfo()

	if v := versioning.GetDeploymentVersion(); v != nil {
		if v.GetDeploymentName() == "" || v.GetBuildId() == "" {
			// Half a version names nothing that can be selected on — the same
			// judgement [engine.DeploymentOptions] makes on the writing side,
			// where a worker configured with half of one is refused rather than
			// dropped quietly to unversioned.
			return ""
		}

		return v.GetDeploymentName() + "." + v.GetBuildId()
	}

	// The deprecated field, read on purpose: it is what a server too old to send
	// the structured form above sends instead, in the identical
	// `name.build-id` spelling, and dropping it would report a run that *is*
	// pinned as pinned to nothing. Deprecated is not absent — the day Temporal
	// removes the field this stops compiling, which is the right moment to
	// decide the fallback is no longer worth carrying.
	//lint:ignore SA1019 the deprecated form is the fallback for a server that sends no structured version
	return versioning.GetVersion()
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

// A page token is opaque, and the opacity is enforced rather than requested.
//
// What a caller hands back is a [v1.ListCursor] serialized, followed by an
// HMAC-SHA256 over those bytes, base64url-encoded. The cursor names where the
// scan stopped — Temporal's own page token, carried intact — together with the
// tenant it was issued to, a digest of the query it was issued for, and when.
// The code at the end is what makes the rest trustworthy: a token that does not
// carry one this server produced is refused as not a token this server issued,
// which is now a sentence the server can stand behind. Before the code was
// there, the same sentence was said of anything that failed to parse, while
// anything that did parse was accepted whatever it named.
//
// What a forged cursor could buy was always limited, because the namespace a
// listing reads is decided by the authenticated caller rather than by anything
// in the token, and Temporal's position is an ordering key rather than an
// authority. What it cost was that the cursor's contract was open: a client
// could build one, so its layout was something a client could come to depend
// on, and a cursor issued for one filter was accepted under another, where the
// page it named need not exist. Signing closes both.
//
// # Refusals
//
// Each check has its own sentence, because they are the caller's different
// mistakes: a token from another process or a hand-built one, a token from
// another tenant, a token from another query, a token kept too long. All are
// InvalidArgument, and all fail closed — a token that cannot be authenticated
// is not a position, whatever it claims to be.

// newListTokenKey derives the key one server process signs page tokens with.
//
// From the system's random source, at startup, and shared with nothing. That
// makes a token valid for exactly one process: a replica behind the same
// address does not hold this key, so a token issued by one is refused by the
// other as not a token it issued, and a caller paging across a load balancer
// starts over. Sharing the key — through configuration, or by deriving it from
// the payload codec's key so that replicas that already agree on one agree on
// this — is the same question multi-replica MCP sessions raise, and is settled
// in #1654 rather than separately here.
func newListTokenKey() ([]byte, error) {
	key := make([]byte, listTokenKeySize)
	if _, err := rand.Read(key); err != nil {
		return nil, fmt.Errorf("deriving the page token key: %w", err)
	}

	return key, nil
}

// listQueryDigest names the question a listing asks, so a token can be bound
// to it.
//
// The filter's text and the effective page size, each length-prefixed so that
// no filter can be mistaken for another by where its bytes fall. A digest
// rather than the text itself, because a filter may be long and a token is
// bounded at 4096 characters by the schema: the token has to fit whatever the
// filter was.
func listQueryDigest(filter string, pageSize int) []byte {
	h := sha256.New()

	var length [8]byte
	binary.BigEndian.PutUint64(length[:], uint64(len(filter)))
	h.Write(length[:])
	h.Write([]byte(filter))

	binary.BigEndian.PutUint64(length[:], uint64(pageSize))
	h.Write(length[:])

	return h.Sum(nil)
}

// issuePageToken renders a position for a caller to hand back, bound to the
// tenant and query it was issued for and to the moment it was issued.
//
// An empty position is the end of the listing, and is reported as an empty
// token rather than a signed cursor naming nothing: an absent token is the one
// signal a caller has that a listing is done.
func (s *FlowstateServer) issuePageToken(position []byte, namespace string, query []byte, now time.Time) (string, error) {
	if len(position) == 0 {
		return "", nil
	}

	// Refused here, where the listing can still say so, rather than issued and
	// refused by the validator on the page after — which would read to a caller
	// as a listing that works exactly once. See [maxListPositionBytes].
	if len(position) > maxListPositionBytes {
		return "", fmt.Errorf("the visibility store's page token is %d bytes, more than the %d a page token can carry",
			len(position), maxListPositionBytes)
	}

	cursor, err := proto.Marshal(&v1.ListCursor{
		Position:    position,
		Namespace:   namespace,
		QueryDigest: query,
		IssuedAt:    timestamppb.New(now),
	})
	if err != nil {
		return "", err
	}

	return base64.RawURLEncoding.EncodeToString(s.sealListCursor(cursor)), nil
}

// listCursorMAC computes the authentication code over a serialized cursor.
//
// The one place the signed input is defined, called by both the sealing and
// the opening side, so what is signed and what is verified cannot drift apart.
func (s *FlowstateServer) listCursorMAC(cursor []byte) []byte {
	mac := hmac.New(sha256.New, s.listTokenKey)
	mac.Write(cursor)

	return mac.Sum(nil)
}

// sealListCursor appends the authentication code a serialized cursor is
// accepted by.
func (s *FlowstateServer) sealListCursor(cursor []byte) []byte {
	return append(cursor, s.listCursorMAC(cursor)...)
}

// openPageToken authenticates the token a caller returns and yields the
// position it carries, or refuses it with the sentence for what was wrong.
//
// The authentication code is checked before anything inside the token is
// read, so the tenant and query comparisons below are between values this
// server wrote and values it holds — never between a caller's claim and the
// truth. An empty token is the start of the listing and carries nothing to
// check.
func (s *FlowstateServer) openPageToken(token, namespace string, query []byte, now time.Time) ([]byte, error) {
	if token == "" {
		return nil, nil
	}

	sealed, err := base64.RawURLEncoding.DecodeString(token)
	if err != nil || len(sealed) < listTokenKeySize {
		return nil, errors.New("page token is not a token this server issued")
	}

	cursor, code := sealed[:len(sealed)-listTokenKeySize], sealed[len(sealed)-listTokenKeySize:]

	if !hmac.Equal(s.listCursorMAC(cursor), code) {
		return nil, errors.New("page token is not a token this server issued")
	}

	var parsed v1.ListCursor
	if err := proto.Unmarshal(cursor, &parsed); err != nil {
		// Unreachable for a token whose code this server produced, since it
		// only ever signs what it marshaled. Refused with the same sentence
		// rather than trusted, because a code that verifies over bytes the
		// server cannot read is exactly the case a fail-closed check is for.
		return nil, errors.New("page token is not a token this server issued")
	}

	if parsed.GetNamespace() != namespace {
		return nil, errors.New("page token was issued to a different namespace")
	}

	if !hmac.Equal(parsed.GetQueryDigest(), query) {
		return nil, errors.New("page token was issued for a different filter or page size; " +
			"continue with the filter and page size the listing started with, or start again without a token")
	}

	// Absent reads as the zero time, which is long expired: fail closed rather
	// than treat a token with no issue time as fresh.
	if now.Sub(parsed.GetIssuedAt().AsTime()) > listTokenLifetime {
		return nil, errors.New("page token has expired; start the listing again without one")
	}

	return parsed.GetPosition(), nil
}
