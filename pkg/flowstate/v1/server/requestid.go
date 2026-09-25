package server

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"time"

	"connectrpc.com/connect"
	"go.temporal.io/api/common/v1"
	"go.temporal.io/api/enums/v1"
	"go.temporal.io/api/serviceerror"
	"go.temporal.io/api/workflowservice/v1"
	"go.temporal.io/sdk/client"
	"google.golang.org/protobuf/proto"

	v1 "github.com/picatz/flowstate/pkg/flowstate/v1"
	"github.com/picatz/flowstate/pkg/flowstate/v1/engine"
)

// [RunRequest.request_id]'s server half: what a request id establishes about a
// submission, how it is recorded on the run, and how a later submission under
// the same id is told apart from a retry.
//
// The mechanism is the webhook receiver's, reached rather than restated: a
// workflow id derived from the key by digest, Temporal's own uniqueness on that
// id as the dedupe, and the cluster's already-started answer — carrying the
// incumbent's run id — as the fact the server states a reuse from. See
// [WebhookReceiver.start] for why that holds where a time-window dedupe cannot.

// requestWorkflowIDPrefix keeps request-addressed runs in an id namespace of
// their own, distinct from `flowstate-workflow-`, [v1.EntityWorkflowID]'s,
// [v1.ConcurrencyWorkflowID]'s and [webhookWorkflowID]'s, so a request id can
// never address, join or block a run any of those created.
const requestWorkflowIDPrefix = "flowstate-request-"

// requestMemoKey records, on a run started with a request id, the digest that
// id derives to — [submissionKey.request], never the id itself, for the reason
// [webhookWorkflowID] gives about an idempotency key: a caller-chosen value
// must not become a durable, broadly readable identifier.
//
// submissionMemoKey records the digest of what that request submitted —
// [submissionKey.submission] — which is what a retry is checked against. Both
// are absent on a run started without a request id, and absence fails closed:
// a run that recorded no request can never be reused by one.
const (
	requestMemoKey    = "flowstate.request"
	submissionMemoKey = "flowstate.submission"
)

// submissionKey is what one request id establishes about one submission.
type submissionKey struct {
	// workflowID is the id the run is addressed by when nothing else — an
	// entity key, a `concurrency:` block — addresses it.
	workflowID string

	// request is the digest the request id derives to under the caller's
	// namespace; the id above is this with [requestWorkflowIDPrefix] in front.
	request string

	// submission is the digest of the specification as sent and the inputs as
	// bound: the pair a retry resends unchanged and a second submission does not.
	submission string
}

// newSubmissionKey composes the key for a request, or returns nil when the
// request carries no request id — the byte-identical-to-today case.
//
// The namespace is the caller's own, from the identity the server attested,
// never from the request: the same rule [v1.EntityWorkflowID] applies to its
// namespace half and for the identical reason. inputs must already be
// [v1.BindRunInputs]'s output, so that a submission that named an input and one
// that took its default digest identically — they are the same submission.
func newSubmissionKey(namespace, requestID string, submitted *v1.Workflow, inputs map[string]*v1.Value) (*submissionKey, error) {
	if requestID == "" {
		return nil, nil
	}

	request := digestOf(namespace, requestID)

	// The submission as one message rather than two digests joined by hand: a
	// deterministic marshal of the same shape the request arrived in, minus the
	// fields that are about *this* call rather than about what it submits (the
	// request id itself, the reason, the entity key that decides the address).
	// Deterministic, because [RunRequest.inputs] is a map and a map's wire order
	// is otherwise whatever the encoder chose that time.
	encoded, err := proto.MarshalOptions{Deterministic: true}.Marshal(&v1.RunRequest{
		Workflow: submitted,
		Inputs:   inputs,
	})
	if err != nil {
		return nil, fmt.Errorf("digesting the submission: %w", err)
	}

	return &submissionKey{
		workflowID: requestWorkflowIDPrefix + request,
		request:    request,
		submission: v1.ContentDigest(encoded),
	}, nil
}

// memo is what the key writes on the run: the two digests, and never the
// request id itself.
func (k *submissionKey) memo() map[string]any {
	return map[string]any{
		requestMemoKey:    k.request,
		submissionMemoKey: k.submission,
	}
}

// digestWorkflowID derives a workflow id from parts that together name one
// thing — a tenant and a key, a tenant, a workflow, a trigger and a key.
//
// Hashed rather than interpolated, for the two reasons [webhookWorkflowID]
// gives and both request ids and delivery keys share: a key is caller-chosen,
// so it can contain characters Temporal refuses, a length past its limit, or a
// value crafted to collide with an id somebody else's run is addressed by; and a
// key can be credential-shaped material that must not become a durable, broadly
// readable identifier. A digest is fixed-length, alphabet-safe, and reveals
// nothing about the key it names.
//
// Parts are separated by a NUL, which no part can contain, so the join is
// unambiguous: `("a", "bc")` and `("ab", "c")` are different inputs and different
// digests, which is what makes "the tenant is inside the digest" a boundary
// rather than a convention.
func digestWorkflowID(prefix string, parts ...string) string {
	return prefix + digestOf(parts...)
}

// digestOf is [digestWorkflowID]'s digest without its prefix, for the one
// reader that records the digest rather than addresses by it — the memo a
// request id leaves on its run.
func digestOf(parts ...string) string {
	sum := sha256.New()
	for i, part := range parts {
		if i > 0 {
			sum.Write([]byte{0})
		}
		sum.Write([]byte(part))
	}

	return hex.EncodeToString(sum.Sum(nil))
}

// reusedSubmission decides whether a run this request collided with is the one
// this request's own earlier attempt started.
//
// Called from [FlowstateServer.Run]'s already-started path, with the run id the
// cluster's answer carried, and only when the request named a request id. Three
// answers:
//
//   - The run recorded this request's digest and this request's submission
//     digest: a retry. The described execution is returned so the caller can be
//     told the run's *current* status, which for a retry that arrives after the
//     run finished is the finished one.
//   - The run recorded this request's digest and a different submission: refused,
//     `AlreadyExists`, naming the run. The caller reused a key for a different
//     payload, and attaching them to a run that will do something else is the
//     one answer an idempotency key must never give.
//   - The run recorded a different request, or none: not a retry, and reported
//     as such (ok false) so the address's own conflict rule decides — a
//     `concurrency:` block's `on_conflict:`, or an entity's refusal.
//
// The run is described through [FlowstateServer.authorizeRunDecision] rather
// than a bare Describe: every id this can be called with is composed under the
// caller's own tenant, so the tenancy check is redundant by construction, and
// it is kept for the reason every other verb keeps it — a boundary that holds
// by construction *and* by check is one a future id scheme cannot quietly open.
// Un-audited, because the decision this request made was recorded when it was
// admitted; see audit.go's one-record rule and [FlowstateServer.SignalWithStart]'s
// identical already-started arm.
func (s *FlowstateServer) reusedSubmission(ctx context.Context, workflowID, runID string, key *submissionKey) (*workflowservice.DescribeWorkflowExecutionResponse, bool, error) {
	_, resp, _, err := s.authorizeRunDecision(ctx, workflowID, runID)
	if err != nil {
		return nil, false, err
	}

	memo := resp.GetWorkflowExecutionInfo().GetMemo()

	request, ok, err := s.memoString(memo, requestMemoKey)
	if err != nil {
		return nil, false, connect.NewError(connect.CodeInternal, err)
	}
	if !ok || request != key.request {
		return nil, false, nil
	}

	submission, ok, err := s.memoString(memo, submissionMemoKey)
	if err != nil {
		return nil, false, connect.NewError(connect.CodeInternal, err)
	}
	if !ok || submission != key.submission {
		// AlreadyExists rather than InvalidArgument: nothing about this request
		// is malformed, and the fact the caller needs is that the key already
		// names a run. Naming it is not disclosure — the id is composed under
		// this caller's own tenant, and the run was checked to be theirs above.
		return nil, false, connect.NewError(connect.CodeAlreadyExists, fmt.Errorf(
			"request_id already started run %s of workflow %s with a different specification or inputs; "+
				"a retry must resend the same submission, and a new submission needs a new request_id",
			runID, workflowID))
	}

	return resp, true, nil
}

// maxTerminateOtherRounds bounds [FlowstateServer.claimAfterTerminatingOther]'s
// terminate-and-verify loop, so two submissions that keep displacing one
// another cannot livelock a request forever (invariant 5). A two-way race —
// the only shape the acceptance criteria of #1966 ask for — resolves in one
// extra round; this leaves headroom for a third caller racing the same id
// before turning a genuine pile-up into a fast, legible refusal rather than a
// hung request.
const maxTerminateOtherRounds = 4

// claimAfterTerminatingOtherTimeout bounds the commitment
// [FlowstateServer.claimAfterTerminatingOther] makes once it has terminated a
// run: the caller's own context stops being a safe signal to give up by, once
// giving up would leave the id it just vacated empty rather than merely
// answer them late. [recordContext] (webhook.go) bounds a different
// after-the-fact commitment the identical way, for the identical reason.
//
// Generous against a slow Temporal round trip and the retries below; short
// enough that a genuinely unreachable cluster still gives up rather than
// pinning this goroutine forever.
const claimAfterTerminatingOtherTimeout = 30 * time.Second

// claimStartRetryDelay is how long [claimStart] waits between a transient
// start failure and retrying it. The SDK's own transport layer already
// retries a single dropped frame; this is for whatever survives that, so it
// does not need to be short — only short enough that a handful of attempts
// still fit inside [claimAfterTerminatingOtherTimeout].
const claimStartRetryDelay = 200 * time.Millisecond

// reusedRunAuditTimeout bounds the second audit record
// [FlowstateServer.Run] writes when [FlowstateServer.claimAfterTerminatingOther]
// answers with a discovered run rather than one this call started — the
// same after-the-fact commitment [recordContext] (webhook.go) makes for a
// post-answer record, and for the identical reason: the decision it
// describes already happened, so a caller who left in the meantime must not
// be the reason a required record is never written. Short, because writing
// one record is not the terminate-and-claim commitment above and does not
// need its bound.
const reusedRunAuditTimeout = 5 * time.Second

// claimAfterTerminatingOther is `on_conflict: terminate_other`'s reissue for
// a request-addressed submission whose probe found an incumbent that is not
// its own retry — #1966.
//
// # The race this closes
//
// Temporal's WorkflowIDConflictPolicy offers no compare-and-terminate:
// TERMINATE_EXISTING destroys whatever is current at the id and starts a new
// run, unconditionally. Two submissions that both probed the same stale
// incumbent and both concluded "this is a different submission, replace it"
// could each issue that unconditional reissue — the first starts run A, the
// second, issued after, destroys A and starts run B. One request_id then
// answers two different callers with two different run ids, and the first
// caller holds a run id that has already been terminated.
//
// # The fix, built from what the client already offers
//
// [client.Client.TerminateWorkflow] takes a specific run id, and a run id is
// a permanent identity: asking Temporal to terminate one can never reach a
// *different*, later run at the same workflow id, whatever that later run's
// state — a run id that has already closed answers [serviceerror.NotFound]
// ("already completed") rather than letting the call touch whatever now
// holds the id. That is a real, if manual, compare-and-terminate: "terminate
// the run I observed, and nothing else," proved against a real Temporal
// namespace rather than assumed from the SDK's doc comment.
//
// So this replaces the incumbent it observed by that specific run id —
// "already gone" is as good an outcome as "just terminated it," since both
// mean the id is no longer held by the run this call observed — and then
// claims the id with FAIL rather than TERMINATE_EXISTING. FAIL cannot
// destroy a sibling's fresh start; it discovers one. A run discovered this
// way is resolved exactly like the retry arm in [FlowstateServer.Run]:
// [FlowstateServer.reusedSubmission] cannot tell "the run this call just
// started" from "an identical submission's run that beat this one to the id"
// apart, because both carry the same recorded digests — which is exactly
// #1966's answer: two racing, byte-identical submissions converge on
// whichever run is actually current, and neither response names a run the
// other call destroyed.
//
// A run discovered this way that is *not* a matching submission is a third,
// genuinely different one that reached the id first, and `on_conflict:`
// still says it is replaced — the loop repeats against it, bounded so two
// submissions that keep displacing each other cannot loop forever.
//
// # Once committed, this does not give up because the caller did
//
// The old, single-call TERMINATE_EXISTING reissue was very likely atomic
// server-side: one Temporal API call, so nothing this server did could
// observe it half-applied. Terminating a specific run and then claiming the
// id as two separate round trips gives that up unless something here puts it
// back — a caller context cancelled between the two (a load balancer
// resetting the connection, an operator's Ctrl-C, a client-side deadline)
// must not leave the id terminated with nothing to replace it, which would
// be a strictly worse outcome than the race this function exists to close.
// So every terminate-and-claim round after the first line of this function
// runs under a context of this call's own — the caller's values, none of
// their cancellation, bounded by [claimAfterTerminatingOtherTimeout] so an
// unreachable cluster still gives up rather than pinning the goroutine
// forever — and [claimStart] retries a transient failure to claim within
// that bound rather than surfacing the first one. See #2061's review.
//
// # A window this split genuinely opened, not one it inherited
//
// Splitting one atomic call into two opens a real window in which the id
// this call vacated can sit empty rather than holding a replacement — say so
// plainly, rather than describing it as a cost the old form already carried,
// which it very likely did not. It has three triggers, all bounded to at
// most [claimAfterTerminatingOtherTimeout]: this server's own process ending
// between the terminate below and the claim it commits to (no context,
// caller-scoped or not, survives that); the commit deadline itself elapsing
// before a claim succeeds against a cluster that stays unreachable or
// overloaded the whole time; and an ambiguous terminate outcome (below)
// followed by a claim that fails for a reason retrying cannot fix. `flow
// list` finds the id simply empty in every case, never corrupted — and the
// recovery is the same one `request_id` already promises: retry the request
// with the same id. A retry that lands after the window closed either finds
// the id still empty and claims it fresh, or finds whichever run last
// claimed it and is told so — never a run it did not ask for.
//
// submission must not be nil: this is reachable only from the request-id arm
// of [FlowstateServer.Run], which is the one case that can produce the
// [serviceerror.WorkflowExecutionAlreadyStarted] this function is called to
// resolve under `on_conflict: terminate_other` — a submission-less request
// under that policy is given TERMINATE_EXISTING outright and never reaches
// this reissue at all. Checked rather than assumed, so a future caller that
// gets this wrong sees a clear error instead of a nil-pointer panic inside
// [FlowstateServer.reusedSubmission].
func (s *FlowstateServer) claimAfterTerminatingOther(
	ctx context.Context,
	temporal client.Client,
	workflowID, incumbentRunID string,
	options client.StartWorkflowOptions,
	state *v1.RunState,
	submission *submissionKey,
	asSubmitted bool,
) (*v1.RunResponse, error) {
	if submission == nil {
		return nil, connect.NewError(connect.CodeInternal, errors.New(
			"claimAfterTerminatingOther: called with no submission recorded, which should be unreachable under on_conflict: terminate_other"))
	}

	// Checked against the caller's own context, before anything below stops
	// listening to it: nothing has been terminated yet, so a caller already
	// gone is refused the ordinary way rather than paying for a bounded
	// commitment nobody is waiting to hear the answer to.
	if err := ctx.Err(); err != nil {
		return nil, connect.NewError(contextCode(err), err)
	}

	commit, cancel := context.WithTimeout(context.WithoutCancel(ctx), claimAfterTerminatingOtherTimeout)
	defer cancel()

	for round := 0; round < maxTerminateOtherRounds; round++ {
		if err := temporal.TerminateWorkflow(commit, workflowID, incumbentRunID,
			"flowstate: superseded by a different submission under `on_conflict: terminate_other`"); err != nil {
			// Already gone — terminated by a racing sibling's own round, or
			// finished on its own between the probe and here — is success
			// for this call's purpose: the id must not still be held by the
			// run it observed, and it is not.
			//
			// Anything else this call cannot tell apart from "Temporal
			// applied it and the answer never arrived" — a deadline the
			// per-call timeout hit well inside this function's own bound, a
			// dropped response — is not treated as failure either: the
			// claim below is safe to attempt regardless, because FAIL can
			// only discover what still holds the id, never destroy it, so a
			// terminate that actually landed is confirmed by that claim
			// succeeding and one that did not is confirmed by the loop
			// finding the same incumbent again. Only a terminatePermanentlyFailed
			// error — one the terminate call itself refused, proving it
			// never reached Temporal at all — is worth reporting as this
			// server's own failure rather than attempting the claim anyway.
			var notFound *serviceerror.NotFound
			if !errors.As(err, &notFound) && terminatePermanentlyFailed(err) {
				return nil, connect.NewError(connect.CodeInternal,
					fmt.Errorf("terminating the run this submission replaces: %w", err))
			}
		}

		options.WorkflowIDConflictPolicy = enums.WORKFLOW_ID_CONFLICT_POLICY_FAIL
		run, err := claimStart(commit, temporal, options, state)
		if err == nil {
			return &v1.RunResponse{
				WorkflowId:               workflowID,
				RunId:                    run.GetRunID(),
				Status:                   v1.RunResponse_STATUS_RUNNING,
				SpecificationAsSubmitted: proto.Bool(asSubmitted),
			}, nil
		}

		var already *serviceerror.WorkflowExecutionAlreadyStarted
		if !errors.As(err, &already) {
			return nil, connect.NewError(connect.CodeInternal, fmt.Errorf("unable to execute workflow: %w", err))
		}

		resp, retry, err := s.reusedSubmission(commit, workflowID, already.RunId, submission)
		if err != nil {
			return nil, err
		}
		if retry {
			return &v1.RunResponse{
				WorkflowId:               workflowID,
				RunId:                    resp.GetWorkflowExecutionInfo().GetExecution().GetRunId(),
				Status:                   getWorkflowExecutionStatus(resp),
				Reused:                   true,
				SpecificationAsSubmitted: proto.Bool(false),
			}, nil
		}

		// Neither this call's own fresh start nor a matching sibling: a
		// third submission reached the id first. `on_conflict:` still says
		// it is replaced — loop, now against that run.
		incumbentRunID = already.RunId
	}

	return nil, connect.NewError(connect.CodeAborted, fmt.Errorf(
		"workflow %q kept being replaced by a different submission before this one could claim it (%d attempts)",
		workflowID, maxTerminateOtherRounds))
}

// claimStart attempts to start the replacement run, retrying a transient
// failure within ctx's own bound rather than surfacing the first one: the
// terminate before this call already vacated the id, so giving up on one
// blip would leave it empty rather than merely slow to fill. A start that
// answers [serviceerror.WorkflowExecutionAlreadyStarted] is returned
// immediately without retrying — it is the expected outcome when a sibling
// claimed the id first, not a failure to recover from.
//
// Only [claimStartTransient] is retried. Everything else — a permission
// denied, an invalid argument, a namespace this deployment no longer routes,
// a specification Temporal's own codec rejects — is a permanent property of
// this request, not of the moment it was asked in, and retrying it for the
// better part of [claimAfterTerminatingOtherTimeout] would only make a
// request that was always going to fail take thirty seconds longer to.
func claimStart(ctx context.Context, temporal client.Client, options client.StartWorkflowOptions, state *v1.RunState) (client.WorkflowRun, error) {
	for {
		run, err := temporal.ExecuteWorkflow(ctx, options, engine.Run, state)
		if err == nil {
			return run, nil
		}

		var already *serviceerror.WorkflowExecutionAlreadyStarted
		if errors.As(err, &already) {
			return nil, err
		}

		if !claimStartTransient(err) {
			return nil, err
		}

		select {
		case <-ctx.Done():
			return nil, err
		case <-time.After(claimStartRetryDelay):
		}
	}
}

// terminatePermanentlyFailed reports whether err proves a [client.Client.TerminateWorkflow]
// call was refused before Temporal ever acted on it — a permission this
// server does not hold, or an argument the call itself was malformed enough
// to reject — as opposed to an ambiguous outcome (the per-call deadline
// elapsing, a dropped response) where Temporal may have applied the
// terminate anyway. Only a permanent failure is worth reporting as this
// server's own error; an ambiguous one is resolved by attempting the claim
// that follows, which is safe either way. See
// [FlowstateServer.claimAfterTerminatingOther]'s own doc for why.
func terminatePermanentlyFailed(err error) bool {
	var permissionDenied *serviceerror.PermissionDenied
	var invalidArgument *serviceerror.InvalidArgument
	var namespaceNotFound *serviceerror.NamespaceNotFound

	return errors.As(err, &permissionDenied) ||
		errors.As(err, &invalidArgument) ||
		errors.As(err, &namespaceNotFound)
}

// claimStartTransient reports whether err is worth [claimStart] retrying: a
// deadline that elapsed on one attempt, or the cluster briefly unavailable —
// the two outcomes the SDK's own transport-level retry does not already
// absorb for a unary call, and the ones a moment later commonly resolves.
func claimStartTransient(err error) bool {
	var deadlineExceeded *serviceerror.DeadlineExceeded
	var unavailable *serviceerror.Unavailable

	return errors.As(err, &deadlineExceeded) || errors.As(err, &unavailable) ||
		errors.Is(err, context.DeadlineExceeded)
}

// memoString reads one string-valued memo field the way [memoStarter] reads
// its own: absent is a real answer (ok false, err nil), and a value that is
// present but cannot be decoded is an error rather than a silent absence.
func (s *FlowstateServer) memoString(memo *common.Memo, memoKey string) (string, bool, error) {
	payload, ok := memo.GetFields()[memoKey]
	if !ok {
		return "", false, nil
	}

	var value string
	if err := s.dataConverter.FromPayload(payload, &value); err != nil {
		return "", false, fmt.Errorf("server: reading %s recorded on a run: %w", memoKey, err)
	}

	return value, true, nil
}

// errNotARetry is the answer [FlowstateServer.Run] gives when a request id
// collided with a run under an address it does not own the conflict rule for —
// an entity key — and the run turned out not to be this submission's own.
var errNotARetry = errors.New("a run already exists at this address and was started by a different submission")
