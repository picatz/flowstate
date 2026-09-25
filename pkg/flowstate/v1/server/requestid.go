package server

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"

	"connectrpc.com/connect"
	"go.temporal.io/api/common/v1"
	"go.temporal.io/api/workflowservice/v1"
	"google.golang.org/protobuf/proto"

	v1 "github.com/picatz/flowstate/pkg/flowstate/v1"
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
