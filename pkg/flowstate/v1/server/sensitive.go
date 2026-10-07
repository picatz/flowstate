package server

import (
	"context"
	"errors"
	"fmt"
	"sync"

	"connectrpc.com/connect"
	enumspb "go.temporal.io/api/enums/v1"

	v1 "github.com/picatz/flowstate/pkg/flowstate/v1"
	"github.com/picatz/flowstate/pkg/flowstate/v1/authz"
)

// The server's half of `sensitive: true`: what a run's declared-sensitive
// values look like when they leave this process.
//
// # Why here
//
// The CLI used to decide, and it could not decide well. `flow get <id>` holds
// no specification, so it withheld every declared output of every run to be
// safe, while the same values reached any API caller in the clear, because
// the RPC returned them raw and only the CLI's renderer knew to hide them. The
// server holds both halves of the decision: the specification the run
// executed, read from the run's own start input, and the caller's authority.
// So it decides once, before a response leaves, and says what it decided in
// SensitiveDisclosure.
//
// # What the caller needs
//
// workload.read reads a run, as before, with its declared-sensitive values
// withheld. Asking for them (reveal_sensitive) additionally needs
// workload.reveal_sensitive, listed explicitly in the caller's policy entry:
// a caller is never implied it, because it is a widening nobody configured
// and the default must not be disclosure. Every reveal request that reaches the
// service is audited, allowed or not, under the field's own action; one the
// schema refuses (validate.NewInterceptor, before any handler) is refused
// before anything is decided or read, and writes no record, exactly as for
// every other action (cmd/flow/rpcoptions_test.go). A caller who asks without the action is
// answered with the values withheld rather than refused, so `flow get
// --reveal-sensitive` degrades to what a caller without the flag sees.
//
// # What it is not
//
// Display control at the API boundary. The values are in history like any
// other, protected there only by payload encryption (docs/ENCRYPTION.md), and
// a value CEL transformed is a different value no declaration follows.

const (
	revealGetField      = "flowstate.v1.GetRequest.reveal_sensitive"
	revealTimelineField = "flowstate.v1.GetTimelineRequest.reveal_sensitive"
)

// Get implements the RPC: FlowstateServer.get's answer, with the run's
// declared-sensitive values withheld unless the caller asked and may.
func (s *FlowstateServer) Get(ctx context.Context, req *connect.Request[v1.GetRequest]) (*connect.Response[v1.GetResponse], error) {
	// A reveal request is decided and audited before the run is read, and
	// whether or not the run turns out to hold anything to reveal: the record
	// is of the attempted elevated read, which depends only on the caller and
	// the field, and a required trail must not miss one because the run was
	// plain, missing, or someone else's.
	revealed := false
	if req.Msg.GetRevealSensitive() {
		var err error
		if revealed, err = s.revealAuthorized(ctx, "Get", revealGetField, req.Msg.GetWorkflowId()); err != nil {
			return nil, err
		}
	}

	resp, err := s.get(ctx, req)
	if err != nil {
		return nil, err
	}

	out := resp.Msg

	decl := s.sensitiveDeclarationsOf(ctx, out.GetWorkflowId(), out.GetRunId())
	switch {
	case !decl.declares:
		out.SensitiveDisclosure = v1.SensitiveDisclosure_SENSITIVE_DISCLOSURE_NONE_DECLARED
		return resp, nil
	case revealed:
		out.SensitiveDisclosure = v1.SensitiveDisclosure_SENSITIVE_DISCLOSURE_REVEALED
		return resp, nil
	}

	withheld := v1.RedactGetResponseDecided(out, decl.outputs, decl.carried)
	withheld = v1.RedactGetResponseFailures(withheld, decl.values)
	if decl.outputs == nil {
		// The specification could not be read, so whether it declares a
		// sensitive output a prompt could echo is unknown: withheld, as the
		// outputs and transcript already are on this path.
		v1.WithholdPendingWaitPrompts(withheld)
	}
	withheld.SensitiveDisclosure = v1.SensitiveDisclosure_SENSITIVE_DISCLOSURE_WITHHELD
	return connect.NewResponse(withheld), nil
}

// GetTimeline implements the RPC: FlowstateServer.getTimeline's answer, with
// sensitive input values removed from its failure text unless the caller
// asked and may. A timeline carries no values but failure text, so that is
// all there is to withhold.
func (s *FlowstateServer) GetTimeline(ctx context.Context, req *connect.Request[v1.GetTimelineRequest]) (*connect.Response[v1.GetTimelineResponse], error) {
	// Decided and audited before the run is read, as in Get.
	revealed := false
	if req.Msg.GetRevealSensitive() {
		var err error
		if revealed, err = s.revealAuthorized(ctx, "GetTimeline", revealTimelineField, req.Msg.GetWorkflowId()); err != nil {
			return nil, err
		}
	}

	resp, err := s.getTimeline(ctx, req)
	if err != nil {
		return nil, err
	}

	out := resp.Msg

	decl := s.sensitiveDeclarationsOf(ctx, req.Msg.GetWorkflowId(), out.GetRunId())
	switch {
	case !decl.declares:
		out.SensitiveDisclosure = v1.SensitiveDisclosure_SENSITIVE_DISCLOSURE_NONE_DECLARED
		return resp, nil
	case revealed:
		out.SensitiveDisclosure = v1.SensitiveDisclosure_SENSITIVE_DISCLOSURE_REVEALED
		return resp, nil
	}

	for _, entry := range out.GetEntries() {
		if entry.GetFailure() != "" {
			entry.Failure = decl.values.RedactText(entry.GetFailure(), v1.FailureWithheldMarker)
		}
	}
	// A marker can be longer than the value it replaces, so the answer's byte
	// bound, applied as it was assembled, is applied again to what leaves.
	if kept := refitTimeline(out.GetEntries()); kept < len(out.GetEntries()) {
		out.Entries = out.Entries[:kept]
		out.Truncated = true
	}
	out.SensitiveDisclosure = v1.SensitiveDisclosure_SENSITIVE_DISCLOSURE_WITHHELD
	return resp, nil
}

// revealAuthorized reports whether the caller may read declared-sensitive
// values in the clear, and records the decision either way.
func (s *FlowstateServer) revealAuthorized(ctx context.Context, rpc, field, workflowID string) (bool, error) {
	action, err := v1.AuthorizationActionForRequestField(field)
	if err != nil {
		return false, connect.NewError(connect.CodeInternal, err)
	}

	allowed := s.decide(ctx, action, authz.Explicit).Allowed

	subject := s.auditSubject(ctx, rpc, v1.AuditResourceKind_AUDIT_RESOURCE_KIND_RUN, workflowID)
	subject.RequestField = field
	if allowed {
		// A required recorder that cannot record the allow releases nothing.
		if err := s.audit.Allow(ctx, subject); err != nil {
			return false, err
		}
		return true, nil
	}
	if err := s.audit.Deny(ctx, subject, v1.AuditDenyCode_AUDIT_DENY_CODE_POLICY_DENIED); err != nil {
		return false, err
	}
	return false, nil
}

// sensitiveDeclarations is what a run's executed specification says about
// sensitive values, reduced to the answers redaction needs: whether it
// declares any, which output names are sensitive, what may be done with
// carried values, and the sensitive input values to remove from failure text.
// The specification itself is not kept: a cache of a thousand of these must
// not pin a thousand specifications of up to MaxSpecBytes each.
//
// Read from the start input of the segment being reported, the RunState the
// engine was started with, which is the specification that ran (a
// deployment-registered copy included) and the inputs it was bound with.
// Every segment carries the same two unchanged across Continue-As-New
// (engine/workflow.go), and the reported segment's history lives as long as
// the segment does, where the first segment's may already be gone. When it
// cannot be read, the answer is the fail-closed one.
type sensitiveDeclarations struct {
	declares bool
	outputs  map[string]bool
	carried  v1.CarriedValues
	values   v1.SensitiveValues
}

// failClosedDeclarations withholds every declared output, the whole
// transcript and carried state, and failure text.
var failClosedDeclarations = sensitiveDeclarations{
	declares: true,
	carried:  v1.CarriedValuesUnverified,
	values:   v1.WithheldSensitiveValues(),
}

// The per-run cache's bounds. An entry holds output names and a
// sensitive-value set, never the specification, but the value set holds the
// run's sensitive inputs, which a caller chose: counting entries alone would
// let a caller who starts runs with near-limit inputs pin a gigabyte. So each
// entry is costed by what it retains, an entry costing more than
// maxDeclarationBytes is not cached at all (the next read of that run reads
// its start event again), and the cache starts over when the total would pass
// maxCachedDeclarationBytes. A segment's start input never changes, so an
// entry is never stale, only evicted; `flow watch` polling one run reads it
// once.
const (
	maxCachedDeclarations     = 1024
	maxDeclarationBytes       = 256 << 10
	maxCachedDeclarationBytes = 16 << 20
)

type declarationCache struct {
	mu      sync.Mutex
	entries map[string]cachedDeclarations
	bytes   int
}

type cachedDeclarations struct {
	d    sensitiveDeclarations
	cost int
}

func (c *declarationCache) get(key string) (sensitiveDeclarations, bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	e, ok := c.entries[key]
	return e.d, ok
}

// put caches d under key at cost bytes, unless it alone is too large to keep.
func (c *declarationCache) put(key string, d sensitiveDeclarations, cost int) {
	if cost > maxDeclarationBytes {
		return
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.entries == nil || len(c.entries) >= maxCachedDeclarations || c.bytes+cost > maxCachedDeclarationBytes {
		c.entries = make(map[string]cachedDeclarations)
		c.bytes = 0
	}
	if old, ok := c.entries[key]; ok {
		c.bytes -= old.cost
	}
	c.entries[key] = cachedDeclarations{d: d, cost: cost}
	c.bytes += cost
}

// declarationCost is what caching d retains: its output names, and what its
// value set holds, which is costed from the set itself because it binds
// declared defaults as well as the caller's inputs.
func declarationCost(d sensitiveDeclarations) int {
	cost := d.values.RetainedBytes()
	for name := range d.outputs {
		cost += len(name)
	}
	return cost
}

func (s *FlowstateServer) sensitiveDeclarationsOf(ctx context.Context, workflowID, runID string) sensitiveDeclarations {
	namespace := s.identityFor(ctx).GetPrincipal().GetNamespace()
	key := namespace + "\x00" + workflowID + "\x00" + runID
	if d, ok := s.declarations.get(key); ok {
		return d
	}

	state, err := s.startedRunState(ctx, namespace, workflowID, runID)
	if err != nil || state.GetWorkflow() == nil {
		return failClosedDeclarations
	}
	declares, err := v1.DeclaresSensitiveValues(state.GetWorkflow())
	if err != nil {
		return failClosedDeclarations
	}
	// A callee's declarations reach the caller's values through expressions
	// this cannot trace, so a run embedding one is withheld whole.
	if callee, err := v1.CalleeDeclaresSensitiveValues(state.GetWorkflow()); err != nil || callee {
		s.declarations.put(key, failClosedDeclarations, 0)
		return failClosedDeclarations
	}

	d := sensitiveDeclarations{
		declares: declares,
		outputs:  v1.SensitiveOutputNames(state.GetWorkflow()),
		carried:  v1.DecideCarriedValues(state.GetWorkflow(), false),
		values:   v1.RunFailureSensitiveValues(state.GetWorkflow(), state.GetInputs()),
	}

	s.declarations.put(key, d, declarationCost(d))
	return d
}

// startedRunState reads the RunState a segment was started with, through the
// client authorization chose for this caller, as FlowstateServer.get did,
// so the history read is of the run that was checked.
//
// Through the SDK's iterator, which reads one page and is stopped after the
// first event. A raw GetWorkflowExecutionHistory call would ask for less, but
// some frontends answer raw reads in RawHistory, which only the SDK
// deserializes (timeline.go says more); read raw, those deployments would see
// no start event and withhold every run.
func (s *FlowstateServer) startedRunState(ctx context.Context, namespace, workflowID, runID string) (*v1.RunState, error) {
	temporal, err := s.clientFor(namespace)
	if err != nil {
		return nil, err
	}
	iter := temporal.GetWorkflowHistory(ctx, workflowID, runID, false, enumspb.HISTORY_EVENT_FILTER_TYPE_ALL_EVENT)
	if iter == nil || !iter.HasNext() {
		return nil, errors.New("the run has no history")
	}
	event, err := iter.Next()
	if err != nil {
		return nil, err
	}
	if event == nil {
		return nil, errors.New("the run has no history")
	}

	payloads := event.GetWorkflowExecutionStartedEventAttributes().GetInput().GetPayloads()
	if len(payloads) == 0 {
		return nil, fmt.Errorf("the run's first event is %s, not a start with an input", event.GetEventType())
	}
	var state v1.RunState
	if err := s.dataConverter.FromPayload(payloads[0], &state); err != nil {
		return nil, err
	}
	return &state, nil
}
