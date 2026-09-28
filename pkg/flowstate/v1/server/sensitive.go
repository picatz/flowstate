package server

import (
	"cmp"
	"context"
	"errors"
	"fmt"
	"slices"
	"sync"

	"connectrpc.com/connect"
	enumspb "go.temporal.io/api/enums/v1"
	"go.temporal.io/sdk/client"

	v1 "github.com/picatz/flowstate/pkg/flowstate/v1"
	"github.com/picatz/flowstate/pkg/flowstate/v1/auth"
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
// an entry with no action list, which is unrestricted for every RPC action,
// is not granted it, because it is a widening nobody configured and the
// default must not be disclosure. Every reveal request is audited, allowed or
// not, under the field's own action. A caller who asks without the action is
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

// Get implements the RPC: [FlowstateServer.get]'s answer, with the run's
// declared-sensitive values withheld unless the caller asked and may.
func (s *FlowstateServer) Get(ctx context.Context, req *connect.Request[v1.GetRequest]) (*connect.Response[v1.GetResponse], error) {
	resp, err := s.get(ctx, req)
	if err != nil {
		return nil, err
	}

	out := resp.Msg
	decl := s.sensitiveDeclarationsOf(ctx, out.GetWorkflowId(), cmp.Or(out.GetFirstRunId(), out.GetRunId()))
	if !decl.declares {
		out.SensitiveDisclosure = v1.SensitiveDisclosure_SENSITIVE_DISCLOSURE_NONE_DECLARED
		return resp, nil
	}

	if req.Msg.GetRevealSensitive() {
		revealed, err := s.revealAuthorized(ctx, "Get", revealGetField, out.GetWorkflowId())
		if err != nil {
			return nil, err
		}
		if revealed {
			out.SensitiveDisclosure = v1.SensitiveDisclosure_SENSITIVE_DISCLOSURE_REVEALED
			return resp, nil
		}
	}

	withheld := v1.RedactGetResponse(out, decl.workflow, false)
	withheld = v1.RedactGetResponseFailures(withheld, decl.values)
	withheld.SensitiveDisclosure = v1.SensitiveDisclosure_SENSITIVE_DISCLOSURE_WITHHELD
	return connect.NewResponse(withheld), nil
}

// GetTimeline implements the RPC: [FlowstateServer.getTimeline]'s answer, with
// sensitive input values removed from its failure text unless the caller
// asked and may. A timeline carries no values but failure text, so that is
// all there is to withhold.
func (s *FlowstateServer) GetTimeline(ctx context.Context, req *connect.Request[v1.GetTimelineRequest]) (*connect.Response[v1.GetTimelineResponse], error) {
	resp, err := s.getTimeline(ctx, req)
	if err != nil {
		return nil, err
	}

	out := resp.Msg
	decl := s.sensitiveDeclarationsOf(ctx, req.Msg.GetWorkflowId(), cmp.Or(out.GetFirstRunId(), out.GetRunId()))
	if !decl.declares {
		out.SensitiveDisclosure = v1.SensitiveDisclosure_SENSITIVE_DISCLOSURE_NONE_DECLARED
		return resp, nil
	}

	if req.Msg.GetRevealSensitive() {
		revealed, err := s.revealAuthorized(ctx, "GetTimeline", revealTimelineField, req.Msg.GetWorkflowId())
		if err != nil {
			return nil, err
		}
		if revealed {
			out.SensitiveDisclosure = v1.SensitiveDisclosure_SENSITIVE_DISCLOSURE_REVEALED
			return resp, nil
		}
	}

	for _, entry := range out.GetEntries() {
		if entry.GetFailure() != "" {
			entry.Failure = decl.values.RedactText(entry.GetFailure(), v1.FailureWithheldMarker)
		}
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
	scope := v1.AuthorizationActionScope(action)

	principal, ok := auth.PrincipalFromContext(ctx)
	allowed := ok && slices.Contains(principal.Actions, scope)

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
// sensitive values: whether it declares any, the specification to redact
// against, and the sensitive input values to remove from failure text.
//
// Read from the run's own start input, the RunState the engine was started
// with, which is the specification that ran (a deployment-registered copy
// included) and the inputs it was bound with. When it cannot be read, the
// answer is the fail-closed one: declares, no specification (every declared
// output and the whole transcript withheld), and failure text withheld.
type sensitiveDeclarations struct {
	declares bool
	workflow *v1.Workflow
	values   v1.SensitiveValues
}

var failClosedDeclarations = sensitiveDeclarations{declares: true, values: v1.WithheldSensitiveValues()}

// maxCachedDeclarations bounds the per-run cache. A run's start input never
// changes, so an entry is never stale, only evicted; `flow watch` polling one
// run reads its history once.
const maxCachedDeclarations = 1024

type declarationCache struct {
	mu      sync.Mutex
	entries map[string]sensitiveDeclarations
}

func (c *declarationCache) get(key string) (sensitiveDeclarations, bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	d, ok := c.entries[key]
	return d, ok
}

func (c *declarationCache) put(key string, d sensitiveDeclarations) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.entries == nil || len(c.entries) >= maxCachedDeclarations {
		c.entries = make(map[string]sensitiveDeclarations)
	}
	c.entries[key] = d
}

func (s *FlowstateServer) sensitiveDeclarationsOf(ctx context.Context, workflowID, firstRunID string) sensitiveDeclarations {
	namespace := s.identityFor(ctx).GetNamespace()
	key := namespace + "\x00" + workflowID + "\x00" + firstRunID
	if d, ok := s.declarations.get(key); ok {
		return d
	}

	// The client authorization chose for this caller, as [FlowstateServer.get]
	// used, so the history read is the run that was checked.
	temporal, err := s.clientFor(namespace)
	if err != nil {
		return failClosedDeclarations
	}
	state, err := s.startedRunState(ctx, temporal, workflowID, firstRunID)
	if err != nil {
		return failClosedDeclarations
	}

	d := sensitiveDeclarations{workflow: state.GetWorkflow()}
	declares, err := v1.DeclaresSensitiveValues(state.GetWorkflow())
	if err != nil {
		return failClosedDeclarations
	}
	d.declares = declares
	if names := v1.SensitiveInputNames(state.GetWorkflow()); len(names) > 0 {
		d.values = v1.SensitiveInputValues(state.GetInputs(), names)
	}

	s.declarations.put(key, d)
	return d
}

// startedRunState reads the RunState a run's first segment was started with.
func (s *FlowstateServer) startedRunState(ctx context.Context, temporal client.Client, workflowID, firstRunID string) (*v1.RunState, error) {
	iter := temporal.GetWorkflowHistory(ctx, workflowID, firstRunID, false, enumspb.HISTORY_EVENT_FILTER_TYPE_ALL_EVENT)
	if iter == nil || !iter.HasNext() {
		return nil, errors.New("the run has no history")
	}
	event, err := iter.Next()
	if err != nil {
		return nil, err
	}
	started := event.GetWorkflowExecutionStartedEventAttributes()
	payloads := started.GetInput().GetPayloads()
	if started == nil || len(payloads) == 0 {
		return nil, fmt.Errorf("the run's first event is %s, not a start with an input", event.GetEventType())
	}
	var state v1.RunState
	if err := s.dataConverter.FromPayload(payloads[0], &state); err != nil {
		return nil, err
	}
	return &state, nil
}
