package server

import (
	"context"
	"fmt"
	"slices"

	"connectrpc.com/connect"
	v1 "github.com/picatz/flowstate/pkg/flowstate/v1"
	"github.com/picatz/flowstate/pkg/flowstate/v1/audit"
	"github.com/picatz/flowstate/pkg/flowstate/v1/auth"
)

// Where an authorization decision is written down.
//
// One seam per RPC, reached before the mutation the decision permits, and
// nowhere else. The rules the rest of this file follows, from #1018:
//
//   - The subject of the record is the *decision*, not the effect. "This
//     attested caller was authorized for workload.signal on run X at server
//     time T" is complete and permanently true the instant the check returns,
//     whether or not Temporal then fails to deliver the signal. That is why
//     the emit is write-ahead and why there is no second record afterwards
//     saying what happened: #993 wrote "accepted" before the acceptance
//     existed, and an audit log is the one artifact here that must not be
//     wrong. The one exception is a handler that panics after its allow:
//     the recover interceptor (recover.go, picatz/flowstate#1761) writes an
//     INTERNAL_ERROR record under the same correlation id, which is not a
//     revision of the decision but the statement that nobody acted on it.
//
//   - Exactly one record per decision. Where a verb resolves a run twice —
//     Signal, walking from a Continue-As-New chain's first run id to the
//     current one — the two lookups are one decision reached in two steps, so
//     they go through [FlowstateServer.authorizeRunDecision] and the verb
//     emits once, after the resolution settles. Two records there would report
//     a denial for a request that was allowed.
//
//   - The record is emitted before the request is necessarily valid, wherever
//     the decision itself does not depend on validity. Whether a caller may
//     start work in their namespace is not a question about whether their
//     workflow parses.
//
//   - A required recorder's failure is the request's failure, which is the
//     whole of "an action that cannot be recorded does not happen". A
//     recorder that is not required swallows its sink's errors rather than
//     turning an operator's collector outage into an outage of the service
//     they did not ask to gate on it.
//
// TestEveryRPCReachesTheAuditSeam walks flowstate.v1.WorkflowService's
// descriptor and this package's own source, so an RPC added without a seam is
// a failure rather than a silence.

// auditAllow records an authorization that was granted, before the mutation it
// permits.
//
// This is also the shared per-action authorization seam. A policy entry with
// no action list preserves legacy behavior; a configured list must contain the
// exact scope bound to this RPC. The check is outside the recorder so disabling
// audit output cannot disable authorization.
func (s *FlowstateServer) auditAllow(ctx context.Context, rpc string, kind v1.AuditResourceKind, key string) error {
	if err := s.authorizeAction(ctx, rpc, kind, key); err != nil {
		return err
	}

	return s.audit.Allow(ctx, s.auditSubject(ctx, rpc, kind, key))
}

// authorizeAction refuses a caller whose policy-assigned actions do not include
// the one this RPC requires, and records that refusal.
//
// Separate from [FlowstateServer.auditAllow] so that a verb which addresses an
// existing resource can ask it *before* resolving that resource. Asked only at
// the allow seam, the check runs after a Describe and a tenant comparison have
// already decided the answer, and the two refusals are different: a run that
// exists in the caller's own tenant reaches the action check and is refused as
// permission denied, while an absent, foreign, or non-Flowstate id was refused
// as not found on the way there. A caller holding no action at all could
// therefore learn which guessed ids name a resource in their tenant, from the
// status alone, without holding the action that reads one (#1119).
//
// The order this restores is the one the refusals were written for: a caller
// who may not act at all is told so before anything is addressed, and a caller
// who may act keeps the uniform "no such run" for everything they cannot see.
// It costs no round trip — the decision is the principal and the RPC, both
// already in hand.
//
// The check is outside the recorder so that disabling audit output cannot
// disable authorization.
func (s *FlowstateServer) authorizeAction(ctx context.Context, rpc string, kind v1.AuditResourceKind, key string) error {
	principal, ok := auth.PrincipalFromContext(ctx)
	if !ok || principal.Actions == nil {
		// A policy entry with no action list preserves legacy behavior.
		return nil
	}

	action, err := v1.AuthorizationActionForRPC(rpc)
	if err != nil {
		return connect.NewError(connect.CodeInternal, err)
	}

	scope := v1.AuthorizationActionScope(action)
	if slices.Contains(principal.Actions, scope) {
		return nil
	}

	refusal := connect.NewError(connect.CodePermissionDenied,
		fmt.Errorf("the caller is not authorized for required action %q", scope))
	refusal.Meta().Set("WWW-Authenticate", fmt.Sprintf(`Bearer error="insufficient_scope", scope=%q`, scope))

	return s.auditDeny(ctx, rpc, kind, key, v1.AuditDenyCode_AUDIT_DENY_CODE_POLICY_DENIED, refusal)
}

// ValidateAuthorizationPolicy checks every policy-assigned action against the
// schema-owned scope vocabulary. The auth package cannot perform this semantic
// check without importing its parent package and creating a cycle, so control
// plane assembly calls this before constructing its verifier.
func ValidateAuthorizationPolicy(policy *auth.Policy) error {
	if policy == nil {
		return nil
	}

	known := v1.AuthorizationActionScopes()
	for i, issuer := range policy.Issuers {
		for j, action := range issuer.Actions {
			if !slices.Contains(known, action) {
				return fmt.Errorf("%w: issuers[%d] (%q): actions[%d] %q is not a Flowstate authorization action; want one of %v",
					auth.ErrInvalidPolicy, i, issuer.Name, j, action, known)
			}
		}
	}

	return nil
}

// auditDeny records a refusal and returns the refusal to hand back.
//
// refusal is returned unchanged in the ordinary case, so a call site reads
// `return nil, s.auditDeny(...)` and cannot accidentally answer a denied
// request with success. A required recorder that could not record replaces it:
// the caller is refused either way, and the operator's own failure is the more
// useful one to surface.
//
// The code, never the refusal's own words. The prose the caller receives is
// deliberately vaguer than the code — a run in another tenant and a run that
// does not exist get the same "no such run", because confirming that a run
// exists elsewhere is what a caller in the wrong tenant must not learn — and
// the audit sink is not the caller.
func (s *FlowstateServer) auditDeny(ctx context.Context, rpc string, kind v1.AuditResourceKind, key string, code v1.AuditDenyCode, refusal error) error {
	if err := s.audit.Deny(ctx, s.auditSubject(ctx, rpc, kind, key), code); err != nil {
		return err
	}

	return refusal
}

// auditSubject reads the caller from the context through the same
// [FlowstateServer.identityFor] a run's own identity is built from, so the
// identity in an audit record is the identity the decision was actually made
// about rather than a second derivation of it.
func (s *FlowstateServer) auditSubject(ctx context.Context, rpc string, kind v1.AuditResourceKind, key string) audit.Subject {
	subject := audit.Subject{
		RPC:          rpc,
		Identity:     s.identityFor(ctx),
		ResourceKind: kind,
		ResourceKey:  key,
	}
	if principal, ok := auth.PrincipalFromContext(ctx); ok {
		subject.IssuerName = principal.IssuerName
		subject.Role = principal.Role
	}

	return subject
}
