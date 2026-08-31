package server

import (
	"context"

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
//     wrong.
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
// The returned error is non-nil only when a required recorder could not
// record, and every caller must return it: that is the fail-closed path, and
// dropping it converts a required sink into an advisory one.
func (s *FlowstateServer) auditAllow(ctx context.Context, rpc string, kind v1.AuditResourceKind, key string) error {
	return s.audit.Allow(ctx, s.auditSubject(ctx, rpc, kind, key))
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
