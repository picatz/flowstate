package flowstatev1

import (
	"context"
	"sync/atomic"
)

// The worker's audit seams (picatz/flowstate#1379).
//
// The control plane writes down every authorization decision it makes
// (pkg/flowstate/v1/server/audit.go). The worker is where the consequential
// decisions are: whether a task may dispatch, whether a secret may be read,
// whether a request may leave, whether a credential may be assumed. Until
// this, none of them were recorded, so a trail could answer "why was this RPC
// permitted" and could not answer "why did this dispatch, resolution or dial
// happen".
//
// # Why the recorder arrives as an interface
//
// The natural spelling would be for each seam to hold an *audit.Recorder.
// It cannot: [AuditRecord] is this package's generated type, so the audit
// package imports this one, and the dependency can only run that way. So this
// package declares the shape it needs and the audit package satisfies it —
// [audit.Recorder] has exactly these two methods — and cmd/flow, which imports
// both, installs one into the other.
//
// A nil auditor records nothing and errors on nothing. That is the local
// rehearsal's answer as well as the library default: `flow run local`,
// `flow test` and `flow task run` install none, deliberately, and the audit
// package's doc argues the zero case where it already argues the others.
//
// # Allow and deny, with what decided
//
// Both directions are recorded. #353's principle 2 is that evidence carries
// the rule and the evaluated facts, allow as well as deny, and a trail holding
// only refusals answers "what was blocked" while leaving "what was permitted,
// and by what" exactly where it started.
//
// The evaluated facts a rule read are in the record structurally: the attested
// identity the rule matched against, and the resource it named. The rule
// itself is in [EnforcementSubject.Rule] when one decided — see that field for
// the one thing that must never be copied into it.

// EnforcementSubject is what a worker-side enforcement decision was about, as
// the seam making it already knows it.
//
// The decision itself is deliberately absent, the way [audit.Subject] leaves
// out the action it derives: a caller says what was decided about, and the
// verb it calls says which way the decision went, so a seam cannot record an
// allow carrying a denial's code.
type EnforcementSubject struct {
	// Point is the seam that decided. Required: a decision that cannot say
	// which policy made it is not evidence of anything.
	Point AuditEnforcementPoint

	// Identity is the workload the policy was evaluated against, as this
	// deployment attested it — the same [WorkloadIdentity] the rule read.
	// Claims are removed before emission by the recorder, which is where that
	// rule belongs; a claim's value is not needed to say who a decision was
	// made about.
	Identity *WorkloadIdentity

	// ResourceKind and ResourceKey say what was addressed: the task
	// dispatched, the secret referenced, the destination dialed, or the
	// credential target asked for.
	ResourceKind AuditResourceKind
	ResourceKey  string

	// Rule is the operator's own policy rule that decided, verbatim, when a
	// single rule did. Empty otherwise — see the schema's own comment on
	// AuditRecord.rule for when that is.
	//
	// Only a rule that *matched* may be copied here. A rule that failed to
	// evaluate reports its failure as prose that quotes the CEL error, and a
	// CEL error can quote the data the rule was reading; that denial carries
	// AUDIT_DENY_CODE_RULE_ERROR and no rule text at all. Nothing else a
	// policy's denial carries — a target, a detail, a message — is admitted
	// here either.
	Rule string
}

// EnforcementAuditor records one worker-side enforcement decision.
//
// [github.com/picatz/flowstate/pkg/flowstate/v1/audit.Recorder] implements it.
// The error is non-nil only when a required recorder could not record, and a
// seam must return it: that is the whole of "an action that cannot be recorded
// does not happen".
type EnforcementAuditor interface {
	// EnforcementAllow records that a policy permitted the subject.
	EnforcementAllow(ctx context.Context, subject EnforcementSubject) error

	// EnforcementDeny records that a policy refused it, under a code from the
	// schema's closed set.
	EnforcementDeny(ctx context.Context, subject EnforcementSubject, code AuditDenyCode) error
}

// defaultEnforcementAuditor is the process-wide auditor, installed once by
// `flow worker` before it polls. nil — the zero value — records nothing.
//
// A package-level global rather than only a context value, for the same reason
// [defaultTaskPolicy] is one: the durable driver's activities run inside
// Temporal's own machinery, which does not thread this process's context
// values into an activity invocation. The context override below exists for
// the same second reason it does there — a test that needs an auditor scoped
// to one call rather than to the whole process.
var defaultEnforcementAuditor atomic.Pointer[EnforcementAuditor]

// SetDefaultEnforcementAuditor installs the process-wide auditor every
// enforcement seam records to. Passing nil clears it, restoring the zero case:
// nothing is recorded.
//
// Called once, before a worker polls — where `flow worker` already installs
// the egress and task-shape policies whose decisions this records — so a
// worker cannot record some dispatches and not others.
func SetDefaultEnforcementAuditor(auditor EnforcementAuditor) {
	if auditor == nil {
		defaultEnforcementAuditor.Store(nil)
		return
	}
	defaultEnforcementAuditor.Store(&auditor)
}

// DefaultEnforcementAuditor returns the process-wide auditor, or nil when none
// is installed. Exported alongside [SetDefaultEnforcementAuditor] so a test can
// save and restore it, exactly as [DefaultTaskPolicy] pairs with its setter.
func DefaultEnforcementAuditor() EnforcementAuditor {
	if auditor := defaultEnforcementAuditor.Load(); auditor != nil {
		return *auditor
	}
	return nil
}

// enforcementAuditorContextKey is the unexported type the context-scoped
// auditor is keyed under, so nothing outside this package can collide with it.
type enforcementAuditorContextKey struct{}

// NewContextWithEnforcementAuditor returns a context whose enforcement seams
// record to auditor, ahead of the process-wide default. Mirrors
// [NewContextWithTaskPolicy] for the identical reason.
func NewContextWithEnforcementAuditor(ctx context.Context, auditor EnforcementAuditor) context.Context {
	return context.WithValue(ctx, enforcementAuditorContextKey{}, auditor)
}

// EnforcementAuditorIn resolves the auditor governing ctx: the context-scoped
// one if [NewContextWithEnforcementAuditor] set one, otherwise the
// process-wide default. nil means nothing is recorded.
func EnforcementAuditorIn(ctx context.Context) EnforcementAuditor {
	if auditor, ok := ctx.Value(enforcementAuditorContextKey{}).(EnforcementAuditor); ok {
		return auditor
	}
	return DefaultEnforcementAuditor()
}

// auditEnforcementAllow records a decision that permitted the subject, and
// returns non-nil only when a required recorder could not record it.
//
// Every caller must return that error: a required sink whose failure is
// swallowed is an advisory sink wearing the word "required", and the action
// the record was about would then happen unrecorded.
func auditEnforcementAllow(ctx context.Context, subject EnforcementSubject) error {
	auditor := EnforcementAuditorIn(ctx)
	if auditor == nil {
		return nil
	}
	return auditor.EnforcementAllow(ctx, subject)
}

// auditEnforcementDeny records a refusal and returns the refusal to hand back,
// mirroring server/audit.go's auditDeny so the two halves of one trail behave
// the same way.
//
// refusal is returned unchanged in the ordinary case, so a seam reads
// `return auditEnforcementDeny(...)` and cannot answer a denied request with
// success. A required recorder that could not record replaces it: the caller
// is refused either way, and the operator's own failure is the more useful one
// to surface.
func auditEnforcementDeny(ctx context.Context, subject EnforcementSubject, code AuditDenyCode, refusal error) error {
	auditor := EnforcementAuditorIn(ctx)
	if auditor == nil {
		return refusal
	}

	if err := auditor.EnforcementDeny(ctx, subject, code); err != nil {
		return err
	}

	return refusal
}
