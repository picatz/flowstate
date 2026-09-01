package flowstatev1

import (
	"context"
	"errors"
	"sync/atomic"

	"github.com/picatz/flowstate/pkg/flowstate/v1/metricschema"
)

// defaultTaskPolicy is the process-wide task-shape policy, installed once by
// `flow worker --task-policy` / `flow run local --task-policy` before polling
// or running begins — see cmd/flow's taskpolicy.go. nil (the zero value)
// means no policy is configured, [TaskPolicy.Check]'s zero case.
//
// A package-level global, not solely a context value, for the same reason
// [DefaultRegistry] is one: the durable driver's activities run inside
// Temporal's own machinery, which does not thread this process's
// context.Context values into every activity invocation the way an ordinary
// call does. A context override exists too ([NewContextWithTaskPolicy]), for
// the local driver and for tests that need a policy scoped to one call rather
// than the whole process — mirroring [NewContextWithRegistry] exactly, whose
// own doc states the identical split: "production never sets one" on the
// context; the global is what a real deployment configures.
var defaultTaskPolicy atomic.Pointer[TaskPolicy]

// SetDefaultTaskPolicy installs the process-wide task-shape policy consulted
// by every dispatch that does not carry its own via
// [NewContextWithTaskPolicy]. Passing nil clears it, restoring the zero
// case: no restriction.
//
// Called once, before a worker polls or a local run starts — exactly when
// [v1.DefaultRegistry] is mutated for egress policy (`egress.go`) — so a
// policy that failed to compile refuses the command instead of governing
// some steps and not others.
func SetDefaultTaskPolicy(policy *TaskPolicy) {
	defaultTaskPolicy.Store(policy)
}

// DefaultTaskPolicy returns the process-wide task-shape policy, or nil when
// none is configured. Exported alongside [SetDefaultTaskPolicy] so a test
// (or `flow test`'s per-case isolation) can save and restore it, the same
// way [Registry.Lookup] pairs with [Registry.Register] for the egress
// policy swap tests already perform.
func DefaultTaskPolicy() *TaskPolicy {
	return defaultTaskPolicy.Load()
}

// taskPolicyContextKey is the unexported type a task-policy context value is
// keyed under, so nothing outside this package can collide with it or read
// it by guessing a string key.
type taskPolicyContextKey struct{}

// dispatchAttemptContextKey is the unexported type a dispatch's attempt number
// is keyed under, alongside the policy this file already carries.
type dispatchAttemptContextKey struct{}

// NewContextWithDispatchAttempt returns a context saying which attempt at one
// dispatch is about to run, so [CheckTaskPolicy] can record the decision once
// per dispatch rather than once per attempt.
//
// The number comes from the substrate, which is the only thing that knows it:
// the durable driver reads Temporal's own activity info (engine's
// dispatchAttempt), because a retried activity is a *new invocation* of the
// same check — the policy is consulted again, correctly, but the dispatch it
// permits is the one already recorded. The local driver sets nothing, and the
// absent value reads as the first attempt, which is exactly right: it consults
// the policy once above its retry loop, so every check it makes is a first one
// (picatz/flowstate#1394).
//
// A fact about one invocation rather than about the deployment, so it travels
// the way [ObserveTaskAttempt]'s own attempt does — from the substrate, into
// the shared seam, as data.
func NewContextWithDispatchAttempt(ctx context.Context, attempt int) context.Context {
	return context.WithValue(ctx, dispatchAttemptContextKey{}, attempt)
}

// dispatchAttemptIn reports which attempt at this dispatch ctx describes,
// defaulting to the first.
//
// The default is what makes this safe to add to a seam with callers that know
// nothing about it: an unset context is a first attempt, which records, which
// is the behavior every caller had before this existed.
func dispatchAttemptIn(ctx context.Context) int {
	if attempt, ok := ctx.Value(dispatchAttemptContextKey{}).(int); ok && attempt > 0 {
		return attempt
	}

	return 1
}

// NewContextWithTaskPolicy returns a context carrying policy, consulted by
// [TaskPolicyIn] ahead of the process-wide default. For a run whose
// task-shape policy must not be (or cannot be) the process global — the
// local driver threading a policy through one call, or a test isolating its
// own policy from every other test's — mirroring [NewContextWithRegistry]
// for the identical reason.
func NewContextWithTaskPolicy(ctx context.Context, policy *TaskPolicy) context.Context {
	return context.WithValue(ctx, taskPolicyContextKey{}, policy)
}

// TaskPolicyFromContext returns the context-scoped task-shape policy, if
// ctx carries one. A policy explicitly set to nil on the context (rather than
// absent) is still "found" — ok is true — so a caller can force "no policy"
// for one call even while a process-wide default is installed; see
// [TaskPolicyIn], which is what a dispatch site actually calls.
func TaskPolicyFromContext(ctx context.Context) (policy *TaskPolicy, ok bool) {
	v := ctx.Value(taskPolicyContextKey{})
	if v == nil {
		return nil, false
	}
	policy, ok = v.(*TaskPolicy)
	return policy, ok
}

// TaskPolicyIn resolves the task-shape policy governing ctx: the
// context-scoped policy if [NewContextWithTaskPolicy] set one, otherwise the
// process-wide default. nil means unrestricted — [TaskPolicy.Check]'s zero
// case — which is what every dispatch sees until an operator configures
// otherwise.
func TaskPolicyIn(ctx context.Context) *TaskPolicy {
	if policy, ok := TaskPolicyFromContext(ctx); ok {
		return policy
	}
	return DefaultTaskPolicy()
}

// CheckTaskPolicy is the one function every task-dispatch call site reaches
// for: it resolves ctx's governing policy via [TaskPolicyIn] and, if task
// under identity is refused, returns a [*TaskError] classified
// [ErrorKindPolicyDenied] wrapping the [*TaskPolicyDeniedError] — the same
// classification egress and secret denials already carry, which is what
// makes [ClassifyError] mark it non-retryable rather than falling through to
// the [ErrorKindInternal] default an unclassified error would get. A
// denial retried is a denial repeated for no reason: the policy's answer
// does not change between attempts of the same dispatch.
//
// Called above wherever a driver retries a failed attempt —
// [runStepWithPolicy] for the local driver, each activity entry point for the
// durable one (`engine/activities.go`) — never inside a retry loop itself.
//
// The two drivers reach that position differently, and the difference is not
// cosmetic. The local driver checks once and then retries beneath the check.
// The durable driver's check is inside the activity, and Temporal's retry
// re-invokes the activity, so the policy is consulted again on every attempt —
// which is correct enforcement and would be a second *record* of one dispatch.
// [NewContextWithDispatchAttempt] is how the durable driver says which attempt
// this is, so the allow is written once per dispatch on both drivers while the
// policy still governs every attempt.
//
// local is [Scope.GetLocal] — true for any local-driver entry point's own
// rehearsal (`flow run local`, `flow test`, `flow task run`, ...), never
// for the durable driver (see engine/workflow.go's varsScope, "Never Local:
// the durable driver always has a server in front of it"). It reaches
// nothing here but [TaskPolicyDeniedError.Local]: the
// decision above — which policy governs, which rule matches, allow versus
// deny — is made by [TaskPolicy.Check] before this ever runs, entirely from
// task and identity. This parameter is set on the resulting error, if any,
// strictly *after* that decision, so it has no path to become the thing
// #652 warns about — a value that exists to be informational and ends up
// load-bearing. See [TestLocalOnlyChangesTheMessageNotTheDecision].
//
// identity is used twice below, and the second use is under the identical
// constraint: [TaskPolicy.Check] evaluates the rules against it, and then
// [TaskPolicyDeniedError.Identity] records a rendering of it for the
// message — set on the error strictly after the decision, never consulted
// by anything that decides.
func CheckTaskPolicy(ctx context.Context, task string, identity *WorkloadIdentity, local bool) error {
	rule, err := TaskPolicyIn(ctx).check(ctx, task, identity)

	// The audit subject is the same either way: this identity, this task, and
	// the rule that decided. Built once so an allow and a deny cannot describe
	// the same dispatch differently (picatz/flowstate#1379).
	subject := EnforcementSubject{
		Point:        AuditEnforcementPoint_AUDIT_ENFORCEMENT_POINT_TASK_DISPATCH,
		Identity:     identity,
		ResourceKind: AuditResourceKind_AUDIT_RESOURCE_KIND_TASK,
		ResourceKey:  task,
		Rule:         rule,
	}

	if err == nil {
		// Recorded before the task runs, which is what the write-ahead rule
		// asks of this seam: the record is written while the dispatch it
		// permits is still ahead of it. A required recorder that could not
		// write refuses the dispatch, which is the whole of "an action that
		// cannot be recorded does not happen".
		//
		// Once per dispatch rather than once per attempt, which is what makes
		// the two drivers' trails the same trail. The local driver consults
		// this policy above its retry loop, so a retried step checks once; the
		// durable driver's check lives *inside* the activity, which Temporal
		// re-invokes for every attempt, so without this a retried dispatch
		// wrote one allow per attempt durably and one in total locally — the
		// same run, two different trails (Codex, picatz/flowstate#1394).
		//
		// The policy is still evaluated on every attempt: what a later attempt
		// skips is the *record*, not the check, so an operator who tightens a
		// policy mid-run still has the next attempt refused — and refusals are
		// recorded whenever they happen, below. Nothing goes unrecorded either:
		// the dispatch this permits was recorded when it was first permitted,
		// and a retry is that same dispatch.
		if dispatchAttemptIn(ctx) > 1 {
			return nil
		}

		return auditEnforcementAllow(ctx, subject)
	}

	denied, isDecision := errors.AsType[*TaskPolicyDeniedError](err)
	if isDecision {
		denied.Local = local

		// Provenance, recorded here for the same reason and under the same
		// constraint as Local: after the decision, read by nothing but the
		// message. This is the one place both drivers pass through with the
		// identity in hand — [TaskPolicy.Check] has it, but builds its
		// denial several frames down in [taskPolicyRuleSet.evaluate], which
		// knows about rules and not about who they were evaluated for — so
		// recording it here is what makes a local and a durable denial
		// describe the identity in the same words.
		denied.Identity = describePolicyIdentity(identity)
	}

	// One refusal, counted once, from the check both drivers share — so
	// "everything is being refused" is a rate an operator can see rather than
	// a pattern in a log file. The task's *name* and the surface, never the
	// refusal's sentence: see [RecordPolicyDenial]. The driver is read from
	// the same `local` this function already receives, so the label cannot
	// disagree with the error's own Local field.
	driver := metricschema.DriverDurable
	if local {
		driver = metricschema.DriverLocal
	}
	RecordPolicyDenial(ctx, metricschema.SurfaceTaskDispatch, task, driver)

	refusal := NewTaskError(task, ErrorKindPolicyDenied, err)

	if !isDecision {
		// Not something the policy decided. [taskPolicyRuleFailure] returns a
		// cancelled or expired context as itself, and running out of time is
		// not a decision — a record saying the rules refused this dispatch
		// would be a sentence the trail must not hold. The error keeps the
		// classification it has always had; only the record is withheld.
		return refusal
	}

	// The metric counts refusals; the record says which identity was refused
	// which task by which rule. Both, because a rate cannot answer "why was
	// this dispatch refused" and a record cannot answer "how often".
	return auditEnforcementDeny(ctx, subject, taskPolicyDenyCode(denied.Reason), refusal)
}

// taskPolicyDenyCode maps a task-shape denial's own closed reason onto the
// audit schema's closed deny code.
//
// Both sets are closed and neither is derived from the other, so the mapping
// is written once, here, rather than at the seam: a reason added to
// [TaskPolicyReason] without a code arrives as UNSPECIFIED, which is visible
// in the trail rather than silently recorded as something it is not.
func taskPolicyDenyCode(reason TaskPolicyReason) AuditDenyCode {
	switch reason {
	case TaskPolicyReasonDenyRule:
		return AuditDenyCode_AUDIT_DENY_CODE_DENY_RULE
	case TaskPolicyReasonNoAllowRule:
		return AuditDenyCode_AUDIT_DENY_CODE_NO_ALLOW_RULE
	case TaskPolicyReasonRuleError:
		return AuditDenyCode_AUDIT_DENY_CODE_RULE_ERROR
	default:
		return AuditDenyCode_AUDIT_DENY_CODE_UNSPECIFIED
	}
}
