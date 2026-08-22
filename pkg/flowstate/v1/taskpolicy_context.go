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
// Called once per dispatch, above wherever a driver retries a failed
// attempt — [runStepWithPolicy] for the local driver, each activity entry
// point for the durable one (`engine/activities.go`) — never inside the
// retry loop itself. A dispatch's identity and task name do not change
// between retries of the same step, so evaluating this once is not an
// optimization so much as it is the accurate description of what the policy
// governs: one dispatch, one decision.
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
	err := TaskPolicyIn(ctx).Check(ctx, task, identity)
	if err == nil {
		return nil
	}

	if denied, ok := errors.AsType[*TaskPolicyDeniedError](err); ok {
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

	return NewTaskError(task, ErrorKindPolicyDenied, err)
}
