package sdk

import (
	"context"

	flowstatev1 "github.com/picatz/flowstate/pkg/flowstate/v1"
)

// Caller is who invoked the task a plugin is executing, and which tenant that
// workload belongs to.
//
// The wire already carries both — [ExecuteRequest] names an identity and a
// namespace — so a plugin task always *could* see them. What was missing was a
// way to read them without widening [Task.Fn]'s signature, which every task in
// every plugin would then have had to accept whether it needed this or not.
type Caller struct {
	// Identity is who the workload acts as. It carries no credentials: it is
	// the same message the engine persists in workflow history, so nothing
	// about receiving it here is a new exposure.
	Identity *flowstatev1.WorkloadIdentity

	// Namespace is the tenant the run belongs to, established by the
	// authenticated caller rather than declared by the workflow.
	Namespace string
}

// callerKey is the context key [CallerFromContext] reads.
type callerKey struct{}

// contextWithCaller returns a context carrying who invoked this task and which
// tenant it belongs to, installed once per request before [Task.Fn] runs.
func contextWithCaller(ctx context.Context, identity *flowstatev1.WorkloadIdentity, namespace string) context.Context {
	return context.WithValue(ctx, callerKey{}, Caller{Identity: identity, Namespace: namespace})
}

// CallerFromContext returns who invoked the task running on ctx, and which
// tenant the run belongs to.
//
// Most tasks should not need this. A task's inputs are already resolved and
// scoped by the time they reach [Task.Fn] — including any secret named in
// [Task.SecretInputs], which the host resolves under the caller's identity
// before this task ever runs — so a task that only reads its inputs and calls
// its backend has no reason to look here at all.
//
// It exists for the task that must apply its own per-tenant policy on top of
// the engine's: one whose backend has its own authorization model keyed by who
// is asking, distinct from which secret was resolved. Namespace is present even
// when Identity is not: a single-tenant deployment with no identity provider
// still has a namespace, the empty one, exactly as [SecretRequest.Namespace]
// does for secret resolution.
//
// The bool reports whether a caller was found at all, which is false only for
// code that calls a task's Fn directly rather than through the engine's own
// dispatch — a test, typically. A request that reached this over the wire
// always has one, because the host installs it before calling Fn whether or
// not the underlying workflow authenticated: an absent identity is a fact about
// the deployment, not a missing context value.
func CallerFromContext(ctx context.Context) (Caller, bool) {
	caller, ok := ctx.Value(callerKey{}).(Caller)
	return caller, ok
}
