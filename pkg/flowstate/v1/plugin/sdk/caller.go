package sdk

import (
	"context"

	flowstatev1 "github.com/picatz/flowstate/pkg/flowstate/v1"
	"github.com/picatz/flowstate/pkg/flowstate/v1/netpolicy"
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

// Mode returns the execution mode the directly connected host established.
// Missing identities, older hosts that did not send the field, and enum values
// this SDK does not understand all return WORKLOAD_IDENTITY_MODE_UNSPECIFIED,
// whose semantic meaning is unknown.
//
// This is a fact, not an authorization grant. A task that avoids production
// side effects during rehearsals must positively require
// WORKLOAD_IDENTITY_MODE_PRODUCTION; testing only for "not rehearsal" would
// misclassify an older host's rehearsal as production.
func (c Caller) Mode() flowstatev1.WorkloadIdentityMode {
	mode := c.Identity.GetMode()
	switch mode {
	case flowstatev1.WorkloadIdentityMode_WORKLOAD_IDENTITY_MODE_PRODUCTION,
		flowstatev1.WorkloadIdentityMode_WORKLOAD_IDENTITY_MODE_REHEARSAL:
		return mode
	default:
		return flowstatev1.WorkloadIdentityMode_WORKLOAD_IDENTITY_MODE_UNSPECIFIED
	}
}

// callerKey is the context key [CallerFromContext] reads.
type callerKey struct{}

// contextWithCaller returns a context carrying who invoked this task and which
// tenant it belongs to, installed once per request before [Task.Fn] runs.
//
// It installs the same identity twice, in two spellings, because two different
// readers need it and only one of them is this package's. [CallerFromContext]
// reads the first for a task applying its own per-tenant rules. The second is
// [netpolicy]'s own context key, which is where the policy behind [HTTPClient]
// looks when it evaluates an `identity.*` rule — a different key in a different
// package, invisible from a task's point of view.
//
// Without it the documented pattern quietly under-enforces: a plugin that calls
// [HTTPClient] and passes its task context, exactly as PLUGINS.md teaches, gets
// every `identity.*` rule evaluated against the zero identity, so an operator's
// `deny: ['identity.namespace == "team-b"']` never fires for a team-b workload.
// It fails open and it fails silently: the request succeeds, and nothing on
// either side reports that a rule did not match because the value it names was
// never there. The first-party plugins that enforce on their own dial paths
// bridge this by hand today, which is the evidence that the generic path did
// not — a mechanism every caller has to remember is not a mechanism.
//
// Credentials are not set here, and that is deliberate. Whether a request
// carries a credential is the plugin's own knowledge about the request it is
// about to make, not something the SDK can infer from the fact that a task is
// running; see [netpolicy.ContextWithCredentials].
func contextWithCaller(ctx context.Context, identity *flowstatev1.WorkloadIdentity, namespace string) context.Context {
	ctx = context.WithValue(ctx, callerKey{}, Caller{Identity: identity, Namespace: namespace})

	return netpolicy.ContextWithIdentity(ctx, netpolicy.Identity{
		Subject:   identity.GetSubject(),
		Issuer:    identity.GetIssuer(),
		Namespace: identity.GetNamespace(),
		Claims:    identity.GetClaims(),
	})
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
