package flowstatev1

import (
	"context"
	"fmt"
	"net/http"

	"github.com/picatz/flowstate/pkg/flowstate/v1/auth"
	"github.com/picatz/flowstate/pkg/flowstate/v1/secrets"
)

// TaskRuntime is the worker-side authority available to one task activity.
// Static-secret and JIT-credential capabilities share the authenticated identity
// and exact execution position, so policy cannot accidentally evaluate two
// different accounts of who is acting.
type TaskRuntime struct {
	Store    *secrets.Store
	Policy   *auth.SecretPolicy
	Broker   *auth.Broker
	Identity auth.WorkloadIdentity
	Step     auth.StepRef
}

// AuthorizeCredential obtains a short-lived credential for target and applies it
// directly to req. Material moves broker-to-request inside the activity and is
// never returned to workflow code.
func AuthorizeCredential(ctx context.Context, req *http.Request, target string) error {
	runtime, ok := ctx.Value(secretRuntimeKey{}).(TaskRuntime)
	if !ok || runtime.Broker == nil {
		return fmt.Errorf("workload identity federation is not configured on this worker")
	}
	return runtime.Broker.Authorize(ctx, req, runtime.Identity, runtime.Step, target)
}

type secretRuntimeKey struct{}

// ContextWithTaskRuntime installs secret access for one task execution.
// References remain inert unless all four parts of the runtime are present.
func ContextWithTaskRuntime(ctx context.Context, runtime TaskRuntime) context.Context {
	return context.WithValue(ctx, secretRuntimeKey{}, runtime)
}

// ContextWithSecretStep derives the authority for a nested local task. It keeps
// the authenticated identity, store and policy and changes only the position the
// policy evaluates.
func ContextWithSecretStep(ctx context.Context, workflow, run, step string) context.Context {
	runtime, ok := ctx.Value(secretRuntimeKey{}).(TaskRuntime)
	if !ok {
		return ctx
	}
	runtime.Step = auth.StepRef{Workflow: workflow, Run: run, Step: step}
	return ContextWithTaskRuntime(ctx, runtime)
}

// TaskStepFromContext reports the id of the step the currently executing task
// was invoked for, when the engine recorded one on the context.
//
// The engine stamps the step id onto each node's context through
// [ContextWithSecretStep] before it runs the node (see runNodes), so any task
// runtime already carrying a [TaskRuntime] can read back which step it is
// serving. It is what lets `flow test` scope a stub to a step id rather than
// only to a task name, without threading a second, parallel channel of the same
// fact through the engine.
//
// It reports ("", false) when no step id is on the context, which is the honest
// answer for a compensation running off the run level context rather than a
// node's: an undo call is not "the step it undoes" running again, so a stub
// scoped to that step must not answer it.
func TaskStepFromContext(ctx context.Context) (string, bool) {
	runtime, ok := ctx.Value(secretRuntimeKey{}).(TaskRuntime)
	if !ok || runtime.Step.Step == "" {
		return "", false
	}
	return runtime.Step.Step, true
}

// ResolveSecret authorizes and resolves a reference from the current task's
// execution context. Authorization runs for every resolution, before the store is
// consulted, so a cache or provider can never turn a denied read into an allowed
// one.
func ResolveSecret(ctx context.Context, ref secrets.Ref) (secrets.Secret, error) {
	runtime, ok := ctx.Value(secretRuntimeKey{}).(TaskRuntime)
	if !ok || runtime.Store == nil || runtime.Policy == nil {
		return secrets.Secret{}, fmt.Errorf("secret access is not configured on this worker")
	}
	if err := runtime.Policy.Authorize(ctx, runtime.Identity, runtime.Step, ref); err != nil {
		return secrets.Secret{}, err
	}
	resolver, err := runtime.Store.For(secretIdentity{namespace: runtime.Identity.Namespace})
	if err != nil {
		return secrets.Secret{}, err
	}
	return resolver.Resolve(ctx, ref)
}

// A deliberately tiny adapter: the secret store needs only the namespace and
// must not gain access to the rest of the caller's identity or claims.
type secretIdentity struct{ namespace string }

func (i secretIdentity) GetNamespace() string { return i.namespace }

// ProtoWorkloadIdentity renders a [TaskRuntime]'s [auth.WorkloadIdentity] as the
// wire type [WorkloadIdentity] carries.
//
// It exists on this side of the boundary rather than as a method on
// auth.WorkloadIdentity because auth deliberately imports no other Flowstate
// package — see [auth.IdentitySource] for why — so the direction that needs the
// generated type has to do the converting. This is the local driver's half of
// the identity the durable driver already has natively: engine/runtime.go's
// activities receive *v1.WorkloadIdentity straight from RunState, while the
// local driver's TaskRuntime.Identity is an auth.WorkloadIdentity built from
// command-line flags, and both need to reach [plugin.NewContextWithIdentity]
// carrying the same shape.
//
// A zero identity converts to a non-nil, all-empty message rather than nil, so
// that a run with no identity still sends an explicitly empty caller across the
// plugin boundary — the negative shape [plugin.IdentityFromContext] and the
// plugin SDK are both written to expect, rather than a caller inventing one.
func ProtoWorkloadIdentity(identity auth.WorkloadIdentity) *WorkloadIdentity {
	return &WorkloadIdentity{
		Subject:    identity.Subject,
		Issuer:     identity.Issuer,
		Claims:     identity.Claims,
		Namespace:  identity.Namespace,
		Deployment: identity.Deployment,
	}
}
