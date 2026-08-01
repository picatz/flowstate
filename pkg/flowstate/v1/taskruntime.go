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
