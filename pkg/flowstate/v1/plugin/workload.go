package plugin

import (
	"context"

	flowstatev1 "github.com/picatz/flowstate/pkg/flowstate/v1"
)

// identityKey is the context key carrying the running workload's identity.
type identityKey struct{}

// NewContextWithIdentity returns a context carrying the identity of the workload
// on whose behalf a plugin will be called.
//
// The protocol sends a plugin the tenant a request belongs to and who the
// workload acts as, so that a plugin serving several tenants can scope what it
// resolves and apply its own authorization. Neither reaches a task any other
// way: [flowstatev1.TaskFunc] takes inputs and a scope, so a worker that wants
// plugin tasks to receive an identity puts it here before executing a step.
//
// An absent identity is not an error. It is what a single-tenant, self-hosted
// deployment with no identity provider has, and the empty namespace is a tenant
// like any other rather than a wildcard — the same rule the secrets package
// applies. A plugin still gets a request; it simply says the workload acts as
// nobody in particular.
//
// The identity holds no credentials. It is the message the engine already
// persists in workflow history, which is durable and broadly readable, so
// nothing secret is in it and passing it across the plugin boundary adds no
// exposure.
func NewContextWithIdentity(ctx context.Context, identity *flowstatev1.WorkloadIdentity) context.Context {
	return context.WithValue(ctx, identityKey{}, identity)
}

// IdentityFromContext returns the identity [NewContextWithIdentity] carried, if
// any.
func IdentityFromContext(ctx context.Context) (*flowstatev1.WorkloadIdentity, bool) {
	identity, ok := ctx.Value(identityKey{}).(*flowstatev1.WorkloadIdentity)
	return identity, ok && identity != nil
}

// identityForNamespace returns the context's identity when it belongs to the
// namespace the call is being made in.
//
// A mismatch is dropped rather than forwarded. The namespace a resolution
// happens in is established from the authenticated caller and arrives through
// the request itself; an identity in the context claiming a different tenant
// means something upstream is inconsistent, and sending a plugin an identity
// from one tenant alongside a namespace from another invites it to authorize
// against the wrong one. Sending no identity at all makes the plugin fall back
// to the namespace, which is the answer that cannot cross a tenant boundary.
func identityForNamespace(ctx context.Context, namespace string) *flowstatev1.WorkloadIdentity {
	identity, ok := IdentityFromContext(ctx)
	if !ok {
		return nil
	}

	if identity.GetNamespace() != "" && identity.GetNamespace() != namespace {
		return nil
	}

	return identity
}
