package netpolicy

import (
	"context"

	"github.com/picatz/flowstate/pkg/flowstate/v1/principal"
)

// identityKey is the context key for a request's [principal.Caller]. It is an unexported
// empty struct type so no other package can collide with it or forge a value.
type identityKey struct{}

// ContextWithIdentity returns a context carrying the workload identity an egress
// rule should see for requests made with it. A task sets this before issuing a
// request through the policy's client so that a rule naming `identity.<field>` is
// evaluated against the run's attested caller.
//
// It is the one seam by which identity enters this package: the value is rendered
// from the run's WorkloadIdentity by the caller, keeping this package free of any
// dependency on how identity is established.
func ContextWithIdentity(ctx context.Context, id principal.Caller) context.Context {
	return context.WithValue(ctx, identityKey{}, id)
}

// identityFromContext returns the identity carried by ctx, or the zero identity —
// "no attested caller" — when none is present. The zero value is a deliberate
// answer rather than a sentinel: a rule is always evaluated against some identity,
// and an absent one reads as empty fields, which a tenant rule declines to match.
func identityFromContext(ctx context.Context) principal.Caller {
	id, _ := ctx.Value(identityKey{}).(principal.Caller)
	return id.Normalized()
}
