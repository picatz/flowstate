package netpolicy

import "context"

// credentialsKey is the context key for a request's credential marker. It is an
// unexported empty struct type so no other package can collide with it or forge
// a value — the same shape as [identityKey].
type credentialsKey struct{}

// ContextWithCredentials returns a context marking whether a request made with it
// carries a worker-resolved credential — a bearer secret or a JIT federation
// target. A task sets this before issuing a request through the policy's client
// so that a rule naming `credentials` (#963) is evaluated against the fact the
// task itself already knows: the credential detector runs before either secret
// path is read, so the mark is available before the request is built, let alone
// sent.
//
// It is the one seam by which the fact enters this package, mirroring
// [ContextWithIdentity]: the caller renders the marker from its own
// task-specific detector, keeping this package free of any dependency on what a
// credential looks like in a given task's schema.
func ContextWithCredentials(ctx context.Context, present bool) context.Context {
	return context.WithValue(ctx, credentialsKey{}, present)
}

// credentialsFromContext returns the credential marker carried by ctx, or false
// — "no credential" — when none is present.
//
// false is the deliberate, fail-closed-compatible default for two audiences at
// once: a request made outside a Flowstate task (nothing ever marked the
// context) and a rule written before this attribute existed (which never
// mentions credentials and so never depended on the answer). Both read the
// absence the same way an absent [Identity] does — as the unremarkable case —
// rather than as an error, which keeps an old rule's meaning exactly what it
// was before this attribute existed.
func credentialsFromContext(ctx context.Context) bool {
	present, _ := ctx.Value(credentialsKey{}).(bool)
	return present
}
