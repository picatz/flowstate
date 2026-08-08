package netpolicy

import "context"

// Identity is the workload identity an egress rule reads as `identity.<field>`.
//
// It is the same identity the other two operator-authored policy surfaces reason
// over — secret access (`auth`'s `workload`) and task shape (`v1`'s task-policy
// `identity`) — so a deployment can gate all three on one notion of who is
// running. The value comes from the run's single attested [WorkloadIdentity]; a
// caller renders it into this shape from that one source rather than deriving it a
// second way, which is what keeps the three surfaces from disagreeing about who is
// calling. netpolicy declares no dependency on the identity's origin, so the
// rendering — and the single source it reads — stay outside this package.
//
// The zero value is "no attested caller": every string empty and no claims, which
// is exactly what a local run or a scope that predates identity presents. A rule
// meaning "only this tenant" therefore both selects its tenant and, by not
// matching the zero value, denies a request that carries no identity at all — the
// fail-closed reading, since a request denied by every allow rule is denied.
//
// The fields are deliberately the tenant-identity subset, the same one the
// task-shape surface renders: `deployment`, the step reference, and the delegation
// chain answer "which installation ran this" or "what is running", not "who may
// reach what", and an egress rule wants the last of those.
type Identity struct {
	Subject   string            `cel:"subject"`
	Issuer    string            `cel:"issuer"`
	Namespace string            `cel:"namespace"`
	Claims    map[string]string `cel:"claims"`
}

// identityTypeName is how [Identity] is named in CEL, which appears in a type
// error when a rule misuses a field. [ext.NativeTypes] derives it from the type's
// Go directory, which for this package is also its declared name.
const identityTypeName = "netpolicy.Identity"

// normalized returns the identity a rule is evaluated against: the same fields,
// with claims guaranteed non-nil. CEL cannot index a null map, so a rule reading
// `identity.claims[...]` against an identity that carries none would error — and
// an errored rule denies — where the intent is for it simply not to match. An
// absent *key* still errors, which is the documented convention (`"k" in
// identity.claims` guards it); only the null map is smoothed here.
func (id Identity) normalized() Identity {
	if id.Claims == nil {
		id.Claims = map[string]string{}
	}
	return id
}

// identityKey is the context key for a request's [Identity]. It is an unexported
// empty struct type so no other package can collide with it or forge a value.
type identityKey struct{}

// ContextWithIdentity returns a context carrying the workload identity an egress
// rule should see for requests made with it. A task sets this before issuing a
// request through the policy's client so that a rule naming `identity.<field>` is
// evaluated against the run's attested caller.
//
// It is the one seam by which identity enters this package: the value is rendered
// from the run's [WorkloadIdentity] by the caller, keeping this package free of any
// dependency on how identity is established.
func ContextWithIdentity(ctx context.Context, id Identity) context.Context {
	return context.WithValue(ctx, identityKey{}, id)
}

// identityFromContext returns the identity carried by ctx, or the zero identity —
// "no attested caller" — when none is present. The zero value is a deliberate
// answer rather than a sentinel: a rule is always evaluated against some identity,
// and an absent one reads as empty fields, which a tenant rule declines to match.
func identityFromContext(ctx context.Context) Identity {
	id, _ := ctx.Value(identityKey{}).(Identity)
	return id.normalized()
}
