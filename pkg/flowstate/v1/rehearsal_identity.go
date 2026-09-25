package flowstatev1

import "context"

// The identity a local run rehearses as, and the one seam by which it reaches
// [Scope.identity].
//
// # What this fixes
//
// `flow run local --as-subject/--as-issuer/--as-namespace/--as-deployment/--as-claim`
// exists to "rehearse policy as" an identity, and it reached three of the six
// identity-aware surfaces: the secret-access policy and the credential broker
// (through [TaskRuntime.Identity]), the plugin caller (through
// [plugin.NewContextWithIdentity]), and `distinct_from_starter:` (through
// [NewPolicedLocalSignals]). It reached none of the three that read
// [Scope.identity] — the task-shape policy (#187), the egress policy's identity
// dimension (#240), and `run.identity` in expressions — because the local driver
// left that field unset.
//
// The consequence was rehearsal-stricter-than-production, which is the polarity
// invariant 3 exists to forbid twice over. `--task-policy` and `--egress-policy`
// are accepted by `flow run local` precisely so an author can rehearse under the
// rules a worker enforces; a rule reading `identity.namespace == "team-a"` then
// matched nothing locally, so the same file, the same flags and the same policy
// were refused on a laptop and permitted in production. An author who believes
// their rehearsal edits a correct rule until it is wrong.
//
// # Why carrying the identity does not make a local run look attested
//
// The property [Scope]'s local driver setup protects — a local run must never
// look like a server-attested one — is real, and this does not weaken it. It is
// held by three mechanisms, none of which is "the identity field is empty":
//
//   - [Scope.local] is true for every local run and false for every durable one,
//     and it is what `run.local` renders. Anything reading the scope can tell the
//     two apart by the field that exists to tell them apart.
//   - [WorkloadIdentity.Mode] carries the same host-established fact across the
//     plugin process boundary. [ProtoWorkloadIdentity] derives REHEARSAL only
//     from [auth.WorkloadIdentity]'s unexported local marker; the durable driver
//     overwrites serialized input with PRODUCTION at its activity boundary.
//     Absence from an older host remains unspecified/unknown rather than production.
//   - A minted credential's subject carries [auth]'s `_local` component, set by
//     an unexported field only [auth.NewLocalWorkloadIdentity] can set — so a
//     third-party relying party, which never sees `run.local`, still cannot be
//     made to accept a rehearsal as production. That path runs through
//     [TaskRuntime.Identity] and is untouched by this: nothing derives an
//     [auth.WorkloadIdentity] from a [Scope].
//   - A rehearsal signal sender is [RehearsalSignalSender], marked local, and the
//     durable path refuses it outright.
//
// Emptiness was a fourth mechanism enforcing the same property the other three
// already enforce, and it was the only one that cost an answer. The precedent
// for the trade is `signals:` itself: a `--signal-as-subject` rehearsal identity
// is admitted to a real policy surface and checked by the same function
// production checks it with, and stays honest because `sender.local` reads true.
// This is that, for the run's own starter. The same argument [LocalRunAddress]
// makes for the address — an honest sentinel rather than a blank, because "a
// blank reads as a field that failed to populate" — applies to identity, except
// that a blank identity does not merely read wrong, it *matches* differently.
//
// # One identity, rendered three ways
//
// The local driver has exactly one identity, built once by `flow run local` from
// its flags as an [auth.WorkloadIdentity]. It reaches the plugin boundary as a
// [WorkloadIdentity] through [ProtoWorkloadIdentity], the secret and credential
// surfaces as itself through [TaskRuntime], and now the scope through this
// context value — rendered from that one source rather than derived a second
// way. CLAUDE.md's account of every driver disagreement found so far is one
// value written down twice; this keeps it written down once.
//
// The durable driver does not read this. Its scope identity comes from
// [RunState.Identity], which the server established from an authenticated
// caller, and nothing in a worker's context ever carries this value.

// rehearsalIdentityContextKey is the unexported type the rehearsal identity is
// keyed under, so nothing outside this package can collide with it or set one
// by guessing a string key. Only the local driver's command sets it.
type rehearsalIdentityContextKey struct{}

// NewContextWithRehearsalIdentity returns a context carrying the identity a
// local run rehearses as, which [RunWithInputs] reads into [Scope.identity].
//
// Set by `flow run local`, `flow task run` and `flow mcp` from the same
// `--as-*` flags that build the run's [TaskRuntime] identity — one source, so
// the surfaces cannot disagree about who is calling. Never set by a worker or a
// server: a durable run's identity is attested, and it arrives on
// [RunState.Identity] rather than through a context value any in-process caller
// could supply.
//
// Passing nil is the same as not setting one at all: the scope keeps the empty
// identity, which is what a caller with no rehearsal identity to name — the
// bare [Run] entry point, `flow test`, this package's own tests — should get.
func NewContextWithRehearsalIdentity(ctx context.Context, identity *WorkloadIdentity) context.Context {
	return context.WithValue(ctx, rehearsalIdentityContextKey{}, identity)
}

// RehearsalIdentityFromContext returns the identity a local run rehearses as,
// or nil when none was set.
//
// nil is a complete answer rather than a missing one: it renders as every field
// empty, which is the honest reading for a local run whose starter nobody named
// — see [runRootValue], which documents the same nil-renders-empty rule for the
// expression surface.
func RehearsalIdentityFromContext(ctx context.Context) *WorkloadIdentity {
	identity, _ := ctx.Value(rehearsalIdentityContextKey{}).(*WorkloadIdentity)
	return identity
}
