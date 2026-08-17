// Package secretstest is the conformance kit a [secrets.Provider] author runs
// against their own implementation.
//
// It is a separate package, on the model of net/http/httptest and
// testing/fstest, rather than a file inside secrets itself: every file here
// imports "testing" and testify, and secrets is imported by every production
// binary that resolves a secret, cmd/flow included. Folding this in there would
// hand the CLI binary a transitive dependency on a testing framework for a
// function it never calls. Nothing outside a test imports this package.
package secretstest

import (
	"context"
	"fmt"
	"testing"

	"github.com/picatz/flowstate/pkg/flowstate/v1/secrets"
)

// NamespaceFixture is one tenant's own secret, as [VerifyNamespaceIsolation]
// needs it: a namespace already provisioned against the [secrets.Provider]
// under test, a reference only that namespace should be able to resolve, and
// the value resolving it under that namespace should produce.
type NamespaceFixture struct {
	// Namespace is the tenant, already configured against the provider under
	// test (an environment prefix registered, a directory populated, and so
	// on — whatever that provider's own construction requires).
	Namespace string

	// Ref is a reference that resolves inside Namespace and nowhere else.
	Ref secrets.Ref

	// Value is what resolving Ref under Namespace must return.
	//
	// Value doubles as a canary: [VerifyNamespaceIsolation] detects a leak by
	// comparing a wrongly-scoped resolution's *value* against this field, so
	// every fixture's Value must be distinct from every other fixture's in
	// the same call. Two isolated tenants sharing a Value — a realistic case,
	// such as both using the same rotated credential — would make a
	// requester's own, correctly-isolated resolution equal another fixture's
	// Value by coincidence, which the check cannot tell apart from an actual
	// leak. [VerifyNamespaceIsolation] rejects a fixture list that violates
	// this before running any subtest, rather than risk a false failure.
	Value string

	// Collisions names further references, distinct from Ref, that a
	// requester might construct in an attempt to land on this fixture's
	// Value — shaped the way a naive prefix-or-path-concatenation scheme
	// would actually produce them, not merely Ref repeated.
	//
	// This is what the plain Ref check above cannot catch. Resolving the
	// same reference under someone else's namespace only ever proves a
	// provider isn't handing out a tenant's own key to a stranger who asks
	// for it by name; it says nothing about a stranger's *own* reference
	// happening to land in the tenant's storage slot. That is exactly the
	// shape the historical bug took (see [VerifyNamespaceIsolation]): the
	// leak was never "team-b asks for team-a's API_KEY", it was "the
	// default tenant asks for its own TEAM_A_API_KEY" and gets team-a's
	// API_KEY back, because $FLOWSTATE_SECRET_ + "TEAM_A_API_KEY" and
	// $FLOWSTATE_SECRET_TEAM_A_ + "API_KEY" are the same string. A fixture
	// whose Collisions field is empty is not exercising that shape at all.
	//
	// A collision only actually collides under one specific requester
	// namespace — "TEAM_A_API_KEY" only lands in team-a's slot when asked
	// from the empty/default namespace; "A_API_KEY" only does when asked
	// from a namespace literally called "team". Each [Collision] therefore
	// names its own requester namespace rather than being tried from every
	// other fixture in the list: trying "TEAM_A_API_KEY" from, say,
	// "team-b" proves nothing, because that pairing was never the attack in
	// the first place — it fails closed against both a naive provider and a
	// correct one, so a check that only ever tries that pairing cannot tell
	// the two apart.
	//
	// Shape each Ref like the provider's actual derivation scheme — the env
	// provider's collision candidate looks like "TEAM_A_API_KEY" or
	// "A_API_KEY"; the file provider's looks like a path segment crafted to
	// land in another tenant's directory. Each entry is checked from its
	// declared FromNamespace, with the same fail-closed rule as Ref: an
	// error is fine, this fixture's Value coming back is not.
	Collisions []Collision
}

// Collision is a requester-crafted reference [NamespaceFixture.Collisions]
// declares, bound to the one requester namespace that actually makes it
// collide with the owning fixture's secret.
//
// Binding matters because a collision is a property of *both* sides: the
// requester namespace and the reference together determine what a naive
// prefix-or-path-concatenation provider derives. Leaving the requester
// namespace implicit — trying a collision reference from every other
// namespace a test happens to define — can miss the one namespace that was
// ever going to reproduce the bug, and silently pass against a genuinely
// vulnerable provider. See [NamespaceFixture.Collisions].
type Collision struct {
	// FromNamespace is the requester namespace this reference must be tried
	// under. It need not be one of the namespaces any [NamespaceFixture] in
	// the list owns — the historical env bug's most damaging collision was
	// reachable from the *empty* namespace, which owns nothing of its own.
	FromNamespace string

	// Ref is the requester's own reference, shaped the way the provider's
	// actual derivation scheme would produce it — not the owner's Ref
	// repeated.
	Ref secrets.Ref
}

// Option configures [VerifyNamespaceIsolation].
type Option func(*options)

type options struct {
	unconfiguredNamespace string
}

// WithUnconfiguredNamespace overrides the sentinel namespace
// [VerifyNamespaceIsolation] uses to probe fallback behavior for a requester
// the provider was never configured for.
//
// The built-in sentinel ("secretstest-unconfigured-tenant") is only ever
// proven absent from the fixture list passed to this call — nothing proves it
// absent from the provider's own configuration, which [secrets.Provider] does
// not expose a way to inspect. A provider under test that happens to have a
// real tenant configured under that exact string would make the sentinel
// probe silently test an ordinary configured requester instead of an
// unconfigured one, which could either false-fail a correct provider or let a
// real fallback bug pass.
//
// Callers who can name a namespace their own provider setup genuinely never
// configures — because they built the provider and know what they didn't
// pass it — should supply it here instead of relying on the built-in
// sentinel.
func WithUnconfiguredNamespace(namespace string) Option {
	return func(o *options) { o.unconfiguredNamespace = namespace }
}

// VerifyNamespaceIsolation is the conformance check every [secrets.Provider]
// author should run against their own implementation, referenced from the
// interface's own doc comment.
//
// Give it a provider already configured for at least two tenants, each
// described by a [NamespaceFixture] naming a secret that tenant, and only that
// tenant, should be able to reach.
//
// It checks both directions, which is the point. This module's own history is
// why: env.go and file.go in the secrets package both shipped a namespace
// boundary that was present, checked, and covered by passing tests, and still
// leaked, because every existing test asserted only that a tenant reaches its
// own secret — see CLAUDE.md, "Test that A cannot reach B, not that A can
// reach A". This helper asserts that half too (each fixture must resolve
// under its own namespace), and then asserts the half that actually catches
// the bug class: no fixture's reference, resolved under any other fixture's
// namespace, may return that fixture's value — and none of its declared
// [NamespaceFixture.Collisions], each resolved under its own declared
// [Collision.FromNamespace], may either. A [secrets.Provider] that
// derives its scoping from a prefix or a path segment that is not
// collision-free — the exact shape both prior bugs took — fails here even
// though every single-tenant case, tested alone, would pass.
//
// The reference-repeated check and the collision check catch different bugs.
// The historical leak was never a requester asking for the owner's own
// reference — it was a requester's *own*, differently-spelled reference
// landing in the owner's storage slot because the provider concatenated
// prefix and name without a collision-free encoding. A fixture that leaves
// Collisions empty only proves the first, weaker property.
//
// A resolution that errors under the wrong namespace is fine; fail-closed is
// the correct answer to a request outside a namespace's configuration. What
// this checks is narrower and non-negotiable: an error is acceptable, another
// tenant's value is not.
func VerifyNamespaceIsolation(t *testing.T, provider secrets.Provider, fixtures []NamespaceFixture, opts ...Option) {
	t.Helper()

	if err := validateFixtures(fixtures); err != nil {
		t.Fatal(err)
	}

	o := options{unconfiguredNamespace: "secretstest-unconfigured-tenant"}
	for _, opt := range opts {
		opt(&o)
	}

	for _, f := range fixtures {
		t.Run("owns/"+f.Namespace, func(t *testing.T) {
			if err := checkOwnFixture(t.Context(), provider, f); err != nil {
				t.Fatal(err)
			}
		})
	}

	for _, requester := range fixtures {
		for _, owner := range fixtures {
			if requester.Namespace == owner.Namespace {
				continue
			}

			t.Run(fmt.Sprintf("%s-cannot-reach-%s", requester.Namespace, owner.Namespace), func(t *testing.T) {
				if err := checkNoCollision(t.Context(), provider, requester.Namespace, owner, owner.Ref); err != nil {
					t.Fatal(err)
				}
			})
		}
	}

	// The loop above only ever probes as a requester a namespace that owns a
	// fixture of its own. That leaves a real fallback uncaught: a provider
	// that correctly separates every *configured* tenant but silently falls
	// back to one tenant's storage for any namespace it was never told
	// about would pass every subtest above, because none of them ever asks
	// as a namespace with no fixture at all. That case can't be represented
	// as a NamespaceFixture — giving it one would make it "own" a fixture,
	// which is exactly the case the loop above already covers, and it would
	// fail the distinct-namespace precondition if it collided with an
	// existing one. So probe it explicitly: an unconfigured
	// requester namespace, guaranteed not to equal any fixture's Namespace,
	// asking for each owner's own plain Ref.
	//
	// This namespace is only proven absent from the fixtures below, not from
	// the provider's own configuration — see [WithUnconfiguredNamespace]. A
	// caller who can name a namespace their own provider setup genuinely
	// never configures should supply it via that option; the built-in
	// default assumes it is not coincidentally a real, configured tenant.
	unconfiguredNamespace := o.unconfiguredNamespace
	for _, f := range fixtures {
		if f.Namespace == unconfiguredNamespace {
			t.Fatalf("fixture list uses the unconfigured namespace %q as a real fixture's Namespace; "+
				"VerifyNamespaceIsolation needs this value to name no configured tenant "+
				"(pass a different one via WithUnconfiguredNamespace)", unconfiguredNamespace)
		}
	}

	for _, owner := range fixtures {
		t.Run(fmt.Sprintf("%s-cannot-reach-%s", unconfiguredNamespace, owner.Namespace), func(t *testing.T) {
			if err := checkNoCollision(t.Context(), provider, unconfiguredNamespace, owner, owner.Ref); err != nil {
				t.Fatal(err)
			}
		})
	}

	// Collisions are checked separately from the loop above, and from each
	// other fixture's Collisions: each one is tried only from the specific
	// requester namespace it declares, never from every namespace the
	// fixture list happens to define — see [Collision] for why substituting
	// an arbitrary requester does not construct the attack.
	for _, owner := range fixtures {
		for _, collision := range owner.Collisions {
			t.Run(fmt.Sprintf("%s-cannot-collide-into-%s", collision.FromNamespace, owner.Namespace), func(t *testing.T) {
				if err := checkNoCollision(t.Context(), provider, collision.FromNamespace, owner, collision.Ref); err != nil {
					t.Fatal(err)
				}
			})
		}
	}
}

// validateFixtures checks the preconditions [VerifyNamespaceIsolation] needs
// to actually prove isolation: at least two fixtures, naming at least two
// distinct namespace values, each with a Value distinct from every other
// fixture's.
//
// The distinct-namespace half exists because the count alone is not enough.
// Two fixtures that share a Namespace string pass the ownership subtests
// trivially — both are just the same namespace resolving its own ref — and
// the negative-direction loop's `requester.Namespace == owner.Namespace`
// guard then skips every pair, since every pair *is* that case. A provider
// that ignores Request.Namespace entirely would pass undetected.
//
// The distinct-value half exists because [checkNoCollision] detects a leak by
// comparing resolved *plaintext* against the owner's Value, not by asking the
// provider which storage entry it touched. If two fixtures legitimately
// share a Value — two tenants using the same rotated credential, say — then a
// requester resolving its own, correctly-isolated secret would coincidentally
// equal another fixture's Value, and the equality check would report a
// cross-tenant leak that never happened. Rejecting duplicate Values up front
// keeps that comparison honest: it turns Value into a canary, unique enough
// that seeing it back only ever means one thing.
//
// It holds no dependency on *testing.T for the same reason [checkOwnFixture]
// and [checkNoCollision] don't: this package's own meta-regression tests need
// to observe the failure without it propagating to every ancestor *testing.T
// the way a failed t.Run subtest — or a t.Fatalf on the *testing.T passed
// into this function — would.
func validateFixtures(fixtures []NamespaceFixture) error {
	if len(fixtures) < 2 {
		return fmt.Errorf("VerifyNamespaceIsolation needs at least two fixtures to prove isolation between namespaces; got %d", len(fixtures))
	}

	distinctNamespaces := make(map[string]struct{}, len(fixtures))
	for _, f := range fixtures {
		distinctNamespaces[f.Namespace] = struct{}{}
	}
	if len(distinctNamespaces) < 2 {
		return fmt.Errorf("VerifyNamespaceIsolation needs at least two fixtures naming distinct namespaces to prove isolation "+
			"between namespaces; got %d fixture(s) sharing %d namespace value(s) — two fixtures with the same "+
			"Namespace make every ownership subtest trivially pass and the negative-direction loop skip every pair, "+
			"so a provider that ignores Request.Namespace entirely would pass undetected", len(fixtures), len(distinctNamespaces))
	}

	valueOwners := make(map[string]string, len(fixtures)) // Value -> owning Namespace
	for _, f := range fixtures {
		if owner, ok := valueOwners[f.Value]; ok {
			return fmt.Errorf("VerifyNamespaceIsolation needs every fixture's Value to be distinct, because "+
				"the isolation check detects a leak by comparing resolved plaintext against it; namespace %q and "+
				"namespace %q share the value %q — if that is deliberate (e.g. both tenants use the same rotated "+
				"credential), give each fixture a distinct canary Value instead, since a shared one makes a "+
				"correctly-isolated resolution indistinguishable from an actual cross-tenant leak",
				owner, f.Namespace, f.Value)
		}
		valueOwners[f.Value] = f.Namespace
	}

	return nil
}

// checkOwnFixture resolves a fixture's own reference under its own namespace
// and reports an error if that fails or returns the wrong value.
//
// It holds no dependency on *testing.T so it can be called directly — by
// [VerifyNamespaceIsolation] from within a subtest, and by this package's own
// meta-regression test, which needs to observe a failure without that
// failure propagating to every ancestor *testing.T the way a failed t.Run
// subtest does.
func checkOwnFixture(ctx context.Context, provider secrets.Provider, f NamespaceFixture) error {
	secret, err := provider.Resolve(ctx, secrets.Request{Namespace: f.Namespace, Ref: f.Ref})
	if err != nil {
		return fmt.Errorf("namespace %q could not resolve its own reference %q: %w", f.Namespace, f.Ref, err)
	}
	if secret.Reveal() != f.Value {
		return fmt.Errorf("namespace %q did not get its own value back for reference %q", f.Namespace, f.Ref)
	}
	return nil
}

// checkNoCollision resolves ref under requesterNamespace and reports an error
// only if that succeeds and returns owner's value — an error from the
// provider is an acceptable, fail-closed answer, so it is never reported
// here. See [checkOwnFixture] for why this returns a plain error rather than
// taking a *testing.T.
//
// requesterNamespace is a plain string, not a [NamespaceFixture], because a
// collision's requester namespace need not own a fixture of its own — see
// [Collision.FromNamespace].
func checkNoCollision(ctx context.Context, provider secrets.Provider, requesterNamespace string, owner NamespaceFixture, ref secrets.Ref) error {
	secret, err := provider.Resolve(ctx, secrets.Request{Namespace: requesterNamespace, Ref: ref})
	if err != nil {
		return nil
	}
	if secret.Reveal() == owner.Value {
		return fmt.Errorf(
			"namespace %q reached namespace %q's secret through reference %q",
			requesterNamespace, owner.Namespace, ref,
		)
	}
	return nil
}
