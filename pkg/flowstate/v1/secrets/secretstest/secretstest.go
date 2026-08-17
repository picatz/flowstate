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
	Value string

	// Collisions names further references, distinct from Ref, that a
	// requester in some *other* namespace might construct in an attempt to
	// land on this fixture's Value — shaped the way a naive
	// prefix-or-path-concatenation scheme would actually produce them, not
	// merely Ref repeated.
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
	// Populate it with references shaped like the provider's actual
	// derivation scheme — the env provider's collision candidate looks like
	// "TEAM_A_API_KEY" or "A_API_KEY"; the file provider's looks like a path
	// segment crafted to land in another tenant's directory. Each entry is
	// checked against every namespace other than this fixture's own, with
	// the same fail-closed rule as Ref: an error is fine, this fixture's
	// Value coming back is not.
	Collisions []secrets.Ref
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
// the bug class: no fixture's reference — nor any of its declared
// [NamespaceFixture.Collisions] — resolved under any other fixture's
// namespace, may return that fixture's value. A [secrets.Provider] that
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
func VerifyNamespaceIsolation(t *testing.T, provider secrets.Provider, fixtures []NamespaceFixture) {
	t.Helper()

	if len(fixtures) < 2 {
		t.Fatalf("VerifyNamespaceIsolation needs at least two fixtures to prove isolation between namespaces; got %d", len(fixtures))
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
				if err := checkNoCollision(t.Context(), provider, requester, owner, owner.Ref); err != nil {
					t.Fatal(err)
				}
			})

			for _, collision := range owner.Collisions {
				t.Run(fmt.Sprintf("%s-cannot-collide-into-%s", requester.Namespace, owner.Namespace), func(t *testing.T) {
					if err := checkNoCollision(t.Context(), provider, requester, owner, collision); err != nil {
						t.Fatal(err)
					}
				})
			}
		}
	}
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

// checkNoCollision resolves ref under requester's namespace and reports an
// error only if that succeeds and returns owner's value — an error from the
// provider is an acceptable, fail-closed answer, so it is never reported
// here. See [checkOwnFixture] for why this returns a plain error rather than
// taking a *testing.T.
func checkNoCollision(ctx context.Context, provider secrets.Provider, requester, owner NamespaceFixture, ref secrets.Ref) error {
	secret, err := provider.Resolve(ctx, secrets.Request{Namespace: requester.Namespace, Ref: ref})
	if err != nil {
		return nil
	}
	if secret.Reveal() == owner.Value {
		return fmt.Errorf(
			"namespace %q reached namespace %q's secret through reference %q",
			requester.Namespace, owner.Namespace, ref,
		)
	}
	return nil
}
