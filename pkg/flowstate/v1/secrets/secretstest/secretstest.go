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
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"

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
// namespace, may return that fixture's value. A [secrets.Provider] that
// derives its scoping from a prefix or a path segment that is not
// collision-free — the exact shape both prior bugs took — fails here even
// though every single-tenant case, tested alone, would pass.
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
			secret, err := provider.Resolve(t.Context(), secrets.Request{Namespace: f.Namespace, Ref: f.Ref})
			require.NoError(t, err, "namespace %q could not resolve its own reference %q", f.Namespace, f.Ref)
			require.Equal(t, f.Value, secret.Reveal(), "namespace %q did not get its own value back", f.Namespace)
		})
	}

	for _, requester := range fixtures {
		for _, owner := range fixtures {
			if requester.Namespace == owner.Namespace {
				continue
			}

			t.Run(fmt.Sprintf("%s-cannot-reach-%s", requester.Namespace, owner.Namespace), func(t *testing.T) {
				secret, err := provider.Resolve(t.Context(), secrets.Request{Namespace: requester.Namespace, Ref: owner.Ref})
				if err == nil {
					require.NotEqual(t, owner.Value, secret.Reveal(),
						"namespace %q reached namespace %q's secret through reference %q",
						requester.Namespace, owner.Namespace, owner.Ref)
				}
			})
		}
	}
}
