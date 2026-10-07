package auth

import (
	"testing"

	"github.com/picatz/flowstate/pkg/flowstate/v1/principal"
)

// TestCredentialKeyDistinguishesTheActChain pins that the same subject acting
// alone, through one actor, or through the same actors in another order are
// different callers to a rule and never share a cached credential.
func TestCredentialKeyDistinguishesTheActChain(t *testing.T) {
	t.Parallel()

	a := principal.Actor{Issuer: "https://a.example", Subject: "a"}
	b := principal.Actor{Issuer: "https://b.example", Subject: "b"}
	identity := func(actors ...principal.Actor) WorkloadIdentity {
		return WorkloadIdentity{Subject: "s", Issuer: "i", Namespace: "n", Actors: actors}
	}

	keys := map[string]string{}
	for name, id := range map[string]WorkloadIdentity{
		"alone": identity(), "a": identity(a), "b": identity(b), "ab": identity(a, b), "ba": identity(b, a),
	} {
		key := credentialKey("target", "sub", id)
		if other, dup := keys[key]; dup {
			t.Fatalf("%q and %q share a credential cache key", name, other)
		}
		keys[key] = name
	}
}

// TestCredentialKeyDistinguishesWhatARuleCanTellApart pins that two identities a
// rule could treat differently never share a cached credential: a typed claim is
// not its text, and a claim, the kind and an action are not one another.
func TestCredentialKeyDistinguishesWhatARuleCanTellApart(t *testing.T) {
	t.Parallel()

	base := WorkloadIdentity{Subject: "s", Issuer: "i", Namespace: "n"}
	with := func(mutate func(*WorkloadIdentity)) WorkloadIdentity {
		identity := base
		mutate(&identity)

		return identity
	}

	cases := []struct {
		name string
		a, b WorkloadIdentity
	}{
		{
			name: "string true is not boolean true",
			a:    with(func(w *WorkloadIdentity) { w.Claims = map[string]any{"admin": "true"} }),
			b:    with(func(w *WorkloadIdentity) { w.Claims = map[string]any{"admin": true} }),
		},
		{
			name: "string 1 is not number 1",
			a:    with(func(w *WorkloadIdentity) { w.Claims = map[string]any{"n": "1"} }),
			b:    with(func(w *WorkloadIdentity) { w.Claims = map[string]any{"n": float64(1)} }),
		},
		{
			name: "a claim is not the kind and an action",
			a:    with(func(w *WorkloadIdentity) { w.Claims = map[string]any{"a": "b"} }),
			b:    with(func(w *WorkloadIdentity) { w.Kind, w.Actions = "a", []string{"b"} }),
		},
		{
			name: "kind is not an action",
			a:    with(func(w *WorkloadIdentity) { w.Kind = "human" }),
			b:    with(func(w *WorkloadIdentity) { w.Actions = []string{"human"} }),
		},
		{
			name: "one list claim is not two",
			a:    with(func(w *WorkloadIdentity) { w.Claims = map[string]any{"g": []any{"a b"}} }),
			b:    with(func(w *WorkloadIdentity) { w.Claims = map[string]any{"g": []any{"a", "b"}} }),
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			if credentialKey("target", "sub", tc.a) == credentialKey("target", "sub", tc.b) {
				t.Fatalf("two identities a rule can tell apart share a credential cache key")
			}
		})
	}

	first := with(func(w *WorkloadIdentity) { w.Claims = map[string]any{"admin": true, "g": []any{"a"}} })
	again := with(func(w *WorkloadIdentity) { w.Claims = map[string]any{"g": []any{"a"}, "admin": true} })
	if credentialKey("target", "sub", first) != credentialKey("target", "sub", again) {
		t.Fatal("the same identity must key the same credential however its claims were built")
	}
}
