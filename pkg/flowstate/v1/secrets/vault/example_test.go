package vault_test

import (
	"context"
	"fmt"
	"log"

	"github.com/picatz/flowstate/pkg/flowstate/v1/secrets"
	"github.com/picatz/flowstate/pkg/flowstate/v1/secrets/vault"
)

// Example shows the whole of what a worker does with this package: build one
// provider at startup, wrap it in a cache, and resolve inside the activity that
// needs the value.
//
// Nothing here changes for OpenBao. The address is the only thing that differs.
func Example() {
	// Built once, at startup. A misconfiguration — an unreachable CA bundle, a pod
	// with no projected service account token — fails here rather than in the first
	// workflow that needs a secret.
	backend, err := vault.NewProvider(
		"https://vault.example.com:8200",
		vault.WithKubernetesAuth("flowstate-worker"),
		vault.WithMount("kv"),
		vault.WithPathPrefix("flowstate"),
	)
	if err != nil {
		log.Fatal(err)
	}

	// The cache is what keeps a network round trip off every use of a secret, and
	// what bounds how stale a rotated one may be. The provider itself caches no
	// value.
	store, err := secrets.NewStore(secrets.NewCache(backend))
	if err != nil {
		log.Fatal(err)
	}

	// The namespace comes from the run's authenticated identity, which is what
	// makes it a tenant boundary rather than a convention. Here it stands in for
	// one.
	resolver, err := store.For(secrets.Namespace("team-a"))
	if err != nil {
		log.Fatal(err)
	}

	ref, err := secrets.ParseRef("vault:apps/api#token")
	if err != nil {
		log.Fatal(err)
	}

	// Inside the activity, and nowhere else. This reads
	// kv/data/flowstate/team-a/apps/api and takes its "token" field.
	secret, err := resolver.Resolve(context.Background(), ref)
	if err != nil {
		// Safe to surface and to record: it names the reference, never the value.
		log.Fatal(err)
	}

	// Keep the revealed value next to the one call that needs it.
	fmt.Println("Authorization: Bearer " + secret.Reveal())
}

// ExampleProvider_SecretPath shows where a reference lands, and why a namespace is
// a segment of the path rather than something applied afterwards: one reference is
// three different secrets in three tenants, and none of them is reachable from
// another.
func ExampleProvider_SecretPath() {
	backend, err := vault.NewProvider(
		"https://vault.example.com:8200",
		vault.WithToken("s.dev-token"),
		vault.WithPathPrefix("flowstate"),
	)
	if err != nil {
		log.Fatal(err)
	}

	for _, namespace := range []string{"team-a", "team-b", ""} {
		path, err := backend.SecretPath(namespace, "apps/api#token")
		if err != nil {
			log.Fatal(err)
		}

		fmt.Printf("%-8q %s\n", namespace, path)
	}

	// A reference cannot leave its namespace, whatever it is spelled as.
	if _, err := backend.SecretPath("team-a", "../team-b/apps/api#token"); err != nil {
		fmt.Println("refused:", err)
	}

	// Output:
	// "team-a" secret/data/flowstate/team-a/apps/api
	// "team-b" secret/data/flowstate/team-b/apps/api
	// ""       secret/data/flowstate/_default/apps/api
	// refused: invalid secret reference: "../team-b/apps/api" points outside its namespace
}
