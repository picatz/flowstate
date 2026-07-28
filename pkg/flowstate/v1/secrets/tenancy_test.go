package secrets

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

// identity stands in for the run identity a namespace is read from. The real one
// is the generated message, which satisfies [NamespaceProvider] through the same
// accessor.
type identity struct{ namespace string }

func (i *identity) GetNamespace() string {
	if i != nil {
		return i.namespace
	}
	return ""
}

func Test_ValidateNamespace(t *testing.T) {
	tests := []struct {
		name    string
		in      string
		wantErr string
	}{
		// Negative cases first. A namespace becomes a path segment and part of an
		// environment variable name, so anything that could escape either is refused
		// even though it arrives from an authenticated identity.
		{name: "path traversal", in: "../other", wantErr: "may only contain"},
		{name: "slash", in: "team/a", wantErr: "may only contain"},
		{name: "backslash", in: `team\a`, wantErr: "may only contain"},
		{name: "dot", in: "team.a", wantErr: "may only contain"},
		{name: "newline", in: "team\na", wantErr: "may only contain"},
		{name: "null byte", in: "team\x00a", wantErr: "may only contain"},
		{name: "uppercase", in: "TeamA", wantErr: "may only contain"},
		{name: "underscore", in: "team_a", wantErr: "may only contain"},
		{name: "leading dash", in: "-team", wantErr: "may not start with a dash"},
		{name: "too long", in: strings.Repeat("a", MaxNamespaceLen+1), wantErr: "longer than"},

		{name: "empty is the default tenant", in: ""},
		{name: "simple", in: "team-a"},
		{name: "digits", in: "team2"},
		{name: "all digits", in: "42"},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			err := ValidateNamespace(test.in)

			if test.wantErr != "" {
				require.ErrorIs(t, err, ErrNamespace)
				require.ErrorContains(t, err, test.wantErr)
				return
			}

			require.NoError(t, err)
		})
	}
}

func Test_Store_For(t *testing.T) {
	newStore := func(t *testing.T, opts ...StoreOption) *Store {
		t.Helper()

		registry := NewRegistry()
		require.NoError(t, registry.Register(&stubProvider{
			scheme: "env",
			secret: NewSecret(NewRef("env", "API_KEY"), "v"),
		}))

		store, err := NewStoreFromRegistry(registry, opts...)
		require.NoError(t, err)

		return store
	}

	t.Run("a malformed namespace is refused", func(t *testing.T) {
		_, err := newStore(t).For(&identity{namespace: "../other"})
		require.ErrorIs(t, err, ErrNamespace)
	})

	t.Run("a nil identity yields the default tenant", func(t *testing.T) {
		// Invariant 8: the engine has to work with no identity provider at all.
		resolver, err := newStore(t).For(nil)
		require.NoError(t, err)
		require.NotNil(t, resolver)
	})

	t.Run("a typed-nil identity yields the default tenant", func(t *testing.T) {
		// The generated accessor is nil-receiver safe, so a run with no identity
		// reads as the empty namespace rather than panicking.
		resolver, err := newStore(t).For((*identity)(nil))
		require.NoError(t, err)
		require.NotNil(t, resolver)
	})

	t.Run("a required namespace refuses the default tenant", func(t *testing.T) {
		// A multi-tenant deployment wants a lost identity to fail rather than fall
		// back to a shared tenant.
		_, err := newStore(t, WithRequiredNamespace()).For(nil)
		require.ErrorIs(t, err, ErrNamespace)
		require.ErrorContains(t, err, "requires a namespace")

		resolver, err := newStore(t, WithRequiredNamespace()).For(&identity{namespace: "team-a"})
		require.NoError(t, err)
		require.NotNil(t, resolver)
	})

	t.Run("the namespace reaches the provider", func(t *testing.T) {
		registry := NewRegistry()
		provider := &stubProvider{scheme: "env", secret: NewSecret(NewRef("env", "API_KEY"), "v")}
		require.NoError(t, registry.Register(provider))

		store, err := NewStoreFromRegistry(registry)
		require.NoError(t, err)

		resolver, err := store.For(&identity{namespace: "team-a"})
		require.NoError(t, err)

		_, err = resolver.Resolve(t.Context(), NewRef("env", "API_KEY"))
		require.NoError(t, err)
		require.Equal(t, "team-a", provider.lastNamespace,
			"a provider that never sees the namespace cannot scope by it")
	})

	t.Run("Namespace can stand in for an identity", func(t *testing.T) {
		resolver, err := newStore(t).For(Namespace("team-a"))
		require.NoError(t, err)
		require.NotNil(t, resolver)
	})
}

// Test_tenancy_isolation is the end-to-end check that two tenants sharing a worker
// cannot reach each other's secrets, through the providers that ship in this
// package.
func Test_tenancy_isolation(t *testing.T) {
	t.Run("environment variables refuse a namespace they are not configured for", func(t *testing.T) {
		// Fail-closed: a derived prefix cannot be made collision-free in a flat
		// environment, so an unmapped namespace is refused rather than guessed at.
		t.Setenv("FLOWSTATE_SECRET_TEAM_A_API_KEY", "team-a-value")

		store, err := NewStore(mustEnvProvider(t))
		require.NoError(t, err)

		resolver, err := store.For(Namespace("team-a"))
		require.NoError(t, err)

		_, err = resolver.Resolve(t.Context(), NewRef("env", "API_KEY"))
		require.ErrorIs(t, err, ErrNamespace)
		require.ErrorContains(t, err, "no prefix configured")
	})

	t.Run("environment variables with configured per-namespace prefixes", func(t *testing.T) {
		t.Setenv("TEAM_A_SECRET_API_KEY", "team-a-value")
		t.Setenv("TEAM_B_SECRET_API_KEY", "team-b-value")
		t.Setenv("FLOWSTATE_SECRET_API_KEY", "default-value")

		provider, err := NewEnvProvider(WithEnvNamespaces(map[string]string{
			"team-a": "TEAM_A_SECRET_",
			"team-b": "TEAM_B_SECRET_",
		}))
		require.NoError(t, err)

		store, err := NewStore(provider)
		require.NoError(t, err)

		ref := NewRef("env", "API_KEY")

		for _, test := range []struct{ namespace, want string }{
			{namespace: "team-a", want: "team-a-value"},
			{namespace: "team-b", want: "team-b-value"},
			{namespace: "", want: "default-value"},
		} {
			resolver, err := store.For(Namespace(test.namespace))
			require.NoError(t, err)

			secret, err := resolver.Resolve(t.Context(), ref)
			require.NoError(t, err)
			require.Equal(t, test.want, secret.Reveal(),
				"namespace %q read the wrong tenant's secret", test.namespace)
		}
	})

	t.Run("no tenant can name another's environment variable", func(t *testing.T) {
		// The bug this pins: with a derived prefix, the default tenant reading
		// "TEAM_A_API_KEY" and namespace "team" reading "A_API_KEY" both resolved
		// $FLOWSTATE_SECRET_TEAM_A_API_KEY, which is namespace "team-a"'s secret.
		t.Setenv("TEAM_A_SECRET_API_KEY", "team-a-value")
		t.Setenv("FLOWSTATE_SECRET_API_KEY", "default-value")

		provider, err := NewEnvProvider(WithEnvNamespaces(map[string]string{
			"team-a": "TEAM_A_SECRET_",
		}))
		require.NoError(t, err)

		store, err := NewStore(provider)
		require.NoError(t, err)

		def, err := store.For(nil)
		require.NoError(t, err)

		for _, name := range []string{"TEAM_A_SECRET_API_KEY", "TEAM_A_API_KEY"} {
			secret, err := def.Resolve(t.Context(), NewRef("env", name))
			if err == nil {
				require.NotEqual(t, "team-a-value", secret.Reveal(),
					"the default tenant reached team-a's secret by naming %q", name)
			}
		}
	})

	t.Run("overlapping prefixes are refused at construction", func(t *testing.T) {
		// One prefix being a prefix of another reintroduces exactly the collision
		// the mapping exists to prevent, so it is caught when configured.
		_, err := NewEnvProvider(WithEnvNamespaces(map[string]string{
			"team-a": "FLOWSTATE_SECRET_TEAM_A_",
		}))
		require.ErrorContains(t, err, "could name the other's variables")
	})

	t.Run("files refuse a namespace unless namespaced", func(t *testing.T) {
		dir := secretDir(t, map[string]string{"api-key": "default-value"})

		provider, err := NewFileProvider(dir)
		require.NoError(t, err)
		t.Cleanup(func() { require.NoError(t, provider.Close()) })

		store, err := NewStore(provider)
		require.NoError(t, err)

		resolver, err := store.For(Namespace("team-a"))
		require.NoError(t, err)

		_, err = resolver.Resolve(t.Context(), NewRef("file", "api-key"))
		require.ErrorIs(t, err, ErrNamespace)
		require.ErrorContains(t, err, "not namespaced")
	})

	t.Run("files", func(t *testing.T) {
		dir := secretDir(t, map[string]string{
			"team-a/api-key":   "team-a-value",
			"team-b/api-key":   "team-b-value",
			"_default/api-key": "default-value",
		})

		provider, err := NewFileProvider(dir, WithFileNamespaced())
		require.NoError(t, err)
		t.Cleanup(func() { require.NoError(t, provider.Close()) })

		store, err := NewStore(provider)
		require.NoError(t, err)

		ref := NewRef("file", "api-key")

		for _, test := range []struct{ namespace, want string }{
			{namespace: "team-a", want: "team-a-value"},
			{namespace: "team-b", want: "team-b-value"},
			{namespace: "", want: "default-value"},
		} {
			resolver, err := store.For(Namespace(test.namespace))
			require.NoError(t, err)

			secret, err := resolver.Resolve(t.Context(), ref)
			require.NoError(t, err)
			require.Equal(t, test.want, secret.Reveal(),
				"namespace %q read the wrong tenant's secret", test.namespace)
		}
	})

	t.Run("the default tenant cannot name another tenant's directory", func(t *testing.T) {
		// Every tenant gets a segment, including the default one. Without that, the
		// default tenant would read <dir>/team-a/api-key just by naming it, since a
		// reference may contain a slash.
		dir := secretDir(t, map[string]string{
			"team-a/api-key":   "team-a-value",
			"_default/api-key": "default-value",
		})

		provider, err := NewFileProvider(dir, WithFileNamespaced())
		require.NoError(t, err)
		t.Cleanup(func() { require.NoError(t, provider.Close()) })

		store, err := NewStore(provider)
		require.NoError(t, err)

		def, err := store.For(nil)
		require.NoError(t, err)

		secret, err := def.Resolve(t.Context(), NewRef("file", "team-a/api-key"))
		if err == nil {
			require.NotEqual(t, "team-a-value", secret.Reveal(),
				"the default tenant reached team-a's secret by naming its directory")
		}
	})

	t.Run("a reference cannot climb out of its namespace", func(t *testing.T) {
		dir := secretDir(t, map[string]string{
			"team-a/api-key": "team-a-value",
			"team-b/api-key": "team-b-value",
		})

		provider, err := NewFileProvider(dir, WithFileNamespaced())
		require.NoError(t, err)
		t.Cleanup(func() { require.NoError(t, provider.Close()) })

		store, err := NewStore(provider)
		require.NoError(t, err)

		resolver, err := store.For(Namespace("team-a"))
		require.NoError(t, err)

		// The namespace is a directory, so the obvious attempt is to escape it with
		// a reference name. The name is cleaned before the namespace is joined, so
		// there is nothing left to climb with.
		for _, name := range []string{"../team-b/api-key", "..%2fteam-b/api-key", "./../team-b/api-key"} {
			_, err := resolver.Resolve(t.Context(), NewRef("file", name))
			require.Error(t, err, "reference %q reached another tenant", name)
			require.NotContains(t, errText(err), "team-b-value")
		}
	})
}

// Test_cacheKey_isTenantSafe covers the cache key, which is where a tenancy leak
// would be invisible: the wrong key returns the right-looking value.
func Test_cacheKey_isTenantSafe(t *testing.T) {
	t.Run("the same reference in two namespaces is two entries", func(t *testing.T) {
		provider := &countingProvider{value: "v"}
		cache := NewCache(provider)

		ref := NewRef("test", "api-key")

		for _, namespace := range []string{"team-a", "team-b"} {
			_, err := cache.Resolve(t.Context(), Request{Namespace: namespace, Ref: ref})
			require.NoError(t, err)
		}

		require.Equal(t, 2, provider.count(), "one tenant's cached value was served to the other")
		require.Equal(t, 2, cache.Len())
	})

	t.Run("the same namespace and reference is one entry", func(t *testing.T) {
		provider := &countingProvider{value: "v"}
		cache := NewCache(provider)

		for range 3 {
			_, err := cache.Resolve(t.Context(), Request{Namespace: "team-a", Ref: NewRef("test", "api-key")})
			require.NoError(t, err)
		}

		require.Equal(t, 1, provider.count())
	})

	t.Run("namespace and reference cannot be spelled two ways", func(t *testing.T) {
		// Without length prefixes, namespace "a" with reference "b:c" and namespace
		// "a:b" with reference "c" would concatenate to the same key. Namespaces
		// cannot contain a colon today, so this pins the encoding rather than a
		// reachable bug.
		first := cacheKey(Request{Namespace: "a", Ref: NewRef("b", "c")})
		second := cacheKey(Request{Namespace: "a:b", Ref: NewRef("", "c")})

		require.NotEqual(t, first, second)
	})

	t.Run("Forget drops one tenant's entry only", func(t *testing.T) {
		provider := &countingProvider{value: "v"}
		cache := NewCache(provider)

		ref := NewRef("test", "api-key")

		for _, namespace := range []string{"team-a", "team-b"} {
			_, err := cache.Resolve(t.Context(), Request{Namespace: namespace, Ref: ref})
			require.NoError(t, err)
		}
		require.Equal(t, 2, cache.Len())

		cache.Forget(Request{Namespace: "team-a", Ref: ref})
		require.Equal(t, 1, cache.Len(), "forgetting one tenant's secret must not drop another's")
	})
}

// errText renders an error for a containment assertion.
func errText(err error) string {
	if err == nil {
		return ""
	}
	return err.Error()
}

func Test_Registry(t *testing.T) {
	t.Run("a nil provider is refused", func(t *testing.T) {
		require.ErrorContains(t, NewRegistry().Register(nil), "must not be nil")
	})

	t.Run("an empty scheme is refused", func(t *testing.T) {
		require.ErrorContains(t, NewRegistry().Register(&stubProvider{}), "empty scheme")
	})

	t.Run("a malformed scheme is refused", func(t *testing.T) {
		require.ErrorContains(t, NewRegistry().Register(&stubProvider{scheme: "Env"}), "may only contain")
	})

	t.Run("a duplicate is refused rather than silently replacing", func(t *testing.T) {
		// Whichever registered last would otherwise answer every reference for that
		// scheme, which is a configuration mistake with a security consequence.
		registry := NewRegistry()
		require.NoError(t, registry.Register(&stubProvider{scheme: "env"}))

		err := registry.Register(&stubProvider{scheme: "env"})
		require.ErrorContains(t, err, "already registered")
		require.Equal(t, 1, registry.Len())
	})

	t.Run("registration is enumerable", func(t *testing.T) {
		registry := NewRegistry()
		for _, scheme := range []string{"vault", "env", "file"} {
			require.NoError(t, registry.Register(&stubProvider{scheme: scheme}))
		}

		require.Equal(t, []string{"env", "file", "vault"}, registry.Schemes())
		require.Len(t, registry.All(), 3)
		require.Equal(t, 3, registry.Len())
	})

	t.Run("MustRegister panics on a duplicate", func(t *testing.T) {
		registry := NewRegistry()
		registry.MustRegister(&stubProvider{scheme: "env"})

		require.Panics(t, func() { registry.MustRegister(&stubProvider{scheme: "env"}) })
	})

	t.Run("a store takes a snapshot", func(t *testing.T) {
		// What a running worker resolves must not change under it.
		registry := NewRegistry()
		require.NoError(t, registry.Register(&stubProvider{scheme: "env"}))

		store, err := NewStoreFromRegistry(registry)
		require.NoError(t, err)
		require.Equal(t, []string{"env"}, store.Schemes())

		require.NoError(t, registry.Register(&stubProvider{scheme: "vault"}))
		require.Equal(t, []string{"env"}, store.Schemes(), "the store must not see a later registration")
	})

	t.Run("an unregistered scheme names what is registered", func(t *testing.T) {
		registry := NewRegistry()
		require.NoError(t, registry.Register(&stubProvider{scheme: "env"}))

		store, err := NewStoreFromRegistry(registry)
		require.NoError(t, err)

		resolver, err := store.For(nil)
		require.NoError(t, err)

		_, err = resolver.Resolve(t.Context(), NewRef("vault", "x"))
		require.ErrorIs(t, err, ErrUnknownScheme)
		require.ErrorContains(t, err, "configured: env")
	})
}

func Test_ValidateScheme(t *testing.T) {
	tests := []struct {
		name    string
		in      string
		wantErr string
	}{
		{name: "empty", in: "", wantErr: "must not be empty"},
		{name: "uppercase", in: "Env", wantErr: "may only contain"},
		{name: "underscore", in: "my_vault", wantErr: "may only contain"},
		{name: "slash", in: "a/b", wantErr: "may only contain"},
		{name: "too long", in: strings.Repeat("a", MaxSchemeLen+1), wantErr: "longer than"},
		{name: "simple", in: "vault"},
		{name: "dashes and digits", in: "gcp-sm2"},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			err := ValidateScheme(test.in)

			if test.wantErr != "" {
				require.ErrorIs(t, err, ErrInvalidRef)
				require.ErrorContains(t, err, test.wantErr)
				return
			}

			require.NoError(t, err)
		})
	}

	t.Run("it agrees with what the registry accepts", func(t *testing.T) {
		// A provider validating its own scheme must reach the same answer the
		// registry will, or it passes its own check and fails registration.
		for _, scheme := range []string{"", "Env", "my_vault", "vault", "gcp-sm2"} {
			viaRegistry := NewRegistry().Register(&stubProvider{scheme: scheme})
			viaExported := ValidateScheme(scheme)

			require.Equal(t, viaExported == nil, viaRegistry == nil,
				"scheme %q: exported check and registry disagree", scheme)
		}
	})
}

func Test_Retryable(t *testing.T) {
	tests := []struct {
		name string
		err  error
		want bool
	}{
		{name: "unavailable is transient", err: ErrUnavailable, want: true},
		{name: "not found is permanent", err: ErrNotFound},
		{name: "empty is permanent", err: ErrEmpty},
		{name: "permission is permanent", err: ErrPermission},
		{name: "invalid reference is permanent", err: ErrInvalidRef},
		{name: "unknown scheme is permanent", err: ErrUnknownScheme},
		{name: "namespace is permanent", err: ErrNamespace},
		{name: "nil is permanent", err: nil},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			require.Equal(t, test.want, Retryable(test.err))

			if test.err != nil {
				// Classification has to survive the wrapping every provider does.
				wrapped := &ResolveError{Ref: NewRef("env", "X"), Err: test.err}
				require.Equal(t, test.want, Retryable(wrapped))
			}
		})
	}
}

// Test_tenancy_isolation_localProviders is the negative direction for the two
// providers the isolation suite never covered.
//
// CLAUDE.md's rule is that an isolation test asserting each party reaches its own
// resource is a functionality test wearing a security test's clothes, and the env
// and file providers were fixed after exactly that gap was probed. The 1Password
// and keychain providers were never probed, and one of them had the same defect.
//
// The op provider mapped a namespace straight onto a vault name. Every character
// legal in a vault name is legal in a namespace, so a tenant whose namespace
// equalled the configured default vault — a team slug, a service-account name,
// whatever an operator passed to WithOnePasswordVault — read the untenanted
// tenant's entire vault. Nothing about the request looked wrong, because nothing
// about it was wrong.
func Test_tenancy_isolation_localProviders(t *testing.T) {
	t.Run("a 1Password namespace cannot name the default tenant's vault", func(t *testing.T) {
		// The collision, written as the attack: the operator's default vault is
		// "flowstate", and a tenant is called "flowstate" too. ValidateNamespace
		// permits it, because lowercase letters are legal in a namespace.
		runner := &fakeRunner{out: []byte("default-tenant-secret\n")}

		provider, err := NewOnePasswordProvider(
			withOnePasswordRunner(runner),
			WithOnePasswordVault("flowstate"),
			WithOnePasswordNamespaced(),
		)
		require.NoError(t, err)

		store, err := NewStore(provider)
		require.NoError(t, err)

		resolver, err := store.For(Namespace("flowstate"))
		require.NoError(t, err)

		_, err = resolver.Resolve(t.Context(), NewRef("op", "api#password"))
		require.NoError(t, err, "the fixture should resolve; what matters is which vault it read")

		// The unnamespaced tenant reads a vault no namespace can spell, so the two
		// cannot land in the same place however the namespace is chosen.
		require.NotEmpty(t, runner.calls)
		argv := strings.Join(runner.calls[0], " ")
		require.Contains(t, argv, "op://flowstate/api/password",
			"a namespaced tenant should read the vault named after it")

		defaultRunner := &fakeRunner{out: []byte("default-tenant-secret\n")}
		defaultProvider, err := NewOnePasswordProvider(
			withOnePasswordRunner(defaultRunner),
			WithOnePasswordVault("flowstate"),
			WithOnePasswordNamespaced(),
		)
		require.NoError(t, err)

		defaultStore, err := NewStore(defaultProvider)
		require.NoError(t, err)

		defaultResolver, err := defaultStore.For(Namespace(""))
		require.NoError(t, err)

		_, err = defaultResolver.Resolve(t.Context(), NewRef("op", "api#password"))
		require.NoError(t, err)

		require.NotEmpty(t, defaultRunner.calls)
		defaultArgv := strings.Join(defaultRunner.calls[0], " ")
		require.Contains(t, defaultArgv, "op://"+DefaultOnePasswordNamespaceVault+"/api/password",
			"the unnamespaced tenant must read a vault a namespace cannot name")

		require.NotEqual(t, argv, defaultArgv,
			"a tenant named after the configured vault read the default tenant's secrets")
	})

	t.Run("an unnamespaced 1Password provider refuses a namespaced run", func(t *testing.T) {
		// Fail closed rather than serving the default vault: a worker configured
		// for one tenant must not become multi-tenant because an identity arrived
		// carrying a namespace.
		provider, err := NewOnePasswordProvider(withOnePasswordRunner(&fakeRunner{}))
		require.NoError(t, err)

		store, err := NewStore(provider)
		require.NoError(t, err)

		resolver, err := store.For(Namespace("team-a"))
		require.NoError(t, err)

		_, err = resolver.Resolve(t.Context(), NewRef("op", "api#password"))
		require.ErrorIs(t, err, ErrNamespace)
		require.ErrorContains(t, err, "WithOnePasswordNamespaced")
	})

	t.Run("an unnamespaced keychain provider refuses a namespaced run", func(t *testing.T) {
		provider, err := NewKeychainProvider(withKeychainRunner(&fakeRunner{}))
		require.NoError(t, err)

		store, err := NewStore(provider)
		require.NoError(t, err)

		resolver, err := store.For(Namespace("team-a"))
		require.NoError(t, err)

		_, err = resolver.Resolve(t.Context(), NewRef("keychain", "api-key"))
		require.ErrorIs(t, err, ErrNamespace)
		require.ErrorContains(t, err, "WithKeychainNamespaced")
	})

	t.Run("a keychain namespace cannot forge another tenant's service", func(t *testing.T) {
		// Safe here for a reason worth pinning: the separator is "/", which
		// ValidateNamespace forbids, so no namespace can spell another's service.
		// That is a property of the separator and the validator together, and it
		// would be lost by anyone "simplifying" either one.
		require.Error(t, ValidateNamespace("flowstate/team-a"),
			"a namespace containing the keychain separator was accepted, which makes the "+
				"service mapping ambiguous")
	})
}
