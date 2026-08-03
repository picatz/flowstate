package secrets

import (
	"context"
	"strconv"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

// mustResolver binds a store to the empty namespace, which is what a single-tenant
// deployment resolves in. Namespace scoping has its own tests.
func mustResolver(t *testing.T, store *Store) Resolver {
	t.Helper()

	resolver, err := store.For(nil)
	require.NoError(t, err)

	return resolver
}

func Test_ParseRef(t *testing.T) {
	tests := []struct {
		name       string
		input      string
		wantScheme string
		wantName   string
		wantErr    string
	}{
		// Negative cases first: a malformed reference must fail while it is still
		// text, which is what makes a bad Flowfile a compile error.
		{
			name:    "no provider",
			input:   "API_KEY",
			wantErr: "has no provider",
		},
		{
			name:    "empty",
			input:   "",
			wantErr: "has no provider",
		},
		{
			name:    "empty provider",
			input:   ":API_KEY",
			wantErr: "provider must not be empty",
		},
		{
			name:    "empty name",
			input:   "env:",
			wantErr: "name must not be empty",
		},
		{
			name:    "only a colon",
			input:   ":",
			wantErr: "provider must not be empty",
		},
		{
			name:    "uppercase provider",
			input:   "ENV:API_KEY",
			wantErr: "may only contain lowercase letters",
		},
		{
			name:    "provider with an underscore",
			input:   "my_vault:key",
			wantErr: "may only contain lowercase letters",
		},
		{
			name:    "newline in the name",
			input:   "env:API_KEY\nnot-a-real-log-line",
			wantErr: "control character",
		},
		{
			name:    "carriage return in the name",
			input:   "env:API\rKEY",
			wantErr: "control character",
		},
		{
			name:    "null byte in the name",
			input:   "env:API\x00KEY",
			wantErr: "control character",
		},
		{
			name:    "provider too long",
			input:   strings.Repeat("a", MaxSchemeLen+1) + ":key",
			wantErr: "longer than",
		},
		{
			name:    "name too long",
			input:   "env:" + strings.Repeat("a", MaxNameLen+1),
			wantErr: "longer than",
		},

		{
			name:       "environment variable",
			input:      "env:API_KEY",
			wantScheme: "env",
			wantName:   "API_KEY",
		},
		{
			name:       "file",
			input:      "file:api-key",
			wantScheme: "file",
			wantName:   "api-key",
		},
		{
			name:       "nested file path",
			input:      "file:db/password",
			wantScheme: "file",
			wantName:   "db/password",
		},
		{
			name:       "a name may contain colons, so a vault path survives",
			input:      "vault:secret/data/app:field",
			wantScheme: "vault",
			wantName:   "secret/data/app:field",
		},
		{
			name:       "a provider may contain digits and dashes",
			input:      "gcp-sm2:projects/p/secrets/s/versions/1",
			wantScheme: "gcp-sm2",
			wantName:   "projects/p/secrets/s/versions/1",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			got, err := ParseRef(test.input)

			if test.wantErr != "" {
				require.ErrorIs(t, err, ErrInvalidRef)
				require.ErrorContains(t, err, test.wantErr)
				require.Nil(t, got, "a rejected reference must not be returned")
				return
			}

			require.NoError(t, err)
			require.Equal(t, test.wantScheme, got.GetScheme())
			require.Equal(t, test.wantName, got.GetName())

			// The text form round-trips, so a reference survives compilation and
			// the wire unchanged.
			require.Equal(t, test.input, RefString(got))

			reparsed, err := ParseRef(RefString(got))
			require.NoError(t, err)
			require.Equal(t, got, reparsed)
		})
	}
}

// A Ref is satisfied by anything exposing the schema's generated accessors. The
// compiled *flowstatev1.SecretRef is checked directly in the external test package,
// which can import the engine without creating a cycle; this keeps the in-package
// tests free of that import.
type stubRef struct {
	scheme string
	name   string
}

func (r *stubRef) GetScheme() string {
	if r != nil {
		return r.scheme
	}
	return ""
}

func (r *stubRef) GetName() string {
	if r != nil {
		return r.name
	}
	return ""
}

var _ Ref = (*stubRef)(nil)

func Test_Ref_nilSafety(t *testing.T) {
	store, err := NewStore(&stubProvider{
		scheme: "env",
		secret: NewSecret(NewRef("env", "API_KEY"), "v"),
	})
	require.NoError(t, err)

	t.Run("a typed-nil message is refused rather than panicking", func(t *testing.T) {
		// A generated getter is nil-receiver safe, so a nil message reads as empty
		// and fails validation instead of dereferencing.
		_, err := mustResolver(t, store).Resolve(t.Context(), (*stubRef)(nil))
		require.ErrorIs(t, err, ErrInvalidRef)
	})

	t.Run("a nil interface is refused rather than panicking", func(t *testing.T) {
		_, err := mustResolver(t, store).Resolve(t.Context(), nil)
		require.ErrorIs(t, err, ErrInvalidRef)
	})

	t.Run("two messages naming one secret share a cache entry", func(t *testing.T) {
		// The cache keys by text, not by identity: an interface holding a pointer
		// would otherwise give each message its own entry.
		provider := &countingProvider{value: "v"}
		cache := NewCache(provider)

		for range 3 {
			_, err := cache.Resolve(t.Context(), Request{Ref: &stubRef{scheme: "test", name: "same"}})
			require.NoError(t, err)
		}

		require.Equal(t, 1, provider.count(), "distinct messages naming one secret must share an entry")
		require.Equal(t, 1, cache.Len())
	})
}

func Test_ValidateRef(t *testing.T) {
	tests := []struct {
		name    string
		ref     Ref
		wantErr string
	}{
		{name: "nil reference", ref: nil, wantErr: "reference is missing"},
		{name: "no provider", ref: NewRef("", "X"), wantErr: "provider must not be empty"},
		{name: "no name", ref: NewRef("env", ""), wantErr: "name must not be empty"},
		{name: "bad provider", ref: NewRef("ENV", "X"), wantErr: "lowercase"},
		{name: "control character", ref: NewRef("env", "A\nB"), wantErr: "control character"},
		{name: "valid", ref: NewRef("env", "API_KEY")},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			err := ValidateRef(test.ref)

			if test.wantErr != "" {
				require.ErrorIs(t, err, ErrInvalidRef)
				require.ErrorContains(t, err, test.wantErr)
				return
			}

			require.NoError(t, err)
		})
	}
}

func Test_RefString(t *testing.T) {
	require.Equal(t, "env:API_KEY", RefString(NewRef("env", "API_KEY")))
	require.Empty(t, RefString(nil), "a nil reference renders as empty rather than panicking")
}

// stubProvider is a Provider for testing the Store's dispatch and its checks on
// what a provider returns.
type stubProvider struct {
	scheme        string
	secret        Secret
	err           error
	calls         int
	lastNamespace string
}

func (p *stubProvider) Scheme() string { return p.scheme }

func (p *stubProvider) Resolve(_ context.Context, req Request) (Secret, error) {
	p.calls++
	p.lastNamespace = req.Namespace
	if p.err != nil {
		return Secret{}, p.err
	}

	return p.secret, nil
}

func Test_NewStore(t *testing.T) {
	tests := []struct {
		name      string
		providers []Provider
		wantErr   string
	}{
		{
			name:      "nil provider",
			providers: []Provider{nil},
			wantErr:   "must not be nil",
		},
		{
			name:      "empty scheme",
			providers: []Provider{&stubProvider{scheme: ""}},
			wantErr:   "empty scheme",
		},
		{
			name:      "invalid scheme",
			providers: []Provider{&stubProvider{scheme: "Env"}},
			wantErr:   "may only contain lowercase letters",
		},
		{
			name: "duplicate scheme",
			providers: []Provider{
				&stubProvider{scheme: "env"},
				&stubProvider{scheme: "env"},
			},
			wantErr: "is already registered by",
		},
		{
			name:      "no providers is valid",
			providers: nil,
		},
		{
			name: "distinct schemes",
			providers: []Provider{
				&stubProvider{scheme: "env"},
				&stubProvider{scheme: "file"},
			},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			store, err := NewStore(test.providers...)

			if test.wantErr != "" {
				require.Nil(t, store)
				require.ErrorContains(t, err, test.wantErr)
				return
			}

			require.NoError(t, err)
			require.NotNil(t, store)
		})
	}
}

func Test_Store_Resolve(t *testing.T) {
	ref := NewRef("env", "API_KEY")

	tests := []struct {
		name     string
		provider *stubProvider
		ref      Ref
		check    func(t *testing.T, secret Secret, err error)
	}{
		{
			name:     "unknown scheme",
			provider: &stubProvider{scheme: "env"},
			ref:      NewRef("vault", "x"),
			check: func(t *testing.T, _ Secret, err error) {
				require.ErrorIs(t, err, ErrUnknownScheme)
				require.ErrorContains(t, err, "not configured on this worker")
				require.ErrorContains(t, err, "configured: env")
			},
		},
		{
			name:     "invalid reference is rejected before dispatch",
			provider: &stubProvider{scheme: "env"},
			ref:      NewRef("env", ""),
			check: func(t *testing.T, _ Secret, err error) {
				require.ErrorIs(t, err, ErrInvalidRef)
			},
		},
		{
			name: "a provider's error is passed through",
			provider: &stubProvider{
				scheme: "env",
				err:    &ResolveError{Ref: ref, Err: ErrNotFound},
			},
			ref: ref,
			check: func(t *testing.T, _ Secret, err error) {
				require.ErrorIs(t, err, ErrNotFound)
			},
		},
		{
			// A provider that returns neither a value nor an error would otherwise
			// hand the caller an empty credential. This is the check that holds for
			// provider implementations outside this package.
			name:     "a provider returning nothing is an error",
			provider: &stubProvider{scheme: "env"},
			ref:      ref,
			check: func(t *testing.T, _ Secret, err error) {
				require.ErrorIs(t, err, ErrEmpty)
				require.ErrorContains(t, err, "returned no value and no error")
			},
		},
		{
			name: "a provider returning another reference's secret is an error",
			provider: &stubProvider{
				scheme: "env",
				secret: NewSecret(NewRef("env", "OTHER"), "value"),
			},
			ref: ref,
			check: func(t *testing.T, _ Secret, err error) {
				require.ErrorContains(t, err, `returned a secret for "env:OTHER"`)
			},
		},
		{
			name: "a resolved secret is returned",
			provider: &stubProvider{
				scheme: "env",
				secret: NewSecret(ref, "the-value"),
			},
			ref: ref,
			check: func(t *testing.T, secret Secret, err error) {
				require.NoError(t, err)
				require.Equal(t, "the-value", secret.Reveal())
				require.Equal(t, ref, secret.Ref())
			},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			store, err := NewStore(test.provider)
			require.NoError(t, err)

			secret, err := mustResolver(t, store).Resolve(t.Context(), test.ref)
			test.check(t, secret, err)
		})
	}
}

func Test_Store_Schemes(t *testing.T) {
	t.Run("empty store", func(t *testing.T) {
		store, err := NewStore()
		require.NoError(t, err)
		require.Empty(t, store.Schemes())

		// An empty store refuses everything rather than resolving from somewhere
		// unexpected, which is the right configuration for a deployment that does
		// not permit secrets at all.
		_, err = mustResolver(t, store).Resolve(t.Context(), NewRef("env", "X"))
		require.ErrorIs(t, err, ErrUnknownScheme)
		require.ErrorContains(t, err, "configured: none")
	})

	t.Run("sorted", func(t *testing.T) {
		store, err := NewStore(
			&stubProvider{scheme: "vault"},
			&stubProvider{scheme: "env"},
			&stubProvider{scheme: "file"},
		)
		require.NoError(t, err)
		require.Equal(t, []string{"env", "file", "vault"}, store.Schemes())
	})
}

func Test_Store_Resolve_context(t *testing.T) {
	store, err := NewStore(&stubProvider{
		scheme: "env",
		secret: NewSecret(NewRef("env", "API_KEY"), "v"),
	})
	require.NoError(t, err)

	ref := NewRef("env", "API_KEY")

	t.Run("a cancelled context stops the lookup", func(t *testing.T) {
		// A lookup may block on a network-mounted directory or a vault round trip,
		// so an activity that has already been cancelled should not start one.
		ctx, cancel := context.WithCancel(t.Context())
		cancel()

		_, err := mustResolver(t, store).Resolve(ctx, ref)
		require.ErrorIs(t, err, context.Canceled)
	})

	t.Run("a nil context is refused rather than panicking", func(t *testing.T) {
		//lint:ignore SA1012 passing nil is the mistake under test; the refusal is the assertion
		_, err := mustResolver(t, store).Resolve(nil, ref)
		require.ErrorContains(t, err, "requires a context")
	})

	t.Run("a live context resolves", func(t *testing.T) {
		secret, err := mustResolver(t, store).Resolve(t.Context(), ref)
		require.NoError(t, err)
		require.Equal(t, "v", secret.Reveal())
	})
}

func Test_Store_concurrentUse(t *testing.T) {
	ref := NewRef("env", "API_KEY")

	t.Setenv("FLOWSTATE_SECRET_API_KEY", "concurrent-value")

	store, err := NewStore(mustEnvProvider(t))
	require.NoError(t, err)

	// One store is shared across every task execution on a worker, so resolving
	// from many goroutines at once must be safe. Run under -race.
	for i := range 32 {
		t.Run(strconv.Itoa(i), func(t *testing.T) {
			t.Parallel()

			secret, err := mustResolver(t, store).Resolve(t.Context(), ref)
			require.NoError(t, err)
			require.Equal(t, "concurrent-value", secret.Reveal())
		})
	}
}
