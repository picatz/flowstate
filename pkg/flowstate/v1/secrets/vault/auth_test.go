package vault

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/picatz/flowstate/pkg/flowstate/v1/secrets"
	"github.com/stretchr/testify/require"
)

// projectedJWT is what the tests stand in for a pod's service account token with.
// It must never appear in an error: it is a credential, like the client token it is
// exchanged for.
const projectedJWT = "projected-jwt-1"

// newKubernetesProvider serves handler as a vault and returns a provider that
// authenticates against it with the Kubernetes auth method, plus the path of the
// file holding the service account token.
func newKubernetesProvider(t *testing.T, handler http.Handler, opts ...Option) (*Provider, string) {
	t.Helper()

	server := httptest.NewServer(handler)
	t.Cleanup(server.Close)

	jwtPath := filepath.Join(t.TempDir(), "token")
	require.NoError(t, os.WriteFile(jwtPath, []byte(projectedJWT), 0o600))

	options := []Option{
		WithKubernetesAuth("flowstate-worker"),
		WithKubernetesJWTPath(jwtPath),
	}

	provider, err := NewProvider(server.URL, append(options, opts...)...)
	require.NoError(t, err)

	return provider, jwtPath
}

// Test_Provider_login covers what the login endpoint can answer with. A login
// failure has to be classified as carefully as a read failure: it is the failure a
// worker sees when its role binding is wrong, and retrying that forever is as bad
// as giving up on a sealed vault.
func Test_Provider_login(t *testing.T) {
	tests := []struct {
		name      string
		status    int
		body      string
		wantIs    error
		wantErr   string
		retryable bool
	}{
		{
			name:    "a refused service account token",
			status:  http.StatusForbidden,
			body:    `{"errors":["permission denied"]}`,
			wantIs:  secrets.ErrPermission,
			wantErr: "refused the Kubernetes login for role",
		},
		{
			name:    "a role that does not exist",
			status:  http.StatusBadRequest,
			body:    `{"errors":["invalid role name \"flowstate-worker\""]}`,
			wantIs:  secrets.ErrPermission,
			wantErr: "bound service account names",
		},
		{
			name:    "an auth method that is not enabled",
			status:  http.StatusNotFound,
			body:    `{"errors":["no handler for route \"auth/kubernetes/login\""]}`,
			wantIs:  secrets.ErrPermission,
			wantErr: `no auth method at "auth/kubernetes/login"`,
		},
		{
			name:      "a vault that is sealed",
			status:    http.StatusServiceUnavailable,
			body:      `{"errors":["Vault is sealed"]}`,
			wantIs:    secrets.ErrUnavailable,
			wantErr:   "sealed",
			retryable: true,
		},
		{
			name:      "a vault that is not answering properly",
			status:    http.StatusBadGateway,
			body:      `<html>gateway error</html>`,
			wantIs:    secrets.ErrUnavailable,
			retryable: true,
		},
		{
			name:    "a login answered with something that is not JSON",
			status:  http.StatusOK,
			body:    `<html>a proxy got in the way</html>`,
			wantErr: "not JSON, at byte",
		},
		{
			name:    "a login answered with no auth block",
			status:  http.StatusOK,
			body:    `{"data":null}`,
			wantErr: "no client token",
		},
		{
			name:    "a login answered with an empty token",
			status:  http.StatusOK,
			body:    `{"auth":{"client_token":"","lease_duration":60}}`,
			wantErr: "no client token",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			provider, _ := newKubernetesProvider(t, jsonHandler(test.status, test.body))

			ref := secrets.NewRef(provider.Scheme(), "apps/api#token")

			secret, err := provider.Resolve(t.Context(), secrets.Request{Namespace: "team-a", Ref: ref})
			require.True(t, secret.IsZero())
			require.Error(t, err)

			if test.wantIs != nil {
				require.ErrorIs(t, err, test.wantIs)
			}

			if test.wantErr != "" {
				require.ErrorContains(t, err, test.wantErr)
			}

			require.Equal(t, test.retryable, secrets.Retryable(err))

			// The service account token is a credential: it goes in one request body
			// and appears nowhere else, however the login went.
			require.NotContains(t, err.Error(), projectedJWT)
		})
	}

	t.Run("a service account token that has gone missing is transient", func(t *testing.T) {
		// The kubelet rewrites the projected token as it rotates, so a worker that
		// treated a failed read of it as permanent would need a restart to recover
		// from something that fixes itself.
		provider, jwtPath := newKubernetesProvider(t, jsonHandler(http.StatusOK, `{}`))
		require.NoError(t, os.Remove(jwtPath))

		ref := secrets.NewRef(provider.Scheme(), "apps/api#token")

		_, err := provider.Resolve(t.Context(), secrets.Request{Namespace: "team-a", Ref: ref})
		require.ErrorIs(t, err, secrets.ErrUnavailable)
		require.ErrorContains(t, err, "reading the Kubernetes service account token")
		require.True(t, secrets.Retryable(err))
	})
}

func Test_Provider_kubernetesAuth(t *testing.T) {
	t.Run("the login carries the role and the projected token", func(t *testing.T) {
		vault := newFakeVault(t)
		provider, _ := vault.kubernetesProvider(t)

		vault.put("secret/data/team-a/apps/api", map[string]any{"token": "abc123"})

		ref := secrets.NewRef(provider.Scheme(), "apps/api#token")

		secret, err := provider.Resolve(t.Context(), secrets.Request{Namespace: "team-a", Ref: ref})
		require.NoError(t, err)
		require.True(t, secret.EqualString("abc123"))

		require.Equal(t, []string{projectedJWT}, vault.presentedJWTs())

		// The read used the token the login returned, not the JWT.
		reads := vault.readsServed()
		require.Len(t, reads, 1)
		require.Equal(t, "issued-token-1", reads[0].token)
	})

	t.Run("one token serves many resolutions", func(t *testing.T) {
		vault := newFakeVault(t)
		provider, _ := vault.kubernetesProvider(t)

		vault.put("secret/data/team-a/apps/api", map[string]any{"token": "abc123"})
		vault.put("secret/data/team-a/db/primary", map[string]any{"password": "hunter2"})

		for range 3 {
			for _, name := range []string{"apps/api#token", "db/primary#password"} {
				ref := secrets.NewRef(provider.Scheme(), name)

				_, err := provider.Resolve(t.Context(), secrets.Request{Namespace: "team-a", Ref: ref})
				require.NoError(t, err)
			}
		}

		require.Equal(t, 1, vault.loginCount(), "a cached token is the point of caching it")

		for _, read := range vault.readsServed() {
			require.Equal(t, "issued-token-1", read.token)
		}
	})

	t.Run("waiting to authenticate honors the caller's deadline", func(t *testing.T) {
		// Logins are serialized, so a caller can arrive while another is in flight.
		// It must not be held past its own deadline by somebody else's round trip.
		vault := newFakeVault(t)
		vault.loginDelay = 2 * time.Second

		provider, _ := vault.kubernetesProvider(t)
		vault.put("secret/data/team-a/apps/api", map[string]any{"token": "abc123"})

		ref := secrets.NewRef(provider.Scheme(), "apps/api#token")

		first := make(chan error, 1)
		go func() {
			_, err := provider.Resolve(context.Background(), secrets.Request{Namespace: "team-a", Ref: ref})
			first <- err
		}()

		// The fake records a login before it delays, so this waits until the login
		// slot is genuinely held rather than guessing with a sleep.
		require.Eventually(t, func() bool { return vault.loginCount() == 1 }, time.Second, time.Millisecond)

		ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
		defer cancel()

		_, err := provider.Resolve(ctx, secrets.Request{Namespace: "team-a", Ref: ref})
		require.ErrorIs(t, err, context.DeadlineExceeded)
		require.ErrorContains(t, err, "waiting to authenticate")

		require.NoError(t, <-first)
	})
}

func Test_Provider_tokenRenewal(t *testing.T) {
	tests := []struct {
		name string
		// lease is what the vault reports as the token's lease duration.
		lease time.Duration
		opts  []Option
		// early is an amount of time that must not trigger a new login, and late
		// one that must.
		early time.Duration
		late  time.Duration
	}{
		{
			name:  "a token is replaced before its lease ends",
			lease: 10 * time.Minute,
			opts:  []Option{WithRenewBefore(time.Minute)},
			early: 8*time.Minute + 59*time.Second,
			late:  9 * time.Minute,
		},
		{
			name: "the margin is capped at half the lease",
			// A margin longer than the lease would otherwise mean logging in for
			// every read, since the token would be due for renewal on arrival.
			lease: 30 * time.Second,
			opts:  []Option{WithRenewBefore(time.Minute)},
			early: 14 * time.Second,
			late:  15 * time.Second,
		},
		{
			name:  "no margin still renews at the lease's end",
			lease: time.Minute,
			opts:  []Option{WithRenewBefore(0)},
			early: 59 * time.Second,
			late:  time.Minute,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			vault := newFakeVault(t)
			vault.lease = test.lease

			provider, _ := vault.kubernetesProvider(t, test.opts...)

			clock := newClock()
			provider.now = clock.Now

			vault.put("secret/data/team-a/apps/api", map[string]any{"token": "abc123"})

			ref := secrets.NewRef(provider.Scheme(), "apps/api#token")

			resolve := func(t *testing.T) {
				t.Helper()

				_, err := provider.Resolve(t.Context(), secrets.Request{Namespace: "team-a", Ref: ref})
				require.NoError(t, err)
			}

			resolve(t)
			require.Equal(t, 1, vault.loginCount())

			clock.advance(test.early)
			resolve(t)
			require.Equal(t, 1, vault.loginCount(), "the token still had life in it")

			clock.advance(test.late - test.early)
			resolve(t)
			require.Equal(t, 2, vault.loginCount(), "the token was due for replacement")

			reads := vault.readsServed()
			require.Equal(t, "issued-token-1", reads[0].token)
			require.Equal(t, "issued-token-2", reads[len(reads)-1].token)
		})
	}

	t.Run("a lease of zero is a token that does not expire", func(t *testing.T) {
		// A root token reports no lease duration. Logging in again on a timer that
		// never fires would be a login per read.
		vault := newFakeVault(t)
		vault.lease = 0

		provider, _ := vault.kubernetesProvider(t)

		clock := newClock()
		provider.now = clock.Now

		vault.put("secret/data/team-a/apps/api", map[string]any{"token": "abc123"})

		ref := secrets.NewRef(provider.Scheme(), "apps/api#token")

		for range 2 {
			_, err := provider.Resolve(t.Context(), secrets.Request{Namespace: "team-a", Ref: ref})
			require.NoError(t, err)

			clock.advance(365 * 24 * time.Hour)
		}

		require.Equal(t, 1, vault.loginCount())
	})

	t.Run("the projected token is read again on every login", func(t *testing.T) {
		// The kubelet rotates a projected token in place, so a JWT kept from
		// startup stops being accepted partway through a worker's life.
		vault := newFakeVault(t)
		vault.lease = time.Minute

		provider, jwtPath := vault.kubernetesProvider(t)

		clock := newClock()
		provider.now = clock.Now

		vault.put("secret/data/team-a/apps/api", map[string]any{"token": "abc123"})

		ref := secrets.NewRef(provider.Scheme(), "apps/api#token")

		_, err := provider.Resolve(t.Context(), secrets.Request{Namespace: "team-a", Ref: ref})
		require.NoError(t, err)

		require.NoError(t, os.WriteFile(jwtPath, []byte("projected-jwt-2"), 0o600))
		clock.advance(time.Minute)

		_, err = provider.Resolve(t.Context(), secrets.Request{Namespace: "team-a", Ref: ref})
		require.NoError(t, err)

		require.Equal(t, []string{projectedJWT, "projected-jwt-2"}, vault.presentedJWTs())
	})
}

func Test_Provider_reauthenticatesOnForbidden(t *testing.T) {
	t.Run("a token revoked between resolutions is replaced once", func(t *testing.T) {
		// Vault answers 403 both for a token it no longer accepts and for a path
		// policy forbids, and does not say which. One retry after a fresh login is
		// what tells them apart.
		vault := newFakeVault(t)
		vault.singleUse = true

		provider, _ := vault.kubernetesProvider(t)
		vault.put("secret/data/team-a/apps/api", map[string]any{"token": "abc123"})

		ref := secrets.NewRef(provider.Scheme(), "apps/api#token")

		for range 3 {
			secret, err := provider.Resolve(t.Context(), secrets.Request{Namespace: "team-a", Ref: ref})
			require.NoError(t, err)
			require.True(t, secret.EqualString("abc123"))
		}

		require.Equal(t, 3, vault.loginCount())
		require.Len(t, vault.readsServed(), 5, "the first resolution needed one read, the rest two")
	})

	t.Run("a policy denial is permanent after one attempt", func(t *testing.T) {
		vault := newFakeVault(t)
		vault.rejectAll = true

		provider, _ := vault.kubernetesProvider(t)
		vault.put("secret/data/team-a/apps/api", map[string]any{"token": "abc123"})

		ref := secrets.NewRef(provider.Scheme(), "apps/api#token")

		_, err := provider.Resolve(t.Context(), secrets.Request{Namespace: "team-a", Ref: ref})
		require.ErrorIs(t, err, secrets.ErrPermission)
		require.False(t, secrets.Retryable(err), "a refused read will be refused again")

		require.Equal(t, 2, vault.loginCount(), "exactly one re-authentication, not a loop")
		require.Len(t, vault.readsServed(), 2)
	})

	t.Run("a static token is not re-authenticated", func(t *testing.T) {
		// There is nothing to log in with, so a second attempt would send the same
		// rejected credential and report the same error a round trip later.
		vault := newFakeVault(t)
		vault.rejectAll = true

		provider := vault.staticProvider(t)
		vault.put("secret/data/team-a/apps/api", map[string]any{"token": "abc123"})

		ref := secrets.NewRef(provider.Scheme(), "apps/api#token")

		_, err := provider.Resolve(t.Context(), secrets.Request{Namespace: "team-a", Ref: ref})
		require.ErrorIs(t, err, secrets.ErrPermission)

		require.Zero(t, vault.loginCount())
		require.Len(t, vault.readsServed(), 1)
	})

	t.Run("one forbidden path does not make every path forbidden", func(t *testing.T) {
		// A 403 may be about the path rather than the token, and a static token
		// cannot be replaced — so discarding it would turn one secret a workflow
		// may not read into every secret being unreadable until a restart.
		vault := newFakeVault(t)
		vault.denied["secret/data/team-a/apps/forbidden"] = true

		provider := vault.staticProvider(t)
		vault.put("secret/data/team-a/apps/forbidden", map[string]any{"token": "abc123"})
		vault.put("secret/data/team-a/apps/api", map[string]any{"token": "def456"})

		forbidden := secrets.NewRef(provider.Scheme(), "apps/forbidden#token")

		_, err := provider.Resolve(t.Context(), secrets.Request{Namespace: "team-a", Ref: forbidden})
		require.ErrorIs(t, err, secrets.ErrPermission)

		allowed := secrets.NewRef(provider.Scheme(), "apps/api#token")

		secret, err := provider.Resolve(t.Context(), secrets.Request{Namespace: "team-a", Ref: allowed})
		require.NoError(t, err, "the token was still good for a path policy allows")
		require.True(t, secret.EqualString("def456"))
	})

	t.Run("a forbidden path does not cost the next read a login", func(t *testing.T) {
		// The re-authentication a 403 triggers leaves its token in place, so a
		// workflow naming a secret it may not read does not keep every other
		// resolution logging in.
		vault := newFakeVault(t)
		vault.denied["secret/data/team-a/apps/forbidden"] = true

		provider, _ := vault.kubernetesProvider(t)
		vault.put("secret/data/team-a/apps/api", map[string]any{"token": "def456"})

		forbidden := secrets.NewRef(provider.Scheme(), "apps/forbidden#token")

		_, err := provider.Resolve(t.Context(), secrets.Request{Namespace: "team-a", Ref: forbidden})
		require.ErrorIs(t, err, secrets.ErrPermission)
		require.Equal(t, 2, vault.loginCount(), "one re-authentication, to tell a stale token from a denial")

		allowed := secrets.NewRef(provider.Scheme(), "apps/api#token")

		secret, err := provider.Resolve(t.Context(), secrets.Request{Namespace: "team-a", Ref: allowed})
		require.NoError(t, err)
		require.True(t, secret.EqualString("def456"))
		require.Equal(t, 2, vault.loginCount(), "the token the denial produced was kept")
	})

	t.Run("a token discarded by one read is not re-fetched by every read in flight", func(t *testing.T) {
		// The generation check is what keeps a slow read's 403 from throwing away a
		// token another goroutine obtained after it started.
		vault := newFakeVault(t)
		provider, _ := vault.kubernetesProvider(t)

		token, generation, err := provider.authToken(t.Context())
		require.NoError(t, err)
		require.Equal(t, "issued-token-1", token)

		// A stale 403, carrying the generation of a token that has since been
		// replaced.
		provider.forget(generation - 1)

		current, _, ok := provider.currentToken()
		require.True(t, ok, "the current token should have survived a stale invalidation")
		require.Equal(t, token, current)

		provider.forget(generation)

		_, _, ok = provider.currentToken()
		require.False(t, ok)
	})

	t.Run("a generation is never reused, so a replacement token survives", func(t *testing.T) {
		// Two reads share a token and both get a 403. The first discards it and the
		// next login replaces it; the second must not then discard the replacement,
		// which is what would happen if the counter restarted when the cache was
		// cleared.
		vault := newFakeVault(t)
		provider, _ := vault.kubernetesProvider(t)

		_, first, err := provider.authToken(t.Context())
		require.NoError(t, err)

		provider.forget(first)

		_, second, err := provider.authToken(t.Context())
		require.NoError(t, err)
		require.NotEqual(t, first, second, "a discarded token's generation must not come back")

		provider.forget(first)

		current, generation, ok := provider.currentToken()
		require.True(t, ok, "the replacement token should have survived the second stale 403")
		require.Equal(t, second, generation)
		require.Equal(t, "issued-token-2", current)
	})
}

// Test_Provider_neverDisclosesCredentials checks the other half of the secret
// hygiene rule: the credentials this provider holds for itself must not leak
// either, since a client token reads every secret a namespace has.
func Test_Provider_neverDisclosesCredentials(t *testing.T) {
	const static = "static-token-do-not-leak"

	vault := newFakeVault(t)
	vault.rejectAll = true

	provider, err := NewProvider(vault.server.URL, WithToken(static))
	require.NoError(t, err)

	ref := secrets.NewRef(provider.Scheme(), "apps/api#token")

	_, err = provider.Resolve(t.Context(), secrets.Request{Namespace: "team-a", Ref: ref})
	require.Error(t, err)
	require.NotContains(t, err.Error(), static)
	require.NotContains(t, fmt.Sprintf("%+v", err), static)

	// The token travelled in a header, which is the one place it belongs.
	reads := vault.readsServed()
	require.Len(t, reads, 1)
	require.Equal(t, static, reads[0].token)
	require.NotContains(t, reads[0].path, static)
}
