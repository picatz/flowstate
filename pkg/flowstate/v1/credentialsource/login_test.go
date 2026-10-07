package credentialsource_test

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/picatz/flowstate/pkg/flowstate/v1/credentialsource"
	"github.com/picatz/flowstate/pkg/flowstate/v1/deviceflow"
)

// refreshIdP is a fake IdP whose token endpoint answers refresh requests: with
// a new access token, or with invalid_grant when reject is set.
type refreshIdP struct {
	*httptest.Server
	refreshes atomic.Int64
	reject    atomic.Bool
}

func newRefreshIdP(t *testing.T) *refreshIdP {
	t.Helper()
	p := &refreshIdP{}
	mux := http.NewServeMux()
	p.Server = httptest.NewServer(mux)
	t.Cleanup(p.Close)
	mux.HandleFunc("/token", func(w http.ResponseWriter, r *http.Request) {
		_ = r.ParseForm()
		p.refreshes.Add(1)
		w.Header().Set("Content-Type", "application/json")
		if p.reject.Load() || r.PostForm.Get("refresh_token") != "refresh-1" {
			w.WriteHeader(http.StatusBadRequest)
			_ = json.NewEncoder(w).Encode(map[string]string{"error": "invalid_grant"})
			return
		}
		_ = json.NewEncoder(w).Encode(map[string]any{
			"access_token": "access-refreshed", "token_type": "Bearer", "expires_in": 3600, "refresh_token": "refresh-2",
		})
	})
	return p
}

const serverOrigin = "https://flowstate.example.com"

// forServer is the context the CLI's transport builds for a request to address.
func forServer(t *testing.T, address string) context.Context {
	t.Helper()
	return credentialsource.ContextWithServerOrigin(t.Context(), address)
}

func storeWith(t *testing.T, p *refreshIdP, expiresAt time.Time, refresh string) *deviceflow.Store {
	t.Helper()
	store := deviceflow.NewStore(filepath.Join(t.TempDir(), "login"))
	require.NoError(t, store.Save(deviceflow.Entry{
		Issuer:       p.URL,
		ClientID:     "flow-cli",
		ServerOrigin: serverOrigin,
		Endpoints:    deviceflow.Endpoints{Issuer: p.URL, Token: p.URL + "/token"},
		Tokens:       deviceflow.Tokens{AccessToken: "access-stored", RefreshToken: refresh, ExpiresAt: expiresAt},
	}))
	return store
}

func TestLoginSourceServesAFreshTokenWithoutTheNetwork(t *testing.T) {
	p := newRefreshIdP(t)
	store := storeWith(t, p, time.Now().Add(time.Hour), "refresh-1")

	source, err := credentialsource.Resolve(credentialsource.SourceLogin, credentialsource.Config{LoginStore: store})
	require.NoError(t, err)
	require.Equal(t, "login", source.Name())

	token, err := source.Token(forServer(t, serverOrigin))
	require.NoError(t, err)
	bearer, ok := token.Bearer()
	require.True(t, ok)
	require.Equal(t, "access-stored", bearer)
	require.Zero(t, p.refreshes.Load())
}

func TestLoginSourceRefreshesNearExpiryAndPersists(t *testing.T) {
	p := newRefreshIdP(t)
	store := storeWith(t, p, time.Now().Add(10*time.Second), "refresh-1")
	source := credentialsource.NewLoginSource(store, p.URL, "flow-cli")

	token, err := source.Token(forServer(t, serverOrigin))
	require.NoError(t, err)
	bearer, _ := token.Bearer()
	require.Equal(t, "access-refreshed", bearer)
	require.WithinDuration(t, time.Now().Add(time.Hour), token.ExpiresAt, time.Minute)

	// The rotated refresh token was stored, and the next call needs no refresh.
	stored, err := store.Load(p.URL, "flow-cli")
	require.NoError(t, err)
	require.Equal(t, "refresh-2", stored.Tokens.RefreshToken)

	_, err = source.Token(forServer(t, serverOrigin))
	require.NoError(t, err)
	require.EqualValues(t, 1, p.refreshes.Load())
}

func TestLoginSourceFailedRefreshFailsClosed(t *testing.T) {
	for name, refresh := range map[string]string{"rejected": "refresh-1", "no refresh token": ""} {
		t.Run(name, func(t *testing.T) {
			p := newRefreshIdP(t)
			p.reject.Store(true)
			store := storeWith(t, p, time.Now().Add(-time.Minute), refresh)
			source := credentialsource.NewLoginSource(store, p.URL, "flow-cli")

			token, err := source.Token(forServer(t, serverOrigin))
			require.ErrorIs(t, err, credentialsource.ErrSourceUnusable)
			require.ErrorContains(t, err, "flow login")
			require.True(t, token.IsZero(), "an expired token must never be returned")
			require.NotContains(t, fmt.Sprint(err), "access-stored")
			require.NotContains(t, fmt.Sprint(err), "refresh-1")
		})
	}
}

func TestLoginSourceWithoutALoginAsksForOne(t *testing.T) {
	source := credentialsource.NewLoginSource(deviceflow.NewStore(filepath.Join(t.TempDir(), "login")), "", "")
	token, err := source.Token(forServer(t, serverOrigin))
	require.ErrorIs(t, err, credentialsource.ErrSourceUnusable)
	require.ErrorIs(t, err, deviceflow.ErrNotLoggedIn)
	require.ErrorContains(t, err, "flow login")
	require.True(t, token.IsZero())
}

func TestLoginSourceRefusesAnotherServer(t *testing.T) {
	p := newRefreshIdP(t)
	// Expired, so a refusal that came late would also have spent the refresh token.
	store := storeWith(t, p, time.Now().Add(-time.Minute), "refresh-1")
	source := credentialsource.NewLoginSource(store, p.URL, "flow-cli")

	for name, ctx := range map[string]context.Context{
		"different host":   forServer(t, "https://evil.example.com"),
		"different scheme": forServer(t, "http://flowstate.example.com"),
		"different port":   forServer(t, "https://flowstate.example.com:8443"),
		"userinfo":         forServer(t, "https://user:pw@flowstate.example.com"),
		"unstated":         t.Context(),
	} {
		t.Run(name, func(t *testing.T) {
			token, err := source.Token(ctx)
			require.ErrorIs(t, err, credentialsource.ErrWrongServer)
			require.ErrorIs(t, err, credentialsource.ErrSourceUnusable)
			require.ErrorContains(t, err, "flow login")
			require.ErrorContains(t, err, "--address")
			require.True(t, token.IsZero())
			require.Zero(t, p.refreshes.Load(), "the refresh token must not be spent for the wrong server")
		})
	}
}

func TestLoginSourceAcceptsOtherSpellingsOfTheSameOrigin(t *testing.T) {
	p := newRefreshIdP(t)
	store := storeWith(t, p, time.Now().Add(time.Hour), "refresh-1")
	source := credentialsource.NewLoginSource(store, p.URL, "flow-cli")

	for _, address := range []string{"https://FlowState.Example.com", "https://flowstate.example.com:443/", serverOrigin} {
		_, err := source.Token(forServer(t, address))
		require.NoError(t, err, address)
	}
}

func TestLoginSourceRefusesAnEntryWithNoRecordedServer(t *testing.T) {
	p := newRefreshIdP(t)
	store := deviceflow.NewStore(filepath.Join(t.TempDir(), "login"))
	require.NoError(t, store.Save(deviceflow.Entry{
		Issuer: p.URL, ClientID: "flow-cli",
		Tokens: deviceflow.Tokens{AccessToken: "a", ExpiresAt: time.Now().Add(time.Hour)},
	}))
	_, err := credentialsource.NewLoginSource(store, "", "").Token(forServer(t, serverOrigin))
	require.ErrorIs(t, err, credentialsource.ErrWrongServer)
}
