package main

import (
	"cmp"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/picatz/flowstate/pkg/flowstate/v1/deviceflow"
)

const loginRefreshToken = "login-test-refresh-token-77aa31"

// loginIdP is a fake identity provider that signs a user in with
// whoamiToken, so a login followed by `flow auth whoami` crosses every layer.
type loginIdP struct {
	*httptest.Server

	mu             sync.Mutex
	denied         bool
	rejectRefresh  bool
	revokeStatus   int
	revoked        []string
	refreshedWith  []string
	accessLifetime int
}

func newLoginIdP(t *testing.T) *loginIdP {
	t.Helper()

	p := &loginIdP{revokeStatus: http.StatusOK, accessLifetime: 3600}
	mux := http.NewServeMux()
	p.Server = httptest.NewServer(mux)
	t.Cleanup(p.Close)

	reply := func(w http.ResponseWriter, status int, v any) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(status)
		_ = json.NewEncoder(w).Encode(v)
	}
	mux.HandleFunc("/.well-known/openid-configuration", func(w http.ResponseWriter, _ *http.Request) {
		reply(w, 200, map[string]string{
			"issuer":                        p.URL,
			"device_authorization_endpoint": p.URL + "/device",
			"token_endpoint":                p.URL + "/token",
			"revocation_endpoint":           p.URL + "/revoke",
		})
	})
	mux.HandleFunc("/device", func(w http.ResponseWriter, _ *http.Request) {
		reply(w, 200, map[string]any{
			"device_code":               "device-code-do-not-print",
			"user_code":                 "ABCD-EFGH",
			"verification_uri":          p.URL + "/activate",
			"verification_uri_complete": p.URL + "/activate?user_code=ABCD-EFGH",
			"expires_in":                600,
			"interval":                  1,
		})
	})
	mux.HandleFunc("/token", func(w http.ResponseWriter, r *http.Request) {
		_ = r.ParseForm()
		p.mu.Lock()
		defer p.mu.Unlock()
		switch r.PostForm.Get("grant_type") {
		case "refresh_token":
			p.refreshedWith = append(p.refreshedWith, r.PostForm.Get("refresh_token"))
			if p.rejectRefresh {
				reply(w, 400, map[string]string{"error": "invalid_grant"})
				return
			}
		default:
			if p.denied {
				reply(w, 400, map[string]string{"error": "access_denied"})
				return
			}
		}
		reply(w, 200, map[string]any{
			"access_token": whoamiToken, "token_type": "Bearer",
			"expires_in": p.accessLifetime, "refresh_token": loginRefreshToken,
		})
	})
	mux.HandleFunc("/revoke", func(w http.ResponseWriter, r *http.Request) {
		_ = r.ParseForm()
		p.mu.Lock()
		defer p.mu.Unlock()
		p.revoked = append(p.revoked, r.PostForm.Get("token"))
		w.WriteHeader(p.revokeStatus)
	})

	return p
}

// isolateLogin points the config directory at a fresh temporary one, clears
// every other credential, and makes polling instant.
func isolateLogin(t *testing.T) *deviceflow.Store {
	t.Helper()

	config := t.TempDir()
	t.Setenv("XDG_CONFIG_HOME", config)
	t.Setenv("HOME", config)
	for _, name := range []string{
		"FLOWSTATE_TOKEN", "FLOWSTATE_TOKEN_FILE", "FLOWSTATE_CREDENTIAL_SOURCE",
		"FLOWSTATE_ISSUER", "FLOWSTATE_CLIENT_ID", "FLOWSTATE_SCOPE", "FLOWSTATE_AUDIENCE",
	} {
		t.Setenv(name, "")
	}

	previous := newDeviceflowClient
	newDeviceflowClient = func() *deviceflow.Client {
		return deviceflow.New(deviceflow.WithSleeper(func(ctx context.Context, _ time.Duration) error { return ctx.Err() }))
	}
	t.Cleanup(func() { newDeviceflowClient = previous })

	store, err := deviceflow.DefaultStore()
	require.NoError(t, err)

	return store
}

func login(t *testing.T, p *loginIdP, extra ...string) flowResult {
	t.Helper()

	address := cmp.Or(os.Getenv("FLOWSTATE_ADDRESS"), "flowstate.example.com:9233")

	return runFlow(t, append([]string{"login", "--issuer", p.URL, "--client-id", "flow-cli", "--address", address}, extra...)...)
}

func requireNoSecrets(t *testing.T, res flowResult) {
	t.Helper()

	for _, secret := range []string{whoamiToken, loginRefreshToken, "device-code-do-not-print"} {
		require.NotContains(t, res.Stdout, secret)
		require.NotContains(t, res.Stderr, secret)
		if res.Err != nil {
			require.NotContains(t, res.Err.Error(), secret)
		}
	}
}

func TestLoginStoresATokenWhoamiThenUsesAndLogoutRemovesIt(t *testing.T) {
	store := isolateLogin(t)
	serveRealWhoami(t)
	p := newLoginIdP(t)

	res := login(t, p, "--audience", "flowstate")
	require.NoError(t, res.Err, res.Stderr)

	// The user code and URL are for the person, on stderr; the success line is stdout.
	require.Contains(t, res.Stderr, "ABCD-EFGH")
	require.Contains(t, res.Stderr, p.URL+"/activate")
	require.Contains(t, res.Stdout, "Logged in to "+p.URL)
	requireNoSecrets(t, res)

	entry, err := store.Load(p.URL, "flow-cli")
	require.NoError(t, err)
	require.Equal(t, whoamiToken, entry.Tokens.AccessToken)
	if runtime.GOOS != "windows" {
		files, err := os.ReadDir(store.Dir())
		require.NoError(t, err)
		require.Len(t, files, 1)
		info, err := files[0].Info()
		require.NoError(t, err)
		require.Equal(t, os.FileMode(0o600), info.Mode().Perm())
	}

	// Through the normal flags: none of them name a credential.
	who := runFlow(t, "auth", "whoami")
	require.NoError(t, who.Err, who.Stderr)
	require.Contains(t, who.Stdout, "authenticated: true")
	require.Contains(t, who.Stdout, "subject: runner-7")
	requireNoSecrets(t, who)

	// An explicit credential outranks the stored login: a bad FLOWSTATE_TOKEN
	// is refused rather than quietly replaced by it.
	t.Setenv("FLOWSTATE_TOKEN", "some-other-token")
	other := runFlow(t, "auth", "whoami")
	require.Error(t, other.Err)
	t.Setenv("FLOWSTATE_TOKEN", "")

	out := runFlow(t, "logout")
	require.NoError(t, out.Err, out.Stderr)
	require.Contains(t, out.Stdout, "revoked its refresh token")
	require.NotContains(t, out.Stdout, "revoked its tokens", "only the refresh token was asked about")
	require.Equal(t, []string{loginRefreshToken}, p.revoked)
	requireNoSecrets(t, out)

	_, err = store.Load(p.URL, "flow-cli")
	require.ErrorIs(t, err, deviceflow.ErrNotLoggedIn)

	// With the login gone the default chain is what it always was: anonymous.
	anon := runFlow(t, "auth", "whoami")
	require.NoError(t, anon.Err, anon.Stderr)
	require.Contains(t, anon.Stdout, "authenticated: false")

	again := runFlow(t, "logout")
	require.NoError(t, again.Err)
	require.Contains(t, again.Stdout, "Not logged in")
}

func TestLoginSourceNamedExplicitly(t *testing.T) {
	isolateLogin(t)
	serveRealWhoami(t)
	p := newLoginIdP(t)
	require.NoError(t, login(t, p).Err)

	who := runFlow(t, "auth", "whoami", "--credential-source", "login")
	require.NoError(t, who.Err, who.Stderr)
	require.Contains(t, who.Stdout, "authenticated: true")
}

func TestLoginDeniedStoresNothing(t *testing.T) {
	store := isolateLogin(t)
	p := newLoginIdP(t)
	p.denied = true

	res := login(t, p)
	require.ErrorContains(t, res.Err, "denied")
	requireNoSecrets(t, res)
	_, err := store.Select("", "")
	require.ErrorIs(t, err, deviceflow.ErrNotLoggedIn)
}

func TestLoginRequiresIssuerAndClientID(t *testing.T) {
	isolateLogin(t)

	res := runFlow(t, "login")
	require.ErrorContains(t, res.Err, "--issuer and --client-id")
}

func TestLoginFlagsReadTheEnvironment(t *testing.T) {
	store := isolateLogin(t)
	p := newLoginIdP(t)
	t.Setenv("FLOWSTATE_ISSUER", p.URL)
	t.Setenv("FLOWSTATE_CLIENT_ID", "from-env")
	t.Setenv("FLOWSTATE_ADDRESS", "flowstate.example.com:9233")

	res := runFlow(t, "login")
	require.NoError(t, res.Err, res.Stderr)
	_, err := store.Load(p.URL, "from-env")
	require.NoError(t, err)
}

func TestLoginRefusesPlainHTTPIssuerOffLoopback(t *testing.T) {
	isolateLogin(t)

	res := runFlow(t, "login", "--issuer", "http://idp.example.com", "--client-id", "x", "--address", "flowstate.example.com:9233")
	require.ErrorContains(t, res.Err, "https")
}

func TestExpiredLoginIsRefreshedThenFailsClosed(t *testing.T) {
	store := isolateLogin(t)
	serveRealWhoami(t)
	p := newLoginIdP(t)
	require.NoError(t, login(t, p).Err)

	expire := func() {
		entry, err := store.Load(p.URL, "flow-cli")
		require.NoError(t, err)
		entry.Tokens.ExpiresAt = time.Now().Add(-time.Minute)
		require.NoError(t, store.Save(entry))
	}

	expire()
	who := runFlow(t, "auth", "whoami")
	require.NoError(t, who.Err, who.Stderr)
	require.Contains(t, who.Stdout, "authenticated: true")
	require.Equal(t, []string{loginRefreshToken}, p.refreshedWith)

	// A refresh the IdP refuses is an error naming the fix, not anonymous.
	p.mu.Lock()
	p.rejectRefresh = true
	p.mu.Unlock()
	expire()
	failed := runFlow(t, "auth", "whoami")
	require.Error(t, failed.Err)
	require.Contains(t, failed.Err.Error()+failed.Stderr, "flow login")
	require.NotContains(t, failed.Stdout, "authenticated")
	requireNoSecrets(t, failed)
}

func TestLooseLoginPermissionsAreRefused(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("file modes are not enforced on windows")
	}
	store := isolateLogin(t)
	serveRealWhoami(t)
	p := newLoginIdP(t)
	require.NoError(t, login(t, p).Err)

	files, err := os.ReadDir(store.Dir())
	require.NoError(t, err)
	require.NoError(t, os.Chmod(filepath.Join(store.Dir(), files[0].Name()), 0o644))

	who := runFlow(t, "auth", "whoami")
	require.Error(t, who.Err)
	require.Contains(t, who.Err.Error()+who.Stderr, "permissions")
	requireNoSecrets(t, who)
}

func TestLogoutWithFailedRevocationStillForgets(t *testing.T) {
	store := isolateLogin(t)
	p := newLoginIdP(t)
	require.NoError(t, login(t, p).Err)
	p.mu.Lock()
	p.revokeStatus = http.StatusInternalServerError
	p.mu.Unlock()

	res := runFlow(t, "logout")
	require.NoError(t, res.Err)
	require.Contains(t, res.Stderr, "warning: could not revoke")
	require.Contains(t, res.Stdout, "Removed the stored login")
	require.Contains(t, res.Stdout, "Nothing was revoked")
	requireNoSecrets(t, res)

	_, err := store.Load(p.URL, "flow-cli")
	require.ErrorIs(t, err, deviceflow.ErrNotLoggedIn)
}

func TestSeveralLoginsNeedASelector(t *testing.T) {
	isolateLogin(t)
	serveRealWhoami(t)
	one, two := newLoginIdP(t), newLoginIdP(t)
	require.NoError(t, login(t, one).Err)
	require.NoError(t, login(t, two).Err)

	ambiguous := runFlow(t, "auth", "whoami")
	require.Error(t, ambiguous.Err)
	require.Contains(t, ambiguous.Err.Error()+ambiguous.Stderr, "--issuer")

	t.Setenv("FLOWSTATE_ISSUER", one.URL)
	who := runFlow(t, "auth", "whoami")
	require.NoError(t, who.Err, who.Stderr)

	out := runFlow(t, "logout", "--issuer", two.URL, "--client-id", "flow-cli")
	require.NoError(t, out.Err)
	require.Equal(t, []string{loginRefreshToken}, two.revoked)
	require.Empty(t, one.revoked)
}

func TestLoginRequiresAServerAddress(t *testing.T) {
	isolateLogin(t)
	p := newLoginIdP(t)

	res := runFlow(t, "login", "--issuer", p.URL, "--client-id", "flow-cli")
	require.ErrorContains(t, res.Err, "--address")

	bad := runFlow(t, "login", "--issuer", p.URL, "--client-id", "flow-cli", "--address", "https://user:pw@flowstate.example.com")
	require.ErrorContains(t, bad.Err, "--address")
}

// TestLoginIsOnlyPresentedToTheServerItWasMadeFor is the binding: a login for
// server A, then commands aimed at server B, which must never see the token
// and which must not fall back to anonymous either.
func TestLoginIsOnlyPresentedToTheServerItWasMadeFor(t *testing.T) {
	isolateLogin(t)
	serveRealWhoami(t) // server A, and FLOWSTATE_ADDRESS
	serverA := os.Getenv("FLOWSTATE_ADDRESS")
	p := newLoginIdP(t)
	require.NoError(t, login(t, p).Err)

	var mu sync.Mutex
	var seen []string
	serverB := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		seen = append(seen, r.Header.Get("Authorization"))
		mu.Unlock()
		http.Error(w, "unexpected", http.StatusTeapot)
	}))
	t.Cleanup(serverB.Close)

	for name, args := range map[string][]string{
		"default chain":   {"auth", "whoami", "--address", serverB.URL},
		"explicit source": {"auth", "whoami", "--address", serverB.URL, "--credential-source", "login"},
	} {
		t.Run(name, func(t *testing.T) {
			res := runFlow(t, args...)
			require.Error(t, res.Err)
			require.Contains(t, res.Err.Error()+res.Stderr, "flow login")
			require.Contains(t, res.Err.Error()+res.Stderr, "--address")
			require.NotContains(t, res.Stdout, "authenticated")
			requireNoSecrets(t, res)
		})
	}
	mu.Lock()
	require.Empty(t, seen, "server B must not receive any request, let alone the token: %q", seen)
	mu.Unlock()

	// The same origin in another spelling still works.
	same := runFlow(t, "auth", "whoami", "--address", "http://"+strings.ToUpper(strings.TrimPrefix(serverA, "http://"))+"/")
	require.NoError(t, same.Err, same.Stderr)
	require.Contains(t, same.Stdout, "authenticated: true")

	// Logout is not bound to an origin.
	require.NoError(t, runFlow(t, "logout").Err)
}
