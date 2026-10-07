package deviceflow_test

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/picatz/flowstate/pkg/flowstate/v1/deviceflow"
)

const (
	secretDeviceCode = "device-code-SECRET-1f2e3d"
	secretAccess     = "access-token-SECRET-aa11bb22"
	secretRefresh    = "refresh-token-SECRET-cc33dd44"
	secretRotated    = "refresh-token-SECRET-rotated-99"
)

// clock is a fake clock whose sleeper advances it, so polling takes no time.
type clock struct {
	mu     sync.Mutex
	now    time.Time
	sleeps []time.Duration
}

func newClock() *clock { return &clock{now: time.Date(2026, 10, 7, 12, 0, 0, 0, time.UTC)} }

func (c *clock) Now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.now
}

func (c *clock) Sleep(ctx context.Context, d time.Duration) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	c.sleeps = append(c.sleeps, d)
	c.now = c.now.Add(d)
	return nil
}

func (c *clock) Sleeps() []time.Duration {
	c.mu.Lock()
	defer c.mu.Unlock()
	return append([]time.Duration(nil), c.sleeps...)
}

func (c *clock) client() *deviceflow.Client {
	return deviceflow.New(deviceflow.WithClock(c.Now), deviceflow.WithSleeper(c.Sleep))
}

// idp is a fake identity provider. tokenAnswers is consumed one per request to
// the token endpoint; the last answer repeats.
type idp struct {
	*httptest.Server

	mu           sync.Mutex
	authForm     url.Values
	tokenForms   []url.Values
	revokeForms  []url.Values
	tokenAnswers []answer
	deviceBody   map[string]any
	discoveryFn  func(w http.ResponseWriter, issuer string)
	revocation   bool
}

type answer struct {
	status int
	body   any
}

func pending() answer  { return answer{400, map[string]any{"error": "authorization_pending"}} }
func slowDown() answer { return answer{400, map[string]any{"error": "slow_down"}} }
func oauthErr(code string) answer {
	return answer{400, map[string]any{"error": code, "error_description": "no\x1b[31m way"}}
}
func granted(refresh string, expiresIn int) answer {
	body := map[string]any{"access_token": secretAccess, "token_type": "Bearer", "expires_in": expiresIn, "scope": "openid"}
	if refresh != "" {
		body["refresh_token"] = refresh
	}
	return answer{200, body}
}

func newIdP(t *testing.T, answers ...answer) *idp {
	t.Helper()
	p := &idp{tokenAnswers: answers, revocation: true}
	p.deviceBody = map[string]any{
		"device_code":               secretDeviceCode,
		"user_code":                 "WDJB-MJHT",
		"verification_uri":          "http://127.0.0.1/activate",
		"verification_uri_complete": "http://127.0.0.1/activate?user_code=WDJB-MJHT",
		"expires_in":                600,
		"interval":                  5,
	}
	mux := http.NewServeMux()
	p.Server = httptest.NewServer(mux)
	t.Cleanup(p.Close)

	writeJSON := func(w http.ResponseWriter, status int, v any) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(status)
		_ = json.NewEncoder(w).Encode(v)
	}
	mux.HandleFunc("/.well-known/openid-configuration", func(w http.ResponseWriter, r *http.Request) {
		if p.discoveryFn != nil {
			p.discoveryFn(w, p.URL)
			return
		}
		doc := map[string]any{
			"issuer":                        p.URL,
			"device_authorization_endpoint": p.URL + "/device",
			"token_endpoint":                p.URL + "/token",
		}
		if p.revocation {
			doc["revocation_endpoint"] = p.URL + "/revoke"
		}
		writeJSON(w, 200, doc)
	})
	mux.HandleFunc("/device", func(w http.ResponseWriter, r *http.Request) {
		_ = r.ParseForm()
		p.mu.Lock()
		p.authForm = r.PostForm
		body := p.deviceBody
		p.mu.Unlock()
		writeJSON(w, 200, body)
	})
	mux.HandleFunc("/token", func(w http.ResponseWriter, r *http.Request) {
		_ = r.ParseForm()
		p.mu.Lock()
		p.tokenForms = append(p.tokenForms, r.PostForm)
		a := p.tokenAnswers[min(len(p.tokenForms), len(p.tokenAnswers))-1]
		p.mu.Unlock()
		writeJSON(w, a.status, a.body)
	})
	mux.HandleFunc("/revoke", func(w http.ResponseWriter, r *http.Request) {
		_ = r.ParseForm()
		p.mu.Lock()
		p.revokeForms = append(p.revokeForms, r.PostForm)
		p.mu.Unlock()
		w.WriteHeader(200)
	})
	return p
}

func (p *idp) config() deviceflow.Config {
	return deviceflow.Config{Issuer: p.URL, ClientID: "flow-cli", Scope: "openid offline_access", Audience: "flowstate"}
}

func (p *idp) tokenRequests() int {
	p.mu.Lock()
	defer p.mu.Unlock()
	return len(p.tokenForms)
}

func seconds(ss ...int) []time.Duration {
	out := make([]time.Duration, len(ss))
	for i, s := range ss {
		out[i] = time.Duration(s) * time.Second
	}
	return out
}

func TestLoginHappyPath(t *testing.T) {
	p := newIdP(t, pending(), pending(), granted(secretRefresh, 3600))
	clk := newClock()

	var shown deviceflow.Authorization
	entry, err := clk.client().Login(t.Context(), p.config(), func(a deviceflow.Authorization) { shown = a })
	require.NoError(t, err)

	require.Equal(t, "WDJB-MJHT", shown.UserCode)
	require.Equal(t, "http://127.0.0.1/activate", shown.VerificationURI)
	require.Equal(t, secretAccess, entry.Tokens.AccessToken)
	require.Equal(t, secretRefresh, entry.Tokens.RefreshToken)
	require.Equal(t, clk.Now().Add(time.Hour), entry.Tokens.ExpiresAt)
	require.Equal(t, p.URL+"/token", entry.Endpoints.Token)
	require.Equal(t, p.URL+"/revoke", entry.Endpoints.Revocation)

	// RFC 8628 §3.1: the request names the client, scope and (IdP-specific) audience.
	require.Equal(t, "flow-cli", p.authForm.Get("client_id"))
	require.Equal(t, "openid offline_access", p.authForm.Get("scope"))
	require.Equal(t, "flowstate", p.authForm.Get("audience"))

	// RFC 8628 §3.4: the poll names the grant type, the device code and the client.
	require.Len(t, p.tokenForms, 3)
	for _, form := range p.tokenForms {
		require.Equal(t, "urn:ietf:params:oauth:grant-type:device_code", form.Get("grant_type"))
		require.Equal(t, secretDeviceCode, form.Get("device_code"))
		require.Equal(t, "flow-cli", form.Get("client_id"))
	}

	// The interval is honoured before every request, pending included.
	require.Equal(t, seconds(5, 5, 5), clk.Sleeps())
}

func TestPollHonoursServerIntervalAndSlowDown(t *testing.T) {
	p := newIdP(t, pending(), slowDown(), pending(), slowDown(), granted("", 3600))
	p.deviceBody["interval"] = 2
	clk := newClock()

	_, err := clk.client().Login(t.Context(), p.config(), func(deviceflow.Authorization) {})
	require.NoError(t, err)

	// RFC 8628 §3.5: each slow_down adds five seconds, permanently.
	require.Equal(t, seconds(2, 2, 7, 7, 12), clk.Sleeps())
}

func TestPollDefaultsIntervalToFiveSeconds(t *testing.T) {
	p := newIdP(t, granted("", 60))
	delete(p.deviceBody, "interval")
	clk := newClock()

	_, err := clk.client().Login(t.Context(), p.config(), func(deviceflow.Authorization) {})
	require.NoError(t, err)
	require.Equal(t, seconds(5), clk.Sleeps())
}

func TestPollTerminalAnswers(t *testing.T) {
	for _, tc := range []struct {
		name    string
		answers []answer
		want    error
	}{
		{"access_denied", []answer{pending(), oauthErr("access_denied")}, deviceflow.ErrAccessDenied},
		{"expired_token", []answer{pending(), oauthErr("expired_token")}, deviceflow.ErrExpiredToken},
	} {
		t.Run(tc.name, func(t *testing.T) {
			p := newIdP(t, tc.answers...)
			clk := newClock()
			_, err := clk.client().Login(t.Context(), p.config(), func(deviceflow.Authorization) {})
			require.ErrorIs(t, err, tc.want)
			require.Equal(t, 2, p.tokenRequests(), "polling must stop at the terminal answer")
		})
	}
}

func TestPollStopsWhenTheCodeExpiresLocally(t *testing.T) {
	p := newIdP(t, pending())
	p.deviceBody["expires_in"] = 20
	clk := newClock()

	_, err := clk.client().Login(t.Context(), p.config(), func(deviceflow.Authorization) {})
	require.ErrorIs(t, err, deviceflow.ErrExpiredToken)

	// 5s, 10s, 15s sleeps are inside the 20s lifetime; the fourth reaches it
	// and must not send another request.
	require.Equal(t, seconds(5, 5, 5, 5), clk.Sleeps())
	require.Equal(t, 3, p.tokenRequests())
}

func TestAuthorizeCapsHostileTimings(t *testing.T) {
	p := newIdP(t)
	p.deviceBody["expires_in"] = 999999999
	p.deviceBody["interval"] = 999999999
	ep, err := deviceflow.New().Discover(t.Context(), p.URL)
	require.NoError(t, err)

	auth, err := deviceflow.New().Authorize(t.Context(), ep, p.config())
	require.NoError(t, err)
	require.Equal(t, deviceflow.MaxDeviceCodeLifetime, auth.ExpiresIn)
	require.Equal(t, deviceflow.MaxPollInterval, auth.Interval)
}

func TestIdPTextIsSanitizedAndTheDeviceCodeIsNotPrinted(t *testing.T) {
	p := newIdP(t, oauthErr("server_error"))
	clk := newClock()

	var shown deviceflow.Authorization
	_, err := clk.client().Login(t.Context(), p.config(), func(a deviceflow.Authorization) { shown = a })
	require.Error(t, err)
	require.NotContains(t, err.Error(), "\x1b", "terminal control characters from the IdP must not reach an error")
	require.Contains(t, err.Error(), "server_error")
	require.NotContains(t, fmt.Sprintf("%v %+v %#v %s", shown, shown, shown, shown), secretDeviceCode)
	require.NotContains(t, err.Error(), secretDeviceCode)
}

func TestRefresh(t *testing.T) {
	prev := deviceflow.Tokens{AccessToken: "old", RefreshToken: secretRefresh}

	t.Run("rotated refresh token replaces the old one", func(t *testing.T) {
		p := newIdP(t, granted(secretRotated, 1800))
		clk := newClock()
		ep, err := clk.client().Discover(t.Context(), p.URL)
		require.NoError(t, err)

		got, err := clk.client().Refresh(t.Context(), ep, "flow-cli", prev)
		require.NoError(t, err)
		require.Equal(t, secretAccess, got.AccessToken)
		require.Equal(t, secretRotated, got.RefreshToken)
		require.Equal(t, clk.Now().Add(30*time.Minute), got.ExpiresAt)

		// RFC 6749 §6 request shape.
		form := p.tokenForms[0]
		require.Equal(t, "refresh_token", form.Get("grant_type"))
		require.Equal(t, secretRefresh, form.Get("refresh_token"))
		require.Equal(t, "flow-cli", form.Get("client_id"))
	})

	t.Run("absent refresh token keeps the old one", func(t *testing.T) {
		p := newIdP(t, granted("", 1800))
		ep, err := deviceflow.New().Discover(t.Context(), p.URL)
		require.NoError(t, err)

		got, err := deviceflow.New().Refresh(t.Context(), ep, "flow-cli", prev)
		require.NoError(t, err)
		require.Equal(t, secretRefresh, got.RefreshToken)
	})

	t.Run("rejection is an OAuthError", func(t *testing.T) {
		p := newIdP(t, oauthErr("invalid_grant"))
		ep, err := deviceflow.New().Discover(t.Context(), p.URL)
		require.NoError(t, err)

		_, err = deviceflow.New().Refresh(t.Context(), ep, "flow-cli", prev)
		var oauth *deviceflow.OAuthError
		require.ErrorAs(t, err, &oauth)
		require.Equal(t, "invalid_grant", oauth.Code)
		require.NotContains(t, err.Error(), secretRefresh)
	})

	t.Run("nothing to refresh with", func(t *testing.T) {
		_, err := deviceflow.New().Refresh(t.Context(), deviceflow.Endpoints{}, "c", deviceflow.Tokens{AccessToken: "a"})
		require.ErrorIs(t, err, deviceflow.ErrNoRefreshToken)
	})
}

func TestRevoke(t *testing.T) {
	p := newIdP(t)
	ep, err := deviceflow.New().Discover(t.Context(), p.URL)
	require.NoError(t, err)

	require.NoError(t, deviceflow.New().Revoke(t.Context(), ep, "flow-cli", secretRefresh, "refresh_token"))
	require.Equal(t, secretRefresh, p.revokeForms[0].Get("token"))
	require.Equal(t, "refresh_token", p.revokeForms[0].Get("token_type_hint"))

	ep.Revocation = ""
	require.ErrorIs(t, deviceflow.New().Revoke(t.Context(), ep, "flow-cli", "t", "refresh_token"), deviceflow.ErrRevocationUnsupported)
}

func TestDiscoveryRefusesPlainHTTPOffLoopback(t *testing.T) {
	// No server exists at this name: the refusal must come before any dial.
	_, err := deviceflow.New().Discover(t.Context(), "http://idp.example.com")
	require.ErrorIs(t, err, deviceflow.ErrInsecureURL)

	_, err = deviceflow.New().Discover(t.Context(), "https://user:pw@idp.example.com")
	require.ErrorIs(t, err, deviceflow.ErrInsecureURL, "userinfo in an issuer is refused")
}

func TestDiscoveryValidatesTheDocument(t *testing.T) {
	for name, tc := range map[string]struct {
		doc  func(issuer string) map[string]any
		want error
	}{
		"issuer mismatch": {func(string) map[string]any {
			return map[string]any{"issuer": "https://other.example", "token_endpoint": "https://other.example/t", "device_authorization_endpoint": "https://other.example/d"}
		}, deviceflow.ErrInvalidDiscovery},
		"insecure token endpoint": {func(i string) map[string]any {
			return map[string]any{"issuer": i, "token_endpoint": "http://evil.example/t", "device_authorization_endpoint": i + "/d"}
		}, deviceflow.ErrInsecureURL},
		"insecure revocation endpoint": {func(i string) map[string]any {
			return map[string]any{"issuer": i, "token_endpoint": i + "/t", "device_authorization_endpoint": i + "/d", "revocation_endpoint": "http://evil.example/r"}
		}, deviceflow.ErrInsecureURL},
		"no device endpoint": {func(i string) map[string]any {
			return map[string]any{"issuer": i, "token_endpoint": i + "/t"}
		}, deviceflow.ErrInvalidDiscovery},
		"no token endpoint": {func(i string) map[string]any {
			return map[string]any{"issuer": i, "device_authorization_endpoint": i + "/d"}
		}, deviceflow.ErrInvalidDiscovery},
	} {
		t.Run(name, func(t *testing.T) {
			p := newIdP(t)
			p.discoveryFn = func(w http.ResponseWriter, issuer string) { _ = json.NewEncoder(w).Encode(tc.doc(issuer)) }
			_, err := deviceflow.New().Discover(t.Context(), p.URL)
			require.ErrorIs(t, err, tc.want)
		})
	}
}

func TestRedirectsOffOriginAreRefused(t *testing.T) {
	var hits int
	elsewhere := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { hits++ }))
	t.Cleanup(elsewhere.Close)

	p := newIdP(t)
	p.discoveryFn = func(w http.ResponseWriter, _ string) {
		w.Header().Set("Location", elsewhere.URL+"/.well-known/openid-configuration")
		w.WriteHeader(http.StatusFound)
	}
	_, err := deviceflow.New().Discover(t.Context(), p.URL)
	require.ErrorIs(t, err, deviceflow.ErrCrossOriginRedirect)
	require.Zero(t, hits, "the redirect target must never be contacted")
}

func TestOversizedResponsesAreBounded(t *testing.T) {
	huge := strings.Repeat("x", deviceflow.MaxResponseBytes+1)

	t.Run("discovery", func(t *testing.T) {
		p := newIdP(t)
		p.discoveryFn = func(w http.ResponseWriter, issuer string) {
			_, _ = fmt.Fprintf(w, `{"issuer":%q,"padding":%q}`, issuer, huge)
		}
		_, err := deviceflow.New().Discover(t.Context(), p.URL)
		require.ErrorIs(t, err, deviceflow.ErrResponseTooLarge)
	})

	t.Run("token endpoint", func(t *testing.T) {
		p := newIdP(t, answer{200, map[string]any{"access_token": huge, "token_type": "Bearer"}})
		clk := newClock()
		_, err := clk.client().Login(t.Context(), p.config(), func(deviceflow.Authorization) {})
		require.ErrorIs(t, err, deviceflow.ErrResponseTooLarge)
	})
}

func TestNonBearerTokensAreRefused(t *testing.T) {
	p := newIdP(t, answer{200, map[string]any{"access_token": secretAccess, "token_type": "DPoP", "expires_in": 60}})
	_, err := newClock().client().Login(t.Context(), p.config(), func(deviceflow.Authorization) {})
	require.ErrorContains(t, err, "only bearer tokens")
	require.NotContains(t, err.Error(), secretAccess)
}

func TestTokensNeverRenderTheirValues(t *testing.T) {
	tokens := deviceflow.Tokens{AccessToken: secretAccess, RefreshToken: secretRefresh, ExpiresAt: time.Now()}
	entry := deviceflow.Entry{Issuer: "https://i.example", ClientID: "c", Tokens: tokens}

	type wrapper struct{ Entry deviceflow.Entry }
	asJSON, err := json.Marshal(wrapper{entry})
	require.NoError(t, err)

	var logged strings.Builder
	slog.New(slog.NewTextHandler(&logged, nil)).Info("login", "tokens", tokens, "entry", entry)

	for _, rendered := range []string{
		fmt.Sprint(tokens), fmt.Sprintf("%+v", tokens), fmt.Sprintf("%#v", tokens), fmt.Sprintf("%q", tokens),
		fmt.Sprint(entry), fmt.Sprintf("%+v", entry), fmt.Sprintf("%#v", wrapper{entry}),
		string(asJSON), logged.String(),
	} {
		require.NotContains(t, rendered, "SECRET")
	}
}

func TestAuthorizationNeverRendersTheDeviceCode(t *testing.T) {
	p := newIdP(t)
	ep, err := deviceflow.New().Discover(t.Context(), p.URL)
	require.NoError(t, err)
	auth, err := deviceflow.New().Authorize(t.Context(), ep, p.config())
	require.NoError(t, err)
	require.Equal(t, secretDeviceCode, auth.DeviceCode)

	asJSON, err := json.Marshal(auth)
	require.NoError(t, err)
	var logged, loggedJSON strings.Builder
	slog.New(slog.NewTextHandler(&logged, nil)).Info("auth", "authorization", auth)
	slog.New(slog.NewJSONHandler(&loggedJSON, nil)).Info("auth", "authorization", auth)
	type wrapper struct{ Auth deviceflow.Authorization }
	wrapped, err := json.Marshal(wrapper{auth})
	require.NoError(t, err)

	for _, rendered := range []string{
		string(asJSON), string(wrapped), logged.String(), loggedJSON.String(),
		fmt.Sprint(auth), fmt.Sprintf("%+v", auth), fmt.Sprintf("%#v", auth), fmt.Sprintf("%#v", wrapper{auth}),
	} {
		require.NotContains(t, rendered, secretDeviceCode)
		require.NotContains(t, rendered, "SECRET")
	}
	require.Contains(t, string(asJSON), "WDJB-MJHT")
}

func TestSubmittedCredentialsAreRedactedFromOAuthErrors(t *testing.T) {
	// The secret sits across the 200-byte cut, so truncating first would leave
	// a recognizable prefix of it.
	describe := func(secret string) answer {
		return answer{400, map[string]any{"error": "server_error", "error_description": strings.Repeat("x", 190) + secret + " trailing"}}
	}

	t.Run("poll", func(t *testing.T) {
		p := newIdP(t, describe(secretDeviceCode))
		_, err := newClock().client().Login(t.Context(), p.config(), func(deviceflow.Authorization) {})
		require.Error(t, err)
		require.NotContains(t, err.Error(), "SECRET")
		require.NotContains(t, err.Error(), "device-code")
		require.Contains(t, err.Error(), "[redacted]")
	})

	t.Run("refresh", func(t *testing.T) {
		p := newIdP(t, describe(secretRefresh))
		ep, err := deviceflow.New().Discover(t.Context(), p.URL)
		require.NoError(t, err)
		_, err = deviceflow.New().Refresh(t.Context(), ep, "flow-cli", deviceflow.Tokens{AccessToken: "a", RefreshToken: secretRefresh})
		require.Error(t, err)
		require.NotContains(t, err.Error(), "SECRET")
		require.NotContains(t, err.Error(), "refresh-token")
		require.Contains(t, err.Error(), "[redacted]")
	})
}

func TestSleepNeverOutlastsTheDeviceCode(t *testing.T) {
	p := newIdP(t, pending())
	p.deviceBody["interval"] = 300
	p.deviceBody["expires_in"] = 1
	clk := newClock()

	_, err := clk.client().Login(t.Context(), p.config(), func(deviceflow.Authorization) {})
	require.ErrorIs(t, err, deviceflow.ErrExpiredToken)
	require.Equal(t, []time.Duration{time.Second}, clk.Sleeps(), "the sleep is clamped to the 1s that remained")
	require.Zero(t, p.tokenRequests(), "no request is made once the code has expired")
}

func TestHTTPSIssuerMayNotNameAnHTTPEndpoint(t *testing.T) {
	var issuer string
	srv := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_ = json.NewEncoder(w).Encode(map[string]any{
			"issuer":                        issuer,
			"device_authorization_endpoint": issuer + "/device",
			"token_endpoint":                "http://127.0.0.1:1/token",
		})
	}))
	t.Cleanup(srv.Close)
	issuer = srv.URL

	_, err := deviceflow.New(deviceflow.WithHTTPClient(srv.Client())).Discover(t.Context(), issuer)
	require.ErrorIs(t, err, deviceflow.ErrInsecureURL)
}
