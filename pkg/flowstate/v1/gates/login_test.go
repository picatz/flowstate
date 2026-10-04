package gates

import (
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"connectrpc.com/connect"
	"github.com/stretchr/testify/require"

	v1 "github.com/picatz/flowstate/pkg/flowstate/v1"
)

const (
	testClientID = "gates-ui"
	testRedirect = "https://flow.example.com" + CallbackPath
	testToken    = "tok-alice"
)

// issuer is a fake OpenID provider: discovery, an authorization endpoint the
// tests read the request of, and a token endpoint that checks PKCE.
type issuer struct {
	*httptest.Server

	discoveryIssuer string // overrides the issuer the document names
	discoveryExtra  map[string]any
	tokenType       string
	tokenBody       func() string
	tokenCalls      atomic.Int32
	lastForm        url.Values
	lastAuth        string
	challenge       string
}

func newIssuer(t *testing.T) *issuer {
	t.Helper()

	is := &issuer{tokenType: "Bearer"}
	mux := http.NewServeMux()
	is.Server = httptest.NewServer(mux)
	t.Cleanup(is.Close)

	mux.HandleFunc("/.well-known/openid-configuration", func(w http.ResponseWriter, _ *http.Request) {
		doc := map[string]any{
			"issuer":                 cmpOr(is.discoveryIssuer, is.URL),
			"authorization_endpoint": is.URL + "/authorize",
			"token_endpoint":         is.URL + "/token",

			"code_challenge_methods_supported": []string{"S256"},
		}
		for k, v := range is.discoveryExtra {
			doc[k] = v
		}
		_ = json.NewEncoder(w).Encode(doc)
	})
	mux.HandleFunc("/token", func(w http.ResponseWriter, r *http.Request) {
		is.tokenCalls.Add(1)
		_ = r.ParseForm()
		is.lastForm = r.PostForm
		is.lastAuth = r.Header.Get("Authorization")

		sum := sha256.Sum256([]byte(r.PostForm.Get("code_verifier")))
		if base64.RawURLEncoding.EncodeToString(sum[:]) != is.challenge || r.PostForm.Get("code") != "good-code" {
			http.Error(w, `{"error":"invalid_grant"}`, http.StatusBadRequest)
			return
		}
		if is.tokenBody != nil {
			_, _ = fmt.Fprint(w, is.tokenBody())
			return
		}
		_, _ = fmt.Fprintf(w, `{"access_token":%q,"token_type":%q,"expires_in":600}`, testToken, is.tokenType)
	})

	return is
}

func cmpOr(a, b string) string {
	if a != "" {
		return a
	}

	return b
}

func (is *issuer) login(t *testing.T, mutate ...func(*LoginConfig)) *Login {
	t.Helper()

	key, err := NewSessionKey()
	require.NoError(t, err)
	cfg := LoginConfig{
		Issuer: is.URL, ClientID: testClientID, RedirectURL: testRedirect,
		Resource: "https://api.example.com", SessionKey: key, HTTPClient: is.Client(),
	}
	for _, m := range mutate {
		m(&cfg)
	}
	l, err := NewLogin(cfg)
	require.NoError(t, err)

	return l
}

// signedInAPI lets alice's token read and answer the gate and refuses the rest.
func signedInAPI() *fakeAPI {
	open := waiting("Approve?")

	return &fakeAPI{get: func(req *connect.Request[v1.GetGateRequest]) (*connect.Response[v1.GetGateResponse], error) {
		if req.Header().Get("Authorization") != "Bearer "+testToken {
			return nil, connect.NewError(connect.CodeUnauthenticated, nil)
		}

		return open(req)
	}}
}

func setCookies(rec *httptest.ResponseRecorder) map[string]*http.Cookie {
	out := map[string]*http.Cookie{}
	for _, c := range rec.Result().Cookies() {
		out[c.Name] = c
	}

	return out
}

func withCookie(req *http.Request, cs ...*http.Cookie) *http.Request {
	for _, c := range cs {
		req.AddCookie(&http.Cookie{Name: c.Name, Value: c.Value})
	}

	return req
}

// begin starts a sign-in and returns the authorization request the browser is
// sent to and the sign-in cookie.
func (is *issuer) begin(t *testing.T, h http.Handler, next string) (url.Values, *http.Cookie) {
	t.Helper()

	target := LoginPath
	if next != "" {
		target += "?next=" + url.QueryEscape(next)
	}
	rec := do(h, httptest.NewRequest(http.MethodGet, target, nil))
	require.Equal(t, http.StatusSeeOther, rec.Code)

	loc, err := url.Parse(rec.Header().Get("Location"))
	require.NoError(t, err)
	require.Equal(t, is.URL+"/authorize", loc.Scheme+"://"+loc.Host+loc.Path)

	q := loc.Query()
	is.challenge = q.Get("code_challenge")

	c := setCookies(rec)[loginCookieName]
	require.NotNil(t, c)

	return q, c
}

func callbackReq(q url.Values, login *http.Cookie) *http.Request {
	req := httptest.NewRequest(http.MethodGet, CallbackPath+"?"+q.Encode(), nil)
	if login != nil {
		withCookie(req, login)
	}

	return req
}

func TestSignInRunsTheCodeFlowAndPresentsTheTokenToTheAPI(t *testing.T) {
	t.Parallel()

	is := newIssuer(t)
	api := signedInAPI()
	h := newHandler(api, WithLogin(is.login(t)))

	// A visitor with nothing is sent to sign in, and back to where they were.
	rec := do(h, gateGet())
	require.Equal(t, http.StatusSeeOther, rec.Code)
	require.Equal(t, LoginPath+"?next="+url.QueryEscape(Path(testWorkflow, testSignal)), rec.Header().Get("Location"))

	q, loginCookie := is.begin(t, h, Path(testWorkflow, testSignal))
	require.Equal(t, "code", q.Get("response_type"))
	require.Equal(t, testClientID, q.Get("client_id"))
	require.Equal(t, testRedirect, q.Get("redirect_uri"))
	require.Equal(t, "S256", q.Get("code_challenge_method"))
	require.Equal(t, "https://api.example.com", q.Get("resource"))
	require.NotEmpty(t, q.Get("state"))

	require.True(t, loginCookie.Secure && loginCookie.HttpOnly)
	require.Equal(t, "/", loginCookie.Path)
	require.NotContains(t, loginCookie.Value, q.Get("state"), "the state is sealed, not readable from the cookie")

	rec = do(h, callbackReq(url.Values{"code": {"good-code"}, "state": {q.Get("state")}, "iss": {is.URL}}, loginCookie))
	require.Equal(t, http.StatusSeeOther, rec.Code)
	require.Equal(t, Path(testWorkflow, testSignal), rec.Header().Get("Location"))
	require.Equal(t, "authorization_code", is.lastForm.Get("grant_type"))
	require.Equal(t, testRedirect, is.lastForm.Get("redirect_uri"))
	require.Equal(t, "https://api.example.com", is.lastForm.Get("resource"))
	require.Empty(t, is.lastAuth, "a public client sends no secret")

	cookies := setCookies(rec)
	require.Equal(t, -1, cookies[loginCookieName].MaxAge, "the sign-in cookie is single use")
	session := cookies[sessionCookieName]
	require.NotNil(t, session)
	require.True(t, session.Secure && session.HttpOnly)
	require.Equal(t, http.SameSiteLaxMode, session.SameSite)
	require.NotContains(t, session.Value, testToken, "the token is sealed in the cookie")

	rec = do(h, withCookie(gateGet(), session))
	require.Equal(t, http.StatusOK, rec.Code)
	require.Equal(t, "Bearer "+testToken, api.credentials()[len(api.credentials())-1], "the API sees the token the issuer minted")
}

func TestAConfidentialClientAuthenticatesTheExchange(t *testing.T) {
	t.Parallel()

	is := newIssuer(t)
	h := newHandler(signedInAPI(), WithLogin(is.login(t, func(c *LoginConfig) { c.ClientSecret = "s3cret/+" })))

	q, c := is.begin(t, h, "")
	rec := do(h, callbackReq(url.Values{"code": {"good-code"}, "state": {q.Get("state")}}, c))

	require.Equal(t, http.StatusOK, rec.Code, "no next: a plain page")
	req, _ := http.NewRequest(http.MethodGet, "/", nil)
	req.Header.Set("Authorization", is.lastAuth)
	user, pass, ok := req.BasicAuth()
	require.True(t, ok)
	require.Equal(t, testClientID, user)
	require.Equal(t, url.QueryEscape("s3cret/+"), pass, "RFC 6749 section 2.3.1: form-encoded before Basic")
}

func TestACallbackIsRefusedUnlessItIsThisBrowsersFlow(t *testing.T) {
	t.Parallel()

	is := newIssuer(t)
	h := newHandler(signedInAPI(), WithLogin(is.login(t)))
	q, c := is.begin(t, h, "")
	good := url.Values{"code": {"good-code"}, "state": {q.Get("state")}}

	tests := map[string]*http.Request{
		"no sign-in cookie":      callbackReq(good, nil),
		"wrong state":            callbackReq(url.Values{"code": {"good-code"}, "state": {"forged"}}, c),
		"no state":               callbackReq(url.Values{"code": {"good-code"}}, c),
		"tampered cookie":        callbackReq(good, &http.Cookie{Name: c.Name, Value: c.Value[:len(c.Value)-2] + "xx"}),
		"garbage cookie":         callbackReq(good, &http.Cookie{Name: c.Name, Value: "!!"}),
		"a session as a sign-in": nil, // filled below
	}
	// A session cookie presented where a sign-in cookie belongs: the purpose is
	// bound into the seal, so it does not open.
	l := is.login(t)
	sealed, err := l.seal("session", session{Token: "x", Expires: time.Now().Add(time.Hour)})
	require.NoError(t, err)
	tests["a session as a sign-in"] = callbackReq(good, &http.Cookie{Name: loginCookieName, Value: sealed})

	for name, req := range tests {
		rec := do(h, req)
		require.Equal(t, http.StatusBadRequest, rec.Code, name)
		require.Nil(t, setCookies(rec)[sessionCookieName], name)
	}
	require.Zero(t, is.tokenCalls.Load(), "no refused callback reached the token endpoint")
}

func TestAnExpiredSignInIsRefused(t *testing.T) {
	t.Parallel()

	is := newIssuer(t)
	now := time.Now()
	clock := &now
	h := newHandler(signedInAPI(), WithLogin(is.login(t, func(c *LoginConfig) { c.Now = func() time.Time { return *clock } })))

	q, c := is.begin(t, h, "")
	later := now.Add(loginTTL + time.Second)
	clock = &later

	rec := do(h, callbackReq(url.Values{"code": {"good-code"}, "state": {q.Get("state")}}, c))
	require.Equal(t, http.StatusBadRequest, rec.Code)
	require.Zero(t, is.tokenCalls.Load())
}

func TestTheIssuerIdentificationParameterIsChecked(t *testing.T) {
	t.Parallel()

	for name, tc := range map[string]struct {
		extra map[string]any
		iss   url.Values
		want  int
	}{
		"matching iss":                  {nil, url.Values{"iss": {"@"}}, http.StatusOK},
		"another issuer":                {nil, url.Values{"iss": {"https://evil.example.com"}}, http.StatusBadRequest},
		"absent where not promised":     {nil, nil, http.StatusOK},
		"absent where always sent":      {map[string]any{"authorization_response_iss_parameter_supported": true}, nil, http.StatusBadRequest},
		"present where always sent":     {map[string]any{"authorization_response_iss_parameter_supported": true}, url.Values{"iss": {"@"}}, http.StatusOK},
		"another issuer where promised": {map[string]any{"authorization_response_iss_parameter_supported": true}, url.Values{"iss": {"https://evil.example.com"}}, http.StatusBadRequest},
	} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()

			is := newIssuer(t)
			is.discoveryExtra = tc.extra
			h := newHandler(signedInAPI(), WithLogin(is.login(t)))
			q, c := is.begin(t, h, "")

			form := url.Values{"code": {"good-code"}, "state": {q.Get("state")}}
			for _, v := range tc.iss["iss"] {
				form.Set("iss", strings.ReplaceAll(v, "@", is.URL))
			}
			require.Equal(t, tc.want, do(h, callbackReq(form, c)).Code)
		})
	}
}

func TestAnErrorFromTheIssuerSignsNobodyIn(t *testing.T) {
	t.Parallel()

	is := newIssuer(t)
	h := newHandler(signedInAPI(), WithLogin(is.login(t)))
	q, c := is.begin(t, h, "")

	rec := do(h, callbackReq(url.Values{
		"error": {"access_denied"}, "error_description": {"<script>alert(1)</script>"}, "state": {q.Get("state")},
	}, c))
	require.Equal(t, http.StatusForbidden, rec.Code)
	require.NotContains(t, rec.Body.String(), "script", "the issuer's text is not echoed")
	require.Nil(t, setCookies(rec)[sessionCookieName])
}

func TestTokensThePageCannotPresentAreRefused(t *testing.T) {
	t.Parallel()

	for name, setup := range map[string]func(*issuer){
		"not a bearer token": func(is *issuer) { is.tokenType = "DPoP" },
		"no access token":    func(is *issuer) { is.tokenBody = func() string { return `{"token_type":"Bearer"}` } },
		"expires too soon": func(is *issuer) {
			is.tokenBody = func() string { return `{"access_token":"a","token_type":"Bearer","expires_in":10}` }
		},
		"too big for a cookie": func(is *issuer) {
			is.tokenBody = func() string { return `{"access_token":"` + strings.Repeat("a", 4000) + `","token_type":"Bearer"}` }
		},
		"a response without end": func(is *issuer) { is.tokenBody = func() string { return strings.Repeat(" ", maxIssuerResponse+10) } },
	} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()

			is := newIssuer(t)
			setup(is)
			h := newHandler(signedInAPI(), WithLogin(is.login(t)))
			q, c := is.begin(t, h, "")

			rec := do(h, callbackReq(url.Values{"code": {"good-code"}, "state": {q.Get("state")}}, c))
			require.Equal(t, http.StatusBadGateway, rec.Code)
			require.Nil(t, setCookies(rec)[sessionCookieName])
		})
	}
}

func TestAnIssuerThatMisnamesItselfCannotSteerTheFlow(t *testing.T) {
	t.Parallel()

	is := newIssuer(t)
	is.discoveryIssuer = "https://other.example.com"
	h := newHandler(signedInAPI(), WithLogin(is.login(t)))

	rec := do(h, httptest.NewRequest(http.MethodGet, LoginPath, nil))
	require.Equal(t, http.StatusBadGateway, rec.Code)
	require.Nil(t, setCookies(rec)[loginCookieName])

	for name, methods := range map[string]any{"plain only": []string{"plain"}, "empty": []string{}, "omitted": nil} {
		is := newIssuer(t)
		is.discoveryExtra = map[string]any{"code_challenge_methods_supported": methods}
		h := newHandler(signedInAPI(), WithLogin(is.login(t)))
		require.Equal(t, http.StatusBadGateway, do(h, httptest.NewRequest(http.MethodGet, LoginPath, nil)).Code,
			"an issuer that does not advertise S256 cannot take part: "+name)
	}
}

func TestSignInReturnsOnlyToThisPagesOwnPaths(t *testing.T) {
	t.Parallel()

	is := newIssuer(t)
	h := newHandler(signedInAPI(), WithLogin(is.login(t)))

	for _, next := range []string{
		"https://evil.example.com/gates/x", "//evil.example.com/gates/x", "/elsewhere", "gates/x",
		`/gates/\evil.example.com`, "/gates/x\r\nSet-Cookie: a=b", "/gates/x#frag", "/gates/" + strings.Repeat("a", maxNextBytes),
	} {
		rec := do(h, httptest.NewRequest(http.MethodGet, LoginPath+"?next="+url.QueryEscape(next), nil))
		require.Equal(t, http.StatusBadRequest, rec.Code, next)
	}
	require.True(t, safeNext(Path("wf", "sig")))
	require.True(t, safeNext(Path("a b", "c/d")))
}

func TestARejectedSessionDoesNotLoop(t *testing.T) {
	t.Parallel()

	is := newIssuer(t)
	l := is.login(t)
	h := newHandler(signedInAPI(), WithLogin(l))

	// A well-formed session whose token the API no longer accepts.
	sealed, err := l.seal("session", session{Token: "revoked", Expires: time.Now().Add(time.Hour)})
	require.NoError(t, err)
	rec := do(h, withCookie(gateGet(), &http.Cookie{Name: sessionCookieName, Value: sealed}))

	require.Equal(t, http.StatusUnauthorized, rec.Code, "not another redirect to the issuer")
	require.Empty(t, rec.Header().Get("Location"))
	require.Equal(t, -1, setCookies(rec)[sessionCookieName].MaxAge, "the dead session is dropped")
	require.Contains(t, rec.Body.String(), `href="`+LoginPath+`?next=`)

	// An expired or foreign cookie is likewise a page, not a loop.
	rec = do(h, withCookie(gateGet(), &http.Cookie{Name: sessionCookieName, Value: "garbage"}))
	require.Equal(t, http.StatusUnauthorized, rec.Code)

	// A header the API refuses wins over a cookie that rides along: it is answered
	// as a bearer client, with no sign-in page and the cookie left alone.
	rec = do(h, withCookie(gateGet("Authorization", "Bearer nope"), &http.Cookie{Name: sessionCookieName, Value: sealed}))
	require.Equal(t, http.StatusUnauthorized, rec.Code)
	require.Equal(t, "Bearer", rec.Header().Get("WWW-Authenticate"))
	require.Empty(t, rec.Result().Cookies())
	require.NotContains(t, rec.Body.String(), LoginPath)
}

func TestAnAuthorizationHeaderWinsOverTheSession(t *testing.T) {
	t.Parallel()

	is := newIssuer(t)
	l := is.login(t)
	api := signedInAPI()
	h := newHandler(api, WithLogin(l))

	sealed, err := l.seal("session", session{Token: "from-session", Expires: time.Now().Add(time.Hour)})
	require.NoError(t, err)
	do(h, withCookie(gateGet("Authorization", "Bearer "+testToken), &http.Cookie{Name: sessionCookieName, Value: sealed}))
	require.Equal(t, []string{"Bearer " + testToken}, api.credentials())

	// A bearer client the API refuses is told so, not sent to a browser sign-in.
	rec := do(h, gateGet("Authorization", "Bearer nope"))
	require.Equal(t, http.StatusUnauthorized, rec.Code)
	require.Equal(t, "Bearer", rec.Header().Get("WWW-Authenticate"))
}

func TestAnExpiredSessionIsNotPresented(t *testing.T) {
	t.Parallel()

	is := newIssuer(t)
	l := is.login(t)
	sealed, err := l.seal("session", session{Token: testToken, Expires: time.Now().Add(-time.Second)})
	require.NoError(t, err)

	require.Empty(t, l.credential(withCookie(gateGet(), &http.Cookie{Name: sessionCookieName, Value: sealed})))
}

func TestAnAnswerIsNeverRedirectedToSignIn(t *testing.T) {
	t.Parallel()

	is := newIssuer(t)
	api := signedInAPI()
	api.signal = func(*connect.Request[v1.SignalRequest]) (*connect.Response[v1.SignalResponse], error) {
		return nil, connect.NewError(connect.CodeUnauthenticated, nil)
	}
	h := newHandler(api, WithLogin(is.login(t)))

	rec := do(h, gatePost(url.Values{"decision": {"approve"}}, "Authorization", "Bearer "+testToken))
	require.Equal(t, http.StatusUnauthorized, rec.Code)
}

func TestSignOutNeedsThePagesOwnPost(t *testing.T) {
	t.Parallel()

	is := newIssuer(t)
	h := newHandler(signedInAPI(), WithLogin(is.login(t)))

	forged := httptest.NewRequest(http.MethodPost, LogoutPath, nil)
	forged.Header.Set("Sec-Fetch-Site", "cross-site")
	rec := do(h, forged)
	require.Equal(t, http.StatusForbidden, rec.Code)
	require.Empty(t, rec.Result().Cookies())

	own := httptest.NewRequest(http.MethodPost, LogoutPath, nil)
	own.Header.Set("Sec-Fetch-Site", "same-origin")
	rec = do(h, own)
	require.Equal(t, http.StatusOK, rec.Code)
	require.Equal(t, -1, setCookies(rec)[sessionCookieName].MaxAge)

	// A GET of the logout path is read as a gate named "logout" and has no
	// workflow, so a link cannot sign anyone out and no session is cleared.
	rec = do(h, httptest.NewRequest(http.MethodGet, LogoutPath, nil))
	require.Equal(t, http.StatusNotFound, rec.Code, "a link cannot sign anyone out")
	require.Empty(t, rec.Result().Cookies())
}

func TestWithoutLoginThereAreNoSignInRoutes(t *testing.T) {
	t.Parallel()

	h := newHandler(signedInAPI())
	require.Equal(t, http.StatusUnauthorized, do(h, gateGet()).Code)
	// Without WithLogin the path is read as a gate named "login": nothing is
	// served that begins a flow.
	rec := do(h, httptest.NewRequest(http.MethodGet, LoginPath, nil))
	require.Equal(t, http.StatusNotFound, rec.Code)
}

func TestNewLoginRefusesWhatCannotWork(t *testing.T) {
	t.Parallel()

	is := newIssuer(t)
	key, err := NewSessionKey()
	require.NoError(t, err)
	base := LoginConfig{Issuer: is.URL, ClientID: "c", RedirectURL: testRedirect, SessionKey: key, HTTPClient: is.Client()}

	for name, mutate := range map[string]func(*LoginConfig){
		"plain http issuer":        func(c *LoginConfig) { c.Issuer = "http://issuer.example.com" },
		"no client id":             func(c *LoginConfig) { c.ClientID = " " },
		"redirect off the path":    func(c *LoginConfig) { c.RedirectURL = "https://flow.example.com/other" },
		"redirect with a query":    func(c *LoginConfig) { c.RedirectURL = testRedirect + "?a=b" },
		"redirect with empty ?":    func(c *LoginConfig) { c.RedirectURL = testRedirect + "?" },
		"redirect with empty #":    func(c *LoginConfig) { c.RedirectURL = testRedirect + "#" },
		"resource with fragment":   func(c *LoginConfig) { c.Resource = "https://api.example.com/#x" },
		"resource with empty #":    func(c *LoginConfig) { c.Resource = "https://api.example.com/#" },
		"plain http redirect":      func(c *LoginConfig) { c.RedirectURL = "http://flow.example.com" + CallbackPath },
		"bad resource":             func(c *LoginConfig) { c.Resource = "not a url" },
		"short key":                func(c *LoginConfig) { c.SessionKey = key[:16] },
		"no client for the issuer": func(c *LoginConfig) { c.HTTPClient = nil },
	} {
		cfg := base
		mutate(&cfg)
		_, err := NewLogin(cfg)
		require.Error(t, err, name)
	}

	_, err = NewLogin(base)
	require.NoError(t, err)
}
