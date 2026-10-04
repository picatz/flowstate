package gates

import (
	"bytes"
	"context"
	"crypto/aes"
	"crypto/cipher"
	"crypto/rand"
	"crypto/sha256"
	"crypto/subtle"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"
	"sync"
	"time"

	"github.com/picatz/flowstate/pkg/flowstate/v1/auth"
)

// Browser sign-in: the OAuth 2.0 authorization code flow with PKCE, so an
// approver who follows a link with no credential is sent to the deployment's
// issuer, comes back with an access token, and the page presents that token to
// the API exactly as a bearer client would.
//
// The page is the OAuth client and the API is the resource server, which is why
// the sign-in adds no authorization of its own: the access token is verified by
// the API's own authenticator on every call (issuer in the trust policy,
// audience, claims), and what the person may do is the API's decision about the
// identity the token carries. The page never reads the token, never trusts an
// ID token, and holds nothing the issuer could not re-issue.
//
// The profile is OAuth 2.1's (draft-ietf-oauth-v2-1) and the security best
// current practice's (RFC 9700): the code flow only, PKCE with S256 on every
// request (RFC 7636 section 4.2), an exact redirect URI, a one-use state bound
// to the browser that started the flow (RFC 9700 section 4.7), the issuer
// identification parameter checked when sent (RFC 9207 section 2.4), the access
// token bound to the API's audience with a resource indicator (RFC 8707
// section 2), and no implicit or password grant at all.

const (
	loginCookieName   = "__Host-gates_login"
	sessionCookieName = "__Host-gates_session"

	// LoginPath and CallbackPath are the sign-in routes. The callback is the
	// redirect URI an operator registers with the issuer.
	LoginPath    = PathPrefix + "login"
	CallbackPath = PathPrefix + "callback"
	LogoutPath   = PathPrefix + "logout"

	// loginTTL is how long a person has between being sent to the issuer and
	// coming back. It bounds a state that leaks.
	loginTTL = 10 * time.Minute

	// maxSession caps a session however long the token says it lives: a stolen
	// cookie is useful for no longer than this, and a person signs in again.
	maxSession = 8 * time.Hour

	// defaultTokenLifetime is the lifetime assumed when the issuer states none.
	// expires_in is only RECOMMENDED (RFC 6749 section 5.1).
	defaultTokenLifetime = 5 * time.Minute

	// sessionSkew ends a session slightly before its token does, so a request
	// is not sent with a token that expires in flight.
	sessionSkew = 30 * time.Second

	// maxCookieValue keeps a sealed cookie inside what browsers store (RFC 6265
	// section 6.1 asks for at least 4096 bytes per cookie).
	maxCookieValue = 3800

	// maxIssuerResponse bounds a discovery document or token response. Both are
	// read from a server a deployment does not run; the egress client already
	// caps a body, and this is the same bound where the client is another.
	maxIssuerResponse = 256 << 10

	// discoveryTTL is how long a discovery document is trusted before it is
	// fetched again.
	discoveryTTL = 10 * time.Minute

	// maxNextBytes bounds the path a sign-in returns to.
	maxNextBytes = 1024
)

// LoginConfig configures [NewLogin].
type LoginConfig struct {
	// Issuer is the OpenID Connect issuer URL: https, or loopback for a local
	// rehearsal. Its discovery document is read from
	// /.well-known/openid-configuration beneath it, and the document's own
	// issuer must equal this exactly (OpenID Connect Discovery 1.0 section 4.3).
	Issuer string

	// ClientID is the client registered with the issuer for this page.
	ClientID string

	// ClientSecret is set only when the registration is a confidential client; a
	// public client, which is the default and needs none because PKCE proves the
	// code exchange, leaves it empty. It is sent as HTTP Basic credentials
	// (RFC 6749 section 2.3.1).
	ClientSecret string

	// RedirectURL is the absolute address of CallbackPath on the deployment as a
	// browser reaches it, registered exactly with the issuer.
	RedirectURL string

	// Resource is the API's audience: the resource indicator (RFC 8707) sent so
	// the issuer mints a token the API's authenticator will accept. Empty omits
	// it, for an issuer that audiences its tokens another way.
	Resource string

	// Scopes are requested with the code. None are required by this page.
	Scopes []string

	// SessionKey seals the session and sign-in cookies with AES-256-GCM: exactly
	// 32 bytes. Replicas that should honour each other's sessions share one; a
	// single process may pass [NewSessionKey]'s result and accept that a restart
	// signs everyone out.
	SessionKey []byte

	// HTTPClient makes every request to the issuer. A deployment passes the
	// client its identity egress policy builds, so sign-in leaves the process by
	// the same boundary the verifier's discovery does. Nil is refused.
	HTTPClient *http.Client

	// Now is the clock; nil is time.Now. A test sets it.
	Now func() time.Time
}

// NewSessionKey returns a fresh random session key.
func NewSessionKey() ([]byte, error) {
	key := make([]byte, 32)
	if _, err := rand.Read(key); err != nil {
		return nil, fmt.Errorf("gates: generating a session key: %w", err)
	}

	return key, nil
}

// Login is the browser sign-in. Build one with [NewLogin] and give it to
// [WithLogin].
type Login struct {
	cfg  LoginConfig
	aead cipher.AEAD
	now  func() time.Time

	mu        sync.Mutex
	discovery *discovery
	fetchedAt time.Time
}

// discovery is the part of the issuer's metadata the flow uses.
type discovery struct {
	Issuer                        string   `json:"issuer"`
	AuthorizationEndpoint         string   `json:"authorization_endpoint"`
	TokenEndpoint                 string   `json:"token_endpoint"`
	CodeChallengeMethods          []string `json:"code_challenge_methods_supported"`
	AuthorizationResponseIssuerOK bool     `json:"authorization_response_iss_parameter_supported"`
}

// NewLogin validates cfg and returns the sign-in it describes. Every refusal is
// at construction, so a deployment that cannot sign people in does not start
// rather than answering each approver with an error.
func NewLogin(cfg LoginConfig) (*Login, error) {
	if _, err := auth.ValidateHTTPSURL(cfg.Issuer, "gates sign-in issuer"); err != nil {
		return nil, err
	}
	if strings.TrimSpace(cfg.ClientID) == "" {
		return nil, errors.New("gates: sign-in needs a client id")
	}
	redirect, err := auth.ValidateHTTPSURL(cfg.RedirectURL, "gates sign-in redirect URL")
	if err != nil {
		return nil, err
	}
	if redirect.Path != CallbackPath || redirect.RawQuery != "" || redirect.Fragment != "" {
		return nil, fmt.Errorf("gates: the sign-in redirect URL %q must end in %s with no query or fragment: "+
			"the issuer redirects the browser to exactly that address", cfg.RedirectURL, CallbackPath)
	}
	if cfg.Resource != "" {
		if _, err := auth.ValidateHTTPSURL(cfg.Resource, "gates sign-in resource"); err != nil {
			return nil, err
		}
	}
	if len(cfg.SessionKey) != 32 {
		return nil, fmt.Errorf("gates: the session key must be exactly 32 bytes, got %d", len(cfg.SessionKey))
	}
	if cfg.HTTPClient == nil {
		return nil, errors.New("gates: sign-in needs an HTTP client for the issuer; pass the one the " +
			"deployment's identity egress policy builds")
	}

	block, err := aes.NewCipher(cfg.SessionKey)
	if err != nil {
		return nil, fmt.Errorf("gates: the session key does not build a cipher: %w", err)
	}
	aead, err := cipher.NewGCM(block)
	if err != nil {
		return nil, fmt.Errorf("gates: the session key does not build a cipher: %w", err)
	}

	now := cfg.Now
	if now == nil {
		now = time.Now
	}

	return &Login{cfg: cfg, aead: aead, now: now}, nil
}

// seal encrypts and authenticates v for the cookie named purpose. The purpose is
// bound as additional data, so a sign-in cookie cannot be presented as a session
// and the reverse.
func (l *Login) seal(purpose string, v any) (string, error) {
	plain, err := json.Marshal(v)
	if err != nil {
		return "", err
	}
	nonce := make([]byte, l.aead.NonceSize())
	if _, err := rand.Read(nonce); err != nil {
		return "", err
	}
	sealed := l.aead.Seal(nonce, nonce, plain, []byte(purpose))

	return base64.RawURLEncoding.EncodeToString(sealed), nil
}

// open reverses seal into v, refusing anything not sealed by this key for this
// purpose.
func (l *Login) open(purpose, value string, v any) error {
	raw, err := base64.RawURLEncoding.DecodeString(value)
	if err != nil || len(raw) < l.aead.NonceSize() {
		return errors.New("gates: malformed cookie")
	}
	nonce, body := raw[:l.aead.NonceSize()], raw[l.aead.NonceSize():]
	plain, err := l.aead.Open(nil, nonce, body, []byte(purpose))
	if err != nil {
		return errors.New("gates: cookie not sealed by this deployment")
	}

	return json.Unmarshal(plain, v)
}

// loginState is what a browser carries between leaving for the issuer and
// returning. Everything the callback must check is here and sealed, so no
// server-side store is needed and any replica can finish a sign-in another
// began.
type loginState struct {
	State    string    `json:"s"`
	Verifier string    `json:"v"`
	Next     string    `json:"n"`
	Expires  time.Time `json:"e"`
}

// session is the signed-in cookie's content.
type session struct {
	Token   string    `json:"t"`
	Expires time.Time `json:"e"`
}

func (l *Login) cookie(name, value string, maxAge time.Duration) *http.Cookie {
	return &http.Cookie{
		Name:     name,
		Value:    value,
		Path:     "/", // required by the __Host- prefix (RFC 6265bis section 4.1.3.2)
		Secure:   true,
		HttpOnly: true,
		// Lax, not Strict: the callback is the issuer's redirect, a top-level
		// cross-site navigation, and Strict would drop the sign-in cookie on it.
		// A state-changing request is protected separately (see fromThisPage).
		SameSite: http.SameSiteLaxMode,
		MaxAge:   int(maxAge / time.Second),
	}
}

func clearing(name string) *http.Cookie {
	return &http.Cookie{
		Name: name, Value: "", Path: "/", Secure: true, HttpOnly: true,
		SameSite: http.SameSiteLaxMode, MaxAge: -1,
	}
}

// credential returns the Authorization header value a request's session
// carries, or the empty string for none, an expired one, or one this
// deployment did not seal.
func (l *Login) credential(r *http.Request) string {
	c, err := r.Cookie(sessionCookieName)
	if err != nil {
		return ""
	}
	var s session
	if err := l.open("session", c.Value, &s); err != nil || s.Token == "" || !l.now().Before(s.Expires) {
		return ""
	}

	return "Bearer " + s.Token
}

// discover returns the issuer's metadata, fetched at most once per
// [discoveryTTL] and refused unless it names the configured issuer and the
// endpoints and PKCE support the flow needs.
func (l *Login) discover(ctx context.Context) (*discovery, error) {
	l.mu.Lock()
	defer l.mu.Unlock()

	if l.discovery != nil && l.now().Sub(l.fetchedAt) < discoveryTTL {
		return l.discovery, nil
	}

	endpoint := strings.TrimRight(l.cfg.Issuer, "/") + "/.well-known/openid-configuration"
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, endpoint, nil)
	if err != nil {
		return nil, err
	}
	req.Header.Set("Accept", "application/json")

	var doc discovery
	if err := l.doJSON(req, &doc); err != nil {
		return nil, fmt.Errorf("gates: reading the issuer's discovery document: %w", err)
	}

	// The document must be about the issuer asked for: otherwise a document
	// served for another issuer, by a proxy or a mix-up, would steer the flow
	// (OpenID Connect Discovery 1.0 section 4.3).
	if doc.Issuer != l.cfg.Issuer {
		return nil, fmt.Errorf("gates: the discovery document names issuer %q, not the configured %q", doc.Issuer, l.cfg.Issuer)
	}
	for field, value := range map[string]string{
		"authorization_endpoint": doc.AuthorizationEndpoint,
		"token_endpoint":         doc.TokenEndpoint,
	} {
		if _, err := auth.ValidateHTTPSURL(value, "discovery "+field); err != nil {
			return nil, err
		}
	}
	// PKCE is not optional in this profile. An issuer that publishes its
	// supported methods without S256 cannot take part (RFC 8414 section 2).
	if len(doc.CodeChallengeMethods) > 0 && !containsString(doc.CodeChallengeMethods, "S256") {
		return nil, errors.New("gates: the issuer does not support PKCE with S256")
	}

	l.discovery, l.fetchedAt = &doc, l.now()

	return &doc, nil
}

func containsString(list []string, want string) bool {
	for _, s := range list {
		if s == want {
			return true
		}
	}

	return false
}

// doJSON sends req and decodes a bounded 200 JSON answer into v. Redirects are
// not followed: neither endpoint has reason to send one, and one is how a
// request body with a code in it ends up somewhere it was not addressed.
func (l *Login) doJSON(req *http.Request, v any) error {
	client := *l.cfg.HTTPClient
	client.CheckRedirect = func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }

	resp, err := client.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	body, err := io.ReadAll(io.LimitReader(resp.Body, maxIssuerResponse+1))
	if err != nil {
		return err
	}
	if len(body) > maxIssuerResponse {
		return errors.New("the answer is larger than the page will read")
	}
	if resp.StatusCode != http.StatusOK {
		// The body is not quoted: it is another server's text.
		return fmt.Errorf("the issuer answered %d", resp.StatusCode)
	}

	return json.Unmarshal(body, v)
}

func randomString(n int) (string, error) {
	buf := make([]byte, n)
	if _, err := rand.Read(buf); err != nil {
		return "", err
	}

	return base64.RawURLEncoding.EncodeToString(buf), nil
}

// safeNext reports a path a sign-in may return to: one of this page's own, as
// a path and nothing else, so the flow cannot be made to redirect elsewhere
// (RFC 9700 section 4.11 on open redirection).
func safeNext(next string) bool {
	if next == "" || len(next) > maxNextBytes || !strings.HasPrefix(next, PathPrefix) {
		return false
	}
	if strings.ContainsAny(next, "\\\r\n") || strings.HasPrefix(next, "//") {
		return false
	}
	u, err := url.Parse(next)

	return err == nil && u.Scheme == "" && u.Host == "" && u.User == nil && !u.ForceQuery && u.Fragment == ""
}

// begin starts a sign-in: it seals the state, sets the cookie that ties the
// flow to this browser, and redirects to the issuer.
func (h *Handler) begin(w http.ResponseWriter, r *http.Request) {
	l := h.login

	next := r.URL.Query().Get("next")
	if next != "" && !safeNext(next) {
		render(w, http.StatusBadRequest, noticePage, notice{Title: "That sign-in could not start"})
		return
	}

	ctx, cancel := context.WithTimeout(r.Context(), apiTimeout)
	defer cancel()

	doc, err := l.discover(ctx)
	if err != nil {
		h.logger.ErrorContext(r.Context(), "gate sign-in: discovery failed", "error", oneLine(err.Error()))
		render(w, http.StatusBadGateway, noticePage, notice{
			Title:  "Sign-in is unavailable",
			Detail: "The identity provider could not be reached. Try again, and tell an operator if it keeps happening.",
		})
		return
	}

	state, err1 := randomString(32)
	verifier, err2 := randomString(32) // 43 characters: RFC 7636 section 4.1
	if err := errors.Join(err1, err2); err != nil {
		http.Error(w, "internal error", http.StatusInternalServerError)
		return
	}

	sealed, err := l.seal("login", loginState{
		State: state, Verifier: verifier, Next: next, Expires: l.now().Add(loginTTL),
	})
	if err != nil {
		http.Error(w, "internal error", http.StatusInternalServerError)
		return
	}

	challenge := sha256.Sum256([]byte(verifier))
	q := url.Values{
		"response_type":         {"code"},
		"client_id":             {l.cfg.ClientID},
		"redirect_uri":          {l.cfg.RedirectURL},
		"state":                 {state},
		"code_challenge":        {base64.RawURLEncoding.EncodeToString(challenge[:])},
		"code_challenge_method": {"S256"},
	}
	if len(l.cfg.Scopes) > 0 {
		q.Set("scope", strings.Join(l.cfg.Scopes, " "))
	}
	if l.cfg.Resource != "" {
		q.Set("resource", l.cfg.Resource)
	}

	authorize, err := url.Parse(doc.AuthorizationEndpoint)
	if err != nil {
		http.Error(w, "internal error", http.StatusInternalServerError)
		return
	}
	merged := authorize.Query()
	for k, v := range q {
		merged[k] = v
	}
	authorize.RawQuery = merged.Encode()

	http.SetCookie(w, l.cookie(loginCookieName, sealed, loginTTL))
	http.Redirect(w, r, authorize.String(), http.StatusSeeOther)
}

// callback finishes a sign-in: it checks the browser is the one that began it,
// exchanges the code, and sets the session.
func (h *Handler) callback(w http.ResponseWriter, r *http.Request) {
	l := h.login

	// The sign-in cookie is single use whatever happens next: a replayed
	// callback finds nothing to complete.
	http.SetCookie(w, clearing(loginCookieName))

	refuse := func(status int, title string) {
		render(w, status, noticePage, notice{Title: title, Detail: "Start again from the link you were sent."})
	}

	c, err := r.Cookie(loginCookieName)
	if err != nil {
		refuse(http.StatusBadRequest, "This sign-in did not start here")
		return
	}
	var st loginState
	if err := l.open("login", c.Value, &st); err != nil || !l.now().Before(st.Expires) {
		refuse(http.StatusBadRequest, "This sign-in expired")
		return
	}

	q := r.URL.Query()

	// The state binds the response to the browser that asked (RFC 9700 section
	// 4.7). Compared before anything else in the response is believed, and in
	// constant time because it is a secret.
	if subtle.ConstantTimeCompare([]byte(q.Get("state")), []byte(st.State)) != 1 {
		refuse(http.StatusBadRequest, "This sign-in did not start here")
		return
	}

	// An error from the issuer is reported without its text, which is another
	// server's to choose.
	if q.Get("error") != "" {
		refuse(http.StatusForbidden, "Sign-in was refused")
		return
	}

	code := q.Get("code")
	if code == "" || len(code) > 2048 {
		refuse(http.StatusBadRequest, "That sign-in could not be completed")
		return
	}

	ctx, cancel := context.WithTimeout(r.Context(), apiTimeout)
	defer cancel()

	doc, err := l.discover(ctx)
	if err != nil {
		h.logger.ErrorContext(r.Context(), "gate sign-in: discovery failed", "error", oneLine(err.Error()))
		refuse(http.StatusBadGateway, "Sign-in is unavailable")
		return
	}

	// RFC 9207 section 2.4: when the response names its issuer it must be the
	// one asked, and when the issuer says it always does, an absent name is a
	// refusal. This is what stops a response from another issuer being accepted.
	if iss := q.Get("iss"); iss != "" && iss != l.cfg.Issuer || iss == "" && doc.AuthorizationResponseIssuerOK {
		refuse(http.StatusBadRequest, "That sign-in came from the wrong identity provider")
		return
	}

	token, lifetime, err := l.exchange(ctx, doc, code, st.Verifier)
	if err != nil {
		h.logger.ErrorContext(r.Context(), "gate sign-in: token exchange failed", "error", oneLine(err.Error()))
		refuse(http.StatusBadGateway, "Sign-in could not be completed")
		return
	}

	sealed, err := l.seal("session", session{Token: token, Expires: l.now().Add(lifetime)})
	if err != nil || len(sealed) > maxCookieValue {
		h.logger.ErrorContext(r.Context(), "gate sign-in: the token does not fit a cookie", "bytes", len(sealed))
		refuse(http.StatusBadGateway, "Sign-in could not be completed")
		return
	}
	http.SetCookie(w, l.cookie(sessionCookieName, sealed, lifetime))

	if st.Next != "" && safeNext(st.Next) {
		http.Redirect(w, r, st.Next, http.StatusSeeOther)
		return
	}
	render(w, http.StatusOK, noticePage, notice{Title: "Signed in", Detail: "Open the link you were sent to answer a gate."})
}

// exchange redeems the code at the token endpoint and returns the access token
// and how long to keep it (RFC 6749 section 4.1.3 with PKCE's code_verifier,
// RFC 7636 section 4.5).
func (l *Login) exchange(ctx context.Context, doc *discovery, code, verifier string) (string, time.Duration, error) {
	form := url.Values{
		"grant_type":    {"authorization_code"},
		"code":          {code},
		"redirect_uri":  {l.cfg.RedirectURL},
		"client_id":     {l.cfg.ClientID},
		"code_verifier": {verifier},
	}
	if l.cfg.Resource != "" {
		form.Set("resource", l.cfg.Resource)
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, doc.TokenEndpoint, bytes.NewReader([]byte(form.Encode())))
	if err != nil {
		return "", 0, err
	}
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	req.Header.Set("Accept", "application/json")
	if l.cfg.ClientSecret != "" {
		// RFC 6749 section 2.3.1: the id and secret are form-urlencoded before
		// they are joined and Base64-encoded.
		req.SetBasicAuth(url.QueryEscape(l.cfg.ClientID), url.QueryEscape(l.cfg.ClientSecret))
	}

	var tok struct {
		AccessToken string  `json:"access_token"`
		TokenType   string  `json:"token_type"`
		ExpiresIn   float64 `json:"expires_in"`
	}
	if err := l.doJSON(req, &tok); err != nil {
		return "", 0, err
	}
	if tok.AccessToken == "" {
		return "", 0, errors.New("the token response carries no access token")
	}
	// A token that is not a bearer token is not one this page can present
	// (RFC 6750); DPoP and similar would need their own proof on each request.
	if !strings.EqualFold(tok.TokenType, "bearer") {
		return "", 0, fmt.Errorf("the token response is of type %q, not bearer", tok.TokenType)
	}

	lifetime := defaultTokenLifetime
	if tok.ExpiresIn > 0 {
		lifetime = time.Duration(tok.ExpiresIn * float64(time.Second))
	}
	lifetime = min(lifetime, maxSession) - sessionSkew
	if lifetime <= 0 {
		return "", 0, errors.New("the access token expires too soon to use")
	}

	return tok.AccessToken, lifetime, nil
}

// logout ends the session. A POST, checked like an answer, so another site
// cannot sign an approver out.
func (h *Handler) logout(w http.ResponseWriter, r *http.Request) {
	if !fromThisPage(r) {
		render(w, http.StatusForbidden, noticePage, notice{Title: "This request did not come from the gate page"})
		return
	}
	http.SetCookie(w, clearing(sessionCookieName))
	render(w, http.StatusOK, noticePage, notice{Title: "Signed out"})
}
