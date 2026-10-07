package deviceflow

import (
	"bytes"
	"cmp"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net"
	"net/http"
	"net/url"
	"strings"
	"time"
	"unicode"
)

// Limits on work an IdP controls. Each is a work bound, not a reporting one: a
// response past it is refused, not truncated.
const (
	// MaxResponseBytes bounds every response body read from the IdP.
	MaxResponseBytes = 256 << 10

	// RequestTimeout bounds one HTTP exchange with the IdP.
	RequestTimeout = 30 * time.Second

	// DefaultPollInterval is the interval used when the device authorization
	// response carries none ([RFC 8628 §3.2]).
	//
	// [RFC 8628 §3.2]: https://www.rfc-editor.org/rfc/rfc8628#section-3.2
	DefaultPollInterval = 5 * time.Second

	// SlowDownIncrement is what a slow_down answer adds to the polling
	// interval ([RFC 8628 §3.5]).
	//
	// [RFC 8628 §3.5]: https://www.rfc-editor.org/rfc/rfc8628#section-3.5
	SlowDownIncrement = 5 * time.Second

	// MaxPollInterval caps the interval, however the IdP got it there.
	MaxPollInterval = 5 * time.Minute

	// MaxDeviceCodeLifetime caps expires_in: a device code that claims to live
	// for a day does not make a terminal wait for one.
	MaxDeviceCodeLifetime = 30 * time.Minute

	maxRedirects = 3

	grantDeviceCode   = "urn:ietf:params:oauth:grant-type:device_code"
	grantRefreshToken = "refresh_token"
)

// Sentinel errors. Distinguish them with [errors.Is].
var (
	// ErrInsecureURL is returned for an issuer, endpoint or redirect that is
	// neither https nor http on a loopback host.
	ErrInsecureURL = errors.New("deviceflow: URL must be https (http is allowed only on a loopback host)")

	// ErrResponseTooLarge is returned when an IdP response exceeds
	// [MaxResponseBytes].
	ErrResponseTooLarge = errors.New("deviceflow: response from the identity provider is too large")

	// ErrCrossOriginRedirect is returned when a request is redirected to a
	// different origin, or too many times.
	ErrCrossOriginRedirect = errors.New("deviceflow: refusing a redirect away from the original origin")

	// ErrInvalidDiscovery is returned for a discovery document that is
	// malformed, names a different issuer, or lacks a required endpoint.
	ErrInvalidDiscovery = errors.New("deviceflow: invalid OpenID provider configuration")

	// ErrAccessDenied is returned when the user declined the request
	// (access_denied, [RFC 8628 §3.5]).
	//
	// [RFC 8628 §3.5]: https://www.rfc-editor.org/rfc/rfc8628#section-3.5
	ErrAccessDenied = errors.New("deviceflow: the sign-in request was denied")

	// ErrExpiredToken is returned when the device code expired before the user
	// approved it, whether the IdP said so (expired_token) or expires_in
	// elapsed locally.
	ErrExpiredToken = errors.New("deviceflow: the device code expired before the sign-in was approved")

	// ErrNoRefreshToken is returned by [Client.Refresh] when there is nothing
	// to refresh with.
	ErrNoRefreshToken = errors.New("deviceflow: no refresh token")

	// ErrRevocationUnsupported is returned by [Client.Revoke] when the IdP
	// advertises no revocation_endpoint.
	ErrRevocationUnsupported = errors.New("deviceflow: the identity provider advertises no revocation endpoint")
)

// OAuthError is an error response from the IdP ([RFC 6749 §5.2]). Both fields
// are untrusted text, stripped of control characters and truncated.
//
// [RFC 6749 §5.2]: https://www.rfc-editor.org/rfc/rfc6749#section-5.2
type OAuthError struct {
	Code        string
	Description string
	Status      int
}

// Error implements error.
func (e *OAuthError) Error() string {
	if e.Description == "" {
		return fmt.Sprintf("identity provider refused the request: %s", e.Code)
	}
	return fmt.Sprintf("identity provider refused the request: %s: %s", e.Code, e.Description)
}

// Config names what a sign-in is for.
type Config struct {
	// Issuer is the OIDC issuer URL, exactly as the IdP's discovery document
	// spells it.
	Issuer string

	// ClientID identifies this CLI as a public client at the IdP.
	ClientID string

	// Scope is the space-delimited scope to request. Empty requests none.
	Scope string

	// Audience, when set, is sent as the audience parameter some IdPs (Auth0,
	// for one) use to choose the API a token is for.
	Audience string
}

// Endpoints are the discovered URLs a login uses.
type Endpoints struct {
	// Issuer is the issuer the document vouched for.
	Issuer string `json:"issuer"`

	// DeviceAuthorization is the device_authorization_endpoint.
	DeviceAuthorization string `json:"device_authorization_endpoint,omitempty"`

	// Token is the token_endpoint.
	Token string `json:"token_endpoint"`

	// Revocation is the revocation_endpoint, empty when not advertised.
	Revocation string `json:"revocation_endpoint,omitempty"`
}

// Authorization is the device authorization response ([RFC 8628 §3.2]), with
// the text fields sanitized for a terminal.
//
// [RFC 8628 §3.2]: https://www.rfc-editor.org/rfc/rfc8628#section-3.2
type Authorization struct {
	// DeviceCode is secret: it is what the poll presents, and is not shown to
	// the user.
	DeviceCode string

	// UserCode is what the user types at the verification URI.
	UserCode string

	// VerificationURI is where the user goes.
	VerificationURI string

	// VerificationURIComplete, when set, embeds UserCode for a QR code or a
	// single click.
	VerificationURIComplete string

	// ExpiresIn is how long the device code lives, capped at
	// [MaxDeviceCodeLifetime].
	ExpiresIn time.Duration

	// Interval is the minimum polling interval, [DefaultPollInterval] when
	// the IdP gave none.
	Interval time.Duration
}

// Format keeps the device code out of any verb that reaches the struct.
func (a Authorization) Format(f fmt.State, _ rune) {
	_, _ = fmt.Fprintf(f, "device authorization for user code %s", a.UserCode)
}

// LogValue implements [slog.LogValuer] without the device code.
func (a Authorization) LogValue() slog.Value {
	return slog.GroupValue(
		slog.String("user_code", a.UserCode),
		slog.String("verification_uri", a.VerificationURI),
		slog.Duration("expires_in", a.ExpiresIn),
	)
}

// MarshalJSON omits the device code, as [Tokens.MarshalJSON] omits its
// secrets.
func (a Authorization) MarshalJSON() ([]byte, error) {
	return json.Marshal(struct {
		UserCode                string `json:"user_code"`
		VerificationURI         string `json:"verification_uri"`
		VerificationURIComplete string `json:"verification_uri_complete,omitempty"`
		ExpiresIn               int64  `json:"expires_in"`
		Interval                int64  `json:"interval"`
	}{a.UserCode, a.VerificationURI, a.VerificationURIComplete, int64(a.ExpiresIn / time.Second), int64(a.Interval / time.Second)})
}

// Client runs the flow. The zero value is not usable; build one with [New].
type Client struct {
	http  *http.Client
	now   func() time.Time
	sleep func(context.Context, time.Duration) error
}

// Option configures a [Client].
type Option func(*Client)

// WithHTTPClient sets the transport. Its redirect policy is replaced with the
// same-origin one, and a zero Timeout becomes [RequestTimeout].
func WithHTTPClient(hc *http.Client) Option {
	return func(c *Client) {
		cp := *hc
		c.http = &cp
	}
}

// WithClock replaces time.Now, for expiry arithmetic.
func WithClock(now func() time.Time) Option {
	return func(c *Client) { c.now = now }
}

// WithSleeper replaces the wait between polls. It must return when the
// context ends.
func WithSleeper(sleep func(context.Context, time.Duration) error) Option {
	return func(c *Client) { c.sleep = sleep }
}

// New builds a [Client].
func New(opts ...Option) *Client {
	c := &Client{now: time.Now, sleep: sleepContext}
	for _, opt := range opts {
		opt(c)
	}
	if c.http == nil {
		c.http = &http.Client{}
	}
	if c.http.Timeout == 0 {
		c.http.Timeout = RequestTimeout
	}
	c.http.CheckRedirect = sameOriginRedirect
	return c
}

func sleepContext(ctx context.Context, d time.Duration) error {
	timer := time.NewTimer(d)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		return nil
	}
}

// sameOriginRedirect allows a few redirects, none leaving the origin of the
// request that started them.
func sameOriginRedirect(req *http.Request, via []*http.Request) error {
	if len(via) > maxRedirects {
		return ErrCrossOriginRedirect
	}
	first := via[0].URL
	if req.URL.Scheme != first.Scheme || req.URL.Host != first.Host {
		return ErrCrossOriginRedirect
	}
	return nil
}

// RequireSecureURL reports whether raw may be dialed: an https URL, or an http
// URL whose host is loopback, with a host and no userinfo.
func RequireSecureURL(raw string) error {
	u, err := url.Parse(raw)
	if err != nil || u.Host == "" || u.User != nil || u.Fragment != "" {
		return fmt.Errorf("%w: %q is not an absolute URL", ErrInsecureURL, clean(raw, 200))
	}
	switch u.Scheme {
	case "https":
		return nil
	case "http":
		if isLoopbackHost(u.Hostname()) {
			return nil
		}
	}
	return fmt.Errorf("%w: %q", ErrInsecureURL, clean(raw, 200))
}

func isLoopbackHost(host string) bool {
	if strings.EqualFold(host, "localhost") {
		return true
	}
	ip := net.ParseIP(host)
	return ip != nil && ip.IsLoopback()
}

// clean strips control characters from untrusted text and truncates it.
func clean(s string, limit int) string {
	var b strings.Builder
	n := 0
	for _, r := range s {
		if unicode.IsControl(r) || !unicode.IsPrint(r) {
			continue
		}
		if n++; n > limit {
			b.WriteString("...")
			break
		}
		b.WriteRune(r)
	}
	return b.String()
}

// get fetches one URL and returns its bounded body.
func (c *Client) get(ctx context.Context, target string) (int, []byte, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, target, nil)
	if err != nil {
		return 0, nil, err
	}
	req.Header.Set("Accept", "application/json")
	return c.do(req)
}

// post sends a form to endpoint and returns the bounded response.
func (c *Client) post(ctx context.Context, endpoint string, form url.Values) (int, []byte, error) {
	if err := RequireSecureURL(endpoint); err != nil {
		return 0, nil, err
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, endpoint, strings.NewReader(form.Encode()))
	if err != nil {
		return 0, nil, err
	}
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	req.Header.Set("Accept", "application/json")
	return c.do(req)
}

func (c *Client) do(req *http.Request) (int, []byte, error) {
	resp, err := c.http.Do(req)
	if err != nil {
		// The URL error carries the request URL, which holds no secret: every
		// secret travels in the body.
		return 0, nil, err
	}
	defer func() { _ = resp.Body.Close() }()

	body, err := io.ReadAll(io.LimitReader(resp.Body, MaxResponseBytes+1))
	if err != nil {
		return 0, nil, fmt.Errorf("reading response from %s: %w", req.URL.Host, err)
	}
	if len(body) > MaxResponseBytes {
		return 0, nil, fmt.Errorf("%w (more than %d bytes from %s)", ErrResponseTooLarge, MaxResponseBytes, req.URL.Host)
	}
	return resp.StatusCode, body, nil
}

// discoveryDocument is the subset of the provider configuration used here.
type discoveryDocument struct {
	Issuer                      string `json:"issuer"`
	DeviceAuthorizationEndpoint string `json:"device_authorization_endpoint"`
	TokenEndpoint               string `json:"token_endpoint"`
	RevocationEndpoint          string `json:"revocation_endpoint"`
}

// Discover reads the provider configuration of issuer
// ([OpenID Connect Discovery 1.0 §4]) and returns the endpoints a device login
// needs. The issuer, the document's issuer value and every endpoint are
// checked: https (or loopback http), and the issuer must match exactly
// ([§4.3]).
//
// [OpenID Connect Discovery 1.0 §4]: https://openid.net/specs/openid-connect-discovery-1_0.html#ProviderConfig
// [§4.3]: https://openid.net/specs/openid-connect-discovery-1_0.html#ProviderConfigurationValidation
func (c *Client) Discover(ctx context.Context, issuer string) (Endpoints, error) {
	if err := RequireSecureURL(issuer); err != nil {
		return Endpoints{}, err
	}
	status, body, err := c.get(ctx, strings.TrimSuffix(issuer, "/")+"/.well-known/openid-configuration")
	if err != nil {
		return Endpoints{}, fmt.Errorf("discovering %s: %w", clean(issuer, 200), err)
	}
	if status != http.StatusOK {
		return Endpoints{}, fmt.Errorf("%w: discovery answered HTTP %d", ErrInvalidDiscovery, status)
	}
	var doc discoveryDocument
	if err := json.Unmarshal(body, &doc); err != nil {
		return Endpoints{}, fmt.Errorf("%w: not a JSON document", ErrInvalidDiscovery)
	}
	if doc.Issuer != issuer {
		return Endpoints{}, fmt.Errorf("%w: the document names issuer %q, not %q",
			ErrInvalidDiscovery, clean(doc.Issuer, 200), clean(issuer, 200))
	}
	if doc.TokenEndpoint == "" {
		return Endpoints{}, fmt.Errorf("%w: no token_endpoint", ErrInvalidDiscovery)
	}
	if doc.DeviceAuthorizationEndpoint == "" {
		return Endpoints{}, fmt.Errorf("%w: no device_authorization_endpoint; this identity provider "+
			"does not support the device authorization grant for this issuer", ErrInvalidDiscovery)
	}
	issuerURL, _ := url.Parse(issuer)
	issuerLoopback := issuerURL != nil && issuerURL.Scheme == "http" && isLoopbackHost(issuerURL.Hostname())
	for name, endpoint := range map[string]string{
		"token_endpoint":                doc.TokenEndpoint,
		"device_authorization_endpoint": doc.DeviceAuthorizationEndpoint,
		"revocation_endpoint":           doc.RevocationEndpoint,
	} {
		if endpoint == "" {
			continue
		}
		if err := RequireSecureURL(endpoint); err != nil {
			return Endpoints{}, fmt.Errorf("%w: %s: %w", ErrInvalidDiscovery, name, err)
		}
		// Plain http is for a loopback rehearsal, where the issuer is loopback
		// too; an https issuer pointing at http (even loopback) is a downgrade.
		if u, perr := url.Parse(endpoint); perr == nil && u.Scheme == "http" && !issuerLoopback {
			return Endpoints{}, fmt.Errorf("%w: %s: %w: an https issuer may not name an http endpoint",
				ErrInvalidDiscovery, name, ErrInsecureURL)
		}
	}
	return Endpoints{
		Issuer:              doc.Issuer,
		DeviceAuthorization: doc.DeviceAuthorizationEndpoint,
		Token:               doc.TokenEndpoint,
		Revocation:          doc.RevocationEndpoint,
	}, nil
}

type authorizationResponse struct {
	DeviceCode              string      `json:"device_code"`
	UserCode                string      `json:"user_code"`
	VerificationURI         string      `json:"verification_uri"`
	VerificationURL         string      `json:"verification_url"` // Google's pre-RFC spelling
	VerificationURIComplete string      `json:"verification_uri_complete"`
	ExpiresIn               json.Number `json:"expires_in"`
	Interval                json.Number `json:"interval"`
	Error                   string      `json:"error"`
	ErrorDescription        string      `json:"error_description"`
}

// Authorize requests a device code ([RFC 8628 §3.1] and [§3.2]).
//
// [RFC 8628 §3.1]: https://www.rfc-editor.org/rfc/rfc8628#section-3.1
// [§3.2]: https://www.rfc-editor.org/rfc/rfc8628#section-3.2
func (c *Client) Authorize(ctx context.Context, ep Endpoints, cfg Config) (Authorization, error) {
	form := url.Values{"client_id": {cfg.ClientID}}
	if cfg.Scope != "" {
		form.Set("scope", cfg.Scope)
	}
	if cfg.Audience != "" {
		form.Set("audience", cfg.Audience)
	}
	status, body, err := c.post(ctx, ep.DeviceAuthorization, form)
	if err != nil {
		return Authorization{}, fmt.Errorf("requesting a device code: %w", err)
	}
	var resp authorizationResponse
	if err := json.Unmarshal(body, &resp); err != nil {
		return Authorization{}, fmt.Errorf("requesting a device code: unexpected response (HTTP %d)", status)
	}
	if resp.Error != "" {
		return Authorization{}, &OAuthError{Code: clean(resp.Error, 64), Description: clean(resp.ErrorDescription, 200), Status: status}
	}
	if status != http.StatusOK || resp.DeviceCode == "" || resp.UserCode == "" {
		return Authorization{}, fmt.Errorf("requesting a device code: incomplete response (HTTP %d)", status)
	}

	verification := cmp.Or(resp.VerificationURI, resp.VerificationURL)
	if verification == "" {
		return Authorization{}, errors.New("requesting a device code: response has no verification_uri")
	}
	for _, u := range []string{verification, resp.VerificationURIComplete} {
		if u == "" {
			continue
		}
		if err := RequireSecureURL(u); err != nil {
			return Authorization{}, fmt.Errorf("requesting a device code: verification URI: %w", err)
		}
	}

	lifetime := seconds(resp.ExpiresIn)
	if lifetime <= 0 {
		return Authorization{}, errors.New("requesting a device code: response has no usable expires_in")
	}
	interval := seconds(resp.Interval)
	if interval <= 0 {
		interval = DefaultPollInterval
	}

	return Authorization{
		DeviceCode:              resp.DeviceCode,
		UserCode:                clean(resp.UserCode, 64),
		VerificationURI:         clean(verification, 512),
		VerificationURIComplete: clean(resp.VerificationURIComplete, 1024),
		ExpiresIn:               min(lifetime, MaxDeviceCodeLifetime),
		Interval:                min(interval, MaxPollInterval),
	}, nil
}

// seconds converts a JSON number of seconds, saturating instead of overflowing.
func seconds(n json.Number) time.Duration {
	f, err := n.Float64()
	if err != nil || f <= 0 {
		return 0
	}
	if f > float64(24*time.Hour/time.Second) {
		return 24 * time.Hour
	}
	return time.Duration(f * float64(time.Second))
}

type tokenResponse struct {
	AccessToken      string      `json:"access_token"`
	TokenType        string      `json:"token_type"`
	ExpiresIn        json.Number `json:"expires_in"`
	RefreshToken     string      `json:"refresh_token"`
	Scope            string      `json:"scope"`
	Error            string      `json:"error"`
	ErrorDescription string      `json:"error_description"`
}

// redact removes every submitted credential from text an IdP sent back. It
// runs on the whole string, before truncation, so a secret that straddles the
// cut is not left as a recognizable prefix.
func redact(s string, secrets []string) string {
	for _, secret := range secrets {
		if secret != "" {
			s = strings.ReplaceAll(s, secret, "[redacted]")
		}
	}
	return s
}

// parseTokens interprets a token endpoint answer: tokens, an *[OAuthError], or
// an error for anything else. previousRefresh is kept when a refresh answer
// carries no new refresh token ([RFC 6749 §6]).
//
// [RFC 6749 §6]: https://www.rfc-editor.org/rfc/rfc6749#section-6
func (c *Client) parseTokens(status int, body []byte, previousRefresh string, submitted ...string) (Tokens, error) {
	var resp tokenResponse
	if err := json.Unmarshal(bytes.TrimSpace(body), &resp); err != nil {
		return Tokens{}, fmt.Errorf("unexpected response from the identity provider (HTTP %d)", status)
	}
	if resp.Error != "" {
		return Tokens{}, &OAuthError{
			Code:        clean(redact(resp.Error, submitted), 64),
			Description: clean(redact(resp.ErrorDescription, submitted), 200),
			Status:      status,
		}
	}
	if status != http.StatusOK || resp.AccessToken == "" {
		return Tokens{}, fmt.Errorf("incomplete token response from the identity provider (HTTP %d)", status)
	}
	if resp.TokenType != "" && !strings.EqualFold(resp.TokenType, "bearer") {
		return Tokens{}, fmt.Errorf("the identity provider issued a %q token; only bearer tokens are supported", clean(resp.TokenType, 32))
	}
	lifetime := seconds(resp.ExpiresIn)
	if lifetime <= 0 {
		lifetime = DefaultTokenLifetime
	}
	return Tokens{
		AccessToken:  resp.AccessToken,
		RefreshToken: cmp.Or(resp.RefreshToken, previousRefresh),
		ExpiresAt:    c.now().Add(lifetime),
		Scope:        clean(resp.Scope, 512),
	}, nil
}

// Poll exchanges the device code for tokens ([RFC 8628 §3.4]) until the user
// approves, declines, or the code expires. It waits the interval before each
// request, adds [SlowDownIncrement] on slow_down and keeps polling on
// authorization_pending ([RFC 8628 §3.5]). Expiry is [ErrExpiredToken], denial
// is [ErrAccessDenied], and any other error ends the poll.
//
// [RFC 8628 §3.4]: https://www.rfc-editor.org/rfc/rfc8628#section-3.4
// [RFC 8628 §3.5]: https://www.rfc-editor.org/rfc/rfc8628#section-3.5
func (c *Client) Poll(ctx context.Context, ep Endpoints, cfg Config, auth Authorization) (Tokens, error) {
	interval := min(cmp.Or(max(auth.Interval, 0), DefaultPollInterval), MaxPollInterval)
	deadline := c.now().Add(min(auth.ExpiresIn, MaxDeviceCodeLifetime))

	form := url.Values{
		"grant_type":  {grantDeviceCode},
		"device_code": {auth.DeviceCode},
		"client_id":   {cfg.ClientID},
	}
	for {
		// Never sleep past the code's lifetime: an interval longer than what
		// is left would hold the user at a code that has already expired.
		remaining := deadline.Sub(c.now())
		if remaining <= 0 {
			return Tokens{}, ErrExpiredToken
		}
		if err := c.sleep(ctx, min(interval, remaining)); err != nil {
			return Tokens{}, err
		}
		if !c.now().Before(deadline) {
			return Tokens{}, ErrExpiredToken
		}

		status, body, err := c.post(ctx, ep.Token, form)
		if err != nil {
			return Tokens{}, fmt.Errorf("polling for the sign-in: %w", err)
		}
		tokens, err := c.parseTokens(status, body, "", auth.DeviceCode)
		if err == nil {
			return tokens, nil
		}
		oauth, ok := errors.AsType[*OAuthError](err)
		if !ok {
			return Tokens{}, fmt.Errorf("polling for the sign-in: %w", err)
		}
		switch oauth.Code {
		case "authorization_pending":
		case "slow_down":
			interval = min(interval+SlowDownIncrement, MaxPollInterval)
		case "access_denied":
			return Tokens{}, ErrAccessDenied
		case "expired_token":
			return Tokens{}, ErrExpiredToken
		default:
			return Tokens{}, fmt.Errorf("polling for the sign-in: %w", oauth)
		}
	}
}

// Refresh renews tokens with the refresh token ([RFC 6749 §6]). When the IdP
// rotates the refresh token the new one replaces the old; when it does not,
// the old one is kept. A refusal is an *[OAuthError] (invalid_grant for a
// revoked or expired refresh token).
//
// [RFC 6749 §6]: https://www.rfc-editor.org/rfc/rfc6749#section-6
func (c *Client) Refresh(ctx context.Context, ep Endpoints, clientID string, prev Tokens) (Tokens, error) {
	if prev.RefreshToken == "" {
		return Tokens{}, ErrNoRefreshToken
	}
	status, body, err := c.post(ctx, ep.Token, url.Values{
		"grant_type":    {grantRefreshToken},
		"refresh_token": {prev.RefreshToken},
		"client_id":     {clientID},
	})
	if err != nil {
		return Tokens{}, fmt.Errorf("refreshing the sign-in: %w", err)
	}
	tokens, err := c.parseTokens(status, body, prev.RefreshToken, prev.RefreshToken)
	if err != nil {
		return Tokens{}, fmt.Errorf("refreshing the sign-in: %w", err)
	}
	return tokens, nil
}

// Revoke asks the IdP to invalidate token ([RFC 7009 §2]). hint is
// "refresh_token" or "access_token". It is [ErrRevocationUnsupported] when the
// IdP advertises no endpoint.
//
// [RFC 7009 §2]: https://www.rfc-editor.org/rfc/rfc7009#section-2
func (c *Client) Revoke(ctx context.Context, ep Endpoints, clientID, token, hint string) error {
	if ep.Revocation == "" {
		return ErrRevocationUnsupported
	}
	status, _, err := c.post(ctx, ep.Revocation, url.Values{
		"token":           {token},
		"token_type_hint": {hint},
		"client_id":       {clientID},
	})
	if err != nil {
		return fmt.Errorf("revoking the token: %w", err)
	}
	if status != http.StatusOK {
		return fmt.Errorf("revoking the token: the identity provider answered HTTP %d", status)
	}
	return nil
}

// Login discovers the issuer, requests a device code, hands it to prompt so the
// user can be shown where to go, then polls until the user decides. The
// returned [Entry] is ready for [Store.Save].
func (c *Client) Login(ctx context.Context, cfg Config, prompt func(Authorization)) (Entry, error) {
	if cfg.Issuer == "" || cfg.ClientID == "" {
		return Entry{}, errors.New("deviceflow: an issuer and a client ID are required")
	}
	ep, err := c.Discover(ctx, cfg.Issuer)
	if err != nil {
		return Entry{}, err
	}
	auth, err := c.Authorize(ctx, ep, cfg)
	if err != nil {
		return Entry{}, err
	}
	prompt(auth)
	tokens, err := c.Poll(ctx, ep, cfg, auth)
	if err != nil {
		return Entry{}, err
	}
	return Entry{Issuer: cfg.Issuer, ClientID: cfg.ClientID, Endpoints: ep, Tokens: tokens}, nil
}

// Renew refreshes e and returns the updated entry; the caller persists it.
func (c *Client) Renew(ctx context.Context, e Entry) (Entry, error) {
	tokens, err := c.Refresh(ctx, e.Endpoints, e.ClientID, e.Tokens)
	if err != nil {
		return e, err
	}
	e.Tokens = tokens
	return e, nil
}
