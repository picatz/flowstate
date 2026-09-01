package main

import (
	"context"
	"crypto"
	"crypto/rand"
	"crypto/rsa"
	"crypto/sha256"
	"crypto/x509"
	"encoding/base64"
	"encoding/json"
	"encoding/pem"
	"fmt"
	"io"
	"net/http"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/picatz/flowstate/pkg/flowstate/v1/plugin/sdk"
)

// secretScheme is the compatibility provider for the worker-wide GitHub App
// or PAT configured below. Plugin tasks declare token through secret_inputs,
// so the host may resolve this scheme or any other configured provider before
// invoking the task.
const secretScheme = "github"

// Environment variables this plugin reads at startup. All are optional
// individually, but at least one complete auth mode (App or PAT) must be
// configured for token resolution to succeed - see resolveSecret.
const (
	envAppID          = "GITHUB_APP_ID"
	envAppPrivateKey  = "GITHUB_APP_PRIVATE_KEY" // PEM, PKCS#1 or PKCS#8
	envAppInstallID   = "GITHUB_APP_INSTALLATION_ID"
	envPAT            = "GITHUB_TOKEN"
	envAPIBaseURL     = "GITHUB_API_BASE_URL" // override for GitHub Enterprise Server
	defaultAPIBaseURL = "https://api.github.com"
)

// appCredentials is a GitHub App's identity, read once at startup. A private
// key held as a plain field (rather than a closure) is the one exception to
// this codebase's "hold material in a closure" rule, and it is deliberate:
// [x509.ParsePKCS1PrivateKey] and its PKCS#8 counterpart return an
// *rsa.PrivateKey, a type this plugin does not control and cannot make
// reflection-opaque, and %v on an *rsa.PrivateKey already redacts its own
// value - crypto/rsa's PrivateKey has no exported field fmt can print
// through that would print the key material itself (D, Primes, etc. are
// exported, actually - see auth_test.go's containment test, which is why
// this comment does not get to simply assert the point).
type appCredentials struct {
	appID          string
	installationID string
	privateKey     *rsa.PrivateKey
}

// authConfig is read once, at process start, from environment variables -
// the same convention flowstate-plugin-example uses for its own secret
// scheme, and consistent with this plugin's own "vcs" sibling.
//
// pat is a closure rather than a plain string field for the reason
// CLAUDE.md gives for holding secret material that way: fmt reaches an
// ordinary field through reflection regardless of whether any caller today
// actually prints an authConfig, and a plain string field is a hazard
// waiting for whoever adds that first debug log line. See auth_test.go's
// TestAuthConfigDoesNotPrintItsToken - the test this codebase's own rule
// says to write - for the containment shapes this has to survive: %v, %+v,
// %#v, and %s, on the value and on a slice of them.
type authConfig struct {
	app     *appCredentials // nil when no GitHub App is configured
	pat     func() string   // nil or returns "" when no personal access token is configured
	baseURL string
}

func (cfg authConfig) patValue() string {
	if cfg.pat == nil {
		return ""
	}
	return cfg.pat()
}

func loadAuthConfig() (authConfig, error) {
	cfg := authConfig{baseURL: strings.TrimSuffix(os.Getenv(envAPIBaseURL), "/")}
	if cfg.baseURL == "" {
		cfg.baseURL = defaultAPIBaseURL
	}

	appID := os.Getenv(envAppID)
	keyPEM := os.Getenv(envAppPrivateKey)
	installID := os.Getenv(envAppInstallID)

	switch {
	case appID == "" && keyPEM == "" && installID == "":
		// No App configured; PAT-only is a legitimate configuration.
	case appID != "" && keyPEM != "" && installID != "":
		key, err := parsePrivateKey(keyPEM)
		if err != nil {
			return authConfig{}, fmt.Errorf("%s: %w", envAppPrivateKey, err)
		}
		cfg.app = &appCredentials{appID: appID, installationID: installID, privateKey: key}
	default:
		// Fail closed on a half-configured App: silently falling back to PAT
		// (or to no auth at all) when an operator plainly intended App auth
		// would run every request as the wrong identity without saying so.
		return authConfig{}, fmt.Errorf(
			"%s, %s, and %s must be set together or not at all", envAppID, envAppPrivateKey, envAppInstallID)
	}

	if pat := os.Getenv(envPAT); pat != "" {
		cfg.pat = func() string { return pat }
	}

	if cfg.app == nil && cfg.patValue() == "" {
		// Not an error: a plugin that only ever talks to public repositories
		// and public data needs no credential, and unauthenticated requests
		// are a real, supported GitHub API mode (at a much lower rate
		// limit). Tasks that need a credential and were not given one fail
		// at the point they discover that, with a message that says so.
	}

	return cfg, nil
}

func parsePrivateKey(pemText string) (*rsa.PrivateKey, error) {
	block, _ := pem.Decode([]byte(pemText))
	if block == nil {
		return nil, fmt.Errorf("does not contain a PEM block")
	}
	if key, err := x509.ParsePKCS1PrivateKey(block.Bytes); err == nil {
		return key, nil
	}
	key, err := x509.ParsePKCS8PrivateKey(block.Bytes)
	if err != nil {
		return nil, fmt.Errorf("not a PKCS#1 or PKCS#8 RSA private key: %w", err)
	}
	rsaKey, ok := key.(*rsa.PrivateKey)
	if !ok {
		return nil, fmt.Errorf("is a %T, not an RSA private key - GitHub Apps require RS256", key)
	}
	return rsaKey, nil
}

// installationTokenCache holds the one installation access token this
// process has minted, so a run of many steps against the same App does not
// mint a fresh token - and spend the App's own rate-limited JWT-auth
// endpoint - on every single task call. There is exactly one entry because
// this plugin process is configured with exactly one App installation; a
// deployment needing several installations runs several plugin processes,
// one per installation, the same way it would run one plugin process per
// distinct PAT.
type installationTokenCache struct {
	mu      sync.Mutex
	token   string
	expires time.Time
}

var installTokenCache installationTokenCache

// installationRefreshSkew is how long before actual expiry this plugin
// treats a cached installation token as already expired, so a token is never
// handed to a task with less than this much life left in it - a task's own
// HTTP request plus GitHub's own clock skew could otherwise turn a
// technically-valid token into one that expires mid-request.
const installationRefreshSkew = 2 * time.Minute

// resolveSecret answers `${secret('github:...')}` for this plugin's own
// scheme.
//
// It ignores the reference's Name entirely and always returns the one
// credential this process is configured for - there is exactly one "github:
// token" this plugin can produce, the one its own environment names, so
// there is nothing for a workflow-supplied name to select between. A future
// version supporting several Apps or several PATs per process would need the
// name to mean something; today it does not, and pretending otherwise would
// invite an author to write `${secret('github:some-other-token')}` and get
// the same credential back with no diagnostic explaining why.
func resolveSecret(ctx context.Context, req sdk.SecretRequest) (sdk.SecretResponse, error) {
	if req.Namespace != "" {
		return sdk.SecretResponse{}, sdk.PermissionDenied(
			"the github secret provider is configured once for the worker and cannot resolve credentials for namespace %q; configure a namespace-aware host secret provider instead",
			req.Namespace)
	}

	cfg, err := loadAuthConfig()
	if err != nil {
		return sdk.SecretResponse{}, sdk.Failed("this plugin's authentication is misconfigured: %v", err)
	}

	if cfg.app != nil {
		token, expires, err := installationToken(ctx, cfg)
		if err != nil {
			return sdk.SecretResponse{}, err
		}
		ttl := time.Until(expires) - installationRefreshSkew
		if ttl < 0 {
			ttl = 0
		}
		return sdk.SecretResponse{Value: []byte(token), ExpiresIn: ttl}, nil
	}

	if pat := cfg.patValue(); pat != "" {
		// A PAT's lifetime is whatever GitHub's own settings say and this
		// plugin has no way to learn it, so ExpiresIn is left zero - the
		// engine applies its own default caching duration, which is safe
		// because revoking a PAT on GitHub's side takes effect for any
		// *new* request immediately regardless of how long this plugin
		// cached the string.
		return sdk.SecretResponse{Value: []byte(pat)}, nil
	}

	return sdk.SecretResponse{}, sdk.NotFound(
		"no GitHub credential is configured on this worker (%s/%s/%s for a GitHub App, or %s for a token)",
		envAppID, envAppPrivateKey, envAppInstallID, envPAT)
}

// installationToken returns a cached token when one is still fresh enough,
// and mints a new one otherwise.
func installationToken(ctx context.Context, cfg authConfig) (string, time.Time, error) {
	installTokenCache.mu.Lock()
	defer installTokenCache.mu.Unlock()

	if installTokenCache.token != "" && time.Now().Before(installTokenCache.expires.Add(-installationRefreshSkew)) {
		return installTokenCache.token, installTokenCache.expires, nil
	}

	token, expires, err := mintInstallationToken(ctx, cfg)
	if err != nil {
		return "", time.Time{}, err
	}
	installTokenCache.token = token
	installTokenCache.expires = expires
	return token, expires, nil
}

// mintInstallationToken is the GitHub App credential exchange this plugin
// prefers over a long-lived PAT: sign a short-lived JWT with the App's own
// private key (never sent anywhere; it only ever signs locally), present it
// to GitHub, and receive back an installation access token scoped to
// whatever repositories and permissions the installation itself grants -
// typically a small subset of an organization's repositories, and never more
// than the App's own configured permission set. That is strictly narrower
// than a PAT, which carries its owning user's full account permissions
// (or, for a fine-grained PAT, whatever the user chose - but still a
// standing grant that exists until someone remembers to revoke it, not one
// that expires within the hour on its own).
//
// # Why this is not "workload identity federation" in this repository's own
// # sense
//
// pkg/flowstate/v1/auth/federation.go's broker exchanges a Flowstate-issued
// OIDC assertion for a credential from an external relying party - AWS STS,
// GCP Workload Identity Federation, or an RFC 8693 token exchange endpoint.
// GitHub Apps have no equivalent: there is no "present an external OIDC
// token, receive an installation token" endpoint, because a GitHub App's
// only proof of identity GitHub accepts is a JWT signed with that specific
// App's own registered key. There is nothing for Flowstate's issuer to
// federate into - the trust anchor is the App's key, not an identity
// provider GitHub has agreed to trust.
//
// That means the ranking this plugin was asked to implement - federation,
// then App, then PAT, in order of preference - collapses to two rungs for
// GitHub specifically: this mode is the best available, and it does not
// route through [auth.Broker] at all. It is reported this way rather than
// forced into the broker's shape, because pretending a GitHub App
// credential is what the broker means by "federation" would be exactly the
// kind of dishonest abstraction CLAUDE.md and this engagement's own design
// review warn against.
//
// There is a second, independent reason this plugin could not have used the
// broker even where a forge does support real federation: [auth.Broker.
// Authorize] runs inside the engine's own process, mutating an *http.Request
// directly, and nothing exposes it - or the workload identity assertion it
// would need - to a plugin process at all. See the README, "SDK gaps," for
// this stated as its own finding independent of GitHub's auth model.
func mintInstallationToken(ctx context.Context, cfg authConfig) (string, time.Time, error) {
	jwt, err := buildAppJWT(cfg.app)
	if err != nil {
		return "", time.Time{}, sdk.Failed("signing the GitHub App JWT: %v", err)
	}

	url := cfg.baseURL + "/app/installations/" + cfg.app.installationID + "/access_tokens"
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, url, nil)
	if err != nil {
		return "", time.Time{}, sdk.Failed("building the installation-token request: %v", err)
	}
	req.Header.Set("Authorization", "Bearer "+jwt)
	req.Header.Set("Accept", "application/vnd.github+json")
	req.Header.Set("X-GitHub-Api-Version", "2022-11-28")

	governed, err := egressClient()
	if err != nil {
		return "", time.Time{}, err
	}

	resp, err := governed.Do(req)
	if err != nil {
		return "", time.Time{}, sdk.Unavailable("reaching the GitHub API to mint an installation token: %v", err)
	}
	defer resp.Body.Close()

	body, err := io.ReadAll(io.LimitReader(resp.Body, maxResponseBytes))
	if err != nil {
		return "", time.Time{}, sdk.Unavailable("reading the installation-token response: %v", err)
	}

	if resp.StatusCode != http.StatusCreated {
		return "", time.Time{}, classifyStatus(resp.StatusCode, resp.Header.Get("Retry-After"), string(body))
	}

	var out struct {
		Token     string `json:"token"`
		ExpiresAt string `json:"expires_at"`
	}
	if err := json.Unmarshal(body, &out); err != nil {
		return "", time.Time{}, sdk.Failed("parsing the installation-token response: %v", err)
	}

	expires, err := time.Parse(time.RFC3339, out.ExpiresAt)
	if err != nil {
		return "", time.Time{}, sdk.Failed("installation token has an unparseable expiry: %v", err)
	}

	return out.Token, expires, nil
}

// buildAppJWT signs a JWT the way GitHub's App authentication requires:
// RS256, an "iat" a little in the past to tolerate clock skew between this
// worker and GitHub, and an "exp" within GitHub's own 10-minute ceiling.
//
// Built by hand with the standard library rather than a JWT dependency,
// consistent with this whole plugin's "own your dependencies" constraint -
// a JWT for this one purpose is three base64url segments and one RSA
// signature, not a reason to take on a general-purpose JWT library's own
// dependency surface and parsing paths for the one shape this ever needs to
// produce (never verify - this plugin only ever signs, never accepts a JWT
// from anyone else).
func buildAppJWT(app *appCredentials) (string, error) {
	now := time.Now()
	header := map[string]string{"alg": "RS256", "typ": "JWT"}
	claims := map[string]any{
		"iat": now.Add(-60 * time.Second).Unix(),
		"exp": now.Add(9 * time.Minute).Unix(),
		"iss": app.appID,
	}

	headerJSON, err := json.Marshal(header)
	if err != nil {
		return "", err
	}
	claimsJSON, err := json.Marshal(claims)
	if err != nil {
		return "", err
	}

	signingInput := base64URLEncode(headerJSON) + "." + base64URLEncode(claimsJSON)

	digest := sha256.Sum256([]byte(signingInput))
	signature, err := rsa.SignPKCS1v15(rand.Reader, app.privateKey, crypto.SHA256, digest[:])
	if err != nil {
		return "", err
	}

	return signingInput + "." + base64URLEncode(signature), nil
}

func base64URLEncode(b []byte) string {
	return base64.RawURLEncoding.EncodeToString(b)
}

// classifyStatus turns a GitHub API response's status code into the sdk's
// classification. Shared between the App token-minting call above and every
// task's own request handling (see errors.go, which is the fuller version of
// this covering go-github's own error types); this copy exists because
// minting a token happens before this plugin ever constructs a *github.
// Client and so cannot reuse the go-github-specific classifier.
func classifyStatus(status int, retryAfter, body string) error {
	msg := truncateForError(body, 512)
	switch {
	case status == http.StatusNotFound:
		return sdk.NotFound("GitHub returned 404: %s", msg)
	case status == http.StatusUnauthorized:
		return sdk.PermissionDenied("GitHub rejected the credential: %s", msg)
	case status == http.StatusForbidden:
		if d, ok := parseRetryAfter(retryAfter); ok {
			return sdk.Unavailable("GitHub rate-limited this request; retry after %s: %s", d, msg)
		}
		return sdk.PermissionDenied("GitHub returned 403: %s", msg)
	case status == http.StatusUnprocessableEntity:
		return sdk.InvalidInput("GitHub rejected the request: %s", msg)
	case status >= 500:
		return sdk.Unavailable("GitHub returned %d: %s", status, msg)
	default:
		return sdk.Failed("GitHub returned %d: %s", status, msg)
	}
}

func parseRetryAfter(v string) (time.Duration, bool) {
	if v == "" {
		return 0, false
	}
	if n, err := strconv.Atoi(v); err == nil {
		return time.Duration(n) * time.Second, true
	}
	return 0, false
}

func truncateForError(s string, n int) string {
	if len(s) <= n {
		return s
	}
	for n > 0 && !isRuneStart(s[n]) {
		n--
	}
	return s[:n] + "..."
}

func isRuneStart(b byte) bool { return b&0xC0 != 0x80 }
