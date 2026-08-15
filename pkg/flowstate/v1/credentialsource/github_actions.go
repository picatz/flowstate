package credentialsource

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"sync"
	"time"

	"github.com/picatz/jose/pkg/jwt"
)

// Environment variables GitHub Actions sets inside a job granted
// `id-token: write`, and no job without that permission.
const (
	envRequestURL   = "ACTIONS_ID_TOKEN_REQUEST_URL"
	envRequestToken = "ACTIONS_ID_TOKEN_REQUEST_TOKEN"
)

// maxTokenResponseBytes bounds the runner's response before it is read into
// memory. A token is a couple of kilobytes; anything approaching this is not
// a token, and an unbounded read of a response body is the mistake this
// repository keeps a rule about.
const maxTokenResponseBytes = 64 << 10

// DefaultGitHubActionsRefreshMargin is how long before its "exp" claim a
// cached token is re-minted, mirroring
// [flowstate/v1/auth.DefaultRefreshMargin] on the outbound side of the same
// idea: refresh shortly before expiry rather than exactly at it, so a request
// already in flight when the margin is crossed still completes against a
// token that has not yet died.
const DefaultGitHubActionsRefreshMargin = time.Minute

// githubActionsSource asks the GitHub Actions runner's OIDC token endpoint for
// a token addressed to one audience, and caches it until shortly before it
// expires.
//
// The endpoint and its request token are read from the environment on every
// mint, not captured once at construction — the same re-read discipline
// [fileSource] uses, in case a caller's process environment changes between
// calls (a re-exec, a test harness). What is cached is the *token*, not the
// coordinates for getting one, and only until [Token.ExpiresWithin] the
// refresh margin says otherwise.
type githubActionsSource struct {
	audience   string
	httpClient *http.Client
	clock      func() time.Time
	margin     time.Duration

	mu     sync.Mutex
	cached Token
}

// GitHubActionsOption configures a [Source] built by [NewGitHubActionsSource].
type GitHubActionsOption func(*githubActionsSource)

// WithGitHubActionsHTTPClient overrides the HTTP client used to reach the
// runner's token endpoint. Exists for tests; production callers get
// [http.DefaultClient].
func WithGitHubActionsHTTPClient(client *http.Client) GitHubActionsOption {
	return func(s *githubActionsSource) { s.httpClient = client }
}

// WithGitHubActionsClock overrides the clock used to decide whether the
// cached token needs re-minting. Exists for tests.
func WithGitHubActionsClock(clock func() time.Time) GitHubActionsOption {
	return func(s *githubActionsSource) { s.clock = clock }
}

// WithGitHubActionsRefreshMargin overrides [DefaultGitHubActionsRefreshMargin].
func WithGitHubActionsRefreshMargin(margin time.Duration) GitHubActionsOption {
	return func(s *githubActionsSource) { s.margin = margin }
}

// NewGitHubActionsSource returns a [Source] that mints tokens from a GitHub
// Actions runner's OIDC token endpoint, addressed to audience.
//
// audience is required: a token minted with no specific relying party in mind
// is one any relying party would accept, which is the same reason
// [flowstate/v1/auth.Issuer.Mint] refuses an empty audience on the outbound
// side.
func NewGitHubActionsSource(audience string, opts ...GitHubActionsOption) (Source, error) {
	if audience == "" {
		return nil, fmt.Errorf("%w: %s needs an audience naming the Flowstate server this token is for",
			ErrSourceUnusable, SourceGitHubActions)
	}

	s := &githubActionsSource{
		audience:   audience,
		httpClient: http.DefaultClient,
		clock:      time.Now,
		margin:     DefaultGitHubActionsRefreshMargin,
	}
	for _, opt := range opts {
		opt(s)
	}

	return s, nil
}

func (s *githubActionsSource) Name() string { return SourceGitHubActions }

// Token returns a cached token when it is not yet within its refresh margin
// of expiring, and mints a fresh one otherwise.
//
// A mint failure never falls back to the stale cached token, even one that
// technically still has a few seconds left: presenting a credential known to
// be about to die is exactly the mid-script failure this source exists to
// prevent, so a failed re-mint is reported rather than papered over with
// borrowed time.
func (s *githubActionsSource) Token(ctx context.Context) (Token, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	now := s.clock()
	if !s.cached.IsZero() && !s.cached.ExpiresWithin(s.margin, now) {
		return s.cached, nil
	}

	token, err := s.mint(ctx)
	if err != nil {
		return Token{}, err
	}

	s.cached = token
	return token, nil
}

func (s *githubActionsSource) mint(ctx context.Context) (Token, error) {
	requestURL := os.Getenv(envRequestURL)
	requestToken := os.Getenv(envRequestToken)
	if requestURL == "" || requestToken == "" {
		return Token{}, fmt.Errorf("%w: %s and %s are unset; this job needs `id-token: write` "+
			"to mint a GitHub Actions OIDC token", ErrSourceUnusable, envRequestURL, envRequestToken)
	}

	target, err := url.Parse(requestURL)
	if err != nil {
		return Token{}, fmt.Errorf("%w: %s is not a URL: %w", ErrSourceUnusable, envRequestURL, err)
	}

	// The endpoint already carries an api-version query parameter, so the
	// audience is added to whatever is there rather than replacing it.
	query := target.Query()
	query.Set("audience", s.audience)
	target.RawQuery = query.Encode()

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, target.String(), nil)
	if err != nil {
		return Token{}, fmt.Errorf("%w: %w", ErrSourceUnusable, err)
	}
	req.Header.Set("Authorization", "Bearer "+requestToken)
	req.Header.Set("Accept", "application/json")

	resp, err := s.httpClient.Do(req)
	if err != nil {
		return Token{}, fmt.Errorf("%w: requesting a token from the runner: %w", ErrSourceUnusable, err)
	}
	defer func() { _ = resp.Body.Close() }()

	// One byte past the limit, so a response at exactly the limit still parses
	// and one over it is refused rather than silently truncated.
	body, err := io.ReadAll(io.LimitReader(resp.Body, maxTokenResponseBytes+1))
	if err != nil {
		return Token{}, fmt.Errorf("%w: reading the runner's response: %w", ErrSourceUnusable, err)
	}
	if len(body) > maxTokenResponseBytes {
		return Token{}, fmt.Errorf("%w: the runner's response is larger than %d bytes, which is not a token",
			ErrSourceUnusable, maxTokenResponseBytes)
	}
	if resp.StatusCode != http.StatusOK {
		return Token{}, fmt.Errorf("%w: runner refused to mint a token (status %d)",
			ErrSourceUnusable, resp.StatusCode)
	}

	var minted struct {
		Value string `json:"value"`
	}
	if err := json.Unmarshal(body, &minted); err != nil {
		return Token{}, fmt.Errorf("%w: runner response is not the JSON object this endpoint documents: %w",
			ErrSourceUnusable, err)
	}
	if minted.Value == "" {
		return Token{}, fmt.Errorf("%w: runner returned an empty token", ErrSourceUnusable)
	}

	expiresAt, err := unverifiedExpiry(minted.Value)
	if err != nil {
		return Token{}, fmt.Errorf("%w: minted token has no readable expiry: %w", ErrSourceUnusable, err)
	}

	return newToken(SourceGitHubActions, minted.Value, expiresAt), nil
}

// unverifiedExpiry reads a token's "exp" claim without checking its signature.
//
// That is not a gap: this package never decides whether a token is
// trustworthy, only when to ask its issuer for a new one. The server's
// OIDCVerifier makes the trust decision, against its own policy, when the
// token actually arrives — so a wrong answer here costs an extra mint at
// worst, never a wrongly-trusted token.
func unverifiedExpiry(raw string) (time.Time, error) {
	token, err := jwt.Parse(raw)
	if err != nil {
		return time.Time{}, fmt.Errorf("parsing token: %w", err)
	}

	expValue, ok := token.Claims[jwt.ExpirationTime]
	if !ok {
		return time.Time{}, fmt.Errorf("token has no %q claim", jwt.ExpirationTime)
	}
	expInt, ok := expValue.(int64)
	if !ok {
		return time.Time{}, fmt.Errorf("%q claim is not a number", jwt.ExpirationTime)
	}

	return time.Unix(expInt, 0), nil
}
