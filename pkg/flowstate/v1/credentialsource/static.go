package credentialsource

import (
	"context"
	"fmt"
	"os"
	"strings"
	"sync"
	"time"

	"github.com/picatz/jose/pkg/jwt"
)

// MaxEnvTokenBytes bounds a token read from an environment variable, for the
// same reason [MaxFileTokenBytes] bounds one read from a file: a JWT with a
// generous claim set is a few kilobytes, and a variable holding vastly more
// than that is pointing at something that is not a token.
const MaxEnvTokenBytes = 64 << 10

// DefaultStaticRefreshMargin is how long before its "exp" claim a cached
// static token is re-read from the environment, mirroring
// [DefaultGitHubActionsRefreshMargin] on the mint-on-demand side.
//
// A static source cannot mint, so the margin does not buy a fresh token the
// way it does for github-actions. What it buys is that the last minute of a
// token's life is spent re-reading the variable on every call rather than
// serving a parsed copy from memory — so a platform or wrapper that does
// replace the value in place is noticed, and an expiry that has actually
// passed is refused here, with a sentence about the job's token lifetime,
// instead of arriving at the server as an indistinguishable "unauthenticated".
const DefaultStaticRefreshMargin = time.Minute

// staticTokenSource serves a bearer token a CI platform placed in the process
// environment before the job started.
//
// This is the shape of every CI OIDC integration in this package that is not
// GitHub Actions: there is no endpoint to ask, no request token to present,
// and no way to mint a second token with different claims. The job's
// configuration decided the audience and the lifetime before the first line
// of the script ran, and all a client can do is read the variable, check that
// what is there is usable, and refuse when it is not.
//
// Refusing is the whole contribution. A static token that has already expired,
// or that was minted for a different relying party, will be rejected by the
// server — as an authentication failure, which looks identical to a wrong
// trust policy, a wrong issuer entry, or clock skew. Detecting it here turns
// that into a sentence naming the job-configuration key to change.
type staticTokenSource struct {
	name     string
	variable string

	// audience, when set, is compared against the token's "aud" claim. See
	// [staticTokenSource.read] for why that comparison is a configuration
	// diagnostic and never a trust decision.
	audience string

	// audienceHint names where the platform binds the audience, so a mismatch
	// error can say which file to edit rather than only that something is
	// wrong.
	audienceHint string

	// absentHint explains what a missing variable means on this platform.
	// Called lazily so it can inspect the environment for the common
	// misconfigurations — a legacy variable set, or the process not running
	// on that platform at all.
	absentHint func() string

	clock  func() time.Time
	margin time.Duration

	mu     sync.Mutex
	cached Token
}

func (s *staticTokenSource) Name() string { return s.name }

// Token returns the token the platform put in the environment.
//
// The cached copy is served only while it is further than the refresh margin
// from expiring; inside the margin, and on the first call, the variable is
// re-read. A token that is expired, unparseable, or addressed to a different
// audience is refused rather than returned: this Source never hands back a
// credential it cannot justify presenting.
func (s *staticTokenSource) Token(ctx context.Context) (Token, error) {
	if err := ctx.Err(); err != nil {
		return Token{}, err
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	now := s.clock()
	if !s.cached.IsZero() && !s.cached.ExpiresWithin(s.margin, now) {
		return s.cached, nil
	}

	token, err := s.read(now)
	if err != nil {
		// A failed re-read never falls back to the cached token, even one with
		// seconds left on it: presenting a credential known to be dying is the
		// mid-script failure this package exists to prevent.
		s.cached = Token{}
		return Token{}, err
	}

	s.cached = token
	return token, nil
}

func (s *staticTokenSource) read(now time.Time) (Token, error) {
	raw := strings.TrimSpace(os.Getenv(s.variable))
	if raw == "" {
		return Token{}, fmt.Errorf("%w: %s is unset or empty; %s", ErrSourceUnusable, s.variable, s.absentHint())
	}
	if len(raw) > MaxEnvTokenBytes {
		return Token{}, fmt.Errorf("%w: %s holds more than %d bytes, which is not a token; check what set it",
			ErrSourceUnusable, s.variable, MaxEnvTokenBytes)
	}

	// Parsed, never verified. This package does not decide whether a token is
	// trustworthy — the server's OIDCVerifier does that against its own trust
	// policy when the token arrives, checking the signature, the issuer, the
	// audience and every claim rule. What is read here is used for exactly two
	// things: deciding when to look at the variable again, and telling an
	// author which line of their job configuration is wrong. A wrong answer
	// here costs a diagnostic, never a wrongly-trusted token.
	parsed, err := jwt.Parse(raw)
	if err != nil {
		return Token{}, fmt.Errorf("%w: %s does not hold a JWT: %w", ErrSourceUnusable, s.variable, err)
	}

	expiresAt, err := claimExpiry(parsed.Claims)
	if err != nil {
		return Token{}, fmt.Errorf("%w: the token in %s %w", ErrSourceUnusable, s.variable, err)
	}

	if !now.Before(expiresAt) {
		return Token{}, fmt.Errorf("%w: the token in %s expired at %s, and %s cannot mint another — "+
			"a job's token is issued once, when the job starts",
			ErrSourceUnusable, s.variable, expiresAt.UTC().Format(time.RFC3339), s.name)
	}

	if s.audience != "" {
		audiences := unverifiedAudiences(parsed.Claims)
		if !containsAudience(audiences, s.audience) {
			return Token{}, fmt.Errorf("%w: the token in %s is addressed to %s, not %q; %s",
				ErrSourceUnusable, s.variable, describeAudiences(audiences), s.audience, s.audienceHint)
		}
	}

	return newToken(s.name, raw, expiresAt), nil
}

// claimExpiry reads the "exp" claim, which [jwt.Parse] has already normalized
// to an int64. The error is phrased to complete a sentence beginning "the
// token in VAR".
func claimExpiry(claims jwt.ClaimsSet) (time.Time, error) {
	value, ok := claims[jwt.ExpirationTime]
	if !ok {
		return time.Time{}, fmt.Errorf("has no %q claim, so there is no way to tell whether it is still good",
			jwt.ExpirationTime)
	}
	seconds, ok := value.(int64)
	if !ok {
		return time.Time{}, fmt.Errorf("has a %q claim that is not a number", jwt.ExpirationTime)
	}
	return time.Unix(seconds, 0), nil
}

// unverifiedAudiences reads the "aud" claim, which RFC 7519 section 4.1.3
// allows to be either one string or an array of them. A JSON decode produces
// []any for the array form, so all three shapes are handled here rather than
// leaving a token minted by a platform that uses the array form looking like a
// token with no audience at all — which would turn a working configuration
// into a refusal.
func unverifiedAudiences(claims jwt.ClaimsSet) []string {
	switch aud := claims[jwt.Audience].(type) {
	case string:
		return []string{aud}
	case []string:
		return aud
	case []any:
		out := make([]string, 0, len(aud))
		for _, value := range aud {
			if s, ok := value.(string); ok {
				out = append(out, s)
			}
		}
		return out
	default:
		return nil
	}
}

func containsAudience(audiences []string, want string) bool {
	for _, audience := range audiences {
		if audience == want {
			return true
		}
	}
	return false
}

// describeAudiences renders the "aud" claim for a diagnostic. A token with no
// readable audience gets a sentence rather than an empty pair of quotes,
// because `addressed to ""` reads like a bug in the error message.
func describeAudiences(audiences []string) string {
	switch len(audiences) {
	case 0:
		return "no audience this client could read"
	case 1:
		return fmt.Sprintf("%q", audiences[0])
	default:
		quoted := make([]string, 0, len(audiences))
		for _, audience := range audiences {
			quoted = append(quoted, fmt.Sprintf("%q", audience))
		}
		return strings.Join(quoted, ", ")
	}
}
