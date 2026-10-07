package credentialsource

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/picatz/flowstate/pkg/flowstate/v1/deviceflow"
)

// LoginRefreshMargin is how close to expiry a stored login's access token may
// get before [SourceLogin] refreshes it, so a token is not spent in the window
// where the server would refuse it.
const LoginRefreshMargin = time.Minute

type serverOriginKey struct{}

// ContextWithServerOrigin records which server the token is being requested
// for, so [SourceLogin] can refuse to present a login made for another. The
// owning transport sets it on every request; address is the server's URL.
func ContextWithServerOrigin(ctx context.Context, address string) context.Context {
	return context.WithValue(ctx, serverOriginKey{}, address)
}

// ErrWrongServer is wrapped by the login source's refusal to present a stored
// login to a server other than the one it was made for.
var ErrWrongServer = errors.New("credentialsource: the stored login is for a different server")

// LoginOption configures [NewLoginSource].
type LoginOption func(*loginSource)

// WithLoginClient sets the [deviceflow.Client] used to refresh.
func WithLoginClient(c *deviceflow.Client) LoginOption {
	return func(s *loginSource) { s.client = c }
}

// WithLoginClock replaces time.Now for the expiry check.
func WithLoginClock(now func() time.Time) LoginOption {
	return func(s *loginSource) { s.now = now }
}

// loginSource presents the access token `flow login` stored, refreshing it
// when it is near expiry.
type loginSource struct {
	store    *deviceflow.Store
	issuer   string
	clientID string
	client   *deviceflow.Client
	now      func() time.Time

	// mu serializes refresh within the process: a rotating refresh token can
	// be spent once, so concurrent requests must not each try.
	mu sync.Mutex
}

// NewLoginSource returns a [Source] that serves the login in store for issuer
// and clientID, either of which may be empty to mean the only stored login
// (see [deviceflow.Store.Select]).
//
// An absent login is an error wrapping [deviceflow.ErrNotLoggedIn] and
// [ErrSourceUnusable]; a login that needs refreshing and cannot be refreshed
// fails closed with a message to run `flow login`. The stale token is never
// returned.
func NewLoginSource(store *deviceflow.Store, issuer, clientID string, opts ...LoginOption) Source {
	s := &loginSource{
		store:    store,
		issuer:   issuer,
		clientID: clientID,
		client:   deviceflow.New(),
		now:      time.Now,
	}
	for _, opt := range opts {
		opt(s)
	}
	return s
}

func (s *loginSource) Name() string { return SourceLogin }

func (s *loginSource) Token(ctx context.Context) (Token, error) {
	if err := ctx.Err(); err != nil {
		return Token{}, err
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	entry, err := s.store.Select(s.issuer, s.clientID)
	if err != nil {
		return Token{}, fmt.Errorf("%w: %w; run `flow login`", ErrSourceUnusable, err)
	}

	// Checked before anything else is done with the entry, so the refresh
	// token is not spent on a request that will be refused.
	if err := checkLoginServer(ctx, entry); err != nil {
		return Token{}, err
	}

	if entry.Tokens.ExpiresWithin(LoginRefreshMargin, s.now()) {
		entry, err = s.refresh(ctx, entry)
		if err != nil {
			return Token{}, err
		}
	}

	return newToken(SourceLogin, entry.Tokens.AccessToken, entry.Tokens.ExpiresAt), nil
}

// refresh renews entry and persists the result. Where another process got to
// the refresh token first, its stored result is used instead of failing.
func (s *loginSource) refresh(ctx context.Context, entry deviceflow.Entry) (deviceflow.Entry, error) {
	renewed, err := s.client.Renew(ctx, entry)
	if err == nil {
		if err := s.store.Save(renewed); err != nil {
			return entry, fmt.Errorf("%w: refreshed the login for %s but could not store it: %w; run `flow login`",
				ErrSourceUnusable, entry.Issuer, err)
		}
		return renewed, nil
	}

	if current, loadErr := s.store.Load(entry.Issuer, entry.ClientID); loadErr == nil &&
		current.Tokens.AccessToken != entry.Tokens.AccessToken &&
		!current.Tokens.ExpiresWithin(LoginRefreshMargin, s.now()) {
		return current, nil
	}

	if errors.Is(err, deviceflow.ErrNoRefreshToken) {
		return entry, fmt.Errorf("%w: the login for %s has expired and has no refresh token; run `flow login`",
			ErrSourceUnusable, entry.Issuer)
	}
	return entry, fmt.Errorf("%w: the login for %s expired and could not be refreshed (%w); run `flow login`",
		ErrSourceUnusable, entry.Issuer, err)
}

// checkLoginServer fails closed unless the request's server origin is known
// and equals the one recorded at login. A missing origin is a refusal, not a
// pass: a caller that does not say where the token is going cannot be shown
// to be sending it to the right place.
func checkLoginServer(ctx context.Context, entry deviceflow.Entry) error {
	hint := fmt.Sprintf("run `flow login --issuer %s --client-id %s --address <server>` for the server you mean",
		entry.Issuer, entry.ClientID)
	if entry.ServerOrigin == "" {
		return fmt.Errorf("%w: %w: the login for %s records no server; %s",
			ErrSourceUnusable, ErrWrongServer, entry.Issuer, hint)
	}
	address, _ := ctx.Value(serverOriginKey{}).(string)
	origin, err := deviceflow.NormalizeOrigin(address)
	if err != nil || origin != entry.ServerOrigin {
		return fmt.Errorf("%w: %w: the login for %s was made for %s and is not sent anywhere else; %s",
			ErrSourceUnusable, ErrWrongServer, entry.Issuer, entry.ServerOrigin, hint)
	}
	return nil
}
