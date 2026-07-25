package vault

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"os"
	"strings"
	"time"

	"github.com/picatz/flowstate/pkg/flowstate/v1/secrets"
)

// cachedToken is the client token the provider authenticates its reads with.
//
// This is the one thing this package caches, and it is not a resolved secret: it
// is a credential the provider obtained for itself, so keeping it is not the second
// place a workflow's value lives that the [secrets.Provider] contract forbids. The
// alternative is a login round trip in front of every read.
type cachedToken struct {
	value string

	// renewAt is when the token stops being offered to a read, which is before it
	// expires so that a read is not started with a token that will not survive it.
	// The zero time means the token is not known to expire, which is the case for
	// a static token and for a root token whose lease duration is zero.
	renewAt time.Time

	// generation identifies this token among the ones the provider has held, so
	// that a read which got a 403 discards the token it used rather than one
	// another goroutine obtained in the meantime. It is a counter rather than a
	// comparison of token values because comparing credentials for identity
	// invites comparing them for equality, and a generation is the thing actually
	// being asked about. Zero means no token is cached.
	generation uint64
}

// authToken returns a client token to authenticate a read with, logging in if
// there is no usable one.
//
// It returns the token's generation alongside it, which is what [Provider.forget]
// needs to invalidate the right token after a 403.
func (p *Provider) authToken(ctx context.Context) (string, uint64, error) {
	if value, generation, ok := p.currentToken(); ok {
		return value, generation, nil
	}

	// One login at a time. Without this, a worker starting up — or one whose token
	// has just aged out — would have every concurrent task execution log in, which
	// is a burst of writes to Vault's token store and a pile of tokens nobody
	// needed. The wait honors the caller's context, because a channel is used
	// rather than a mutex precisely so that a task with a nearer deadline is not
	// blocked past it by somebody else's round trip.
	select {
	case p.logins <- struct{}{}:
		defer func() { <-p.logins }()
	case <-ctx.Done():
		return "", 0, fmt.Errorf("waiting to authenticate to %s: %w", p.addr, ctx.Err())
	}

	// The goroutine that held the slot may have just produced a usable token.
	if value, generation, ok := p.currentToken(); ok {
		return value, generation, nil
	}

	value, ttl, err := p.login(ctx)
	if err != nil {
		return "", 0, err
	}

	stored, generation := p.storeToken(value, ttl)

	return stored, generation, nil
}

// currentToken returns the cached token if it is still worth using.
func (p *Provider) currentToken() (string, uint64, bool) {
	p.mu.Lock()
	defer p.mu.Unlock()

	if p.token.generation == 0 {
		return "", 0, false
	}

	if !p.token.renewAt.IsZero() && !p.now().Before(p.token.renewAt) {
		return "", 0, false
	}

	return p.token.value, p.token.generation, true
}

// storeToken caches a freshly issued token and returns it with its generation.
func (p *Provider) storeToken(value string, ttl time.Duration) (string, uint64) {
	p.mu.Lock()
	defer p.mu.Unlock()

	// The counter advances rather than continuing from the cached token's
	// generation, which may have been reset to zero by forget. Reusing a
	// generation would let a 403 from the read that caused the reset invalidate
	// the token that replaced it, and every read in flight would then log in.
	p.generation++

	token := cachedToken{value: value, generation: p.generation}

	if ttl > 0 {
		// The margin is capped at half the lease so that a margin longer than the
		// tokens Vault issues does not mean logging in for every read: a 30 second
		// token with a one minute margin would otherwise be expired on arrival.
		margin := min(p.renewBefore, ttl/2)
		token.renewAt = p.now().Add(ttl - margin)
	}

	p.token = token

	return token.value, token.generation
}

// forget discards the cached token if it is still the one the caller used.
//
// The generation check is what keeps a slow read's 403 from throwing away a token
// that another goroutine obtained after it started, which would otherwise turn one
// stale token into a login for every request in flight.
func (p *Provider) forget(generation uint64) {
	p.mu.Lock()
	defer p.mu.Unlock()

	if p.token.generation == generation {
		p.token = cachedToken{}
	}
}

// canReauthenticate reports whether the provider can obtain a new token, which
// decides whether a 403 is worth retrying.
//
// A static token cannot be replaced, so its 403 is final: retrying would send the
// same rejected credential a second time and report the same error a round trip
// later.
func (p *Provider) canReauthenticate() bool {
	return p.role != ""
}

// login exchanges the pod's service account token for a Vault client token, and
// reports the lease duration it came with.
func (p *Provider) login(ctx context.Context) (string, time.Duration, error) {
	if !p.canReauthenticate() {
		// Only reachable when a static token was refused, since a static token is
		// seeded into the cache at construction and never expires from here.
		return "", 0, fmt.Errorf(
			"%w: %s refused the configured token, and a static token cannot be renewed",
			secrets.ErrPermission, p.addr,
		)
	}

	jwt, err := p.readJWT()
	if err != nil {
		return "", 0, err
	}

	// The JWT is a credential, so it goes in the body of one request and nowhere
	// else. json.Marshal on a struct is what keeps it out of a format string,
	// where a stray %v in a later edit could put it in an error.
	body, err := json.Marshal(struct {
		Role string `json:"role"`
		JWT  string `json:"jwt"`
	}{Role: p.role, JWT: jwt})
	if err != nil {
		return "", 0, fmt.Errorf("building the login request: %w", err)
	}

	status, response, err := p.do(ctx, http.MethodPost, p.loginPath(), "", body)
	if err != nil {
		// As in read: the status is the better classification, unless what ended
		// was the caller, whose error must not come back as something retryable.
		if ctx.Err() == nil && unavailable(status) {
			return "", 0, unavailableStatus(p.addr, status, p.loginPath())
		}

		return "", 0, err
	}

	switch {
	case status == http.StatusOK:
	case status == http.StatusForbidden, status == http.StatusBadRequest:
		// Vault answers both with 403 when the JWT is not accepted and with 400
		// when the role does not exist or does not permit this service account.
		// Both are permanent: the same service account presenting the same role
		// will be refused again until an operator changes the binding.
		return "", 0, fmt.Errorf(
			"%w: %s refused the Kubernetes login for role %q (status %d); "+
				"check the role's bound service account names and namespaces",
			secrets.ErrPermission, p.addr, p.role, status,
		)
	case status == http.StatusNotFound:
		return "", 0, fmt.Errorf(
			"%w: %s has no auth method at %q; enable Kubernetes auth or set WithKubernetesAuthMount",
			secrets.ErrPermission, p.addr, p.loginPath(),
		)
	default:
		return "", 0, unavailableStatus(p.addr, status, p.loginPath())
	}

	var payload struct {
		Auth *struct {
			ClientToken   string `json:"client_token"`
			LeaseDuration int64  `json:"lease_duration"`
			Renewable     bool   `json:"renewable"`
		} `json:"auth"`
	}

	if err := decodeJSON(response, &payload); err != nil {
		return "", 0, fmt.Errorf("%s answered the login with %w", p.addr, err)
	}

	if payload.Auth == nil || payload.Auth.ClientToken == "" {
		// Not classified as unavailable: a vault that answers 200 with no token is
		// misconfigured or is not a vault, and the contract says to treat what we
		// cannot classify as permanent rather than retry it forever.
		return "", 0, fmt.Errorf("%s answered the login with no client token", p.addr)
	}

	// Renewable is deliberately unused. This provider logs in again rather than
	// renewing, which works the same whether or not a token can be renewed and
	// keeps working past the max TTL where renewal stops.
	return payload.Auth.ClientToken, time.Duration(payload.Auth.LeaseDuration) * time.Second, nil
}

// readJWT reads the pod's projected service account token.
//
// It is read on every login rather than kept from construction because the kubelet
// rotates a projected token in place, and a copy taken at startup stops being
// accepted partway through a worker's life. The token's contents never leave this
// function except in the login request body.
func (p *Provider) readJWT() (string, error) {
	contents, err := os.ReadFile(p.jwtPath)
	if err != nil {
		// The error names the path, which is configuration, and cannot include the
		// file's contents. It is classified as unavailable rather than as a
		// permission failure because the file is written by the kubelet as the
		// token rotates, so a read that fails now may well succeed on the next
		// attempt — and a worker that gave up permanently on a transient
		// projection failure would need a restart to recover.
		return "", fmt.Errorf(
			"%w: reading the Kubernetes service account token at %q: %w",
			secrets.ErrUnavailable, p.jwtPath, err,
		)
	}

	// A projected token has no trailing newline, but a token written by hand in a
	// test or a docker-compose file often does, and a JWT with a newline in it is
	// refused in a way that looks like a rejected credential rather than a
	// malformed one.
	jwt := strings.TrimSpace(string(contents))
	if jwt == "" {
		return "", fmt.Errorf(
			"%w: the Kubernetes service account token at %q is empty",
			secrets.ErrUnavailable, p.jwtPath,
		)
	}

	return jwt, nil
}
