package auth

import (
	"errors"
	"math"
	"strings"
	"testing"
	"time"
)

// TestTokenResponseCredentialLifetimeBoundaries walks every expires_in a token
// endpoint could report against a target's ceiling.
//
// Each refusal asserts the message and not only the sentinel, because the three
// refusals here are three different things to tell an operator — the provider
// sent nothing usable, the provider sent more than this target allows, or the
// target's own ceiling is unusable — and a single "invalid expires_in" for all
// three points at the wrong one two times out of three. Asserting only
// errors.Is(ErrExchangeFailed) is also what let the ceiling check below go
// unreached: with a zero ceiling the over-policy branch fires first and produces
// the same sentinel, so the test passed with that branch deleted.
func TestTokenResponseCredentialLifetimeBoundaries(t *testing.T) {
	now := time.Date(2026, time.August, 23, 12, 0, 0, 0, time.UTC)
	assertion := Assertion{ID: "assertion-id"}
	tests := []struct {
		name      string
		expiresIn int64
		max       time.Duration
		want      time.Duration
		wantErr   string
	}{
		{name: "omitted", max: time.Hour, wantErr: "reported no usable expires_in (0)"},
		{name: "negative", expiresIn: -1, max: time.Hour, wantErr: "reported no usable expires_in (-1)"},
		{name: "one second", expiresIn: 1, max: time.Hour, want: time.Second},
		{name: "policy boundary", expiresIn: 3600, max: time.Hour, want: time.Hour},
		{name: "over policy", expiresIn: 3601, max: time.Hour, wantErr: "longer than the 1h0m0s this target allows"},

		// A Duration holds at most math.MaxInt64 nanoseconds, so the largest
		// ceiling anyone can express is math.MaxInt64/1e9 seconds — which is
		// exactly why the policy check is also the overflow check. There is no
		// expires_in that clears a ceiling and still wraps when multiplied.
		{name: "duration overflow", expiresIn: math.MaxInt64, max: MaxCredentialLifetime, wantErr: "longer than the 24h0m0s this target allows"},
		{name: "duration overflow against the largest expressible ceiling", expiresIn: math.MaxInt64, max: math.MaxInt64, wantErr: "longer than the 2562047h47m16.854775807s this target allows"},

		// A ceiling is the target's own policy, so an unusable one is reported as
		// a policy fault rather than blamed on the token that happened to arrive
		// first. Every constructor applies the same one-second floor at startup;
		// reaching this means a caller of this method that did not.
		{name: "no ceiling at all", expiresIn: 1, max: 0, wantErr: "no usable credential lifetime policy (0s)"},
		{name: "a ceiling below one second", expiresIn: 1, max: 500 * time.Millisecond, wantErr: "no usable credential lifetime policy (500ms)"},
		{name: "a negative ceiling", expiresIn: 1, max: -time.Hour, wantErr: "no usable credential lifetime policy (-1h0m0s)"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			credential, err := (tokenResponse{AccessToken: "opaque", TokenType: "Bearer", ExpiresIn: test.expiresIn}).credential(
				"provider", "target", assertion, now, test.max)
			if test.wantErr != "" {
				if !errors.Is(err, ErrExchangeFailed) {
					t.Fatalf("error = %v, want ErrExchangeFailed", err)
				}
				if !strings.Contains(err.Error(), test.wantErr) {
					t.Fatalf("error = %v, want it to name %q", err, test.wantErr)
				}
				if !credential.IsZero() {
					t.Fatal("failed exchange returned a credential")
				}
				return
			}
			if err != nil {
				t.Fatal(err)
			}
			if got := credential.ExpiresAt.Sub(now); got != test.want {
				t.Fatalf("lifetime = %s, want %s", got, test.want)
			}
		})
	}
}

// TestCredentialLifetimeCeiling pins what a target may configure, at the one
// place every OAuth constructor reads it.
func TestCredentialLifetimeCeiling(t *testing.T) {
	tests := []struct {
		name       string
		configured time.Duration
		want       time.Duration
		wantErr    bool
	}{
		{name: "zero takes the default", want: DefaultMaxCredentialLifetime},
		{name: "one second", configured: time.Second, want: time.Second},
		{name: "the hard bound", configured: MaxCredentialLifetime, want: MaxCredentialLifetime},
		{name: "a second past the hard bound", configured: MaxCredentialLifetime + time.Second, wantErr: true},
		{name: "below one second", configured: 500 * time.Millisecond, wantErr: true},
		{name: "negative", configured: -time.Second, wantErr: true},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			got, err := credentialLifetimeCeiling("target", test.configured)
			if test.wantErr {
				if !errors.Is(err, ErrInvalidPolicy) {
					t.Fatalf("error = %v, want ErrInvalidPolicy", err)
				}
				if got != 0 {
					t.Fatalf("ceiling = %s, want none alongside an error", got)
				}
				return
			}
			if err != nil {
				t.Fatal(err)
			}
			if got != test.want {
				t.Fatalf("ceiling = %s, want %s", got, test.want)
			}
		})
	}
}
