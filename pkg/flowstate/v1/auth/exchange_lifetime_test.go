package auth

import (
	"errors"
	"math"
	"testing"
	"time"
)

func TestTokenResponseCredentialLifetimeBoundaries(t *testing.T) {
	now := time.Date(2026, time.August, 23, 12, 0, 0, 0, time.UTC)
	assertion := Assertion{ID: "assertion-id"}
	tests := []struct {
		name      string
		expiresIn int64
		max       time.Duration
		want      time.Duration
		wantErr   bool
	}{
		{name: "omitted", max: time.Hour, wantErr: true},
		{name: "negative", expiresIn: -1, max: time.Hour, wantErr: true},
		{name: "one second", expiresIn: 1, max: time.Hour, want: time.Second},
		{name: "policy boundary", expiresIn: 3600, max: time.Hour, want: time.Hour},
		{name: "over policy", expiresIn: 3601, max: time.Hour, wantErr: true},
		{name: "duration overflow", expiresIn: math.MaxInt64, max: MaxCredentialLifetime, wantErr: true},
		{name: "no positive policy", expiresIn: 1, max: 0, wantErr: true},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			credential, err := (tokenResponse{AccessToken: "opaque", TokenType: "Bearer", ExpiresIn: test.expiresIn}).credential(
				"provider", "target", assertion, now, test.max)
			if test.wantErr {
				if !errors.Is(err, ErrExchangeFailed) {
					t.Fatalf("error = %v, want ErrExchangeFailed", err)
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
