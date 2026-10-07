package deviceflow

import (
	"fmt"
	"io"
	"log/slog"
	"time"
)

// Tokens is what a successful grant or refresh yields.
//
// The bearer values are plain strings so the [Store] can persist them, which is
// why every way a fmt verb, a logger or a JSON encoder could reach them is
// closed here: [Tokens.Format], [Tokens.LogValue] and [Tokens.MarshalJSON]
// carry no secret. Only [Store] writes them out.
type Tokens struct {
	// AccessToken is the bearer credential presented to a Flowstate server.
	AccessToken string

	// RefreshToken renews AccessToken ([RFC 6749 §6]). Empty when the IdP
	// issued none, in which case an expired login cannot be renewed.
	//
	// [RFC 6749 §6]: https://www.rfc-editor.org/rfc/rfc6749#section-6
	RefreshToken string

	// ExpiresAt is when AccessToken stops working: now plus the response's
	// expires_in, or [DefaultTokenLifetime] when the IdP did not say.
	ExpiresAt time.Time

	// Scope is the scope the IdP granted, when it said.
	Scope string
}

// DefaultTokenLifetime is assumed when a token response carries no expires_in
// ([RFC 6749 §5.1] makes it RECOMMENDED, not required). An hour errs toward
// refreshing early rather than presenting a token the server refuses.
//
// [RFC 6749 §5.1]: https://www.rfc-editor.org/rfc/rfc6749#section-5.1
const DefaultTokenLifetime = time.Hour

// ExpiresWithin reports whether the access token expires within d of now.
func (t Tokens) ExpiresWithin(d time.Duration, now time.Time) bool {
	return !now.Add(d).Before(t.ExpiresAt)
}

// String describes the tokens without revealing them.
func (t Tokens) String() string {
	if t.AccessToken == "" {
		return "no token"
	}
	return fmt.Sprintf("login token, expires %s", t.ExpiresAt.UTC().Format(time.RFC3339))
}

// Format implements [fmt.Formatter] so no verb, %#v included, prints a value.
func (t Tokens) Format(f fmt.State, _ rune) { _, _ = io.WriteString(f, t.String()) }

// LogValue implements [slog.LogValuer], recording only the expiry.
func (t Tokens) LogValue() slog.Value {
	return slog.GroupValue(slog.Time("expires_at", t.ExpiresAt))
}

// MarshalJSON emits no secret: a Tokens that reaches an encoder by accident
// must not carry a credential out with it.
func (t Tokens) MarshalJSON() ([]byte, error) {
	return fmt.Appendf(nil, `{"expires_at":%q}`, t.ExpiresAt.UTC().Format(time.RFC3339)), nil
}
