package credentialsource

import (
	"fmt"
	"io"
	"log/slog"
	"time"

	"github.com/picatz/flowstate/pkg/flowstate/v1/auth"
)

// Token is a bearer credential a [Source] produced, ready to present to a
// Flowstate server.
//
// Its secret value lives in an [auth.Material], which is what keeps it out of
// reach of fmt, log, and every serializer — including through an unexported
// field of some other struct, where a String method alone would not help. See
// [auth.Material] for why that is a closure and not a plain string field: a
// plain string field is how the client-secret leak this rule replaced happened
// in the first place.
type Token struct {
	material auth.Material

	// SourceName names the [Source] that produced this token, such as
	// "github-actions" or "file". Never secret, always safe to log.
	SourceName string

	// ExpiresAt is when the source believes the token stops working, or the
	// zero time when it cannot say.
	//
	// A file or environment token's lifetime is whatever its issuer already
	// committed to, and this package has no way to read it without parsing a
	// format the token might not even be — so it is left zero, and
	// [Token.ExpiresWithin] treats zero as "unknown", never as "about to
	// expire". A github-actions token's expiry comes from its own "exp" claim,
	// read unverified: nothing here is deciding whether to trust the token,
	// only when to ask for a new one.
	ExpiresAt time.Time
}

// newToken builds a Token holding one bearer value.
func newToken(sourceName, raw string, expiresAt time.Time) Token {
	return Token{
		material:   auth.NewSingleMaterial(raw),
		SourceName: sourceName,
		ExpiresAt:  expiresAt,
	}
}

// Bearer returns the token's value, reporting false for the zero Token and for
// one that has been through a serializer.
func (t Token) Bearer() (string, bool) {
	return t.material.Single()
}

// IsZero reports whether the token carries no value.
func (t Token) IsZero() bool {
	return t.material.IsZero()
}

// Expired reports whether the token's known expiry has passed. A Token with
// unknown expiry (ExpiresAt is zero) is never expired by this check — the
// same "unknown is not evidence of staleness" reasoning [Token.ExpiresWithin]
// documents.
func (t Token) Expired(now time.Time) bool {
	if t.ExpiresAt.IsZero() {
		return false
	}
	return !now.Before(t.ExpiresAt)
}

// ExpiresWithin reports whether the token expires within d of now.
//
// A Token with unknown expiry (ExpiresAt is zero) never reports true: unknown is
// not "about to expire", it is "cannot say", and treating it as always-fresh is
// what lets a file or environment Token — whose whole contract is that it is
// read fresh on every call — pass through a cache's margin check without ever
// being coerced into looking expired.
func (t Token) ExpiresWithin(d time.Duration, now time.Time) bool {
	if t.ExpiresAt.IsZero() {
		return false
	}
	return !now.Add(d).Before(t.ExpiresAt)
}

// String describes the token without revealing it.
func (t Token) String() string {
	if t.IsZero() {
		return "no token"
	}
	if t.ExpiresAt.IsZero() {
		return fmt.Sprintf("%s token", t.SourceName)
	}
	return fmt.Sprintf("%s token, expires %s", t.SourceName, t.ExpiresAt.UTC().Format(time.RFC3339))
}

// Format implements [fmt.Formatter], which closes the gap a String method
// leaves: %#v ignores String, and a Formatter is consulted before both String
// and GoString. Every verb renders the same redacted description.
func (t Token) Format(f fmt.State, verb rune) {
	_, _ = io.WriteString(f, t.String())
}

// LogValue implements [slog.LogValuer], recording which source produced this
// token and when it expires, and never its value.
func (t Token) LogValue() slog.Value {
	return slog.GroupValue(
		slog.String("source", t.SourceName),
		slog.Time("expires_at", t.ExpiresAt),
	)
}
