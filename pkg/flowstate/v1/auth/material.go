package auth

import (
	"fmt"
	"io"
	"log/slog"
)

// Material is secret bytes that cannot be printed, logged, or serialized.
//
// It exists so that everything in Flowstate carrying a secret — a brokered
// credential, a resolved secret, a signing key — carries it the same way, with one
// implementation of the protections rather than one per type. A type holding
// secrets embeds a Material and keeps its own metadata as ordinary exported
// fields, which stay visible and serializable because they are not secret.
//
// # What it protects against
//
// A String method alone is not enough, and neither is an unexported field. fmt
// cannot call a method on a value it reaches through an unexported field: the
// reflected value is read-only and cannot be converted back to an interface, so fmt
// prints the fields it can see instead. A secret in a plain field is therefore
// printed in full by %v on any struct that happens to contain one, however
// carefully the containing type redacts itself. `type taskState struct{ cred
// Credential }` and a debug log of it is the whole exploit.
//
// So the values live in a closure. A func field has nothing structural for fmt to
// print, at any depth, through any container, with any verb. Format covers the
// remaining direct case, %#v, which ignores String.
//
// # What it does not protect against
//
// A revealed value is an ordinary Go string and can be copied, logged, or returned
// like any other. Material bounds where a secret travels by default; it cannot
// follow one that has been deliberately taken out.
//
// A Material also cannot be serialized, by design. That is what makes the rule
// against secrets entering durable workflow history enforceable: a value that has
// been through a serializer arrives empty, and using it fails rather than
// succeeding with nothing.
//
// The zero Material carries nothing, and every read of it reports absence.
type Material struct {
	// reveal returns a named value. Nothing else in this struct, and nothing in a
	// struct containing it, is reachable by reflection as anything but a func.
	reveal func(name string) (string, bool)
}

// NewMaterial holds a copy of the given values.
//
// The copy matters: a caller that reuses or mutates its map afterwards cannot
// change material already handed out. Empty values are dropped, since a name
// present with no value is indistinguishable from absence to every reader.
func NewMaterial(values map[string]string) Material {
	held := make(map[string]string, len(values))
	for name, value := range values {
		if value != "" {
			held[name] = value
		}
	}

	if len(held) == 0 {
		return Material{}
	}

	return Material{
		reveal: func(name string) (string, bool) {
			value, ok := held[name]
			return value, ok
		},
	}
}

// NewSingleMaterial holds one unnamed value, for a secret that is just a string.
// Read it back with [Material.Single].
func NewSingleMaterial(value string) Material {
	return NewMaterial(map[string]string{singleValueName: value})
}

// singleValueName is the name a single unnamed value is held under.
const singleValueName = "value"

// Value returns the named value, reporting false when it is absent — including for
// the zero Material and for one that has been through a serializer.
func (m Material) Value(name string) (string, bool) {
	if m.reveal == nil {
		return "", false
	}
	return m.reveal(name)
}

// Single returns the value held by [NewSingleMaterial].
func (m Material) Single() (string, bool) {
	return m.Value(singleValueName)
}

// IsZero reports whether the material carries nothing.
func (m Material) IsZero() bool { return m.reveal == nil }

// String returns a fixed placeholder. It never renders any part of the material,
// including its length or which names it holds.
func (m Material) String() string {
	if m.IsZero() {
		return "[no material]"
	}
	return "[redacted]"
}

// Format implements [fmt.Formatter], which closes the gap a String method leaves:
// %#v ignores String, and a Formatter is consulted before both String and
// GoString. Every verb renders the placeholder, because there is no verb for which
// printing the material would be correct.
func (m Material) Format(f fmt.State, verb rune) {
	_, _ = io.WriteString(f, m.String())
}

// LogValue implements [slog.LogValuer] so that a logger records the placeholder
// rather than reflecting over the value.
func (m Material) LogValue() slog.Value {
	return slog.StringValue(m.String())
}

// MarshalJSON writes null.
//
// Refusing to serialize would be louder, but wrong for the types that hold
// material alongside metadata worth recording: an audit record should be able to
// say which credential was used and when it expires. Writing null keeps the
// metadata and drops the secret, and the read side fails closed.
func (m Material) MarshalJSON() ([]byte, error) {
	return []byte("null"), nil
}

// UnmarshalJSON accepts and discards whatever it is given.
//
// A Material that has been through a serializer carries nothing, whatever was in
// the document. Anything claiming to be material in a serialized document did not
// come from here, and honoring it would defeat the point.
func (m *Material) UnmarshalJSON([]byte) error {
	m.reveal = nil
	return nil
}
