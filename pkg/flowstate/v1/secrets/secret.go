package secrets

import (
	"crypto/subtle"
	"fmt"
	"io"
	"log/slog"
)

// Redacted is what a [Secret] renders as everywhere a value could otherwise
// escape: formatting, structured logging, JSON, and text. It is also what a
// [Scrubber] substitutes for a value it finds in text.
const Redacted = "[REDACTED]"

// Secret holds a resolved secret value.
//
// Every way a Go value normally becomes text is overridden to produce [Redacted]
// instead of the value, because the paths that leak a credential are the ones
// nobody wrote on purpose: a %v in a log line, a struct marshaled to JSON, a
// value interpolated into an error that is recorded in workflow history. Reaching
// the value at all takes [Secret.Reveal].
//
// A Secret cannot be compared with ==, so an equality check has to go through
// [Secret.EqualString], which is constant-time. That is deliberate: == on a
// credential is a timing side channel, and making it a compile error is more
// reliable than asking people to remember.
//
// The zero Secret holds nothing. [Secret.Reveal] returns the empty string for it,
// so a caller that ignores a resolution error gets an empty credential rather than
// a plausible one; [Secret.IsZero] distinguishes the case, and
// [Secret.EqualString] matches nothing at all.
//
// # Limits worth knowing
//
// Hold a Secret in a named field, never as an embedded one. Embedding promotes
// this type's String, MarshalJSON, and UnmarshalJSON to the outer type, so
// formatting or marshaling the outer value silently yields only [Redacted] and
// drops its other fields, with no error to notice.
//
// Never place a Secret in a template or expression evaluation context. A template
// can call methods on its data, so a workflow-authored {{.Reveal}} would print the
// value; pass the result of [Secret.Reveal] to the one call that needs it instead.
//
// [Secret.Len] reports the value's exact length, which is a small side channel
// accepted because a secret that is present but wrong — one that picked up a
// trailing newline, say — is otherwise very hard to diagnose.
//
// A revealed value cannot be wiped: Go strings are immutable, so it survives in
// memory until it is collected, and a [Cache] deliberately retains it for its TTL.
// A heap or core dump therefore exposes it. This package bounds where a value
// travels, not how long it lives.
//
// A Secret is immutable and safe for concurrent use.
type Secret struct {
	ref Ref

	// reveal closes over the value rather than storing it in a field, which
	// closes the one leak the methods below cannot cover.
	//
	// When a Secret sits in an *unexported* field of some other struct, fmt may
	// not call its methods, so it reflects over its fields instead. A string
	// field would print in full; a pointer field is worse than it looks, because
	// a verb that is invalid for pointers sends fmt down a path that
	// dereferences and prints what it points at. Reflection cannot reach a
	// variable captured by a closure, so a func field is the only shape with
	// nothing to find: every verb renders it as an address.
	//
	// The func field also makes Secret incomparable, so == is a compile error and
	// an equality check has to go through the constant-time comparison.
	reveal func() string
}

// NewSecret builds a resolved secret. It is how a [Provider] returns its result,
// including a provider implemented outside this package.
//
// Nothing else should call it. A Secret that did not come from a provider did not
// come from anywhere trustworthy, and constructing one from data is what
// [Secret.UnmarshalText] refuses to do.
func NewSecret(ref Ref, value string) Secret {
	if value == "" {
		// An empty value is no value: leaving the func nil keeps IsZero
		// meaningful and stops an empty credential from looking resolved.
		return Secret{ref: ref}
	}

	return Secret{ref: ref, reveal: func() string { return value }}
}

// Ref returns the reference the secret was resolved from. It is safe to log.
func (s Secret) Ref() Ref {
	return s.ref
}

// IsZero reports whether the secret holds no value.
func (s Secret) IsZero() bool {
	return s.reveal == nil
}

// Len returns the length of the value in bytes, without exposing it. It is useful
// for diagnosing a secret that is present but wrong, such as one that picked up a
// trailing newline.
func (s Secret) Len() int {
	return len(s.Reveal())
}

// Reveal returns the secret value.
//
// This is the only way to obtain it, and it is named to read as the deliberate
// act it is at the call site. Keep what it returns as close to its use as
// possible: pass it to the thing that needs it and do not store it, log it, put it
// in an error, or return it from an activity. If it may end up inside an error
// produced by code that does not know it is a secret, register the secret with a
// [Scrubber] and pass that error through [Scrubber.ScrubError].
func (s Secret) Reveal() string {
	if s.reveal == nil {
		return ""
	}

	return s.reveal()
}

// EqualString reports whether the secret's value equals other, in constant time.
//
// Use it instead of revealing the value to compare it, so that a comparison
// against attacker-supplied input does not leak the value through how long it
// takes.
//
// An unresolved secret equals nothing, not even the empty string. Otherwise a
// caller that ignored a resolution error would authenticate anyone presenting an
// empty credential, which is the one direction this must never fail.
func (s Secret) EqualString(other string) bool {
	if s.reveal == nil {
		return false
	}

	return subtle.ConstantTimeCompare([]byte(s.Reveal()), []byte(other)) == 1
}

// Equal reports whether two secrets hold the same value, in constant time.
//
// It also makes a Secret comparable to [github.com/google/go-cmp/cmp], which
// honors an Equal method; without it cmp panics on the unexported fields. Note
// that reflect.DeepEqual, and therefore testify's assertions, report two secrets
// as unequal even when they hold the same value, because a Secret holds its value
// in a func field and functions are only ever DeepEqual when both are nil. Compare
// secrets with this method or [Secret.EqualString].
func (s Secret) Equal(other Secret) bool {
	if s.reveal == nil || other.reveal == nil {
		return s.reveal == nil && other.reveal == nil
	}

	return s.EqualString(other.Reveal())
}

// Format implements [fmt.Formatter], rendering [Redacted] for every verb.
//
// Implementing Format rather than only String is what makes the redaction
// complete: fmt consults a Formatter before String, so %q, %x, %d, and %#v are
// covered too, and a future verb cannot open a new hole.
//
// The exceptions are %p and %T, which fmt resolves before consulting a Formatter
// and which therefore cannot be redacted. Neither exposes the value: %T is a type
// name, and %p reports the reference and the address of the closure holding the
// value.
func (s Secret) Format(f fmt.State, verb rune) {
	if verb == 'q' {
		fmt.Fprintf(f, "%q", Redacted)
		return
	}

	io.WriteString(f, Redacted)
}

// String implements [fmt.Stringer], returning [Redacted]. It covers the callers
// that reach for a string without going through fmt.
func (s Secret) String() string {
	return Redacted
}

// GoString implements [fmt.GoStringer], returning [Redacted], so %#v and a
// debugger's pretty-printer do not expose the value.
func (s Secret) GoString() string {
	return Redacted
}

// LogValue implements [slog.LogValuer], returning [Redacted], so a secret logged
// as a structured attribute is redacted rather than reflected over.
func (s Secret) LogValue() slog.Value {
	return slog.StringValue(Redacted)
}

// MarshalText implements [encoding.TextMarshaler], returning [Redacted].
//
// Marshaling succeeds rather than failing because a failure invites a caller to
// fall back to something less careful, and because the result of marshaling a
// secret must be safe whether or not anyone checks the error.
func (s Secret) MarshalText() ([]byte, error) {
	return []byte(Redacted), nil
}

// MarshalJSON implements [json.Marshaler], returning [Redacted] as a JSON string.
func (s Secret) MarshalJSON() ([]byte, error) {
	return []byte(`"` + Redacted + `"`), nil
}

// UnmarshalText implements [encoding.TextUnmarshaler] by refusing.
//
// A secret value comes from a resolver, never from data. Decoding one would mean a
// Flowfile, an API request, or a workflow history entry had carried a value, which
// is the thing this package exists to prevent — so this fails loudly instead of
// accepting it.
func (s *Secret) UnmarshalText([]byte) error {
	return ErrNotDeserializable
}

// UnmarshalJSON implements [json.Unmarshaler] by refusing, for the reason given on
// [Secret.UnmarshalText].
func (s *Secret) UnmarshalJSON([]byte) error {
	return ErrNotDeserializable
}
