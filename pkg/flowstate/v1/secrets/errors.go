package secrets

import (
	"errors"
	"fmt"
)

// Sentinel errors, so a caller can tell a misconfigured worker from a
// misconfigured workflow. Every one of them is safe to surface to a user and to
// record: they name the reference, never the value.
var (
	// ErrNotFound reports that no secret exists at the reference. It means the
	// environment variable is unset or the file is absent.
	ErrNotFound = errors.New("secret not found")

	// ErrEmpty reports that the secret exists but holds nothing. An empty
	// credential is treated as a configuration mistake rather than a value,
	// because using one produces a confusing failure further away: an
	// unauthenticated request rather than a missing secret.
	ErrEmpty = errors.New("secret is empty")

	// ErrUnknownScheme reports that no provider is registered for the
	// reference's scheme, so this worker cannot resolve it.
	ErrUnknownScheme = errors.New("unknown secret provider")

	// ErrInvalidRef reports that a reference is malformed. It is what a Flowfile
	// naming a bad secret should fail with, at compile time.
	ErrInvalidRef = errors.New("invalid secret reference")

	// ErrNotDeserializable reports an attempt to decode a [Secret] from data.
	// Values come from resolvers; data carries references.
	ErrNotDeserializable = errors.New("a secret cannot be deserialized: resolve a reference instead")

	// ErrTooLarge reports that a secret exceeded the provider's size limit.
	ErrTooLarge = errors.New("secret is too large")

	// ErrPermission reports that the backend refused the read. It is permanent:
	// retrying the same request with the same credentials will be refused again.
	ErrPermission = errors.New("permission denied reading secret")

	// ErrUnavailable reports that the backend could not be reached, or did not
	// answer in time. It is the one transient classification, so a step that
	// failed on it is worth another attempt.
	ErrUnavailable = errors.New("secret backend is unavailable")

	// ErrNamespace reports that a namespace is missing or malformed. It means the
	// tenant boundary could not be established, so nothing is resolved.
	ErrNamespace = errors.New("invalid namespace")
)

// Retryable reports whether a resolution failure could plausibly succeed on
// another attempt.
//
// Only [ErrUnavailable] is transient. Everything else — a missing secret, an empty
// one, a refused read, a malformed reference — describes a state that another
// attempt will find unchanged, and retrying it wastes the step's attempt budget.
// An unclassified error is treated as permanent, because guessing that a failure is
// retryable is the more expensive mistake.
func Retryable(err error) bool {
	return errors.Is(err, ErrUnavailable)
}

// ResolveError reports that a reference could not be resolved. It names the
// reference, which is safe to log, and wraps the cause.
//
// This package's own providers put only the reference and the cause in it, so their
// failures are safe to log and to record. A [Provider] implemented elsewhere is
// responsible for the same discipline: a vault client's error routinely quotes the
// request it made, so pass such an error through a [Scrubber] before returning it
// rather than assuming this type makes it safe.
type ResolveError struct {
	// Ref is the reference that could not be resolved.
	Ref Ref

	// Err is the underlying cause.
	Err error
}

// Error implements the error interface.
//
// The reference is quoted rather than interpolated raw. A Ref built directly from a
// protobuf message has not necessarily been through [Ref.Validate], so its name may
// hold a control character, and this message is bound for logs and workflow
// history where a raw newline would let it forge a line.
func (e *ResolveError) Error() string {
	return fmt.Sprintf("secrets: resolving %q: %v", RefString(e.Ref), e.Err)
}

// Unwrap returns the cause, so the sentinels above match through errors.Is.
func (e *ResolveError) Unwrap() error {
	return e.Err
}
