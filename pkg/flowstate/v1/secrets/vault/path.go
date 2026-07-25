package vault

import (
	"fmt"
	"strings"
	"unicode"

	"github.com/picatz/flowstate/pkg/flowstate/v1/secrets"
)

// fieldSeparator divides a reference's path from the field to read within the
// secret stored there: "apps/api#token".
//
// It is "#" because that is what a fragment is in a URI — a selection within the
// thing the rest of the reference names — and because it cannot be confused with a
// path separator the way a second colon or a dot could.
const fieldSeparator = "#"

// maxFieldLen bounds a field name. A field name is not a path, so it is not a
// traversal risk, but it is workflow-authored text that appears in an error, and
// an unbounded one would be a way to make a log entry as large as a reference is
// allowed to be.
const maxFieldLen = 256

// SecretPath returns the KV v2 API path a reference reads within a namespace,
// relative to the vault's /v1/ root — for example
// "secret/data/team-a/apps/api".
//
// It takes the reference's name, with or without a "#field" suffix, and reports
// the path alone; the field selects within what that path returns and is not part
// of it. Use it to write the Vault policy a worker needs, and to check in a test
// that two namespaces cannot reach one path. It performs no request, so it reveals
// nothing about whether the secret exists.
func (p *Provider) SecretPath(namespace, name string) (string, error) {
	path, _, err := parseName(name)
	if err != nil {
		return "", err
	}

	return p.secretPath(namespace, path)
}

// secretPath builds the KV v2 data path for an already-parsed reference path.
//
// The layout is <mount>/data/[<prefix>/]<namespace>/<path>. The "data" segment is
// KV v2's: the engine exposes the current value of a secret under data/ and its
// version history under metadata/, so the path a reader uses is not the path an
// operator types into "vault kv get".
func (p *Provider) secretPath(namespace, path string) (string, error) {
	// The namespace arrives already validated from the store, and is validated
	// again here because a Provider is exported and may be called directly. This
	// is the check that keeps the namespace from being anything but a single path
	// segment.
	if err := secrets.ValidateNamespace(namespace); err != nil {
		return "", err
	}

	segment := namespace
	if segment == "" {
		segment = EmptyNamespaceSegment
	}

	parts := make([]string, 0, 5)
	parts = append(parts, p.mount, "data")

	if p.prefix != "" {
		parts = append(parts, p.prefix)
	}

	parts = append(parts, segment, path)

	return strings.Join(parts, "/"), nil
}

// loginPath returns the API path of the Kubernetes auth method's login endpoint.
func (p *Provider) loginPath() string {
	return "auth/" + p.authMount + "/login"
}

// parseName splits a reference name into a KV path and the optional field within
// the secret it names.
//
// Everything about a name is checked here, before any request is made, so that a
// hostile reference fails while it is still text and a well-formed one needs no
// escaping afterwards: a path that survives this contains only characters that
// mean themselves in a URL, so there is no encoding through which a name can
// become a different path than the one it reads as.
func parseName(name string) (path, field string, err error) {
	if name == "" {
		return "", "", fmt.Errorf("%w: name must not be empty", secrets.ErrInvalidRef)
	}

	path, field, hasField := strings.Cut(name, fieldSeparator)

	if err := validatePath(path); err != nil {
		return "", "", err
	}

	if hasField {
		if err := validateField(field); err != nil {
			return "", "", err
		}
	}

	return path, field, nil
}

// validatePath reports whether a reference's path is a usable, contained KV path.
func validatePath(path string) error {
	switch {
	case path == "":
		return fmt.Errorf(
			"%w: name must be a path within the mount, such as \"apps/api%stoken\"",
			secrets.ErrInvalidRef, fieldSeparator,
		)
	case strings.HasPrefix(path, "/"):
		return fmt.Errorf(
			"%w: %q must be relative to the mount, not an absolute path",
			secrets.ErrInvalidRef, path,
		)
	case strings.HasSuffix(path, "/"):
		return fmt.Errorf("%w: %q must name a secret, not a directory", secrets.ErrInvalidRef, path)
	case strings.Contains(path, `\`):
		return fmt.Errorf("%w: %q must use forward slashes", secrets.ErrInvalidRef, path)
	}

	for segment := range strings.SplitSeq(path, "/") {
		switch segment {
		case "":
			return fmt.Errorf("%w: %q has an empty path segment", secrets.ErrInvalidRef, path)
		case ".", "..":
			// Rejected rather than cleaned. A name is workflow-authored and a
			// namespace is the tenant boundary, so a name that walks upwards is an
			// attempt to leave that boundary, not a path in need of tidying — and
			// reporting it is more useful than quietly resolving something else.
			return fmt.Errorf(
				"%w: %q points outside its namespace",
				secrets.ErrInvalidRef, path,
			)
		}

		if i := strings.IndexFunc(segment, invalidPathRune); i >= 0 {
			return fmt.Errorf(
				"%w: %q may only contain letters, digits, dashes, underscores, dots, and slashes",
				secrets.ErrInvalidRef, path,
			)
		}
	}

	return nil
}

// invalidPathRune reports whether r may not appear in a path segment.
//
// The permitted set is narrow on purpose. Vault itself accepts a good deal more,
// but a path built from this set needs no URL escaping and cannot carry a "%", a
// "?", a "#", or a control character — the characters through which a name could
// otherwise become a different request than it appears to be, or forge a line in a
// log that records it.
func invalidPathRune(r rune) bool {
	switch {
	case r >= 'a' && r <= 'z', r >= 'A' && r <= 'Z':
		return false
	case r >= '0' && r <= '9':
		return false
	case r == '-', r == '_', r == '.':
		return false
	default:
		return true
	}
}

// validateField reports whether a field name is usable.
//
// A field is looked up in a decoded JSON object rather than put into a URL, so it
// is not held to the path's character set: a secret written by another team may
// well have a key this package did not anticipate. What it may not hold is a
// control character, since the name appears in an error that is recorded.
func validateField(field string) error {
	switch {
	case field == "":
		return fmt.Errorf(
			"%w: a %q with nothing after it names no field; drop it to read a single-field secret",
			secrets.ErrInvalidRef, fieldSeparator,
		)
	case len(field) > maxFieldLen:
		return fmt.Errorf(
			"%w: field name is longer than %d characters", secrets.ErrInvalidRef, maxFieldLen,
		)
	case strings.Contains(field, fieldSeparator):
		return fmt.Errorf(
			"%w: a name may select one field, so it may hold one %q",
			secrets.ErrInvalidRef, fieldSeparator,
		)
	}

	if i := strings.IndexFunc(field, isControl); i >= 0 {
		return fmt.Errorf(
			"%w: field name contains a control character at offset %d",
			secrets.ErrInvalidRef, i,
		)
	}

	return nil
}

// isControl reports whether r is a control character, which includes the newlines
// and escapes that would let workflow-authored text forge log output.
func isControl(r rune) bool {
	return r == unicode.ReplacementChar || unicode.IsControl(r)
}

// cleanMount vets an operator-configured path fragment — a mount, a prefix, a
// Vault namespace — and returns it without surrounding slashes.
//
// These come from the worker's own configuration rather than from a workflow, so
// the check is about catching a typo before it becomes a confusing 404, not about
// containing an attacker. It is the same character set the reference path uses, so
// that a configured value cannot introduce escaping the reference path was
// designed to avoid either.
func cleanMount(value, option string) (string, error) {
	cleaned := strings.Trim(value, "/")
	if cleaned == "" {
		return "", fmt.Errorf("secrets/vault: %s was given an empty path", option)
	}

	if err := validatePath(cleaned); err != nil {
		return "", fmt.Errorf("secrets/vault: %s was given %q: %w", option, value, err)
	}

	return cleaned, nil
}
