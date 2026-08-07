package auth

import "fmt"

// MaxNamespaceLen is the longest permitted namespace.
//
// It lives here, not only in [secrets.ValidateNamespace], because a namespace
// reaches two places that used to check it two different ways: a signed
// assertion subject (this package) and a secret provider's path or environment
// variable name (secrets). One value, one grammar, checked once — see
// [ValidateNamespace].
const MaxNamespaceLen = 63

// ValidateNamespace reports whether a namespace is well formed enough to enter
// a signed assertion subject.
//
// This is the canonical namespace grammar. [secrets.ValidateNamespace]
// delegates to it rather than checking separately, because a namespace this
// package rejected reaching a secret provider, or one [secrets.ValidateNamespace]
// rejected reaching a subject, both used to be possible: this package imports
// nothing (see the package doc), so [SubjectFor] used to check a namespace
// claim only for the two characters that could split a subject into extra
// components — "/" and ":" — while secrets required the full grammar. A
// namespace of "Prod Team", "..", a control character, or several kilobytes of
// text satisfied the first and would have been rejected by the second, and
// nothing ever compared the two, so the looser check is what actually decided
// whether the value reached a signed subject. There is now one grammar, and it
// is the stricter of the two: lowercase ASCII letters, digits, and a dash that
// is never the first character.
//
// The empty namespace is always valid: it is the single-tenant default, not a
// namespace an attacker can spell, and every caller of this function already
// treats "" as "no namespace" rather than as a value to reject.
func ValidateNamespace(namespace string) error {
	if namespace == "" {
		return nil
	}

	if len(namespace) > MaxNamespaceLen {
		return fmt.Errorf("namespace is longer than %d characters", MaxNamespaceLen)
	}

	for i, c := range namespace {
		switch {
		case c >= 'a' && c <= 'z', c >= '0' && c <= '9':
		case c == '-' && i > 0:
		default:
			return fmt.Errorf(
				"namespace %q may only contain lowercase letters, digits, and dashes, and may not start with a dash",
				namespace,
			)
		}
	}

	return nil
}
