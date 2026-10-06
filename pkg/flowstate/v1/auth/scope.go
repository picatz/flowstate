package auth

import (
	"fmt"
	"slices"
	"strings"
)

// The claims a token's OAuth scopes travel in. RFC 9068 §2.2.3.1 registers
// "scope" as a space-delimited string (RFC 6749 §3.3); several identity
// providers instead issue "scp", a JSON array of strings.
const (
	scopeClaim = "scope"
	scpClaim   = "scp"

	// maxScopeValues bounds the scopes read from one token. A token is another
	// party's input; the work spent on it is bounded here rather than by the
	// size of whatever the issuer chose to sign.
	maxScopeValues = 64
)

// narrowActions applies the scopes a verified token carries to the actions its
// trust policy entry grants and returns the effective allowlist.
//
// Narrowing is intersect-only: a token can give up authority the entry grants
// and can never add any, so the result is always a subset of granted, and a
// scope naming an action the entry does not grant is ignored. It is also opt-in:
// an entry with a nil allowlist grants unrestricted actions and the token's
// scopes do not apply, because intersecting with "everything" would have to
// invent the vocabulary this package deliberately does not own.
//
// A token without a scope claim keeps everything the entry grants; one whose
// scope claim is present and empty keeps nothing. A token carrying both "scope"
// and "scp", or either of the wrong shape, is refused as malformed rather than
// resolved by picking one, since two claims that could disagree are two answers
// to a question that must have one.
func narrowActions(granted ActionScopes, claims map[string]any) (ActionScopes, error) {
	if granted == nil {
		return nil, nil
	}

	scopes, present, err := tokenScopes(claims)
	if err != nil {
		return nil, err
	}
	if !present {
		return slices.Clone(granted), nil
	}

	narrowed := make(ActionScopes, 0, len(granted))
	for _, action := range granted {
		if slices.Contains(scopes, action) {
			narrowed = append(narrowed, action)
		}
	}

	return narrowed, nil
}

// tokenScopes reads the scope claim, reporting whether one was present.
func tokenScopes(claims map[string]any) (scopes []string, present bool, err error) {
	scopeValue, hasScope := claims[scopeClaim]
	scpValue, hasScp := claims[scpClaim]

	switch {
	case hasScope && hasScp:
		return nil, false, fmt.Errorf("%w: token carries both %q and %q claims", ErrMalformedToken, scopeClaim, scpClaim)
	case hasScope:
		text, ok := scopeValue.(string)
		if !ok {
			return nil, false, fmt.Errorf("%w: %q claim is %T, not a string", ErrMalformedToken, scopeClaim, scopeValue)
		}
		scopes = strings.Fields(text)
	case hasScp:
		switch typed := scpValue.(type) {
		case string:
			scopes = strings.Fields(typed)
		case []string:
			scopes = slices.Clone(typed)
		case []any:
			for _, element := range typed {
				text, ok := element.(string)
				if !ok {
					return nil, false, fmt.Errorf("%w: %q claim contains a %T, not a string", ErrMalformedToken, scpClaim, element)
				}
				scopes = append(scopes, text)
			}
		default:
			return nil, false, fmt.Errorf("%w: %q claim is %T, not an array of strings", ErrMalformedToken, scpClaim, scpValue)
		}
	default:
		return nil, false, nil
	}

	if len(scopes) > maxScopeValues {
		return nil, false, fmt.Errorf("%w: token carries %d scopes, more than the %d this server reads", ErrMalformedToken, len(scopes), maxScopeValues)
	}
	for _, scope := range scopes {
		if len(scope) > maxClaimValueLength {
			return nil, false, fmt.Errorf("%w: a scope in the token is longer than %d bytes", ErrMalformedToken, maxClaimValueLength)
		}
	}

	return scopes, true, nil
}
