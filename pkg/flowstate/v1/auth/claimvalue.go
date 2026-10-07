package auth

import (
	"fmt"
	"maps"
	"slices"

	"google.golang.org/protobuf/types/known/structpb"

	"github.com/picatz/flowstate/internal/textbound"
)

// Bounds on the shape of a carried claim whose value is not a string.
//
// A `groups` list or a `slack: {user: ...}` object is data an operator named in
// a trust policy entry, and it is read wherever the work is spent: the proto
// reader walks it, a rule iterates it, a minted assertion signs it. So the walk
// is bounded by what it costs rather than by what it reports.
const (
	// MaxCarriedClaimStructuredBytes bounds a list or object claim, measured by
	// [claimBytes]: four times a string claim's own bound, because a group list is
	// many small strings. It is still well inside [MaxCarriedClaimBytes], which
	// bounds the whole set.
	MaxCarriedClaimStructuredBytes = 4096

	// MaxCarriedClaimDepth bounds nesting: a claim may hold a list or object that
	// holds one more, which is as deep as any real claim set measured here goes
	// (`slack.user`, `groups[]`). A deeper one is refused rather than walked.
	MaxCarriedClaimDepth = 4

	// MaxCarriedClaimNodes bounds how many values one claim holds, counting every
	// list element and object member. It caps the walk where the byte bound alone
	// would let a long list of empty values through.
	MaxCarriedClaimNodes = 512
)

// claimBytes measures a JSON-shaped claim value, and reports false when it is
// not one this package carries or is over the depth or node bounds.
//
// The measure is deterministic and encoding-free: string bytes, object member
// names, and a fixed eight for every other scalar. Both halves of a bound — the
// proto reader and the mint — call this one function, so they cannot disagree
// about a size.
func claimBytes(value any) (int, bool) {
	nodes := 0

	var walk func(any, int) (int, bool)
	walk = func(v any, depth int) (int, bool) {
		if depth > MaxCarriedClaimDepth {
			return 0, false
		}
		nodes++
		if nodes > MaxCarriedClaimNodes {
			return 0, false
		}

		switch v := v.(type) {
		case nil, bool, float64:
			return 8, true
		case string:
			return len(v), true
		case []any:
			total := 0
			for _, item := range v {
				n, ok := walk(item, depth+1)
				if !ok {
					return 0, false
				}
				total += n + 8
			}

			return total, true
		case map[string]any:
			total := 0
			for name, item := range v {
				n, ok := walk(item, depth+1)
				if !ok {
					return 0, false
				}
				total += len(name) + n + 8
			}

			return total, true
		default:
			return 0, false
		}
	}

	return walk(value, 0)
}

// checkCarriedClaim returns why a claim is over its bounds, naming the claim
// and the size and never the value (see [validateCarriedClaims]).
func checkCarriedClaim(name string, value any) error {
	switch value := value.(type) {
	case string:
		if len(value) > MaxCarriedClaimValueBytes {
			return fmt.Errorf("%w: carried claim %q has a %d byte value, and at most %d are allowed",
				ErrInvalidIdentity, textbound.Truncate(name, 64), len(value), MaxCarriedClaimValueBytes)
		}

		return nil
	case nil, bool, float64:
		return nil
	}

	size, ok := claimBytes(value)
	if !ok {
		return fmt.Errorf("%w: carried claim %q is not a bounded JSON value: it nests deeper than %d, holds more than %d values, or has an unsupported type",
			ErrInvalidIdentity, textbound.Truncate(name, 64), MaxCarriedClaimDepth, MaxCarriedClaimNodes)
	}
	if size > MaxCarriedClaimStructuredBytes {
		return fmt.Errorf("%w: carried claim %q is %d bytes of list or object, and at most %d are allowed",
			ErrInvalidIdentity, textbound.Truncate(name, 64), size, MaxCarriedClaimStructuredBytes)
	}

	return nil
}

// cloneClaim deep-copies a JSON-shaped value, so a later change to the source
// cannot change what an identity says.
func cloneClaim(value any) any {
	switch v := value.(type) {
	case []any:
		out := make([]any, len(v))
		for i, item := range v {
			out[i] = cloneClaim(item)
		}

		return out
	case map[string]any:
		out := make(map[string]any, len(v))
		for name, item := range v {
			out[name] = cloneClaim(item)
		}

		return out
	default:
		return v
	}
}

// WithWireClaims returns the identity with its claims read from the wire form of
// a claim set (flowstate.v1.Principal's `claims`).
//
// The read is where the walk is bounded for a message another process wrote,
// since a plugin or a stored run can hold any Value and protovalidate cannot see
// recursion: a claim nested deeper than [MaxCarriedClaimDepth] or holding more than
// [MaxCarriedClaimNodes] values is not walked past the bound, and more than
// [MaxCarriedClaims] claims are not read.
//
// What the walk refuses is refused, not trimmed. The claim is left out of
// [WorkloadIdentity.Claims], so a rule that reads it errors and denies, and the
// identity remembers that it was refused so that [WorkloadIdentity.Validate] (and
// with it every mint) fails, naming the claim and never its value. A truncated
// claim set would be an assertion that says something other than what was
// authorized.
func (w WorkloadIdentity) WithWireClaims(wire map[string]*structpb.Value) WorkloadIdentity {
	w.Claims, w.unreadable = nil, nil
	if len(wire) == 0 {
		return w
	}

	w.Claims = make(map[string]any, min(len(wire), MaxCarriedClaims))
	for i, name := range slices.Sorted(maps.Keys(wire)) {
		if i >= MaxCarriedClaims {
			w.unreadable = fmt.Errorf("%w: identity carries %d claims, and an assertion may carry at most %d",
				ErrInvalidIdentity, len(wire), MaxCarriedClaims)

			break
		}

		decoded, ok := decodeBounded(wire[name])
		if !ok {
			w.unreadable = fmt.Errorf("%w: carried claim %q nests deeper than %d or holds more than %d values",
				ErrInvalidIdentity, textbound.Truncate(name, 64), MaxCarriedClaimDepth, MaxCarriedClaimNodes)

			continue
		}
		w.Claims[name] = decoded
	}

	return w
}

// decodeBounded is structpb.Value.AsInterface with the depth and node bounds
// applied while walking, so a hostile value costs at most the bound.
func decodeBounded(value *structpb.Value) (any, bool) {
	nodes := 0

	var walk func(*structpb.Value, int) (any, bool)
	walk = func(v *structpb.Value, depth int) (any, bool) {
		if depth > MaxCarriedClaimDepth {
			return nil, false
		}
		nodes++
		if nodes > MaxCarriedClaimNodes {
			return nil, false
		}

		switch kind := v.GetKind().(type) {
		case nil, *structpb.Value_NullValue:
			return nil, true
		case *structpb.Value_BoolValue:
			return kind.BoolValue, true
		case *structpb.Value_NumberValue:
			return kind.NumberValue, true
		case *structpb.Value_StringValue:
			return kind.StringValue, true
		case *structpb.Value_ListValue:
			items := make([]any, 0, len(kind.ListValue.GetValues()))
			for _, item := range kind.ListValue.GetValues() {
				decoded, ok := walk(item, depth+1)
				if !ok {
					return nil, false
				}
				items = append(items, decoded)
			}

			return items, true
		case *structpb.Value_StructValue:
			members := make(map[string]any, len(kind.StructValue.GetFields()))
			for name, item := range kind.StructValue.GetFields() {
				decoded, ok := walk(item, depth+1)
				if !ok {
					return nil, false
				}
				members[name] = decoded
			}

			return members, true
		default:
			return nil, false
		}
	}

	return walk(value, 0)
}

// ClaimsToStruct is the wire form of a carried claim set, the inverse of
// [WorkloadIdentity.WithWireClaims]. It does not apply the size bounds: those are
// [WorkloadIdentity.Validate]'s and the schema's to refuse, so an over-bound
// claim reaches them whole instead of vanishing here. A value that is not
// JSON-shaped cannot be encoded and is left out.
func ClaimsToStruct(claims map[string]any) map[string]*structpb.Value {
	if len(claims) == 0 {
		return nil
	}

	out := make(map[string]*structpb.Value, len(claims))
	for name, value := range claims {
		encoded, err := structpb.NewValue(value)
		if err != nil {
			continue
		}
		out[name] = encoded
	}

	return out
}
