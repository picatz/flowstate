package principal

import (
	"reflect"

	"github.com/google/cel-go/common/types"
	"github.com/google/cel-go/common/types/ref"
	"github.com/google/cel-go/common/types/traits"
)

// Claims is the set of carried claims on a [Caller], and the CEL carrier that
// lets a rule read all of them: `identity.claims.groups` (a list),
// `identity.claims.slack.user` (a nested object), and `"k" in identity.claims`
// (the guard that makes an absent claim a non-match instead of an error).
//
// # Why a struct and not map[string]any
//
// cel-go's native types derive a field's CEL type from its Go type, and an
// interface-valued map or slice has no CEL type, so a plain `map[string]any`
// field is silently dropped from the declared object and `identity.claims`
// becomes a compile error. The one hook the derivation honors is a struct
// type that itself implements [ref.Val]: its CEL type is whatever its Type
// method says. Claims is that struct. It declares `map(string, dyn)` and
// delegates every operation to the standard CEL map built over the JSON-shaped
// Go value it holds, so a read behaves exactly as it would on a map the
// engine built itself: an absent key is a "no such key" evaluation error and a
// rule that errors denies. Reading a claim never falls back to an empty value.
//
// Values are JSON-shaped: string, bool, float64, nil, []any and
// map[string]any, as protobuf's Value decodes to. The zero Claims is the empty
// map, so a caller with no claims needs no smoothing.
type Claims struct {
	m map[string]any
}

// NewClaims wraps a JSON-shaped map as [Claims]. The map is not copied; the
// caller hands over ownership.
func NewClaims(m map[string]any) Claims { return Claims{m: m} }

// StringClaims is NewClaims for the common all-string claim set.
func StringClaims(m map[string]string) Claims {
	if len(m) == 0 {
		return Claims{}
	}
	out := make(map[string]any, len(m))
	for k, v := range m {
		out[k] = v
	}

	return Claims{m: out}
}

// Map returns the JSON-shaped claims, never nil.
func (c Claims) Map() map[string]any {
	if c.m == nil {
		return map[string]any{}
	}

	return c.m
}

// Len is how many claims are carried.
func (c Claims) Len() int { return len(c.m) }

func (c Claims) val() traits.Mapper {
	return types.DefaultTypeAdapter.NativeToValue(c.Map()).(traits.Mapper)
}

var claimsType = types.NewMapType(types.StringType, types.DynType)

// Type reports `map(string, dyn)`, which is what cel-go reads to declare the
// field.
func (Claims) Type() ref.Type { return claimsType }

// Value is the JSON-shaped Go map.
func (c Claims) Value() any { return c.Map() }

// ConvertToNative converts as the underlying CEL map would.
func (c Claims) ConvertToNative(t reflect.Type) (any, error) { return c.val().ConvertToNative(t) }

// ConvertToType converts as the underlying CEL map would.
func (c Claims) ConvertToType(t ref.Type) ref.Val {
	if t == types.TypeType {
		return claimsType
	}

	return c.val().ConvertToType(t)
}

// Equal compares as the underlying CEL map would.
func (c Claims) Equal(other ref.Val) ref.Val {
	if o, ok := other.(Claims); ok {
		other = o.val()
	}

	return c.val().Equal(other)
}

// Contains implements `"k" in identity.claims`.
func (c Claims) Contains(k ref.Val) ref.Val { return c.val().Contains(k) }

// Get implements `identity.claims.k` and `identity.claims["k"]`; an absent key
// is an error value.
func (c Claims) Get(k ref.Val) ref.Val { return c.val().Get(k) }

// Find is the presence-aware lookup.
func (c Claims) Find(k ref.Val) (ref.Val, bool) { return c.val().Find(k) }

// Iterator ranges over the claim names, for comprehensions.
func (c Claims) Iterator() traits.Iterator { return c.val().Iterator() }

// Size implements `size(identity.claims)`.
func (c Claims) Size() ref.Val { return c.val().Size() }

var _ traits.Mapper = Claims{}
