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
//
// # Refused claims
//
// A claim set that could not be read within its bounds is not the same as one
// without the claim: a rule that only tests absence (`!("k" in identity.claims)`,
// or a deny rule on `"contractors" in identity.claims.groups`) would evaluate
// against a set the dropped claim is missing from and permit. [RefusedClaims]
// is the carrier for that case. Every read of it, whatever the rule asks, is an
// evaluation error, and an errored rule denies on every surface, so the one
// place a Caller's claims are built is also the one place this fails closed.
type Claims struct {
	m       map[string]any
	refused error
}

// RefusedClaims is the claim set of a caller whose claims were over their
// bounds and so were not all read. Every CEL operation on it evaluates to an
// error (see "Refused claims" on [Claims]); [Claims.Map] still reports the
// claims that were read, for diagnostics that must not fail.
func RefusedClaims(read map[string]any, err error) Claims {
	return Claims{m: read, refused: err}
}

// Refused is why the claims were not read, or nil for a normal claim set.
func (c Claims) Refused() error { return c.refused }

// refusal is the evaluation error every operation on refused claims returns.
func (c Claims) refusal() ref.Val {
	return types.NewErr("carried claims were refused and are unreadable: %v", c.refused)
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
func (c Claims) ConvertToNative(t reflect.Type) (any, error) {
	if c.refused != nil {
		return nil, c.refused
	}

	return c.val().ConvertToNative(t)
}

// ConvertToType converts as the underlying CEL map would.
func (c Claims) ConvertToType(t ref.Type) ref.Val {
	if c.refused != nil {
		return c.refusal()
	}
	if t == types.TypeType {
		return claimsType
	}

	return c.val().ConvertToType(t)
}

// Equal compares as the underlying CEL map would.
func (c Claims) Equal(other ref.Val) ref.Val {
	if c.refused != nil {
		return c.refusal()
	}
	if o, ok := other.(Claims); ok {
		other = o.val()
	}

	return c.val().Equal(other)
}

// Contains implements `"k" in identity.claims`.
func (c Claims) Contains(k ref.Val) ref.Val {
	if c.refused != nil {
		return c.refusal()
	}

	return c.val().Contains(k)
}

// Get implements `identity.claims.k` and `identity.claims["k"]`; an absent key
// is an error value.
func (c Claims) Get(k ref.Val) ref.Val {
	if c.refused != nil {
		return c.refusal()
	}

	return c.val().Get(k)
}

// Find is the presence-aware lookup.
func (c Claims) Find(k ref.Val) (ref.Val, bool) {
	if c.refused != nil {
		return c.refusal(), true
	}

	return c.val().Find(k)
}

// Iterator ranges over the claim names, for comprehensions.
func (c Claims) Iterator() traits.Iterator {
	if c.refused != nil {
		return &refusedIterator{err: c.refusal()}
	}

	return c.val().Iterator()
}

// refusedIterator yields one error element, so a comprehension over refused
// claims (`identity.claims.all(k, ...)`) evaluates to an error rather than to
// the vacuous answer an empty range gives. It ends after that one element,
// because cel-go does not stop folding on an error condition.
type refusedIterator struct {
	err  ref.Val
	done bool
}

func (*refusedIterator) Type() ref.Type { return types.IteratorType }
func (i *refusedIterator) Value() any   { return i.err }
func (i *refusedIterator) ConvertToNative(reflect.Type) (any, error) {
	return nil, i.err.(error)
}
func (i *refusedIterator) ConvertToType(ref.Type) ref.Val { return i.err }
func (i *refusedIterator) Equal(ref.Val) ref.Val          { return i.err }
func (i *refusedIterator) HasNext() ref.Val               { return types.Bool(!i.done) }
func (i *refusedIterator) Next() ref.Val {
	i.done = true

	return i.err
}

// Size implements `size(identity.claims)`.
func (c Claims) Size() ref.Val {
	if c.refused != nil {
		return c.refusal()
	}

	return c.val().Size()
}

var _ traits.Mapper = Claims{}
