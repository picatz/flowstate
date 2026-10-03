package flowstatev1

import (
	"strings"

	"github.com/google/cel-go/cel"
	"github.com/google/cel-go/common/types"
)

// Projections over [Type].
//
// [Type] is the one vocabulary for "what shape is this value", and every consumer
// reads it through a projection kept here rather than through a switch of its
// own: the CEL checker through [CELType], a diagnostic through [TypeString], the
// legacy durable enum through [TypeOfLegacy], and a type the checker inferred
// back into the vocabulary through [TypeOfCEL].
//
// Each is total over the message's kinds, and `TestTypeProjectionsAreTotal`
// ranges over every kind and scalar so that adding one without a projection
// fails the build rather than quietly answering `dyn`.

// TypeOfLegacy is the structural form of a legacy declared type.
//
// The legacy enum cannot say what a list holds or what a map's values are, so
// `list` and `struct` project to the loosest structural type that is true of
// them: `list(dyn)` and `map(string, dyn)`. TYPE_UNSPECIFIED has no structural
// form and answers nil, which a caller reads as "undeclared" rather than as
// `dyn`: an input with no type promised nothing, and `dyn` would promise
// "anything".
func TypeOfLegacy(t InputDeclaration_Type) *Type {
	switch t {
	case InputDeclaration_TYPE_STRING:
		return scalarType(Type_SCALAR_STRING)
	case InputDeclaration_TYPE_INT:
		return scalarType(Type_SCALAR_INT)
	case InputDeclaration_TYPE_FLOAT:
		return scalarType(Type_SCALAR_DOUBLE)
	case InputDeclaration_TYPE_BOOL:
		return scalarType(Type_SCALAR_BOOL)
	case InputDeclaration_TYPE_STRUCT:
		return &Type{Kind: &Type_Map_{Map: &Type_Map{Value: dynType()}}}
	case InputDeclaration_TYPE_LIST:
		return &Type{Kind: &Type_List{List: dynType()}}
	case InputDeclaration_TYPE_ENUM:
		return &Type{Kind: &Type_Enum{Enum: true}}
	default:
		return nil
	}
}

// DeclaredType is the type an input declares: the structural `value_type` where
// the writer carried one, else the structural form of the legacy `type`.
//
// Nil when the declaration states no type at all.
func (x *InputDeclaration) DeclaredType() *Type {
	if t := x.GetValueType(); t != nil {
		return t
	}

	return TypeOfLegacy(x.GetType())
}

// DeclaredType is the type an output declares, by the rule
// [InputDeclaration.DeclaredType] states.
func (x *OutputDeclaration) DeclaredType() *Type {
	if t := x.GetValueType(); t != nil {
		return t
	}

	return TypeOfLegacy(x.GetType())
}

// CELType is the type CEL's checker holds for a value of type t.
//
// A nil Type is `dyn`: a caller asking about something undeclared gets the one
// type the checker is silent about. An enum is a string, because an enum value's
// wire shape is a string and membership is a set-fact the checker cannot see
// (see [StringShaped]). A message is `dyn` until a descriptor registry reaches
// the checker, since cel-go refuses an object type it was never told about, and
// refusing every expression that touches one would be a false diagnostic.
//
// Bounded: a type nested past [MaxStructureDepth] is `dyn` from there down. A
// hand-built message can nest without limit, or point back at itself, and
// `flowfile.Validate` accepts one before the declaration bounds are checked, so
// a projection that recursed without a bound would spend a stack on it.
func CELType(t *Type) *cel.Type {
	return celTypeAt(t, 0)
}

func celTypeAt(t *Type, depth int) *cel.Type {
	if depth > MaxStructureDepth {
		return cel.DynType
	}

	switch kind := t.GetKind().(type) {
	case *Type_Scalar_:
		return celScalar(kind.Scalar)
	case *Type_List:
		return cel.ListType(celTypeAt(kind.List, depth+1))
	case *Type_Map_:
		return cel.MapType(cel.StringType, celTypeAt(kind.Map.GetValue(), depth+1))
	case *Type_Enum:
		return cel.StringType
	case *Type_Message:
		return cel.DynType
	case *Type_Dyn:
		return cel.DynType
	default:
		return cel.DynType
	}
}

func celScalar(s Type_Scalar) *cel.Type {
	switch s {
	case Type_SCALAR_STRING:
		return cel.StringType
	case Type_SCALAR_INT:
		return cel.IntType
	case Type_SCALAR_DOUBLE:
		return cel.DoubleType
	case Type_SCALAR_BOOL:
		return cel.BoolType
	case Type_SCALAR_BYTES:
		return cel.BytesType
	case Type_SCALAR_TIMESTAMP:
		return cel.TimestampType
	case Type_SCALAR_DURATION:
		return cel.DurationType
	case Type_SCALAR_NULL_TYPE:
		return cel.NullType
	default:
		return cel.DynType
	}
}

// TypeOfCEL is the structural form of a type CEL's checker inferred.
//
// Total, and loose where [Type] cannot say more: a type the vocabulary has no
// word for (`uint`, an opaque or object type, a type parameter, an error) is
// `dyn`, and so is a map whose keys are not strings, because [Type_Map] fixes
// string keys (a caller that must tell the two apart reads the CEL type it
// started from). `uint` is deliberately not `int`: the checker treats them as
// different types, and answering `int` would make `1u + 1` look checkable.
func TypeOfCEL(t *cel.Type) *Type {
	if t == nil {
		return dynType()
	}

	switch t.Kind() {
	case types.StringKind:
		return scalarType(Type_SCALAR_STRING)
	case types.IntKind:
		return scalarType(Type_SCALAR_INT)
	case types.DoubleKind:
		return scalarType(Type_SCALAR_DOUBLE)
	case types.BoolKind:
		return scalarType(Type_SCALAR_BOOL)
	case types.BytesKind:
		return scalarType(Type_SCALAR_BYTES)
	case types.TimestampKind:
		return scalarType(Type_SCALAR_TIMESTAMP)
	case types.DurationKind:
		return scalarType(Type_SCALAR_DURATION)
	case types.NullTypeKind:
		return scalarType(Type_SCALAR_NULL_TYPE)
	case types.ListKind:
		if params := t.Parameters(); len(params) == 1 {
			return &Type{Kind: &Type_List{List: TypeOfCEL(params[0])}}
		}
	case types.MapKind:
		if params := t.Parameters(); len(params) == 2 && params[0].Kind() == types.StringKind {
			return &Type{Kind: &Type_Map_{Map: &Type_Map{Value: TypeOfCEL(params[1])}}}
		}
	}

	return dynType()
}

// TypeString spells a type the way a Flowfile author writes it: a CEL type
// expression, `list(string)`, `map(string, int)`, `timestamp`.
//
// An enum is `enum` and a message is its full name. A nil type is `dyn`, which
// is how every projection here reads one.
//
// Bounded like [CELType]: past [MaxStructureDepth] it prints `dyn`.
func TypeString(t *Type) string {
	return typeStringAt(t, 0)
}

func typeStringAt(t *Type, depth int) string {
	if depth > MaxStructureDepth {
		return "dyn"
	}

	switch kind := t.GetKind().(type) {
	case *Type_Scalar_:
		return scalarName(kind.Scalar)
	case *Type_List:
		return "list(" + typeStringAt(kind.List, depth+1) + ")"
	case *Type_Map_:
		return "map(string, " + typeStringAt(kind.Map.GetValue(), depth+1) + ")"
	case *Type_Enum:
		return "enum"
	case *Type_Message:
		return kind.Message
	default:
		return "dyn"
	}
}

func scalarName(s Type_Scalar) string {
	switch s {
	case Type_SCALAR_NULL_TYPE:
		return "null_type"
	case Type_SCALAR_UNSPECIFIED:
		return "dyn"
	default:
		return strings.ToLower(strings.TrimPrefix(s.String(), "SCALAR_"))
	}
}

// IsDyn reports whether t says nothing about a value's shape: no type at all, or
// `dyn`. The checker is silent about these, and so is every diagnostic built on
// one.
func IsDyn(t *Type) bool {
	switch t.GetKind().(type) {
	case nil, *Type_Dyn:
		return true
	default:
		return false
	}
}

func scalarType(s Type_Scalar) *Type {
	return &Type{Kind: &Type_Scalar_{Scalar: s}}
}

func dynType() *Type {
	return &Type{Kind: &Type_Dyn{Dyn: true}}
}
