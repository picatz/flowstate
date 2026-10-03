package flowstatev1_test

import (
	"testing"

	"github.com/google/cel-go/cel"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/reflect/protoreflect"

	v1 "github.com/picatz/flowstate/pkg/flowstate/v1"
)

// exampleTypes holds one value of every kind [v1.Type] has, keyed by the oneof
// arm's name, so the totality test below can say which arm is missing.
func exampleTypes() map[protoreflect.Name]*v1.Type {
	return map[protoreflect.Name]*v1.Type{
		"scalar":  scalarType(v1.Type_SCALAR_STRING),
		"list":    {Kind: &v1.Type_List{List: scalarType(v1.Type_SCALAR_INT)}},
		"map":     {Kind: &v1.Type_Map_{Map: &v1.Type_Map{Value: scalarType(v1.Type_SCALAR_BOOL)}}},
		"enum":    {Kind: &v1.Type_Enum{Enum: true}},
		"message": {Kind: &v1.Type_Message{Message: "example.v1.Customer"}},
		"dyn":     dynType(),
	}
}

// TestTypeProjectionsAreTotal ranges over every kind and every scalar of [v1.Type]
// and asks every projection about it. A kind added to the schema without a
// projection arm answers `dyn` or an empty string here, which this refuses by
// name, rather than reaching a checker as a silent "anything".
func TestTypeProjectionsAreTotal(t *testing.T) {
	t.Parallel()

	examples := exampleTypes()
	arms := (&v1.Type{}).ProtoReflect().Descriptor().Oneofs().ByName("kind").Fields()
	for i := range arms.Len() {
		name := arms.Get(i).Name()
		ty, ok := examples[name]
		require.True(t, ok, "Type has a %q arm with no example in this test: add it, and a projection arm for it", name)

		assert.NotEmpty(t, v1.TypeString(ty), "TypeString is empty for the %q arm", name)
		assert.NotNil(t, v1.CELType(ty), "CELType is nil for the %q arm", name)
		assert.NotNil(t, v1.TypeOfCEL(v1.CELType(ty)), "TypeOfCEL is nil for the %q arm", name)
	}

	// The two arms whose CEL type is deliberately looser than their own: an enum
	// is a string on the wire, and a message waits for a descriptor registry.
	assert.True(t, v1.CELType(examples["enum"]).IsExactType(cel.StringType))
	assert.True(t, v1.CELType(examples["message"]).IsExactType(cel.DynType))

	scalars := v1.Type_Scalar(0).Descriptor().Values()
	for i := range scalars.Len() {
		scalar := v1.Type_Scalar(scalars.Get(i).Number())
		if scalar == v1.Type_SCALAR_UNSPECIFIED {
			continue
		}

		ty := scalarType(scalar)
		got := v1.CELType(ty)
		assert.False(t, got.IsExactType(cel.DynType), "scalar %s projects to dyn: CELType has no arm for it", scalar)
		assert.NotEqual(t, "dyn", v1.TypeString(ty), "scalar %s is spelled dyn: TypeString has no arm for it", scalar)
		assert.True(t, v1.TypeOfCEL(got).GetScalar() == scalar, "scalar %s does not survive CEL and back: got %s", scalar, v1.TypeString(v1.TypeOfCEL(got)))
	}
}

func TestTypeStringIsTheAuthorsSpelling(t *testing.T) {
	t.Parallel()

	list := func(elem *v1.Type) *v1.Type { return &v1.Type{Kind: &v1.Type_List{List: elem}} }
	mapOf := func(value *v1.Type) *v1.Type {
		return &v1.Type{Kind: &v1.Type_Map_{Map: &v1.Type_Map{Value: value}}}
	}

	for want, ty := range map[string]*v1.Type{
		"string":                        scalarType(v1.Type_SCALAR_STRING),
		"double":                        scalarType(v1.Type_SCALAR_DOUBLE),
		"timestamp":                     scalarType(v1.Type_SCALAR_TIMESTAMP),
		"duration":                      scalarType(v1.Type_SCALAR_DURATION),
		"bytes":                         scalarType(v1.Type_SCALAR_BYTES),
		"null_type":                     scalarType(v1.Type_SCALAR_NULL_TYPE),
		"list(dyn)":                     list(dynType()),
		"list(list(bytes))":             list(list(scalarType(v1.Type_SCALAR_BYTES))),
		"map(string, list(int))":        mapOf(list(scalarType(v1.Type_SCALAR_INT))),
		"enum":                          {Kind: &v1.Type_Enum{Enum: true}},
		"example.v1.Customer":           {Kind: &v1.Type_Message{Message: "example.v1.Customer"}},
		"dyn":                           nil,
		"map(string, map(string, dyn))": mapOf(mapOf(dynType())),
	} {
		assert.Equal(t, want, v1.TypeString(ty))
	}
}

// TestTypeOfCELDoesNotInventCertainty holds the projection to the half of its
// contract that protects a checker: what the vocabulary cannot say becomes
// `dyn`, never a neighbouring type.
func TestTypeOfCELDoesNotInventCertainty(t *testing.T) {
	t.Parallel()

	for name, ty := range map[string]*cel.Type{
		"uint":                cel.UintType,
		"int-keyed map":       cel.MapType(cel.IntType, cel.StringType),
		"type parameter":      cel.TypeParamType("T"),
		"opaque":              cel.OpaqueType("optional_type", cel.IntType),
		"nil":                 nil,
		"list of int-keyed":   cel.ListType(cel.MapType(cel.IntType, cel.IntType)),
		"object":              cel.ObjectType("example.v1.Customer"),
		"google.protobuf.Any": cel.AnyType,
	} {
		got := v1.TypeOfCEL(ty)
		switch name {
		case "list of int-keyed":
			assert.Equal(t, "list(dyn)", v1.TypeString(got), name)
		default:
			assert.True(t, v1.IsDyn(got), "%s projected to %s", name, v1.TypeString(got))
		}
	}
}

func TestLegacyTypesProjectToTheirStructuralForm(t *testing.T) {
	t.Parallel()

	values := v1.InputDeclaration_Type(0).Descriptor().Values()
	for i := range values.Len() {
		legacy := v1.InputDeclaration_Type(values.Get(i).Number())
		if legacy == v1.InputDeclaration_TYPE_UNSPECIFIED {
			assert.Nil(t, v1.TypeOfLegacy(legacy), "an undeclared type must stay undeclared, not become dyn")
			continue
		}

		got := v1.TypeOfLegacy(legacy)
		require.NotNil(t, got, "legacy %s has no structural form", legacy)
		assert.Equal(t, legacyStructuralType(legacy).String(), got.String(), legacy.String())
	}
}

func TestDeclaredTypePrefersTheStructuralType(t *testing.T) {
	t.Parallel()

	legacyOnly := &v1.InputDeclaration{Type: v1.InputDeclaration_TYPE_LIST}
	assert.Equal(t, "list(dyn)", v1.TypeString(legacyOnly.DeclaredType()))

	both := &v1.InputDeclaration{
		Type:      v1.InputDeclaration_TYPE_LIST,
		ValueType: &v1.Type{Kind: &v1.Type_List{List: scalarType(v1.Type_SCALAR_STRING)}},
	}
	assert.Equal(t, "list(string)", v1.TypeString(both.DeclaredType()))

	assert.Nil(t, (&v1.InputDeclaration{}).DeclaredType())
	assert.Nil(t, (&v1.OutputDeclaration{}).DeclaredType())
	assert.Equal(t, "int", v1.TypeString((&v1.OutputDeclaration{Type: v1.InputDeclaration_TYPE_INT}).DeclaredType()))
}
