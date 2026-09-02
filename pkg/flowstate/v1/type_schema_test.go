package flowstatev1_test

import (
	"testing"

	v1 "github.com/picatz/flowstate/pkg/flowstate/v1"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protoreflect"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func scalarType(scalar v1.Type_Scalar) *v1.Type {
	return &v1.Type{Kind: &v1.Type_Scalar_{Scalar: scalar}}
}

func dynType() *v1.Type {
	return &v1.Type{Kind: &v1.Type_Dyn{Dyn: true}}
}

func legacyStructuralType(legacy v1.InputDeclaration_Type) *v1.Type {
	switch legacy {
	case v1.InputDeclaration_TYPE_STRING:
		return scalarType(v1.Type_SCALAR_STRING)
	case v1.InputDeclaration_TYPE_INT:
		return scalarType(v1.Type_SCALAR_INT)
	case v1.InputDeclaration_TYPE_FLOAT:
		return scalarType(v1.Type_SCALAR_DOUBLE)
	case v1.InputDeclaration_TYPE_BOOL:
		return scalarType(v1.Type_SCALAR_BOOL)
	case v1.InputDeclaration_TYPE_STRUCT:
		return &v1.Type{Kind: &v1.Type_Map_{Map: &v1.Type_Map{Value: dynType()}}}
	case v1.InputDeclaration_TYPE_LIST:
		return &v1.Type{Kind: &v1.Type_List{List: dynType()}}
	case v1.InputDeclaration_TYPE_ENUM:
		return &v1.Type{Kind: &v1.Type_Enum{Enum: true}}
	default:
		return nil
	}
}

// TestTypeKindsAreValidAndRoundTrip enumerates the complete structural
// vocabulary. It pins both recursive shapes and every scalar so adding a kind
// without extending the compatibility and projection tests is visible.
func TestTypeKindsAreValidAndRoundTrip(t *testing.T) {
	t.Parallel()

	valid := map[protoreflect.Name]*v1.Type{
		"scalar":  scalarType(v1.Type_SCALAR_STRING),
		"list":    {Kind: &v1.Type_List{List: scalarType(v1.Type_SCALAR_INT)}},
		"map":     {Kind: &v1.Type_Map_{Map: &v1.Type_Map{Value: scalarType(v1.Type_SCALAR_BOOL)}}},
		"enum":    {Kind: &v1.Type_Enum{Enum: true}},
		"message": {Kind: &v1.Type_Message{Message: "example.v1.Customer"}},
		"dyn":     dynType(),
	}

	kind := (&v1.Type{}).ProtoReflect().Descriptor().Oneofs().ByName("kind")
	require.NotNil(t, kind)
	require.Equal(t, kind.Fields().Len(), len(valid), "every Type kind needs a valid round-trip case")
	for i := range kind.Fields().Len() {
		field := kind.Fields().Get(i)
		t.Run(string(field.Name()), func(t *testing.T) {
			original, ok := valid[field.Name()]
			require.True(t, ok, "Type kind %q has no test case", field.Name())
			require.NoError(t, v1.Validate(original))

			wire, err := proto.Marshal(original)
			require.NoError(t, err)
			decoded := new(v1.Type)
			require.NoError(t, proto.Unmarshal(wire, decoded))
			assert.True(t, proto.Equal(original, decoded))
		})
	}

	for scalar := v1.Type_SCALAR_STRING; scalar <= v1.Type_SCALAR_NULL_TYPE; scalar++ {
		require.NoErrorf(t, v1.Validate(scalarType(scalar)), "scalar %s", scalar)
	}
}

func TestTypeValidationRefusesMalformedKinds(t *testing.T) {
	t.Parallel()

	for _, test := range []struct {
		name      string
		typeValue *v1.Type
	}{
		{name: "missing kind", typeValue: &v1.Type{}},
		{name: "unspecified scalar", typeValue: scalarType(v1.Type_SCALAR_UNSPECIFIED)},
		{name: "unknown scalar", typeValue: scalarType(v1.Type_Scalar(99))},
		{name: "list without element", typeValue: &v1.Type{Kind: &v1.Type_List{}}},
		{name: "map without shape", typeValue: &v1.Type{Kind: &v1.Type_Map_{}}},
		{name: "map without value", typeValue: &v1.Type{Kind: &v1.Type_Map_{Map: &v1.Type_Map{}}}},
		{name: "false enum marker", typeValue: &v1.Type{Kind: &v1.Type_Enum{}}},
		{name: "unqualified message", typeValue: &v1.Type{Kind: &v1.Type_Message{Message: ".Customer"}}},
		{name: "false dyn marker", typeValue: &v1.Type{Kind: &v1.Type_Dyn{}}},
	} {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			require.Error(t, v1.Validate(test.typeValue))
		})
	}
}

// TestDeclarationTypeMigration pins the additive wire contract: all seven
// legacy input kinds remain valid and readable, new-only inputs are valid, and
// a writer that sends both representations cannot make old and new readers
// enforce different contracts. Outputs preserve their historical unspecified
// meaning while applying the same agreement rule when both fields are set.
func TestDeclarationTypeMigration(t *testing.T) {
	t.Parallel()

	legacyKinds := []v1.InputDeclaration_Type{
		v1.InputDeclaration_TYPE_STRING,
		v1.InputDeclaration_TYPE_INT,
		v1.InputDeclaration_TYPE_FLOAT,
		v1.InputDeclaration_TYPE_BOOL,
		v1.InputDeclaration_TYPE_STRUCT,
		v1.InputDeclaration_TYPE_LIST,
		v1.InputDeclaration_TYPE_ENUM,
	}
	for _, legacy := range legacyKinds {
		t.Run(legacy.String(), func(t *testing.T) {
			legacyOnly := &v1.InputDeclaration{Name: "value", Type: legacy}
			wire, err := proto.Marshal(legacyOnly)
			require.NoError(t, err)

			decoded := new(v1.InputDeclaration)
			require.NoError(t, proto.Unmarshal(wire, decoded))
			require.Equal(t, legacy, decoded.GetType())
			require.Nil(t, decoded.GetValueType())
			require.NoError(t, v1.Validate(decoded))

			jsonWire, err := protojson.Marshal(legacyOnly)
			require.NoError(t, err)
			jsonDecoded := new(v1.InputDeclaration)
			require.NoError(t, protojson.Unmarshal(jsonWire, jsonDecoded))
			require.Equal(t, legacy, jsonDecoded.GetType())
			require.Nil(t, jsonDecoded.GetValueType())
			require.NoError(t, v1.Validate(jsonDecoded))

			matching := proto.Clone(decoded).(*v1.InputDeclaration)
			matching.ValueType = legacyStructuralType(legacy)
			require.NoError(t, v1.Validate(matching))

			disagreeing := proto.Clone(matching).(*v1.InputDeclaration)
			disagreeing.ValueType = scalarType(v1.Type_SCALAR_BYTES)
			require.Error(t, v1.Validate(disagreeing))
		})
	}

	require.NoError(t, v1.Validate(&v1.InputDeclaration{
		Name: "value", ValueType: scalarType(v1.Type_SCALAR_BYTES),
	}))
	require.Error(t, v1.Validate(&v1.InputDeclaration{Name: "value"}))

	require.NoError(t, v1.Validate(&v1.OutputDeclaration{Name: "value", Value: v1.NewExpr("42")}))
	require.NoError(t, v1.Validate(&v1.OutputDeclaration{
		Name: "value", Value: v1.NewExpr("42"), ValueType: scalarType(v1.Type_SCALAR_INT),
	}))
	require.NoError(t, v1.Validate(&v1.OutputDeclaration{
		Name: "value", Value: v1.NewExpr("42"), Type: v1.InputDeclaration_TYPE_INT,
		ValueType: scalarType(v1.Type_SCALAR_INT),
	}))
	require.Error(t, v1.Validate(&v1.OutputDeclaration{
		Name: "value", Value: v1.NewExpr("42"), Type: v1.InputDeclaration_TYPE_STRING,
		ValueType: scalarType(v1.Type_SCALAR_INT),
	}))
}
