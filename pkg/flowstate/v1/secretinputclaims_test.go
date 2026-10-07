package flowstatev1_test

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protodesc"
	"google.golang.org/protobuf/reflect/protoreflect"
	"google.golang.org/protobuf/reflect/protoregistry"
	"google.golang.org/protobuf/types/descriptorpb"

	v1 "github.com/picatz/flowstate/pkg/flowstate/v1"
)

// claimField describes one field of a message built for these tests: its name,
// its type, and the secret claim set on it, if any.
type claimField struct {
	name     string
	typ      descriptorpb.FieldDescriptorProto_Type
	typeName string
	repeated bool
	claim    *v1.Secret
}

// claimMessage links a message with the given fields, in the given order, and
// returns its descriptor. It exists only as a descriptor, the way a plugin's
// schema does on the host.
func claimMessage(t *testing.T, fields ...claimField) protoreflect.MessageDescriptor {
	t.Helper()

	var out []*descriptorpb.FieldDescriptorProto
	for i, f := range fields {
		label := descriptorpb.FieldDescriptorProto_LABEL_OPTIONAL
		if f.repeated {
			label = descriptorpb.FieldDescriptorProto_LABEL_REPEATED
		}
		fd := &descriptorpb.FieldDescriptorProto{
			Name: proto.String(f.name), Number: proto.Int32(int32(i + 1)), Label: label.Enum(), Type: f.typ.Enum(),
		}
		if f.typeName != "" {
			fd.TypeName = proto.String(f.typeName)
		}
		if f.claim != nil {
			fd.Options = &descriptorpb.FieldOptions{}
			proto.SetExtension(fd.Options, v1.E_Input, &v1.InputOptions{Secret: *f.claim})
		}
		out = append(out, fd)
	}

	file, err := protodesc.NewFile(&descriptorpb.FileDescriptorProto{
		Name:        proto.String("claims/v1/claims.proto"),
		Package:     proto.String("claims.v1"),
		Syntax:      proto.String("proto3"),
		Dependency:  []string{"flowstate/v1/schema.proto", "flowstate/v1/value.proto"},
		MessageType: []*descriptorpb.DescriptorProto{{Name: proto.String("Inputs"), Field: out}},
	}, protoregistry.GlobalFiles)
	require.NoError(t, err)

	return file.Messages().ByName("Inputs")
}

func claim(c v1.Secret) *v1.Secret { return &c }

func TestSecretInputClaims(t *testing.T) {
	t.Parallel()

	const (
		str   = descriptorpb.FieldDescriptorProto_TYPE_STRING
		i64   = descriptorpb.FieldDescriptorProto_TYPE_INT64
		msg   = descriptorpb.FieldDescriptorProto_TYPE_MESSAGE
		value = ".flowstate.v1.Value"
	)

	for _, tc := range []struct {
		name         string
		fields       []claimField
		whole        []string
		required     []string
		nested       []string
		wantErr      string
		noDescriptor bool
	}{
		{name: "no claims", fields: []claimField{{name: "a", typ: str}}},
		{
			name: "an unspecified claim is no claim",
			fields: []claimField{
				{name: "a", typ: msg, typeName: value, claim: claim(v1.Secret_SECRET_UNSPECIFIED)},
			},
		},
		{
			name: "whole value, in sorted order whatever the declaration order",
			fields: []claimField{
				{name: "zeta", typ: msg, typeName: value, claim: claim(v1.Secret_SECRET_WHOLE_VALUE)},
				{name: "alpha", typ: str, claim: claim(v1.Secret_SECRET_WHOLE_VALUE)},
				{name: "plain", typ: str},
			},
			whole: []string{"alpha", "zeta"},
		},
		{
			name: "required implies whole value",
			fields: []claimField{
				{name: "token", typ: msg, typeName: value, claim: claim(v1.Secret_SECRET_REQUIRED)},
				{name: "key", typ: msg, typeName: value, claim: claim(v1.Secret_SECRET_WHOLE_VALUE)},
			},
			whole:    []string{"key", "token"},
			required: []string{"token"},
		},
		{
			name: "nested on a value and is not whole value",
			fields: []claimField{
				{name: "body", typ: msg, typeName: value, claim: claim(v1.Secret_SECRET_NESTED)},
			},
			nested: []string{"body"},
		},
		{
			name:         "a nil message has no claims",
			noDescriptor: true,
		},
		{
			name:    "whole value on an integer fails closed",
			fields:  []claimField{{name: "n", typ: i64, claim: claim(v1.Secret_SECRET_WHOLE_VALUE)}},
			wantErr: `input "n"`,
		},
		{
			name:    "required on a message that is not a Value fails closed",
			fields:  []claimField{{name: "n", typ: msg, typeName: ".flowstate.v1.InputOptions", claim: claim(v1.Secret_SECRET_REQUIRED)}},
			wantErr: `input "n"`,
		},
		{
			name:    "whole value on a repeated Value fails closed",
			fields:  []claimField{{name: "n", typ: msg, typeName: value, repeated: true, claim: claim(v1.Secret_SECRET_WHOLE_VALUE)}},
			wantErr: `input "n"`,
		},
		{
			name:    "nested on a scalar fails closed",
			fields:  []claimField{{name: "n", typ: i64, claim: claim(v1.Secret_SECRET_NESTED)}},
			wantErr: `input "n"`,
		},
		{
			name:    "an unknown claim number fails closed",
			fields:  []claimField{{name: "n", typ: str, claim: claim(v1.Secret(42))}},
			wantErr: "unknown secret claim 42",
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			var md protoreflect.MessageDescriptor
			if !tc.noDescriptor {
				md = claimMessage(t, tc.fields...)
			}

			whole, required, nested, err := v1.SecretInputClaims(md)
			if tc.wantErr != "" {
				require.ErrorContains(t, err, tc.wantErr)
				assert.Empty(t, whole)
				assert.Empty(t, required)
				assert.Empty(t, nested)
				return
			}

			require.NoError(t, err)
			assert.Equal(t, tc.whole, whole)
			assert.Equal(t, tc.required, required)
			assert.Equal(t, tc.nested, nested)
		})
	}
}

// TestTheHTTPTaskDerivesItsSecretClaimsFromItsSchema proves the lists the
// built-in definition used to spell by hand are the ones its input message now
// declares, so nothing downstream of them (the claims digest, the notes `flow
// tasks` prints, the documentation) moves.
func TestTheHTTPTaskDerivesItsSecretClaimsFromItsSchema(t *testing.T) {
	t.Parallel()

	whole, required, nested, err := v1.SecretInputClaims((&v1.Task_HTTP_Inputs{}).ProtoReflect().Descriptor())
	require.NoError(t, err)
	assert.Empty(t, whole, "the http task's whole-value credential is an authority input, not a secret input")
	assert.Empty(t, required)
	assert.Equal(t, []string{"form", "headers", "json"}, nested)

	def := v1.HTTPTaskDef(nil)
	assert.Equal(t, []string{"form", "headers", "json"}, def.NestedSecretInputs,
		"the list before it was derived from the schema")
	assert.Empty(t, def.SecretInputs)
	assert.Empty(t, def.RequiredSecretInputs)

	// And through the description a reader gets, which is what the digest hashes.
	described := v1.DescribeTask(def)
	assert.Empty(t, described.GetSecretInputs())
	assert.Empty(t, described.GetRequiredSecretInputs())
}
