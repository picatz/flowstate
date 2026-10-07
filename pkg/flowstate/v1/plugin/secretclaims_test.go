package plugin

import (
	"maps"
	"slices"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protodesc"
	"google.golang.org/protobuf/reflect/protoregistry"
	"google.golang.org/protobuf/types/descriptorpb"
	"google.golang.org/protobuf/types/dynamicpb"

	pluginv1 "github.com/picatz/flowstate/pkg/flowstate/plugin/v1"
	flowstatev1 "github.com/picatz/flowstate/pkg/flowstate/v1"
)

// claimedMessage is the full name of the message [claimedInputs] describes.
const claimedMessage = "plugintest.v1.Claimed"

// claimedInputs is a descriptor for a task input message whose string fields
// carry the given secret claims on the `flowstate.v1.input` option, plus an
// unclaimed `count` field. Like [widgetFile] it exists only as bytes, which is
// the host's position with a real plugin's schema; unlike it, its fields say
// which of them accept a secret reference, so a manifest listing them agrees.
func claimedInputs(claims map[string]flowstatev1.Secret) []byte {
	optional := descriptorpb.FieldDescriptorProto_LABEL_OPTIONAL.Enum()

	fields := []*descriptorpb.FieldDescriptorProto{{
		Name: proto.String("count"), Number: proto.Int32(1), Label: optional,
		Type: descriptorpb.FieldDescriptorProto_TYPE_INT64.Enum(),
	}}
	number := int32(2)
	for _, name := range slices.Sorted(maps.Keys(claims)) {
		options := &descriptorpb.FieldOptions{}
		proto.SetExtension(options, flowstatev1.E_Input, &flowstatev1.InputOptions{Secret: claims[name]})
		fields = append(fields, &descriptorpb.FieldDescriptorProto{
			Name: proto.String(name), Number: proto.Int32(number), Label: optional,
			Type: descriptorpb.FieldDescriptorProto_TYPE_STRING.Enum(), Options: options,
		})
		number++
	}

	raw, err := proto.Marshal(&descriptorpb.FileDescriptorSet{File: []*descriptorpb.FileDescriptorProto{{
		Name:        proto.String("plugintest/v1/claimed.proto"),
		Package:     proto.String("plugintest.v1"),
		Syntax:      proto.String("proto3"),
		Dependency:  []string{"flowstate/v1/schema.proto"},
		MessageType: []*descriptorpb.DescriptorProto{{Name: proto.String("Claimed"), Field: fields}},
	}}})
	if err != nil {
		panic(err)
	}
	return raw
}

// claimedInputsMessage is [claimedInputs] as a message an SDK plugin can take
// as its task's input, built without registering it anywhere global.
func claimedInputsMessage(claims map[string]flowstatev1.Secret) proto.Message {
	var set descriptorpb.FileDescriptorSet
	if err := proto.Unmarshal(claimedInputs(claims), &set); err != nil {
		panic(err)
	}
	file, err := protodesc.NewFile(set.GetFile()[0], protoregistry.GlobalFiles)
	if err != nil {
		panic(err)
	}
	return dynamicpb.NewMessage(file.Messages().ByName("Claimed"))
}

// TestAManifestMustAgreeWithItsDescriptorAboutSecretInputs is the refusal in
// both directions: a name the manifest lists that the descriptor does not
// claim, and a claim the descriptor makes that the manifest does not list. Each
// is a plugin saying two things about one input, and the host believes neither.
func TestAManifestMustAgreeWithItsDescriptorAboutSecretInputs(t *testing.T) {
	t.Parallel()

	for _, tc := range []struct {
		name     string
		claims   map[string]flowstatev1.Secret
		secret   []string
		required []string
		wantErr  string
	}{
		{
			name:     "agreeing",
			claims:   map[string]flowstatev1.Secret{"token": flowstatev1.Secret_SECRET_REQUIRED, "key": flowstatev1.Secret_SECRET_WHOLE_VALUE},
			secret:   []string{"token", "key"},
			required: []string{"token"},
		},
		{
			name:   "none declared anywhere",
			claims: nil,
		},
		{
			name:    "manifest lists a name the descriptor does not claim",
			claims:  map[string]flowstatev1.Secret{"token": flowstatev1.Secret_SECRET_WHOLE_VALUE},
			secret:  []string{"token", "count"},
			wantErr: `secret_inputs names "count"`,
		},
		{
			name:    "descriptor claims a name the manifest does not list",
			claims:  map[string]flowstatev1.Secret{"token": flowstatev1.Secret_SECRET_WHOLE_VALUE, "key": flowstatev1.Secret_SECRET_WHOLE_VALUE},
			secret:  []string{"token"},
			wantErr: `input "key" is declared secret in its descriptor but secret_inputs does not name it`,
		},
		{
			name:     "manifest requires what the descriptor only permits",
			claims:   map[string]flowstatev1.Secret{"token": flowstatev1.Secret_SECRET_WHOLE_VALUE},
			secret:   []string{"token"},
			required: []string{"token"},
			wantErr:  `required_secret_inputs names "token"`,
		},
		{
			name:    "descriptor requires what the manifest only permits",
			claims:  map[string]flowstatev1.Secret{"token": flowstatev1.Secret_SECRET_REQUIRED},
			secret:  []string{"token"},
			wantErr: `input "token" is declared secret in its descriptor but required_secret_inputs does not name it`,
		},
		{
			name:    "a nested claim is never a plugin's",
			claims:  map[string]flowstatev1.Secret{"token": flowstatev1.Secret_SECRET_NESTED},
			wantErr: "SECRET_NESTED",
		},
		{
			name:    "an unknown claim fails closed",
			claims:  map[string]flowstatev1.Secret{"token": flowstatev1.Secret(99)},
			secret:  []string{"token"},
			wantErr: "unknown secret claim 99",
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			_, err := (&Plugin{name: "example"}).taskDef(&pluginv1.TaskManifest{
				Name:                 "task",
				InputDescriptor:      claimedInputs(tc.claims),
				InputMessage:         claimedMessage,
				OutputMessage:        "flowstate.v1.Task.Log.Outputs",
				SecretInputs:         tc.secret,
				RequiredSecretInputs: tc.required,
			}, Config{}.withDefaults())

			if tc.wantErr == "" {
				require.NoError(t, err)
				return
			}
			require.Error(t, err)
			assert.Contains(t, err.Error(), tc.wantErr)
		})
	}
}

// TestACatalogTaskMustAgreeWithItsDescriptorAboutSecretInputs is the same
// refusal for a task rebuilt from a catalog document, which carries both the
// descriptor and the lists the launching host described it with.
func TestACatalogTaskMustAgreeWithItsDescriptorAboutSecretInputs(t *testing.T) {
	t.Parallel()

	described := func(secret ...string) *flowstatev1.TaskDescription {
		return &flowstatev1.TaskDescription{
			Name:            "example.task",
			InputDescriptor: claimedInputs(map[string]flowstatev1.Secret{"token": flowstatev1.Secret_SECRET_WHOLE_VALUE}),
			InputMessage:    claimedMessage,
			OutputMessage:   "flowstate.v1.Task.Log.Outputs",
			SecretInputs:    secret,
		}
	}

	_, err := TaskDefFromDescription(described("token"), Config{})
	require.NoError(t, err)

	_, err = TaskDefFromDescription(described(), Config{})
	require.ErrorContains(t, err, `input "token" is declared secret in its descriptor`)

	_, err = TaskDefFromDescription(described("token", "count"), Config{})
	require.ErrorContains(t, err, `secret_inputs names "count"`)
}

// fetchInputsDescriptor is an http-shaped input message a plugin could ship: a
// url and an `outputs` input, without the engine's own nested secret claims.
func fetchInputsDescriptor(t *testing.T) []byte {
	t.Helper()

	optional := descriptorpb.FieldDescriptorProto_LABEL_OPTIONAL.Enum()

	return mustMarshal(t, &descriptorpb.FileDescriptorSet{File: []*descriptorpb.FileDescriptorProto{{
		Name:       proto.String("plugintest/v1/fetch.proto"),
		Package:    proto.String("plugintest.v1"),
		Syntax:     proto.String("proto3"),
		Dependency: []string{"flowstate/v1/value.proto"},
		MessageType: []*descriptorpb.DescriptorProto{{
			Name: proto.String("FetchInputs"),
			Field: []*descriptorpb.FieldDescriptorProto{
				{Name: proto.String("url"), Number: proto.Int32(1), Label: optional, Type: descriptorpb.FieldDescriptorProto_TYPE_STRING.Enum()},
				{
					Name: proto.String("outputs"), Number: proto.Int32(2), Label: optional,
					Type: descriptorpb.FieldDescriptorProto_TYPE_MESSAGE.Enum(), TypeName: proto.String(".flowstate.v1.Value"),
				},
			},
		}},
	}}})
}
