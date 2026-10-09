package sdk

import (
	"context"
	"strings"
	"testing"

	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protodesc"
	"google.golang.org/protobuf/reflect/protoreflect"
	"google.golang.org/protobuf/reflect/protoregistry"
	"google.golang.org/protobuf/types/descriptorpb"
	"google.golang.org/protobuf/types/dynamicpb"

	flowstatev1 "github.com/picatz/flowstate/pkg/flowstate/v1"
)

// credentialInput is a task input message with one `token` string carrying the
// given options, linked the way a plugin's own schema is.
func credentialInput(t *testing.T, name string, options *flowstatev1.InputOptions) proto.Message {
	t.Helper()

	fieldOptions := &descriptorpb.FieldOptions{}
	proto.SetExtension(fieldOptions, flowstatev1.E_Input, options)

	file, err := protodesc.NewFile(&descriptorpb.FileDescriptorProto{
		Name:       proto.String("sdktest/v1/" + strings.ToLower(name) + ".proto"),
		Package:    proto.String("sdktest.v1"),
		Syntax:     proto.String("proto3"),
		Dependency: []string{"flowstate/v1/schema.proto"},
		MessageType: []*descriptorpb.DescriptorProto{{Name: proto.String(name), Field: []*descriptorpb.FieldDescriptorProto{{
			Name: proto.String("token"), Number: proto.Int32(1), Label: descriptorpb.FieldDescriptorProto_LABEL_OPTIONAL.Enum(),
			Type: descriptorpb.FieldDescriptorProto_TYPE_STRING.Enum(), Options: fieldOptions,
		}}}},
	}, protoregistry.GlobalFiles)
	if err != nil {
		t.Fatalf("linking %s: %v", name, err)
	}

	return dynamicpb.NewMessage(file.Messages().ByName(protoreflect.Name(name)))
}

// TestPluginManifestAppliesTheCredentialLattice checks that the SDK refuses at
// build time what the host would refuse at launch, in both directions, and
// carries a valid declaration onto the manifest.
func TestPluginManifestAppliesTheCredentialLattice(t *testing.T) {
	t.Parallel()

	run := func(context.Context, map[string]*flowstatev1.Value, *flowstatev1.Scope) (*flowstatev1.Node_Outputs, error) {
		return nil, nil
	}
	task := func(t *testing.T, name string, options *flowstatev1.InputOptions) Task {
		t.Helper()

		return Task{
			Name: "do", Input: credentialInput(t, name, options), Fn: run,
			SecretInputs: []string{"token"}, RequiredSecretInputs: []string{"token"},
		}
	}
	declare := func(names ...string) []*flowstatev1.CredentialDeclaration {
		var out []*flowstatev1.CredentialDeclaration
		for _, n := range names {
			out = append(out, &flowstatev1.CredentialDeclaration{Name: n, Description: "the " + n, Federated: n == "fed"})
		}

		return out
	}

	manifest, err := Plugin{
		Name: "p", Credentials: declare("bot_token", "fed"),
		Tasks: []Task{task(t, "Good", &flowstatev1.InputOptions{Credential: "bot_token"}), task(t, "Good2", &flowstatev1.InputOptions{Credential: "fed"})},
	}.manifest()
	if err != nil {
		t.Fatalf("a declared and claimed credential was refused: %v", err)
	}
	if got := manifest.GetCredentials(); len(got) != 2 || got[0].GetName() != "bot_token" || !got[1].GetFederated() {
		t.Errorf("manifest credentials = %v, want both declarations carried", got)
	}

	for _, tc := range []struct {
		name   string
		plugin Plugin
		want   string
	}{
		{
			name:   "an undeclared credential name",
			plugin: Plugin{Name: "p", Credentials: declare("bot_token"), Tasks: []Task{task(t, "U", &flowstatev1.InputOptions{Credential: "other"})}},
			want:   `claims credential "other", which the plugin does not declare`,
		},
		{
			name:   "a declared but unused credential",
			plugin: Plugin{Name: "p", Credentials: declare("bot_token", "spare"), Tasks: []Task{task(t, "D", &flowstatev1.InputOptions{Credential: "bot_token"})}},
			want:   `credential "spare" is declared but no task input claims it`,
		},
		{
			name:   "a credential beside a literal claim",
			plugin: Plugin{Name: "p", Credentials: declare("bot_token"), Tasks: []Task{task(t, "L", &flowstatev1.InputOptions{Credential: "bot_token", Literal: true})}},
			want:   "literal",
		},
		{
			name:   "a credential beside a weaker secret claim",
			plugin: Plugin{Name: "p", Credentials: declare("bot_token"), Tasks: []Task{task(t, "W", &flowstatev1.InputOptions{Credential: "bot_token", Secret: flowstatev1.Secret_SECRET_WHOLE_VALUE})}},
			want:   "always SECRET_REQUIRED",
		},
		{
			name:   "a declaration on a plugin that implements only secrets",
			plugin: Plugin{Name: "p", Credentials: declare("bot_token"), Secrets: &Secrets{Schemes: []string{"p"}, Resolve: func(context.Context, SecretRequest) (SecretResponse, error) { return SecretResponse{}, nil }}},
			want:   "declared but no task input claims it",
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			if _, err := tc.plugin.manifest(); err == nil || !strings.Contains(err.Error(), tc.want) {
				t.Errorf("manifest() error = %v, want it to mention %q", err, tc.want)
			}
		})
	}
}
