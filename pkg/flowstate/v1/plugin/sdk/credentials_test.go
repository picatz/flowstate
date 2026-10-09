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

// TestDecodeInputsRefusesACredentialThatIsNotTheResolvedString is the SDK's half
// of the contract a credential claim makes: the host hands the task the string it
// resolved the author's reference to, and anything else means the host did not
// deliver what the claim promised. The negative directions are the point; the
// string and an unclaimed field show the refusal reaches the claim and no
// further.
func TestDecodeInputsRefusesACredentialThatIsNotTheResolvedString(t *testing.T) {
	t.Parallel()

	secretRef := &flowstatev1.Value{Kind: &flowstatev1.Value_SecretRef{SecretRef: &flowstatev1.SecretRef{Scheme: "env", Name: "BOT_TOKEN"}}}
	for _, tc := range []struct {
		name  string
		value *flowstatev1.Value
		want  string
	}{
		{"an unresolved secret reference", secretRef, "an unresolved secret reference"},
		{"an unresolved credential reference", flowstatev1.NewCredentialRef("partner"), "an unresolved credential reference"},
		{"an unresolved expression", flowstatev1.NewExpr("inputs.token"), "an unresolved expression"},
		{"a number", flowstatev1.NewLiteral(int64(7)), "not a string"},
		{"a list", flowstatev1.NewValue([]any{"a"}), "not a string"},
		{"an absent value", &flowstatev1.Value{}, "no value"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			in := credentialInput(t, "Refused"+strings.ReplaceAll(tc.name, " ", ""), &flowstatev1.InputOptions{Credential: "bot_token"})
			err := DecodeInputs(map[string]*flowstatev1.Value{"token": tc.value}, in)
			if err == nil || !strings.Contains(err.Error(), tc.want) || !strings.Contains(err.Error(), `credential "bot_token"`) {
				t.Fatalf("DecodeInputs error = %v, want a refusal naming the credential and %q", err, tc.want)
			}
			if strings.Contains(err.Error(), "BOT_TOKEN") {
				t.Errorf("the refusal named the reference: %v", err)
			}
			if IsInvalidInput(err) {
				t.Errorf("a host-contract refusal was classified as the workflow's invalid input: %v", err)
			}
			if in.ProtoReflect().Has(in.ProtoReflect().Descriptor().Fields().ByName("token")) {
				t.Error("the field was filled before it was refused")
			}
		})
	}

	t.Run("the resolved string is decoded", func(t *testing.T) {
		t.Parallel()

		in := credentialInput(t, "Resolved", &flowstatev1.InputOptions{Credential: "bot_token"})
		if err := DecodeInputs(map[string]*flowstatev1.Value{"token": flowstatev1.NewValue("xoxb-resolved")}, in); err != nil {
			t.Fatalf("DecodeInputs refused the resolved string: %v", err)
		}
		if got := in.ProtoReflect().Get(in.ProtoReflect().Descriptor().Fields().ByName("token")).String(); got != "xoxb-resolved" {
			t.Errorf("token = %q, want the resolved string", got)
		}
	})

	t.Run("an unclaimed field is not held to it", func(t *testing.T) {
		t.Parallel()

		in := credentialInput(t, "Unclaimed", &flowstatev1.InputOptions{})
		if err := DecodeInputs(map[string]*flowstatev1.Value{"token": flowstatev1.NewLiteral(int64(7))}, in); err == nil {
			t.Fatal("a number in a string field was accepted")
		} else if strings.Contains(err.Error(), "claims credential") {
			t.Errorf("an unclaimed field was refused as a credential: %v", err)
		}
	})
}
