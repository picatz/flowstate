package plugin

import (
	"maps"
	"slices"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/descriptorpb"

	pluginv1 "github.com/picatz/flowstate/pkg/flowstate/plugin/v1"
	flowstatev1 "github.com/picatz/flowstate/pkg/flowstate/v1"
)

// credentialInputs is a descriptor for a task input message whose string fields
// carry the given InputOptions, plus an unclaimed `count`, as
// [claimedInputs] does for secret claims alone.
func credentialInputs(options map[string]*flowstatev1.InputOptions) []byte {
	optional := descriptorpb.FieldDescriptorProto_LABEL_OPTIONAL.Enum()

	fields := []*descriptorpb.FieldDescriptorProto{{
		Name: proto.String("count"), Number: proto.Int32(1), Label: optional,
		Type: descriptorpb.FieldDescriptorProto_TYPE_INT64.Enum(),
	}}
	number := int32(2)
	for _, name := range slices.Sorted(maps.Keys(options)) {
		o := &descriptorpb.FieldOptions{}
		proto.SetExtension(o, flowstatev1.E_Input, options[name])
		fields = append(fields, &descriptorpb.FieldDescriptorProto{
			Name: proto.String(name), Number: proto.Int32(number), Label: optional,
			Type: descriptorpb.FieldDescriptorProto_TYPE_STRING.Enum(), Options: o,
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

func credentialDeclarations(names ...string) []*flowstatev1.CredentialDeclaration {
	var out []*flowstatev1.CredentialDeclaration
	for _, n := range names {
		out = append(out, &flowstatev1.CredentialDeclaration{Name: n, Description: "the " + n})
	}

	return out
}

// credentialTask is a manifest whose `token` input claims credential.
func credentialTask(name, credential string) *pluginv1.TaskManifest {
	return &pluginv1.TaskManifest{
		Name:                 name,
		InputDescriptor:      credentialInputs(map[string]*flowstatev1.InputOptions{"token": {Credential: credential}}),
		InputMessage:         claimedMessage,
		OutputMessage:        "flowstate.v1.Task.Log.Outputs",
		SecretInputs:         []string{"token"},
		RequiredSecretInputs: []string{"token"},
	}
}

// TestALaunchedPluginMustDeclareTheCredentialsItsInputsClaim is the lattice at the
// host boundary in both directions, plus the options that may not sit beside a
// credential. A refused plugin keeps no task registered.
func TestALaunchedPluginMustDeclareTheCredentialsItsInputsClaim(t *testing.T) {
	t.Parallel()

	for _, tc := range []struct {
		name    string
		declare []*flowstatev1.CredentialDeclaration
		tasks   []*pluginv1.TaskManifest
		wantErr string
	}{
		{
			name:    "declared and claimed",
			declare: credentialDeclarations("bot_token"),
			tasks:   []*pluginv1.TaskManifest{credentialTask("post", "bot_token"), credentialTask("update", "bot_token")},
		},
		{
			name:  "nothing declared, nothing claimed",
			tasks: []*pluginv1.TaskManifest{{Name: "plain", InputMessage: "flowstate.v1.Task.Log.Inputs", OutputMessage: "flowstate.v1.Task.Log.Outputs"}},
		},
		{
			name:    "a claim naming an undeclared credential",
			declare: credentialDeclarations("bot_token"),
			tasks:   []*pluginv1.TaskManifest{credentialTask("post", "bot_token"), credentialTask("update", "other")},
			wantErr: `claims credential "other", which the plugin does not declare`,
		},
		{
			name:    "a claim when the plugin declares none",
			tasks:   []*pluginv1.TaskManifest{credentialTask("post", "bot_token")},
			wantErr: "does not declare",
		},
		{
			name:    "a declaration no input claims",
			declare: credentialDeclarations("bot_token", "spare"),
			tasks:   []*pluginv1.TaskManifest{credentialTask("post", "bot_token")},
			wantErr: `credential "spare" is declared but no task input claims it`,
		},
		{
			name:    "a declaration on a plugin whose only claimant is gone",
			declare: credentialDeclarations("bot_token"),
			tasks:   nil,
			wantErr: "declared but no task input claims it",
		},
		{
			name:    "more declarations than a plugin may have",
			declare: credentialDeclarations("a", "b", "c", "d", "e", "f", "g", "h", "i"),
			tasks:   []*pluginv1.TaskManifest{credentialTask("post", "a")},
			wantErr: "more than the 8",
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			p := &Plugin{name: "example", manifest: &pluginv1.PluginManifest{Credentials: tc.declare}}
			defs := map[string]taskBinding{}
			for _, task := range tc.tasks {
				def, err := p.taskDef(task, Config{}.withDefaults())
				require.NoError(t, err)
				defs["example."+task.GetName()] = taskBinding{plugin: p, def: def}
			}

			err := checkManifestCredentials(p, defs)
			if tc.wantErr == "" {
				require.NoError(t, err)
				return
			}
			require.ErrorIs(t, err, ErrManifest)
			assert.Contains(t, err.Error(), tc.wantErr)
		})
	}
}

// TestATaskMayNotClaimACredentialBesideAnotherClaim covers the claims that cannot
// share a field with a credential, as the host sees them in a plugin's schema.
func TestATaskMayNotClaimACredentialBesideAnotherClaim(t *testing.T) {
	t.Parallel()

	for name, tc := range map[string]struct {
		options *flowstatev1.InputOptions
		list    []string
		wantErr string
	}{
		"literal": {
			options: &flowstatev1.InputOptions{Credential: "bot_token", Literal: true},
			list:    []string{"token"}, wantErr: "literal",
		},
		"SECRET_WHOLE_VALUE": {
			options: &flowstatev1.InputOptions{Credential: "bot_token", Secret: flowstatev1.Secret_SECRET_WHOLE_VALUE},
			list:    []string{"token"}, wantErr: "always SECRET_REQUIRED",
		},
		"SECRET_NESTED": {
			options: &flowstatev1.InputOptions{Credential: "bot_token", Secret: flowstatev1.Secret_SECRET_NESTED},
			wantErr: "always SECRET_REQUIRED",
		},
		"a malformed name": {
			options: &flowstatev1.InputOptions{Credential: "Bot Token"},
			list:    []string{"token"}, wantErr: "not a name matching",
		},
	} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()

			_, err := (&Plugin{name: "example"}).taskDef(&pluginv1.TaskManifest{
				Name:                 "task",
				InputDescriptor:      credentialInputs(map[string]*flowstatev1.InputOptions{"token": tc.options}),
				InputMessage:         claimedMessage,
				OutputMessage:        "flowstate.v1.Task.Log.Outputs",
				SecretInputs:         tc.list,
				RequiredSecretInputs: tc.list,
			}, Config{}.withDefaults())
			require.Error(t, err)
			assert.Contains(t, err.Error(), tc.wantErr)
		})
	}
}

// TestACredentialChangeIsNotTheSamePlugin: a plugin that comes back declaring a
// different credential set, or flipping federated, is not the one workflows were
// checked against.
func TestACredentialChangeIsNotTheSamePlugin(t *testing.T) {
	t.Parallel()

	before := &pluginv1.PluginManifest{Credentials: credentialDeclarations("bot_token")}
	require.NoError(t, manifestUnchanged(before, proto.Clone(before).(*pluginv1.PluginManifest)))

	federated := proto.Clone(before).(*pluginv1.PluginManifest)
	federated.Credentials[0].Federated = true
	require.ErrorContains(t, manifestUnchanged(before, federated), `credential "bot_token" differently`)

	require.ErrorContains(t, manifestUnchanged(before, &pluginv1.PluginManifest{}), "declaring 0 credentials rather than 1")
}

// TestACatalogMustAgreeAboutCredentials holds a catalog document to the same
// lattice, and to its own credential_inputs: a described map that disagrees
// with the descriptor is refused, not trusted.
func TestACatalogMustAgreeAboutCredentials(t *testing.T) {
	t.Parallel()

	described := func(credentialInputsMap map[string]string) *flowstatev1.TaskDescription {
		return &flowstatev1.TaskDescription{
			Name:                 "example.post",
			InputDescriptor:      credentialInputs(map[string]*flowstatev1.InputOptions{"token": {Credential: "bot_token"}}),
			InputMessage:         claimedMessage,
			OutputMessage:        "flowstate.v1.Task.Log.Outputs",
			SecretInputs:         []string{"token"},
			CredentialInputs:     credentialInputsMap,
			RequiredSecretInputs: []string{"token"},
		}
	}
	catalog := func(task *flowstatev1.TaskDescription, declare ...string) *flowstatev1.PluginCatalog {
		return &flowstatev1.PluginCatalog{
			ClaimsSchemaVersion: flowstatev1.CurrentClaimsSchemaVersion,
			Plugins: []*flowstatev1.PluginDescription{{
				Name: "example", Tasks: []*flowstatev1.TaskDescription{task}, Credentials: credentialDeclarations(declare...),
			}},
		}
	}

	good := map[string]string{"token": "bot_token"}

	_, err := TaskDefsFromCatalog(catalog(described(good), "bot_token"), Config{})
	require.NoError(t, err)

	_, err = TaskDefsFromCatalog(catalog(described(good), "other"), Config{})
	require.ErrorContains(t, err, `claims credential "bot_token", which the plugin does not declare`)

	_, err = TaskDefsFromCatalog(catalog(described(good), "bot_token", "spare"), Config{})
	require.ErrorContains(t, err, `credential "spare" is declared but no task input claims it`)

	_, err = TaskDefsFromCatalog(catalog(described(nil), "bot_token"), Config{})
	require.ErrorContains(t, err, "credential_inputs does not match")

	_, err = TaskDefsFromCatalog(catalog(described(map[string]string{"token": "elsewhere"}), "bot_token"), Config{})
	require.ErrorContains(t, err, "credential_inputs does not match")
}
