package flowstatev1_test

import (
	"strings"
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

// credField is one field of a message built for these tests, with the whole
// InputOptions set on it so a test can combine claims.
type credField struct {
	name     string
	typ      descriptorpb.FieldDescriptorProto_Type
	typeName string
	repeated bool
	options  *v1.InputOptions
}

const (
	credStr = descriptorpb.FieldDescriptorProto_TYPE_STRING
	credMsg = descriptorpb.FieldDescriptorProto_TYPE_MESSAGE
	credI64 = descriptorpb.FieldDescriptorProto_TYPE_INT64
)

func credToken(options *v1.InputOptions) credField {
	return credField{name: "token", typ: credMsg, typeName: ".flowstate.v1.Value", options: options}
}

func credMessage(t *testing.T, name string, fields ...credField) protoreflect.MessageDescriptor {
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
		if f.options != nil {
			fd.Options = &descriptorpb.FieldOptions{}
			proto.SetExtension(fd.Options, v1.E_Input, f.options)
		}
		out = append(out, fd)
	}

	file, err := protodesc.NewFile(&descriptorpb.FileDescriptorProto{
		Name:        proto.String("creds/v1/" + strings.ToLower(name) + ".proto"),
		Package:     proto.String("creds.v1"),
		Syntax:      proto.String("proto3"),
		Dependency:  []string{"flowstate/v1/schema.proto", "flowstate/v1/value.proto"},
		MessageType: []*descriptorpb.DescriptorProto{{Name: proto.String(name), Field: out}},
	}, protoregistry.GlobalFiles)
	require.NoError(t, err)

	return file.Messages().ByName(protoreflect.Name(name))
}

func declaration(name string) *v1.CredentialDeclaration {
	return &v1.CredentialDeclaration{Name: name, Description: "the " + name}
}

func TestInputClaimsReadsACredentialAsRequired(t *testing.T) {
	t.Parallel()

	for name, f := range map[string]credField{
		"a Value":                credToken(&v1.InputOptions{Credential: "bot_token"}),
		"a string":               {name: "token", typ: credStr, options: &v1.InputOptions{Credential: "bot_token"}},
		"beside SECRET_REQUIRED": credToken(&v1.InputOptions{Credential: "bot_token", Secret: v1.Secret_SECRET_REQUIRED}),
	} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()

			md := credMessage(t, "Ok", f, credField{name: "channel", typ: credStr})
			claims, err := v1.InputClaims(md)
			require.NoError(t, err)
			require.Len(t, claims, 1)
			assert.Equal(t, "token", claims[0].Name)
			assert.Equal(t, "bot_token", claims[0].Credential)
			assert.Equal(t, v1.Secret_SECRET_REQUIRED, claims[0].Secret, "a credential implies SECRET_REQUIRED")
			assert.Equal(t, map[string]string{"token": "bot_token"}, v1.CredentialInputs(claims))

			// The projection every secret consumer reads sees it as a required whole
			// value, so the manifest lists and the resolution path need no new case.
			whole, required, nested, err := v1.SecretInputClaims(md)
			require.NoError(t, err)
			assert.Equal(t, []string{"token"}, whole)
			assert.Equal(t, []string{"token"}, required)
			assert.Empty(t, nested)
		})
	}
}

func TestInputClaimsRefusesAMalformedCredentialClaim(t *testing.T) {
	t.Parallel()

	for name, tc := range map[string]struct {
		field credField
		want  string
	}{
		"beside SECRET_WHOLE_VALUE": {credToken(&v1.InputOptions{Credential: "bot_token", Secret: v1.Secret_SECRET_WHOLE_VALUE}), "always SECRET_REQUIRED"},
		"beside SECRET_NESTED":      {credToken(&v1.InputOptions{Credential: "bot_token", Secret: v1.Secret_SECRET_NESTED}), "always SECRET_REQUIRED"},
		"beside an unknown secret":  {credToken(&v1.InputOptions{Credential: "bot_token", Secret: v1.Secret(99)}), "always SECRET_REQUIRED"},
		"beside literal":            {credField{name: "token", typ: credStr, options: &v1.InputOptions{Credential: "bot_token", Literal: true}}, "literal"},
		"uppercase name":            {credToken(&v1.InputOptions{Credential: "Bot_Token"}), "not a name matching"},
		"leading digit":             {credToken(&v1.InputOptions{Credential: "1bot"}), "not a name matching"},
		"hyphenated name":           {credToken(&v1.InputOptions{Credential: "bot-token"}), "not a name matching"},
		"an over-long name":         {credToken(&v1.InputOptions{Credential: "a" + strings.Repeat("b", 32)}), "not a name matching"},
		"on an integer":             {credField{name: "token", typ: credI64, options: &v1.InputOptions{Credential: "bot_token"}}, "neither a flowstate.v1.Value nor a string"},
		"on a repeated string":      {credField{name: "token", typ: credStr, repeated: true, options: &v1.InputOptions{Credential: "bot_token"}}, "neither a flowstate.v1.Value nor a string"},
		"on a repeated Value":       {credField{name: "token", typ: credMsg, typeName: ".flowstate.v1.Value", repeated: true, options: &v1.InputOptions{Credential: "bot_token"}}, "neither a flowstate.v1.Value nor a string"},
	} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()

			_, err := v1.InputClaims(credMessage(t, "Bad", tc.field))
			require.Error(t, err)
			assert.Contains(t, err.Error(), tc.want)
			assert.Contains(t, err.Error(), `"token"`, "the refusal names the input")

			// Every reader of the option fails the same way, so none reads it
			// charitably.
			_, _, _, err = v1.SecretInputClaims(credMessage(t, "Bad2", tc.field))
			require.Error(t, err)
		})
	}
}

func TestCheckPluginCredentials(t *testing.T) {
	t.Parallel()

	uses := func(t *testing.T, name, credential string) protoreflect.MessageDescriptor {
		t.Helper()

		return credMessage(t, name, credToken(&v1.InputOptions{Credential: credential}))
	}
	many := func(n int) []*v1.CredentialDeclaration {
		var out []*v1.CredentialDeclaration
		for i := range n {
			out = append(out, declaration("c"+strings.Repeat("x", i)))
		}

		return out
	}

	t.Run("a declared credential used by two tasks", func(t *testing.T) {
		t.Parallel()
		require.NoError(t, v1.CheckPluginCredentials(
			[]*v1.CredentialDeclaration{declaration("bot_token")},
			[]protoreflect.MessageDescriptor{uses(t, "A1", "bot_token"), uses(t, "A2", "bot_token")}))
	})

	t.Run("no credentials at all", func(t *testing.T) {
		t.Parallel()
		require.NoError(t, v1.CheckPluginCredentials(nil, []protoreflect.MessageDescriptor{credMessage(t, "Plain", credField{name: "channel", typ: credStr}), nil}))
	})

	t.Run("exactly the maximum", func(t *testing.T) {
		t.Parallel()
		decls := many(v1.MaxPluginCredentials)
		var tasks []protoreflect.MessageDescriptor
		for i, d := range decls {
			tasks = append(tasks, uses(t, "Max"+strings.Repeat("X", i), d.GetName()))
		}
		require.NoError(t, v1.CheckPluginCredentials(decls, tasks))
	})

	for name, tc := range map[string]struct {
		decls []*v1.CredentialDeclaration
		tasks func(t *testing.T) []protoreflect.MessageDescriptor
		want  string
	}{
		"an undeclared credential name is refused": {
			decls: []*v1.CredentialDeclaration{declaration("bot_token")},
			tasks: func(t *testing.T) []protoreflect.MessageDescriptor {
				return []protoreflect.MessageDescriptor{uses(t, "U1", "bot_token"), uses(t, "U2", "other")}
			},
			want: `claims credential "other", which the plugin does not declare`,
		},
		"a claim with no declarations at all is refused": {
			tasks: func(t *testing.T) []protoreflect.MessageDescriptor {
				return []protoreflect.MessageDescriptor{uses(t, "N1", "bot_token")}
			},
			want: "does not declare",
		},
		"a declared but unused credential is refused": {
			decls: []*v1.CredentialDeclaration{declaration("bot_token"), declaration("spare")},
			tasks: func(t *testing.T) []protoreflect.MessageDescriptor {
				return []protoreflect.MessageDescriptor{uses(t, "D1", "bot_token")}
			},
			want: `credential "spare" is declared but no task input claims it`,
		},
		"a declaration on a plugin with no tasks is refused": {
			decls: []*v1.CredentialDeclaration{declaration("bot_token")},
			tasks: func(*testing.T) []protoreflect.MessageDescriptor { return nil },
			want:  "declared but no task input claims it",
		},
		"a duplicate declaration is refused": {
			decls: []*v1.CredentialDeclaration{declaration("bot_token"), declaration("bot_token")},
			tasks: func(t *testing.T) []protoreflect.MessageDescriptor {
				return []protoreflect.MessageDescriptor{uses(t, "Dup", "bot_token")}
			},
			want: "declared twice",
		},
		"a malformed declared name is refused": {
			decls: []*v1.CredentialDeclaration{declaration("Bot-Token")},
			tasks: func(*testing.T) []protoreflect.MessageDescriptor { return nil },
			want:  "not a name matching",
		},
		"an empty declared name is refused": {
			decls: []*v1.CredentialDeclaration{{}},
			tasks: func(*testing.T) []protoreflect.MessageDescriptor { return nil },
			want:  "not a name matching",
		},
		"a nil declaration is refused": {
			decls: []*v1.CredentialDeclaration{nil},
			tasks: func(*testing.T) []protoreflect.MessageDescriptor { return nil },
			want:  "not a name matching",
		},
		"too many declarations are refused": {
			decls: many(v1.MaxPluginCredentials + 1),
			tasks: func(*testing.T) []protoreflect.MessageDescriptor { return nil },
			want:  "more than the 8",
		},
		"an over-long description is refused": {
			decls: []*v1.CredentialDeclaration{{Name: "bot_token", Description: strings.Repeat("d", 257)}},
			tasks: func(*testing.T) []protoreflect.MessageDescriptor { return nil },
			want:  "description",
		},
		"a malformed claim in any task is refused": {
			decls: []*v1.CredentialDeclaration{declaration("bot_token")},
			tasks: func(t *testing.T) []protoreflect.MessageDescriptor {
				return []protoreflect.MessageDescriptor{
					uses(t, "G1", "bot_token"),
					credMessage(t, "G2", credToken(&v1.InputOptions{Credential: "bot_token", Secret: v1.Secret_SECRET_NESTED})),
				}
			},
			want: "always SECRET_REQUIRED",
		},
	} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()

			err := v1.CheckPluginCredentials(tc.decls, tc.tasks(t))
			require.Error(t, err)
			assert.Contains(t, err.Error(), tc.want)
		})
	}
}

func TestCredentialInputsReachTheCatalogAndItsClaimsProjection(t *testing.T) {
	t.Parallel()

	def := v1.TaskDef{
		Name:   "p.t",
		Inputs: credMessage(t, "Described", credToken(&v1.InputOptions{Credential: "bot_token"}), credField{name: "channel", typ: credStr}),
	}
	described := v1.DescribeTask(def)
	assert.Equal(t, map[string]string{"token": "bot_token"}, described.GetCredentialInputs())

	// The claims-only projection keeps it, so ClaimsDigest moves with it, and the
	// schema-only projection drops it, so TaskSchemaDigest does not.
	assert.Equal(t, described.GetCredentialInputs(), v1.TaskDescriptionClaimsOnly(described).GetCredentialInputs())
	assert.Empty(t, v1.TaskDescriptionSansClaims(described).GetCredentialInputs())

	plain := v1.DescribeTask(v1.TaskDef{Name: "p.t", Inputs: credMessage(t, "Plain2", credField{name: "channel", typ: credStr})})
	assert.Empty(t, plain.GetCredentialInputs())
	assert.False(t, proto.Equal(v1.TaskDescriptionClaimsOnly(described), v1.TaskDescriptionClaimsOnly(plain)))
}

func TestCurrentClaimsSchemaVersionCoversTheCredentialClaim(t *testing.T) {
	t.Parallel()

	// Version 3 readers ignore the credential option as an unknown one; 4 is the
	// first that enforces it, so a catalog from a version 3 build is refused.
	assert.GreaterOrEqual(t, v1.CurrentClaimsSchemaVersion, uint32(4))
}
