package conformance

import (
	"context"
	"fmt"
	"maps"

	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protodesc"
	"google.golang.org/protobuf/reflect/protoreflect"
	"google.golang.org/protobuf/reflect/protoregistry"
	"google.golang.org/protobuf/types/descriptorpb"

	v1 "github.com/picatz/flowstate/pkg/flowstate/v1"
	"github.com/picatz/flowstate/pkg/flowstate/v1/auth"
)

// What `plugins:` credential bindings mean, asked of both drivers at once.
//
// A binding is expanded into per-step secret references before either driver
// sees the specification ([v1.BindPluginCredentials], run by the Flowfile
// compiler and by the server's admission), so the drivers agree by construction
// as long as each is handed the expanded specification. These cases are what
// would fail if one were not: a step that omits the credential input must reach
// the task as the binding's reference, a step that writes it must reach the task
// as its own, and in both the value must stay a reference up to the task and
// stay out of the run's outputs.

// The fixture plugin the cases run against, shaped like a plugin that declares
// one credential and a task that claims it.
const (
	// BoundCredentialPlugin is the plugin's name, the segment before the dot in
	// [BoundCredentialTaskName].
	BoundCredentialPlugin = "bound"

	// BoundCredentialTaskName is the fixture task, dotted like every plugin
	// task's.
	BoundCredentialTaskName = BoundCredentialPlugin + ".use"

	// BoundCredentialName is the credential the plugin declares and the task's
	// `token` input claims.
	BoundCredentialName = "api_token"

	// BoundCredentialMaterial is what the fixture provider resolves any
	// reference to, and what the cases prove never reaches the run's outputs.
	BoundCredentialMaterial = "bound-credential-secret-material"

	boundSecretName    = "BOUND_TOKEN"
	overrideSecretName = "OVERRIDE_TOKEN"
)

// BoundCredentialTaskDef is a [v1.TaskDef] shaped like the one a plugin task
// with a credential input is loaded as: `token` claims [BoundCredentialName], so
// it is a required secret input, and `note` is an ordinary input. Its Fn reports
// the kind and name of the reference it was handed and the length of what the
// reference resolves to, never the value, for the reason
// [PluginTaskInputsTaskDef] gives.
func BoundCredentialTaskDef() v1.TaskDef {
	return v1.TaskDef{
		Name:                 BoundCredentialTaskName,
		Summary:              "test fixture standing in for a plugin task with a credential input",
		Inputs:               boundCredentialDescriptor(),
		SecretInputs:         []string{"token"},
		RequiredSecretInputs: []string{"token"},
		Fn: func(ctx context.Context, inputs map[string]*v1.Value, _ *v1.Scope) (*v1.Node_Outputs, error) {
			out := map[string]*v1.Value{"token_kind": v1.NewLiteral(valueKindName(inputs["token"]))}

			ref := inputs["token"].GetSecretRef()
			if ref == nil {
				return &v1.Node_Outputs{NamedValues: out}, nil
			}
			out["token_ref"] = v1.NewLiteral(fmt.Sprintf("%s:%s", ref.GetScheme(), ref.GetName()))

			secret, err := v1.ResolveSecret(ctx, ref)
			if err != nil {
				return nil, v1.NewTaskError(BoundCredentialTaskName, v1.ErrorKindPolicyDenied,
					fmt.Errorf("resolving input %q (%s:%s): %w", "token", ref.GetScheme(), ref.GetName(), err))
			}
			out["token_length"] = v1.NewLiteral(int64(len(secret.Reveal())))

			return &v1.Node_Outputs{NamedValues: out}, nil
		},
	}
}

// boundCredentialDescriptor builds the fixture's input schema, with the
// credential claim set the way a generated plugin message sets it.
func boundCredentialDescriptor() protoreflect.MessageDescriptor {
	claimed := &descriptorpb.FieldOptions{}
	proto.SetExtension(claimed, v1.E_Input, &v1.InputOptions{Credential: BoundCredentialName})

	file, err := protodesc.NewFile(&descriptorpb.FileDescriptorProto{
		Name:       proto.String("flowstate/tests/boundcredential/v1/boundcredential.proto"),
		Package:    proto.String("flowstate.tests.boundcredential.v1"),
		Syntax:     proto.String("proto3"),
		Dependency: []string{"flowstate/v1/schema.proto"},
		MessageType: []*descriptorpb.DescriptorProto{{
			Name: proto.String("UseInputs"),
			Field: []*descriptorpb.FieldDescriptorProto{
				{
					Name: proto.String("token"), Number: proto.Int32(1), JsonName: proto.String("token"),
					Label:   descriptorpb.FieldDescriptorProto_LABEL_OPTIONAL.Enum(),
					Type:    descriptorpb.FieldDescriptorProto_TYPE_STRING.Enum(),
					Options: claimed,
				},
				stringField("note", 2),
			},
		}},
	}, protoregistry.GlobalFiles)
	if err != nil {
		// A literal descriptor with no input to it: a failure is this function
		// being wrong, as in pluginTaskInputsDescriptor.
		panic(fmt.Sprintf("building the bound credential fixture's descriptors: %v", err))
	}

	return file.Messages().Get(0)
}

// BoundCredentialRequirement is the `plugins:` entry the cases bind the
// credential in.
func BoundCredentialRequirement(credentials map[string]*v1.Value) *v1.PluginRequirement {
	return &v1.PluginRequirement{Name: BoundCredentialPlugin, MinimumVersion: "v0.1.0", Credentials: credentials}
}

func fixtureSecretRef(name string) *v1.Value {
	return &v1.Value{Kind: &v1.Value_SecretRef{SecretRef: &v1.SecretRef{Scheme: PluginTaskInputsScheme, Name: name}}}
}

// boundCredentialStep is a step of the fixture task. It always writes `note`,
// because a task with no input at all is refused by the schema before binding is
// reached, and these steps are about the one input they leave out.
func boundCredentialStep(id string, inputs map[string]*v1.Value) *v1.Node {
	inputs = maps.Clone(inputs)
	if inputs == nil {
		inputs = make(map[string]*v1.Value)
	}
	inputs["note"] = v1.NewLiteral("hello")

	return &v1.Node{Id: id, Kind: &v1.Node_Task{Task: &v1.Task{Name: BoundCredentialTaskName, Inputs: inputs}}}
}

func boundCredentialOutputs(secret string) *v1.Node_Outputs {
	return &v1.Node_Outputs{NamedValues: map[string]*v1.Value{
		"token_kind":   v1.NewLiteral("secret_ref"),
		"token_ref":    v1.NewLiteral(PluginTaskInputsScheme + ":" + secret),
		"token_length": v1.NewLiteral(int64(len(BoundCredentialMaterial))),
	}}
}

// CredentialBindingCases are the shared cases both drivers run for a credential
// bound once under `plugins:`. The workflow is the specification as an author
// wrote it, bindings unexpanded: each driver's runner expands it the way its
// submit boundary does (the local driver inside Run, the durable runner as the
// server's admission would), and the expected outputs are the same for both.
//
// [BoundCredentialTaskDef] must be registered in [v1.DefaultRegistry] first.
func CredentialBindingCases() []AuthorityCase {
	identity := auth.WorkloadIdentity{Subject: "svc-reader", Issuer: "https://issuer.example", Namespace: "acme-tenant"}
	authority := Authority{
		Scheme: PluginTaskInputsScheme, FixtureValue: BoundCredentialMaterial,
		Allow: []string{"true"}, Identity: identity,
	}
	binding := map[string]*v1.Value{BoundCredentialName: fixtureSecretRef(boundSecretName)}

	return []AuthorityCase{
		{
			// The positive direction for both halves of the rule at once: the
			// omitting step gets the binding and the writing step keeps its own.
			// A driver that expanded every step, or none, disagrees with one of
			// the two expected references.
			Case: Case{
				Name: "a step that omits the credential input receives the binding and one that writes it keeps its own",
				Workflow: &v1.Workflow{
					Name:               "credential-binding",
					Profile:            v1.CurrentProfile,
					PluginRequirements: []*v1.PluginRequirement{BoundCredentialRequirement(binding)},
					Steps: []*v1.Node{
						boundCredentialStep("bound", nil),
						boundCredentialStep("overridden", map[string]*v1.Value{"token": fixtureSecretRef(overrideSecretName)}),
					},
				},
				ExpectedOutputs: &v1.Workflow_StepOutputs{StepValues: map[string]*v1.Node_Outputs{
					"bound":      boundCredentialOutputs(boundSecretName),
					"overridden": boundCredentialOutputs(overrideSecretName),
				}},
			},
			Authority:        authority,
			ContainmentValue: BoundCredentialMaterial,
		},
	}
}

// CredentialBindingRefusalCases are hand-built specifications that omit or
// mis-bind a credential, with no compiler in front of them. Both submit
// boundaries, the server's admission and the local driver's, must refuse each
// before any step runs; [BoundCredentialTaskDef] must be registered first, and
// the server's catalog must hold a plugin named [BoundCredentialPlugin].
func CredentialBindingRefusalCases() []Refusal {
	workflow := func(name string, requirements []*v1.PluginRequirement, steps ...*v1.Node) *v1.Workflow {
		return &v1.Workflow{Name: name, Profile: v1.CurrentProfile, PluginRequirements: requirements, Steps: steps}
	}
	bind := func(credentials map[string]*v1.Value) []*v1.PluginRequirement {
		return []*v1.PluginRequirement{BoundCredentialRequirement(credentials)}
	}
	unbound := `input "token" receives the plugin's credential "api_token"`

	tooMany := make(map[string]*v1.Value)
	for i := range v1.MaxPluginCredentials + 1 {
		tooMany[fmt.Sprintf("credential_%d", i)] = fixtureSecretRef(boundSecretName)
	}

	callee := &v1.Workflow{
		Name:    "bound-callee",
		Profile: v1.CurrentProfile,
		Steps:   []*v1.Node{boundCredentialStep("inside", nil)},
	}

	return []Refusal{
		{
			Name:     "a credential input written nowhere is refused",
			Workflow: workflow("binding-omitted", bind(nil), boundCredentialStep("send", nil)),
			Contains: `step "send": task "bound.use" ` + unbound,
		},
		{
			Name:     "a credential input with no plugins entry at all is refused",
			Workflow: workflow("binding-no-entry", nil, boundCredentialStep("send", nil)),
			Contains: unbound,
		},
		{
			Name:     "a binding for a credential the plugin does not declare is refused",
			Workflow: workflow("binding-unknown", bind(map[string]*v1.Value{"nope": fixtureSecretRef(boundSecretName)}), boundCredentialStep("send", nil)),
			Contains: `plugin "bound" has no credential "nope" to bind; it declares api_token`,
		},
		{
			Name: "a binding that is a literal is refused",
			Workflow: workflow("binding-literal", bind(map[string]*v1.Value{BoundCredentialName: v1.NewLiteral("plain-text")}),
				boundCredentialStep("send", nil)),
			Contains: `plugin "bound" credential "api_token" must be bound to a whole secret reference`,
			Omits:    "plain-text",
		},
		{
			Name: "a binding that is an expression is refused",
			Workflow: workflow("binding-expression", bind(map[string]*v1.Value{BoundCredentialName: v1.NewExpr(`inputs.token`)}),
				boundCredentialStep("send", nil)),
			Contains: `plugin "bound" credential "api_token" must be bound to a whole secret reference`,
		},
		{
			Name: "a binding that is a credential reference is refused",
			Workflow: workflow("binding-credential-ref", bind(map[string]*v1.Value{BoundCredentialName: v1.NewCredentialRef("partner")}),
				boundCredentialStep("send", nil)),
			Contains: `plugin "bound" credential "api_token" must be bound to a whole secret reference`,
		},
		{
			Name:     "a binding named like no credential is refused",
			Workflow: workflow("binding-bad-name", bind(map[string]*v1.Value{"Not-A-Name": fixtureSecretRef(boundSecretName)}), boundCredentialStep("send", nil)),
			Contains: `credential "Not-A-Name", which is not a name matching`,
		},
		{
			Name:     "more bindings than a plugin may declare are refused",
			Workflow: workflow("binding-too-many", bind(tooMany), boundCredentialStep("send", nil)),
			Contains: fmt.Sprintf("binds %d credentials, more than the %d a plugin may declare", v1.MaxPluginCredentials+1, v1.MaxPluginCredentials),
		},
		{
			Name: "a plugin required twice that binds is refused",
			Workflow: workflow("binding-twice", []*v1.PluginRequirement{
				BoundCredentialRequirement(map[string]*v1.Value{BoundCredentialName: fixtureSecretRef(boundSecretName)}),
				BoundCredentialRequirement(nil),
			}, boundCredentialStep("send", nil)),
			Contains: `plugin "bound" is required twice and binds credentials`,
		},
		{
			// An override is held to the claim it overrides: it is a reference
			// or it is refused, bound or not.
			Name: "a step that overrides the binding with a literal is refused",
			Workflow: workflow("binding-literal-override", bind(map[string]*v1.Value{BoundCredentialName: fixtureSecretRef(boundSecretName)}),
				boundCredentialStep("send", map[string]*v1.Value{"token": v1.NewLiteral("plain-text")})),
			Contains: `step "send": task "bound.use" requires input "token" to be a whole secret reference`,
			Omits:    "plain-text",
		},
		{
			// A caller's binding does not cross a call: the callee binds its own,
			// so a callee that binds none is refused though its caller does.
			Name: "a caller's binding does not reach a callee's step",
			Workflow: workflow("binding-across-call", bind(map[string]*v1.Value{BoundCredentialName: fixtureSecretRef(boundSecretName)}),
				&v1.Node{Id: "call", Kind: &v1.Node_Call{Call: &v1.Call{Workflow: callee, Source: "callee.yaml"}}}),
			Contains: `step "inside": task "bound.use" ` + unbound,
		},
	}
}
