package conformance

import (
	"context"
	"fmt"
	"maps"
	"sync/atomic"

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

// The second fixture plugin declares its one credential federated, so it is
// bound by a `${credential()}` reference and by nothing else, where [BoundCredentialPlugin]'s
// is bound by a `${secret()}` and by nothing else.
const (
	// FederatedCredentialPlugin is the federated fixture plugin's name.
	FederatedCredentialPlugin = "federated"

	// FederatedCredentialTaskName is its task, whose `token` input claims
	// [FederatedCredentialName].
	FederatedCredentialTaskName = FederatedCredentialPlugin + ".use"

	// FederatedCredentialName is the credential the plugin declares federated.
	FederatedCredentialName = "partner_token"

	// FederatedCredentialTarget is the federation target the cases bind it to.
	FederatedCredentialTarget = "partner"
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
		Inputs:               credentialFixtureDescriptor("boundcredential", BoundCredentialName),
		SecretInputs:         []string{"token"},
		RequiredSecretInputs: []string{"token"},
		Fn:                   credentialFixtureFn(BoundCredentialTaskName),
	}
}

// FederatedCredentialTaskDef is [BoundCredentialTaskDef]'s counterpart for the
// plugin that declares its credential federated: the same task shape, claiming
// [FederatedCredentialName], with the declaration carried the way a loaded
// plugin's is ([v1.TaskDef.FederatedCredentials]).
func FederatedCredentialTaskDef() v1.TaskDef {
	return v1.TaskDef{
		Name:                 FederatedCredentialTaskName,
		Summary:              "test fixture standing in for a plugin task with a federated credential input",
		Inputs:               credentialFixtureDescriptor("federatedcredential", FederatedCredentialName),
		SecretInputs:         []string{"token"},
		RequiredSecretInputs: []string{"token"},
		FederatedCredentials: []string{FederatedCredentialName},
		Fn:                   credentialFixtureFn(FederatedCredentialTaskName),
	}
}

// credentialFixtureFn is what both fixture tasks do: report the kind and name of
// the reference the input arrived as and the length of what it resolves to, never
// the value, through the same resolvers a plugin host calls.
func credentialFixtureFn(task string) v1.TaskFunc {
	return func(ctx context.Context, inputs map[string]*v1.Value, _ *v1.Scope) (*v1.Node_Outputs, error) {
		out := map[string]*v1.Value{"token_kind": v1.NewLiteral(valueKindName(inputs["token"]))}

		switch {
		case inputs["token"].GetSecretRef() != nil:
			ref := inputs["token"].GetSecretRef()
			out["token_ref"] = v1.NewLiteral(fmt.Sprintf("%s:%s", ref.GetScheme(), ref.GetName()))

			secret, err := v1.ResolveSecret(ctx, ref)
			if err != nil {
				return nil, v1.NewTaskError(task, v1.ErrorKindPolicyDenied,
					fmt.Errorf("resolving input %q (%s:%s): %w", "token", ref.GetScheme(), ref.GetName(), err))
			}
			out["token_length"] = v1.NewLiteral(int64(len(secret.Reveal())))
		case inputs["token"].GetCredentialRef() != nil:
			target := inputs["token"].GetCredentialRef().GetTarget()
			out["token_ref"] = v1.NewLiteral("credential:" + target)

			secret, err := v1.ResolveCredential(ctx, target)
			if err != nil {
				return nil, v1.NewTaskError(task, v1.ErrorKindPolicyDenied,
					fmt.Errorf("resolving input %q (credential %q): %w", "token", target, err))
			}
			out["token_length"] = v1.NewLiteral(int64(len(secret.Reveal())))
		}

		return &v1.Node_Outputs{NamedValues: out}, nil
	}
}

// credentialFixtureDescriptor builds a fixture's input schema, with the
// credential claim set the way a generated plugin message sets it.
func credentialFixtureDescriptor(name, credential string) protoreflect.MessageDescriptor {
	claimed := &descriptorpb.FieldOptions{}
	proto.SetExtension(claimed, v1.E_Input, &v1.InputOptions{Credential: credential})

	file, err := protodesc.NewFile(&descriptorpb.FileDescriptorProto{
		Name:       proto.String("flowstate/tests/" + name + "/v1/" + name + ".proto"),
		Package:    proto.String("flowstate.tests." + name + ".v1"),
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

// FederatedCredentialRequirement is the `plugins:` entry the federated cases
// bind the credential in.
func FederatedCredentialRequirement(credentials map[string]*v1.Value) *v1.PluginRequirement {
	return &v1.PluginRequirement{Name: FederatedCredentialPlugin, MinimumVersion: "v0.1.0", Credentials: credentials}
}

func fixtureSecretRef(name string) *v1.Value {
	return &v1.Value{Kind: &v1.Value_SecretRef{SecretRef: &v1.SecretRef{Scheme: PluginTaskInputsScheme, Name: name}}}
}

// boundCredentialStep is a step of the fixture task. It always writes `note`,
// because a task with no input at all is refused by the schema before binding is
// reached, and these steps are about the one input they leave out.
func boundCredentialStep(id string, inputs map[string]*v1.Value) *v1.Node {
	return fixtureTaskStep(BoundCredentialTaskName, id, inputs)
}

func federatedCredentialStep(id string, inputs map[string]*v1.Value) *v1.Node {
	return fixtureTaskStep(FederatedCredentialTaskName, id, inputs)
}

func fixtureTaskStep(task, id string, inputs map[string]*v1.Value) *v1.Node {
	inputs = maps.Clone(inputs)
	if inputs == nil {
		inputs = make(map[string]*v1.Value)
	}
	inputs["note"] = v1.NewLiteral("hello")

	return &v1.Node{Id: id, Kind: &v1.Node_Task{Task: &v1.Task{Name: task, Inputs: inputs}}}
}

func federatedCredentialOutputs(target string) *v1.Node_Outputs {
	return &v1.Node_Outputs{NamedValues: map[string]*v1.Value{
		"token_kind":   v1.NewLiteral("credential_ref"),
		"token_ref":    v1.NewLiteral("credential:" + target),
		"token_length": v1.NewLiteral(int64(len(BoundCredentialMaterial))),
	}}
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
	// The allow rule reads the `task` attribute, so the positive case below also
	// proves the task reaches the secret policy under both drivers: a driver that
	// left it empty would refuse every step.
	authority := Authority{
		Scheme: PluginTaskInputsScheme, FixtureValue: BoundCredentialMaterial,
		Allow: []string{`task == "` + BoundCredentialTaskName + `" && secret.scheme == "` + PluginTaskInputsScheme + `"`}, Identity: identity,
	}
	binding := map[string]*v1.Value{BoundCredentialName: fixtureSecretRef(boundSecretName)}
	federatedBinding := map[string]*v1.Value{FederatedCredentialName: v1.NewCredentialRef(FederatedCredentialTarget)}
	federation := &Federation{
		Target: FederatedCredentialTarget, Token: BoundCredentialMaterial,
		Allow: []string{`task == "` + FederatedCredentialTaskName + `"`},
	}

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
		{
			// The federated half: the plugin declares its credential federated, so
			// the binding is a credential reference, expanded into the step and
			// minted inside the task by the broker, whose assumption rule reads the
			// same `task` attribute. The output holds a target name and a length.
			Case: Case{
				Name: "a federated credential bound once is minted for the step that omits it",
				Workflow: &v1.Workflow{
					Name:               "credential-binding-federated",
					Profile:            v1.CurrentProfile,
					PluginRequirements: []*v1.PluginRequirement{FederatedCredentialRequirement(federatedBinding)},
					Steps:              []*v1.Node{federatedCredentialStep("minted", nil)},
				},
				ExpectedOutputs: &v1.Workflow_StepOutputs{StepValues: map[string]*v1.Node_Outputs{
					"minted": federatedCredentialOutputs(FederatedCredentialTarget),
				}},
			},
			Authority:        Authority{Identity: identity, Federation: federation},
			ContainmentValue: BoundCredentialMaterial,
		},
		{
			// A deny rule on the `task` attribute refuses the step it names, at
			// the secret policy, before the provider is asked.
			Case: Case{
				Name: "a secret deny rule on the task attribute refuses the bound step",
				Workflow: &v1.Workflow{
					Name:               "credential-binding-task-denied",
					Profile:            v1.CurrentProfile,
					PluginRequirements: []*v1.PluginRequirement{BoundCredentialRequirement(binding)},
					Steps:              []*v1.Node{credentialContinue(boundCredentialStep("denied", nil))},
				},
				ExpectedOutputs: &v1.Workflow_StepOutputs{StepValues: map[string]*v1.Node_Outputs{
					"denied": v1.FailedStepOutputs(v1.StepFailure{Kind: v1.ErrorKindPolicyDenied, Text: `task "bound.use" failed (PolicyDenied): ` +
						`resolving input "token" (fixture-secret:BOUND_TOKEN): ` +
						`auth: denied by secret access policy: no rule permits workload ` +
						`"flowstate:acme-tenant/_default/credential-binding-task-denied/denied" ` +
						`in namespace "acme-tenant" to read fixture-secret:BOUND_TOKEN (deny rule: task == "bound.use")`}),
				}},
			},
			Authority: Authority{
				Scheme: PluginTaskInputsScheme, FixtureValue: BoundCredentialMaterial,
				Allow: []string{"true"}, Deny: []string{`task == "` + BoundCredentialTaskName + `"`}, Identity: identity,
				ProviderCalls: new(atomic.Int32),
			},
		},
		{
			// The same on the assumption policy: the federated step is refused
			// by a rule on `task`, and nothing is minted.
			Case: Case{
				Name: "an assumption deny rule on the task attribute refuses the federated step",
				Workflow: &v1.Workflow{
					Name:               "credential-binding-federated-denied",
					Profile:            v1.CurrentProfile,
					PluginRequirements: []*v1.PluginRequirement{FederatedCredentialRequirement(federatedBinding)},
					Steps:              []*v1.Node{credentialContinue(federatedCredentialStep("denied", nil))},
				},
				ExpectedOutputs: &v1.Workflow_StepOutputs{StepValues: map[string]*v1.Node_Outputs{
					"denied": v1.FailedStepOutputs(v1.StepFailure{Kind: v1.ErrorKindPolicyDenied, Text: `task "federated.use" failed (PolicyDenied): ` +
						`resolving input "token" (credential "partner"): auth: denied by assumption policy: ` +
						`"flowstate:acme-tenant/_default/credential-binding-federated-denied/denied" may not assume "partner" ` +
						`(deny rule: task == "federated.use")`}),
				}},
			},
			Authority: Authority{
				Identity: identity,
				Federation: &Federation{
					Target: FederatedCredentialTarget, Token: BoundCredentialMaterial,
					Deny: []string{`task == "` + FederatedCredentialTaskName + `"`}, ExchangeCalls: new(atomic.Int32),
				},
			},
		},
	}
}

// credentialContinue lets a refused step be recorded rather than fail the run,
// the shape the denial cases use so the refusal is observed in the outputs.
func credentialContinue(node *v1.Node) *v1.Node {
	node.Policy = &v1.StepPolicy{ContinueOnError: true}

	return node
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
			Contains: `plugin "bound" credential "api_token" must be bound to a whole reference`,
			Omits:    "plain-text",
		},
		{
			Name: "a binding that is an expression is refused",
			Workflow: workflow("binding-expression", bind(map[string]*v1.Value{BoundCredentialName: v1.NewExpr(`inputs.token`)}),
				boundCredentialStep("send", nil)),
			Contains: `plugin "bound" credential "api_token" must be bound to a whole reference`,
		},
		{
			// The declaration decides: api_token is not federated, so a credential
			// reference neither binds it nor stands in for the secret it takes.
			Name: "a credential reference bound to a credential that is not federated is refused",
			Workflow: workflow("binding-credential-ref", bind(map[string]*v1.Value{BoundCredentialName: v1.NewCredentialRef("partner")}),
				boundCredentialStep("send", nil)),
			Contains: `plugin "bound" credential "api_token": is not declared federated, so bind it to a whole secret reference`,
			Omits:    "partner",
		},
		{
			Name: "a stored secret bound to a federated credential is refused",
			Workflow: workflow("binding-secret-federated", []*v1.PluginRequirement{
				FederatedCredentialRequirement(map[string]*v1.Value{FederatedCredentialName: fixtureSecretRef(boundSecretName)}),
			}, federatedCredentialStep("send", nil)),
			Contains: `plugin "federated" credential "partner_token": is declared federated, so bind it to a whole credential reference`,
			Omits:    boundSecretName,
		},
		{
			// A step's own reference is held to the declaration exactly as a
			// binding is, with or without a binding, so a hand-built specification
			// cannot reach a worker with the wrong kind in either place.
			Name: "a step that writes a credential reference into a credential that is not federated is refused",
			Workflow: workflow("step-credential-ref", bind(nil),
				boundCredentialStep("send", map[string]*v1.Value{"token": v1.NewCredentialRef("partner")})),
			Contains: `step "send": task "bound.use" input "token" receives the plugin's credential "api_token", which is not federated`,
			Omits:    "partner",
		},
		{
			Name: "a step that writes a stored secret into a federated credential is refused",
			Workflow: workflow("step-secret-federated", []*v1.PluginRequirement{FederatedCredentialRequirement(nil)},
				federatedCredentialStep("send", map[string]*v1.Value{"token": fixtureSecretRef(boundSecretName)})),
			Contains: `step "send": task "federated.use" input "token" receives the plugin's federated credential "partner_token"`,
			Omits:    boundSecretName,
		},
		{
			Name: "a federated credential input written nowhere is refused naming the credential reference",
			Workflow: workflow("federated-omitted", []*v1.PluginRequirement{FederatedCredentialRequirement(nil)},
				federatedCredentialStep("send", nil)),
			Contains: `input "token" receives the plugin's credential "partner_token", so write it as a whole credential reference`,
		},
		{
			// An override keeps the binding's kind too: a binding does not make a
			// wrong-kind step reference acceptable.
			Name: "a step that overrides a federated binding with a stored secret is refused",
			Workflow: workflow("federated-override-secret",
				[]*v1.PluginRequirement{FederatedCredentialRequirement(map[string]*v1.Value{FederatedCredentialName: v1.NewCredentialRef(FederatedCredentialTarget)})},
				federatedCredentialStep("send", map[string]*v1.Value{"token": fixtureSecretRef(boundSecretName)})),
			Contains: `step "send": task "federated.use" input "token" receives the plugin's federated credential "partner_token"`,
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
			Contains: `step "send": task "bound.use" input "token" receives the plugin's credential "api_token", which is not federated and takes a whole secret reference`,
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
