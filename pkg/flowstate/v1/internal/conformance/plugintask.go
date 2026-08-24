package conformance

import (
	"context"
	"fmt"

	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protodesc"
	"google.golang.org/protobuf/reflect/protoreflect"
	"google.golang.org/protobuf/types/descriptorpb"

	v1 "github.com/picatz/flowstate/pkg/flowstate/v1"
	"github.com/picatz/flowstate/pkg/flowstate/v1/auth"
)

// What a plugin task is handed, asked of both drivers at once.
//
// `flow run local` could not execute a plugin task at all until #436, so every
// claim about how a plugin task's inputs arrive was a claim about the durable
// driver alone: the local driver had no plugin to hand anything to, and the
// question could not be asked of it. Now that it can, the question has to be
// asked of both, because the answer is three separate decisions the two drivers
// make independently and could each get wrong on its own:
//
//   - an input written as an expression is evaluated *before* the task is
//     dispatched, so the task receives a value rather than a tree;
//   - an input the task declares as deferred is passed through untouched, so
//     the task evaluates it in a scope the workflow does not have;
//   - an input holding a secret reference stays a reference right up to the
//     task boundary, and is resolved inside the task, which is the whole of
//     what keeps the material out of durable history.
//
// The third is the one worth stating twice. A driver that resolved a reference
// a step earlier would still produce a working run and a correct answer, and
// the only observable difference would be a secret written into workflow
// history, which is exactly the leak class CLAUDE.md's "Secrets never enter
// workflow history" section is about, and not a thing an outputs comparison
// would notice. So the fixture reports what *kind* of value reached it, and the
// case pins that.

// PluginTaskInputsTaskName is the name [PluginTaskInputsTaskDef] registers
// under. Dotted like every plugin task's, because the dot is what keeps the
// two namespaces apart and a fixture spelled as a built-in would be dispatched
// down a path no plugin task takes.
const PluginTaskInputsTaskName = "test.plugin_inputs"

// PluginTaskInputsSecretName is the name the case's reference asks for, and
// PluginTaskInputsScheme the scheme it asks it of.
const (
	PluginTaskInputsScheme     = "fixture-secret"
	PluginTaskInputsSecretName = "PLUGIN_TOKEN"
)

// PluginTaskInputsMaterial is what the fixture provider resolves that
// reference to, and what [AuthorityCase.ContainmentValue] then proves never
// reached the run's outputs.
const PluginTaskInputsMaterial = "plugin-task-secret-material"

// PluginTaskInputsTaskDef is a [v1.TaskDef] shaped exactly like the one
// `plugin.Plugin.taskDef` builds for a real plugin task, and whose Fn reports
// what each of its inputs looked like on arrival.
//
// It stands in for a plugin process rather than launching one, for the reason
// [PluginIdentityTaskDef] gives beside it: what is under test is the driver's
// dispatch, which decides what a task is handed, and that decision is made
// before the plugin protocol's wire is reached. `pkg/flowstate/v1/plugin`'s own
// tests carry the same values one process boundary further out, and
// cmd/flow's TestRunLocalExecutesAPluginTaskFromAnExample runs the real binary
// end to end.
//
// The three inputs are the three decisions:
//
//   - `resolved` is an ordinary expression input, so the engine evaluates it
//     and the task receives a literal.
//   - `deferred` is named in DeferredInputs, so it arrives as the parsed tree.
//     Its expression deliberately references a step that does not exist: a
//     driver that evaluated it anyway does not merely disagree, it fails.
//   - `token` holds a [v1.SecretRef], which the task resolves itself through
//     [v1.ResolveSecret], the same call `plugin/task.go`'s
//     resolvePluginSecretInputs makes, at the same position.
func PluginTaskInputsTaskDef() v1.TaskDef {
	return v1.TaskDef{
		Name:           PluginTaskInputsTaskName,
		Summary:        "test fixture standing in for a plugin task",
		Inputs:         pluginTaskInputsDescriptor(),
		DeferredInputs: []string{"deferred"},
		Fn: func(ctx context.Context, inputs map[string]*v1.Value, _ *v1.Scope) (*v1.Node_Outputs, error) {
			out := map[string]*v1.Value{
				"resolved_kind": v1.NewLiteral(valueKindName(inputs["resolved"])),
				"resolved_text": v1.NewLiteral(inputs["resolved"].GetLiteral().GetStringValue()),
				"deferred_kind": v1.NewLiteral(valueKindName(inputs["deferred"])),
				"token_kind":    v1.NewLiteral(valueKindName(inputs["token"])),
			}

			ref := inputs["token"].GetSecretRef()
			if ref == nil {
				// Not an error the case expects: reaching here means the value
				// was resolved before the task, which token_kind above already
				// records. Reported as an output rather than a failure so the
				// comparison names the disagreement instead of a step error
				// burying it.
				out["token_ref"] = v1.NewLiteral("")
				out["token_length"] = v1.NewLiteral(int64(0))

				return &v1.Node_Outputs{NamedValues: out}, nil
			}

			out["token_ref"] = v1.NewLiteral(fmt.Sprintf("%s:%s", ref.GetScheme(), ref.GetName()))

			secret, err := v1.ResolveSecret(ctx, ref)
			if err != nil {
				return nil, v1.NewTaskError(PluginTaskInputsTaskName, v1.ErrorKindPolicyDenied,
					fmt.Errorf("resolving input %q (%s:%s): %w", "token", ref.GetScheme(), ref.GetName(), err))
			}

			// The length, never the value. A fixture that echoed the material
			// back would make [AssertNoLeak] fail on this case for a reason
			// that has nothing to do with either driver.
			out["token_length"] = v1.NewLiteral(int64(len(secret.Reveal())))

			return &v1.Node_Outputs{NamedValues: out}, nil
		},
	}
}

// pluginTaskInputsDescriptor builds the fixture's input schema at run time,
// out of a file descriptor this build has never compiled.
//
// That is not decoration. It is how a plugin task's schema comes to exist at
// all: the descriptors arrive over the socket in the plugin's manifest and are
// linked into a [protoreflect.MessageDescriptor] by plugin/descriptor.go, so a
// fixture whose schema was a generated Go type would be shaped like a built-in
// rather than like the thing it stands in for. It also keeps the fixture honest
// against TestEveryTaskDescribesItself, which audits every task the default
// registry holds for the schema an author's editor reads.
func pluginTaskInputsDescriptor() protoreflect.MessageDescriptor {
	file, err := protodesc.NewFile(&descriptorpb.FileDescriptorProto{
		Name:    proto.String("flowstate/tests/pluginfixture/v1/pluginfixture.proto"),
		Package: proto.String("flowstate.tests.pluginfixture.v1"),
		Syntax:  proto.String("proto3"),
		MessageType: []*descriptorpb.DescriptorProto{{
			Name: proto.String("PluginInputs"),
			Field: []*descriptorpb.FieldDescriptorProto{
				stringField("resolved", 1),
				stringField("deferred", 2),
				stringField("token", 3),
			},
		}},
	}, nil)
	if err != nil {
		// Unreachable for a descriptor written out above as a literal: there is
		// no input to this, so a failure here is this function being wrong
		// rather than anything a caller did.
		panic(fmt.Sprintf("building the plugin fixture's descriptors: %v", err))
	}

	return file.Messages().Get(0)
}

func stringField(name string, number int32) *descriptorpb.FieldDescriptorProto {
	return &descriptorpb.FieldDescriptorProto{
		Name:     proto.String(name),
		Number:   proto.Int32(number),
		Label:    descriptorpb.FieldDescriptorProto_LABEL_OPTIONAL.Enum(),
		Type:     descriptorpb.FieldDescriptorProto_TYPE_STRING.Enum(),
		JsonName: proto.String(name),
	}
}

// valueKindName names the oneof arm a value arrived in, which is the whole
// observation these cases make.
func valueKindName(value *v1.Value) string {
	switch value.GetKind().(type) {
	case *v1.Value_SecretRef:
		return "secret_ref"
	case *v1.Value_Expr:
		return "expression"
	case *v1.Value_Literal:
		return "literal"
	case *v1.Value_Structure_:
		return "structure"
	case *v1.Value_Error_:
		return "error"
	case nil:
		return "absent"
	default:
		return "unknown"
	}
}

// PluginTaskInputStep builds the one-step workflow the cases run, the way a
// Flowfile compiles a plugin task step.
func PluginTaskInputStep(workflowName, stepID string) *v1.Workflow {
	return &v1.Workflow{
		Name: workflowName,
		Steps: []*v1.Node{{
			Id: stepID,
			Kind: &v1.Node_Task{Task: &v1.Task{
				Name: PluginTaskInputsTaskName,
				Inputs: map[string]*v1.Value{
					"resolved": v1.NewExpr(`"hello" + " " + "world"`),
					"deferred": v1.NewExpr("steps.nowhere.value"),
					"token": {Kind: &v1.Value_SecretRef{SecretRef: &v1.SecretRef{
						Scheme: PluginTaskInputsScheme, Name: PluginTaskInputsSecretName,
					}}},
				},
			}},
		}},
	}
}

// PluginTaskInputCases are the shared cases both drivers run for what a plugin
// task is handed.
//
// Registering [PluginTaskInputsTaskDef] is each driver's own business, because
// the two reach a registry differently: the local driver can be handed one on
// the context, the durable driver's activities run in a context Temporal built
// and can only see [v1.DefaultRegistry]. That is the same split
// [PluginIdentityTaskDef]'s two callers already live with.
func PluginTaskInputCases() []AuthorityCase {
	return []AuthorityCase{
		{
			Case: Case{
				Name:     "a plugin task is handed evaluated, deferred and referenced inputs",
				Workflow: PluginTaskInputStep("plugin-task-inputs", "call"),
				ExpectedOutputs: &v1.Workflow_StepOutputs{StepValues: map[string]*v1.Node_Outputs{
					"call": {NamedValues: map[string]*v1.Value{
						// The engine evaluated it, so the task got a value.
						"resolved_kind": v1.NewLiteral("literal"),
						"resolved_text": v1.NewLiteral("hello world"),

						// The engine left it alone, so the task can evaluate it
						// in a scope the workflow does not have.
						"deferred_kind": v1.NewLiteral("expression"),

						// The reference travelled as a reference, and was
						// resolved inside the task.
						"token_kind":   v1.NewLiteral("secret_ref"),
						"token_ref":    v1.NewLiteral(PluginTaskInputsScheme + ":" + PluginTaskInputsSecretName),
						"token_length": v1.NewLiteral(int64(len(PluginTaskInputsMaterial))),
					}},
				}},
			},
			Authority: Authority{
				Scheme:       PluginTaskInputsScheme,
				FixtureValue: PluginTaskInputsMaterial,
				Allow:        []string{"true"},
				Identity: auth.WorkloadIdentity{
					Subject: "svc-reader", Issuer: "https://issuer.example", Namespace: "acme-tenant",
				},
			},
			ContainmentValue: PluginTaskInputsMaterial,
		},
	}
}
