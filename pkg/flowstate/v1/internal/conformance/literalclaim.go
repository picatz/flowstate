package conformance

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protodesc"
	"google.golang.org/protobuf/reflect/protoregistry"
	"google.golang.org/protobuf/types/descriptorpb"

	v1 "github.com/picatz/flowstate/pkg/flowstate/v1"
)

// LiteralClaimTask is the name of the task [RegisterLiteralClaimTask] installs:
// one string input, `template`, that claims `literal`, beside an unclaimed `note`.
const LiteralClaimTask = "conformance-literal.claim"

// RegisterLiteralClaimTask installs [LiteralClaimTask] in the default registry
// for the test, which is the registry the server's admission and a local run
// without its own both read.
func RegisterLiteralClaimTask(tb testing.TB) {
	tb.Helper()

	str := descriptorpb.FieldDescriptorProto_TYPE_STRING.Enum()
	opt := descriptorpb.FieldDescriptorProto_LABEL_OPTIONAL.Enum()
	claimed := &descriptorpb.FieldOptions{}
	proto.SetExtension(claimed, v1.E_Input, &v1.InputOptions{Literal: true})

	file, err := protodesc.NewFile(&descriptorpb.FileDescriptorProto{
		Name:       proto.String("conformance/literal/v1/claim.proto"),
		Package:    proto.String("conformance.literal.v1"),
		Syntax:     proto.String("proto3"),
		Dependency: []string{"flowstate/v1/schema.proto"},
		MessageType: []*descriptorpb.DescriptorProto{{
			Name: proto.String("Inputs"),
			Field: []*descriptorpb.FieldDescriptorProto{
				{Name: proto.String("template"), Number: proto.Int32(1), Label: opt, Type: str, Options: claimed},
				{Name: proto.String("note"), Number: proto.Int32(2), Label: opt, Type: str},
			},
		}},
	}, protoregistry.GlobalFiles)
	require.NoError(tb, err)

	require.NoError(tb, v1.DefaultRegistry().Register(v1.TaskDef{
		Name:   LiteralClaimTask,
		Inputs: file.Messages().ByName("Inputs"),
		Fn: func(context.Context, map[string]*v1.Value, *v1.Scope) (*v1.Node_Outputs, error) {
			return &v1.Node_Outputs{}, nil
		},
	}))
	tb.Cleanup(func() { v1.DefaultRegistry().Unregister(LiteralClaimTask) })
}

// LiteralClaimRefusalCases are hand-built specifications that put something other
// than a literal where a task's input claims one. Both submit boundaries, the
// server's admission and the local driver's, must refuse them before any step
// runs; the task must have been installed with [RegisterLiteralClaimTask].
func LiteralClaimRefusalCases() []Refusal {
	task := func(name string, inputs map[string]*v1.Value) *v1.Workflow {
		return &v1.Workflow{
			Name:    name,
			Profile: v1.CurrentProfile,
			Steps: []*v1.Node{{
				Id:   "send",
				Kind: &v1.Node_Task{Task: &v1.Task{Name: LiteralClaimTask, Inputs: inputs}},
			}},
		}
	}
	want := `step "send": task "` + LiteralClaimTask + `" input "template": template must be written as a literal, but is an expression`

	return []Refusal{
		{
			Name:     "an expression in a literal-claimed input is refused",
			Workflow: task("literal-claim-expression", map[string]*v1.Value{"template": v1.NewExpr("'<b>' + 'x'")}),
			Contains: want,
		},
		{
			Name: "an expression in a literal-claimed input is refused beside an unclaimed expression",
			Workflow: task("literal-claim-sibling", map[string]*v1.Value{
				"template": v1.NewExpr("'<b>' + 'x'"),
				"note":     v1.NewExpr("'fine'"),
			}),
			Contains: want,
		},
	}
}
