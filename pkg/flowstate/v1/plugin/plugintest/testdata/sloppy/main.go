// Command sloppy is a plugin that declares as little as the SDK allows, so the
// conformance checks have something to find. It is built by the tests, never
// shipped.
package main

import (
	"context"

	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protodesc"
	"google.golang.org/protobuf/reflect/protoreflect"
	"google.golang.org/protobuf/types/descriptorpb"
	"google.golang.org/protobuf/types/dynamicpb"

	flowstatev1 "github.com/picatz/flowstate/pkg/flowstate/v1"
	"github.com/picatz/flowstate/pkg/flowstate/v1/plugin/sdk"
)

func main() {
	sdk.Main(sdk.Plugin{
		Name: "sloppy",
		Tasks: []sdk.Task{
			{
				// No summary, no output message, and an input whose only
				// field carries no comment.
				Name:  "bare",
				Input: message("BareInput"),
				Fn:    echo,
			},
			{
				Name:    "claims",
				Summary: "Declares everything but comments.",
				Input:   message("ClaimsInput"),
				Output:  message("ClaimsOutput"),
				Fn:      echo,
			},
		},
	})
}

// message is a one-field message in a file with no comments, which is what a
// plugin gets from a schema nobody documented. A dynamic message stands in for
// generated code so the fixture needs no generation step.
func message(name string) proto.Message {
	file, err := protodesc.NewFile(&descriptorpb.FileDescriptorProto{
		Name:    proto.String("sloppy/" + name + ".proto"),
		Package: proto.String("sloppy"),
		Syntax:  proto.String("proto3"),
		MessageType: []*descriptorpb.DescriptorProto{{
			Name: proto.String(name),
			Field: []*descriptorpb.FieldDescriptorProto{{
				Name:     proto.String("value"),
				Number:   proto.Int32(1),
				Label:    descriptorpb.FieldDescriptorProto_LABEL_OPTIONAL.Enum(),
				Type:     descriptorpb.FieldDescriptorProto_TYPE_STRING.Enum(),
				JsonName: proto.String("value"),
			}},
		}},
	}, nil)
	if err != nil {
		panic(err)
	}
	return dynamicpb.NewMessage(file.Messages().ByName(protoreflect.Name(name)))
}

func echo(_ context.Context, _ map[string]*flowstatev1.Value, _ *flowstatev1.Scope) (*flowstatev1.Node_Outputs, error) {
	return &flowstatev1.Node_Outputs{}, nil
}
