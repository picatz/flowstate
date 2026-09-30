// Command flaky is a plugin whose descriptors differ on every launch and whose
// health poll declines without saying why, so the checks for those two have
// something to find. It is built by the tests, never shipped.
package main

import (
	"context"
	"errors"
	"fmt"
	"time"

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
		Name:        "flaky",
		Version:     "0.0.1",
		Description: "Declares a different schema every time it starts.",
		Tasks: []sdk.Task{{
			Name:    "drift",
			Summary: "Its input field is named for the moment it launched.",
			Input:   message("DriftInput"),
			Output:  message("DriftOutput"),
			Fn:      noop,
		}},
		// An error with no text: not serving, and no reason given.
		Health: func(context.Context) error { return errors.New("") },
	})
}

// message is a one-field message whose field name depends on the launch time,
// which is what a descriptor built from a clock or from map order looks like.
func message(name string) proto.Message {
	field := fmt.Sprintf("f_%d", time.Now().UnixNano())
	file, err := protodesc.NewFile(&descriptorpb.FileDescriptorProto{
		Name:    proto.String("flaky/" + name + ".proto"),
		Package: proto.String("flaky"),
		Syntax:  proto.String("proto3"),
		MessageType: []*descriptorpb.DescriptorProto{{
			Name: proto.String(name),
			Field: []*descriptorpb.FieldDescriptorProto{{
				Name:     proto.String(field),
				Number:   proto.Int32(1),
				Label:    descriptorpb.FieldDescriptorProto_LABEL_OPTIONAL.Enum(),
				Type:     descriptorpb.FieldDescriptorProto_TYPE_STRING.Enum(),
				JsonName: proto.String(field),
			}},
		}},
	}, nil)
	if err != nil {
		panic(err)
	}
	return dynamicpb.NewMessage(file.Messages().ByName(protoreflect.Name(name)))
}

func noop(context.Context, map[string]*flowstatev1.Value, *flowstatev1.Scope) (*flowstatev1.Node_Outputs, error) {
	return &flowstatev1.Node_Outputs{}, nil
}
