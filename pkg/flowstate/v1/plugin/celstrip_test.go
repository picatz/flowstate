package plugin

import (
	"errors"
	"testing"
	"time"

	"buf.build/gen/go/bufbuild/protovalidate/protocolbuffers/go/buf/validate"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protoreflect"
	"google.golang.org/protobuf/types/descriptorpb"
	"google.golang.org/protobuf/types/dynamicpb"

	v1 "github.com/picatz/flowstate/pkg/flowstate/v1"
)

// ruledFile is a plugin descriptor whose one string field carries a standard
// rule (max_len), a `cel` rule that has no business running, and a message
// level `cel` rule, the shapes #1531 says a peer can use to make the host's
// second evaluator spin.
func ruledFile(t *testing.T) *descriptorpb.FileDescriptorProto {
	t.Helper()

	fieldOptions := &descriptorpb.FieldOptions{}
	proto.SetExtension(fieldOptions, validate.E_Field, &validate.FieldRules{
		Cel: []*validate.Rule{{
			Id:         proto.String("pathological"),
			Message:    proto.String("never"),
			Expression: proto.String(`this.matches('(a+)+$')`),
		}},
		Type: &validate.FieldRules_String_{String_: &validate.StringRules{MaxLen: proto.Uint64(8)}},
	})

	messageOptions := &descriptorpb.MessageOptions{}
	proto.SetExtension(messageOptions, validate.E_Message, &validate.MessageRules{
		Cel: []*validate.Rule{{Id: proto.String("message-level"), Expression: proto.String(`this.name.size() < 0`)}},
	})

	return &descriptorpb.FileDescriptorProto{
		Name:       proto.String("plugintest/v1/ruled.proto"),
		Package:    proto.String("plugintest.v1"),
		Syntax:     proto.String("proto3"),
		Dependency: []string{"buf/validate/validate.proto"},
		MessageType: []*descriptorpb.DescriptorProto{{
			Name:    proto.String("Ruled"),
			Options: messageOptions,
			Field: []*descriptorpb.FieldDescriptorProto{{
				Name:    proto.String("name"),
				Number:  proto.Int32(1),
				Label:   descriptorpb.FieldDescriptorProto_LABEL_OPTIONAL.Enum(),
				Type:    descriptorpb.FieldDescriptorProto_TYPE_STRING.Enum(),
				Options: fieldOptions,
			}},
		}},
	}
}

func TestAPluginsCELRulesAreNotEvaluatedAndItsStandardRulesAre(t *testing.T) {
	t.Parallel()

	desc, err := messageDescriptor(mustMarshal(t, ruledFile(t)), "plugintest.v1.Ruled", Config{}.withDefaults())
	require.NoError(t, err)

	// The attacker's value: a string the field-level `cel` rule would take
	// exponential time on, were it evaluated, and one that also breaks the
	// standard max_len rule so the standard rule is seen to still hold.
	long := dynamicpb.NewMessage(desc)
	long.Set(desc.Fields().ByName("name"), protoValue("aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa!"))

	done := make(chan error, 1)
	go func() { done <- v1.Validate(long) }()

	select {
	case err := <-done:
		var invalid *v1.ValidationError
		require.True(t, errors.As(err, &invalid), "the standard max_len rule must still refuse: %v", err)
		require.Len(t, invalid.Violations, 1, "only the standard rule may fire, not the stripped cel rules")
	case <-time.After(10 * time.Second):
		t.Fatal("validation did not finish: a plugin's cel rule was evaluated")
	}

	short := dynamicpb.NewMessage(desc)
	short.Set(desc.Fields().ByName("name"), protoValue("ok"))
	require.NoError(t, v1.Validate(short), "the message-level cel rule `this.name.size() < 0` must not have been kept")

}

func TestStripCELRulesCountsWhatItRemovesAndKeepsStandardRules(t *testing.T) {
	t.Parallel()

	file := ruledFile(t)
	assert.Equal(t, 2, stripCELRules(file))
	assert.Zero(t, stripCELRules(file), "stripping twice must find nothing the second time")

	rules, ok := proto.GetExtension(file.GetMessageType()[0].GetField()[0].GetOptions(), validate.E_Field).(*validate.FieldRules)
	require.True(t, ok)
	assert.Equal(t, uint64(8), rules.GetString().GetMaxLen(), "a standard rule survives")
}

func protoValue(s string) protoreflect.Value { return protoreflect.ValueOfString(s) }
