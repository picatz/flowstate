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

// TestStripCELRulesReachesEveryPlaceAnExpressionCanBeWritten walks the shapes
// the first test does not: the `cel_expression` spelling, a repeated rule's
// items, a map rule's keys and values, a rule on a nested message and on an
// extension, and a predefined rule's own `cel`. Each is a place protovalidate
// would read an expression from, so each must come out empty while the standard
// rule beside it survives.
func TestStripCELRulesReachesEveryPlaceAnExpressionCanBeWritten(t *testing.T) {
	t.Parallel()

	rule := func() *validate.Rule {
		return &validate.Rule{Id: proto.String("r"), Expression: proto.String("true")}
	}
	field := func(rules *validate.FieldRules) *descriptorpb.FieldOptions {
		options := &descriptorpb.FieldOptions{}
		proto.SetExtension(options, validate.E_Field, rules)

		return options
	}

	itemRules := &validate.FieldRules{CelExpression: []string{"this != ''"}}
	keyRules := &validate.FieldRules{Cel: []*validate.Rule{rule()}}
	valueRules := &validate.FieldRules{CelExpression: []string{"this > 0"}}

	predefined := func() *descriptorpb.FieldOptions {
		options := &descriptorpb.FieldOptions{}
		proto.SetExtension(options, validate.E_Predefined, &validate.PredefinedRules{Cel: []*validate.Rule{rule()}})

		return options
	}

	messageOptions := &descriptorpb.MessageOptions{}
	proto.SetExtension(messageOptions, validate.E_Message, &validate.MessageRules{CelExpression: []string{"true"}})

	file := &descriptorpb.FileDescriptorProto{
		MessageType: []*descriptorpb.DescriptorProto{{
			Name: proto.String("Outer"),
			Field: []*descriptorpb.FieldDescriptorProto{
				{Name: proto.String("tags"), Options: field(&validate.FieldRules{
					CelExpression: []string{"true"},
					Type: &validate.FieldRules_Repeated{Repeated: &validate.RepeatedRules{
						MinItems: proto.Uint64(1), Items: itemRules,
					}},
				})},
				{Name: proto.String("scores"), Options: field(&validate.FieldRules{
					Type: &validate.FieldRules_Map{Map: &validate.MapRules{
						MinPairs: proto.Uint64(1), Keys: keyRules, Values: valueRules,
					}},
				})},
			},
			Extension: []*descriptorpb.FieldDescriptorProto{{Name: proto.String("declared"), Options: predefined()}},
			NestedType: []*descriptorpb.DescriptorProto{{
				Name:    proto.String("Inner"),
				Options: messageOptions,
			}},
		}},
		Extension: []*descriptorpb.FieldDescriptorProto{{Name: proto.String("filewide"), Options: predefined()}},
	}

	// Seven expression-bearing rules: tags.cel_expression, items, keys, values,
	// the nested message, and a predefined rule on a message-level extension
	// and on a file-level one.
	assert.Equal(t, 7, stripCELRules(file))
	assert.Zero(t, stripCELRules(file), "nothing is left to strip the second time")

	tags, ok := proto.GetExtension(file.GetMessageType()[0].GetField()[0].GetOptions(), validate.E_Field).(*validate.FieldRules)
	require.True(t, ok)
	assert.Empty(t, tags.GetCelExpression())
	assert.Equal(t, uint64(1), tags.GetRepeated().GetMinItems(), "the standard rule beside the expression survives")
	assert.Empty(t, tags.GetRepeated().GetItems().GetCelExpression())

	scores, ok := proto.GetExtension(file.GetMessageType()[0].GetField()[1].GetOptions(), validate.E_Field).(*validate.FieldRules)
	require.True(t, ok)
	assert.Equal(t, uint64(1), scores.GetMap().GetMinPairs())
	assert.Empty(t, scores.GetMap().GetKeys().GetCel())
	assert.Empty(t, scores.GetMap().GetValues().GetCelExpression())
}
