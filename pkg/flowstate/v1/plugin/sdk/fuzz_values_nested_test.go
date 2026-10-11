package sdk

import (
	"math"
	"testing"

	expr "google.golang.org/genproto/googleapis/api/expr/v1alpha1"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protoreflect"
	"google.golang.org/protobuf/types/dynamicpb"
)

// TestRepeatedNestedMessagesRoundTrip is the #1456 shape built by hand, since a
// byte-driven seed cannot be relied on to reach it: a repeated message field and
// a message-valued map holding Nodes, one of which nests another, beside numbers
// at the edges of the ranges the bridge converts. It asserts the encoded values
// themselves, so a regression in repeated nested-message encoding or in numeric
// conversion fails here with the seeds alone, and then the round trip.
func TestRepeatedNestedMessagesRoundTrip(t *testing.T) {
	rich, node := fuzzSchema(t, false)
	field := func(name protoreflect.Name) protoreflect.FieldDescriptor { return rich.Fields().ByName(name) }
	newNode := func(name string) *dynamicpb.Message {
		n := dynamicpb.NewMessage(node)
		n.Set(node.Fields().ByName("name"), protoreflect.ValueOfString(name))
		return n
	}

	inner := newNode("inner")
	outer := newNode("outer")
	outer.Set(node.Fields().ByName("child"), protoreflect.ValueOfMessage(inner))
	kids := outer.Mutable(node.Fields().ByName("kids")).List()
	kids.Append(protoreflect.ValueOfMessage(newNode("kid")))

	msg := dynamicpb.NewMessage(rich)
	rn := msg.Mutable(field("rn")).List()
	rn.Append(protoreflect.ValueOfMessage(newNode("first")))
	rn.Append(protoreflect.ValueOfMessage(outer))
	msg.Mutable(field("mn")).Map().Set(protoreflect.ValueOfString("k").MapKey(), protoreflect.ValueOfMessage(newNode("mapped")))
	msg.Set(field("i32"), protoreflect.ValueOfInt32(math.MinInt32))
	msg.Set(field("i64"), protoreflect.ValueOfInt64(math.MaxInt64))
	msg.Set(field("u32"), protoreflect.ValueOfUint32(math.MaxUint32))
	msg.Set(field("u64"), protoreflect.ValueOfUint64(math.MaxInt64))
	msg.Set(field("fl"), protoreflect.ValueOfFloat32(1.5))
	msg.Set(field("db"), protoreflect.ValueOfFloat64(-2.25))
	msg.Set(field("color"), protoreflect.ValueOfEnum(2))

	outputs, err := EncodeOutputs(msg)
	if err != nil {
		t.Fatal(err)
	}
	values := outputs.GetNamedValues()

	list := values["rn"].GetLiteral().GetListValue().GetValues()
	if len(list) != 2 {
		t.Fatalf("rn encoded %d elements, want 2: %v", len(list), values["rn"])
	}
	entry := func(m *expr.Value, key string) *expr.Value {
		for _, e := range m.GetMapValue().GetEntries() {
			if e.GetKey().GetStringValue() == key {
				return e.GetValue()
			}
		}
		t.Fatalf("no key %q in %v", key, m)
		return nil
	}
	if got := entry(list[0], "name").GetStringValue(); got != "first" {
		t.Errorf("rn[0].name = %q", got)
	}
	if got := entry(entry(list[1], "child"), "name").GetStringValue(); got != "inner" {
		t.Errorf("rn[1].child.name = %q", got)
	}
	if got := len(entry(list[1], "kids").GetListValue().GetValues()); got != 1 {
		t.Errorf("rn[1].kids has %d elements, want 1", got)
	}
	if got := entry(values["mn"].GetLiteral(), "k"); entry(got, "name").GetStringValue() != "mapped" {
		t.Errorf("mn[k] = %v", got)
	}

	for name, want := range map[string]*expr.Value{
		"i32":   {Kind: &expr.Value_Int64Value{Int64Value: math.MinInt32}},
		"i64":   {Kind: &expr.Value_Int64Value{Int64Value: math.MaxInt64}},
		"u32":   {Kind: &expr.Value_Uint64Value{Uint64Value: math.MaxUint32}},
		"u64":   {Kind: &expr.Value_Uint64Value{Uint64Value: math.MaxInt64}},
		"fl":    {Kind: &expr.Value_DoubleValue{DoubleValue: 1.5}},
		"db":    {Kind: &expr.Value_DoubleValue{DoubleValue: -2.25}},
		"color": {Kind: &expr.Value_Int64Value{Int64Value: 2}},
	} {
		if got := values[name].GetLiteral(); !proto.Equal(got, want) {
			t.Errorf("%s encoded as %v, want %v", name, got, want)
		}
	}

	// And back: the values the encoder produced rebuild the message.
	checkRoundTrip(t, msg, rich)
}
