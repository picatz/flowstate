package sdk

import (
	"errors"
	"math"
	"strings"
	"testing"

	expr "google.golang.org/genproto/googleapis/api/expr/v1alpha1"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protoreflect"
	"google.golang.org/protobuf/types/dynamicpb"

	flowstatev1 "github.com/picatz/flowstate/pkg/flowstate/v1"
)

// TestUint64AboveMaxInt64RoundTrips pins #2697 (1): a uint64 field holds the
// whole unsigned range, so what EncodeOutputs emits for it decodes back.
func TestUint64AboveMaxInt64RoundTrips(t *testing.T) {
	rich, _ := fuzzSchema(t)
	for _, name := range []protoreflect.Name{"u64", "fx64"} {
		field := rich.Fields().ByName(name)
		for _, want := range []uint64{math.MaxInt64, 1 << 63, 1<<63 + 1, math.MaxUint64} {
			msg := dynamicpb.NewMessage(rich)
			msg.Set(field, protoreflect.ValueOfUint64(want))

			outputs, err := EncodeOutputs(msg)
			if err != nil {
				t.Fatalf("%s=%d: %v", name, want, err)
			}
			literal := outputs.GetNamedValues()[string(name)].GetLiteral()
			if got, ok := literal.GetKind().(*expr.Value_Uint64Value); !ok || got.Uint64Value != want {
				t.Fatalf("%s=%d encoded as %v", name, want, literal)
			}

			back := dynamicpb.NewMessage(rich)
			err = DecodeInputs(map[string]*flowstatev1.Value{string(name): outputs.GetNamedValues()[string(name)]}, back)
			if err != nil {
				t.Fatalf("%s=%d did not decode: %v", name, want, err)
			}
			if got := back.Get(field).Uint(); got != want {
				t.Fatalf("%s=%d came back as %d", name, want, got)
			}
		}
	}
}

// TestUint64FieldRefusesWhatDoesNotFit holds the other side of the widening: a
// uint64 field still refuses a negative, a fraction and a double out of range.
func TestUint64FieldRefusesWhatDoesNotFit(t *testing.T) {
	rich, _ := fuzzSchema(t)
	for name, value := range map[string]*expr.Value{
		"minus one":         {Kind: &expr.Value_Int64Value{Int64Value: -1}},
		"min int64":         {Kind: &expr.Value_Int64Value{Int64Value: math.MinInt64}},
		"negative double":   {Kind: &expr.Value_DoubleValue{DoubleValue: -1}},
		"fraction":          {Kind: &expr.Value_DoubleValue{DoubleValue: 1.5}},
		"double at 2^64":    {Kind: &expr.Value_DoubleValue{DoubleValue: 1 << 64}},
		"double above 2^64": {Kind: &expr.Value_DoubleValue{DoubleValue: 1e30}},
		"string":            {Kind: &expr.Value_StringValue{StringValue: "1"}},
	} {
		err := DecodeInputs(map[string]*flowstatev1.Value{"u64": fuzzLiteralValue(value)}, dynamicpb.NewMessage(rich))
		if !classifiedInvalid(err) {
			t.Errorf("%s: want InvalidInput, got %v", name, err)
		}
	}

	// A uint32 field is untouched: it stays bounded by MaxUint32.
	for _, n := range []uint64{math.MaxUint32 + 1, 1 << 63, math.MaxUint64} {
		err := DecodeInputs(map[string]*flowstatev1.Value{"u32": fuzzLiteralValue(&expr.Value{Kind: &expr.Value_Uint64Value{Uint64Value: n}})}, dynamicpb.NewMessage(rich))
		if !classifiedInvalid(err) {
			t.Errorf("u32=%d: want InvalidInput, got %v", n, err)
		}
	}

	// An exactly integral double in range is still accepted.
	msg := dynamicpb.NewMessage(rich)
	if err := DecodeInputs(map[string]*flowstatev1.Value{"u64": fuzzLiteralValue(&expr.Value{Kind: &expr.Value_DoubleValue{DoubleValue: 1 << 63}})}, msg); err != nil {
		t.Fatal(err)
	}
	if got := msg.Get(rich.Fields().ByName("u64")).Uint(); got != 1<<63 {
		t.Fatalf("got %d", got)
	}
}

// TestLiteralYieldsErrorValueForUnrepresentable pins #2697 (2): Literal never
// returns nil, and an input it cannot represent is an error value that reading
// the literal back refuses, at the top and nested in a list or a map.
func TestLiteralYieldsErrorValueForUnrepresentable(t *testing.T) {
	for name, input := range map[string]any{
		"chan":            make(chan int),
		"error":           errors.New("boom"),
		"uint64 over max": uint64(math.MaxInt64) + 1,
		"max uint64":      uint64(math.MaxUint64),
		"func":            func() {},
	} {
		got := Literal(input)
		requireErrorLiteral(t, input, got)
		if _, err := flowstatev1.LiteralToGo(got); err == nil {
			t.Errorf("%s: reading the error value back succeeded", name)
		}

		nested := Literal(map[string]any{"list": []any{1, input}, "k": input})
		if nested == nil {
			t.Fatalf("%s: nested Literal is nil", name)
		}
		for _, entry := range nested.GetMapValue().GetEntries() {
			switch entry.GetKey().GetStringValue() {
			case "k":
				requireErrorLiteral(t, input, entry.GetValue())
			case "list":
				values := entry.GetValue().GetListValue().GetValues()
				if len(values) != 2 || values[0].GetInt64Value() != 1 {
					t.Fatalf("%s: list became %v", name, entry.GetValue())
				}
				requireErrorLiteral(t, input, values[1])
			}
		}
	}

	if got := Literal(uint64(math.MaxInt64)); got.GetInt64Value() != math.MaxInt64 {
		t.Errorf("uint64 at MaxInt64 became %v", got)
	}
	if message := Literal(make(chan int)).GetObjectValue().String(); !strings.Contains(message, "chan int") {
		t.Errorf("the error value does not name the type: %s", message)
	}
}

// TestUnsetCELValueEncodesNull pins the second half of #2697 (2): an unset cel
// Value output is null like an unset flowstate Value, not an empty literal.
func TestUnsetCELValueEncodesNull(t *testing.T) {
	outputs, err := EncodeOutputs(&flowstatev1.Task_HTTP_Outputs{})
	if err != nil {
		t.Fatal(err)
	}
	literal := outputs.GetNamedValues()["json"].GetLiteral()
	if _, null := literal.GetKind().(*expr.Value_NullValue); !null {
		t.Fatalf("an unset cel Value encoded as %v, want null", literal)
	}

	rich, _ := fuzzSchema(t)
	outputs, err = EncodeOutputs(dynamicpb.NewMessage(rich))
	if err != nil {
		t.Fatal(err)
	}
	for _, name := range []string{"any", "val"} {
		if _, null := outputs.GetNamedValues()[name].GetLiteral().GetKind().(*expr.Value_NullValue); !null {
			t.Errorf("an unset %s encoded as %v, want null", name, outputs.GetNamedValues()[name])
		}
	}

	// Set to an empty literal on purpose, it is kept as written.
	set, err := EncodeOutputs(&flowstatev1.Task_HTTP_Outputs{Json: &expr.Value{}})
	if err != nil {
		t.Fatal(err)
	}
	if got := set.GetNamedValues()["json"].GetLiteral(); got == nil || got.GetKind() != nil {
		t.Errorf("a set empty literal encoded as %v", got)
	}
}

// TestDynamicValueFieldsAreMatchedByName pins #2697 (3): a dynamicpb message's
// cel and flowstate Value fields are encoded as values, as generated code's are,
// and not as ordinary maps of their own fields.
func TestDynamicValueFieldsAreMatchedByName(t *testing.T) {
	rich, _ := fuzzSchema(t)
	field := func(name protoreflect.Name) protoreflect.FieldDescriptor { return rich.Fields().ByName(name) }

	document := Literal(map[string]any{"id": 7, "tags": []any{"a", "b"}})
	msg := dynamicpb.NewMessage(rich)

	set := func(fd protoreflect.FieldDescriptor, m proto.Message) protoreflect.Value {
		dyn := dynamicpb.NewMessage(fd.Message())
		raw, err := proto.Marshal(m)
		if err != nil {
			t.Fatal(err)
		}
		if err := proto.Unmarshal(raw, dyn); err != nil {
			t.Fatal(err)
		}
		return protoreflect.ValueOfMessage(dyn)
	}
	msg.Set(field("any"), set(field("any"), document))
	msg.Set(field("val"), set(field("val"), &flowstatev1.Value{Kind: &flowstatev1.Value_Literal{Literal: document}}))

	list := msg.Mutable(field("anys")).List()
	list.Append(set(field("anys"), document))
	msg.Mutable(field("many")).Map().Set(protoreflect.ValueOfString("k").MapKey(), set(field("many").MapValue(), document))

	outputs, err := EncodeOutputs(msg)
	if err != nil {
		t.Fatal(err)
	}
	values := outputs.GetNamedValues()
	for _, name := range []string{"any", "val"} {
		if got := values[name].GetLiteral(); !proto.Equal(got, document) {
			t.Errorf("%s encoded as %v, want %v", name, got, document)
		}
	}
	if got := values["anys"].GetLiteral().GetListValue().GetValues(); len(got) != 1 || !proto.Equal(got[0], document) {
		t.Errorf("anys encoded as %v", values["anys"])
	}
	if got := values["many"].GetLiteral().GetMapValue().GetEntries(); len(got) != 1 || !proto.Equal(got[0].GetValue(), document) {
		t.Errorf("many encoded as %v", values["many"])
	}

	// A dynamic flowstate Value holding an unresolved expression is refused, as
	// the generated one is.
	bad := dynamicpb.NewMessage(rich)
	bad.Set(field("val"), set(field("val"), &flowstatev1.Value{Kind: &flowstatev1.Value_Expr{}}))
	if _, err := EncodeOutputs(bad); err == nil {
		t.Error("a dynamic Value holding an expression was encoded")
	}
}
