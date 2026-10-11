package sdk

import (
	"encoding/binary"
	"errors"
	"math"
	"strconv"
	"strings"
	"testing"
	"time"

	"connectrpc.com/connect"
	expr "google.golang.org/genproto/googleapis/api/expr/v1alpha1"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protodesc"
	"google.golang.org/protobuf/reflect/protoreflect"
	"google.golang.org/protobuf/reflect/protoregistry"
	"google.golang.org/protobuf/types/descriptorpb"
	"google.golang.org/protobuf/types/dynamicpb"

	flowstatev1 "github.com/picatz/flowstate/pkg/flowstate/v1"
)

// The bounds [flowstatev1.SetLiteralField] documents and DecodeInputs inherits,
// and the one EncodeOutputs states for itself. They are restated here rather
// than imported because the decoder's are unexported: a change to either side
// that the docs do not follow is exactly what these targets should notice, and
// TestFuzzBoundsAreTheDocumentedOnes pins the restatement to the behaviour.
const (
	fuzzMaxDecodeDepth    = 16
	fuzzMaxDecodeElements = 1024
	fuzzMaxDecodeNodes    = 1 << 16
	fuzzMaxEncodeDepth    = 32

	// fuzzMaxGenNodes caps the values one iteration's generator builds, which is
	// what keeps an iteration cheap whatever the fuzzer feeds it. The hostile
	// shapes the bounds exist for — deep, wide — come from explicit parameters
	// instead, so they cost one allocation per level and not one per input byte.
	fuzzMaxGenNodes = 512
	// fuzzMaxChain bounds the nesting a fuzz parameter may request: far past
	// every limit under test, and still linear work.
	fuzzMaxChain = 4096
)

// fuzzSchema is one descriptor with every field shape the bridge converts, built
// without generated code so the targets do not depend on a plugin's own protos.
//
// The four fields typed as a CEL or flowstate value (any, anys, many, val) are
// matched by full name, so a dynamic message with them takes the same path as
// generated code.
//
//	enum Color { COLOR_UNSPECIFIED = 0; RED = 1; GREEN = 2; }
//	message Node { string name = 1; Node child = 2; repeated Node kids = 3; }
//	message Rich {
//	  string s = 1; bytes b = 2; bool flag = 3;
//	  int32 i32 = 4; int64 i64 = 5; sint32 si32 = 6; sint64 si64 = 7;
//	  sfixed32 sf32 = 8; sfixed64 sf64 = 9;
//	  uint32 u32 = 10; uint64 u64 = 11; fixed32 fx32 = 12; fixed64 fx64 = 13;
//	  float fl = 14; double db = 15; Color color = 16;
//	  repeated string rs = 17; repeated int32 ri = 18; repeated Node rn = 19;
//	  map<string, string> ms = 20; map<string, int64> mi = 21; map<string, Node> mn = 22;
//	  Node node = 23;
//	  oneof choice { string a = 24; Node n = 25; }
//	  optional string opt = 26;
//	  google.api.expr.v1alpha1.Value any = 27;
//	  repeated google.api.expr.v1alpha1.Value anys = 28;
//	  map<string, google.api.expr.v1alpha1.Value> many = 29;
//	  flowstate.v1.Value val = 30;
//	  google.protobuf.Timestamp ts = 31;
//	}
func fuzzSchema(tb testing.TB) (rich, node protoreflect.MessageDescriptor) {
	tb.Helper()

	str := descriptorpb.FieldDescriptorProto_TYPE_STRING
	msg := descriptorpb.FieldDescriptorProto_TYPE_MESSAGE
	optional := descriptorpb.FieldDescriptorProto_LABEL_OPTIONAL
	repeated := descriptorpb.FieldDescriptorProto_LABEL_REPEATED

	field := func(name string, number int32, kind descriptorpb.FieldDescriptorProto_Type, label descriptorpb.FieldDescriptorProto_Label, typeName string) *descriptorpb.FieldDescriptorProto {
		f := &descriptorpb.FieldDescriptorProto{
			Name: proto.String(name), Number: proto.Int32(number), Type: kind.Enum(), Label: label.Enum(),
			JsonName: proto.String(name),
		}
		if typeName != "" {
			f.TypeName = proto.String(typeName)
		}
		return f
	}
	scalar := func(name string, number int32, kind descriptorpb.FieldDescriptorProto_Type) *descriptorpb.FieldDescriptorProto {
		return field(name, number, kind, optional, "")
	}
	entry := func(name string, valueKind descriptorpb.FieldDescriptorProto_Type, valueType string) *descriptorpb.DescriptorProto {
		return &descriptorpb.DescriptorProto{
			Name:    proto.String(name),
			Options: &descriptorpb.MessageOptions{MapEntry: proto.Bool(true)},
			Field: []*descriptorpb.FieldDescriptorProto{
				scalar("key", 1, str),
				field("value", 2, valueKind, optional, valueType),
			},
		}
	}
	inChoice := func(f *descriptorpb.FieldDescriptorProto) *descriptorpb.FieldDescriptorProto {
		f.OneofIndex = proto.Int32(0)
		return f
	}

	const celValue = ".google.api.expr.v1alpha1.Value"
	fdp := &descriptorpb.FileDescriptorProto{
		Name:       proto.String("fuzzvalues.proto"),
		Package:    proto.String("fuzzvalues.v1"),
		Syntax:     proto.String("proto3"),
		Dependency: []string{"google/api/expr/v1alpha1/value.proto", "flowstate/v1/value.proto", "google/protobuf/timestamp.proto"},
		EnumType: []*descriptorpb.EnumDescriptorProto{{
			Name: proto.String("Color"),
			Value: []*descriptorpb.EnumValueDescriptorProto{
				{Name: proto.String("COLOR_UNSPECIFIED"), Number: proto.Int32(0)},
				{Name: proto.String("RED"), Number: proto.Int32(1)},
				{Name: proto.String("GREEN"), Number: proto.Int32(2)},
			},
		}},
		MessageType: []*descriptorpb.DescriptorProto{
			{
				Name: proto.String("Node"),
				Field: []*descriptorpb.FieldDescriptorProto{
					scalar("name", 1, str),
					field("child", 2, msg, optional, ".fuzzvalues.v1.Node"),
					field("kids", 3, msg, repeated, ".fuzzvalues.v1.Node"),
				},
			},
			{
				Name: proto.String("Rich"),
				Field: []*descriptorpb.FieldDescriptorProto{
					scalar("s", 1, str),
					scalar("b", 2, descriptorpb.FieldDescriptorProto_TYPE_BYTES),
					scalar("flag", 3, descriptorpb.FieldDescriptorProto_TYPE_BOOL),
					scalar("i32", 4, descriptorpb.FieldDescriptorProto_TYPE_INT32),
					scalar("i64", 5, descriptorpb.FieldDescriptorProto_TYPE_INT64),
					scalar("si32", 6, descriptorpb.FieldDescriptorProto_TYPE_SINT32),
					scalar("si64", 7, descriptorpb.FieldDescriptorProto_TYPE_SINT64),
					scalar("sf32", 8, descriptorpb.FieldDescriptorProto_TYPE_SFIXED32),
					scalar("sf64", 9, descriptorpb.FieldDescriptorProto_TYPE_SFIXED64),
					scalar("u32", 10, descriptorpb.FieldDescriptorProto_TYPE_UINT32),
					scalar("u64", 11, descriptorpb.FieldDescriptorProto_TYPE_UINT64),
					scalar("fx32", 12, descriptorpb.FieldDescriptorProto_TYPE_FIXED32),
					scalar("fx64", 13, descriptorpb.FieldDescriptorProto_TYPE_FIXED64),
					scalar("fl", 14, descriptorpb.FieldDescriptorProto_TYPE_FLOAT),
					scalar("db", 15, descriptorpb.FieldDescriptorProto_TYPE_DOUBLE),
					field("color", 16, descriptorpb.FieldDescriptorProto_TYPE_ENUM, optional, ".fuzzvalues.v1.Color"),
					field("rs", 17, str, repeated, ""),
					field("ri", 18, descriptorpb.FieldDescriptorProto_TYPE_INT32, repeated, ""),
					field("rn", 19, msg, repeated, ".fuzzvalues.v1.Node"),
					field("ms", 20, msg, repeated, ".fuzzvalues.v1.Rich.MsEntry"),
					field("mi", 21, msg, repeated, ".fuzzvalues.v1.Rich.MiEntry"),
					field("mn", 22, msg, repeated, ".fuzzvalues.v1.Rich.MnEntry"),
					field("node", 23, msg, optional, ".fuzzvalues.v1.Node"),
					inChoice(scalar("a", 24, str)),
					inChoice(field("n", 25, msg, optional, ".fuzzvalues.v1.Node")),
					func() *descriptorpb.FieldDescriptorProto {
						f := scalar("opt", 26, str)
						f.OneofIndex = proto.Int32(1)
						f.Proto3Optional = proto.Bool(true)
						return f
					}(),
					field("any", 27, msg, optional, celValue),
					field("anys", 28, msg, repeated, celValue),
					field("many", 29, msg, repeated, ".fuzzvalues.v1.Rich.ManyEntry"),
					field("val", 30, msg, optional, ".flowstate.v1.Value"),
					field("ts", 31, msg, optional, ".google.protobuf.Timestamp"),
				},
				NestedType: []*descriptorpb.DescriptorProto{
					entry("MsEntry", str, ""),
					entry("MiEntry", descriptorpb.FieldDescriptorProto_TYPE_INT64, ""),
					entry("MnEntry", msg, ".fuzzvalues.v1.Node"),
					entry("ManyEntry", msg, celValue),
				},
				OneofDecl: []*descriptorpb.OneofDescriptorProto{{Name: proto.String("choice")}, {Name: proto.String("_opt")}},
			},
		},
	}

	file, err := protodesc.NewFile(fdp, protoregistry.GlobalFiles)
	if err != nil {
		tb.Fatalf("building the fuzz schema: %v", err)
	}
	return file.Messages().ByName("Rich"), file.Messages().ByName("Node")
}

// fuzzGen turns fuzz bytes into values. It reads past the end as zeroes and
// stops building at fuzzMaxGenNodes, so every input is valid and every iteration
// costs about the same.
type fuzzGen struct {
	data  []byte
	pos   int
	nodes int
	names []string
}

func newFuzzGen(data []byte, rich protoreflect.MessageDescriptor) *fuzzGen {
	g := &fuzzGen{data: data}
	for _, desc := range []protoreflect.MessageDescriptor{rich, rich.Fields().ByName("node").Message()} {
		for i := range desc.Fields().Len() {
			g.names = append(g.names, string(desc.Fields().Get(i).Name()))
		}
	}
	return g
}

func (g *fuzzGen) next() byte {
	if g.pos >= len(g.data) {
		return 0
	}
	b := g.data[g.pos]
	g.pos++
	return b
}

func (g *fuzzGen) word() uint64 {
	var raw [8]byte
	for i := range raw {
		raw[i] = g.next()
	}
	return binary.LittleEndian.Uint64(raw[:])
}

// edgeIntegers are the values every conversion table's range checks turn on.
var edgeIntegers = []int64{
	0, 1, -1, 2, math.MaxInt8, math.MaxInt16, math.MaxInt32, math.MaxInt32 + 1,
	math.MinInt32, math.MinInt32 - 1, math.MaxUint32, math.MaxUint32 + 1,
	math.MaxInt64, math.MinInt64,
}

func (g *fuzzGen) integer() int64 {
	if b := g.next(); int(b) < len(edgeIntegers) {
		return edgeIntegers[b]
	}
	return int64(g.word())
}

func (g *fuzzGen) text() string {
	n := int(g.next() % 8)
	if n == 0 {
		return g.names[int(g.next())%len(g.names)]
	}
	out := make([]byte, 0, n)
	for range n {
		out = append(out, g.next())
	}
	return string(out)
}

// literal builds a CEL value of any kind. Map keys are drawn from the schema's
// own field names more often than not, so a nested map lands on real fields.
func (g *fuzzGen) literal(depth int) *expr.Value {
	g.nodes++
	if g.nodes > fuzzMaxGenNodes || depth > 24 {
		return &expr.Value{Kind: &expr.Value_NullValue{}}
	}

	switch g.next() % 11 {
	case 0:
		return &expr.Value{Kind: &expr.Value_NullValue{}}
	case 1:
		return &expr.Value{Kind: &expr.Value_BoolValue{BoolValue: g.next()&1 == 1}}
	case 2:
		return &expr.Value{Kind: &expr.Value_Int64Value{Int64Value: g.integer()}}
	case 3:
		return &expr.Value{Kind: &expr.Value_Uint64Value{Uint64Value: fuzzUnsigned(g)}}
	case 4:
		return &expr.Value{Kind: &expr.Value_DoubleValue{DoubleValue: g.double()}}
	case 5:
		return &expr.Value{Kind: &expr.Value_StringValue{StringValue: g.text()}}
	case 6:
		return &expr.Value{Kind: &expr.Value_BytesValue{BytesValue: []byte(g.text())}}
	case 7:
		return &expr.Value{Kind: &expr.Value_EnumValue{EnumValue: &expr.EnumValue{Type: "x", Value: int32(g.integer())}}}
	case 8, 9:
		n := int(g.next() % 5)
		list := &expr.ListValue{}
		for range n {
			list.Values = append(list.Values, g.literal(depth+1))
		}
		return &expr.Value{Kind: &expr.Value_ListValue{ListValue: list}}
	default:
		n := int(g.next() % 5)
		m := &expr.MapValue{}
		for range n {
			var key *expr.Value
			if g.next()%8 == 0 {
				key = g.literal(depth + 1) // a key that is not a string is a refusal to reach
			} else {
				key = &expr.Value{Kind: &expr.Value_StringValue{StringValue: g.names[int(g.next())%len(g.names)]}}
			}
			m.Entries = append(m.Entries, &expr.MapValue_Entry{Key: key, Value: g.literal(depth + 1)})
		}
		return &expr.Value{Kind: &expr.Value_MapValue{MapValue: m}}
	}
}

func (g *fuzzGen) double() float64 {
	switch b := g.next(); b % 8 {
	case 0:
		return math.NaN()
	case 1:
		return math.Inf(1)
	case 2:
		return math.MaxFloat32
	case 3:
		return math.MaxFloat32 * 2
	case 4:
		return 1.5
	default:
		return math.Float64frombits(g.word())
	}
}

// value wraps a literal in the other arms a request can carry, which DecodeInputs
// must refuse by classification and never by falling through.
func (g *fuzzGen) value() *flowstatev1.Value {
	switch g.next() % 12 {
	case 0:
		return &flowstatev1.Value{Kind: &flowstatev1.Value_Expr{}}
	case 1:
		return &flowstatev1.Value{Kind: &flowstatev1.Value_SecretRef{}}
	case 2:
		return &flowstatev1.Value{Kind: &flowstatev1.Value_CredentialRef{}}
	case 3:
		return &flowstatev1.Value{Kind: &flowstatev1.Value_Error_{Error: &flowstatev1.Value_Error{Message: g.text()}}}
	case 4:
		return &flowstatev1.Value{}
	case 5:
		return &flowstatev1.Value{Kind: &flowstatev1.Value_Structure_{}}
	default:
		return &flowstatev1.Value{Kind: &flowstatev1.Value_Literal{Literal: g.literal(0)}}
	}
}

// fuzzChain is a self-referential Node literal of n messages, deepest last:
// {"name": "0", "child": {"name": "1", "child": ...}}.
func fuzzChain(n int) *expr.Value {
	var inner *expr.Value
	for i := n - 1; i >= 0; i-- {
		entries := []*expr.MapValue_Entry{{
			Key:   &expr.Value{Kind: &expr.Value_StringValue{StringValue: "name"}},
			Value: &expr.Value{Kind: &expr.Value_StringValue{StringValue: strconv.Itoa(i)}},
		}}
		if inner != nil {
			entries = append(entries, &expr.MapValue_Entry{
				Key:   &expr.Value{Kind: &expr.Value_StringValue{StringValue: "child"}},
				Value: inner,
			})
		}
		inner = &expr.Value{Kind: &expr.Value_MapValue{MapValue: &expr.MapValue{Entries: entries}}}
	}
	return inner
}

func fuzzInts(n int) *expr.Value {
	list := &expr.ListValue{Values: make([]*expr.Value, n)}
	for i := range list.Values {
		list.Values[i] = &expr.Value{Kind: &expr.Value_Int64Value{Int64Value: int64(i % 100)}}
	}
	return &expr.Value{Kind: &expr.Value_ListValue{ListValue: list}}
}

func fuzzLiteralValue(l *expr.Value) *flowstatev1.Value {
	return &flowstatev1.Value{Kind: &flowstatev1.Value_Literal{Literal: l}}
}

// classifiedInvalid reports whether err is the workflow-input refusal.
func classifiedInvalid(err error) bool {
	var c *classified
	return errors.As(err, &c) && c.code == connect.CodeInvalidArgument
}

// acceptableDecodeRefusal is the full set of ways DecodeInputs may say no: the
// workflow's mistake, classified InvalidInput, or the task's own declaration
// disagreeing with the wire, which stays unclassified on purpose.
func acceptableDecodeRefusal(err error) bool {
	return classifiedInvalid(err) || errors.Is(err, errTaskDeclaration)
}

// FuzzDecodeInputs fuzzes [DecodeInputs], the bridge from a request's named
// values into a plugin's typed message. The inputs are peer-controlled once a
// gateway serves the plugin protocol to workers it does not run (#1524), and a
// panic in this path reaches the host as a dropped connection it retries (#1490).
//
// Three kinds of input drive it: data builds an arbitrary Value tree for every
// field of one rich descriptor; chain and width then add the hostile shapes the
// documented bounds exist for, a Node nested chain messages deep and a repeated
// field width entries wide, which the byte stream alone would almost never reach.
//
// The invariants under fuzz:
//
//   - No panic, and no iteration costs more than the bounds allow.
//   - A refusal is classified: InvalidInput for the workflow's mistake, or the
//     unclassified task-declaration error, and never a bare error.
//   - The chain bound holds exactly: a Node chain of at most 16 messages is
//     accepted and a longer one is refused with InvalidInput naming the depth.
//   - The width bound holds exactly: at most 1024 entries are accepted.
//   - A message DecodeInputs accepts is within the documented bounds: nested no
//     deeper than 16 messages, no list or map over 1024 entries, no more than
//     65536 values in all.
//   - The request is not modified, though a cel Value field may share its nodes.
//   - What decodes survives an encode and a decode: EncodeOutputs of an accepted
//     message, fed back to DecodeInputs, rebuilds an equal message.
func FuzzDecodeInputs(f *testing.F) {
	rich, _ := fuzzSchema(f)

	for _, depth := range []int{0, 1, fuzzMaxDecodeDepth - 1, fuzzMaxDecodeDepth, fuzzMaxDecodeDepth + 1, 64, 300} {
		for _, width := range []int{0, 1, fuzzMaxDecodeElements - 1, fuzzMaxDecodeElements, fuzzMaxDecodeElements + 1} {
			f.Add([]byte{}, uint16(depth), uint16(width))
		}
	}
	// Shapes the hand-found defects turned on (#1442, #1456): numbers at the edge
	// of each conversion table's range, a nested message, a null and a bytes value
	// where a string is wanted, and a list of nulls.
	f.Add([]byte{1, 2, 3, 1, 6, 4, 9, 2, 1, 2, 3, 10, 1, 4, 2, 2, 1, 2, 2, 6}, uint16(2), uint16(2))
	f.Add([]byte("\x03\x0a\x05\x02\x01\x0b\x00\x06\x07\x0c\x08"), uint16(0), uint16(0))
	f.Add([]byte("\x00\x00\x00\x00\x08\x09\x00\x00\x00\x08\x09\x00\x00\x00"), uint16(1), uint16(1))
	f.Add([]byte("\x05\x0a\x03\x0a\x0b\x0a\x0b\x0a\x0b\x0a\x0b\x0a\x0b"), uint16(8), uint16(8))

	f.Fuzz(func(t *testing.T, data []byte, depth uint16, width uint16) {
		g := newFuzzGen(data, rich)
		inputs := map[string]*flowstatev1.Value{}
		fields := rich.Fields()
		for i := range fields.Len() {
			if g.next()%3 == 0 {
				continue
			}
			inputs[string(fields.Get(i).Name())] = g.value()
		}

		before := cloneInputs(inputs)
		decoded := dynamicpb.NewMessage(rich)
		err := DecodeInputs(inputs, decoded)
		requireUnchanged(t, before, inputs)
		if err != nil {
			if !acceptableDecodeRefusal(err) {
				t.Fatalf("DecodeInputs refused with an unclassified error: %v", err)
			}
		} else {
			checkWithinDecodeBounds(t, decoded)
		}

		// The round trip covers every field, value-typed ones included.
		if err == nil {
			checkRoundTrip(t, decoded, rich)
		}

		// The nested chain, alone, so its verdict is exact. A chain of n messages
		// is n deep, and the bound admits 16.
		n := min(int(depth), fuzzMaxChain)
		if n > 0 {
			chained := dynamicpb.NewMessage(rich)
			err := DecodeInputs(map[string]*flowstatev1.Value{"node": fuzzLiteralValue(fuzzChain(n))}, chained)
			switch {
			case n <= fuzzMaxDecodeDepth && err != nil:
				t.Fatalf("a chain of %d messages is within the %d-deep bound but was refused: %v", n, fuzzMaxDecodeDepth, err)
			case n > fuzzMaxDecodeDepth && err == nil:
				t.Fatalf("a chain of %d messages passed a %d-deep bound", n, fuzzMaxDecodeDepth)
			case n > fuzzMaxDecodeDepth:
				if !classifiedInvalid(err) || !strings.Contains(err.Error(), strconv.Itoa(fuzzMaxDecodeDepth)+" deep") {
					t.Fatalf("a chain of %d was refused, but not as InvalidInput naming the depth: %v", n, err)
				}
				if chained.Has(rich.Fields().ByName("node")) {
					t.Fatalf("a chain of %d was refused and the field was set anyway", n)
				}
			}
		}

		// The wide list, alone.
		w := int(width) % (fuzzMaxDecodeElements * 3)
		wide := dynamicpb.NewMessage(rich)
		err = DecodeInputs(map[string]*flowstatev1.Value{"ri": fuzzLiteralValue(fuzzInts(w))}, wide)
		switch {
		case w <= fuzzMaxDecodeElements && err != nil:
			t.Fatalf("%d entries is within the %d bound but was refused: %v", w, fuzzMaxDecodeElements, err)
		case w > fuzzMaxDecodeElements && !classifiedInvalid(err):
			t.Fatalf("%d entries passed or was refused unclassified (bound %d): %v", w, fuzzMaxDecodeElements, err)
		case w > fuzzMaxDecodeElements && wide.Mutable(rich.Fields().ByName("ri")).List().Len() != 0:
			t.Fatalf("%d entries was refused but %d were set", w, wide.Get(rich.Fields().ByName("ri")).List().Len())
		}
	})
}

func cloneInputs(in map[string]*flowstatev1.Value) map[string]*flowstatev1.Value {
	out := make(map[string]*flowstatev1.Value, len(in))
	for k, v := range in {
		out[k] = proto.Clone(v).(*flowstatev1.Value)
	}
	return out
}

func requireUnchanged(t *testing.T, before, after map[string]*flowstatev1.Value) {
	t.Helper()
	if len(before) != len(after) {
		t.Fatalf("DecodeInputs changed the number of inputs from %d to %d", len(before), len(after))
	}
	for k, want := range before {
		if !proto.Equal(want, after[k]) {
			t.Fatalf("DecodeInputs modified input %q", k)
		}
	}
}

// checkWithinDecodeBounds walks an accepted message and holds it to the limits
// SetLiteralField documents. Message-typed fields that carry a cel Value whole
// are leaves: their size is the caller's, not a conversion's.
func checkWithinDecodeBounds(t *testing.T, msg protoreflect.Message) {
	t.Helper()
	nodes := 0
	var walk func(m protoreflect.Message, depth int)
	walk = func(m protoreflect.Message, depth int) {
		if depth > fuzzMaxDecodeDepth {
			t.Fatalf("an accepted message nests %d messages deep, past %d", depth, fuzzMaxDecodeDepth)
		}
		m.Range(func(fd protoreflect.FieldDescriptor, v protoreflect.Value) bool {
			visit := func(fd protoreflect.FieldDescriptor, v protoreflect.Value) {
				nodes++
				if fd.Kind() == protoreflect.MessageKind && !isValueMessage(fd.Message().FullName()) {
					walk(v.Message(), depth+1)
				}
			}
			switch {
			case fd.IsMap():
				if v.Map().Len() > fuzzMaxDecodeElements {
					t.Fatalf("an accepted map %s holds %d entries, past %d", fd.Name(), v.Map().Len(), fuzzMaxDecodeElements)
				}
				v.Map().Range(func(_ protoreflect.MapKey, e protoreflect.Value) bool {
					visit(fd.MapValue(), e)
					return true
				})
			case fd.IsList():
				if v.List().Len() > fuzzMaxDecodeElements {
					t.Fatalf("an accepted list %s holds %d elements, past %d", fd.Name(), v.List().Len(), fuzzMaxDecodeElements)
				}
				for i := range v.List().Len() {
					visit(fd, v.List().Get(i))
				}
			default:
				visit(fd, v)
			}
			return true
		})
	}
	walk(msg, 1)
	if nodes > fuzzMaxDecodeNodes {
		t.Fatalf("an accepted message holds %d values, past %d", nodes, fuzzMaxDecodeNodes)
	}
}

// checkRoundTrip encodes an accepted message and decodes the result, which has
// to rebuild it. A null stands for an absent field on the way in, so it is left
// out, as a workflow reading the output and passing it on would.
func checkRoundTrip(t *testing.T, decoded protoreflect.Message, rich protoreflect.MessageDescriptor) {
	t.Helper()

	outputs, err := EncodeOutputs(decoded.Interface())
	if err != nil {
		// The one refusal an accepted message can meet is a well-known type, and
		// the schema's only one is ts, which DecodeInputs refuses first.
		t.Fatalf("EncodeOutputs refused a message DecodeInputs accepted: %v", err)
	}

	again := map[string]*flowstatev1.Value{}
	for name, value := range outputs.GetNamedValues() {
		literal := value.GetLiteral()
		if literal == nil {
			t.Fatalf("output %q is not a literal: %v", name, value)
		}
		if _, null := literal.GetKind().(*expr.Value_NullValue); null {
			continue
		}
		again[name] = value
	}

	rebuilt := dynamicpb.NewMessage(rich)
	if err := DecodeInputs(again, rebuilt); err != nil {
		t.Fatalf("an encoded message does not decode: %v", err)
	}

	// A proto3 optional field that was never set encodes as its zero value, on
	// purpose (see [encodeFieldValue]), so it comes back present and empty. That
	// is the documented loss of presence, not a difference to report.
	if opt := rich.Fields().ByName("opt"); !decoded.Has(opt) && rebuilt.Has(opt) && rebuilt.Get(opt).String() == "" {
		rebuilt.Clear(opt)
	}
	if !proto.Equal(decoded.Interface(), rebuilt.Interface()) {
		t.Fatalf("encode then decode changed the message:\n got %v\nwant %v", rebuilt, decoded)
	}
}

// FuzzEncodeOutputs fuzzes [EncodeOutputs], the bridge from a plugin's typed
// message back to the values a step produces.
//
// data fills a message of the rich descriptor with every field kind; chain
// nests a Node that many messages deep, since a message type may refer to itself
// and the data a task returns is shaped by whatever it read.
//
// The invariants under fuzz:
//
//   - No panic, and a refusal returns no outputs, so a partial result is never
//     handed back beside an error.
//   - An accepted message yields exactly one literal value per field, named by
//     the field.
//   - The nesting bound holds exactly: a Node chain of at most 32 messages is
//     encoded and a longer one is refused, naming the bound.
//   - A message holding a well-known type is refused naming the field, because
//     what a timestamp is on the workflow side is undecided (#1436).
//   - What encodes decodes back to the same message when the message is within
//     the decode bounds (the round trip is checked inside [FuzzDecodeInputs]
//     for what the generator builds; here, for what a typed message holds).
func FuzzEncodeOutputs(f *testing.F) {
	rich, _ := fuzzSchema(f)

	for _, chain := range []int{0, 1, fuzzMaxEncodeDepth - 1, fuzzMaxEncodeDepth, fuzzMaxEncodeDepth + 1, 100, 2000} {
		f.Add([]byte{}, uint16(chain), false)
		f.Add([]byte("seed\x01\x02\x03\x04\x05\x06\x07\x08\x09\x0a"), uint16(chain), true)
	}
	f.Add([]byte{0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff}, uint16(3), false)

	f.Fuzz(func(t *testing.T, data []byte, chain uint16, withTimestamp bool) {
		g := newFuzzGen(data, rich)
		msg := dynamicpb.NewMessage(rich)
		populate(g, msg)

		n := min(int(chain), fuzzMaxChain)
		if n > 0 {
			msg.Set(rich.Fields().ByName("node"), protoreflect.ValueOfMessage(nodeChain(rich, n)))
		}
		if withTimestamp {
			ts := rich.Fields().ByName("ts")
			stamp := msg.Mutable(ts).Message()
			stamp.Set(ts.Message().Fields().ByName("seconds"), protoreflect.ValueOfInt64(int64(g.word()>>20)))
		}

		start := time.Now()
		outputs, err := EncodeOutputs(msg.Interface())
		if elapsed := time.Since(start); elapsed > 10*time.Second {
			t.Fatalf("EncodeOutputs took %s on %d chained messages", elapsed, n)
		}

		// Fields encode in declaration order and node comes before ts, so a chain
		// past the bound is the refusal that is seen when both are present.
		switch {
		case n > fuzzMaxEncodeDepth:
			if err == nil || !strings.Contains(err.Error(), strconv.Itoa(fuzzMaxEncodeDepth)+" deep") {
				t.Fatalf("a chain of %d messages passed or was refused without naming the %d bound: %v", n, fuzzMaxEncodeDepth, err)
			}
		case withTimestamp:
			if err == nil || !strings.Contains(err.Error(), `"ts"`) {
				t.Fatalf("a timestamp was not refused by name: %v", err)
			}
		default:
			if err != nil {
				t.Fatalf("a message within every bound was refused: %v", err)
			}
		}

		if err != nil {
			if outputs != nil {
				t.Fatalf("EncodeOutputs returned outputs beside the error %v", err)
			}
			return
		}

		fields := rich.Fields()
		if len(outputs.GetNamedValues()) != fields.Len() {
			t.Fatalf("got %d outputs for %d fields", len(outputs.GetNamedValues()), fields.Len())
		}
		for i := range fields.Len() {
			name := string(fields.Get(i).Name())
			if outputs.GetNamedValues()[name].GetLiteral() == nil {
				t.Fatalf("output %q is not a literal: %v", name, outputs.GetNamedValues()[name])
			}
		}

		// The round trip, for what the decode bounds admit.
		if n <= fuzzMaxDecodeDepth {
			checkRoundTrip(t, msg, rich)
		}
	})
}

// nodeChain builds a Node that is n messages deep through child.
func nodeChain(rich protoreflect.MessageDescriptor, n int) protoreflect.Message {
	nodeDesc := rich.Fields().ByName("node").Message()
	child := nodeDesc.Fields().ByName("child")
	name := nodeDesc.Fields().ByName("name")

	head := dynamicpb.NewMessage(nodeDesc)
	tail := head
	tail.Set(name, protoreflect.ValueOfString("0"))
	for i := 1; i < n; i++ {
		next := dynamicpb.NewMessage(nodeDesc)
		next.Set(name, protoreflect.ValueOfString(strconv.Itoa(i)))
		tail.Set(child, protoreflect.ValueOfMessage(next))
		tail = next
	}
	return head
}

// populate fills a Rich message from the generator with values its fields can
// hold, so a refusal on the way out is the encoder's and never the setter's.
func populate(g *fuzzGen, msg *dynamicpb.Message) {
	fields := msg.Descriptor().Fields()
	for i := range fields.Len() {
		fd := fields.Get(i)
		if fd.Name() == "ts" || g.next()%3 == 0 {
			continue
		}
		switch {
		case fd.IsMap():
			m := msg.Mutable(fd).Map()
			for range int(g.next() % 4) {
				m.Set(protoreflect.ValueOfString(g.text()).MapKey(), fuzzElement(g, fd.MapValue(), m.NewValue))
			}
		case fd.IsList():
			l := msg.Mutable(fd).List()
			for range int(g.next() % 4) {
				l.Append(fuzzElement(g, fd, l.NewElement))
			}
		default:
			msg.Set(fd, fuzzElement(g, fd, func() protoreflect.Value { return msg.NewField(fd) }))
		}
	}
}

// fuzzElement builds one value of a field's type. An enum takes a number the
// schema defines and a uint64 one over its whole range.
func fuzzElement(g *fuzzGen, fd protoreflect.FieldDescriptor, fresh func() protoreflect.Value) protoreflect.Value {
	switch fd.Kind() {
	case protoreflect.StringKind:
		return protoreflect.ValueOfString(g.text())
	case protoreflect.BytesKind:
		return protoreflect.ValueOfBytes([]byte(g.text()))
	case protoreflect.BoolKind:
		return protoreflect.ValueOfBool(g.next()&1 == 1)
	case protoreflect.Int32Kind, protoreflect.Sint32Kind, protoreflect.Sfixed32Kind:
		return protoreflect.ValueOfInt32(int32(g.integer()))
	case protoreflect.Int64Kind, protoreflect.Sint64Kind, protoreflect.Sfixed64Kind:
		return protoreflect.ValueOfInt64(g.integer())
	case protoreflect.Uint32Kind, protoreflect.Fixed32Kind:
		return protoreflect.ValueOfUint32(uint32(g.integer()))
	case protoreflect.Uint64Kind, protoreflect.Fixed64Kind:
		return protoreflect.ValueOfUint64(fuzzUnsigned(g))
	case protoreflect.FloatKind:
		return protoreflect.ValueOfFloat32(float32(g.integer()) / 3)
	case protoreflect.DoubleKind:
		return protoreflect.ValueOfFloat64(g.double())
	case protoreflect.EnumKind:
		return protoreflect.ValueOfEnum(protoreflect.EnumNumber(g.next() % 3))
	case protoreflect.MessageKind:
		node := fresh().Message()
		// A value-typed field holds a literal that is not null: an empty or null
		// one encodes as null, which decodes back as an absent field, the same
		// documented loss of presence as an unset optional.
		if full := fd.Message().FullName(); isValueMessage(full) {
			literal := g.literal(0)
			if _, null := literal.GetKind().(*expr.Value_NullValue); null {
				literal = &expr.Value{Kind: &expr.Value_StringValue{StringValue: g.text()}}
			}
			var held proto.Message = literal
			if full == flowstateValueName {
				held = fuzzLiteralValue(literal)
			}
			wire, err := proto.Marshal(held)
			if err != nil {
				// Random bytes are not always valid UTF-8, which a message that
				// holds the literal cannot carry; a plain string stands in.
				held = &expr.Value{Kind: &expr.Value_StringValue{StringValue: "x"}}
				if full == flowstateValueName {
					held = fuzzLiteralValue(held.(*expr.Value))
				}
				wire, err = proto.Marshal(held)
			}
			if err == nil {
				err = proto.Unmarshal(wire, node.Interface())
			}
			if err != nil {
				panic("fuzz value does not round trip through the wire: " + err.Error())
			}
			return protoreflect.ValueOfMessage(node)
		}
		if name := node.Descriptor().Fields().ByName("name"); name != nil {
			node.Set(name, protoreflect.ValueOfString(g.text()))
		}
		return protoreflect.ValueOfMessage(node)
	}
	panic("fuzz schema has a kind fuzzElement does not build: " + fd.Kind().String())
}

// fuzzUnsigned is an unsigned 64-bit value over the whole range, including the
// half above [math.MaxInt64] that a signed read cannot hold.
func fuzzUnsigned(g *fuzzGen) uint64 {
	if b := g.next(); b < 4 {
		return [...]uint64{1 << 63, math.MaxUint64, math.MaxInt64, 0}[b]
	}
	return g.word()
}

// goValue builds a Go value of the kinds [Literal] documents, from fuzz bytes:
// maps, slices, and the ordinary scalars, nested depth deep along one branch.
func (g *fuzzGen) goValue(depth int) any {
	g.nodes++
	if g.nodes > fuzzMaxGenNodes || depth > 24 {
		return nil
	}
	switch g.next() % 16 {
	case 0:
		return nil
	case 1:
		return g.next()&1 == 1
	case 2:
		return int(g.integer())
	case 3:
		return g.integer()
	case 4:
		return int32(g.integer())
	case 5:
		return uint8(g.next())
	case 6:
		return uint32(g.word())
	case 7:
		return g.double()
	case 8:
		return float32(g.double())
	case 9:
		return g.text()
	case 10:
		return []byte(g.text())
	case 11:
		out := make([]any, int(g.next()%5))
		for i := range out {
			out[i] = g.goValue(depth + 1)
		}
		return out
	case 13:
		return fuzzUnsigned(g)
	case 14:
		return errors.New("fuzz")
	case 15:
		return make(chan int)
	default:
		out := make(map[string]any, 4)
		for range int(g.next() % 5) {
			out[g.text()] = g.goValue(depth + 1)
		}
		return out
	}
}

// FuzzLiteral fuzzes [Literal], which builds the value of an output whose type
// is not fixed from whatever Go value a plugin hands it.
//
// data builds a value of the documented kinds; nest wraps it in that many
// single-element lists and maps, since the doc promises "nested to any depth".
//
// The invariants under fuzz:
//
//   - No panic, at any depth the fuzzer reaches.
//   - Every input yields a non-nil literal, an error value for the kinds Literal
//     cannot represent (an error, a channel, a uint64 above MaxInt64), and the
//     literal keeps the shape of its input: the same lengths of every list, the same keys of every
//     map, and an integer, string, bool or bytes holding what went in.
//   - The result is usable as an output: EncodeOutputs of a message carrying it
//     succeeds and returns it unchanged, and DecodeInputs of it into a cel Value
//     field succeeds.
func FuzzLiteral(f *testing.F) {
	rich, _ := fuzzSchema(f)

	for _, nest := range []int{0, 1, 16, 17, 33, 1000} {
		f.Add([]byte{}, uint16(nest))
		f.Add([]byte("\x0b\x03\x09\x02a\x0c\x02\x09\x01b\x03\x01\x09\x01c\x0a\x03xyz"), uint16(nest))
	}
	f.Add([]byte{3, 0, 3, 1, 3, 2, 3, 3, 3, 4, 3, 5, 3, 6, 3, 7, 3, 8, 3, 9, 3, 10, 3, 11, 3, 12, 3, 13}, uint16(0))
	f.Add([]byte{7, 0, 7, 1, 7, 2, 7, 3, 7, 4, 7, 5}, uint16(0))

	f.Fuzz(func(t *testing.T, data []byte, nest uint16) {
		g := newFuzzGen(data, rich)
		input := g.goValue(0)
		for range min(int(nest), fuzzMaxChain) {
			if g.next()&1 == 0 {
				input = []any{input}
			} else {
				input = map[string]any{"k": input}
			}
		}

		got := Literal(input)
		if got == nil {
			t.Fatalf("Literal(%T) returned nil", input)
		}
		checkLiteralShape(t, input, got)

		// A generated message, which is what a plugin returns: its cel Value field
		// is the *expr.Value EncodeOutputs recognises.
		outputs, err := EncodeOutputs(&flowstatev1.Task_HTTP_Outputs{Json: got})
		if err != nil {
			t.Fatalf("EncodeOutputs refused a Literal: %v", err)
		}
		if out := outputs.GetNamedValues()["json"].GetLiteral(); !proto.Equal(out, got) {
			t.Fatalf("EncodeOutputs changed a Literal:\n got %v\nwant %v", out, got)
		}

		into := dynamicpb.NewMessage(rich)
		if err := DecodeInputs(map[string]*flowstatev1.Value{"any": fuzzLiteralValue(got), "val": fuzzLiteralValue(got)}, into); err != nil {
			t.Fatalf("DecodeInputs refused a Literal into a cel Value field: %v", err)
		}
	})
}

// requireErrorLiteral requires the error value Literal yields for what it cannot
// represent: an object holding a flowstate error, never nil and never null.
func requireErrorLiteral(t *testing.T, in any, out *expr.Value) {
	t.Helper()

	object, ok := out.GetKind().(*expr.Value_ObjectValue)
	if !ok {
		t.Fatalf("%T %v became %v, want an error value", in, in, out)
	}
	var failure flowstatev1.Value_Error
	if err := object.ObjectValue.UnmarshalTo(&failure); err != nil || failure.GetMessage() == "" {
		t.Fatalf("%T became %v, which is not an error value with a message (%v)", in, out, err)
	}
}

// checkLiteralShape compares a literal with the Go value it was built from.
func checkLiteralShape(t *testing.T, in any, out *expr.Value) {
	t.Helper()

	switch v := in.(type) {
	case nil:
		if _, ok := out.GetKind().(*expr.Value_NullValue); !ok {
			t.Fatalf("nil became %v", out)
		}
	case bool:
		if _, ok := out.GetKind().(*expr.Value_BoolValue); !ok || out.GetBoolValue() != v {
			t.Fatalf("%v became %v", v, out)
		}
	case string:
		if _, ok := out.GetKind().(*expr.Value_StringValue); !ok || out.GetStringValue() != v {
			t.Fatalf("%q became %v", v, out)
		}
	case []byte:
		if _, ok := out.GetKind().(*expr.Value_BytesValue); !ok || string(out.GetBytesValue()) != string(v) {
			t.Fatalf("%q became %v", v, out)
		}
	case int, int32, int64, uint8, uint32:
		if _, ok := out.GetKind().(*expr.Value_Int64Value); !ok {
			t.Fatalf("%T %v became %v", v, v, out)
		}
	case uint64:
		if v > math.MaxInt64 {
			requireErrorLiteral(t, v, out)
		} else if got, ok := out.GetKind().(*expr.Value_Int64Value); !ok || uint64(got.Int64Value) != v {
			t.Fatalf("uint64 %v became %v", v, out)
		}
	case error, chan int:
		requireErrorLiteral(t, v, out)
	case float32, float64:
		if _, ok := out.GetKind().(*expr.Value_DoubleValue); !ok {
			t.Fatalf("%T %v became %v", v, v, out)
		}
	case []any:
		list, ok := out.GetKind().(*expr.Value_ListValue)
		if !ok || len(list.ListValue.GetValues()) != len(v) {
			t.Fatalf("a list of %d became %v", len(v), out)
		}
		for i := range v {
			checkLiteralShape(t, v[i], list.ListValue.Values[i])
		}
	case map[string]any:
		m, ok := out.GetKind().(*expr.Value_MapValue)
		if !ok || len(m.MapValue.GetEntries()) != len(v) {
			t.Fatalf("a map of %d became %v", len(v), out)
		}
		for _, entry := range m.MapValue.Entries {
			want, found := v[entry.GetKey().GetStringValue()]
			if !found {
				t.Fatalf("key %v appeared from nowhere in %v", entry.GetKey(), v)
			}
			checkLiteralShape(t, want, entry.GetValue())
		}
	default:
		t.Fatalf("goValue built a %T the check does not know", in)
	}
}

// TestFuzzBoundsAreTheDocumentedOnes pins the restated limits to the behaviour,
// at each boundary and one either side, so a fuzz target that quietly stopped
// asserting the real bound cannot pass.
func TestFuzzBoundsAreTheDocumentedOnes(t *testing.T) {
	rich, _ := fuzzSchema(t)
	decode := func(name string, l *expr.Value) error {
		return DecodeInputs(map[string]*flowstatev1.Value{name: fuzzLiteralValue(l)}, dynamicpb.NewMessage(rich))
	}

	for _, n := range []int{fuzzMaxDecodeDepth - 1, fuzzMaxDecodeDepth} {
		if err := decode("node", fuzzChain(n)); err != nil {
			t.Errorf("a chain of %d messages: %v", n, err)
		}
	}
	err := decode("node", fuzzChain(fuzzMaxDecodeDepth+1))
	if !classifiedInvalid(err) || !strings.Contains(err.Error(), "16 deep") {
		t.Errorf("a chain of %d messages: want InvalidInput naming the depth, got %v", fuzzMaxDecodeDepth+1, err)
	}

	if err := decode("ri", fuzzInts(fuzzMaxDecodeElements)); err != nil {
		t.Errorf("%d elements: %v", fuzzMaxDecodeElements, err)
	}
	if err := decode("ri", fuzzInts(fuzzMaxDecodeElements+1)); !classifiedInvalid(err) {
		t.Errorf("%d elements: want InvalidInput, got %v", fuzzMaxDecodeElements+1, err)
	}

	// The total bound is on values converted across the whole input, not per
	// list: each rn element carries a kids list of 1024 empty messages, so about
	// 64 elements spend the 65536 values although no list is over 1024. The
	// elements share one kids literal, so the input costs one list to build.
	kids := &expr.ListValue{}
	for range fuzzMaxDecodeElements {
		kids.Values = append(kids.Values, &expr.Value{Kind: &expr.Value_MapValue{MapValue: &expr.MapValue{}}})
	}
	withKids := func(elements int) error {
		element := &expr.Value{Kind: &expr.Value_MapValue{MapValue: &expr.MapValue{Entries: []*expr.MapValue_Entry{{
			Key:   &expr.Value{Kind: &expr.Value_StringValue{StringValue: "kids"}},
			Value: &expr.Value{Kind: &expr.Value_ListValue{ListValue: kids}},
		}}}}}
		list := &expr.ListValue{}
		for range elements {
			list.Values = append(list.Values, element)
		}
		return decode("rn", &expr.Value{Kind: &expr.Value_ListValue{ListValue: list}})
	}
	if err := withKids(fuzzMaxDecodeNodes/(fuzzMaxDecodeElements+1) - 1); err != nil {
		t.Errorf("an input under the total bound: %v", err)
	}
	if err := withKids(fuzzMaxDecodeElements); !classifiedInvalid(err) || !strings.Contains(err.Error(), "65536") {
		t.Errorf("an input over the total bound: want InvalidInput naming it, got %v", err)
	}

	// EncodeOutputs's own bound.
	enc := func(n int) error {
		msg := dynamicpb.NewMessage(rich)
		msg.Set(rich.Fields().ByName("node"), protoreflect.ValueOfMessage(nodeChain(rich, n)))
		_, err := EncodeOutputs(msg.Interface())
		return err
	}
	if err := enc(fuzzMaxEncodeDepth); err != nil {
		t.Errorf("an output chain of %d: %v", fuzzMaxEncodeDepth, err)
	}
	if err := enc(fuzzMaxEncodeDepth + 1); err == nil {
		t.Errorf("an output chain of %d passed a %d bound", fuzzMaxEncodeDepth+1, fuzzMaxEncodeDepth)
	}
}

// TestHostileValuesTerminateQuickly feeds the decoder and encoder shapes far
// past every bound and requires an answer in bounded time: the depth bound has
// to stop the walk, not merely report after it.
func TestHostileValuesTerminateQuickly(t *testing.T) {
	rich, _ := fuzzSchema(t)

	start := time.Now()
	err := DecodeInputs(map[string]*flowstatev1.Value{"node": fuzzLiteralValue(fuzzChain(20_000))}, dynamicpb.NewMessage(rich))
	if !classifiedInvalid(err) {
		t.Fatalf("a chain 20000 deep: want InvalidInput, got %v", err)
	}
	if elapsed := time.Since(start); elapsed > 5*time.Second {
		t.Fatalf("a chain 20000 deep took %s to refuse", elapsed)
	}

	start = time.Now()
	err = DecodeInputs(map[string]*flowstatev1.Value{"ri": fuzzLiteralValue(fuzzInts(500_000))}, dynamicpb.NewMessage(rich))
	if !classifiedInvalid(err) {
		t.Fatalf("500000 entries: want InvalidInput, got %v", err)
	}
	if elapsed := time.Since(start); elapsed > 5*time.Second {
		t.Fatalf("500000 entries took %s to refuse", elapsed)
	}

	start = time.Now()
	msg := dynamicpb.NewMessage(rich)
	msg.Set(rich.Fields().ByName("node"), protoreflect.ValueOfMessage(nodeChain(rich, 20_000)))
	if _, err := EncodeOutputs(msg.Interface()); err == nil {
		t.Fatal("an output chain 20000 deep was encoded")
	}
	if elapsed := time.Since(start); elapsed > 5*time.Second {
		t.Fatalf("an output chain 20000 deep took %s to refuse", elapsed)
	}
}
