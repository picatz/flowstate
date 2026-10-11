package sdk

import (
	"errors"
	"fmt"
	"strings"
	"unicode/utf8"

	expr "google.golang.org/genproto/googleapis/api/expr/v1alpha1"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protoreflect"
	"google.golang.org/protobuf/types/known/anypb"

	"github.com/picatz/flowstate/internal/textbound"
	flowstatev1 "github.com/picatz/flowstate/pkg/flowstate/v1"
)

// DecodeInputs fills a message from a task's inputs, matching each field by
// name.
//
// A task declares its input schema as a message, and this is how it gets one: a
// plugin's task function receives the named values the engine sends and turns
// them into the typed message the task was written against.
//
//	func greet(ctx context.Context, inputs map[string]*flowstatev1.Value, _ *flowstatev1.Scope) (*flowstatev1.Node_Outputs, error) {
//		var in examplev1.GreetInputs
//		if err := sdk.DecodeInputs(inputs, &in); err != nil {
//			return nil, err
//		}
//		...
//	}
//
// A field may be one of the task's own messages, a list of them, or a map of
// them. It is filled from a map literal keyed by field name, a oneof by naming
// the member it holds, with depth, entry and total-value bounds; see
// [flowstatev1.SetLiteralField]. Inside a nested message an unknown key is
// refused by name, since a misspelling there would otherwise drop what the author
// wrote. Protobuf's well-known types are still not converted.
//
// An input the message has no field for is ignored rather than refused, so that
// a workflow written against a newer version of a task does not fail against an
// older plugin. An input whose expression the engine has not resolved is
// refused, since the plugin has no way to evaluate one it did not declare as
// deferred.
//
// A field that claims a plugin credential with `(flowstate.v1.input).credential`
// is held to the contract the claim makes: it must arrive as the string the host
// resolved the author's reference to. Anything else, an unresolved reference
// included, is refused before the field is filled, unclassified because it is
// the host's delivery that failed and not the workflow's input.
//
// A value that does not fit its field is the workflow's mistake and never a
// transient one, so that error comes back already classified as [InvalidInput]
// and is returned as it is: wrapping it in [InvalidInput] again is harmless but
// redundant, and a task that returned it bare used to reach the host as an
// unclassified failure, which named the task rather than its input as the
// cause. The cause is wrapped, so [errors.Is] and [errors.As] reach through the
// classification to it.
//
// Not every refusal is the workflow's. A field of a kind this cannot fill, a
// map keyed by something other than strings, a typed field the task itself
// declared deferred so the engine forwarded the expression, or a nil message,
// is the task's own declaration disagreeing with what it asked to decode into;
// blaming the input would send a workflow's error dispatch down the wrong
// branch. Those stay unclassified, which the host records as the permanent
// failure they are, and the message says what the plugin author changes.
func DecodeInputs(inputs map[string]*flowstatev1.Value, msg proto.Message) error {
	if msg == nil {
		return fmt.Errorf("sdk: DecodeInputs needs a message to fill")
	}

	reflectMsg := msg.ProtoReflect()
	fields := reflectMsg.Descriptor().Fields()

	// A credential-claimed field the inputs leave out is refused too: the claim
	// promises the host delivered one, and an omitted key is not that.
	for i := range fields.Len() {
		field := fields.Get(i)
		if _, supplied := inputs[string(field.Name())]; !supplied {
			if err := checkCredentialValue(field, nil); err != nil {
				return fmt.Errorf("input %q: %w", field.Name(), err)
			}
		}
	}

	for name, value := range inputs {
		field := fields.ByName(protoreflect.Name(name))
		if field == nil {
			continue
		}

		if err := checkCredentialValue(field, value); err != nil {
			return fmt.Errorf("input %q: %w", name, err)
		}

		if err := setField(reflectMsg, field, value); err != nil {
			if errors.Is(err, errTaskDeclaration) {
				return fmt.Errorf("input %q: %w", name, err)
			}
			return InvalidInput("input %q: %w", name, err)
		}
	}

	return nil
}

// checkCredentialValue refuses, for a field that claims a plugin credential
// with `(flowstate.v1.input).credential`, anything but the string the host
// resolved the author's reference to. A reference still unresolved, a number, a
// structure or an absent value means the host did not deliver what the claim
// promised, so the field is not filled with it.
//
// Unclassified, like a task's own declaration disagreeing with the wire: the
// workflow did nothing wrong that its admission would not have refused, so
// blaming its input would send the failure down the wrong branch. The message
// names the input and the kind that arrived and never the value.
func checkCredentialValue(field protoreflect.FieldDescriptor, value *flowstatev1.Value) error {
	options, _ := proto.GetExtension(field.Options(), flowstatev1.E_Input).(*flowstatev1.InputOptions)
	credential := options.GetCredential()
	if credential == "" {
		return nil
	}

	if _, isString := value.GetLiteral().GetKind().(*expr.Value_StringValue); !isString {
		return fmt.Errorf("claims credential %q and must hold the string the host resolved, but holds %s; "+
			"the host resolves a secret reference before dispatch, so this was not delivered as one",
			credential, describeKind(value))
	}

	return nil
}

// describeKind names the arm a value holds, for a message that must not carry
// the value.
func describeKind(value *flowstatev1.Value) string {
	switch kind := value.GetKind().(type) {
	case *flowstatev1.Value_Literal:
		return fmt.Sprintf("a literal that is not a string (%T)", kind.Literal.GetKind())
	case *flowstatev1.Value_Expr:
		return "an unresolved expression"
	case *flowstatev1.Value_SecretRef:
		return "an unresolved secret reference"
	case *flowstatev1.Value_CredentialRef:
		return "an unresolved credential reference"
	case *flowstatev1.Value_Structure_:
		return "a structure"
	case *flowstatev1.Value_Error_:
		return "an error value"
	default:
		return "no value"
	}
}

// errTaskDeclaration marks a refusal that is about what the task declared
// rather than what the workflow sent: the field cannot hold any value of this
// input's kind, whatever value that is. [DecodeInputs] leaves such an error
// unclassified so the host records a task failure, not an invalid input.
var errTaskDeclaration = flowstatev1.ErrFieldUnsupported

// setField assigns one input to one field.
func setField(msg protoreflect.Message, field protoreflect.FieldDescriptor, value *flowstatev1.Value) error {
	// A field declared as flowstate's own Value type takes the input whole,
	// which is how a task accepts something whose shape it does not constrain —
	// including an expression it declared deferred, or a secret reference.
	if field.Kind() == protoreflect.MessageKind &&
		field.Message().FullName() == "flowstate.v1.Value" && !field.IsList() && !field.IsMap() {
		msg.Set(field, protoreflect.ValueOfMessage(value.ProtoReflect()))
		return nil
	}

	switch kind := value.GetKind().(type) {
	case *flowstatev1.Value_Literal:
		return flowstatev1.SetLiteralField(msg, field, kind.Literal)
	case *flowstatev1.Value_Expr:
		return fmt.Errorf(
			"is an unresolved expression; the engine resolves inputs before sending them, so this one was declared in DeferredInputs and the task has to evaluate it itself (%w)",
			errTaskDeclaration)
	case *flowstatev1.Value_SecretRef:
		return fmt.Errorf(
			"is a secret reference, which this field's type cannot hold; declare the field as flowstate.v1.Value to receive one (%w)",
			errTaskDeclaration)
	case *flowstatev1.Value_CredentialRef:
		return fmt.Errorf(
			"is a credential reference, which this field's type cannot hold; declare the field as flowstate.v1.Value to receive one (%w)",
			errTaskDeclaration)
	case *flowstatev1.Value_Error_:
		return fmt.Errorf("is an error value: %s", textbound.Truncate(kind.Error.GetMessage(), 256))
	default:
		return fmt.Errorf("has no value")
	}
}

// EncodeOutputs turns a task's output message into the named values a step
// produces, one per field.
//
// Later steps reference them as ${steps.step_id.field_name}, so the field
// names in the output message are the names a workflow author writes.
func EncodeOutputs(msg proto.Message) (*flowstatev1.Node_Outputs, error) {
	if msg == nil {
		return nil, fmt.Errorf("sdk: EncodeOutputs needs a message")
	}

	reflectMsg := msg.ProtoReflect()
	fields := reflectMsg.Descriptor().Fields()
	outputs := &flowstatev1.Node_Outputs{
		NamedValues: make(map[string]*flowstatev1.Value, fields.Len()),
	}

	for i := range fields.Len() {
		field := fields.Get(i)
		name := string(field.Name())

		value, err := encodeField(reflectMsg, field)
		if err != nil {
			return nil, fmt.Errorf("sdk: output %q: %w", name, err)
		}

		outputs.NamedValues[name] = value
	}

	return outputs, nil
}

// encodeField turns one field into a named value.
func encodeField(msg protoreflect.Message, field protoreflect.FieldDescriptor) (*flowstatev1.Value, error) {
	converted, err := encodeFieldValue(msg, field, 0)
	if err != nil {
		return nil, err
	}
	return literalValue(converted), nil
}

// encodeFieldValue turns one field into a CEL literal, whatever its cardinality.
//
// It is shared by the output message's own fields and by the fields of a nested
// message, so a repeated string means the same thing at either depth. depth is
// how many messages have been entered to reach this field, and is what
// [encodeMessage] bounds.
func encodeFieldValue(msg protoreflect.Message, field protoreflect.FieldDescriptor, depth int) (*expr.Value, error) {
	value := msg.Get(field)

	switch {
	case field.IsMap():
		entries := make([]*expr.MapValue_Entry, 0, value.Map().Len())
		var mapErr error
		value.Map().Range(func(key protoreflect.MapKey, element protoreflect.Value) bool {
			converted, err := encodeScalar(field.MapValue(), element, depth)
			if err != nil {
				mapErr = err
				return false
			}
			entries = append(entries, &expr.MapValue_Entry{
				Key:   &expr.Value{Kind: &expr.Value_StringValue{StringValue: key.String()}},
				Value: converted,
			})
			return true
		})
		if mapErr != nil {
			return nil, mapErr
		}
		return &expr.Value{
			Kind: &expr.Value_MapValue{MapValue: &expr.MapValue{Entries: entries}},
		}, nil

	case field.IsList():
		list := value.List()
		values := make([]*expr.Value, 0, list.Len())
		for i := range list.Len() {
			converted, err := encodeScalar(field, list.Get(i), depth)
			if err != nil {
				return nil, err
			}
			values = append(values, converted)
		}
		return &expr.Value{
			Kind: &expr.Value_ListValue{ListValue: &expr.ListValue{Values: values}},
		}, nil

	default:
		// The alternatives of a real oneof that were not chosen are absent, not
		// zero. Emitting "" for every arm nobody took would leave a workflow
		// unable to tell "no error" from "an empty error", and would describe a
		// message as holding several alternatives at once when a oneof holds one.
		//
		// A proto3 `optional` field is a synthetic oneof and deliberately stays on
		// the zero-value path below: its whole point is to be an ordinary field
		// that may be absent, and it is not an alternative to anything.
		if oneof := field.ContainingOneof(); oneof != nil && !oneof.IsSynthetic() && !msg.Has(field) {
			return nullValue(), nil
		}

		// An unset singular message is null rather than a map of its own zero
		// fields. That is what proto3 presence already means, and it is also what
		// makes a self-referential message type terminate: descending into an
		// unset field would otherwise produce another unset field forever.
		if field.Kind() == protoreflect.MessageKind && !isValueMessage(field.Message().FullName()) && !msg.Has(field) {
			return nullValue(), nil
		}
		return encodeScalar(field, value, depth)
	}
}

// encodeScalar turns one field value into a CEL literal.
func encodeScalar(field protoreflect.FieldDescriptor, value protoreflect.Value, depth int) (*expr.Value, error) {
	switch field.Kind() {
	case protoreflect.StringKind:
		return &expr.Value{Kind: &expr.Value_StringValue{StringValue: value.String()}}, nil
	case protoreflect.BytesKind:
		return &expr.Value{Kind: &expr.Value_BytesValue{BytesValue: value.Bytes()}}, nil
	case protoreflect.BoolKind:
		return &expr.Value{Kind: &expr.Value_BoolValue{BoolValue: value.Bool()}}, nil
	case protoreflect.Int32Kind, protoreflect.Int64Kind, protoreflect.Sint32Kind,
		protoreflect.Sint64Kind, protoreflect.Sfixed32Kind, protoreflect.Sfixed64Kind:
		return &expr.Value{Kind: &expr.Value_Int64Value{Int64Value: value.Int()}}, nil
	case protoreflect.Uint32Kind, protoreflect.Uint64Kind, protoreflect.Fixed32Kind, protoreflect.Fixed64Kind:
		return &expr.Value{Kind: &expr.Value_Uint64Value{Uint64Value: value.Uint()}}, nil
	case protoreflect.FloatKind, protoreflect.DoubleKind:
		return &expr.Value{Kind: &expr.Value_DoubleValue{DoubleValue: value.Float()}}, nil
	case protoreflect.EnumKind:
		return &expr.Value{Kind: &expr.Value_Int64Value{Int64Value: int64(value.Enum())}}, nil
	case protoreflect.MessageKind:
		switch field.Message().FullName() {
		// The type this schema already uses to carry a value of unconstrained
		// shape. A task returning structured data — a list of objects, a parsed
		// response body — declares its output as this and is done: there is
		// nothing to convert, because it is already the representation a step
		// output is made of.
		case celValueName:
			// An unset field is an absent output, the same null an unset message of
			// any other type produces, rather than an empty literal that only reads
			// as null by accident of how the engine treats a literal with no kind.
			message := value.Message()
			if !message.IsValid() {
				return nullValue(), nil
			}
			v, err := asMessage[*expr.Value](message)
			if err != nil {
				return nil, err
			}
			return v, nil

		// flowstate's own wrapper around the same thing, provided it holds a
		// literal. An output carrying an unevaluated expression or a secret
		// reference is not an output: the first is something the task was
		// supposed to evaluate, and the second must never become a step output at
		// all, since step outputs are written to workflow history.
		case flowstateValueName:
			v, err := asMessage[*flowstatev1.Value](value.Message())
			if err != nil {
				return nil, err
			}
			if literal := v.GetLiteral(); literal != nil {
				return literal, nil
			}
			// Nothing was set at all, which is an absent output rather than a
			// wrong one — the same null an unset message of any other type
			// produces. The refusal below is for a value that holds something a
			// task was supposed to have resolved first.
			if v.GetKind() == nil {
				return nullValue(), nil
			}
			return nil, fmt.Errorf(
				"holds a %T rather than a value; a task's outputs must be values it computed",
				v.GetKind(),
			)
		}

		// A well-known type is still refused. What a timestamp or a duration is
		// on the workflow side is undecided (#1436), and converting one here
		// would answer that question in this package rather than in the schema —
		// the mistake the paragraph below exists to avoid.
		if wellKnown(field.Message().FullName()) {
			return nil, fmt.Errorf(
				"has message type %s, which EncodeOutputs does not convert. "+
					"Declare the field as %s to return data of any shape, and build it with sdk.Literal — "+
					"for example `Data: sdk.Literal(map[string]any{\"items\": items})`",
				field.Message().FullName(), celValueName,
			)
		}

		// Any other message becomes a map keyed by its own field names.
		//
		// The mapping is the schema's rather than this package's invention: the
		// keys are the descriptor's field names, which is exactly what the
		// engine's built-in bridge already produces for a nested message, so
		// `${steps.log.commits[0].author.name}` reads the same whether the task
		// is built in or shipped by a plugin. That is what makes "the schema is
		// the contract" true for a plugin author who declares a typed nested
		// message rather than an untyped value.
		return encodeMessage(value.Message(), depth+1)
	}

	return nil, fmt.Errorf(
		"has type %s, which EncodeOutputs does not convert; build the named value directly",
		field.Kind(),
	)
}

// encodeMessage turns a nested message into a CEL map keyed by field name.
//
// Every field is encoded, present or not, so a step output has the shape its
// descriptor declares rather than a shape that varies with the data: reading
// `commits[0].old_path` on a commit that was not a rename yields "" instead of
// failing with "no such key". The one exception is a singular message field,
// which is null when unset — see [encodeFieldValue].
func encodeMessage(msg protoreflect.Message, depth int) (*expr.Value, error) {
	if depth > maxOutputMessageDepth {
		return nil, fmt.Errorf(
			"nests messages more than %d deep, which EncodeOutputs does not convert",
			maxOutputMessageDepth,
		)
	}

	fields := msg.Descriptor().Fields()
	entries := make([]*expr.MapValue_Entry, 0, fields.Len())
	for i := range fields.Len() {
		field := fields.Get(i)

		converted, err := encodeFieldValue(msg, field, depth)
		if err != nil {
			return nil, fmt.Errorf("field %q: %w", field.Name(), err)
		}

		entries = append(entries, &expr.MapValue_Entry{
			Key:   &expr.Value{Kind: &expr.Value_StringValue{StringValue: string(field.Name())}},
			Value: converted,
		})
	}

	return &expr.Value{
		Kind: &expr.Value_MapValue{MapValue: &expr.MapValue{Entries: entries}},
	}, nil
}

// maxOutputMessageDepth bounds how far EncodeOutputs descends into nested
// messages.
//
// A message type may refer to itself, and the data a task returns is shaped by
// whatever it read — a repository, a response body — so the nesting is not this
// package's to trust. Presence alone already terminates the unset case; this
// bounds the set one, and is far deeper than a task output any author writes.
const maxOutputMessageDepth = 32

// The two message types a task output may be declared as when its shape is not
// fixed. They are named as constants because the check is by full name — a
// plugin's own copy of a descriptor is a different Go type but the same protobuf
// type, and comparing names is what makes both work.
const (
	celValueName       = "google.api.expr.v1alpha1.Value"
	flowstateValueName = "flowstate.v1.Value"
)

// asMessage returns a message as the generated type T. A generated message is
// returned as it is; any other implementation of the same protobuf type, such
// as a dynamicpb message, is copied through its wire form, which is what makes
// the match by full name the comment above promises rather than by Go type.
func asMessage[T proto.Message](message protoreflect.Message) (T, error) {
	var zero T
	if v, ok := message.Interface().(T); ok {
		return v, nil
	}
	wire, err := proto.Marshal(message.Interface())
	if err != nil {
		return zero, fmt.Errorf("holds a %s that cannot be read: %w", message.Descriptor().FullName(), err)
	}
	out := zero.ProtoReflect().New().Interface().(T)
	if err := proto.Unmarshal(wire, out); err != nil {
		return zero, fmt.Errorf("holds a %s that cannot be read: %w", message.Descriptor().FullName(), err)
	}
	return out, nil
}

// isValueMessage reports whether a message type carries a value directly, rather
// than being a message this package converts field by field.
func isValueMessage(name protoreflect.FullName) bool {
	return name == celValueName || name == flowstateValueName
}

// wellKnown reports whether a message type is one of protobuf's own.
//
// These are refused rather than converted: each has a meaning outside its
// fields — a timestamp is an instant, not a {seconds, nanos} pair — and what
// they become on the workflow side is #1436's to decide, once, for every
// boundary rather than here for one.
func wellKnown(name protoreflect.FullName) bool {
	return strings.HasPrefix(string(name), "google.protobuf.")
}

// Literal builds a value of any shape, for a task output whose type is not fixed.
//
// It is the companion to declaring an output field as
// google.api.expr.v1alpha1.Value: that says "this output can be anything", and
// this is how a plugin says what it is this time. Maps, slices, and the ordinary
// scalars all work, nested to any depth:
//
//	outputs, err := sdk.EncodeOutputs(&examplev1.QueryOutputs{
//		Rows:  sdk.Literal([]any{map[string]any{"id": 1, "name": "a"}}),
//		Count: 1,
//	})
//
// A workflow then reads it the way it reads anything else —
// `${steps.query.rows[0].name}` — with no step in between to parse it.
//
// A type it cannot represent, a channel, an error, or an unsigned integer above
// [math.MaxInt64], yields an error value rather than a panic or a nil, so a
// plugin that hands it something unexpected produces a diagnosable output
// instead of taking the process down. A literal has no error arm, so the error
// value is an object value holding a [flowstatev1.Value_Error]: the engine does
// not read it as data, and reading it fails naming the kind rather than
// yielding null. The same holds for such a value nested in a list or a map,
// which becomes an error value in its place.
func Literal(v any) *expr.Value {
	value := flowstatev1.NewValue(v)
	if literal := value.GetLiteral(); literal != nil {
		budget := maxLiteralWalkNodes
		return markUnrepresentable(literal, &budget)
	}
	// Only the Go type is kept. The text of an error, or of whatever else
	// NewValue refused, can carry a secret from upstream, and a step output is
	// written to workflow history.
	return errorLiteral(fmt.Sprintf("flowstatev1: a value of type %s cannot be represented", unrepresentableType(v)))
}

// Bounds on the walk [markUnrepresentable] makes over a literal, which may be a
// caller-supplied tree that is deeper than any real output or even cyclic. Past
// either, the rest is returned as it is.
const maxLiteralWalkNodes = 1 << 14

// unrepresentableType names the Go type of a refused value, with only
// characters a type name has, so nothing from the value reaches the output.
func unrepresentableType(v any) string {
	name := fmt.Sprintf("%T", v)
	if len(name) > 128 || !utf8.ValidString(name) {
		return "(unnamed)"
	}
	return name
}

// markUnrepresentable replaces what [flowstatev1.NewValue] left out of a list or
// a map, an element it had no literal for, with an error value. It is bounded by
// the node budget, which also stops a cycle, and tolerates nil
// entries, since the literal may hold a caller-supplied tree.
func markUnrepresentable(literal *expr.Value, budget *int) *expr.Value {
	if literal == nil || *budget <= 0 {
		return literal
	}
	*budget--

	switch kind := literal.GetKind().(type) {
	case *expr.Value_ListValue:
		values := kind.ListValue.GetValues()
		for i, element := range values {
			if element == nil {
				values[i] = errorLiteral("")
				continue
			}
			markUnrepresentable(element, budget)
		}
	case *expr.Value_MapValue:
		for _, entry := range kind.MapValue.GetEntries() {
			if entry == nil {
				continue
			}
			if entry.Value == nil {
				entry.Value = errorLiteral("")
				continue
			}
			markUnrepresentable(entry.Value, budget)
		}
	}
	return literal
}

// errorLiteral is the error value [Literal] yields.
func errorLiteral(message string) *expr.Value {
	if message == "" {
		message = "flowstatev1: a value nested here is of a type Literal cannot represent"
	}
	packed, err := anypb.New(&flowstatev1.Value_Error{
		Message: message,
		Code:    flowstatev1.Value_Error_CODE_INTERNAL,
	})
	if err != nil {
		// Unreachable for a fixed message of valid text; a literal that still
		// names nothing of the value beats a panic if it somehow happened.
		return &expr.Value{Kind: &expr.Value_NullValue{}}
	}
	return &expr.Value{Kind: &expr.Value_ObjectValue{ObjectValue: packed}}
}

// nullValue is the absent answer: a field a message does not hold.
func nullValue() *expr.Value {
	return &expr.Value{Kind: &expr.Value_NullValue{}}
}

// literalValue wraps a CEL literal as a step output.
func literalValue(literal *expr.Value) *flowstatev1.Value {
	return &flowstatev1.Value{Kind: &flowstatev1.Value_Literal{Literal: literal}}
}
