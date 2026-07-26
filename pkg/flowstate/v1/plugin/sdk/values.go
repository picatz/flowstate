package sdk

import (
	"fmt"
	"math"

	expr "google.golang.org/genproto/googleapis/api/expr/v1alpha1"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protoreflect"

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
//			return nil, sdk.InvalidInput("%v", err)
//		}
//		...
//	}
//
// An input the message has no field for is ignored rather than refused, so that
// a workflow written against a newer version of a task does not fail against an
// older plugin. An input whose expression the engine has not resolved is
// refused, since the plugin has no way to evaluate one it did not declare as
// deferred.
func DecodeInputs(inputs map[string]*flowstatev1.Value, msg proto.Message) error {
	if msg == nil {
		return fmt.Errorf("sdk: DecodeInputs needs a message to fill")
	}

	reflectMsg := msg.ProtoReflect()
	fields := reflectMsg.Descriptor().Fields()

	for name, value := range inputs {
		field := fields.ByName(protoreflect.Name(name))
		if field == nil {
			continue
		}

		if err := setField(reflectMsg, field, value); err != nil {
			return fmt.Errorf("input %q: %w", name, err)
		}
	}

	return nil
}

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
		return setLiteral(msg, field, kind.Literal)
	case *flowstatev1.Value_Expr:
		return fmt.Errorf(
			"is an unresolved expression; the engine resolves inputs before sending them, so this one was declared in DeferredInputs and the task has to evaluate it itself")
	case *flowstatev1.Value_SecretRef:
		return fmt.Errorf(
			"is a secret reference, which this field's type cannot hold; declare the field as flowstate.v1.Value to receive one")
	case *flowstatev1.Value_Error_:
		return fmt.Errorf("is an error value: %s", truncate(kind.Error.GetMessage(), 256))
	default:
		return fmt.Errorf("has no value")
	}
}

// setLiteral assigns a CEL literal to a field, converting by the field's kind.
func setLiteral(msg protoreflect.Message, field protoreflect.FieldDescriptor, literal *expr.Value) error {
	switch {
	case field.IsMap():
		// Only string keys, checked before anything is built: the key is
		// constructed as a string below, and protobuf's reflection panics rather
		// than erroring when a value of the wrong type is set on a map. A panic
		// here would surface to the engine as a dropped connection, which reads
		// as a transient failure and gets retried into the same panic.
		if kind := field.MapKey().Kind(); kind != protoreflect.StringKind {
			return fmt.Errorf(
				"has %s map keys, which DecodeInputs does not convert; read this input from the map directly",
				kind,
			)
		}

		entries := literal.GetMapValue()
		if entries == nil {
			return fmt.Errorf("wants a map")
		}

		mapValue := msg.Mutable(field).Map()
		for _, entry := range entries.GetEntries() {
			key, isString := entry.GetKey().GetKind().(*expr.Value_StringValue)
			if !isString {
				return fmt.Errorf("has a map key that is not a string")
			}

			converted, err := scalar(field.MapValue(), entry.GetValue())
			if err != nil {
				return fmt.Errorf("map value for %q: %w", truncate(key.StringValue, 64), err)
			}

			mapValue.Set(protoreflect.ValueOfString(key.StringValue).MapKey(), converted)
		}
		return nil

	case field.IsList():
		values := literal.GetListValue()
		if values == nil {
			return fmt.Errorf("wants a list")
		}
		list := msg.Mutable(field).List()
		for i, element := range values.GetValues() {
			converted, err := scalar(field, element)
			if err != nil {
				return fmt.Errorf("element %d: %w", i, err)
			}
			list.Append(converted)
		}
		return nil

	default:
		converted, err := scalar(field, literal)
		if err != nil {
			return err
		}
		msg.Set(field, converted)
		return nil
	}
}

// scalar converts one CEL literal into a value of a field's type.
func scalar(field protoreflect.FieldDescriptor, value *expr.Value) (protoreflect.Value, error) {
	switch field.Kind() {
	case protoreflect.StringKind:
		if v, ok := value.GetKind().(*expr.Value_StringValue); ok {
			return protoreflect.ValueOfString(v.StringValue), nil
		}
	case protoreflect.BytesKind:
		if v, ok := value.GetKind().(*expr.Value_BytesValue); ok {
			return protoreflect.ValueOfBytes(v.BytesValue), nil
		}
		if v, ok := value.GetKind().(*expr.Value_StringValue); ok {
			return protoreflect.ValueOfBytes([]byte(v.StringValue)), nil
		}
	case protoreflect.BoolKind:
		if v, ok := value.GetKind().(*expr.Value_BoolValue); ok {
			return protoreflect.ValueOfBool(v.BoolValue), nil
		}
	// The range checks below are the same reasoning [integer] applies to a
	// fractional value: a number that does not fit is a workflow author's
	// mistake, and wrapping it into a plausible one — 4294967296 becoming 0, or
	// 1e300 becoming +Inf — turns a diagnosable failure into a wrong answer.
	case protoreflect.Int32Kind, protoreflect.Sint32Kind, protoreflect.Sfixed32Kind:
		if n, ok := integer(value); ok && n >= math.MinInt32 && n <= math.MaxInt32 {
			return protoreflect.ValueOfInt32(int32(n)), nil
		}
	case protoreflect.Int64Kind, protoreflect.Sint64Kind, protoreflect.Sfixed64Kind:
		if n, ok := integer(value); ok {
			return protoreflect.ValueOfInt64(n), nil
		}
	case protoreflect.Uint32Kind, protoreflect.Fixed32Kind:
		if n, ok := integer(value); ok && n >= 0 && n <= math.MaxUint32 {
			return protoreflect.ValueOfUint32(uint32(n)), nil
		}
	case protoreflect.Uint64Kind, protoreflect.Fixed64Kind:
		if n, ok := integer(value); ok && n >= 0 {
			return protoreflect.ValueOfUint64(uint64(n)), nil
		}
	case protoreflect.FloatKind:
		if f, ok := number(value); ok && !overflowsFloat32(f) {
			return protoreflect.ValueOfFloat32(float32(f)), nil
		}
	case protoreflect.DoubleKind:
		if f, ok := number(value); ok {
			return protoreflect.ValueOfFloat64(f), nil
		}
	case protoreflect.EnumKind:
		if n, ok := integer(value); ok && n >= math.MinInt32 && n <= math.MaxInt32 {
			return protoreflect.ValueOfEnum(protoreflect.EnumNumber(n)), nil
		}
		if v, ok := value.GetKind().(*expr.Value_StringValue); ok {
			if enum := field.Enum().Values().ByName(protoreflect.Name(v.StringValue)); enum != nil {
				return protoreflect.ValueOfEnum(enum.Number()), nil
			}
			return protoreflect.Value{}, fmt.Errorf("%q is not a value of %s", truncate(v.StringValue, 64), field.Enum().FullName())
		}
	case protoreflect.MessageKind:
		// A field, element, or map value whose declared type does not constrain
		// its shape — the http task's `outputs` map and `json` input are both
		// this. Either spelling is accepted, going in as well as coming out, so
		// that a task can take structured input as readily as it can return
		// structured output.
		switch field.Message().FullName() {
		case celValueName:
			return protoreflect.ValueOfMessage(value.ProtoReflect()), nil
		case flowstateValueName:
			wrapped := &flowstatev1.Value{Kind: &flowstatev1.Value_Literal{Literal: value}}
			return protoreflect.ValueOfMessage(wrapped.ProtoReflect()), nil
		}
	default:
		return protoreflect.Value{}, fmt.Errorf(
			"has type %s, which DecodeInputs does not convert; read it from the input map directly",
			field.Kind(),
		)
	}

	return protoreflect.Value{}, fmt.Errorf("is not a %s", field.Kind())
}

// integer reads a literal as a signed integer, accepting the several ways CEL
// can carry one.
func integer(value *expr.Value) (int64, bool) {
	switch v := value.GetKind().(type) {
	case *expr.Value_Int64Value:
		return v.Int64Value, true
	case *expr.Value_Uint64Value:
		if v.Uint64Value > 1<<63-1 {
			return 0, false
		}
		return int64(v.Uint64Value), true
	case *expr.Value_DoubleValue:
		// Only when it is exactly an integer: silently truncating 1.5 into 1
		// would turn a workflow author's mistake into a plausible result.
		if n := int64(v.DoubleValue); float64(n) == v.DoubleValue {
			return n, true
		}
	}
	return 0, false
}

// overflowsFloat32 reports whether a float64 cannot be held as a float32.
//
// An infinity that was already infinite is fine; one produced by narrowing is
// not, because it silently replaces a number with something that is not one.
func overflowsFloat32(f float64) bool {
	if math.IsInf(f, 0) || math.IsNaN(f) {
		return false
	}
	return math.Abs(f) > math.MaxFloat32
}

// number reads a literal as a float.
func number(value *expr.Value) (float64, bool) {
	switch v := value.GetKind().(type) {
	case *expr.Value_DoubleValue:
		return v.DoubleValue, true
	case *expr.Value_Int64Value:
		return float64(v.Int64Value), true
	case *expr.Value_Uint64Value:
		return float64(v.Uint64Value), true
	}
	return 0, false
}

// EncodeOutputs turns a task's output message into the named values a step
// produces, one per field.
//
// Later steps reference them as ${step_id.field_name}, so the field names in the
// output message are the names a workflow author writes.
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
	value := msg.Get(field)

	switch {
	case field.IsMap():
		entries := make([]*expr.MapValue_Entry, 0, value.Map().Len())
		var mapErr error
		value.Map().Range(func(key protoreflect.MapKey, element protoreflect.Value) bool {
			converted, err := encodeScalar(field.MapValue(), element)
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
		return literalValue(&expr.Value{
			Kind: &expr.Value_MapValue{MapValue: &expr.MapValue{Entries: entries}},
		}), nil

	case field.IsList():
		list := value.List()
		values := make([]*expr.Value, 0, list.Len())
		for i := range list.Len() {
			converted, err := encodeScalar(field, list.Get(i))
			if err != nil {
				return nil, err
			}
			values = append(values, converted)
		}
		return literalValue(&expr.Value{
			Kind: &expr.Value_ListValue{ListValue: &expr.ListValue{Values: values}},
		}), nil

	default:
		converted, err := encodeScalar(field, value)
		if err != nil {
			return nil, err
		}
		return literalValue(converted), nil
	}
}

// encodeScalar turns one field value into a CEL literal.
func encodeScalar(field protoreflect.FieldDescriptor, value protoreflect.Value) (*expr.Value, error) {
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
			if v, ok := value.Message().Interface().(*expr.Value); ok {
				return v, nil
			}

		// flowstate's own wrapper around the same thing, provided it holds a
		// literal. An output carrying an unevaluated expression or a secret
		// reference is not an output: the first is something the task was
		// supposed to evaluate, and the second must never become a step output at
		// all, since step outputs are written to workflow history.
		case flowstateValueName:
			v, ok := value.Message().Interface().(*flowstatev1.Value)
			if !ok {
				break
			}
			if literal := v.GetLiteral(); literal != nil {
				return literal, nil
			}
			return nil, fmt.Errorf(
				"holds a %T rather than a value; a task's outputs must be values it computed",
				v.GetKind(),
			)
		}

		// Any other message is refused rather than converted.
		//
		// Converting one would mean inventing a mapping from its fields onto a
		// CEL value, and that mapping would be this package's invention rather
		// than the schema's: field names would come out however JSON naming
		// mangles them, and the result would not match the descriptor the engine
		// validates the task against. Refusing precisely is worth more than
		// converting approximately, so the error says what to do instead.
		return nil, fmt.Errorf(
			"has message type %s, which EncodeOutputs does not convert. "+
				"Declare the field as %s to return data of any shape, and build it with sdk.Literal — "+
				"for example `Data: sdk.Literal(map[string]any{\"items\": items})`",
			field.Message().FullName(), celValueName,
		)
	}

	return nil, fmt.Errorf(
		"has type %s, which EncodeOutputs does not convert; build the named value directly",
		field.Kind(),
	)
}

// The two message types a task output may be declared as when its shape is not
// fixed. They are named as constants because the check is by full name — a
// plugin's own copy of a descriptor is a different Go type but the same protobuf
// type, and comparing names is what makes both work.
const (
	celValueName       = "google.api.expr.v1alpha1.Value"
	flowstateValueName = "flowstate.v1.Value"
)

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
// `${query.rows[0].name}` — with no step in between to parse it.
//
// A type it cannot represent yields an error value rather than a panic, so a
// plugin that hands it something unexpected produces a diagnosable output instead
// of taking the process down.
func Literal(v any) *expr.Value {
	return flowstatev1.NewValue(v).GetLiteral()
}

// literalValue wraps a CEL literal as a step output.
func literalValue(literal *expr.Value) *flowstatev1.Value {
	return &flowstatev1.Value{Kind: &flowstatev1.Value_Literal{Literal: literal}}
}
