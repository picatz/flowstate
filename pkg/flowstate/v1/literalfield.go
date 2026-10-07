package flowstatev1

import (
	"errors"
	"fmt"
	"math"
	"strings"

	expr "google.golang.org/genproto/googleapis/api/expr/v1alpha1"
	"google.golang.org/protobuf/reflect/protoreflect"

	"github.com/picatz/flowstate/internal/textbound"
)

// ErrFieldUnsupported marks a refusal that is about what the field's declaration
// can hold rather than about the value written: no value of this input's kind
// could fill it. [SetLiteralField] wraps it so a caller can tell a task that
// disagrees with itself from a workflow that wrote the wrong thing.
var ErrFieldUnsupported = errors.New("the task's declaration, not the input, is what to change")

// SetLiteralField converts a CEL literal into the value a message field holds and
// sets it: a scalar by the field's kind, a list into a repeated field, a map
// literal into a map or into a nested message keyed by field name, and a message
// naming a oneof's member into that member.
//
// It is the one conversion both ends of a task input use. The plugin SDK fills a
// task's input message with it, and the host checks a nested input against the
// task's declared schema with it, so what a plugin accepts and what `flow
// validate` accepts cannot drift apart.
//
// The work is bounded where it is spent: messages nest at most 16 deep, a list
// or map literal holds at most 1024 entries, and one call converts at most 65536
// values in all. Messages are built through the descriptor's own constructors,
// so a field whose type is described only by a dynamic descriptor works as well
// as a generated one.
//
// An enum value the schema marks test-only is accepted unless the caller passes
// [RefuseTestOnlyEnums]: the plugin that owns the task decides at its point of use
// what a released build refuses, and the host, which cannot, refuses it where the
// author can see it.
func SetLiteralField(msg protoreflect.Message, field protoreflect.FieldDescriptor, literal *expr.Value, options ...LiteralOption) error {
	d := &literalDecoder{}
	for _, option := range options {
		option(d)
	}

	return d.setLiteral(msg, field, literal, 0)
}

// A LiteralOption adjusts one call of [SetLiteralField].
type LiteralOption func(*literalDecoder)

// RefuseTestOnlyEnums makes [SetLiteralField] refuse an enum value the schema
// marks `test_only`, by name or by number, as a released build does.
func RefuseTestOnlyEnums() LiteralOption {
	return func(d *literalDecoder) { d.refuseTestOnly = true }
}

// Bounds on a structural decode. A nested input is shaped by whatever the
// workflow author (or the data an expression read) produced, so the plugin side
// bounds the work it spends rather than trusting the payload's size alone.
const (
	// maxLiteralDepth is how many messages deep a literal may nest.
	maxLiteralDepth = 16
	// maxLiteralElements is how many entries one list or map literal may hold.
	maxLiteralElements = 1024
	// maxLiteralNodes is how many values one input may convert in all.
	maxLiteralNodes = 1 << 16
)

// literalDecoder carries the work budget of decoding one input.
type literalDecoder struct {
	nodes          int
	refuseTestOnly bool
}

// spend charges n converted values against the input's budget.
func (d *literalDecoder) spend(n int) error {
	d.nodes += n
	if d.nodes > maxLiteralNodes {
		return fmt.Errorf("holds more than %d values", maxLiteralNodes)
	}
	return nil
}

// setLiteral assigns a CEL literal to a field, converting by the field's kind.
// depth is how many messages have been entered to reach the field.
func (d *literalDecoder) setLiteral(msg protoreflect.Message, field protoreflect.FieldDescriptor, literal *expr.Value, depth int) error {
	switch {
	case field.IsMap():
		// Only string keys, checked before anything is built: the key is
		// constructed as a string below, and protobuf's reflection panics rather
		// than erroring when a value of the wrong type is set on a map. A panic
		// here would surface to the engine as a dropped connection, which reads
		// as a transient failure and gets retried into the same panic.
		if kind := field.MapKey().Kind(); kind != protoreflect.StringKind {
			return fmt.Errorf(
				"has %s map keys, which DecodeInputs does not convert; read this input from the map directly (%w)",
				kind, ErrFieldUnsupported,
			)
		}

		entries := literal.GetMapValue()
		if entries == nil {
			return fmt.Errorf("wants a map")
		}

		if n := len(entries.GetEntries()); n > maxLiteralElements {
			return fmt.Errorf("has %d entries; at most %d are accepted", n, maxLiteralElements)
		}

		mapValue := msg.Mutable(field).Map()
		for _, entry := range entries.GetEntries() {
			key, isString := entry.GetKey().GetKind().(*expr.Value_StringValue)
			if !isString {
				return fmt.Errorf("has a map key that is not a string")
			}

			converted, err := d.scalar(field.MapValue(), entry.GetValue(), depth, mapValue.NewValue)
			if err != nil {
				return fmt.Errorf("key %q: %w", textbound.Truncate(key.StringValue, 64), err)
			}

			mapValue.Set(protoreflect.ValueOfString(key.StringValue).MapKey(), converted)
		}
		return nil

	case field.IsList():
		values := literal.GetListValue()
		if values == nil {
			return fmt.Errorf("wants a list")
		}
		if n := len(values.GetValues()); n > maxLiteralElements {
			return fmt.Errorf("has %d elements; at most %d are accepted", n, maxLiteralElements)
		}
		list := msg.Mutable(field).List()
		for i, element := range values.GetValues() {
			converted, err := d.scalar(field, element, depth, list.NewElement)
			if err != nil {
				return fmt.Errorf("element %d: %w", i, err)
			}
			list.Append(converted)
		}
		return nil

	default:
		converted, err := d.scalar(field, literal, depth, func() protoreflect.Value { return msg.NewField(field) })
		if err != nil {
			return err
		}
		msg.Set(field, converted)
		return nil
	}
}

// scalar converts one CEL literal into a value of a field's type.
func (d *literalDecoder) scalar(field protoreflect.FieldDescriptor, value *expr.Value, depth int, fresh func() protoreflect.Value) (protoreflect.Value, error) {
	if err := d.spend(1); err != nil {
		return protoreflect.Value{}, err
	}

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
		// Names resolve as the schema's own diagnostics resolve them: either
		// spelling, any case, and no zero by name. A number must name a value the
		// enum defines.
		if n, ok := integer(value); ok && n >= math.MinInt32 && n <= math.MaxInt32 {
			if enum := field.Enum().Values().ByNumber(protoreflect.EnumNumber(n)); enum != nil {
				if d.refuseTestOnly && EnumValueTestOnly(enum) {
					return protoreflect.Value{}, withheldEnum(field.Enum(), enum.Name())
				}
				return protoreflect.ValueOfEnum(enum.Number()), nil
			}
			return protoreflect.Value{}, fmt.Errorf("%d is not one of %s", n, strings.Join(EnumValueNames(field.Enum()), ", "))
		}
		if v, ok := value.GetKind().(*expr.Value_StringValue); ok {
			if enum := enumValueWritten(field.Enum(), v.StringValue); enum != nil {
				if d.refuseTestOnly && EnumValueTestOnly(enum) {
					return protoreflect.Value{}, withheldEnum(field.Enum(), protoreflect.Name(textbound.Truncate(v.StringValue, 64)))
				}
				return protoreflect.ValueOfEnum(enum.Number()), nil
			}
			return protoreflect.Value{}, fmt.Errorf("%q is not one of %s", textbound.Truncate(v.StringValue, 64), strings.Join(EnumValueNames(field.Enum()), ", "))
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
		case flowValueName:
			wrapped := &Value{Kind: &Value_Literal{Literal: value}}
			return protoreflect.ValueOfMessage(wrapped.ProtoReflect()), nil
		}
		// A well-known type is still refused, for the reason [encodeScalar]
		// gives: what a timestamp or a duration is on the workflow side is
		// #1436's to decide once, for every boundary.
		if strings.HasPrefix(string(field.Message().FullName()), "google.protobuf.") {
			return protoreflect.Value{}, fmt.Errorf(
				"is a %s, which DecodeInputs does not convert; declare it as flowstate.v1.Value or read it from the input map directly (%w)",
				field.Message().FullName(), ErrFieldUnsupported,
			)
		}

		// Any other message is filled from a map literal keyed by field name,
		// the same spelling [EncodeOutputs] produces.
		return d.message(fresh().Message(), value, depth+1)
	default:
		return protoreflect.Value{}, fmt.Errorf(
			"has type %s, which DecodeInputs does not convert; read it from the input map directly (%w)",
			field.Kind(), ErrFieldUnsupported,
		)
	}

	return protoreflect.Value{}, fmt.Errorf("expected a %s, got %s", field.Kind(), literalKindName(value))
}

// message converts a map literal into a message of the given type.
//
// Keys are field names, so a oneof is written by naming the member it holds —
// `{section: {text: "hi"}}` — and a literal naming two members of one oneof is
// refused rather than resolved by whichever came last. A key the message has no
// field for is refused by name, unlike at the top level of [DecodeInputs]: a
// misspelt nested key would otherwise vanish and leave a message that looks
// accepted and is missing the part the author wrote. A null value leaves its
// field unset.
func (d *literalDecoder) message(built protoreflect.Message, value *expr.Value, depth int) (protoreflect.Value, error) {
	desc := built.Descriptor()
	if depth > maxLiteralDepth {
		return protoreflect.Value{}, fmt.Errorf("nests messages more than %d deep", maxLiteralDepth)
	}

	entries := value.GetMapValue()
	if entries == nil {
		return protoreflect.Value{}, fmt.Errorf("wants a map for %s", desc.FullName())
	}
	if n := len(entries.GetEntries()); n > maxLiteralElements {
		return protoreflect.Value{}, fmt.Errorf("has %d entries; at most %d are accepted", n, maxLiteralElements)
	}

	fields := desc.Fields()
	for _, entry := range entries.GetEntries() {
		key, isString := entry.GetKey().GetKind().(*expr.Value_StringValue)
		if !isString {
			return protoreflect.Value{}, fmt.Errorf("has a key that is not a string")
		}
		field := fields.ByName(protoreflect.Name(key.StringValue))
		if field == nil {
			return protoreflect.Value{}, fmt.Errorf("has no field %q in %s; its fields are %s",
				textbound.Truncate(key.StringValue, 64), desc.FullName(), fieldNames(desc))
		}
		if _, null := entry.GetValue().GetKind().(*expr.Value_NullValue); null {
			continue
		}
		if oneof := field.ContainingOneof(); oneof != nil && !oneof.IsSynthetic() {
			if held := built.WhichOneof(oneof); held != nil && held != field {
				return protoreflect.Value{}, fmt.Errorf("names both %q and %q, which are alternatives in %q; write one",
					held.Name(), field.Name(), oneof.Name())
			}
		}
		if err := d.setLiteral(built, field, entry.GetValue(), depth); err != nil {
			return protoreflect.Value{}, fmt.Errorf("field %q: %w", field.Name(), err)
		}
	}

	return protoreflect.ValueOfMessage(built), nil
}

// fieldNames lists a message's field names, bounded so a wide message does not
// make the diagnostic its own problem.
func fieldNames(desc protoreflect.MessageDescriptor) string {
	fields := desc.Fields()
	names := make([]string, 0, min(fields.Len(), 12))
	for i := range min(fields.Len(), 12) {
		names = append(names, string(fields.Get(i).Name()))
	}
	list := strings.Join(names, ", ")
	if fields.Len() > 12 {
		list += ", …"
	}
	return list
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

// withheldEnum is the refusal for a value compiled into test builds only.
func withheldEnum(enum protoreflect.EnumDescriptor, written protoreflect.Name) error {
	return fmt.Errorf("%q is compiled into test builds of %s only, and a released build refuses it", written, enum.FullName())
}
