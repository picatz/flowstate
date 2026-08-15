package flowstatev1

import (
	"context"
	"fmt"
	"strings"

	"github.com/google/cel-go/cel"
	ref "github.com/google/cel-go/common/types/ref"
	expr "google.golang.org/genproto/googleapis/api/expr/v1alpha1"
	"google.golang.org/protobuf/proto"
	protoreflect "google.golang.org/protobuf/reflect/protoreflect"
	"google.golang.org/protobuf/reflect/protoregistry"
)

// This file holds the conversions between a step's inputs and outputs and the
// proto messages a task declares: a CEL literal into a [Value] map, a [Value]
// map into a task's inputs message, a proto message back out as
// [Node_Outputs].
//
// It was carved out of eval_task_library.go rather than moved with the built-in
// tasks (see the note there), because it is not task-specific: the evaluator
// and [pkg/flowstate/v1/flowfile] use it for any task, including a plugin's.
// It imports nothing under pkg/flowstate/v1/ and belongs to the schema layer,
// which is exactly what makes it the wrong half of that file to relocate.

func literalToValueMap(lit *expr.Value) (map[string]*Value, error) {
	if lit == nil {
		return nil, nil
	}
	mv, ok := lit.GetKind().(*expr.Value_MapValue)
	if !ok {
		return nil, fmt.Errorf("outputs literal must be a map")
	}
	if len(mv.MapValue.Entries) == 0 {
		return nil, nil
	}
	out := make(map[string]*Value, len(mv.MapValue.Entries))
	for _, entry := range mv.MapValue.Entries {
		key := entry.GetKey().GetStringValue()
		if key == "" {
			return nil, fmt.Errorf("outputs map keys must be strings")
		}
		out[key] = &Value{
			Kind: &Value_Literal{
				Literal: entry.Value,
			},
		}
	}
	return out, nil
}

// valueToCEL resolves the given Value into a CEL value. Literals are converted
// directly, while expressions are evaluated against previous step outputs under
// the shared evaluator's limits.
func valueToCEL(ctx context.Context, v *Value, scope *Scope) (ref.Val, error) {
	switch kind := v.GetKind().(type) {
	case *Value_Literal:
		return cel.ValueToRefValue(TypeAdapter, kind.Literal)
	case *Value_Expr:
		return DefaultEvaluator().EvalParsedBase(ctx, scope.GetProfile(), kind.Expr, scope.Activation(ctx))
	default:
		return nil, fmt.Errorf("unsupported value kind %T", kind)
	}
}

func nodeOutputsFromProtoMessage(msg proto.Message) (*Node_Outputs, error) {
	outputs := &Node_Outputs{
		NamedValues: map[string]*Value{},
	}
	msgFields := msg.ProtoReflect().Descriptor().Fields()
	for i := 0; i < msgFields.Len(); i++ {
		fieldDesc := msgFields.Get(i)
		fieldName := string(fieldDesc.Name())
		val := msg.ProtoReflect().Get(fieldDesc)
		if fieldDesc.IsList() {
			valList := val.List()
			var values []*expr.Value
			for j := 0; j < valList.Len(); j++ {
				elem := valList.Get(j)
				switch fieldDesc.Kind() {
				case protoreflect.StringKind:
					values = append(values, &expr.Value{Kind: &expr.Value_StringValue{StringValue: elem.String()}})
				case protoreflect.Int32Kind, protoreflect.Int64Kind:
					values = append(values, &expr.Value{Kind: &expr.Value_Int64Value{Int64Value: elem.Int()}})
				case protoreflect.BoolKind:
					values = append(values, &expr.Value{Kind: &expr.Value_BoolValue{BoolValue: elem.Bool()}})
				case protoreflect.MessageKind:
					if v, ok := elem.Message().Interface().(*Value); ok {
						if lit := v.GetLiteral(); lit != nil {
							values = append(values, lit)
						} else {
							// fallback: wrap as struct or skip
						}
					} else {
						return nil, fmt.Errorf("unsupported message type in list output for field %q", fieldName)
					}
				default:
					return nil, fmt.Errorf("unsupported list element type in output: %s", fieldDesc.Kind().String())
				}
			}
			outputs.NamedValues[fieldName] = &Value{
				Kind: &Value_Literal{
					Literal: &expr.Value{
						Kind: &expr.Value_ListValue{
							ListValue: &expr.ListValue{Values: values},
						},
					},
				},
			}
			continue
		}
		if fieldDesc.IsMap() {
			// Convert proto map fields into a CEL MapValue literal.
			mv := msg.ProtoReflect().Get(fieldDesc).Map()
			entries := make([]*expr.MapValue_Entry, 0, mv.Len())
			mv.Range(func(k protoreflect.MapKey, v protoreflect.Value) bool {
				// Only string keys are supported in our protos.
				key := &expr.Value{Kind: &expr.Value_StringValue{StringValue: k.String()}}

				// Convert value based on value kind.
				var val *expr.Value
				switch fieldDesc.MapValue().Kind() {
				case protoreflect.StringKind:
					val = &expr.Value{Kind: &expr.Value_StringValue{StringValue: v.String()}}
				case protoreflect.Int32Kind, protoreflect.Int64Kind:
					val = &expr.Value{Kind: &expr.Value_Int64Value{Int64Value: v.Int()}}
				case protoreflect.BoolKind:
					val = &expr.Value{Kind: &expr.Value_BoolValue{BoolValue: v.Bool()}}
				case protoreflect.MessageKind:
					// If the value is a flowstate.v1.Value, unwrap its literal.
					if vv, ok := v.Message().Interface().(*Value); ok {
						if lit := vv.GetLiteral(); lit != nil {
							val = lit
							break
						}
					}
					// Fallback: unsupported message kind in map
					val = &expr.Value{Kind: &expr.Value_NullValue{}}
				default:
					// Fallback to null for unsupported kinds to avoid panic.
					val = &expr.Value{Kind: &expr.Value_NullValue{}}
				}
				entries = append(entries, &expr.MapValue_Entry{Key: key, Value: val})
				return true
			})
			outputs.NamedValues[fieldName] = &Value{
				Kind: &Value_Literal{Literal: &expr.Value{Kind: &expr.Value_MapValue{MapValue: &expr.MapValue{Entries: entries}}}},
			}
			continue
		}
		val = msg.ProtoReflect().Get(fieldDesc)

		// Emit the field unless the schema gives it explicit presence and it is
		// unset. Skipping any field that merely holds a zero value would drop a
		// legitimately empty result — an empty response body, a count of zero —
		// leaving downstream ${steps.<id>.field} references unresolvable.
		if fieldDesc.HasPresence() && !msg.ProtoReflect().Has(fieldDesc) {
			continue
		}

		switch fieldDesc.Kind() {
		case protoreflect.StringKind:
			outputs.NamedValues[fieldName] = &Value{
				Kind: &Value_Literal{
					Literal: &expr.Value{
						Kind: &expr.Value_StringValue{StringValue: val.String()},
					},
				},
			}
		case protoreflect.Int32Kind, protoreflect.Int64Kind:
			outputs.NamedValues[fieldName] = &Value{
				Kind: &Value_Literal{
					Literal: &expr.Value{
						Kind: &expr.Value_Int64Value{Int64Value: val.Int()},
					},
				},
			}
		case protoreflect.BoolKind:
			outputs.NamedValues[fieldName] = &Value{
				Kind: &Value_Literal{
					Literal: &expr.Value{
						Kind: &expr.Value_BoolValue{BoolValue: val.Bool()},
					},
				},
			}
		case protoreflect.DoubleKind, protoreflect.FloatKind:
			outputs.NamedValues[fieldName] = &Value{
				Kind: &Value_Literal{
					Literal: &expr.Value{
						Kind: &expr.Value_DoubleValue{DoubleValue: val.Float()},
					},
				},
			}
		case protoreflect.BytesKind:
			outputs.NamedValues[fieldName] = &Value{
				Kind: &Value_Literal{
					Literal: &expr.Value{
						Kind: &expr.Value_BytesValue{BytesValue: val.Bytes()},
					},
				},
			}
		case protoreflect.MessageKind:
			msgType := fieldDesc.Message().FullName()
			switch msgType {
			case "google.api.expr.v1alpha1.Value":
				if v, ok := val.Message().Interface().(*expr.Value); ok {
					outputs.NamedValues[fieldName] = &Value{
						Kind: &Value_Literal{Literal: v},
					}
				}
			case "flowstate.v1.Value":
				if v, ok := val.Message().Interface().(*Value); ok {
					outputs.NamedValues[fieldName] = v
				}
			default:
				// Generic nested message -> convert to a CEL map by reflecting fields.
				nested := val.Message()
				nd := nested.Descriptor().Fields()
				nestedEntries := make([]*expr.MapValue_Entry, 0, nd.Len())
				for i := 0; i < nd.Len(); i++ {
					f := nd.Get(i)
					fv := nested.Get(f)
					key := &expr.Value{Kind: &expr.Value_StringValue{StringValue: string(f.Name())}}
					var ev *expr.Value
					switch f.Kind() {
					case protoreflect.StringKind:
						ev = &expr.Value{Kind: &expr.Value_StringValue{StringValue: fv.String()}}
					case protoreflect.Int32Kind, protoreflect.Int64Kind:
						ev = &expr.Value{Kind: &expr.Value_Int64Value{Int64Value: fv.Int()}}
					case protoreflect.BoolKind:
						ev = &expr.Value{Kind: &expr.Value_BoolValue{BoolValue: fv.Bool()}}
					default:
						// For now, represent unsupported nested kinds as null.
						ev = &expr.Value{Kind: &expr.Value_NullValue{}}
					}
					nestedEntries = append(nestedEntries, &expr.MapValue_Entry{Key: key, Value: ev})
				}
				outputs.NamedValues[fieldName] = &Value{
					Kind: &Value_Literal{Literal: &expr.Value{Kind: &expr.Value_MapValue{MapValue: &expr.MapValue{Entries: nestedEntries}}}},
				}
			}
		default:
			return nil, fmt.Errorf("unsupported field type: %s", fieldDesc.Kind().String())
		}
	}
	return outputs, nil
}

// appendListElements converts CEL list elements into a repeated protobuf field.
//
// Both a literal list and the result of a list expression pass through here, so
// the two cannot disagree about what a list may contain. They previously did: the
// expression path inspected the evaluated value's native Go type and understood
// only strings, integers, and booleans, which rejected every list of
// message-typed elements — including printf's args, whose elements are
// flowstate.v1.Value. A list mixing a step reference with a literal therefore
// failed, while the same list written entirely as literals worked.
func appendListElements(
	ctx context.Context,
	elems []*expr.Value,
	fieldDesc protoreflect.FieldDescriptor,
	listField protoreflect.List,
	scope *Scope,
) error {
	for i, elem := range elems {
		if fieldDesc.Kind() == protoreflect.MessageKind {
			pv, err := listMessageElement(ctx, elem, fieldDesc, scope)
			if err != nil {
				return fmt.Errorf("element %d: %w", i, err)
			}
			listField.Append(pv)
			continue
		}

		pv, err := scalarFromLiteral(elem, fieldDesc)
		if err != nil {
			return fmt.Errorf("element %d: %w", i, err)
		}
		listField.Append(pv)
	}
	return nil
}

// listMessageElement converts one CEL value into a message element of a repeated
// field.
func listMessageElement(
	ctx context.Context,
	elem *expr.Value,
	fieldDesc protoreflect.FieldDescriptor,
	scope *Scope,
) (protoreflect.Value, error) {
	msgType := fieldDesc.Message()

	// A flowstate.v1.Value carries the CEL value as-is, which is what lets a task
	// like printf accept arguments of mixed type.
	if msgType.FullName() == "flowstate.v1.Value" {
		wrapped := &Value{Kind: &Value_Literal{Literal: elem}}
		return protoreflect.ValueOfMessage(wrapped.ProtoReflect()), nil
	}

	mapVal, ok := elem.GetKind().(*expr.Value_MapValue)
	if !ok {
		return protoreflect.Value{}, fmt.Errorf("expected a map to build %s, got %s",
			msgType.FullName(), literalKindName(elem))
	}

	msgTypeInfo, err := protoregistry.GlobalTypes.FindMessageByName(msgType.FullName())
	if err != nil {
		return protoreflect.Value{}, fmt.Errorf("could not find message type %q: %w", msgType.FullName(), err)
	}

	nested := msgTypeInfo.New().Interface()
	inputMap := make(map[string]*Value, len(mapVal.MapValue.GetEntries()))
	for _, e := range mapVal.MapValue.GetEntries() {
		inputMap[e.GetKey().GetStringValue()] = &Value{Kind: &Value_Literal{Literal: e.GetValue()}}
	}
	if err := populateProtoMessageFromValueMap(ctx, inputMap, nested, scope); err != nil {
		return protoreflect.Value{}, err
	}
	return protoreflect.ValueOfMessage(nested.ProtoReflect()), nil
}

// scalarFromLiteral converts a CEL literal to a protobuf value for a scalar
// field.
//
// The conversion is driven by which kind the literal actually holds, not by
// whether the extracted value is non-zero. Testing the extracted value conflates
// "wrong type" with "legitimately empty", which rejected every zero value a
// workflow could supply: an empty string message, a count of 0, a flag set to
// false.
// setMapEntries writes a CEL map's entries into a protobuf map field.
//
// One implementation for the literal and the expression paths, and — more to the point
// — the *same* conversion a scalar field uses. Each map path used to have its own
// switch calling the typed getter for the field's kind directly, and a protobuf getter
// answers the zero value for a value of some other kind rather than failing. So
// `headers: {X-Count: 5}` sent the header as an empty string, and `fields: {code: 500}`
// logged `code=`: the wrong thing, silently, in a request and a durable record.
//
// A key that cannot hold its value is now an error naming both, which is what the
// equivalent scalar field has always done. There was never a reason for a value inside
// a mapping to follow looser rules than the same value written beside a key.
func setMapEntries(entries []*expr.MapValue_Entry, fieldDesc protoreflect.FieldDescriptor, m protoreflect.Map) error {
	valueDesc := fieldDesc.MapValue()

	for _, e := range entries {
		key := e.GetKey().GetStringValue()

		// A flowstate.v1.Value map carries whatever the author wrote, unconverted —
		// the shape is the point, so there is nothing to check it against. Handled
		// before the scalar path, which has no case for it.
		if valueDesc.Kind() == protoreflect.MessageKind {
			if e.GetValue().GetKind() == nil {
				continue
			}
			held := &Value{Kind: &Value_Literal{Literal: e.GetValue()}}
			m.Set(protoreflect.ValueOfString(key).MapKey(), protoreflect.ValueOfMessage(held.ProtoReflect()))

			continue
		}

		converted, err := scalarFromLiteral(e.GetValue(), valueDesc)
		if err != nil {
			return fmt.Errorf("key %q: %w", key, err)
		}
		m.Set(protoreflect.ValueOfString(key).MapKey(), converted)
	}

	return nil
}

func scalarFromLiteral(lit *expr.Value, fieldDesc protoreflect.FieldDescriptor) (protoreflect.Value, error) {
	switch fieldDesc.Kind() {
	case protoreflect.StringKind:
		v, ok := lit.GetKind().(*expr.Value_StringValue)
		if !ok {
			return protoreflect.Value{}, fmt.Errorf("expected a string, got %s", literalKindName(lit))
		}
		return protoreflect.ValueOfString(v.StringValue), nil
	case protoreflect.Int32Kind, protoreflect.Int64Kind:
		switch v := lit.GetKind().(type) {
		case *expr.Value_Int64Value:
			if fieldDesc.Kind() == protoreflect.Int32Kind {
				return protoreflect.ValueOfInt32(int32(v.Int64Value)), nil
			}
			return protoreflect.ValueOfInt64(v.Int64Value), nil
		case *expr.Value_Uint64Value:
			if fieldDesc.Kind() == protoreflect.Int32Kind {
				return protoreflect.ValueOfInt32(int32(v.Uint64Value)), nil
			}
			return protoreflect.ValueOfInt64(int64(v.Uint64Value)), nil
		default:
			return protoreflect.Value{}, fmt.Errorf("expected an integer, got %s", literalKindName(lit))
		}
	case protoreflect.BoolKind:
		v, ok := lit.GetKind().(*expr.Value_BoolValue)
		if !ok {
			return protoreflect.Value{}, fmt.Errorf("expected a boolean, got %s", literalKindName(lit))
		}
		return protoreflect.ValueOfBool(v.BoolValue), nil
	case protoreflect.DoubleKind, protoreflect.FloatKind:
		v, ok := lit.GetKind().(*expr.Value_DoubleValue)
		if !ok {
			return protoreflect.Value{}, fmt.Errorf("expected a number, got %s", literalKindName(lit))
		}
		if fieldDesc.Kind() == protoreflect.FloatKind {
			return protoreflect.ValueOfFloat32(float32(v.DoubleValue)), nil
		}
		return protoreflect.ValueOfFloat64(v.DoubleValue), nil
	case protoreflect.BytesKind:
		v, ok := lit.GetKind().(*expr.Value_BytesValue)
		if !ok {
			return protoreflect.Value{}, fmt.Errorf("expected bytes, got %s", literalKindName(lit))
		}
		return protoreflect.ValueOfBytes(v.BytesValue), nil
	case protoreflect.EnumKind:
		// Written as the choice, not as a number. `level: warn` is what a Flowfile
		// says; the number is storage, and a language whose author has to know the
		// storage has failed at being one.
		v, ok := lit.GetKind().(*expr.Value_StringValue)
		if !ok {
			return protoreflect.Value{}, fmt.Errorf("expected one of %s, got %s",
				strings.Join(EnumValueNames(fieldDesc.Enum()), ", "), literalKindName(lit))
		}
		number, known := EnumValueNumber(fieldDesc.Enum(), v.StringValue)
		if !known {
			return protoreflect.Value{}, fmt.Errorf("%q is not one of %s",
				v.StringValue, strings.Join(EnumValueNames(fieldDesc.Enum()), ", "))
		}
		return protoreflect.ValueOfEnum(number), nil
	default:
		return protoreflect.Value{}, fmt.Errorf("unsupported field type %s", fieldDesc.Kind())
	}
}

// literalKindName names the kind a CEL literal holds, for error messages that
// tell a workflow author what they actually supplied.
func literalKindName(lit *expr.Value) string {
	switch lit.GetKind().(type) {
	case *expr.Value_StringValue:
		return "a string"
	case *expr.Value_Int64Value:
		return "an integer"
	case *expr.Value_Uint64Value:
		return "an unsigned integer"
	case *expr.Value_DoubleValue:
		return "a number"
	case *expr.Value_BoolValue:
		return "a boolean"
	case *expr.Value_BytesValue:
		return "bytes"
	case *expr.Value_NullValue:
		return "null"
	case *expr.Value_ListValue:
		return "a list"
	case *expr.Value_MapValue:
		return "a map"
	case nil:
		return "nothing"
	default:
		return fmt.Sprintf("%T", lit.GetKind())
	}
}

// nestedSecretHelp says where a reference nested in a list or a mapping can go,
// for a specification that reached a worker without passing `flow validate`.
//
// It names the http task the way the neighbouring message about `bearer:` does,
// rather than asking the registry which inputs accept one: this file builds the
// registry, so reading it from here is an initialization cycle. The author-facing
// answer — which inputs of *this* task accept a reference, named from the task's
// own definition — is the compiler's, in `flowfile`, where there is a line and a
// column to put it on.
const nestedSecretHelp = "; an input that accepts one applies its entries itself, inside the " +
	"activity, which is what lets the reference stay a reference — the http task's headers, " +
	"form and json are the ones built today"

func populateProtoMessageFromValueMap(ctx context.Context, input map[string]*Value, msg proto.Message, scope *Scope) error {
	msgFields := msg.ProtoReflect().Descriptor().Fields()
	for i := 0; i < msgFields.Len(); i++ {
		fieldDesc := msgFields.Get(i)
		fieldName := string(fieldDesc.Name())
		val, ok := input[fieldName]
		if !ok {
			continue // Field not provided in input map
		}
		if fieldDesc.IsMap() {
			// Support string-keyed maps with primitive values and flowstate.v1.Value messages.
			m := msg.ProtoReflect().Mutable(fieldDesc).Map()
			switch v := val.GetKind().(type) {
			case *Value_Literal:
				if mv, ok := v.Literal.GetKind().(*expr.Value_MapValue); ok {
					if err := setMapEntries(mv.MapValue.GetEntries(), fieldDesc, m); err != nil {
						return fmt.Errorf("field %q: %w", fieldName, err)
					}

					continue
				}
				return fmt.Errorf("expected map literal for field %q", fieldName)
			case *Value_Expr:
				// Evaluate the CEL expression and convert to a protobuf expr.Value.
				out, err := valueToCEL(ctx, val, scope)
				if err != nil {
					return fmt.Errorf("field %q: %w", fieldName, err)
				}
				pv, err := cel.RefValueToValue(out)
				if err != nil {
					return fmt.Errorf("failed to convert CEL value: %w", err)
				}
				if mv, ok := pv.GetKind().(*expr.Value_MapValue); ok {
					if err := setMapEntries(mv.MapValue.GetEntries(), fieldDesc, m); err != nil {
						return fmt.Errorf("field %q: %w", fieldName, err)
					}
					continue
				}
				return fmt.Errorf("expected map from CEL for field %q", fieldName)
			case *Value_Structure_:
				// A mapping whose entries are values in their own right, which is
				// the shape that can hold a secret reference. It is set entry by
				// entry and unconverted: the field's value type is
				// flowstate.v1.Value, so what the author wrote arrives at the task
				// exactly as written, reference included, and the task resolves it
				// where it uses it.
				entries, isMap := StructureMap(val)
				if !isMap {
					return fmt.Errorf("field %q expects a mapping, but a list was given", fieldName)
				}
				if fieldDesc.MapValue().Message() == nil ||
					fieldDesc.MapValue().Message().FullName() != "flowstate.v1.Value" {
					// map<string, string> and its like. The entries could be
					// flattened into strings, and a reference among them could
					// not — and this is the branch a reference would arrive by,
					// so flattening would resolve one into a field that a `%+v`
					// anywhere would print.
					return fmt.Errorf(
						"field %q holds plain values, so it cannot carry a secret reference%s",
						fieldName, nestedSecretHelp)
				}
				for name, entry := range entries {
					if name == "" {
						return fmt.Errorf("field %q has an entry with an empty name", fieldName)
					}
					m.Set(protoreflect.ValueOfString(name).MapKey(),
						protoreflect.ValueOfMessage(entry.ProtoReflect()))
				}
				continue
			default:
				return fmt.Errorf("unsupported map input for field %q: %T", fieldName, val)
			}
		}
		if fieldDesc.IsList() {
			listField := msg.ProtoReflect().Mutable(fieldDesc).List()
			switch v := val.GetKind().(type) {
			case *Value_Literal:
				lv, ok := v.Literal.GetKind().(*expr.Value_ListValue)
				if !ok {
					return fmt.Errorf("field %q expects a list, but got %s",
						fieldName, literalKindName(v.Literal))
				}
				if err := appendListElements(ctx, lv.ListValue.GetValues(), fieldDesc, listField, scope); err != nil {
					return fmt.Errorf("field %q: %w", fieldName, err)
				}
			case *Value_Expr:
				// Evaluate the expression, then convert its result through the
				// same path a literal list takes. Inspecting the CEL value's
				// native Go type instead would diverge from the literal path —
				// which is how a list mixing a reference with a literal, such as
				// printf's args, came to be rejected.
				out, err := valueToCEL(ctx, val, scope)
				if err != nil {
					return fmt.Errorf("field %q: %w", fieldName, err)
				}
				converted, err := cel.RefValueToValue(out)
				if err != nil {
					return fmt.Errorf("field %q: converting expression result: %w", fieldName, err)
				}
				lv, ok := converted.GetKind().(*expr.Value_ListValue)
				if !ok {
					return fmt.Errorf("field %q expects a list, but the expression produced %s",
						fieldName, literalKindName(converted))
				}
				if err := appendListElements(ctx, lv.ListValue.GetValues(), fieldDesc, listField, scope); err != nil {
					return fmt.Errorf("field %q: %w", fieldName, err)
				}
			default:
				return fmt.Errorf("unsupported value type for list field %q: %T", fieldName, val)
			}
			continue
		}
		// A singular flowstate.v1.Value field carries whatever the author wrote,
		// unconverted: a literal of any shape, or a secret reference. The http task's
		// `json` body is one, since a request body can be an object of any shape and
		// flattening it to a scalar would lose it.
		if fieldDesc.Kind() == protoreflect.MessageKind &&
			fieldDesc.Message().FullName() == "flowstate.v1.Value" {
			resolved := val

			// An expression is evaluated first, so the field holds a value rather
			// than something still to be computed.
			if val.GetExpr() != nil {
				out, err := valueToCEL(ctx, val, scope)
				if err != nil {
					return fmt.Errorf("field %q: %w", fieldName, err)
				}
				literal, err := cel.RefValueToValue(out)
				if err != nil {
					return fmt.Errorf("field %q: converting expression result: %w", fieldName, err)
				}
				resolved = &Value{Kind: &Value_Literal{Literal: literal}}
			}

			msg.ProtoReflect().Set(fieldDesc, protoreflect.ValueOfMessage(resolved.ProtoReflect()))
			continue
		}

		switch kind := val.GetKind().(type) {
		case *Value_Expr:
			out, err := valueToCEL(ctx, val, scope)
			if err != nil {
				return fmt.Errorf("field %q: %w", fieldName, err)
			}
			value, err := cel.RefValueToValue(out)
			if err != nil {
				return fmt.Errorf("failed to convert CEL reference to value: %w", err)
			}
			pv, err := scalarFromLiteral(value, fieldDesc)
			if err != nil {
				return fmt.Errorf("field %q: %w", fieldName, err)
			}
			msg.ProtoReflect().Set(fieldDesc, pv)
		case *Value_Literal:
			pv, err := scalarFromLiteral(kind.Literal, fieldDesc)
			if err != nil {
				return fmt.Errorf("field %q: %w", fieldName, err)
			}
			msg.ProtoReflect().Set(fieldDesc, pv)
		case *Value_SecretRef:
			// The one kind that is deliberately inert everywhere except the activity
			// that uses it, and which no task input accepts yet.
			//
			// Named rather than left to the default below, which reported
			// `unsupported value type: *flowstatev1.Value` — a Go type, naming
			// neither the input nor the reference, for a spelling `flow validate`
			// had just accepted. An author who wrote `${secret(...)}` where it does
			// not go got no way to tell what they had written wrong.
			//
			// About the *field* and not the task, which is a distinction with a
			// caller: `plugin/sdk/values.go` takes a singular `flowstate.v1.Value`
			// field whole, secret reference included, and says so in the same words.
			// A task-wide claim would send an author away from another input on the
			// same task that would have worked.
			return fmt.Errorf(
				"field %q was given a secret reference (%s:%s), which this field's type "+
					"cannot hold; a field declared as flowstate.v1.Value receives one whole, "+
					"which is how a task takes a value it resolves itself — the http task's "+
					"bearer: input is the one built today",
				fieldName, kind.SecretRef.GetScheme(), kind.SecretRef.GetName())

		case *Value_Structure_:
			// A list or a mapping where the field holds one value. Named for the
			// same reason the reference above is: this is the shape a Flowfile
			// compiles a structure holding a reference into, so an author who put
			// one where a scalar belongs meets a sentence about what they wrote.
			return fmt.Errorf(
				"field %q was given a list or a mapping, which this field's type cannot hold%s",
				fieldName, nestedSecretHelp)

		default:
			return fmt.Errorf("field %q: unsupported value type: %T", fieldName, val)
		}
	}
	return nil
}

// PopulateLiterals fills msg from the inputs an author wrote as literals, ignoring
// every other kind.
//
// It exists so a *compiler* can ask what the engine would say about the part of a
// step it can already see. `flow validate` used to check an input's type against the
// field and stop there, so a file declaring `method: FETCH` validated cleanly and
// then failed at run time with a Protobuf-flavoured message naming no line — the
// author learning about a rule the schema had stated all along, from the surface
// least able to point at it.
//
// Literals only, and that is the whole discipline. An expression's value depends on
// step outputs that do not exist yet, so a rule checked against it would be checked
// against nothing; a secret is resolved in the activity that needs it and is not a
// value here at all. Both are left out, which means the message this fills is
// deliberately *partial* — so a caller must ignore any violation about a field being
// absent, since absence here says nothing about the file.
//
// The context is unused and cannot be otherwise: resolving an expression is the one
// thing that would need one, and there are none.
func PopulateLiterals(msg proto.Message, inputs map[string]*Value) error {
	literals := make(map[string]*Value, len(inputs))
	for name, value := range inputs {
		if _, isLiteral := value.GetKind().(*Value_Literal); isLiteral {
			literals[name] = value
		}
	}

	return populateProtoMessageFromValueMap(context.Background(), literals, msg, nil)
}
