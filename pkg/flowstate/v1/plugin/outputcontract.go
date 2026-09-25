package plugin

import (
	"fmt"
	"maps"
	"math"
	"slices"
	"strconv"

	expr "google.golang.org/genproto/googleapis/api/expr/v1alpha1"
	"google.golang.org/protobuf/reflect/protoreflect"

	flowstatev1 "github.com/picatz/flowstate/pkg/flowstate/v1"
)

const (
	celValueMessage       = protoreflect.FullName("google.api.expr.v1alpha1.Value")
	flowstateValueMessage = protoreflect.FullName("flowstate.v1.Value")
	maxOutputMessageDepth = 32
)

// checkOutputContract verifies that a plugin returned the shape its output
// descriptor promises. It runs inside the host boundary before either driver
// can put the result into downstream expression scope or durable history.
func checkOutputContract(descriptor protoreflect.MessageDescriptor, outputs *flowstatev1.Node_Outputs) error {
	values := outputs.GetNamedValues()
	selected := make(map[protoreflect.FullName]string)
	for _, name := range slices.Sorted(maps.Keys(values)) {
		value := values[name]
		if descriptor == nil {
			return fmt.Errorf("returned an undeclared output")
		}
		field := descriptor.Fields().ByName(protoreflect.Name(name))
		if field == nil {
			return fmt.Errorf("returned an undeclared output")
		}
		literal := value.GetLiteral()
		if literal == nil {
			return fmt.Errorf("output %q must be a computed value, got %T", name, value.GetKind())
		}
		if err := checkOneofSelection(field, literal, selected); err != nil {
			return fmt.Errorf("output %q: %w", name, err)
		}
		if err := checkOutputField(field, literal, name, 0); err != nil {
			return fmt.Errorf("output %q: %w", name, err)
		}
	}

	return nil
}

func checkOutputField(field protoreflect.FieldDescriptor, value *expr.Value, path string, depth int) error {
	if field.IsMap() {
		mapping, ok := value.GetKind().(*expr.Value_MapValue)
		if !ok {
			return fmt.Errorf("expected a map, got %s", literalKindName(value))
		}
		seen := make(map[string]struct{}, len(mapping.MapValue.GetEntries()))
		for i, entry := range mapping.MapValue.GetEntries() {
			key, ok := entry.GetKey().GetKind().(*expr.Value_StringValue)
			if !ok {
				return fmt.Errorf("map entry %d key must be a string, got %s", i, literalKindName(entry.GetKey()))
			}
			if !validOutputMapKey(field.MapKey().Kind(), key.StringValue) {
				return fmt.Errorf("map entry %d key is not a valid %s", i, field.MapKey().Kind())
			}
			if _, duplicate := seen[key.StringValue]; duplicate {
				return fmt.Errorf("map entry %d repeats an earlier key", i)
			}
			seen[key.StringValue] = struct{}{}
			if err := checkOutputScalar(field.MapValue(), entry.GetValue(), fmt.Sprintf("%s[%d]", path, i), depth); err != nil {
				return fmt.Errorf("map entry %d: %w", i, err)
			}
		}
		return nil
	}
	if field.IsList() {
		list, ok := value.GetKind().(*expr.Value_ListValue)
		if !ok {
			return fmt.Errorf("expected a list, got %s", literalKindName(value))
		}
		for i, element := range list.ListValue.GetValues() {
			if err := checkOutputScalar(field, element, fmt.Sprintf("%s[%d]", path, i), depth); err != nil {
				return fmt.Errorf("element %d: %w", i, err)
			}
		}
		return nil
	}
	return checkOutputScalar(field, value, path, depth)
}

func validOutputMapKey(kind protoreflect.Kind, value string) bool {
	switch kind {
	case protoreflect.StringKind:
		return true
	case protoreflect.BoolKind:
		return value == "true" || value == "false"
	case protoreflect.Int32Kind, protoreflect.Sint32Kind, protoreflect.Sfixed32Kind:
		parsed, err := strconv.ParseInt(value, 10, 32)
		return err == nil && strconv.FormatInt(parsed, 10) == value
	case protoreflect.Int64Kind, protoreflect.Sint64Kind, protoreflect.Sfixed64Kind:
		parsed, err := strconv.ParseInt(value, 10, 64)
		return err == nil && strconv.FormatInt(parsed, 10) == value
	case protoreflect.Uint32Kind, protoreflect.Fixed32Kind:
		parsed, err := strconv.ParseUint(value, 10, 32)
		return err == nil && strconv.FormatUint(parsed, 10) == value
	case protoreflect.Uint64Kind, protoreflect.Fixed64Kind:
		parsed, err := strconv.ParseUint(value, 10, 64)
		return err == nil && strconv.FormatUint(parsed, 10) == value
	default:
		return false
	}
}

func checkOutputScalar(field protoreflect.FieldDescriptor, value *expr.Value, path string, depth int) error {
	kind := value.GetKind()
	if _, null := kind.(*expr.Value_NullValue); null {
		oneof := field.ContainingOneof()
		if (oneof != nil && !oneof.IsSynthetic()) || field.Kind() == protoreflect.MessageKind || field.Kind() == protoreflect.GroupKind {
			return nil
		}
	}
	switch field.Kind() {
	case protoreflect.StringKind:
		if _, ok := kind.(*expr.Value_StringValue); !ok {
			return fmt.Errorf("expected a string, got %s", literalKindName(value))
		}
	case protoreflect.BytesKind:
		if _, ok := kind.(*expr.Value_BytesValue); !ok {
			return fmt.Errorf("expected bytes, got %s", literalKindName(value))
		}
	case protoreflect.BoolKind:
		if _, ok := kind.(*expr.Value_BoolValue); !ok {
			return fmt.Errorf("expected a boolean, got %s", literalKindName(value))
		}
	case protoreflect.Int32Kind, protoreflect.Sint32Kind, protoreflect.Sfixed32Kind:
		integer, ok := kind.(*expr.Value_Int64Value)
		if !ok {
			return fmt.Errorf("expected an integer, got %s", literalKindName(value))
		}
		if integer.Int64Value < math.MinInt32 || integer.Int64Value > math.MaxInt32 {
			return fmt.Errorf("integer is outside the range of %s", field.Kind())
		}
	case protoreflect.Int64Kind, protoreflect.Sint64Kind, protoreflect.Sfixed64Kind:
		if _, ok := kind.(*expr.Value_Int64Value); !ok {
			return fmt.Errorf("expected an integer, got %s", literalKindName(value))
		}
	case protoreflect.Uint32Kind, protoreflect.Fixed32Kind:
		integer, ok := kind.(*expr.Value_Uint64Value)
		if !ok {
			return fmt.Errorf("expected an unsigned integer, got %s", literalKindName(value))
		}
		if integer.Uint64Value > math.MaxUint32 {
			return fmt.Errorf("integer is outside the range of %s", field.Kind())
		}
	case protoreflect.Uint64Kind, protoreflect.Fixed64Kind:
		if _, ok := kind.(*expr.Value_Uint64Value); !ok {
			return fmt.Errorf("expected an unsigned integer, got %s", literalKindName(value))
		}
	case protoreflect.FloatKind, protoreflect.DoubleKind:
		number, ok := kind.(*expr.Value_DoubleValue)
		if !ok {
			return fmt.Errorf("expected a number, got %s", literalKindName(value))
		}
		if field.Kind() == protoreflect.FloatKind && outputOverflowsFloat32(number.DoubleValue) {
			return fmt.Errorf("number is outside the range of float")
		}
	case protoreflect.EnumKind:
		integer, ok := kind.(*expr.Value_Int64Value)
		if !ok {
			return fmt.Errorf("expected an enum number, got %s", literalKindName(value))
		}
		if integer.Int64Value < math.MinInt32 || integer.Int64Value > math.MaxInt32 ||
			field.Enum().Values().ByNumber(protoreflect.EnumNumber(integer.Int64Value)) == nil {
			return fmt.Errorf("enum number is not declared by %s", field.Enum().FullName())
		}
	case protoreflect.MessageKind, protoreflect.GroupKind:
		if field.Message().FullName() == celValueMessage || field.Message().FullName() == flowstateValueMessage {
			return nil
		}
		if depth >= maxOutputMessageDepth {
			return fmt.Errorf("nests messages more than %d deep", maxOutputMessageDepth)
		}
		mapping, ok := kind.(*expr.Value_MapValue)
		if !ok {
			return fmt.Errorf("expected a map for %s, got %s", field.Message().FullName(), literalKindName(value))
		}
		if err := checkOutputMessage(field.Message(), mapping.MapValue, path, depth+1); err != nil {
			return err
		}
	default:
		return fmt.Errorf("unsupported declared type %s", field.Kind())
	}
	return nil
}

func outputOverflowsFloat32(value float64) bool {
	return !math.IsInf(value, 0) && !math.IsNaN(value) && math.Abs(value) > math.MaxFloat32
}

func literalKindName(value *expr.Value) string {
	switch value.GetKind().(type) {
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
		return fmt.Sprintf("%T", value.GetKind())
	}
}

func checkOutputMessage(descriptor protoreflect.MessageDescriptor, value *expr.MapValue, path string, depth int) error {
	seen := make(map[string]struct{}, len(value.GetEntries()))
	selected := make(map[protoreflect.FullName]string)
	for _, entry := range value.GetEntries() {
		key, ok := entry.GetKey().GetKind().(*expr.Value_StringValue)
		if !ok {
			return fmt.Errorf("field name must be a string, got %s", literalKindName(entry.GetKey()))
		}
		field := descriptor.Fields().ByName(protoreflect.Name(key.StringValue))
		if field == nil {
			return fmt.Errorf("%s contains a field not declared by %s", path, descriptor.FullName())
		}
		if _, duplicate := seen[key.StringValue]; duplicate {
			return fmt.Errorf("field %q appears more than once", path+"."+key.StringValue)
		}
		seen[key.StringValue] = struct{}{}
		if err := checkOneofSelection(field, entry.GetValue(), selected); err != nil {
			return fmt.Errorf("field %q: %w", path+"."+key.StringValue, err)
		}
		if err := checkOutputField(field, entry.GetValue(), path+"."+key.StringValue, depth); err != nil {
			return fmt.Errorf("field %q: %w", path+"."+key.StringValue, err)
		}
	}

	return nil
}

func checkOneofSelection(field protoreflect.FieldDescriptor, value *expr.Value, selected map[protoreflect.FullName]string) error {
	oneof := field.ContainingOneof()
	if oneof == nil || oneof.IsSynthetic() {
		return nil
	}
	if _, null := value.GetKind().(*expr.Value_NullValue); null {
		return nil
	}
	if previous, ok := selected[oneof.FullName()]; ok {
		return fmt.Errorf("sets oneof %s together with %q", oneof.FullName(), previous)
	}
	selected[oneof.FullName()] = string(field.Name())
	return nil
}
