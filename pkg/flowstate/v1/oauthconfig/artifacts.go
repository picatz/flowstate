package oauthconfig

import (
	"encoding/json"
	"fmt"
	"strings"

	"github.com/google/cel-go/cel"
	v1 "github.com/picatz/flowstate/pkg/flowstate/v1"
	"google.golang.org/protobuf/reflect/protoreflect"
)

// CELTypes returns the CEL environment option for the same generated message
// descriptors used by YAML, validation, help, and schema generation.
func CELTypes() cel.EnvOption {
	return cel.Types(
		&v1.OAuthConfiguration{}, &v1.AuthorizationServer{},
		&v1.ProtectedResource{}, &v1.GrantProfile{}, &v1.SecretReference{},
	)
}

// Reference renders a compact configuration reference suitable for CLI help or
// generated documentation. Names and descriptions come from the descriptor.
func Reference() string {
	var out strings.Builder
	out.WriteString("# OAuth configuration\n\n")
	writeReference(&out, (&v1.OAuthConfiguration{}).ProtoReflect().Descriptor(), map[protoreflect.FullName]bool{})
	return out.String()
}

func writeReference(out *strings.Builder, message protoreflect.MessageDescriptor, seen map[protoreflect.FullName]bool) {
	if seen[message.FullName()] {
		return
	}
	seen[message.FullName()] = true
	fmt.Fprintf(out, "## `%s`\n\n| YAML key | Type | Description |\n|---|---|---|\n", message.Name())
	for i := range message.Fields().Len() {
		field := message.Fields().Get(i)
		description := strings.Join(strings.Fields(field.ParentFile().SourceLocations().ByDescriptor(field).LeadingComments), " ")
		kind := field.Kind().String()
		if field.IsList() {
			kind = "list of " + kind
		}
		fmt.Fprintf(out, "| `%s` | %s | %s |\n", field.Name(), kind, description)
	}
	out.WriteString("\n")
	for i := range message.Fields().Len() {
		if child := message.Fields().Get(i).Message(); child != nil && !message.Fields().Get(i).IsMap() {
			writeReference(out, child, seen)
		}
	}
}

// JSONSchema renders the protojson/YAML object shape from protobuf descriptors.
func JSONSchema() ([]byte, error) {
	root := schemaForMessage((&v1.OAuthConfiguration{}).ProtoReflect().Descriptor(), map[protoreflect.FullName]bool{})
	root["$schema"] = "https://json-schema.org/draft/2020-12/schema"
	root["title"] = "Flowstate OAuth configuration"
	return json.MarshalIndent(root, "", "  ")
}

func schemaForMessage(message protoreflect.MessageDescriptor, stack map[protoreflect.FullName]bool) map[string]any {
	if stack[message.FullName()] {
		return map[string]any{"type": "object"}
	}
	stack[message.FullName()] = true
	defer delete(stack, message.FullName())
	properties := map[string]any{}
	for i := range message.Fields().Len() {
		field := message.Fields().Get(i)
		value := schemaForField(field, stack)
		if field.IsList() {
			value = map[string]any{"type": "array", "items": value}
		}
		properties[string(field.Name())] = value
	}
	return map[string]any{"type": "object", "additionalProperties": false, "properties": properties}
}

func schemaForField(field protoreflect.FieldDescriptor, stack map[protoreflect.FullName]bool) map[string]any {
	switch field.Kind() {
	case protoreflect.MessageKind:
		return schemaForMessage(field.Message(), stack)
	case protoreflect.EnumKind:
		values := make([]string, 0, field.Enum().Values().Len())
		for i := range field.Enum().Values().Len() {
			values = append(values, string(field.Enum().Values().Get(i).Name()))
		}
		return map[string]any{"type": "string", "enum": values}
	case protoreflect.BoolKind:
		return map[string]any{"type": "boolean"}
	case protoreflect.Uint32Kind, protoreflect.Int32Kind:
		return map[string]any{"type": "integer"}
	default:
		return map[string]any{"type": "string"}
	}
}
