// Package schemaifacepilot contains the bounded experiment for issue #1227.
//
// It is not a CLI framework. The real Cobra tree continues to own command
// hierarchy, eligibility, spelling, and presentation copy; this package only
// compares reflective and generated mechanics for two explicitly selected
// GetRequest fields.
package schemaifacepilot

import (
	"fmt"

	"google.golang.org/protobuf/reflect/protoreflect"
	"google.golang.org/protobuf/types/descriptorpb"
)

// Exposure is command-owned eligibility. Descriptor shape alone never grants
// eligibility, and unknown values are refused.
type Exposure uint8

const (
	ExposureUnspecified Exposure = iota
	ExposureInput
	ExposureServerOwned
)

// Selection is the deliberately small bridge between a command and one request
// field. SurfaceName and Usage are UI decisions; ProtoName identifies the
// schema contract. GoField exists only so the static pilot can generate direct
// assignments rather than rediscovering Go naming rules.
type Selection struct {
	ProtoName   protoreflect.Name
	GoField     string
	SurfaceName string
	Usage       string
	Positional  bool
	Exposure    Exposure
}

// GetSelections is the entire exposure decision in the pilot. A field added to
// GetRequest is not selected by appearing in its descriptor.
var GetSelections = []Selection{
	{
		ProtoName:   "workflow_id",
		GoField:     "WorkflowId",
		SurfaceName: "workflow-id",
		Usage:       "workflow whose run should be reported",
		Positional:  true,
		Exposure:    ExposureInput,
	},
	{
		ProtoName:   "run_id",
		GoField:     "RunId",
		SurfaceName: "run-id",
		Usage:       "ask about one attempt of the workload; unset asks about whichever is current",
		Exposure:    ExposureInput,
	},
}

type selectedField struct {
	selection  Selection
	descriptor protoreflect.FieldDescriptor
}

func selectFields(message protoreflect.MessageDescriptor, selections []Selection) ([]selectedField, error) {
	if message == nil {
		return nil, fmt.Errorf("schema interface pilot: nil message descriptor")
	}

	seenSurface := map[string]protoreflect.Name{}
	seenProto := map[protoreflect.Name]bool{}
	selected := make([]selectedField, 0, len(selections))
	for _, selection := range selections {
		if selection.Exposure != ExposureInput {
			return nil, fmt.Errorf("schema interface pilot: %s has unknown or non-input exposure %d", selection.ProtoName, selection.Exposure)
		}
		if selection.ProtoName == "" || selection.SurfaceName == "" {
			return nil, fmt.Errorf("schema interface pilot: selected fields require protobuf and surface names")
		}
		if prior, exists := seenSurface[selection.SurfaceName]; exists {
			return nil, fmt.Errorf("schema interface pilot: surface name %q collides between %s and %s", selection.SurfaceName, prior, selection.ProtoName)
		}
		if seenProto[selection.ProtoName] {
			return nil, fmt.Errorf("schema interface pilot: protobuf field %s is selected twice", selection.ProtoName)
		}

		field := message.Fields().ByName(selection.ProtoName)
		if field == nil {
			return nil, fmt.Errorf("schema interface pilot: selected field %s.%s does not exist", message.FullName(), selection.ProtoName)
		}
		if err := supportedField(field); err != nil {
			return nil, fmt.Errorf("schema interface pilot: selected field %s.%s: %w", message.FullName(), selection.ProtoName, err)
		}
		if selection.Positional && field.HasPresence() {
			return nil, fmt.Errorf("schema interface pilot: optional field %s cannot be flattened into a required positional", selection.ProtoName)
		}

		seenSurface[selection.SurfaceName] = selection.ProtoName
		seenProto[selection.ProtoName] = true
		selected = append(selected, selectedField{selection: selection, descriptor: field})
	}
	return selected, nil
}

// ValidateSelections checks that every selected field still exists and remains
// within the pilot's deliberately narrow supported shape.
func ValidateSelections(message protoreflect.MessageDescriptor, selections []Selection) error {
	_, err := selectFields(message, selections)
	return err
}

// supportedField is intentionally narrower than protobuf. Widening this list
// is a reviewed product decision, never a side effect of adding a schema field.
func supportedField(field protoreflect.FieldDescriptor) error {
	switch {
	case field.IsMap():
		return fmt.Errorf("maps are unsupported")
	case field.Cardinality() == protoreflect.Repeated:
		return fmt.Errorf("repeated fields are unsupported")
	case field.ContainingOneof() != nil && !field.ContainingOneof().IsSynthetic():
		return fmt.Errorf("oneof fields are unsupported")
	case field.Kind() != protoreflect.StringKind:
		return fmt.Errorf("kind %s is unsupported", field.Kind())
	case field.HasDefault():
		return fmt.Errorf("schema defaults are unsupported; command defaults remain explicit")
	case fieldDeprecated(field):
		return fmt.Errorf("deprecated fields require an explicit CLI migration")
	default:
		return nil
	}
}

func fieldDeprecated(field protoreflect.FieldDescriptor) bool {
	options, _ := field.Options().(*descriptorpb.FieldOptions)
	return options.GetDeprecated()
}

func selectionByName(fields []selectedField, name protoreflect.Name) (selectedField, bool) {
	for _, field := range fields {
		if field.selection.ProtoName == name {
			return field, true
		}
	}
	return selectedField{}, false
}
