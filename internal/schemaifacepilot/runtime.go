package schemaifacepilot

import (
	"fmt"

	"github.com/spf13/pflag"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protoreflect"
)

// RuntimeBinding is the reflective pilot. It walks only the explicit
// selections, creates flags for selected non-positionals, and writes values
// through protoreflection. It never walks all request fields to discover inputs.
type RuntimeBinding struct {
	message proto.Message
	flags   *pflag.FlagSet
	fields  []selectedField
	values  map[protoreflect.Name]*string
}

func NewRuntimeBinding(message proto.Message, selections []Selection, flags *pflag.FlagSet) (*RuntimeBinding, error) {
	if message == nil || !message.ProtoReflect().IsValid() {
		return nil, fmt.Errorf("schema interface pilot: invalid message")
	}
	if flags == nil {
		return nil, fmt.Errorf("schema interface pilot: nil flag set")
	}
	fields, err := selectFields(message.ProtoReflect().Descriptor(), selections)
	if err != nil {
		return nil, err
	}

	binding := &RuntimeBinding{
		message: message,
		flags:   flags,
		fields:  fields,
		values:  make(map[protoreflect.Name]*string, len(fields)),
	}
	for _, field := range fields {
		if field.selection.Positional {
			continue
		}
		value := new(string)
		binding.values[field.selection.ProtoName] = value
		flags.StringVar(value, field.selection.SurfaceName, "", field.selection.Usage)
	}
	return binding, nil
}

// Apply writes the command-owned positional values and changed flags, then
// invokes the same validator the caller would otherwise invoke. Optional flags
// remain absent when unset.
func (b *RuntimeBinding) Apply(positionals map[protoreflect.Name]string, validate func(proto.Message) error) error {
	reflection := b.message.ProtoReflect()
	for _, field := range b.fields {
		selection := field.selection
		if selection.Positional {
			value, exists := positionals[selection.ProtoName]
			if !exists {
				return fmt.Errorf("schema interface pilot: missing positional %s", selection.SurfaceName)
			}
			reflection.Set(field.descriptor, protoreflect.ValueOfString(value))
			continue
		}
		if b.flags.Changed(selection.SurfaceName) {
			reflection.Set(field.descriptor, protoreflect.ValueOfString(*b.values[selection.ProtoName]))
		}
	}
	if validate != nil {
		return validate(b.message)
	}
	return nil
}

func (b *RuntimeBinding) Message() proto.Message { return b.message }
