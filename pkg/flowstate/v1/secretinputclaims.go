package flowstatev1

import (
	"fmt"

	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protoreflect"
)

// SecretInputClaims reads the secret claims a task's input message declares on
// its own fields with the `flowstate.v1.input` option, as the three name sets
// [TaskDef] carries: whole-value inputs, the subset of those that are required,
// and inputs a secret reference may sit nested inside.
//
// Only the message's top-level fields are read, in declaration order, and each
// list is sorted and deduplicated so it digests the same however it was
// declared. SECRET_REQUIRED implies SECRET_WHOLE_VALUE, so a required name is in
// both whole and required. A field with no claim is in none.
//
// It fails closed: a claim on a field of the wrong shape, or a number the host
// does not know, is an error rather than a claim read charitably. A whole-value
// or required claim needs a `flowstate.v1.Value` field, which holds a
// reference unresolved, or a string field, which a plugin task receives the
// resolved value in; a nested claim needs a Value or a map of them (a map of
// strings is accepted, since that is how a descriptor documents a mapping the
// task decodes as a structure).
func SecretInputClaims(md protoreflect.MessageDescriptor) (whole, required, nested []string, err error) {
	if md == nil {
		return nil, nil, nil, nil
	}

	fields := md.Fields()
	for i := range fields.Len() {
		fd := fields.Get(i)

		input, _ := proto.GetExtension(fd.Options(), E_Input).(*InputOptions)
		if input == nil {
			continue
		}

		name := string(fd.Name())
		claim := input.GetSecret()

		switch claim {
		case Secret_SECRET_UNSPECIFIED:
			continue
		case Secret_SECRET_WHOLE_VALUE, Secret_SECRET_REQUIRED:
			if !isValueField(fd) && !isStringField(fd) {
				return nil, nil, nil, fmt.Errorf("input %q declares %s but is neither a flowstate.v1.Value nor a string", name, claim)
			}
			whole = append(whole, name)
			if claim == Secret_SECRET_REQUIRED {
				required = append(required, name)
			}
		case Secret_SECRET_NESTED:
			if !isValueField(fd) && !isValueMapField(fd) {
				return nil, nil, nil, fmt.Errorf("input %q declares %s but is neither a flowstate.v1.Value nor a map of them", name, claim)
			}
			nested = append(nested, name)
		default:
			return nil, nil, nil, fmt.Errorf("input %q declares unknown secret claim %d", name, int32(claim))
		}
	}

	return canonicalStrings(whole), canonicalStrings(required), canonicalStrings(nested), nil
}

const valueFullName protoreflect.FullName = "flowstate.v1.Value"

// isValueField reports whether fd is a single flowstate.v1.Value.
func isValueField(fd protoreflect.FieldDescriptor) bool {
	return !fd.IsList() && !fd.IsMap() && fd.Kind() == protoreflect.MessageKind && fd.Message().FullName() == valueFullName
}

// isValueMapField reports whether fd is a map whose values are Values or
// strings.
func isValueMapField(fd protoreflect.FieldDescriptor) bool {
	if !fd.IsMap() {
		return false
	}
	v := fd.MapValue()
	return v.Kind() == protoreflect.StringKind || (v.Kind() == protoreflect.MessageKind && v.Message().FullName() == valueFullName)
}

// isStringField reports whether fd is a single string.
func isStringField(fd protoreflect.FieldDescriptor) bool {
	return !fd.IsList() && !fd.IsMap() && fd.Kind() == protoreflect.StringKind
}
