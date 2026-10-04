package flowstatev1

import (
	"encoding/base64"
	"errors"
	"fmt"
	"time"

	expr "google.golang.org/genproto/googleapis/api/expr/v1alpha1"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/anypb"
	"google.golang.org/protobuf/types/known/durationpb"
	"google.golang.org/protobuf/types/known/timestamppb"
)

// The data kinds CEL treats as primitives and the legacy enum can now name:
// timestamp, duration and bytes (#1436).
//
// Each travels as a string, which is what a caller can write on a command line,
// in a JSON file, in a `default:`, or in an MCP argument: RFC 3339 for a
// timestamp, a Go-form duration for a duration, standard padded base64 for bytes.
// The binder turns that string into the value CEL reads, once, so every
// expression and `must:` sees `inputs.at` as a timestamp rather than as the text
// it arrived as, and both drivers see the same thing because both bind through
// [BindRunInputs].
//
// Normalizing is idempotent. A call boundary hands a callee a value an
// expression already produced, and a Temporal run carries inputs the
// server already bound, so the normalized shape is accepted as is.

// IsDataKind reports whether t is one of the declared types whose wire shape is
// a string that a binder turns into a CEL timestamp, duration or bytes value.
func IsDataKind(t InputDeclaration_Type) bool {
	switch t {
	case InputDeclaration_TYPE_TIMESTAMP, InputDeclaration_TYPE_DURATION, InputDeclaration_TYPE_BYTES:
		return true
	default:
		return false
	}
}

// maxBytesInput bounds the decoded size of a declared `bytes` input. A bound is
// stated rather than inherited because a caller controls the text, and the text
// is decoded before anything else looks at it.
const maxBytesInput = 1 << 20

// WireHint is the sentence fragment that says how a data kind is written, for a
// refusal that names what the caller should send instead.
func WireHint(t InputDeclaration_Type) string {
	switch t {
	case InputDeclaration_TYPE_TIMESTAMP:
		return "an RFC 3339 timestamp such as 2026-01-01T00:00:00Z"
	case InputDeclaration_TYPE_DURATION:
		return "a duration such as 90m or 5400s"
	default:
		return "standard base64 text"
	}
}

// NormalizeDataKind returns the value a declared timestamp, duration or bytes
// input holds once bound, and an error when the literal is not one.
//
// A string is parsed. A literal already in the normalized shape passes through,
// and any other kind of literal is refused naming what the declaration expects.
// The error never repeats the offending text, because an input may be
// `sensitive:` and a refusal is not the place to print it.
func NormalizeDataKind(t InputDeclaration_Type, literal *expr.Value) (*expr.Value, error) {
	switch kind := literal.GetKind().(type) {
	case *expr.Value_StringValue:
		return parseDataKind(t, kind.StringValue)

	case *expr.Value_BytesValue:
		if t == InputDeclaration_TYPE_BYTES {
			return literal, boundBytes(len(kind.BytesValue))
		}

	case *expr.Value_ObjectValue:
		if matchesDataKind(t, kind.ObjectValue) {
			if err := checkNormalized(t, kind.ObjectValue); err != nil {
				return nil, err
			}

			return literal, nil
		}
	}

	return nil, fmt.Errorf("is not %s", WireHint(t))
}

func matchesDataKind(t InputDeclaration_Type, object *anypb.Any) bool {
	switch t {
	case InputDeclaration_TYPE_TIMESTAMP:
		return object.MessageIs(&timestamppb.Timestamp{})
	case InputDeclaration_TYPE_DURATION:
		return object.MessageIs(&durationpb.Duration{})
	default:
		return false
	}
}

// checkNormalized holds an already-packed timestamp or duration to what the
// text path holds one to: the payload must decode, and be in range. The type
// URL alone says nothing about the bytes behind it, and a submitted value is
// not trusted to be one this package packed.
func checkNormalized(t InputDeclaration_Type, object *anypb.Any) error {
	switch t {
	case InputDeclaration_TYPE_TIMESTAMP:
		var stamp timestamppb.Timestamp
		if object.UnmarshalTo(&stamp) != nil {
			return errors.New("is not " + WireHint(t))
		}
		if stamp.CheckValid() != nil {
			return errors.New("is outside the years 0001 to 9999 a timestamp can hold")
		}
	case InputDeclaration_TYPE_DURATION:
		var span durationpb.Duration
		if object.UnmarshalTo(&span) != nil || span.CheckValid() != nil {
			return errors.New("is not " + WireHint(t))
		}
	}

	return nil
}

func parseDataKind(t InputDeclaration_Type, text string) (*expr.Value, error) {
	switch t {
	case InputDeclaration_TYPE_TIMESTAMP:
		parsed, err := time.Parse(time.RFC3339Nano, text)
		if err != nil {
			return nil, errors.New("is not " + WireHint(t))
		}
		if err := timestamppb.New(parsed).CheckValid(); err != nil {
			return nil, errors.New("is outside the years 0001 to 9999 a timestamp can hold")
		}

		return objectValue(timestamppb.New(parsed))

	case InputDeclaration_TYPE_DURATION:
		parsed, err := time.ParseDuration(text)
		if err != nil {
			return nil, errors.New("is not " + WireHint(t))
		}

		return objectValue(durationpb.New(parsed))

	case InputDeclaration_TYPE_BYTES:
		// Bounded before decoded: the decoded size is at most three quarters of
		// the text, so a text longer than the bound allows can be refused
		// without allocating for it.
		if base64.StdEncoding.DecodedLen(len(text)) > maxBytesInput {
			return nil, fmt.Errorf("decodes to more than %d bytes", maxBytesInput)
		}
		decoded, err := base64.StdEncoding.DecodeString(text)
		if err != nil {
			return nil, errors.New("is not " + WireHint(t))
		}

		return &expr.Value{Kind: &expr.Value_BytesValue{BytesValue: decoded}}, boundBytes(len(decoded))

	default:
		return nil, fmt.Errorf("is not a timestamp, duration or bytes declaration")
	}
}

func boundBytes(n int) error {
	if n > maxBytesInput {
		return fmt.Errorf("decodes to more than %d bytes", maxBytesInput)
	}

	return nil
}

func objectValue(message proto.Message) (*expr.Value, error) {
	packed, err := anypb.New(message)
	if err != nil {
		return nil, fmt.Errorf("cannot be stored: %w", err)
	}

	return &expr.Value{Kind: &expr.Value_ObjectValue{ObjectValue: packed}}, nil
}

// dataKindValue is the literal a run holds for a Go time.Time or time.Duration, the
// well-known message in an Any, which is what CEL produces for one. A value that
// cannot be packed answers as an error value, like any other type [NewValue] cannot
// hold.
func dataKindValue(v any) *Value {
	var message proto.Message
	switch val := v.(type) {
	case time.Time:
		message = timestamppb.New(val)
	case time.Duration:
		message = durationpb.New(val)
	}

	literal, err := objectValue(message)
	if err != nil {
		return &Value{Kind: &Value_Error_{Error: &Value_Error{Message: err.Error(), Code: Value_Error_CODE_INTERNAL}}}
	}

	return &Value{Kind: &Value_Literal{Literal: literal}}
}

// dataKindString is a normalized timestamp or duration as the plain string a run
// document, an embedder or an http body carries (RFC 3339, a Go duration). Bytes
// are not here: they are already a byte slice, which JSON writes as base64.
func dataKindString(literal *expr.Value) (string, bool) {
	if _, isBytes := literal.GetKind().(*expr.Value_BytesValue); isBytes {
		return "", false
	}

	return dataKindText(literal)
}

// dataKindText spells a normalized timestamp, duration or bytes literal back as
// the text a caller would write, for a refusal that quotes the value it judged.
func dataKindText(literal *expr.Value) (string, bool) {
	switch kind := literal.GetKind().(type) {
	case *expr.Value_BytesValue:
		return base64.StdEncoding.EncodeToString(kind.BytesValue), true

	case *expr.Value_ObjectValue:
		var stamp timestamppb.Timestamp
		if kind.ObjectValue.UnmarshalTo(&stamp) == nil && kind.ObjectValue.MessageIs(&stamp) && stamp.CheckValid() == nil {
			return stamp.AsTime().Format(time.RFC3339Nano), true
		}

		var span durationpb.Duration
		if kind.ObjectValue.UnmarshalTo(&span) == nil && kind.ObjectValue.MessageIs(&span) && span.CheckValid() == nil {
			return span.AsDuration().String(), true
		}
	}

	return "", false
}
