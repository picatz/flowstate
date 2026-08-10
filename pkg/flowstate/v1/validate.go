package flowstatev1

import (
	"errors"
	"fmt"
	"strings"
	"sync"

	"buf.build/go/protovalidate"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protoreflect"
)

// ErrValidatorUnavailable reports that the validation rules could not be
// evaluated at all, as opposed to being evaluated and failed. It wraps
// failures to construct the protovalidate runtime, rules in the Protobuf
// schema that cannot be compiled, and type errors raised while evaluating a
// rule's CEL expression.
//
// An error wrapping ErrValidatorUnavailable is never a statement about the
// message being valid, and callers must not treat it as one: it means no
// verdict was reached. Servers should map it to an internal error rather than
// to an invalid-argument error, and must not proceed as though validation
// passed.
var ErrValidatorUnavailable = errors.New("flowstate: validator unavailable")

// sharedValidator returns the one [protovalidate.Validator] used by this
// package, constructing it on first use.
//
// A validator compiles and caches the rule evaluators for every message it
// sees, so building one is expensive and sharing one is what makes repeated
// validation cheap. Validators are safe for concurrent use. The result,
// including a construction failure, is computed exactly once.
var sharedValidator = sync.OnceValues(func() (protovalidate.Validator, error) {
	v, err := protovalidate.New()
	if err != nil {
		return nil, fmt.Errorf("%w: %w", ErrValidatorUnavailable, err)
	}
	return v, nil
})

// Validate reports whether m satisfies the validation rules declared for it in
// the Protobuf schema.
//
// The rules live in proto/flowstate/v1/flowstate.proto as protovalidate
// options, spelled (buf.validate.field) and (buf.validate.oneof). The url field
// of Task.HTTP.Inputs, for example, declares string.uri, and its method field
// declares a pattern matching the HTTP verbs the task accepts. Those options are
// read off the message's descriptor and enforced here at runtime, so there is no
// generated validation code to keep in sync with the schema.
//
// Validate returns nil if and only if every rule held. A message that violates
// one or more rules yields a [*ValidationError], which names the offending
// fields and the rules they failed. Any other error means the rules could not be
// evaluated and wraps [ErrValidatorUnavailable]. Validation never passes by
// default, so a nil message is reported as invalid rather than accepted.
func Validate(m proto.Message) error {
	if m == nil {
		return &ValidationError{Violations: []Violation{missingMessage()}}
	}

	// A typed-nil message has no fields to read, and protovalidate reports a
	// nil message as valid. Reject it here so that a nil never reads as a pass.
	refl := m.ProtoReflect()
	if !refl.IsValid() {
		return &ValidationError{
			MessageName: string(refl.Descriptor().FullName()),
			Violations:  []Violation{missingMessage()},
		}
	}

	v, err := sharedValidator()
	if err != nil {
		return err
	}

	err = v.Validate(m)
	if err == nil {
		return nil
	}

	var invalid *protovalidate.ValidationError
	if errors.As(err, &invalid) {
		return newValidationError(refl.Descriptor().FullName(), invalid)
	}

	// A compilation or runtime error is a defect in the schema's rules, not in
	// the message, so it must not be reported to the caller as bad input.
	return fmt.Errorf("%w: evaluating rules for %s: %w",
		ErrValidatorUnavailable, refl.Descriptor().FullName(), err)
}

// missingMessage returns the violation reported when there is no message to
// validate at all.
func missingMessage() Violation {
	return Violation{Rule: "required", Message: "no message was provided"}
}

// ValidationError reports that a message violated one or more of the
// validation rules declared for it in the Protobuf schema. Servers should map
// it to an invalid-argument error, since it describes input the caller can
// correct.
//
// Unwrap returns the underlying [*protovalidate.ValidationError], whose ToProto
// method yields the violations as a buf.validate.Violations message suitable
// for attaching to an RPC error as machine-readable detail.
type ValidationError struct {
	// MessageName is the full name of the Protobuf message that was validated,
	// such as "flowstate.v1.Task.HTTP.Inputs". It is empty if no message was
	// provided at all.
	MessageName string

	// Violations lists every rule the message failed. It always holds at least
	// one violation. The order is the order the rules were evaluated in, which
	// does not necessarily match the order the fields are declared in.
	Violations []Violation

	// cause is the error reported by protovalidate, retained so that callers
	// can reach its proto form.
	cause error
}

// newValidationError converts a protovalidate validation failure into a
// [*ValidationError] describing the message named by name.
func newValidationError(name protoreflect.FullName, err *protovalidate.ValidationError) *ValidationError {
	violations := make([]Violation, 0, len(err.Violations))
	for _, v := range err.Violations {
		violations = append(violations, Violation{
			Field:   protovalidate.FieldPathString(v.Proto.GetField()),
			Rule:    v.Proto.GetRuleId(),
			Message: v.Proto.GetMessage(),
		})
	}
	return &ValidationError{
		MessageName: string(name),
		Violations:  violations,
		cause:       err,
	}
}

// Error summarizes the failed rules, one per line when there is more than one.
func (e *ValidationError) Error() string {
	subject := e.MessageName
	if subject == "" {
		subject = "message"
	}

	var b strings.Builder
	b.WriteString("invalid ")
	b.WriteString(subject)
	b.WriteString(": ")

	if len(e.Violations) == 1 {
		b.WriteString(e.Violations[0].String())
		return b.String()
	}

	fmt.Fprintf(&b, "%d rules violated:", len(e.Violations))
	for _, v := range e.Violations {
		b.WriteString("\n  - ")
		b.WriteString(v.String())
	}
	return b.String()
}

// Unwrap returns the underlying [*protovalidate.ValidationError].
func (e *ValidationError) Unwrap() error { return e.cause }

// Violation describes a single validation rule that a message failed.
type Violation struct {
	// Field is the dotted path to the offending field, such as "url" or
	// "steps[0].task.name". It is empty when the rule applies to the message as
	// a whole rather than to one field.
	Field string

	// Rule identifies the rule that failed, such as "string.uri",
	// "string.pattern", or "required".
	Rule string

	// Message explains the failure in terms a workflow author can act on, such
	// as "must be a valid URI".
	Message string
}

// String renders the violation as the offending field, what went wrong, and the
// rule that was not met.
func (v Violation) String() string {
	var b strings.Builder
	if v.Field != "" {
		b.WriteString(v.Field)
		b.WriteString(": ")
	}
	if v.Message != "" {
		b.WriteString(v.Message)
	} else {
		b.WriteString("rule not met")
	}
	if v.Rule != "" {
		fmt.Fprintf(&b, " (%s)", v.Rule)
	}
	return b.String()
}
