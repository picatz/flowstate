package flowstatev1

import (
	"errors"
	"fmt"
	"maps"
	"slices"
	"strings"

	"google.golang.org/protobuf/reflect/protoreflect"
	"google.golang.org/protobuf/types/dynamicpb"
)

// checkDeclaredOutputs holds what a task returned to the output schema it
// declares (#2507), so a result a peer or a plugin made up never becomes a step's
// outputs just because it arrived: an `http` answer of status 999 against a
// declared 100..599 is refused here rather than read by an expression three steps
// later.
//
// It judges only what it can be sure of. A step that shapes its outputs replaces
// the declared names, a task with no output descriptor declares nothing to hold
// the result to, and a value that is not a literal (a structure an expression
// built) has no schema field to land in. For the rest, every returned name that
// the schema declares is set on a message of the schema and validated with the
// schema's own rules; names it does not declare are ignored, because
// [Task.EvalInScope] cannot tell an extra output from a derived one.
//
// The failure is [ErrorKindUpstreamUnknown], which is permanent: the task ran and
// may have taken effect, so another attempt is not a repair.
func checkDeclaredOutputs(t *Task, def TaskDef, out *Node_Outputs) error {
	if def.Outputs == nil || out == nil || len(out.GetNamedValues()) == 0 {
		return nil
	}
	if def.ShapesOutputs && t.GetInputs()[ShapingInput] != nil {
		return nil
	}

	message := dynamicpb.NewMessage(def.Outputs)
	fields := def.Outputs.Fields()
	returned := map[string]bool{}
	for _, name := range slices.Sorted(maps.Keys(out.GetNamedValues())) {
		field := fields.ByName(protoreflect.Name(name))
		literal := out.GetNamedValues()[name].GetLiteral()
		if field == nil || literal == nil {
			continue
		}
		// A value [SetLiteralField] cannot place is not judged here. It is an
		// input decoder, stricter than the shape a plugin's output contract
		// accepts (non-string map keys, well-known types, a null optional, a list
		// past its 1024-element cap), and the type of a result is that
		// contract's question; this check answers only the schema's rules.
		if err := SetLiteralField(message.ProtoReflect(), field, literal); err != nil {
			continue
		}
		returned[name] = true
	}

	err := Validate(message)
	if err == nil {
		return nil
	}
	var validation *ValidationError
	if !errors.As(err, &validation) {
		// The rules could not be evaluated: never a pass by default.
		return NewTaskError(t.Name, ErrorKindUpstreamUnknown, fmt.Errorf("the declared output could not be validated: %w", err))
	}
	for _, violation := range validation.Violations {
		if returned[ViolationRoot(violation.Field)] {
			return NewTaskError(t.Name, ErrorKindUpstreamUnknown, fmt.Errorf("output %s", violation.String()))
		}
	}

	return nil
}

// ViolationRoot is the top-level field a violation's dotted path starts at, so a
// rule failing inside a returned struct or list is attributed to that output.
func ViolationRoot(path string) string {
	root, _, _ := strings.Cut(path, ".")
	root, _, _ = strings.Cut(root, "[")

	return root
}
