package flowstatev1

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"strings"

	expr "google.golang.org/genproto/googleapis/api/expr/v1alpha1"
)

// FailTask is the task name a raised failure carries on its [TaskError]. A
// `fail:` step is not a task, but every renderer of a failure ([StepErrorText],
// the timeline, the run's last failure) frames it as `task "<name>" failed
// (<kind>): <cause>`, and a raised failure reads the same way as every other.
const FailTask = "fail"

// MaxDeclaredErrors bounds the errors one workflow declares, matching the
// schema's bound for a specification that skipped the schema.
const MaxDeclaredErrors = 64

// MaxFailMessageBytes bounds the sentence a `fail:` step carries. The message is
// evaluated from an expression and lands in `error`, in `failure.message` and in
// the run's failure, so a result larger than this is refused rather than
// truncated: an author who builds a long message has a bug to be told about.
const MaxFailMessageBytes = 4096

// CheckErrorDeclarations reports whether the errors wf declares are well-formed
// and whether every `fail:` step names one of them.
//
// A name may not spell a built-in [ErrorKind], which a declaration could
// otherwise redefine (a workflow declaring `Timeout` would make
// `failure.kind == "Timeout"` mean two things). The same rules run when a Flowfile
// compiles, with a line to point at, and again at submit for a specification
// that never was a Flowfile.
func CheckErrorDeclarations(wf *Workflow) error {
	if n := len(wf.GetDeclaredErrors()); n > MaxDeclaredErrors {
		return fmt.Errorf("%d errors are declared; the most a workflow declares is %d", n, MaxDeclaredErrors)
	}

	declared := make(map[string]bool, len(wf.GetDeclaredErrors()))
	for _, declaration := range wf.GetDeclaredErrors() {
		name := declaration.GetName()
		if declared[name] {
			return fmt.Errorf("error %q is declared twice", name)
		}
		declared[name] = true

		if err := Validate(declaration); err != nil {
			return fmt.Errorf("error %q is invalid: %w", name, err)
		}
		if _, builtin := ParseErrorKind(name); builtin {
			return fmt.Errorf("error %q is a built-in kind (%s); declare it under a name of its own",
				name, strings.Join(errorKindNames(), ", "))
		}
	}

	var err error
	WalkNodes(wf.GetSteps(), Walk{Node: func(node *Node) {
		fail := node.GetFail()
		if err != nil || fail == nil {
			return
		}
		if !declared[fail.GetError()] {
			err = &UndeclaredFailError{Step: node.GetId(), Name: fail.GetError(), Declared: DeclaredErrorNames(wf)}
		}
	}})

	return err
}

// UndeclaredFailError is a `fail:` that names an error its workflow does not
// declare. A type of its own so the Flowfile validator, which reports it at the
// step's own `fail.error` with a did-you-mean, can tell it from a fault in the
// declarations themselves.
type UndeclaredFailError struct {
	Step     string
	Name     string
	Declared []string
}

func (e *UndeclaredFailError) Error() string {
	message := fmt.Sprintf("step %q raises %q, which this workflow does not declare under `errors:`", e.Step, e.Name)
	if len(e.Declared) > 0 {
		message += "; it declares " + strings.Join(e.Declared, ", ")
	}

	return message
}

func errorKindNames() []string {
	kinds := ErrorKinds()
	names := make([]string, 0, len(kinds))
	for _, kind := range kinds {
		names = append(names, kind.String())
	}

	return names
}

// DeclaredErrorNames returns the names wf declares under `errors:`, in the order
// written.
func DeclaredErrorNames(wf *Workflow) []string {
	names := make([]string, 0, len(wf.GetDeclaredErrors()))
	for _, declaration := range wf.GetDeclaredErrors() {
		names = append(names, declaration.GetName())
	}

	return names
}

// KnownFailureKind reports whether kind names a failure a step of wf can record:
// a built-in [ErrorKind] or one wf declares. A workflow that calls another can
// also see the callee's declared kinds, which is why a `call:` step's own kind
// is not judged by this.
func KnownFailureKind(wf *Workflow, kind string) bool {
	if _, builtin := ParseErrorKind(kind); builtin {
		return true
	}

	return slices.Contains(DeclaredErrorNames(wf), kind)
}

// ParseReportedKind recognizes a string that crossed a driver boundary as the
// kind of a run or step failure: a built-in [ErrorKind], or the name of an error
// a workflow declared.
//
// The boundary (Temporal's ApplicationError.Type) carries no workflow, so a
// declared name is recognized by its spelling, the `^[A-Z][A-Za-z0-9_]*$` rule a
// declaration must satisfy, and not by membership. That is looser than
// [ParseErrorKind] on purpose and only here: a run's reported kind is a label for
// operators and agents, never an input to retry or tolerance, which decide on
// the workflow's own declarations and so stay closed.
func ParseReportedKind(s string) (ErrorKind, bool) {
	if kind, ok := ParseErrorKind(s); ok {
		return kind, true
	}
	if s == "" || s[0] < 'A' || s[0] > 'Z' {
		return "", false
	}
	for i := 1; i < len(s); i++ {
		c := s[i]
		if !(c >= 'a' && c <= 'z' || c >= 'A' && c <= 'Z' || c >= '0' && c <= '9' || c == '_') {
			return "", false
		}
	}

	return ErrorKind(s), true
}

// EvalFailNode raises what a `fail:` step names: the [TaskError] a driver treats
// like any other step failure, classified as the declared kind.
//
// Both drivers call this one function from workflow-side code, so the failure a
// step raises, its sentence and its kind cannot be spelled twice. The returned
// cost is the CEL cost of evaluating the message, which the durable driver
// charges against the segment as it does a `value:`'s.
func EvalFailNode(ctx context.Context, fail *Fail, scope *Scope) (uint64, error) {
	message := fail.GetError()

	var cost uint64
	if fail.GetMessage() != nil {
		outputs, c, err := EvalValueNodeWithCost(ctx, fail.GetMessage(), scope)
		cost = c
		if err != nil {
			return cost, fmt.Errorf("evaluating fail message: %w", err)
		}

		literal := outputs.GetNamedValues()[ValueOutput].GetLiteral()
		if _, isString := literal.GetKind().(*expr.Value_StringValue); !isString {
			return cost, fmt.Errorf("a `fail:` message must be a string, and this one is %T", literal.GetKind())
		}
		message = literal.GetStringValue()
	}

	if len(message) > MaxFailMessageBytes {
		return cost, fmt.Errorf("a `fail:` message is %d bytes; the most a failure carries is %d", len(message), MaxFailMessageBytes)
	}

	return cost, &TaskError{Task: FailTask, Kind: ErrorKind(fail.GetError()), Err: errors.New(message)}
}
