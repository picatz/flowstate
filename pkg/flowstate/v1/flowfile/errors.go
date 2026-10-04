package flowfile

import (
	"errors"
	"fmt"
	"slices"
	"strings"

	yaml "github.com/goccy/go-yaml"

	"github.com/goccy/go-yaml/ast"
	"google.golang.org/protobuf/proto"

	v1 "github.com/picatz/flowstate/pkg/flowstate/v1"
	"github.com/picatz/flowstate/pkg/flowstate/v1/nearest"
)

// The `errors:` block and the `fail:` step: the failures a workflow may raise.
//
//	errors:
//	  PaymentDeclined:
//	    description: The card issuer refused the charge.
//
//	steps:
//	  - id: decline
//	    fail:
//	      error: PaymentDeclined
//	      message: card ending ${inputs.last4} was declined
//
// A declared name is a failure kind: the one `${steps.<id>.failure.kind}` reads and
// a failed run reports. What a declaration may not be, a built-in kind's name or
// a duplicate, is [v1.CheckErrorDeclarations]'s to refuse, with the same words at
// submit as at compile.

var (
	errorKeys = []string{"description"}
	failKeys  = []string{"error", "message"}
)

// declaredErrors compiles the top-level `errors:` block, one entry per error, in
// the order written.
func (c *compiler) declaredErrors(n ast.Node, path string, r ref) []*v1.ErrorDeclaration {
	c.pos.record(path, spanOfNode(c.resolveQuiet(n)))

	entries, ok := c.entries(n, path, r)
	if !ok {
		return nil
	}

	// Bounded before anything is built, because the file is the sender's.
	if len(entries) > v1.MaxDeclaredErrors {
		c.report(spanOfNode(c.resolveQuiet(n)), r,
			"declares %d errors; the most a workflow declares is %d", len(entries), v1.MaxDeclaredErrors)

		return nil
	}

	declarations := make([]*v1.ErrorDeclaration, 0, len(entries))
	for _, e := range entries {
		declarations = append(declarations, c.declaredError(e, path))
	}

	if len(declarations) == 0 {
		return nil
	}

	return declarations
}

func (c *compiler) declaredError(e entry, parent string) *v1.ErrorDeclaration {
	path := fieldPath(parent, e.name)
	r := ref{path: path, label: "error " + e.name}

	c.pos.record(path, spanOfNode(c.resolveQuiet(e.value)))

	declaration := &v1.ErrorDeclaration{Name: e.name}

	// An error with nothing to say about itself is written `Name: {}` or with a
	// null; only a mapping can carry keys.
	if _, isNull := c.resolveQuiet(e.value).(*ast.NullNode); isNull {
		return declaration
	}

	entries, ok := c.entries(e.value, path, r)
	if !ok {
		return declaration
	}

	fields := c.check(entries, r, errorKeys)

	if f, found := fields.get("description"); found {
		descriptionPath := fieldPath(path, "description")
		if description, ok := c.text(f.value, descriptionPath,
			ref{path: descriptionPath, label: "error " + e.name + " description"}); ok {
			declaration.Description = proto.String(description)
		}
	}

	return declaration
}

// fail compiles a step's `fail:` mapping.
func (c *compiler) fail(n ast.Node, path string, r ref) *v1.Fail {
	fields, ok := c.fields(n, path, r, failKeys)
	if !ok {
		return nil
	}

	fail := &v1.Fail{}

	if f, found := fields.get("error"); found {
		errorPath := fieldPath(path, "error")
		if name, ok := c.text(f.value, errorPath, ref{step: r.step, path: errorPath, label: "fail error"}); ok {
			fail.Error = name
		}
	} else {
		c.report(spanOfNode(n), r, "fail requires error, the declared name of what is raised (see the top-level `errors:`)")
	}

	if f, found := fields.get("message"); found {
		messagePath := fieldPath(path, "message")
		c.pos.record(messagePath, spanOfNode(c.resolveQuiet(f.value)))

		// Fence-optional text like a log message: a bare string is words, and
		// `${...}` interpolates, so a message reads the way it is written.
		fail.Message = c.inputValue(f.value, messagePath,
			ref{step: r.step, path: messagePath, label: "fail message"})
	}

	return fail
}

// declaredErrorsToYAML is the inverse of [compiler.declaredErrors].
func declaredErrorsToYAML(declared []*v1.ErrorDeclaration) yaml.MapSlice {
	out := make(yaml.MapSlice, 0, len(declared))
	for _, d := range declared {
		var entry yaml.MapSlice
		if d.Description != nil {
			entry = append(entry, yaml.MapItem{Key: "description", Value: textToYAML(d.GetDescription())})
		}
		if len(entry) == 0 {
			out = append(out, yaml.MapItem{Key: d.GetName(), Value: yaml.MapSlice{}})
			continue
		}
		out = append(out, yaml.MapItem{Key: d.GetName(), Value: entry})
	}

	return out
}

// failToYAML is the inverse of [compiler.fail].
func failToYAML(fail *v1.Fail) (yaml.MapSlice, error) {
	out := yaml.MapSlice{{Key: "error", Value: fail.GetError()}}

	if fail.GetMessage() != nil {
		message, err := inputValueToYAML(fail.GetMessage())
		if err != nil {
			return nil, fmt.Errorf("message: %w", err)
		}
		out = append(out, yaml.MapItem{Key: "message", Value: message})
	}

	return out, nil
}

// validateDeclaredErrors reports what is wrong with the `errors:` block and with
// every `fail:` that names an error, by the rules submit enforces.
func validateDeclaredErrors(wf *v1.Workflow) Diagnostics {
	if err := v1.CheckErrorDeclarations(wf); err != nil {
		// A `fail:` naming an error nobody declared is reported where it is
		// written, by [validateFail].
		if _, undeclared := errors.AsType[*v1.UndeclaredFailError](err); undeclared {
			return nil
		}

		return Diagnostics{{Field: "errors", Message: err.Error()}}
	}

	return nil
}

// validateFail checks one `fail:` step: that it names a declared error, with a
// did-you-mean, and that its message reads only what is in scope.
func validateFail(id string, fail *v1.Fail, scope refScope, index int, wf *v1.Workflow) Diagnostics {
	var ds Diagnostics

	declared := v1.DeclaredErrorNames(wf)
	if !slices.Contains(declared, fail.GetError()) {
		message := fmt.Sprintf("raises %q, which this workflow does not declare under `errors:`", fail.GetError())
		if suggestion, ok := nearest.Name(fail.GetError(), declared); ok {
			message += fmt.Sprintf("; did you mean %q?", suggestion)
		} else if len(declared) > 0 {
			message += "; it declares " + strings.Join(declared, ", ")
		} else {
			message += "; declare it as `errors: {" + fail.GetError() + ": {}}`"
		}

		ds = append(ds, Diagnostic{
			Step: id, Field: "fail.error", Value: fail.GetError(), Message: message,
			Code: v1.DiagnosticCodeUnresolvedReference,
		})
	}

	if fail.GetMessage() != nil {
		ds = append(ds, validateInputRefs(id, "fail.message", fail.GetMessage(), scope, index, wf)...)
	}

	return ds
}

// validatePolicyKinds reports the kind lists on a step's policy that name a kind
// this workflow cannot fail with, or ask for a retry that never happens, each at
// the key the author wrote. The rule is [v1.PolicyKindProblems], the same one
// submit applies; this attaches a position and, for a misspelled kind, the
// nearest one.
func validatePolicyKinds(id string, node *v1.Node, wf *v1.Workflow) Diagnostics {
	var ds Diagnostics

	for _, problem := range v1.PolicyKindProblems(wf, node) {
		message := strings.TrimPrefix(problem.Message, fmt.Sprintf("step %q: ", id))
		if !v1.KnownFailureKindAt(wf, node, problem.Kind) && problem.Kind != "" {
			known := append(errorKindNames(), v1.FailureKindNamesAt(wf, node)...)
			if suggestion, ok := nearest.Name(problem.Kind, known); ok {
				message += fmt.Sprintf("; did you mean %q?", suggestion)
			}
		}

		ds = append(ds, Diagnostic{
			Step: id, Field: problem.Field, Value: problem.Kind, Message: message,
			Code: v1.DiagnosticCodeConstraintViolation,
		})
	}

	return ds
}

func errorKindNames() []string {
	kinds := v1.ErrorKinds()
	names := make([]string, 0, len(kinds))
	for _, kind := range kinds {
		names = append(names, kind.String())
	}

	return names
}
