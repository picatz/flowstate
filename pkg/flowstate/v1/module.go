package flowstatev1

import (
	"errors"
	"fmt"

	"google.golang.org/protobuf/reflect/protoreflect"
)

// moduleFields are the only fields of a [Workflow] a module may carry: what it
// is called, what it is for, the vocabulary it declares, and the profile and
// source digest the compiler stamps on a file it compiles. Neither stamp changes
// what a file does.
var moduleFields = map[protoreflect.Name]bool{
	"name":               true,
	"description":        true,
	"profile":            true,
	"source_digest":      true,
	"declared_types":     true,
	"declared_functions": true,
	"declared_errors":    true,
	"modules":            true,
}

// IsModule reports whether w is a module: a Flowfile that declares types,
// functions or errors and nothing that runs.
//
// A module is not a new kind of message. It is a [Workflow] with no steps, which
// the schema already refuses to run (`steps` requires an item), so the property
// is derived rather than stored and a spec cannot claim it falsely: there is no
// marker to set on something that has steps. Both drivers and the submit path
// refuse a spec with no steps whether or not it is a module.
//
// "Nothing that runs" is judged on content: an empty `inputs: {}` compiles to the
// same message as no `inputs:` at all, so an empty block is ignored. That is
// harmless, since it carries no behavior and the file is still unrunnable.
//
// Everything beyond [moduleFields] disqualifies the file, so a field added to
// [Workflow] later makes a file that uses it a broken workflow rather than a
// silently valid module. A file declaring nothing at all is not a module either:
// it is an empty workflow, and calling it a module would hide that mistake.
func IsModule(w *Workflow) bool {
	if w == nil || len(w.GetSteps()) > 0 {
		return false
	}
	if len(w.GetDeclaredTypes()) == 0 && len(w.GetDeclaredFunctions()) == 0 && len(w.GetDeclaredErrors()) == 0 {
		return false
	}

	module := true
	w.ProtoReflect().Range(func(fd protoreflect.FieldDescriptor, _ protoreflect.Value) bool {
		if !moduleFields[fd.Name()] {
			module = false
		}
		return module
	})
	return module
}

// ErrModule is what running a module is refused with. The text completes a
// sentence about the file ("lib/ids.yaml is a module ...").
var ErrModule = errors.New("is a module (no steps); import it with use:, don't run it")

// RefuseEmpty is the error a driver or submit path answers a workflow with no
// steps: [ErrModule] when the spec is a module, so the author learns what the
// file is, and the plain refusal otherwise. Nil when there is something to run.
func RefuseEmpty(w *Workflow) error {
	switch {
	case w != nil && len(w.GetSteps()) > 0:
		return nil
	case IsModule(w):
		return fmt.Errorf("workflow %q %w", w.GetName(), ErrModule)
	default:
		return errors.New("workflow cannot be nil or empty")
	}
}
