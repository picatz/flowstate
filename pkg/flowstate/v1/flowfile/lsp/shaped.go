package lsp

import (
	celast "github.com/google/cel-go/common/ast"
	v1 "github.com/picatz/flowstate/pkg/flowstate/v1"
)

// A task step's `outputs:` input replaces the task's declared outputs with
// names the author defines — the same replacement a `wait_for_signal:`'s
// shaping performs, written one level deeper because for a task the key is an
// input the task itself evaluates. Hover and completion must therefore stop
// reading the descriptor the moment the input is present: the descriptor
// describes a set the step no longer produces, and prose derived from it —
// "the task does not declare an output named X" — is false for exactly the
// names the author is pointing at.

// taskShapingKey is the input whose presence replaces a task's declared
// outputs.
//
// Decided by presence rather than by task name, because that is the
// validator's own rule (flowfile/validate.go's output-reference check): a
// plugin adopting the same shape inherits the exemption there, and this
// package answering from a narrower rule would have the editor contradict the
// diagnostics.
const taskShapingKey = "outputs"

// shapingEntry returns the step's `outputs:` shaping input, or nil for a step
// that does not shape.
//
// Only a task step can have one: s.inputs is populated from the task entry's
// mapping, so a wait's `outputs:` (held in waitShapingEntries) and the
// workflow's declared `outputs:` (held on the file) cannot reach here.
func (s *parsedStep) shapingEntry() *entry {
	if s.taskEntry == nil {
		return nil
	}
	return s.input(taskShapingKey)
}

// shapedOutputNames returns the names a shaping entry defines, when they are
// statically knowable, and reports whether they are.
//
// Two spellings are knowable: a YAML mapping, whose keys are the names
// directly, and a `${...}` whose top level is a CEL map literal with every key
// a string literal. Anything else — a variable, a function call, a computed
// key — yields names only the run can know, and the honest answer is no names
// at all rather than a guess: a fabricated candidate an author accepts is a
// reference nothing may produce.
func shapedOutputNames(e *entry) ([]string, bool) {
	if e == nil || e.value == nil {
		return nil, false
	}

	if e.value.kind == kindMapping {
		names := make([]string, 0, len(e.value.entries))
		for _, kv := range e.value.entries {
			if kv.key == "" {
				return nil, false
			}
			names = append(names, kv.key)
		}
		return names, true
	}

	if !e.value.fenced {
		return nil, false
	}
	env, err := v1.DefaultEvaluator().ProfileEnv(v1.CurrentProfile)
	if err != nil {
		return nil, false
	}
	parsed, issues := env.Parse(e.value.expr)
	if issues != nil && issues.Err() != nil {
		return nil, false
	}
	expr := parsed.NativeRep().Expr()
	if expr.Kind() != celast.MapKind {
		return nil, false
	}
	entries := expr.AsMap().Entries()
	names := make([]string, 0, len(entries))
	for _, entry := range entries {
		if entry.Kind() != celast.MapEntryKind {
			return nil, false
		}
		key := entry.AsMapEntry().Key()
		if key.Kind() != celast.LiteralKind {
			return nil, false
		}
		name, ok := key.AsLiteral().Value().(string)
		if !ok {
			return nil, false
		}
		names = append(names, name)
	}
	return names, true
}
