package lsp

import (
	v1 "github.com/picatz/flowstate/pkg/flowstate/v1"
)

// A task step's `outputs:` input replaces the task's declared outputs with
// names the author defines — the same replacement a `wait_for_signal:`'s
// shaping performs, written one level deeper because for a task the key is an
// input the task itself evaluates. Hover and completion must therefore stop
// reading the descriptor the moment a *shaping* task is given one: the
// descriptor describes a set the step no longer produces, and prose derived
// from it — "the task does not declare an output named X" — is false for
// exactly the names the author is pointing at.

// taskShapingKey is the input a shaping task reads its replacement outputs
// from.
//
// The engine's own name for it, so the editor cannot come to disagree with the
// compiler about which key shapes. Which *tasks* read it that way is
// [v1.TaskShapesOutputs] — a declared capability rather than the presence of
// this name, which is what the editor and the validator used to decide by, and
// what they were both wrong about for any plugin that named an ordinary input
// `outputs` (#324).
const taskShapingKey = v1.ShapingInput

// shapingEntry returns the step's `outputs:` shaping input, or nil for a step
// that does not shape.
//
// Only a task step can have one: s.inputs is populated from the task entry's
// mapping, so a wait's `outputs:` (held in waitShapingEntries) and the
// workflow's declared `outputs:` (held on the file) cannot reach here. And only
// a task that *declares* shaping, so an ordinary input by that name on any
// other task is an ordinary input, completed and hovered as one.
func (s *parsedStep) shapingEntry(tasks *v1.Registry) *entry {
	if s.taskEntry == nil || !registryShapesOutputs(tasks, s.taskName) {
		return nil
	}
	return s.input(taskShapingKey)
}

// shapes reports whether this step replaces its task's declared outputs.
//
// Written for the line scan, which sees an input's key without its value and so
// cannot ask [shapedOutputNames] anything: what it needs is only whether the
// declared names still describe the step.
func (s *outlineStep) shapes(tasks *v1.Registry) bool {
	return registryShapesOutputs(tasks, s.taskName) && containsKey(s.inputKeys, taskShapingKey)
}

// registryShapesOutputs asks *this server's* registry whether a task shapes.
//
// [v1.TaskShapesOutputs] asks the default registry, which is the right question
// for the compiler and the validator — both build from the built-ins alone — and
// the wrong one here. The registry is a property of the server ([doc.Tasks]):
// `flow lsp` opens a plugin host, registers what it found, and hands that
// registry in, so a shaping plugin task is registered *there* and unknown to the
// default one. Asked the wrong registry, a plugin that declares shaping reads as
// a task that does not, and the editor offers the outputs its descriptor
// declares — the exact names the author's `outputs:` replaced, which is the
// failure #324 is the record of, arrived at from the other side.
//
// Nil is the default registry, which is what every other lookup in this package
// already means by it (see [newDocument]).
func registryShapesOutputs(tasks *v1.Registry, taskName string) bool {
	if tasks == nil {
		return v1.TaskShapesOutputs(taskName)
	}
	def, found := tasks.Lookup(taskName)
	return found && def.ShapesOutputs
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
//
// The second spelling is answered by [v1.ShapedNamesInSource] rather than here,
// and that is the point of the split: what counts as a statically knowable map
// is an opinion, the validator holds the same one, and an editor holding its own
// copy of it is how the two come to say different things about one file.
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

	return v1.ShapedNamesInSource(e.value.expr)
}

// containsKey reports whether a list of keys holds one.
func containsKey(keys []string, want string) bool {
	for _, key := range keys {
		if key == want {
			return true
		}
	}
	return false
}
