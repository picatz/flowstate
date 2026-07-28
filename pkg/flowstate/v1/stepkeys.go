package flowstatev1

import "slices"

// The words a step key can be other than the name of a task.
//
// A step in a Flowfile names the work it does directly — `http:` on the step,
// with the request under it, rather than a `task:` block wrapping a `name:`. So a
// key on a step is one of two things — a property of the step, or
// the name of a task — and the reader distinguishes them by asking the registry.
// That only works while the two sets are disjoint. A task named `timeout` would
// make `timeout: 30s` mean two incompatible things in the same position, and no
// amount of care at the parser could recover the author's intent, because both
// readings are legitimate.
//
// So the constraint is enforced at the only moment it can be: when a task chooses
// its name. [Registry.Register] refuses a reserved one, which turns an ambiguous
// grammar into a worker that will not start — a plugin author learns immediately,
// rather than an author of a Flowfile learning much later that their step did
// something else.
//
// This list lives here rather than in the flowfile package because the registry
// has to consult it and cannot import a parser. It is the grammar's vocabulary,
// and the grammar is part of what this package describes.
var reservedStepKeys = []string{
	// Step properties.
	"id",
	"task",
	"if",
	"timeout",
	"retry",
	"continue_on_error",

	// Kinds of work that are not tasks. These name a node kind in the schema
	// rather than anything in the registry, so a task could never provide one.
	"for_each",
	"parallel",
	"sleep",
	"wait_until",
	"wait_for_signal",

	// Not part of the grammar today, and reserved so that adding them later is a
	// change to this package rather than a break for anyone who registered a task
	// under the name in the meantime. Cheap now; a compatibility problem later.
	"description",
	"call",
	"vars",
	"undo",
	"needs",
}

// ReservedStepKeys returns the step keys a task name may not take.
func ReservedStepKeys() []string { return slices.Clone(reservedStepKeys) }

// IsReservedStepKey reports whether name is spoken for by the step grammar.
func IsReservedStepKey(name string) bool { return slices.Contains(reservedStepKeys, name) }
