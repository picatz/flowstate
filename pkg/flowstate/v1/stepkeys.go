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
// grammarStepKeys are the words a step accepts today.
//
// The parser has its own spelling of this set, because it needs them in the order
// it reports them and split by what they mean. The two are held together by
// TestEveryGrammarKeyIsReserved rather than by anyone remembering: a key added
// there and not here is a task name a plugin may still take, which is the
// ambiguity this whole file exists to prevent.
var grammarStepKeys = []string{
	// Step properties.
	"id",
	"task",
	"description",
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
}

// futureStepKeys are reserved for grammar that is planned and not built.
//
// Reserving a word before it exists means adding it later is a change to this
// package rather than a break for whoever registered a task under the name in the
// meantime. Cheap now; a compatibility problem later.
//
// It is deliberately short. Every word here is one a plugin may not use, so
// reserving speculatively has a real cost to somebody — `queue` and `schedule`
// are plausible task names, and taking them on the strength of a roadmap noun
// would be spending someone else's namespace on a guess. A word earns a place
// here by being a *step key* in a design that named it as one, not by appearing
// in a list of things the engine will eventually do.
var futureStepKeys = []string{
	"call",
	"vars",
	"undo",
	"needs",
}

// reservedStepKeys is every word a task name may not take.
var reservedStepKeys = slices.Concat(grammarStepKeys, futureStepKeys)

// ReservedStepKeys returns the step keys a task name may not take.
func ReservedStepKeys() []string { return slices.Clone(reservedStepKeys) }

// IsReservedStepKey reports whether name is spoken for by the step grammar,
// whether it is in use today or held for later.
func IsReservedStepKey(name string) bool { return slices.Contains(reservedStepKeys, name) }

// IsFutureStepKey reports whether name is reserved for grammar that does not
// exist yet.
//
// The distinction is for diagnostics. A step written with `vars:` on it is not the
// same mistake as one written with `varz:`, and telling an author the key is
// unknown — listing it among the keys they could have meant instead — describes a
// typo they did not make. What they wrote is a key this version has not built.
func IsFutureStepKey(name string) bool { return slices.Contains(futureStepKeys, name) }
