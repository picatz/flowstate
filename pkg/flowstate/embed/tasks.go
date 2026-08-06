package embed

import (
	"fmt"
	"sync"

	v1 "github.com/picatz/flowstate/pkg/flowstate/v1"
	protoreflect "google.golang.org/protobuf/reflect/protoreflect"
)

// A Task is a Go function an embedding program registers as a workflow task,
// alongside the metadata that lets validation, editor completion, and
// generated documentation describe it.
//
// This is the curated shape of [v1.TaskDef]: the fields an embedder actually
// needs to fill in, with everything about secrets, authority, and deferred
// evaluation left at its zero value — an embedded task speaks for itself and
// leaves that machinery to the built-in tasks that need it.
type Task struct {
	// Name is how a Flowfile refers to this task.
	Name string

	// Summary is a one-line description, shown by `flow tasks` and in editor
	// completion.
	Summary string

	// Inputs and Outputs describe the task's shape as the descriptors of
	// generated protobuf messages, the same contract [v1.TaskDef] documents.
	//
	// Either may be left nil for a task with no `.proto` message describing
	// its shape. That is a real, supported escape hatch — not every embedder
	// wants to define a schema for a task used in one program — but it costs
	// something every time it is taken: `flow validate`, the language
	// server, and generated reference docs can check and document only what
	// a descriptor tells them, so a nil-descriptor task is invisible to all
	// three beyond its name and summary. Give it a descriptor when the task
	// will be written into more than one Flowfile, or read by anyone who
	// isn't its author.
	Inputs  protoreflect.MessageDescriptor
	Outputs protoreflect.MessageDescriptor

	// Fn executes the task.
	Fn v1.TaskFunc
}

// toDef renders t as the [v1.TaskDef] the registry actually stores.
func (t Task) toDef() v1.TaskDef {
	return v1.TaskDef{
		Name:    t.Name,
		Summary: t.Summary,
		Inputs:  t.Inputs,
		Outputs: t.Outputs,
		Fn:      t.Fn,
	}
}

// Tasks is a set of custom tasks an embedding program registers, in addition
// to whatever this build already provides.
//
// # Why validation and execution need telling separately
//
// Validating a Flowfile and running it ask two different questions about a
// task's name, of two different registries, and they always have — see
// [v1.LookupTask] and [v1.LookupTaskIn]'s docs. [flowfile.Validate] and the
// language server read the process-wide [v1.DefaultRegistry] to ask "does
// this build know a task by this name at all", because that question is a
// property of the build and has to have one answer regardless of which run
// asks it — [Compile] itself does not ask it; see its doc. [RunLocal] and
// the durable driver's activities read a *run's* own registry to ask "what
// does this Fn actually do", because two different embedders — or two
// different [RunLocal] calls in the same process — must never see each
// other's tasks.
//
// A Tasks set is built once and consulted for both questions with its own
// method for each: [Tasks.Install] answers the first, by registering into
// [v1.DefaultRegistry] for as long as it stays installed, so [Compile] can
// see the task's name and shape. [RunOptions.Tasks] answers the second: every
// [RunLocal] call builds a fresh, run-scoped registry from the Tasks set on
// its own [RunOptions], independent of whether Install was ever called — see
// [RunLocal]'s doc for what that means for the two questions disagreeing.
//
// A Tasks value is safe for concurrent use.
type Tasks struct {
	mu    sync.Mutex
	tasks map[string]Task
}

// NewTasks returns an empty [Tasks] set.
func NewTasks() *Tasks {
	return &Tasks{tasks: make(map[string]Task)}
}

// Register adds a task, replacing any task already registered under the same
// name in this set.
//
// It reports an error for a definition [v1.Registry.Register] would refuse —
// no name, no function, or a name the step grammar already uses — so a
// misconfigured embedder fails at registration time rather than mid-run or,
// worse, mid-validation.
func (t *Tasks) Register(task Task) error {
	if task.Name == "" {
		return fmt.Errorf("flowstate/embed: task has no name")
	}
	if task.Fn == nil {
		return fmt.Errorf("flowstate/embed: task %q has no function", task.Name)
	}

	// Ask a scratch registry to validate the definition — reserved-name
	// checking and the rest of [v1.Registry.Register]'s rules — without
	// touching anything global. A Tasks set is meant to be built and
	// installed independently of any other embedder's in the same process,
	// and validating against [v1.DefaultRegistry] here would fail a
	// perfectly good registration the moment two embedders happened to reuse
	// a name, which is exactly the disagreement [Tasks.Install] exists to
	// report at the one moment — install time — where it is actually a
	// conflict.
	if err := v1.NewRegistry().Register(task.toDef()); err != nil {
		return fmt.Errorf("flowstate/embed: %w", err)
	}

	t.mu.Lock()
	defer t.mu.Unlock()
	if t.tasks == nil {
		t.tasks = make(map[string]Task)
	}
	t.tasks[task.Name] = task
	return nil
}

// defs returns every registered task's definition, for [Tasks.Install] and
// [RunOptions.Tasks] to consult without exposing the set's internal map.
func (t *Tasks) defs() []v1.TaskDef {
	t.mu.Lock()
	defer t.mu.Unlock()
	defs := make([]v1.TaskDef, 0, len(t.tasks))
	for _, task := range t.tasks {
		defs = append(defs, task.toDef())
	}
	return defs
}

// Install registers every task in this set into [v1.DefaultRegistry], so
// [flowfile.Validate] and anything else asking what this build knows —
// `flow`'s own language server, if this program also exposes one — can see
// them, and returns a func that undoes exactly this call.
//
// Call it before validating a Flowfile that names one of these tasks: a step
// naming a task [v1.DefaultRegistry] does not know is reported "unknown
// task" by [flowfile.Validate], the same diagnostic a misspelled built-in
// task gets — [Compile] itself does not run that check, so a workflow with
// an uninstalled task's name still compiles cleanly and only fails once a
// run actually reaches that step; see [Compile]'s doc. Install is not
// required before [RunLocal]: see [RunOptions.Tasks] for why execution reads
// the Tasks set directly and does not need this call at all.
//
// The whole registration — every task in the set — happens as one unit under
// [v1.LockDefaultRegistry], so a concurrent Install of a different Tasks set,
// or a concurrent `flow test` run in the same process, can never observe half
// of this set registered. Returned uninstall restores exactly what Install
// found: a name that was already registered to something else gets that
// definition back, and a name this set introduced is removed with
// [v1.Registry.Unregister] — not merely overwritten, so a step naming it
// after uninstall is unknown again, exactly as if it had never been
// installed.
//
// Calling Install twice on the same set installs twice; call the first
// uninstall before installing again, or the second install's uninstall will
// restore only to the state the first install left behind.
func (t *Tasks) Install() (uninstall func()) {
	defs := t.defs()

	unlock := v1.LockDefaultRegistry()
	defer unlock()

	registry := v1.DefaultRegistry()

	type saved struct {
		def     v1.TaskDef
		existed bool
	}
	originals := make(map[string]saved, len(defs))

	for _, def := range defs {
		if existing, ok := registry.Lookup(def.Name); ok {
			originals[def.Name] = saved{def: existing, existed: true}
		} else {
			originals[def.Name] = saved{existed: false}
		}
		// Already validated by [Tasks.Register]; a failure here would mean
		// this set's own bookkeeping disagreed with what it registered,
		// which is a defect in this package rather than something an
		// embedder configured.
		if err := registry.Register(def); err != nil {
			panic("flowstate/embed: " + err.Error())
		}
	}

	return func() {
		unlock := v1.LockDefaultRegistry()
		defer unlock()

		for name, s := range originals {
			if s.existed {
				// Panics for the same reason as above: this is putting back a
				// definition the registry already accepted once.
				if err := registry.Register(s.def); err != nil {
					panic("flowstate/embed: restoring " + name + ": " + err.Error())
				}
				continue
			}
			registry.Unregister(name)
		}
	}
}
