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

// installedExactly reports whether every task in t is currently registered
// in [v1.DefaultRegistry] as *this Tasks set's own* Install, not merely a
// task of the same name existing there at all.
//
// The distinction is [RunDurable]'s whole precondition: a task NAME
// existing in [v1.DefaultRegistry] is true for every built-in the moment
// the process starts, whether or not this set was ever installed — so
// checking existence alone would let a custom override of a built-in (a
// program's own "log", say) pass the check without [Tasks.Install] having
// run at all, and a durable worker would then execute the *built-in*
// log while [RunLocal] executes the program's own — two drivers silently
// disagreeing about what one step does. [installOwners] is authoritative for
// which Tasks set, if any, currently owns a name's registration — set only
// by [Tasks.Install] and cleared only by its uninstall — so comparing
// against it answers "is the definition mine" rather than "does a
// definition exist".
func (t *Tasks) installedExactly() (missing string, ok bool) {
	defs := t.defs()

	unlock := v1.LockDefaultRegistry()
	defer unlock()

	for _, def := range defs {
		if installOwners[def.Name] != t {
			return def.Name, false
		}
	}
	return "", true
}

// installOwners tracks, for every task name currently claimed by some
// [Tasks] set's still-live Install call, which set claimed it.
//
// Guarded by [v1.LockDefaultRegistry] rather than a mutex of its own — every
// accessor already holds that lock for the registry mutation this
// bookkeeping accompanies, so a second lock could only add a second order to
// get wrong, not add any real independence.
//
// This is what makes two different Tasks sets contending for one name a
// refusal at Install time rather than a race two later, independent
// uninstalls can corrupt. Without it: A installs "x", B installs "x" (saving
// A's definition as what B will restore), A uninstalls "x" (removing it
// entirely, since A never knew B had taken it over), and B uninstalls "x"
// (restoring A's definition) — "x" is now permanently registered, installed
// by neither remaining live Tasks set, and nothing will ever remove it.
// Comparing the registry's *current* value against what an uninstall
// personally wrote before touching it (a compare-and-swap) does not fix
// this either: in the same trace, A's uninstall would see B's definition in
// place of its own and correctly decline to touch it, but B's uninstall
// still finds its own definition exactly as it left it and still restores
// A's — the leaked registration is unavoidable once two lifecycles are
// allowed to overlap on one name at all, whatever runs at teardown. Refusing
// the second Install outright is the only design where B's take-over never
// happens, so there is nothing for either uninstall to disagree about.
var installOwners = make(map[string]*Tasks)

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
// Install refuses — returning a non-nil error and a nil uninstall, having
// registered nothing — when any task in this set names something already
// claimed by a *different*, still-installed Tasks set. This is a real
// runtime condition, not a defect in this package: two independently
// authored embedders (or plugins) can legitimately both want to call their
// task "log", and the right answer is to tell whichever one asks second,
// clearly, rather than let the two lifecycles silently overlap — see
// [installOwners]'s doc for the corruption that overlap otherwise produces.
// The fix is always one of: uninstall the other set first, or give this
// task a different name. Installing the very same *Tasks set twice without
// uninstalling in between is also refused, for the same reason — a second,
// unrelated call from the same set is exactly the collision above, just
// with t on both sides.
//
// The whole check-then-registration — every task in the set — happens as
// one unit under [v1.LockDefaultRegistry], so a concurrent Install of a
// different Tasks set, or a concurrent `flow test` run in the same process,
// can never observe half of this set registered, and never races the
// ownership check above against another Install's own. On success, returned
// uninstall restores exactly what this call found: a name that was already
// registered to something else (a built-in task, most commonly) gets that
// definition back, and a name this set introduced is removed with
// [v1.Registry.Unregister] — not merely overwritten, so a step naming it
// after uninstall is unknown again, exactly as if it had never been
// installed. uninstall also re-acquires [v1.LockDefaultRegistry] for its own
// restore, rather than the lock from Install being held open across the
// whole Install-to-uninstall lifetime — holding it that long would block
// every other Install, and every `flow test` case, for as long as this
// program keeps its tasks installed, which can be indefinitely.
func (t *Tasks) Install() (uninstall func(), err error) {
	defs := t.defs()

	unlock := v1.LockDefaultRegistry()
	defer unlock()

	registry := v1.DefaultRegistry()

	for _, def := range defs {
		if _, claimed := installOwners[def.Name]; claimed {
			return nil, fmt.Errorf(
				"flowstate/embed: Install: task %q is already installed by a different Tasks set; "+
					"uninstall it first, or give this task a different name", def.Name)
		}
	}

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
		if err := registry.Replace(def); err != nil {
			panic("flowstate/embed: " + err.Error())
		}
		installOwners[def.Name] = t
	}

	return func() {
		unlock := v1.LockDefaultRegistry()
		defer unlock()

		for name, s := range originals {
			// Always this call's own claim: a name this call registered
			// stays owned by t until this same uninstall runs, because a
			// different Tasks set naming it would have been refused above,
			// and this same set cannot install it again without uninstalling
			// first either. There is therefore nothing to compare against —
			// ownership alone is enough to know this restore is still safe.
			delete(installOwners, name)

			if s.existed {
				// Panics for the same reason as above: this is putting back a
				// definition the registry already accepted once.
				if err := registry.Replace(s.def); err != nil {
					panic("flowstate/embed: restoring " + name + ": " + err.Error())
				}
				continue
			}
			registry.Unregister(name)
		}
	}, nil
}
