package flowstatev1

import (
	"context"
	"fmt"
	"maps"
	"slices"
	"strings"
	"sync"

	protoreflect "google.golang.org/protobuf/reflect/protoreflect"
)

// A TaskFunc executes one task, given its resolved inputs and the scope its own
// expressions are evaluated against.
//
// The scope carries prior step outputs and any variables bound by enclosing
// control flow, which is what lets a task inside a loop evaluate an expression
// referring to the current item.
type TaskFunc func(ctx context.Context, inputs map[string]*Value, scope *Scope) (*Node_Outputs, error)

// A TaskDef describes a task: what it is called, how the engine must treat its
// inputs, and how to run it.
//
// This is the single source of truth for a task. Execution dispatch, spec
// validation, editor completion, and documentation all derive from these
// definitions, so a task cannot behave one way and document another. In
// particular, the engine consults these fields rather than testing a task's name,
// which is what keeps adding a task from requiring engine changes.
type TaskDef struct {
	// Name is how a Flowfile refers to this task.
	Name string

	// Summary is a one-line description, shown by `flow tasks` and in editor
	// completion.
	Summary string

	// Inputs and Outputs describe the task's schema.
	//
	// These are the descriptors of the generated Protobuf messages, which makes
	// the schema the single definition of a task's shape: field names, types,
	// repeated-ness, and the protovalidate constraints all come from the same
	// place the engine validates against. Tooling — editor completion, hover
	// documentation, generated reference docs — reads these rather than
	// maintaining its own copy, so it cannot describe a task the engine would
	// reject.
	//
	// Either may be nil for a task whose shape is not expressed as a message.
	Inputs  protoreflect.MessageDescriptor
	Outputs protoreflect.MessageDescriptor

	// DeferredInputs names inputs whose expressions the task evaluates itself,
	// in a scope the workflow does not have.
	//
	// The engine resolves expression inputs before scheduling a step, which
	// keeps payloads small. That is wrong for an input like the http task's
	// `outputs`, whose expression references response variables that exist only
	// after the request completes: resolving it early fails with an unresolved
	// reference, since the workflow has no `body` or `status_code` in scope.
	// Inputs named here are passed through untouched.
	DeferredInputs []string

	// NeedsPrevOutputs reports whether the task must receive the outputs of
	// earlier steps.
	//
	// Most tasks do not: the engine resolves their expressions up front, so
	// there is nothing left to look up, and sending prior outputs would put
	// data into the payload for no reason. A task that evaluates expressions
	// itself does need them.
	NeedsPrevOutputs bool

	// Fn executes the task.
	Fn TaskFunc
}

// deferred reports whether the named input is evaluated by the task itself.
func (d TaskDef) deferred(input string) bool {
	return slices.Contains(d.DeferredInputs, input)
}

// A Registry holds the tasks available to workflows.
//
// A Registry is safe for concurrent use. Workers register the tasks they provide
// at startup and the engine reads them per step.
type Registry struct {
	mu    sync.RWMutex
	tasks map[string]TaskDef
}

// NewRegistry returns an empty [Registry].
func NewRegistry() *Registry {
	return &Registry{tasks: make(map[string]TaskDef)}
}

// Register adds a task definition, replacing any task already registered under
// the same name.
//
// It reports an error for a definition that could not be executed — an empty
// name or a nil function — so a misconfigured worker fails at startup rather
// than mid-run.
func (r *Registry) Register(def TaskDef) error {
	if def.Name == "" {
		return fmt.Errorf("task definition has no name")
	}
	if def.Fn == nil {
		return fmt.Errorf("task %q has no function", def.Name)
	}

	// A step names its task directly, so a task named for part of the step
	// grammar would make one key mean two things — and both readings would be
	// legitimate, which is not something a parser can resolve. Refused here
	// because this is the only moment the name is chosen; see stepkeys.go.
	if IsReservedStepKey(def.Name) {
		return fmt.Errorf("task %q takes a name the step grammar already uses, "+
			"so `%s:` on a step would be ambiguous; the reserved names are %s",
			def.Name, def.Name, strings.Join(ReservedStepKeys(), ", "))
	}

	r.mu.Lock()
	defer r.mu.Unlock()
	r.tasks[def.Name] = def
	return nil
}

// MustRegister adds a task definition, panicking if it is invalid.
//
// It is meant for package initialization, where an invalid definition is a
// programming error rather than a runtime condition.
func (r *Registry) MustRegister(def TaskDef) {
	if err := r.Register(def); err != nil {
		panic("flowstate: " + err.Error())
	}
}

// Lookup returns the definition registered under name.
func (r *Registry) Lookup(name string) (TaskDef, bool) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	def, ok := r.tasks[name]
	return def, ok
}

// Names returns the registered task names, sorted.
func (r *Registry) Names() []string {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return slices.Sorted(maps.Keys(r.tasks))
}

// All returns the registered definitions, sorted by name.
func (r *Registry) All() []TaskDef {
	r.mu.RLock()
	defer r.mu.RUnlock()
	defs := make([]TaskDef, 0, len(r.tasks))
	for _, name := range slices.Sorted(maps.Keys(r.tasks)) {
		defs = append(defs, r.tasks[name])
	}
	return defs
}

// defaultRegistry holds the built-in tasks, populated on first use.
var defaultRegistry = sync.OnceValue(func() *Registry {
	r := NewRegistry()
	for _, def := range builtinTasks() {
		r.MustRegister(def)
	}
	return r
})

// DefaultRegistry returns the registry of built-in tasks.
func DefaultRegistry() *Registry {
	return defaultRegistry()
}

// LookupTask returns the built-in task definition registered under name.
//
// The engine uses this to decide how to treat a step's inputs without knowing
// anything about specific tasks.
func LookupTask(name string) (TaskDef, bool) {
	return DefaultRegistry().Lookup(name)
}

// TaskNames returns the names of the built-in tasks, sorted.
func TaskNames() []string {
	return DefaultRegistry().Names()
}

// ResolvableInputs partitions a task's inputs into those the engine should
// resolve before scheduling the step and those the task evaluates itself.
//
// An unknown task name yields no deferred inputs; execution then fails with an
// unknown-task error, which is a clearer report than a resolution failure.
func ResolvableInputs(taskName string, inputs map[string]*Value) (resolve, defer_ map[string]*Value) {
	def, known := LookupTask(taskName)
	resolve = make(map[string]*Value, len(inputs))
	defer_ = make(map[string]*Value)
	for name, v := range inputs {
		if known && def.deferred(name) {
			defer_[name] = v
			continue
		}
		resolve[name] = v
	}
	return resolve, defer_
}

// TaskNeedsPrevOutputs reports whether the named task must receive the outputs
// of earlier steps.
func TaskNeedsPrevOutputs(taskName string) bool {
	def, ok := LookupTask(taskName)
	return ok && def.NeedsPrevOutputs
}
