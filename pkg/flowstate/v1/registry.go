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
//
// # Why this is a Go struct and not a schema message
//
// Types describing this system live in the schema, not as hand-written Go
// structs. This is the exception the rule names: a type defined by a boundary it
// refuses to cross. [TaskDef.Fn] is a Go func and [TaskDef.Inputs]/[TaskDef.Outputs]
// are live [protoreflect.MessageDescriptor] values — a running program's behavior
// and a resolved view of the type registry. Neither survives serialization, so
// this cannot be a message however much of it looks like data.
//
// That leaves three names for task-shaped things, which reads like the same shape
// written down three times and is not. There is one source and two projections,
// each aimed at a different boundary:
//
//   - TaskDef is the truth, in process.
//   - [TaskDescription] is the flattened view a client or editor reads, derived
//     from a TaskDef by [DescribeTask] — descriptors rendered into TaskField
//     lists, because the reader wants field names and types rather than a
//     descriptor it has no registry to resolve.
//   - plugin/v1's TaskManifest is the wire form a plugin sends so the host can
//     rebuild a TaskDef, which is why it carries descriptors as bytes: that is
//     how a descriptor crosses a process. What it deliberately omits, and why, is
//     recorded where it is rebuilt (pkg/flowstate/v1/plugin's taskDef).
//
// Both projections are generated from this struct rather than maintained beside
// it, so adding a field here is not an obligation in two other places — but
// deciding whether a *plugin* can express a new field is a real decision, and it
// belongs in that same rebuild site rather than here.
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

	// ExpressionInputs names inputs that have to be *written* as an expression.
	//
	// Distinct from [TaskDef.DeferredInputs], which says who evaluates an input.
	// This says what an input has to be, and it is the half that was missing: a
	// schema field typed `Value` is deliberately permissive, so `expect:` accepted
	// a mapping, `flow validate` said ok, and the run failed on its first request.
	// A file the validator blesses and the engine refuses is the worst answer the
	// tool can give, because it moves the discovery from the author's terminal to
	// production.
	//
	// Checkable without any scope, which is why it belongs here rather than in the
	// type system: whether a value carries a `${...}` fence is lexical, decided by
	// the parser, and needs nothing about what the expression would evaluate to.
	// What it cannot check is the *type* — an `expect:` that is an expression
	// returning a string is still wrong, and that is Phase 2's problem.
	ExpressionInputs []string

	// NeedsPrevOutputs reports whether the task must receive the outputs of
	// earlier steps.
	//
	// Most tasks do not: the engine resolves their expressions up front, so
	// there is nothing left to look up, and sending prior outputs would put
	// data into the payload for no reason. A task that evaluates expressions
	// itself does need them.
	NeedsPrevOutputs bool

	// AuthorityInputs names inputs whose presence requires the activity to carry
	// the authenticated workload identity and exact execution position. Secret
	// resolution and JIT credential exchange both need that authority, while an
	// ordinary task stays on the legacy activity name for replay compatibility.
	//
	// A plugin task declares its own secret-accepting inputs on a different
	// field entirely — TaskManifest.secret_inputs, plumbed through
	// plugin.Plugin.taskDef — and does not add its names here. That is not an
	// oversight: TaskNeedsAuthority scans a task's actual invocation for a held
	// SecretRef regardless of which input carries it or what a TaskDef declared
	// about that input, so a plugin task with a secret input is already routed
	// to the identity-aware activity by the same scan a built-in task's
	// `bearer:` is, with nothing further to declare here.
	AuthorityInputs []string

	// NestedSecretInputs names the inputs whose entries the task applies itself,
	// one at a time, so a secret reference nested inside a list or a mapping
	// reaches the worker as a reference and is resolved where the value is used.
	//
	// It is not the same question as [TaskDef.AuthorityInputs], which says which
	// inputs need the identity-aware activity, and it is deliberately narrower
	// than "every input of a Value type". An input's resolution has to happen
	// *inside the activity* for a reference to be safe there: the http task's
	// `query` is a Value map like `form` is, and it is absent from this list
	// because a query string is written to access logs, browser history and
	// Referer headers, so the position is wrong however the value gets there.
	//
	// The compiler reads it. A reference nested anywhere else is refused where it
	// is written, with a diagnostic naming the inputs that do accept one, rather
	// than compiling into a specification that fails on its first request.
	NestedSecretInputs []string

	// CredentialInputs is the subset of authority inputs whose literal value
	// names a deployment federation target. Deployment-aware validators use this
	// metadata instead of knowing built-in task names, so AWS-aware and plugin
	// tasks can compose with the same target catalog.
	CredentialInputs []string

	// ShapesOutputs declares that this task evaluates its [ShapingInput] as a
	// replacement for the outputs it declares.
	//
	// One rule with three readers: the validator stands down from checking a
	// step's output references against a set the author replaced, the language
	// server stops offering the descriptor's names, and the compiler compiles a
	// mapping written there per entry so the shaped names stay statically
	// visible. All three used to decide by the *presence of an input called
	// `outputs`*, which is a rule about a spelling rather than about a
	// capability: a plugin declaring an ordinary input by that name got all
	// three surfaces agreeing that its declared outputs had been replaced, while
	// its executor returned exactly the outputs it declared. Nothing flagged it,
	// because the disagreement was between the surfaces and execution rather
	// than among the surfaces.
	//
	// False by default, which is the fail-closed direction: a task that has not
	// said it shapes is checked against what it declares, and an `outputs:`
	// written at one is an unknown input reported where it is written.
	//
	// See plugin/v1's TaskManifest.shapes_outputs for the wire form, and
	// [TaskShapesOutputs] for the question every reader asks.
	ShapesOutputs bool

	// CheckLiteral is what the task alone can say about an input written out in
	// full, before anything runs.
	//
	// The schema answers whether a value is well-formed and the type system
	// whether it fits the field. Neither can answer whether the thing the task
	// would then *do* with it is something this build permits — and the http
	// task's `url:` is the case that made this necessary: `ftp://example.com` is
	// a valid URI, satisfies every rule the schema carries, and is refused by the
	// egress policy on the first request. So `flow validate` said ok on a file
	// that could not run, which is the worst answer the tool can give.
	//
	// Nil for a task with nothing to add, which is most of them. The error's
	// message is shown to an author, so it is written the way the rest of
	// `flowfile` writes one: what is wrong, and what to write instead.
	//
	// # What it must not do
	//
	// It is called by `flow validate` and by the language server on a keystroke,
	// so anything a *run's* environment decides belongs in the task and not here.
	// Resolving a name, opening a connection, or reading configuration a worker
	// has and an editor does not would make a diagnostic depend on where it was
	// asked — and a validator that reports a problem another machine would not
	// have is worse than one that reports nothing, because an author cannot tell
	// which kind they are looking at.
	//
	// Called only for an input written as a literal, and only for one the task
	// does not evaluate itself: there is nothing to check about an expression
	// before it has a scope to be evaluated against.
	CheckLiteral func(input string, value *Value) error

	// Fn executes the task.
	Fn TaskFunc
}

// TaskNeedsAuthority reports whether this concrete task invocation needs the
// identity-aware activity entry point.
func TaskNeedsAuthority(task *Task) bool {
	if task == nil {
		return false
	}
	def, found := LookupTask(task.GetName())
	if !found {
		return false
	}
	for _, name := range def.AuthorityInputs {
		if value := task.GetInputs()[name]; value != nil {
			return true
		}
	}

	// And any input *holding* a reference, wherever in it the reference sits.
	//
	// The named inputs above answer for the ones declared to take a whole value —
	// `bearer:`, `credential:` — and cannot answer for a reference nested inside a
	// header map, because the input carrying it is `headers`, which needs no
	// authority when it holds only strings. Asking the value rather than the name
	// is what keeps the two in step: an input that gains a reference gains the
	// authority to resolve it in the same breath, and a task that never mentions
	// one stays on the activity name replay compatibility depends on.
	for _, value := range task.GetInputs() {
		if ValueHoldsSecretRef(value) {
			return true
		}
	}

	return false
}

// AcceptsNestedSecret reports whether the named input of a task applies its
// entries itself, and so can carry a secret reference nested inside a list or a
// mapping.
//
// Asked by the compiler, which refuses one anywhere else. An unknown task accepts
// none: a build that cannot describe a task cannot promise where that task
// resolves a value, and guessing yes would compile a specification whose only
// discovery of the mistake is a worker resolving a secret somewhere it should not.
func AcceptsNestedSecret(taskName, input string) bool {
	def, found := LookupTask(taskName)
	return found && slices.Contains(def.NestedSecretInputs, input)
}

// NestedSecretInputs returns the inputs of a task that accept a nested reference,
// sorted, for a diagnostic offering an author somewhere to put one.
func NestedSecretInputs(taskName string) []string {
	def, found := LookupTask(taskName)
	if !found {
		return nil
	}

	return slices.Sorted(slices.Values(def.NestedSecretInputs))
}

// CheckLiteralInput asks the task what it can say about a literal input.
//
// Nil when the task has nothing to add or is not registered, which is the same
// answer: a task this build does not have is reported on its own, and inventing a
// complaint about an input of it would be a second diagnostic about a step whose
// real problem is already named.
func CheckLiteralInput(taskName, input string, value *Value) error {
	def, found := LookupTask(taskName)
	if !found || def.CheckLiteral == nil {
		return nil
	}

	return def.CheckLiteral(input, value)
}

// deferred reports whether the named input is evaluated by the task itself.
func (d TaskDef) deferred(input string) bool {
	return slices.Contains(d.DeferredInputs, input)
}

// MustBeExpression reports whether the named input has to be written as one.
func MustBeExpression(taskName, input string) bool {
	def, found := LookupTask(taskName)
	return found && slices.Contains(def.ExpressionInputs, input)
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

// NewBuiltinRegistry returns a new registry containing only the task
// definitions Flowstate ships with.
//
// Unlike [DefaultRegistry], the returned registry never includes tasks added
// by plugin hosts or other process-wide extensions. Callers that need an
// execution capability boundary can therefore add only the custom tasks that
// particular execution is allowed to use.
func NewBuiltinRegistry() *Registry {
	r := NewRegistry()
	for _, def := range builtinTasks() {
		r.MustRegister(def)
	}
	return r
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
	for _, input := range def.CredentialInputs {
		if !slices.Contains(def.AuthorityInputs, input) {
			return fmt.Errorf("task %q credential input %q is not an authority input", def.Name, input)
		}
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

// Unregister removes the task definition registered under name, so a later
// lookup answers unknown again.
//
// There is deliberately no bulk form: a caller restoring a save point after a
// compound sequence of Register calls (see [LockDefaultRegistry]) knows
// exactly which names it added and removes them one at a time, the same way
// it registered them one at a time.
func (r *Registry) Unregister(name string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	delete(r.tasks, name)
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
	return NewBuiltinRegistry()
})

// DefaultRegistry returns the registry every lookup in the engine reads.
//
// It starts as the built-in tasks and is added to at startup by anything
// extending this build's capability — today that is a worker's plugin host. So
// it is the registry to register into and *not* the way to ask what is built in,
// which is what [IsBuiltinTask] is for.
func DefaultRegistry() *Registry {
	return defaultRegistry()
}

// defaultRegistryMu serializes a *compound* sequence of mutations against
// [DefaultRegistry] — save current state, register over it, later restore —
// as seen by every other such sequence.
//
// A single Register or Unregister call is already safe on its own, guarded by
// the [Registry]'s own mutex. What that does not protect is two callers each
// doing save-register-...-restore at once: interleaved, the second caller's
// save can capture the first caller's temporary registrations, and the second
// caller's restore can then put those temporary registrations back after the
// first caller believed it had already undone them. [flowtest]'s per-case
// stub swap and an embedding program's [LockDefaultRegistry]-based install are
// both this shape, and both need to see each other's whole window rather than
// interleave with it — see [LockDefaultRegistry].
var defaultRegistryMu sync.Mutex

// LockDefaultRegistry acquires the lock serializing compound mutations of
// [DefaultRegistry] and returns the matching unlock.
//
// Anything that captures [DefaultRegistry]'s current state, registers one or
// more definitions over it, and later restores what it captured — rather than
// making one isolated Register call — must hold this for the whole window.
// Without it, two such sequences running concurrently can each restore over
// the other's still-in-progress changes.
//
// This does not, on its own, give a *run* isolation from what another
// goroutine registers globally — that is what [NewContextWithRegistry] is
// for, and it is why execution reads a context-scoped registry rather than
// this one (see its doc for issue #195, which this lock does not solve).
// What this protects is narrower and different: the bookkeeping of a
// save-then-restore sequence against the shared global itself, so a
// definition one sequence temporarily installed for compilation cannot be
// clobbered, or wrongly left behind, by another sequence's restore. Exported
// so more than one package's registry-swap can share it instead of adding a
// second, uncoordinated one — see [flowtest]'s per-case stub swap and
// pkg/flowstate/embed's Tasks.Install.
func LockDefaultRegistry() func() {
	defaultRegistryMu.Lock()
	return defaultRegistryMu.Unlock
}

// builtinTaskNames is the set of tasks this build ships, frozen at first use.
//
// Frozen because the question "is this name a built-in" has a fixed answer, and
// the default registry stopped being able to give it the moment anything could
// add to that registry. A plugin host asked exactly that question through
// [LookupTask], which was correct while nothing registered anything — and became
// wrong in a way that reads as a security refusal: after one host registered its
// tasks, a second host opening in the same process was told its own task
// collided with a *built-in*, naming a conflict that does not exist.
var builtinTaskNames = sync.OnceValue(func() map[string]struct{} {
	defs := builtinTasks()

	names := make(map[string]struct{}, len(defs))
	for _, def := range defs {
		names[def.Name] = struct{}{}
	}

	return names
})

// IsBuiltinTask reports whether name is a task this build ships.
//
// Distinct from a lookup in [DefaultRegistry] succeeding, which also answers yes
// for a task a plugin added. Both questions are worth asking and only one of them
// is about what a workflow can rely on being there.
func IsBuiltinTask(name string) bool {
	_, ok := builtinTaskNames()[name]

	return ok
}

// LookupTask returns the built-in task definition registered under name.
//
// The engine uses this to decide how to treat a step's inputs without knowing
// anything about specific tasks.
//
// This reads [DefaultRegistry] and takes no context, so it answers what this
// *build* provides. Anything on the execution path — where a definition's Fn is
// about to be called — must use [LookupTaskIn] instead, so that a run given its
// own registry runs that registry's tasks. See [NewContextWithRegistry].
func LookupTask(name string) (TaskDef, bool) {
	return DefaultRegistry().Lookup(name)
}

// TaskNames returns the names of the built-in tasks, sorted.
func TaskNames() []string {
	return DefaultRegistry().Names()
}

// registryContextKey carries a per-run [Registry] override.
type registryContextKey struct{}

// NewContextWithRegistry returns a context whose runs resolve tasks through
// registry rather than through [DefaultRegistry].
//
// This exists because isolation cannot be built out of mutating a process
// global. `flow test` promises that no task runs for real — no network, no side
// effects — and the only way to keep that promise for a run is to hand *that
// run* the set of tasks it may execute, rather than swapping the global set out
// from under every other goroutine in the process and hoping the timing holds.
// It does not hold: two concurrent runs, or one run racing anything else that
// reads the registry, can each observe the other's window, and what escapes is a
// real task doing real work (see issue #195, where a real DNS lookup escaped a
// test whose http task was supposedly stubbed).
//
// A context registry is consulted only by [LookupTaskIn] — the execution path.
// Every other reader, including the compiler and the validator, keeps asking
// what the build provides through [LookupTask]: they ask about *shapes*, which
// are a property of the build, and a per-run override of a shape would let a
// file compile against one definition and run against another.
//
// Production never sets one. With no registry on the context, [LookupTaskIn] is
// [LookupTask], which is what keeps the durable driver and every ordinary local
// run reading exactly the registry they read before this existed.
func NewContextWithRegistry(ctx context.Context, registry *Registry) context.Context {
	return context.WithValue(ctx, registryContextKey{}, registry)
}

// RegistryFromContext returns the registry a context carries, and whether one
// was set.
func RegistryFromContext(ctx context.Context) (*Registry, bool) {
	registry, ok := ctx.Value(registryContextKey{}).(*Registry)
	return registry, ok && registry != nil
}

// LookupTaskIn returns the task definition a *run* should execute under name:
// the one its context's registry provides, or the build's own when the context
// carries none.
//
// The execution path uses this and nothing else, so that what a run executes is
// decided by what that run was given rather than by what the process-wide
// registry happens to hold at the instant the step is reached.
func LookupTaskIn(ctx context.Context, name string) (TaskDef, bool) {
	if registry, ok := RegistryFromContext(ctx); ok {
		return registry.Lookup(name)
	}
	return LookupTask(name)
}

// TaskNamesIn returns the task names available to a run, sorted — its context's
// registry, or the build's own. Used by the unknown-task diagnostic, so that a
// run told to use a particular registry is told what *that* registry offers
// rather than a list it cannot reach.
func TaskNamesIn(ctx context.Context) []string {
	if registry, ok := RegistryFromContext(ctx); ok {
		return registry.Names()
	}
	return TaskNames()
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
