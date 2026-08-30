package embed

import (
	"fmt"

	"go.temporal.io/sdk/worker"

	"github.com/picatz/flowstate/pkg/flowstate/v1/engine"
)

// RunDurable registers the Flowstate interpreter on w, so a worker owned by
// the embedding program executes workflows durably through its own Temporal
// client and connection — this package does not open one on an embedder's
// behalf, the same way [engine.Register] does not.
//
// It is [engine.Register] under this package's name, with one precondition
// this package checks rather than silently satisfies on an embedder's
// behalf: every task in tasks must already be [Tasks.Install]ed — as
// *this exact Tasks set*, not merely as a task of the same name existing in
// [v1.DefaultRegistry] under someone else's ownership. Checking a bare name
// would pass for a program that overrides a built-in task (its own "log",
// say) without ever calling Install: the name "log" already exists, from
// the built-in, so a name-only check sees nothing wrong, and the worker
// then executes the *built-in* log for every run while [RunLocal] — which
// always reads a Tasks set directly, install or not — executes the
// program's own. Two drivers silently disagreeing about what one step does
// is exactly the thing CLAUDE.md's driver-parity rule exists to catch, so
// this checks ownership ([Tasks.installedExactly]) rather than existence.
//
// Unlike [RunLocal], which reads a Tasks set fresh on every call, a durable
// run's activities execute in a Temporal activity context this package never
// sees — there is no per-run registry to hand them, only
// [v1.DefaultRegistry], the same one [v1.LookupTaskIn] falls back to
// whenever a context carries no override (see its doc). So a custom task
// this worker is meant to run has exactly one path in: registered globally,
// before this worker starts polling, and left registered for as long as it
// does — calling Install's uninstall while the worker is still running makes
// every in-flight activity for that task fail as unknown, mid-run.
//
// RunDurable does not call Install itself, so that installing stays the
// single, explicit act [Tasks.Install]'s own doc describes, done once by the
// embedder at a moment of its own choosing rather than as a side effect
// buried in a call that also touches the worker. What it does instead is
// refuse to register the worker at all when a task in tasks was not
// installed — a worker that started anyway would poll for activities it can
// never actually execute, which is a `flow worker` misconfiguration this
// package can catch before the worker takes its first task.
// The installed set must also be complete before RunDurable: worker registration
// freezes its task-capability names for admission, just as it freezes the task
// functions dispatch will use for the worker's lifetime.
//
// runtime configures the worker-side authority available to every task this
// worker runs — the durable counterpart to [RunOptions.Secrets] — built with
// [engine.NewTaskRuntimeConfig]. Passing none leaves secret resolution and
// credential federation refused for every workflow this worker runs, the
// same fail-closed default [RunOptions.Secrets] has locally.
//
// It also carries the plugin inventory this worker's runs are admitted
// against, added with [engine.TaskRuntimeConfig.WithPluginCatalog]. Passing
// none says this worker has no plugins, so a run pinned to one is refused
// here — rather than admitted against a catalog some *other* worker in this
// process launched, which is what a process-wide catalog did before #777.
// An embedder running two worker fleets with different plugin sets gets one
// answer per worker, because the answer travels with the registration.
func RunDurable(w worker.Registry, tasks *Tasks, runtime ...engine.TaskRuntimeConfig) error {
	if tasks != nil {
		if missing, ok := tasks.installedExactly(); !ok {
			return fmt.Errorf(
				"flowstate/embed: RunDurable: task %q is not installed by this Tasks set — "+
					"a definition of that name may be registered from elsewhere (a built-in task, "+
					"or a Tasks set that has since been uninstalled), but this worker would then "+
					"run that definition, not this set's; call Tasks.Install on this exact set "+
					"and keep it installed for the life of this worker", missing)
		}
	}

	engine.Register(w, runtime...)
	return nil
}
