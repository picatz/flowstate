package embed

import (
	"fmt"

	"go.temporal.io/sdk/worker"

	v1 "github.com/picatz/flowstate/pkg/flowstate/v1"
	"github.com/picatz/flowstate/pkg/flowstate/v1/engine"
)

// RunDurable registers the Flowstate interpreter on w, so a worker owned by
// the embedding program executes workflows durably through its own Temporal
// client and connection — this package does not open one on an embedder's
// behalf, the same way [engine.Register] does not.
//
// It is [engine.Register] under this package's name, with one precondition
// this package checks rather than silently satisfies on an embedder's
// behalf: every task in tasks must already be [Tasks.Install]ed.
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
//
// runtime configures the worker-side authority available to every task this
// worker runs — the durable counterpart to [RunOptions.Secrets] — built with
// [engine.NewTaskRuntimeConfig]. Passing none leaves secret resolution and
// credential federation refused for every workflow this worker runs, the
// same fail-closed default [RunOptions.Secrets] has locally.
func RunDurable(w worker.Registry, tasks *Tasks, runtime ...engine.TaskRuntimeConfig) error {
	if tasks != nil {
		for _, def := range tasks.defs() {
			if _, ok := v1.LookupTask(def.Name); !ok {
				return fmt.Errorf(
					"flowstate/embed: RunDurable: task %q is not installed; call Tasks.Install "+
						"before RunDurable so this worker's activities can find it", def.Name)
			}
		}
	}

	engine.Register(w, runtime...)
	return nil
}
