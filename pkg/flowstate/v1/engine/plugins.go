package engine

import (
	"context"
	"sync/atomic"

	"go.temporal.io/sdk/temporal"
	"go.temporal.io/sdk/workflow"

	v1 "github.com/picatz/flowstate/pkg/flowstate/v1"
)

// The worker's half of the plugin contract.
//
// A submission is pinned by the control plane to the exact plugins it will run
// against (name, version, protocol version, task schema digest, distribution
// digest), and that pin is only a contract if something refuses to execute the
// run where it does not hold. This file is that something.
//
// # Where the check can honestly live
//
// It cannot live in workflow code, because the answer is a property of the
// process rather than of history: two workers polling one task queue during a
// rollout hold different catalogs, so a branch on "what is installed here" is a
// branch on a value replay does not reproduce, which is exactly what invariant 4
// forbids. It cannot live in the task activities either, because those are handed
// a [v1.Task] and never see the specification, so they have nothing to compare.
// A check per task would also be per *dispatch* rather than per run, which is the
// wrong unit for a decision about whether this worker may run this thing at all.
//
// So it is split at the seam the interpreter already uses for exactly this shape
// of problem, the one [WorkflowVars] uses: the deterministic half runs in
// workflow code ([v1.PinnedPlugins], a pure function of the specification), and
// the half that reads the process runs in an activity ([CheckPlugins]). What
// crosses is the contract itself rather than the specification, so the check
// costs a handful of tuples in history rather than a second copy of the workload.
//
// It runs before any step of a segment executes, and in every segment, because
// Continue-As-New is where a run can move to a worker that was not there when it
// started.

// configuredPluginCatalog is what the worker's plugin host reported at startup.
//
// A process value for the same reason [configuredConverter] is one: the thing
// that needs it is reached by Temporal rather than constructed by the caller, and
// there is nowhere on the call to put it. It is written once, before the worker
// polls, and only read afterwards.
var configuredPluginCatalog atomic.Pointer[v1.PluginCatalog]

// UsePluginCatalog tells the interpreter which plugins this worker actually has.
//
// Called once, at worker construction, before [Register], with what the plugin
// host launched: see cmd/flow's startPlugins. A worker that never calls it has
// no plugins, which is the truthful answer for a stock worker and the fail-closed
// one for a worker whose operator forgot: every run pinned to a plugin is refused
// here rather than executed by a process that has none of it.
func UsePluginCatalog(catalog *v1.PluginCatalog) {
	if catalog == nil {
		configuredPluginCatalog.Store(nil)

		return
	}
	configuredPluginCatalog.Store(catalog)
}

// workerPluginCatalog is what this process can run, for the admission check.
func workerPluginCatalog() *v1.PluginCatalog {
	return configuredPluginCatalog.Load()
}

// CheckPlugins is the activity that admits a run to this worker.
//
// It receives the run's replay contract, derived from the specification by
// workflow code, and compares it against the catalog this process launched. A
// mismatch is not retryable: the tuple will not change on the second attempt,
// because it is a fact about which binary this worker is holding. The run fails
// with a message naming the plugin, the field, what the run expects and what is
// here, so an operator reads a half-finished rollout rather than a mystery.
func CheckPlugins(ctx context.Context, pins []*v1.ResolvedPlugin) error {
	if err := v1.CheckPluginsAvailable(pins, workerPluginCatalog()); err != nil {
		return temporal.NewNonRetryableApplicationError(err.Error(), "PluginContractMismatch", err)
	}

	return nil
}

// admitPlugins runs the plugin admission check for a segment.
//
// Deterministic on every path that decides whether to schedule the activity: the
// pins come from the specification alone, so two workers replaying one history
// schedule the same thing. Nothing is scheduled for a run that is pinned to no
// plugins, which is every run written before this existed. A history that could
// not contain this activity is therefore never replayed against code that expects
// one.
func admitPlugins(ctx workflow.Context, wf *v1.Workflow) error {
	pins, err := v1.PinnedPlugins(wf)
	if err != nil {
		return &ErrRunFailed{Message: err.Error()}
	}
	if len(pins) == 0 {
		return nil
	}

	return workflow.ExecuteActivity(ctx, CheckPlugins, pins).Get(ctx, nil)
}
