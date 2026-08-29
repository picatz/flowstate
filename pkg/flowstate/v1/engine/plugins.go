package engine

import (
	"context"

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
// worker rather than of history: two workers polling one task queue during a
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
// the half that reads the worker runs in an activity
// ([taskActivities.CheckPlugins]). What crosses is the contract itself rather
// than the specification, so the check costs a handful of tuples in history
// rather than a second copy of the workload.
//
// It runs before any step of a segment executes, and in every segment, because
// Continue-As-New is where a run can move to a worker that was not there when it
// started.
//
// # Whose catalog, and why that is not a process value
//
// The catalog is worker-scoped, carried on [TaskRuntimeConfig] to the activity
// receiver [Register] builds, and this used to be a process global instead —
// which was wrong in a way the check's own sentence gives away (#777). "A fact
// about which binary this worker is holding" is only true if the answer belongs
// to the worker that asked. Two workers registered in one process — the
// embedding surface [Register] exists to serve, per its doc, and what
// pkg/flowstate/embed's RunDurable hands to an embedder — share one process, so
// a process global gives them one answer, and the last one constructed wins.
// Both failure directions are live from there: a run pinned to a plugin the
// polling worker *has* is refused non-retryably against the other worker's
// inventory, and a run pinned to one it *lacks* is admitted and then fails step
// by step with `unknown task`, which is the exact mystery this check exists to
// prevent.
//
// [configuredConverter] stays a process value and is not the same case: its
// reader runs in workflow code, where nothing per-worker can be threaded without
// an interceptor. This one is an activity, and an activity is precisely the
// thing this package already hands per-worker state to.

// checkPluginsActivity is the name the admission activity is registered under,
// and the name workflow code schedules by.
//
// A string rather than a function reference because the activity is a method
// now: workflow code has no receiver to name one with, and there is exactly one
// name here rather than two spellings of it that could drift. History names an
// activity by string in any case, which is what makes the move receiver-ward
// replay-safe — see [Register].
const checkPluginsActivity = "CheckPlugins"

// CheckPlugins is the activity that admits a run to this worker.
//
// It receives the run's replay contract, derived from the specification by
// workflow code, and compares it against the catalog this worker launched. A
// mismatch is not retryable: the tuple will not change on the second attempt,
// because it is a fact about which binary this worker is holding. The run fails
// with a message naming the plugin, the field, what the run expects and what is
// here, so an operator reads a half-finished rollout rather than a mystery.
//
// A nil catalog — a worker registered without [TaskRuntimeConfig.WithPluginCatalog]
// — is a worker with no plugins, and refuses every pinned run. That is the
// fail-closed direction: an unconfigured worker admits nothing rather than
// borrowing an answer from whoever configured last.
func (a taskActivities) CheckPlugins(ctx context.Context, pins []*v1.ResolvedPlugin) error {
	if err := v1.CheckPluginsAvailable(pins, a.configured.catalog); err != nil {
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

	return workflow.ExecuteActivity(withSummary(ctx, pluginAdmissionSummary),
		checkPluginsActivity, pins).Get(ctx, nil)
}
