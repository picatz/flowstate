package engine

import (
	"context"
	"fmt"

	v1 "github.com/picatz/flowstate/pkg/flowstate/v1"
	"github.com/picatz/flowstate/pkg/flowstate/v1/auth"
	"github.com/picatz/flowstate/pkg/flowstate/v1/plugin"
	"github.com/picatz/flowstate/pkg/flowstate/v1/secrets"
)

// TaskRuntimeConfig is the immutable configuration owned by one worker: the
// sensitive capabilities its tasks run with, and the plugin inventory its runs
// are admitted against. It is passed to activity registration rather than stored
// globally, so two workers embedded in one process cannot overwrite each other's
// tenant or federation configuration — or each other's answer to "which plugins
// is this worker holding".
//
// The inventory is not a capability, and naming it here widens what this type is
// about. That is the cost being paid, deliberately: the alternative is a second
// per-worker vehicle beside this one, and the whole defect [WithPluginCatalog]
// closes (#777) is that the catalog had a second vehicle — a process global —
// which the last worker to be constructed won. One thing carrying everything one
// worker owns cannot disagree with itself about which worker it belongs to.
type TaskRuntimeConfig struct {
	store   *secrets.Store
	policy  *auth.SecretPolicy
	broker  *auth.Broker
	catalog *v1.PluginCatalog
}

// NewTaskRuntimeConfig validates and assembles worker task capabilities.
func NewTaskRuntimeConfig(store *secrets.Store, policy *auth.SecretPolicy, broker *auth.Broker) (TaskRuntimeConfig, error) {
	if (store == nil) != (policy == nil) {
		return TaskRuntimeConfig{}, fmt.Errorf("secret store and access policy must be configured together")
	}
	return TaskRuntimeConfig{store: store, policy: policy, broker: broker}, nil
}

// WithPluginCatalog returns a copy carrying the plugins this worker actually has.
//
// Called with what the worker's plugin host launched — see cmd/flow's
// startPlugins — and the result passed to [Register], before the worker polls.
// A worker registered without one has no plugins, which is the truthful answer
// for a stock worker and the fail-closed one for a worker whose operator forgot:
// every run pinned to a plugin is refused by the admission check in plugins.go
// rather than executed by a worker that has none of it.
//
// A copy rather than a mutation because the zero value has to keep meaning "no
// plugins" for every worker that never says otherwise, and a builder that
// mutated a shared value would be the process global again wearing a method's
// clothes.
//
// It is separate from [NewTaskRuntimeConfig] rather than a fourth parameter to
// it because the two answer to different owners: the store, policy and broker
// are a deployment's grant of authority to this worker's tasks and are checked
// against each other, while the catalog is an observation of what this process
// launched and can only be wrong by being somebody else's.
func (c TaskRuntimeConfig) WithPluginCatalog(catalog *v1.PluginCatalog) TaskRuntimeConfig {
	c.catalog = catalog

	return c
}

type taskActivities struct{ configured TaskRuntimeConfig }

func (a taskActivities) context(ctx context.Context, identity *v1.WorkloadIdentity, workflowName, runID, stepID string) context.Context {
	ctx = v1.ContextWithTaskRuntime(ctx, v1.TaskRuntime{
		Store: a.configured.store, Policy: a.configured.policy, Broker: a.configured.broker,
		Identity: auth.IdentityFrom(identity),
		Step:     auth.StepRef{Workflow: workflowName, Run: runID, Step: stepID},
	})

	// The same identity, carried a second way, for a task that reaches across
	// the plugin process boundary rather than staying inside this one. A
	// plugin task's Fn cannot read [TaskRuntime] — it lives in a package this
	// one cannot import without a cycle back to itself, per
	// [plugin.NewContextWithIdentity]'s doc — so it reads this context key
	// instead. Installed here, at the one place this driver already builds a
	// per-task context from the run's authenticated identity, so a plugin
	// task on this activity sees exactly the identity a built-in task's secret
	// resolution does: the same RunState.Identity, the same context, one call
	// site rather than a second place that could drift from it.
	return plugin.NewContextWithIdentity(ctx, orEmptyIdentity(identity))
}

// orEmptyIdentity substitutes an explicit, present, all-empty identity for a
// nil one.
//
// [v1.RunState.Identity] is unset for a run nobody authenticated, and that is
// a real, common case rather than an error — but a nil *v1.WorkloadIdentity
// stored under [plugin.NewContextWithIdentity]'s context key is
// indistinguishable, to [plugin.IdentityFromContext], from a context nothing
// ever called it on at all, which is also a real case (a driver that predates
// this fix, or a caller outside either driver). Collapsing that distinction
// here — the one place both call sites in this package reach for an
// identity — is what lets #235's negative-shape test assert "no identity"
// looks the same on both drivers: present, and empty, never absent and never
// invented. [v1.ProtoWorkloadIdentity] makes the identical promise for the
// local driver, which never has a nil auth.WorkloadIdentity to begin with.
func orEmptyIdentity(identity *v1.WorkloadIdentity) *v1.WorkloadIdentity {
	if identity == nil {
		return &v1.WorkloadIdentity{}
	}
	return identity
}

// Both heartbeat, exactly as the three in activities.go do, and that is not
// symmetry for its own sake: `HeartbeatTimeout` is set on *every* activity's
// options, so an entry point that does not heartbeat is one whose healthy
// long-running requests are failed at thirty seconds and retried. These two are
// precisely the paths a slow request is most likely to take — a task needing
// authority is a task talking to something that authenticates it — so leaving them
// out would have broken the case the timeout exists to serve.
//
// The two authorized entry points are also the two that know where they are.
//
// They are handed the step's id, so their spans carry it — which is the
// attribute that turns "some task failed" into "this step failed", and the one
// the pre-scope activities in activities.go cannot supply. Nothing about the
// identity is written to the span: a subject and an issuer identify a person or
// a workload to anyone reading the collector, and a trace does not need them to
// say which step ran. See the span rules in activities.go.

func (a taskActivities) TaskAuthorized(ctx context.Context, task *v1.Task, identity *v1.WorkloadIdentity, workflowName, runID, stepID string, continueOnError bool) (*v1.Node_Outputs, error) {
	ctx, span := startTaskSpan(ctx, task, stepID)
	defer span.End()

	// The deployment's task-shape policy (#187), checked here against the
	// same identity parameter [executor.dispatch] already threads through
	// for authorization — the fix for the gap found in review: this is the
	// arm [v1.TaskNeedsAuthority] selects, so it is exactly the tasks that
	// resolve secrets and act under the run's own identity that a
	// deployment's policy most needs to be able to gate, and the first cut
	// of #187 slice 1 checked [Task]/[TaskInScope] but not this one or
	// [TaskInScopeAuthorized] — see [checkTaskDispatchPolicy]'s own doc.
	// Checked before [a.context] installs the runtime a resolved secret
	// reference would use, so a denied dispatch still resolves no
	// credential (invariant 7's echo, restated for this arm).
	if err := checkTaskDispatchPolicy(ctx, span, task, identity); err != nil {
		return nil, err
	}

	ctx, stop := withHeartbeat(ctx)
	defer stop()

	ctx = a.context(withActivityLogger(ctx), identity, workflowName, runID, stepID)
	out, err := task.Eval(ctx, nil)
	recordTaskOutcome(span, err)

	return out, activityError(task.GetName(), err, continueOnError)
}

func (a taskActivities) TaskInScopeAuthorized(ctx context.Context, task *v1.Task, scope *v1.Scope, identity *v1.WorkloadIdentity, workflowName, runID, stepID string, continueOnError bool) (*v1.Node_Outputs, error) {
	ctx, span := startTaskSpan(ctx, task, stepID)
	defer span.End()

	// See [TaskAuthorized]'s identical check, this arm's sibling on the
	// other axis (scope-carrying rather than not) of [executor.dispatch]'s
	// four-way split.
	if err := checkTaskDispatchPolicy(ctx, span, task, identity); err != nil {
		return nil, err
	}

	ctx, stop := withHeartbeat(ctx)
	defer stop()

	ctx = a.context(withActivityLogger(ctx), identity, workflowName, runID, stepID)
	out, err := task.EvalInScope(ctx, scope)
	recordTaskOutcome(span, err)

	return out, activityError(task.GetName(), err, continueOnError)
}
