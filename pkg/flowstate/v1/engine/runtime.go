package engine

import (
	"context"
	"fmt"

	v1 "github.com/picatz/flowstate/pkg/flowstate/v1"
	"github.com/picatz/flowstate/pkg/flowstate/v1/auth"
	"github.com/picatz/flowstate/pkg/flowstate/v1/secrets"
)

// TaskRuntimeConfig is the immutable set of sensitive capabilities owned by one
// worker. It is passed to activity registration rather than stored globally, so
// two workers embedded in one process cannot overwrite each other's tenant or
// federation configuration.
type TaskRuntimeConfig struct {
	store  *secrets.Store
	policy *auth.SecretPolicy
	broker *auth.Broker
}

// NewTaskRuntimeConfig validates and assembles worker task capabilities.
func NewTaskRuntimeConfig(store *secrets.Store, policy *auth.SecretPolicy, broker *auth.Broker) (TaskRuntimeConfig, error) {
	if (store == nil) != (policy == nil) {
		return TaskRuntimeConfig{}, fmt.Errorf("secret store and access policy must be configured together")
	}
	return TaskRuntimeConfig{store: store, policy: policy, broker: broker}, nil
}

type taskActivities struct{ configured TaskRuntimeConfig }

func (a taskActivities) context(ctx context.Context, identity *v1.WorkloadIdentity, workflowName, runID, stepID string) context.Context {
	return v1.ContextWithTaskRuntime(ctx, v1.TaskRuntime{
		Store: a.configured.store, Policy: a.configured.policy, Broker: a.configured.broker,
		Identity: auth.IdentityFrom(identity),
		Step:     auth.StepRef{Workflow: workflowName, Run: runID, Step: stepID},
	})
}

// The two authorized entry points are also the two that know where they are.
//
// They are handed the step's id, so their spans carry it — which is the
// attribute that turns "some task failed" into "this step failed", and the one
// the pre-scope activities in activities.go cannot supply. Nothing about the
// identity is written to the span: a subject and an issuer identify a person or
// a workload to anyone reading the collector, and a trace does not need them to
// say which step ran. See the span rules in activities.go.

func (a taskActivities) TaskAuthorized(ctx context.Context, task *v1.Task, identity *v1.WorkloadIdentity, workflowName, runID, stepID string) (*v1.Node_Outputs, error) {
	ctx, span := startTaskSpan(ctx, task, stepID)
	defer span.End()

	ctx = a.context(withActivityLogger(ctx), identity, workflowName, runID, stepID)
	out, err := task.Eval(ctx, nil)
	recordTaskOutcome(span, err)

	return out, activityError(task.GetName(), err)
}

func (a taskActivities) TaskInScopeAuthorized(ctx context.Context, task *v1.Task, scope *v1.Scope, identity *v1.WorkloadIdentity, workflowName, runID, stepID string) (*v1.Node_Outputs, error) {
	ctx, span := startTaskSpan(ctx, task, stepID)
	defer span.End()

	ctx = a.context(withActivityLogger(ctx), identity, workflowName, runID, stepID)
	out, err := task.EvalInScope(ctx, scope)
	recordTaskOutcome(span, err)

	return out, activityError(task.GetName(), err)
}
