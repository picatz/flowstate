package tests

import (
	"context"

	v1 "github.com/picatz/flowstate/pkg/flowstate/v1"
	"github.com/picatz/flowstate/pkg/flowstate/v1/plugin"
)

// PluginIdentityTaskName is the name [PluginIdentityTaskDef] registers under.
const PluginIdentityTaskName = "test.plugin_identity"

// PluginIdentityTaskDef is a [v1.TaskDef] whose Fn reads the caller exactly the
// way a real plugin task's does — through [plugin.IdentityFromContext] — at
// exactly the position `plugin/task.go`'s taskFunc reads it, and reports what it
// found as outputs.
//
// It stands in for a real plugin process rather than spawning one: what #235 is
// about is whether the context a task's Fn executes in carries an identity by
// the time execution reaches it, and that is a property of the driver's
// dispatch, not of the plugin protocol's RPC framing — a real subprocess would
// answer the identical question one process boundary further out, and
// `pkg/flowstate/v1/plugin`'s own TestSecretIdentityCrossesBoundary and
// TestTaskDefExecutes already cover that the wire carries whatever
// [plugin.NewContextWithIdentity] put in the context across to a real plugin.
// This fixture is what proves each driver's production seam is the thing that
// puts it there in the first place, registered as an ordinary task so both
// drivers dispatch to it exactly as they would a plugin's.
//
// needsScope selects which of the durable driver's two unauthorized entry
// points a plugin task with this property reaches — NeedsPrevOutputs true
// schedules TaskInScope, false schedules Task — which is worth exercising as
// two separate cases even though #187's task-shape policy work means both
// carry an identity today: TaskInScope's arrives by way of Scope.Identity,
// Task's as a parameter threaded from executor.dispatch's own e.identity, and
// a regression that broke only one of the two would not show through the
// other.
func PluginIdentityTaskDef(needsScope bool) v1.TaskDef {
	return v1.TaskDef{
		Name:             PluginIdentityTaskName,
		NeedsPrevOutputs: needsScope,
		Fn: func(ctx context.Context, _ map[string]*v1.Value, _ *v1.Scope) (*v1.Node_Outputs, error) {
			identity, present := plugin.IdentityFromContext(ctx)
			return &v1.Node_Outputs{NamedValues: map[string]*v1.Value{
				"present":   v1.NewLiteral(present),
				"subject":   v1.NewLiteral(identity.GetSubject()),
				"namespace": v1.NewLiteral(identity.GetNamespace()),
			}}, nil
		},
	}
}

// PluginIdentityStep builds a one-step workflow naming
// [PluginIdentityTaskName], the way a Flowfile compiles a plugin task step.
func PluginIdentityStep(workflowName, stepID string) *v1.Workflow {
	return &v1.Workflow{
		Name: workflowName,
		Steps: []*v1.Node{{
			Id:   stepID,
			Kind: &v1.Node_Task{Task: &v1.Task{Name: PluginIdentityTaskName}},
		}},
	}
}
