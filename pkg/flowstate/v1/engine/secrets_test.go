package engine_test

import (
	"context"
	"testing"

	v1 "github.com/picatz/flowstate/pkg/flowstate/v1"
	"github.com/picatz/flowstate/pkg/flowstate/v1/auth"
	"github.com/picatz/flowstate/pkg/flowstate/v1/engine"
	"github.com/picatz/flowstate/pkg/flowstate/v1/secrets"
	"github.com/stretchr/testify/require"
	"go.temporal.io/sdk/testsuite"
)

type activitySecretProvider struct{ namespace string }

func (p *activitySecretProvider) Scheme() string { return "test-secret" }
func (p *activitySecretProvider) Resolve(_ context.Context, req secrets.Request) (secrets.Secret, error) {
	p.namespace = req.Namespace
	return secrets.NewSecret(req.Ref, "resolved-in-activity"), nil
}

func TestSecretActivityCarriesIdentityAndStepToPolicy(t *testing.T) {
	provider := &activitySecretProvider{}
	store, err := secrets.NewStore(provider)
	require.NoError(t, err)
	policy, err := (auth.SecretAccessPolicy{Allow: []string{
		`workload.namespace == "acme" && workload.workflow == "secret-workflow" && workload.step == "read"`,
	}}).Compile()
	require.NoError(t, err)
	runtime, err := engine.NewTaskRuntimeConfig(store, policy, nil)
	require.NoError(t, err)

	const taskName = "test-secret-activity"
	require.NoError(t, v1.DefaultRegistry().Register(v1.TaskDef{
		Name:            taskName,
		AuthorityInputs: []string{"credential"},
		Fn: func(ctx context.Context, inputs map[string]*v1.Value, _ *v1.Scope) (*v1.Node_Outputs, error) {
			secret, err := v1.ResolveSecret(ctx, inputs["credential"].GetSecretRef())
			if err != nil {
				return nil, err
			}
			return &v1.Node_Outputs{NamedValues: map[string]*v1.Value{"length": v1.NewValue(int64(secret.Len()))}}, nil
		},
	}))

	workflow := &v1.Workflow{Name: "secret-workflow", Steps: []*v1.Node{{
		Id: "read",
		Kind: &v1.Node_Task{Task: &v1.Task{Name: taskName, Inputs: map[string]*v1.Value{
			"credential": {Kind: &v1.Value_SecretRef{SecretRef: &v1.SecretRef{Scheme: "test-secret", Name: "token"}}},
		}}},
	}}}
	state := &v1.RunState{Workflow: workflow, Identity: &v1.WorkloadIdentity{
		Subject: "caller", Issuer: "https://issuer.example", Namespace: "acme",
	}}

	var suite testsuite.WorkflowTestSuite
	env := suite.NewTestWorkflowEnvironment()
	engine.Register(env, runtime)
	env.ExecuteWorkflow(engine.Run, state)
	require.NoError(t, env.GetWorkflowError())
	require.Equal(t, "acme", provider.namespace)
}
