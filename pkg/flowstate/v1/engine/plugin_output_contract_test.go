package engine_test

import (
	"context"
	"os"
	"os/exec"
	"path/filepath"
	"testing"
	"time"

	"github.com/google/cel-go/cel"
	"github.com/stretchr/testify/require"
	"go.temporal.io/sdk/activity"
	"go.temporal.io/sdk/converter"
	"go.temporal.io/sdk/testsuite"

	v1 "github.com/picatz/flowstate/pkg/flowstate/v1"
	"github.com/picatz/flowstate/pkg/flowstate/v1/engine"
	"github.com/picatz/flowstate/pkg/flowstate/v1/plugin"
)

// TestPluginOutputContractsOnBothDrivers crosses a real SDK plugin subprocess
// on both execution drivers. The valid task's declared value reaches a later
// expression; the adversarial task substitutes an undeclared field carrying a
// marker path, and is refused before that value can reach the following task or
// the durable workflow result.
func TestPluginOutputContractsOnBothDrivers(t *testing.T) {
	dir := t.TempDir()
	binary := filepath.Join(dir, "flowstate-plugin-output-contract")
	build := exec.CommandContext(t.Context(), "go", "build", "-o", binary,
		"github.com/picatz/flowstate/pkg/flowstate/v1/plugin/testdata/outputcontract")
	build.Dir = "../../../.."
	output, err := build.CombinedOutput()
	require.NoError(t, err, "building adversarial plugin: %s", output)

	host, err := plugin.NewHost(plugin.Config{
		SearchPath:          []string{dir},
		DisableHealthChecks: true,
	})
	require.NoError(t, err)
	require.NoError(t, host.Open(t.Context()))
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		require.NoError(t, host.Close(ctx))
	})
	require.NoError(t, host.Register(v1.DefaultRegistry(), nil))

	for _, driver := range []struct {
		name string
		run  func(*testing.T, *v1.Workflow) (*v1.Workflow_StepOutputs, error)
	}{
		{
			name: "local",
			run: func(_ *testing.T, workflow *v1.Workflow) (*v1.Workflow_StepOutputs, error) {
				return v1.Run(t.Context(), workflow)
			},
		},
		{
			name: "durable",
			run: func(t *testing.T, workflow *v1.Workflow) (*v1.Workflow_StepOutputs, error) {
				testSuite := &testsuite.WorkflowTestSuite{}
				env := testSuite.NewTestWorkflowEnvironment()
				failedActivities := 0
				env.SetOnActivityCompletedListener(func(_ *activity.Info, result converter.EncodedValue, err error) {
					if err != nil {
						failedActivities++
						require.Nil(t, result,
							"a rejected plugin response completed its activity with a result payload")
					}
				})
				engine.Register(env)
				env.ExecuteWorkflow(engine.Run, &v1.RunState{Workflow: workflow})
				if err := env.GetWorkflowError(); err != nil {
					require.Positive(t, failedActivities,
						"the workflow failed without recording the plugin call as a failed activity")
					return nil, err
				}
				var outputs v1.Workflow_StepOutputs
				if err := env.GetWorkflowResult(&outputs); err != nil {
					return nil, err
				}
				return &outputs, nil
			},
		},
	} {
		t.Run(driver.name, func(t *testing.T) {
			validMarker := filepath.Join(dir, driver.name+"-valid")
			outputs, err := driver.run(t, outputContractWorkflow("valid", validMarker))
			require.NoError(t, err)
			require.Equal(t, "observed", readMarker(t, validMarker))
			require.Equal(t, validMarker,
				outputs.GetStepValues()["probe"].GetNamedValues()["message"].GetLiteral().GetStringValue())

			for _, adversarial := range []struct {
				task, diagnostic string
			}{
				{"malformed", `an undeclared output`},
				{"no_contract", `an undeclared output`},
				{"wrong_type", `output "message": expected a string, got an integer`},
			} {
				t.Run(adversarial.task, func(t *testing.T) {
					malformedMarker := filepath.Join(dir, driver.name+"-"+adversarial.task)
					outputs, err = driver.run(t, outputContractWorkflow(adversarial.task, malformedMarker))
					require.Error(t, err)
					require.ErrorContains(t, err, `task "output-contract.`+adversarial.task+`"`)
					require.ErrorContains(t, err, adversarial.diagnostic)
					require.NotContains(t, err.Error(), malformedMarker,
						"the rejected output value leaked through the diagnostic")
					require.NotContains(t, outputs.String(), malformedMarker,
						"the rejected output value entered the driver's returned transcript")
					_, statErr := os.Stat(malformedMarker)
					require.ErrorIs(t, statErr, os.ErrNotExist,
						"a downstream task observed the malformed plugin output")
				})
			}
		})
	}
}

func outputContractWorkflow(task, marker string) *v1.Workflow {
	return &v1.Workflow{
		Name: "plugin-output-contract-" + task,
		Steps: []*v1.Node{
			{
				Id: "probe",
				Kind: &v1.Node_Task{Task: &v1.Task{
					Name:   "output-contract." + task,
					Inputs: map[string]*v1.Value{"message": v1.NewLiteral(marker)},
				}},
			},
			{
				Id: "observe",
				Kind: &v1.Node_Task{Task: &v1.Task{
					Name: "output-contract.observe",
					Inputs: map[string]*v1.Value{
						"message": outputContractExpression("steps.probe." + map[bool]string{true: "undeclared", false: "message"}[task == "malformed"]),
					},
				}},
			},
		},
	}
}

func outputContractExpression(source string) *v1.Value {
	env, err := v1.DefaultEvaluator().ProfileEnv(v1.CurrentProfile)
	if err != nil {
		panic(err)
	}
	parsed, issues := env.Parse(source)
	if err := issues.Err(); err != nil {
		panic(err)
	}
	stored, err := cel.AstToParsedExpr(parsed)
	if err != nil {
		panic(err)
	}
	return &v1.Value{Kind: &v1.Value_Expr{Expr: stored}}
}

func readMarker(t *testing.T, path string) string {
	t.Helper()
	value, err := os.ReadFile(path)
	require.NoError(t, err)
	return string(value)
}
