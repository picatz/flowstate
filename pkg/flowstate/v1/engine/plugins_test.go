package engine_test

import (
	"testing"

	"github.com/stretchr/testify/require"
	"go.temporal.io/sdk/testsuite"

	v1 "github.com/picatz/flowstate/pkg/flowstate/v1"
	"github.com/picatz/flowstate/pkg/flowstate/v1/engine"
)

// The worker's admission check, from the interpreter's side.
//
// What these assert is the thing the branch shipped without: a run pinned to a
// plugin this worker does not have must not execute a single step here. The
// refusal is per run and per segment rather than per task, so the assertion is
// that the workload never starts, not that a step failed.

func pluginCatalog(digest string) *v1.PluginCatalog {
	return &v1.PluginCatalog{Plugins: []*v1.PluginDescription{{
		Name:               "slack",
		Version:            "v2.1.0",
		ProtocolVersion:    2,
		TaskSchemaDigest:   digest,
		DistributionDigest: "sha256:binary",
		ClaimsDigest:       "sha256:claims",
	}}}
}

// pinnedWorkflow is a one-step workload pinned against the catalog it is given.
func pinnedWorkflow(t *testing.T, catalog *v1.PluginCatalog) *v1.Workflow {
	t.Helper()

	wf := &v1.Workflow{
		Name:               "needs-slack",
		PluginRequirements: []*v1.PluginRequirement{{Name: "slack", MinimumVersion: "v2.0.0"}},
		Steps: []*v1.Node{{
			Id: "say",
			Kind: &v1.Node_Task{Task: &v1.Task{
				Name:   "log",
				Inputs: map[string]*v1.Value{"message": v1.NewLiteral("hello")},
			}},
		}},
	}
	require.NoError(t, v1.ResolvePlugins(wf, catalog))

	return wf
}

// runOnWorkerWith executes a run on a worker holding the given catalog.
func runOnWorkerWith(t *testing.T, catalog *v1.PluginCatalog, wf *v1.Workflow) error {
	t.Helper()

	// The process value a real worker installs from its plugin host, restored
	// afterwards so one test's fleet is not another's.
	engine.UsePluginCatalog(catalog)
	t.Cleanup(func() { engine.UsePluginCatalog(nil) })

	suite := &testsuite.WorkflowTestSuite{}
	env := suite.NewTestWorkflowEnvironment()
	env.RegisterWorkflow(engine.Run)
	env.RegisterActivity(engine.Task)
	env.RegisterActivity(engine.TaskInScope)
	env.RegisterActivity(engine.WorkflowVars)
	env.RegisterActivity(engine.CheckPlugins)

	env.ExecuteWorkflow(engine.Run, &v1.RunState{Workflow: wf})
	require.True(t, env.IsWorkflowCompleted())

	return env.GetWorkflowError()
}

// TestAWorkerWithTheSamePluginRunsThePinnedWorkload is the positive direction,
// and it is here so the negatives below cannot pass for the wrong reason.
func TestAWorkerWithTheSamePluginRunsThePinnedWorkload(t *testing.T) {
	catalog := pluginCatalog("sha256:schema")

	require.NoError(t, runOnWorkerWith(t, catalog, pinnedWorkflow(t, catalog)))
}

// TestAWorkerWithADifferentPluginRefusesTheRun is the rollout case: the same
// plugin at the same version, built from a different task schema, on a worker
// polling the same task queue.
func TestAWorkerWithADifferentPluginRefusesTheRun(t *testing.T) {
	wf := pinnedWorkflow(t, pluginCatalog("sha256:submitted"))

	err := runOnWorkerWith(t, pluginCatalog("sha256:rolled-out"), wf)
	require.Error(t, err, "a worker that cannot reproduce the run's contract executed it anyway")
	require.ErrorContains(t, err, "replay contract")
	require.ErrorContains(t, err, "task schema digest")
}

// TestAWorkerWithoutThePluginRefusesTheRun is the stock worker: registration
// never happened, so nothing here can serve the run. Fail closed.
func TestAWorkerWithoutThePluginRefusesTheRun(t *testing.T) {
	wf := pinnedWorkflow(t, pluginCatalog("sha256:submitted"))

	err := runOnWorkerWith(t, nil, wf)
	require.Error(t, err, "a worker with no plugins at all executed a run pinned to one")
	require.ErrorContains(t, err, "no such plugin installed")
}

// TestAnUnpinnedSpecificationIsRefused covers the specification that reached a
// worker without ever passing through a control plane that resolves plugins.
// There is no contract to check, so there is nothing that says this worker may
// run it.
func TestAnUnpinnedSpecificationIsRefused(t *testing.T) {
	wf := pinnedWorkflow(t, pluginCatalog("sha256:schema"))
	wf.ResolvedPlugins = nil

	err := runOnWorkerWith(t, pluginCatalog("sha256:schema"), wf)
	require.Error(t, err)
	require.ErrorContains(t, err, "was never resolved against a deployment")
}

// TestAWorkloadWithNoPluginsSchedulesNoCheck is the determinism argument, made
// as a test: a run pinned to nothing must not schedule the admission activity at
// all, because every history written before this activity existed is such a run
// and replaying one against an interpreter that schedules something new does not
// replay.
func TestAWorkloadWithNoPluginsSchedulesNoCheck(t *testing.T) {
	engine.UsePluginCatalog(nil)
	t.Cleanup(func() { engine.UsePluginCatalog(nil) })

	suite := &testsuite.WorkflowTestSuite{}
	env := suite.NewTestWorkflowEnvironment()
	env.RegisterWorkflow(engine.Run)
	env.RegisterActivity(engine.Task)
	env.RegisterActivity(engine.TaskInScope)
	env.RegisterActivity(engine.WorkflowVars)

	// Deliberately not registered: if the interpreter schedules it for a workload
	// that requires no plugins, the run fails with an unknown activity type and
	// this test says so.
	//
	// env.RegisterActivity(engine.CheckPlugins)

	env.ExecuteWorkflow(engine.Run, &v1.RunState{Workflow: &v1.Workflow{
		Name: "no-plugins",
		Steps: []*v1.Node{{
			Id: "say",
			Kind: &v1.Node_Task{Task: &v1.Task{
				Name:   "log",
				Inputs: map[string]*v1.Value{"message": v1.NewLiteral("hello")},
			}},
		}},
	}})

	require.True(t, env.IsWorkflowCompleted())
	require.NoError(t, env.GetWorkflowError())
}
