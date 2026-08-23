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
//
// Every test here is parallel, and that is a claim rather than a decoration.
// The catalog used to be a process value that each of these wrote and reset
// around itself, which is what kept them serialized against each other: two of
// them running at once would have been two fleets sharing one answer. They can
// run together now because a catalog belongs to the worker it was registered
// with (#777), and a test that could not be parallel is the shape of the defect.

func pluginCatalog(digest string) *v1.PluginCatalog {
	return &v1.PluginCatalog{ClaimsSchemaVersion: v1.CurrentClaimsSchemaVersion, Plugins: []*v1.PluginDescription{{
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

// workerHolding is a worker registered with the plugins a real one's host
// launched, through the same [engine.Register] a `flow worker` process calls.
//
// Registered rather than installed: the catalog rides the configuration this
// worker was registered with, so the environment returned is the whole of what
// it knows about plugins and nothing outside it can change that answer.
func workerHolding(t *testing.T, catalog *v1.PluginCatalog) *testsuite.TestWorkflowEnvironment {
	t.Helper()

	suite := &testsuite.WorkflowTestSuite{}
	env := suite.NewTestWorkflowEnvironment()
	engine.Register(env, engine.TaskRuntimeConfig{}.WithPluginCatalog(catalog))

	return env
}

// runOn executes a run on an already-registered worker and reports its outcome.
func runOn(t *testing.T, env *testsuite.TestWorkflowEnvironment, wf *v1.Workflow) error {
	t.Helper()

	env.ExecuteWorkflow(engine.Run, &v1.RunState{Workflow: wf})
	require.True(t, env.IsWorkflowCompleted())

	return env.GetWorkflowError()
}

// runOnWorkerWith executes a run on a worker holding the given catalog.
func runOnWorkerWith(t *testing.T, catalog *v1.PluginCatalog, wf *v1.Workflow) error {
	t.Helper()

	return runOn(t, workerHolding(t, catalog), wf)
}

// TestAWorkerWithTheSamePluginRunsThePinnedWorkload is the positive direction,
// and it is here so the negatives below cannot pass for the wrong reason.
func TestAWorkerWithTheSamePluginRunsThePinnedWorkload(t *testing.T) {
	t.Parallel()

	catalog := pluginCatalog("sha256:schema")

	require.NoError(t, runOnWorkerWith(t, catalog, pinnedWorkflow(t, catalog)))
}

// TestAWorkerWithADifferentPluginRefusesTheRun is the rollout case: the same
// plugin at the same version, built from a different task schema, on a worker
// polling the same task queue.
func TestAWorkerWithADifferentPluginRefusesTheRun(t *testing.T) {
	t.Parallel()

	wf := pinnedWorkflow(t, pluginCatalog("sha256:submitted"))

	err := runOnWorkerWith(t, pluginCatalog("sha256:rolled-out"), wf)
	require.Error(t, err, "a worker that cannot reproduce the run's contract executed it anyway")
	require.ErrorContains(t, err, "replay contract")
	require.ErrorContains(t, err, "task schema digest")
}

// TestAWorkerWithoutThePluginRefusesTheRun is the stock worker: registration
// never happened, so nothing here can serve the run. Fail closed.
func TestAWorkerWithoutThePluginRefusesTheRun(t *testing.T) {
	t.Parallel()

	wf := pinnedWorkflow(t, pluginCatalog("sha256:submitted"))

	err := runOnWorkerWith(t, nil, wf)
	require.Error(t, err, "a worker with no plugins at all executed a run pinned to one")
	require.ErrorContains(t, err, "no such plugin installed")
}

// TestOneWorkerCannotAdmitAgainstAnothersCatalog is the isolation direction.
//
// Three workers registered in one process, all of them live at once, each with
// a different answer to "which plugins am I holding": the build the run was
// submitted against, a later build of the same plugin, and none at all. The run
// is pinned to the first. Each of the other two must refuse it *on its own
// inventory* — not admit it because a sibling in this process happens to have
// what it lacks, and not refuse it because a sibling lacks what it has.
//
// The assertion that matters is the one an "each worker reaches its own
// resource" test cannot make. With the catalog held per process — which is what
// this was before #777 — the three registrations write one slot and the last one
// wins, so every worker below answers with the stock worker's empty inventory:
// the admission fails outright, and the *reason* the second worker gives is
// wrong too, "no such plugin installed" where it holds a build of that plugin.
// The third refusal is the one that would still pass, and it is the shape a
// weaker test would have stopped at. Registering all three before running
// anything, and asserting the reason as well as the outcome, is what tells a
// worker answering for itself from a worker relaying its neighbour's answer.
//
// Verified by construction: reintroducing the process value while writing this
// failed here, on the first assertion, with the stock worker's message.
func TestOneWorkerCannotAdmitAgainstAnothersCatalog(t *testing.T) {
	t.Parallel()

	submitted := pluginCatalog("sha256:submitted")

	// Registered up front, and deliberately in an order that leaves the *wrong*
	// answer last: a process value would hand every worker below the stock
	// worker's nil catalog.
	holdsIt := workerHolding(t, submitted)
	holdsAnother := workerHolding(t, pluginCatalog("sha256:rolled-out"))
	holdsNone := workerHolding(t, nil)

	wf := pinnedWorkflow(t, submitted)

	// The worker that has the plugin runs it, while two workers that do not are
	// registered alongside. A shared answer breaks here.
	require.NoError(t, runOn(t, holdsIt, wf),
		"a worker holding exactly the plugin this run is pinned to refused it; "+
			"it was admitted against some other worker's catalog")

	// The worker holding a different build refuses on its own contract, not on
	// its neighbour's — the mismatch names the digest it actually has.
	err := runOn(t, holdsAnother, wf)
	require.Error(t, err, "a worker holding a different build of the plugin executed the run anyway")
	require.ErrorContains(t, err, "task schema digest")

	// And the stock worker refuses for the reason that is true of *it*: no such
	// plugin, rather than the wrong build of one its neighbour has.
	err = runOn(t, holdsNone, wf)
	require.Error(t, err, "a worker with no plugins executed a run pinned to one")
	require.ErrorContains(t, err, "no such plugin installed")
}

// TestAnUnpinnedSpecificationIsRefused covers the specification that reached a
// worker without ever passing through a control plane that resolves plugins.
// There is no contract to check, so there is nothing that says this worker may
// run it.
func TestAnUnpinnedSpecificationIsRefused(t *testing.T) {
	t.Parallel()

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
	t.Parallel()

	suite := &testsuite.WorkflowTestSuite{}
	env := suite.NewTestWorkflowEnvironment()
	env.RegisterWorkflow(engine.Run)
	env.RegisterActivity(engine.Task)
	env.RegisterActivity(engine.TaskInScope)
	env.RegisterActivity(engine.WorkflowVars)

	// Registered by hand rather than through [engine.Register], which is the
	// point: the admission activity is deliberately left out, so an interpreter
	// that schedules it for a workload requiring no plugins fails the run with an
	// unknown activity type and this test says so.

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
