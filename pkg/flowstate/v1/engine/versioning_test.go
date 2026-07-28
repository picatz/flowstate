package engine_test

import (
	"reflect"
	"runtime"
	"strings"
	"testing"
	"time"

	"github.com/nexus-rpc/sdk-go/nexus"
	"github.com/stretchr/testify/require"
	"go.temporal.io/sdk/activity"
	"go.temporal.io/sdk/client"
	"go.temporal.io/sdk/testsuite"
	"go.temporal.io/sdk/worker"
	"go.temporal.io/sdk/workflow"

	v1 "github.com/picatz/flowstate/pkg/flowstate/v1"
	"github.com/picatz/flowstate/pkg/flowstate/v1/engine"
)

// TestDeploymentOptionsNeedsBothHalves covers the pair rule.
//
// A deployment version is a name and a build id together. Accepting one without
// the other would produce a worker that has opted into versioning and cannot be
// addressed by it — visible only once a deploy failed to route anywhere.
func TestDeploymentOptionsNeedsBothHalves(t *testing.T) {
	t.Parallel()

	for _, test := range []struct {
		name       string
		deployment string
		buildID    string
		versioned  bool
	}{
		{name: "neither"},
		{name: "only a deployment name", deployment: "flowstate"},
		{name: "only a build id", buildID: "abc123"},
		{name: "both", deployment: "flowstate", buildID: "abc123", versioned: true},
	} {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()

			options := engine.DeploymentOptions(test.deployment, test.buildID)

			require.Equal(t, test.versioned, options.UseVersioning)
			if !test.versioned {
				// Not merely "versioning off": the SDK panics when a version is
				// present and versioning is not enabled, so half a version must
				// leave nothing behind at all.
				require.Equal(t, worker.DeploymentOptions{}, options)
				return
			}

			require.Equal(t, test.deployment, options.Version.DeploymentName)
			require.Equal(t, test.buildID, options.Version.BuildID)

			// Deliberately unset: Run declares Pinned at registration, and a
			// worker-level default could only ever mask the day that declaration
			// goes missing. The SDK panics without it, which is the failure we
			// want — loud, and before any run exists.
			require.Equal(t, workflow.VersioningBehaviorUnspecified, options.DefaultVersioningBehavior)
		})
	}
}

// TestRegisterInstallsEverythingHistoryCanName pins the registration list.
//
// It was hand-copied into four places before [engine.Register] existed, which is
// four places to forget [engine.TaskWithPrev] — an activity with no callers in
// current code and a name that appears in the history of every run started before
// scopes existed. A worker that does not answer to a name in a run's history
// cannot finish that run.
func TestRegisterInstallsEverythingHistoryCanName(t *testing.T) {
	t.Parallel()

	registry := &recordingRegistry{}
	engine.Register(registry)

	require.Equal(t, []string{"Run"}, registry.workflows,
		"the interpreter is the one workflow type, and it must be registered exactly once")
	require.ElementsMatch(t, []string{"Task", "TaskInScope", "TaskWithPrev"}, registry.activities)
}

// TestRegisterPinsTheInterpreter is the assertion the whole versioning posture
// rests on.
//
// One workflow type runs every definition here, so a change to the interpreter is
// a change to every run in flight at once — and Temporal replays a run's history
// through whatever code the worker has now. Pinned is what keeps a deploy from
// reaching a run that is already going.
//
// Asserted at registration rather than trusted to a comment, because the failure
// it prevents is invisible until a specific deploy meets a specific in-flight run.
func TestRegisterPinsTheInterpreter(t *testing.T) {
	t.Parallel()

	registry := &recordingRegistry{}
	engine.Register(registry)

	require.Equal(t, workflow.VersioningBehaviorPinned, registry.behavior,
		"the interpreter is not pinned; a deploy would change runs already in flight")
}

// TestAPinnedRunTakesTheCurrentVersionAtContinueAsNew is the traversal, not the
// step: the two halves of the posture only work together, and each on its own is
// a defect.
//
// Pinned alone holds a long workload on its original interpreter across every
// Continue-As-New for as long as it lives — so a version with runs on it can never
// drain and an operator can never retire one. Upgrade-at-Continue-As-New alone
// would move a run mid-flight.
//
// The test makes the second half falsifiable by taking the old worker away. A run
// parks at its first gate on build one; build two becomes current; the gate opens,
// the budget of one step is spent, and the run continues as new. Build one is then
// stopped. If the new run had inherited the pin — which is the SDK's default for a
// pinned workflow — nothing could serve it and it would hang until the test's
// deadline. Completing is only possible on build two.
func TestAPinnedRunTakesTheCurrentVersionAtContinueAsNew(t *testing.T) {
	t.Parallel()

	devServer, err := testsuite.StartDevServer(t.Context(), testsuite.DevServerOptions{
		ClientOptions: &client.Options{},
	})
	require.NoError(t, err)
	t.Cleanup(func() { _ = devServer.Stop() })

	temporal := devServer.Client()

	const (
		deployment = "flowstate-versioning-test"
		buildOne   = "build-one"
		buildTwo   = "build-two"
	)

	// Its own queue, so the two builds are the only pollers and a run that fails
	// to route has nowhere else to go — which is what makes the negative result
	// meaningful rather than merely slow.
	taskQueue := "versioning-" + t.Name()

	stopOne := startVersionedWorker(t, temporal, taskQueue, deployment, buildOne)
	setCurrentVersion(t, temporal, deployment, buildOne)

	// Two gates and a budget of one step, so the run suspends between them: the
	// first gate is spent on build one, and whatever resumes gets the second.
	spec := &v1.Workflow{
		Name: "versioned-gates",
		Steps: []*v1.Node{
			signalStep("first-gate", "one", 0),
			signalStep("second-gate", "two", 0),
			echoStep("done", "finished"),
		},
	}

	run, err := temporal.ExecuteWorkflow(t.Context(), client.StartWorkflowOptions{
		ID:        "versioning-" + t.Name(),
		TaskQueue: taskQueue,
	}, engine.Run, &v1.RunState{Workflow: spec, StepsBudget: 1})
	require.NoError(t, err)

	first := run.GetRunID()
	require.NoError(t, temporal.SignalWorkflow(t.Context(), run.GetID(), "", "one", &v1.Node_Outputs{}))

	// Build two is current before the run continues as new, so there is a version
	// to move *to*. Without this the assertion below would pass for the wrong
	// reason: build one would still be the only place to go.
	startVersionedWorker(t, temporal, taskQueue, deployment, buildTwo)
	setCurrentVersion(t, temporal, deployment, buildTwo)

	// Continue-As-New starts a new run under the same workflow id, so the run id
	// changing is the event, and it is the only externally visible one.
	require.Eventually(t, func() bool {
		description, err := temporal.DescribeWorkflowExecution(t.Context(), run.GetID(), "")
		if err != nil {
			return false
		}
		return description.GetWorkflowExecutionInfo().GetExecution().GetRunId() != first
	}, 60*time.Second, 200*time.Millisecond, "the run never continued as new")

	// The falsifying step. From here nothing build one can do matters.
	stopOne()

	require.NoError(t, temporal.SignalWorkflow(t.Context(), run.GetID(), "", "two", &v1.Node_Outputs{}))

	var outputs v1.Workflow_StepOutputs
	require.NoError(t, run.Get(t.Context(), &outputs),
		"the resumed run never completed, so it stayed pinned to a build that is gone")
}

// startVersionedWorker runs one build of the interpreter, returning a function
// that takes it away.
//
// Stopping is returned rather than left to cleanup because taking a build out of
// service mid-test is the point of the test above, and a worker stopped twice is
// a panic.
func startVersionedWorker(t *testing.T, temporal client.Client, taskQueue, deployment, buildID string) func() {
	t.Helper()

	w := worker.New(temporal, taskQueue, worker.Options{
		DeploymentOptions: engine.DeploymentOptions(deployment, buildID),
	})
	engine.Register(w)
	require.NoError(t, w.Start())

	var stopped bool
	stop := func() {
		if stopped {
			return
		}
		stopped = true
		w.Stop()
	}
	t.Cleanup(stop)

	return stop
}

// setCurrentVersion points a deployment at a build, waiting for that build's
// pollers to be visible first.
//
// The server refuses to route to a version it has never seen poll, which is a
// protection worth keeping rather than overriding with AllowNoPollers: a test that
// disabled it would pass against a worker that never started.
func setCurrentVersion(t *testing.T, temporal client.Client, deployment, buildID string) {
	t.Helper()

	handle := temporal.WorkerDeploymentClient().GetHandle(deployment)

	require.Eventually(t, func() bool {
		_, err := handle.SetCurrentVersion(t.Context(), client.WorkerDeploymentSetCurrentVersionOptions{
			BuildID: buildID,
		})
		return err == nil
	}, 60*time.Second, 250*time.Millisecond, "build %q never became the current version", buildID)
}

// recordingRegistry captures what [engine.Register] installs.
//
// A fake rather than a real worker because the assertion is about the arguments,
// and a real worker keeps them to itself — the registration options are not
// readable back off one.
type recordingRegistry struct {
	workflows  []string
	activities []string
	behavior   workflow.VersioningBehavior
}

func (r *recordingRegistry) RegisterWorkflow(w any) {
	r.RegisterWorkflowWithOptions(w, workflow.RegisterOptions{})
}

func (r *recordingRegistry) RegisterWorkflowWithOptions(w any, options workflow.RegisterOptions) {
	r.workflows = append(r.workflows, functionName(w))
	r.behavior = options.VersioningBehavior
}

func (r *recordingRegistry) RegisterActivity(a any) {
	r.activities = append(r.activities, functionName(a))
}

func (r *recordingRegistry) RegisterActivityWithOptions(a any, _ activity.RegisterOptions) {
	r.activities = append(r.activities, functionName(a))
}

func (r *recordingRegistry) RegisterDynamicWorkflow(any, workflow.DynamicRegisterOptions) {}
func (r *recordingRegistry) RegisterDynamicActivity(any, activity.DynamicRegisterOptions) {}
func (r *recordingRegistry) RegisterNexusService(*nexus.Service)                          {}

// functionName is the name Temporal registers a function under: the bare Go
// function name, which is also the name that appears in history.
func functionName(fn any) string {
	full := runtime.FuncForPC(reflect.ValueOf(fn).Pointer()).Name()
	return full[strings.LastIndex(full, ".")+1:]
}
