package engine_test

import (
	"bytes"
	"runtime/pprof"
	"strings"
	"testing"

	v1 "github.com/picatz/flowstate/pkg/flowstate/v1"
	"github.com/picatz/flowstate/pkg/flowstate/v1/engine"
	"github.com/picatz/flowstate/pkg/flowstate/v1/internal/conformance"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	"go.temporal.io/sdk/testsuite"
)

// The one check that can see a leaked coroutine, because nothing else looks.
//
// #492's `async:` runs a step's whole body — its retries, its `vars:`, its
// compensation — on a Temporal coroutine (`workflow.Go` in [engine.startAsync])
// and joins it by receiving on a channel ([asyncStep.wait] in async.go). Every
// exit a scope can take has to reach that receive: the normal join, the
// scope-end join for a step nothing read, and the join a failing scope makes
// on its way out to register compensation before the unwind. A path that
// forgets one leaves a goroutine parked on a channel nobody will ever send to
// — which every other check in this repo is the wrong shape to catch. A race
// test proves two goroutines never corrupt shared state; it says nothing about
// one of them existing forever. A leaked coroutine does no work, trips no
// assertion on the workflow's result, and the test that started it passes.
//
// The [runtime/pprof] "goroutineleak" profile is GA in Go 1.26, gated behind
// GOEXPERIMENT=goroutineleakprofile at build time — the deep tier sets it; an
// ordinary `go test` does not, and every other suite already runs these same
// scenarios (TestRunWorkflowAsync) without paying for the detection GC cycle
// this triggers. So this test skips rather than fails when the profile is not
// registered, and stays this repo's only place that turns it on.
//
// The profile can name a goroutine blocked forever anywhere in the process —
// Temporal's own SDK runs background goroutines this binary did not write.
// Asserting the process is leak-free end to end would make this test a soak
// test for a library, and a failure there would be reported against code that
// never ran. So the assertion is scoped to what the task actually owns: a
// leaked stack is a finding only when its trace names [engine.startAsync] or
// this file's own async.go, which is where a coroutine this package started
// would be parked. A leak elsewhere is logged, not failed — worth someone's
// attention, but not evidence against the coroutine drain this test targets.
func TestAsyncCoroutinesDoNotLeak(t *testing.T) {
	p := pprof.Lookup("goroutineleak")
	if p == nil {
		t.Skip("binary was not built with GOEXPERIMENT=goroutineleakprofile; " +
			"see the goroutineleak job in .github/workflows/deep.yml")
	}

	baseURL := conformance.NewHTTPServer(t)
	for _, test := range conformance.AsyncCases(baseURL) {
		t.Run(test.Name, func(t *testing.T) {
			inputs, err := v1.BindRunInputs(test.Workflow, test.Inputs)
			require.NoError(t, err, "the submission was refused")

			testSuite := &testsuite.WorkflowTestSuite{}
			env := testSuite.NewTestWorkflowEnvironment()
			env.RegisterWorkflow(engine.Run)
			env.OnActivity(engine.Task, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(engine.Task)
			env.OnActivity(engine.TaskInScope, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(engine.TaskInScope)
			env.OnActivity(engine.WorkflowVars, mock.Anything, mock.Anything).Return(engine.WorkflowVars)

			env.ExecuteWorkflow(engine.Run, &v1.RunState{Workflow: test.Workflow, Inputs: inputs})
			require.True(t, env.IsWorkflowCompleted())
			// Deliberately not asserting success/failure here beyond completion:
			// [conformance.AsyncCases] already pins every outcome (TestRunWorkflowAsync
			// runs the identical set for that), including the failing and
			// tolerated-failure cases whose scope has to join on the way out. What
			// this test needs from each case is only that every coroutine it
			// started gets a chance to be joined and then be asked about.
		})
	}

	// One leak-detection GC across every scenario run above, rather than one
	// per case: the detector wants goroutines to have reached a fixed point,
	// and running it after the whole set costs one GC pause instead of dozens.
	var buf bytes.Buffer
	require.NoError(t, p.WriteTo(&buf, 1))

	if p.Count() == 0 {
		return
	}

	report := buf.String()
	if !strings.Contains(report, "engine.(*executor).startAsync") && !strings.Contains(report, "engine/async.go") {
		t.Logf("the runtime found %d leaked goroutine(s) outside async.go; not this test's target, but worth a look:\n%s", p.Count(), report)

		return
	}

	t.Fatalf("a coroutine started by engine.startAsync was never joined:\n%s", report)
}
