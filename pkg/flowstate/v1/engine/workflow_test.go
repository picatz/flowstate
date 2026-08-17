package engine_test

import (
	"errors"
	"fmt"
	"net/http"
	"strconv"
	"testing"
	"time"

	"github.com/google/go-cmp/cmp"
	v1 "github.com/picatz/flowstate/pkg/flowstate/v1"
	"github.com/picatz/flowstate/pkg/flowstate/v1/engine"
	"github.com/picatz/flowstate/pkg/flowstate/v1/flowfile"
	"github.com/picatz/flowstate/pkg/flowstate/v1/internal/conformance"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	"go.temporal.io/sdk/converter"
	"go.temporal.io/sdk/temporal"
	"go.temporal.io/sdk/testsuite"
	"go.temporal.io/sdk/worker"
	"go.temporal.io/sdk/workflow"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/testing/protocmp"
	"google.golang.org/protobuf/types/known/durationpb"
)

// atABound gives an environment the deadlock budget a test running at the
// largest input a bound admits needs, and returns it so it can wrap whichever
// constructor built the environment.
//
// See [conformance.BoundaryDeadlockDetectionTimeout] for why only these tests raise
// it, and why the raise costs their assertions nothing. Only the tests named
// there use this; a workflow goroutine that does not yield for a second anywhere
// else is still a finding, and still fails.
//
// The SDK's setter replaces the worker options wholesale rather than merging, so
// an environment that also needs an interceptor has to set both in one call
// (see tenantWorker, which sets its own and is not a boundary test).
func atABound(env *testsuite.TestWorkflowEnvironment) *testsuite.TestWorkflowEnvironment {
	env.SetWorkerOptions(worker.Options{
		DeadlockDetectionTimeout: conformance.BoundaryDeadlockDetectionTimeout,
	})

	return env
}

func runWorkflow(t *testing.T, input *v1.Workflow, expected *v1.Workflow_StepOutputs) {
	t.Helper()

	testSuite := &testsuite.WorkflowTestSuite{}

	env := testSuite.NewTestWorkflowEnvironment()

	env.RegisterWorkflow(engine.Run)
	env.OnActivity(engine.Task, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(engine.Task)
	env.OnActivity(engine.TaskWithPrev, mock.Anything, mock.Anything, mock.Anything).Return(engine.TaskWithPrev)
	env.OnActivity(engine.TaskInScope, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(engine.TaskInScope)
	// Registered here rather than only in the `vars:` tests: a workflow's block is
	// evaluated in an activity, so any shared case that declares one needs it, and a
	// missing registration surfaces as ActivityNotRegistered rather than as anything
	// about vars.
	env.OnActivity(engine.WorkflowVars, mock.Anything, mock.Anything).Return(engine.WorkflowVars)

	env.ExecuteWorkflow(engine.Run, &v1.RunState{Workflow: input})
	require.True(t, env.IsWorkflowCompleted())
	require.NoError(t, env.GetWorkflowError())

	var output v1.Workflow_StepOutputs
	err := env.GetWorkflowResult(&output)
	require.NoError(t, err)
	require.NotEmpty(t, &output, "Workflow returned empty output")
	require.True(
		t,
		proto.Equal(expected, &output),
		"Expected output does not match actual output:\n%s",
		cmp.Diff(expected, &output, protocmp.Transform()),
	)
}

func TestRunWorkflow(t *testing.T) {
	baseURL := conformance.NewHTTPServer(t)
	for _, test := range conformance.Workflows(baseURL) {
		t.Run(test.Name, func(t *testing.T) {
			b, err := flowfile.Marshal(test.Workflow)
			require.NoError(t, err)
			fmt.Println("\n" + string(b) + "\n")
			runWorkflow(t, test.Workflow, test.ExpectedOutputs)
		})
	}
}

// TestRunWorkflowPolicy runs the shared condition and policy cases against the
// durable driver.
//
// These are the same cases the local driver runs, which is the point: control flow
// is where the two would most easily diverge, and a condition that skipped a step
// locally but ran it here would make local runs untrustworthy.
func TestRunWorkflowPolicy(t *testing.T) {
	failedSteps := conformance.PolicyCaseFailedSteps()

	for _, test := range conformance.PolicyCases() {
		t.Run(test.Name, func(t *testing.T) {
			testSuite := &testsuite.WorkflowTestSuite{}
			env := testSuite.NewTestWorkflowEnvironment()
			env.RegisterWorkflow(engine.Run)
			env.OnActivity(engine.Task, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(engine.Task)
			env.OnActivity(engine.TaskWithPrev, mock.Anything, mock.Anything, mock.Anything).Return(engine.TaskWithPrev)
			env.OnActivity(engine.TaskInScope, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(engine.TaskInScope)
			env.OnActivity(engine.TaskInScope, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(engine.TaskInScope)

			env.ExecuteWorkflow(engine.Run, &v1.RunState{Workflow: test.Workflow})
			require.True(t, env.IsWorkflowCompleted())
			require.NoError(t, env.GetWorkflowError())

			var output v1.Workflow_StepOutputs
			require.NoError(t, env.GetWorkflowResult(&output))

			if test.ExpectedOutputs == nil {
				step, ok := failedSteps[test.Name]
				require.True(t, ok, "case with no expected outputs must name its failed step")
				require.Contains(t, output.GetStepValues(), step)
				require.Contains(t, output.GetStepValues()[step].GetNamedValues(), "error",
					"a step tolerated by continue_on_error must record its failure")
				return
			}

			require.True(
				t,
				proto.Equal(test.ExpectedOutputs, &output),
				"Expected output does not match actual output:\n%s",
				cmp.Diff(test.ExpectedOutputs, &output, protocmp.Transform()),
			)
		})
	}
}

// TestRunWorkflowErrorKind runs the shared [conformance.ErrorKindCases] against the
// durable driver, pinning that a run failing outright is classified the same
// way [flowstatev1_test.TestRunWorkflowErrorKind] pins for the local one —
// invariant 3's "shared cases, two verified callers" for the [v1.ErrorKind]
// #241's P2 puts on RunResponse.Error.kind.
//
// Read from the application error's own Type rather than from
// [v1.ClassifyError] on env.GetWorkflowError() directly: that is exactly what
// a real client does through [flowstatev1.ParseErrorKind] (see server.go's
// failureError), and it is what proves engine.classifyRunError actually wired
// the kind onto the wire rather than only computing it. A bare ClassifyError
// on the SDK's error would pass even if classifyRunError were deleted, because
// ClassifyError's own fallback (Internal, for anything it does not recognize)
// happens to be silent about the difference.
func TestRunWorkflowErrorKind(t *testing.T) {
	baseURL := conformance.NewHTTPServer(t)
	for _, tc := range conformance.ErrorKindCases(baseURL) {
		t.Run(tc.Name, func(t *testing.T) {
			testSuite := &testsuite.WorkflowTestSuite{}
			env := testSuite.NewTestWorkflowEnvironment()
			env.RegisterWorkflow(engine.Run)
			env.OnActivity(engine.Task, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(engine.Task)
			env.OnActivity(engine.TaskInScope, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(engine.TaskInScope)
			env.OnActivity(engine.WorkflowVars, mock.Anything, mock.Anything).Return(engine.WorkflowVars)

			env.ExecuteWorkflow(engine.Run, &v1.RunState{Workflow: tc.Workflow})
			require.True(t, env.IsWorkflowCompleted())

			err := env.GetWorkflowError()
			require.Error(t, err, "the case must fail the run outright")

			var app *temporal.ApplicationError
			require.True(t, errors.As(err, &app),
				"a terminal run failure must reach the client as an ApplicationError, got: %v", err)

			kind, ok := v1.ParseErrorKind(app.Type())
			require.True(t, ok, "the application error's Type %q must be a recognized ErrorKind", app.Type())
			require.Equal(t, tc.ExpectedKind, kind)
		})
	}
}

// TestRunWorkflowTaskPolicy runs #187 slice 1's shared task-shape policy
// cases ([conformance.TaskPolicyCases]) against the durable driver — the same
// cases [flowstatev1_test.TestRunWorkflowTaskPolicy] runs against the local
// one, which is what keeps the two agreeing about which dispatches a
// deployment's policy refuses (invariant 3).
//
// Errors round-trip through Temporal's failure conversion here, unlike the
// local driver's bare Go error chain, so this asserts by text — the same
// choice [TestRunWorkflowTaskOutputElementBound] already makes for the
// identical reason (see its own comment: `require.Contains(t, err.Error(),
// "10000")` rather than errors.As) — rather than pretending a
// *v1.TaskPolicyDeniedError survives the wire.
func TestRunWorkflowTaskPolicy(t *testing.T) {
	for _, tc := range conformance.TaskPolicyCases() {
		t.Run(tc.Name, func(t *testing.T) {
			policy, err := tc.Policy.Policy()
			require.NoError(t, err, "every case's policy must itself compile")

			v1.SetDefaultTaskPolicy(policy)
			t.Cleanup(func() { v1.SetDefaultTaskPolicy(nil) })

			testSuite := &testsuite.WorkflowTestSuite{}
			env := testSuite.NewTestWorkflowEnvironment()
			env.RegisterWorkflow(engine.Run)
			env.OnActivity(engine.Task, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(engine.Task)
			env.OnActivity(engine.TaskWithPrev, mock.Anything, mock.Anything, mock.Anything).Return(engine.TaskWithPrev)
			env.OnActivity(engine.TaskInScope, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(engine.TaskInScope)

			// The durable driver's route for the case's identity: the server
			// established it and it rides on the run's own state, which
			// workflow.go copies into the scope every task is dispatched in.
			// A different route from the local driver's rehearsal context
			// value, deliberately — what the pair of callers compares is the
			// answer, not the plumbing.
			env.ExecuteWorkflow(engine.Run, &v1.RunState{Workflow: tc.Workflow, Identity: tc.Identity})
			require.True(t, env.IsWorkflowCompleted())

			if tc.DeniedTask != "" {
				workflowErr := env.GetWorkflowError()
				require.Error(t, workflowErr, "the policy must refuse this dispatch")
				require.Contains(t, workflowErr.Error(), tc.DeniedTask,
					"the denial must name the task it refused")
				require.Contains(t, workflowErr.Error(), string(tc.DeniedReason),
					"the denial must name why — the rule category the design record's "+
						"diagnostics rule asks a deployment refusal to state")
				require.Contains(t, workflowErr.Error(), "task-shape policy",
					"the denial must read as a deployment refusal, not an ordinary task failure")
				return
			}

			require.NoError(t, env.GetWorkflowError())

			var output v1.Workflow_StepOutputs
			require.NoError(t, env.GetWorkflowResult(&output))
			require.Empty(t, cmp.Diff(tc.ExpectedOutputs, &output, protocmp.Transform()))
		})
	}
}

// TestRunWorkflowHasGuardOnToleratedSuccess runs
// [conformance.ToleratedSuccessHasGuardCases] against the durable driver — the value
// both drivers must agree `has(steps.<id>.error)` reads once a `continue_on_error`
// step has actually succeeded, before any Continue-As-New handover is involved.
// See #176 and, for the seam-specific case this value must also survive
// compaction across, TestContinueAsNewCarriesATolerantStepReferencedOnlyByAnAbsentField
// below.
func TestRunWorkflowHasGuardOnToleratedSuccess(t *testing.T) {
	for _, test := range conformance.ToleratedSuccessHasGuardCases() {
		t.Run(test.Name, func(t *testing.T) {
			runWorkflow(t, test.Workflow, test.ExpectedOutputs)
		})
	}
}

// TestRunWorkflowControlFlow runs the shared loop and parallel cases against the
// durable driver, where iterations and branches are genuinely concurrent.
func TestRunWorkflowControlFlow(t *testing.T) {
	baseURL := conformance.NewHTTPServer(t)
	for _, test := range conformance.ControlFlowCases(baseURL) {
		t.Run(test.Name, func(t *testing.T) {
			runWorkflow(t, test.Workflow, test.ExpectedOutputs)
		})
	}
}

// chained returns three steps where each reads the value the one before produced.
//
// A chain rather than three independent steps because what the budget tests are
// about is *carryover*: a step's output has to still be there for the step that
// names it, whether or not the run suspended in between. Three steps that ignore
// each other would pass with the carryover removed entirely.
//
// The steps are `http` against the loopback echo server because a value now has to
// come from somewhere. `echo` retired at edition v2026.2 and nothing that remains
// produces a value locally — `log` deliberately returns none, so a chain built from
// it would have nothing to chain. The server hands each request's body back, which
// makes `<step>.said` a real recorded output the next step's expression can read.
func chained(httpBaseURL string) []*v1.Node {
	echoes := func(id, body string) *v1.Node {
		return &v1.Node{
			Id: id,
			Kind: &v1.Node_Task{Task: &v1.Task{
				Name: "http",
				Inputs: map[string]*v1.Value{
					"method":  v1.NewLiteral(http.MethodPost),
					"url":     v1.NewLiteral(httpBaseURL + "/echo"),
					"body":    v1.NewExpr(body),
					"outputs": v1.NewExpr(`{"said": response.body}`),
				},
			}},
		}
	}

	return []*v1.Node{
		echoes("a", `"hi"`),
		echoes("b", "a.said"),
		echoes("c", "b.said"),
	}
}

// saidHi is what [chained] produces when every link of the chain held.
//
// Every step carries the same string deliberately: `a`'s literal reaching `c`
// unchanged is the claim, so any link that lost its predecessor's value shows up
// as a missing or empty `said` rather than as a plausible-looking different one.
func saidHi() *v1.Workflow_StepOutputs {
	said := func() *v1.Node_Outputs {
		return &v1.Node_Outputs{NamedValues: map[string]*v1.Value{"said": v1.NewLiteral("hi")}}
	}

	return &v1.Workflow_StepOutputs{StepValues: map[string]*v1.Node_Outputs{
		"a": said(), "b": said(), "c": said(),
	}}
}

// A budget decides whether a run finishes in one segment or suspends, and there used to
// be two tests about it that were byte-for-byte the same.
//
// Both set a budget of three against three steps, and the one named
// TestRunWorkflow_ContinueAsNewBudget said so in its own comment — "equal to number of
// steps to avoid Continue-As-New". So the suite carried two copies of a test that
// exercised the machinery its name is about *not at all*, which is the most expensive
// kind of coverage: it reads as tested.
//
// They are one test per outcome now, named for the outcome.

// budgetEnv builds a test environment able to run a workflow and continue it.
//
// The workflow is registered because Continue-As-New dispatches the next run through
// the registry rather than by calling the function again.
func budgetEnv(t *testing.T) *testsuite.TestWorkflowEnvironment {
	t.Helper()

	testSuite := &testsuite.WorkflowTestSuite{}
	env := testSuite.NewTestWorkflowEnvironment()

	env.RegisterWorkflow(engine.Run)
	env.OnActivity(engine.Task, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(engine.Task)
	env.OnActivity(engine.TaskWithPrev, mock.Anything, mock.Anything, mock.Anything).Return(engine.TaskWithPrev)
	env.OnActivity(engine.TaskInScope, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(engine.TaskInScope)
	env.OnActivity(engine.WorkflowVars, mock.Anything, mock.Anything).Return(engine.WorkflowVars)

	return env
}

// TestABudgetThatFitsRunsInOneSegment is the boundary case: exactly enough budget for
// the steps there are.
//
// Worth having on its own, because off-by-one here is a run that suspends when it did
// not need to — correct, invisible, and paying for a Continue-As-New every time.
func TestABudgetThatFitsRunsInOneSegment(t *testing.T) {
	baseURL := conformance.NewHTTPServer(t)
	env := budgetEnv(t)

	wf := &v1.Workflow{Name: "budget-fits", Steps: chained(baseURL)}

	env.ExecuteWorkflow(engine.Run, &v1.RunState{Workflow: wf, StepsBudget: 3})
	require.True(t, env.IsWorkflowCompleted())
	require.NoError(t, env.GetWorkflowError(),
		"a run with exactly enough budget suspended instead of finishing")

	var output v1.Workflow_StepOutputs
	require.NoError(t, env.GetWorkflowResult(&output))

	expected := saidHi()
	require.True(
		t,
		proto.Equal(expected, &output),
		"Expected output does not match actual output:\n%s",
		cmp.Diff(expected, &output, protocmp.Transform()),
	)
}

// TestABudgetSmallerThanTheWorkflowContinuesAsNew is the case neither old test reached.
//
// A run out of budget does not fail and does not finish: it suspends, carrying forward
// where it had got to and the outputs the remaining steps still need. In the test
// environment that surfaces as a ContinueAsNewError rather than as a second segment
// running, which is what makes the *carried state* inspectable — and the carried state
// is the whole of what these tests are about.
//
// What is asserted is that it advanced and that it carried: a suspension that resumed
// from the beginning, or one that arrived with nothing for `b` to read `a.said` out of,
// are the two ways this goes wrong and neither shows up as an error.
func TestABudgetSmallerThanTheWorkflowContinuesAsNew(t *testing.T) {
	baseURL := conformance.NewHTTPServer(t)
	env := budgetEnv(t)

	wf := &v1.Workflow{Name: "budget-exhausted", Steps: chained(baseURL)}

	env.ExecuteWorkflow(engine.Run, &v1.RunState{Workflow: wf, StepsBudget: 1})
	require.True(t, env.IsWorkflowCompleted())

	err := env.GetWorkflowError()
	require.Error(t, err, "a run with one step of budget and three steps finished in one segment")

	var continued *workflow.ContinueAsNewError
	require.ErrorAs(t, err, &continued,
		"a run out of budget failed instead of continuing as new: %v", err)

	// The state the next segment starts from, read back out of what was handed to it.
	require.Len(t, continued.Input.GetPayloads(), 1, "the next segment was passed no state")

	var next v1.RunState
	require.NoError(t, converter.GetDefaultDataConverter().
		FromPayload(continued.Input.GetPayloads()[0], &next))

	// It moved. A suspension that carried a position of zero would resume from the
	// first step and run the whole workflow again, once per segment, forever.
	require.NotEmpty(t, resumedPosition(&next),
		"the next segment resumes from the beginning, so the run would never end")

	// And it carried what is still needed. `b` reads `a.said`, so a segment arriving
	// without `a`'s outputs fails on an unresolved reference — the failure mode the
	// carryover exists to prevent, and one that only appears on the *second* segment.
	require.Contains(t, next.GetOutputs().GetStepValues(), "a",
		"the outputs a later step reads were not carried into the next segment")
}

// TestContinueAsNewCarriesATolerantStepReferencedOnlyByAnAbsentField is issue
// #176's exact reproduction: `StepsBudget: 1` forcing a handover right after a
// `parallel:` block, where the only later reference to one of its branches is a
// field the branch's own successful run never produced.
//
// `checkout` is marked `continue_on_error` and succeeds, so its outputs are the
// empty message `log` always returns — no `error` field. `summary`'s condition
// names nothing else about it: `!has(steps.checkout.error)`. `neededOutputs`
// walks that reference, finds no `error` in `checkout`'s actual outputs, and used
// to report "nothing needed" for the whole step — which `compactOutputsForRemainingSteps`
// then read as "drop the key", not "keep it with nothing filtered in". The
// resumed segment evaluated `steps.checkout` against a carried state with no
// `checkout` key at all: `no such key: checkout`, a hard CEL error, in the exact
// place `has()` exists to answer `false` instead.
//
// Reverting the [neededOutputs] fix turns this red with that exact message; see
// the fix's own comment on why the key has to survive regardless of which field,
// if any, matched.
func TestContinueAsNewCarriesATolerantStepReferencedOnlyByAnAbsentField(t *testing.T) {
	wf := &v1.Workflow{
		Name: "compaction-haskey-176",
		Steps: []*v1.Node{
			{
				Id: "checks",
				Kind: &v1.Node_Parallel{Parallel: &v1.Parallel{
					Branches: []*v1.Parallel_Branch{
						{Steps: []*v1.Node{bestEffort(logStep("checkout", "ok"))}},
					},
				}},
			},
			gatedOn(logStep("summary", "done"), "!has(steps.checkout.error)"),
		},
	}

	env := budgetEnv(t)
	env.ExecuteWorkflow(engine.Run, &v1.RunState{Workflow: wf, StepsBudget: 1})
	require.True(t, env.IsWorkflowCompleted())

	err := env.GetWorkflowError()
	require.Error(t, err, "a budget of one step, with a parallel block first, finished in one segment")

	var continued *workflow.ContinueAsNewError
	require.ErrorAs(t, err, &continued, "did not continue as new: %v", err)

	var next v1.RunState
	require.NoError(t, converter.GetDefaultDataConverter().FromPayload(continued.Input.GetPayloads()[0], &next))

	// The seam lands exactly where the issue describes it: right after the
	// parallel block, with `summary` still to run.
	require.Contains(t, next.GetOutputs().GetStepValues(), "checkout",
		"the tolerated step's key was dropped from carryover because the only "+
			"reference to it — has(steps.checkout.error) — matched no field on its "+
			"own (empty, successful) outputs; see #176")

	// Drive the resumed segment(s) to completion the way a real worker would,
	// mirroring the other budget tests in this file.
	state := &next
	var out v1.Workflow_StepOutputs
	for segment := 0; ; segment++ {
		require.Less(t, segment, 10, "did not converge in a reasonable number of segments")

		seg := budgetEnv(t)
		seg.ExecuteWorkflow(engine.Run, state)
		require.True(t, seg.IsWorkflowCompleted())

		segErr := seg.GetWorkflowError()
		if segErr == nil {
			require.NoError(t, seg.GetWorkflowResult(&out))
			break
		}

		var again *workflow.ContinueAsNewError
		require.ErrorAsf(t, segErr, &again,
			"segment %d failed instead of continuing as new: %v", segment, segErr)

		state = &v1.RunState{}
		require.NoError(t, converter.GetDefaultDataConverter().FromPayload(again.Input.GetPayloads()[0], state))
	}

	// `summary` ran, which it can only do if `has(steps.checkout.error)`
	// resolved to `false` rather than erroring the CEL evaluation outright.
	require.Contains(t, out.GetStepValues(), "summary",
		"the resumed run did not reach the step gated on has(steps.checkout.error); "+
			"a dropped key resolves that as a hard CEL error rather than false")
	require.Contains(t, out.GetStepValues(), "checkout",
		"the tolerated step's own outputs did not survive to the finished run")

	// Both drivers must agree — the same run in one local-driver segment.
	local, err := v1.Run(t.Context(), wf)
	require.NoError(t, err)
	require.Empty(t, cmp.Diff(local, &out, protocmp.Transform()))
}

// TestCallSuspendsMidCalleeAndCarriesItsOutputs is the invariant-10 test
// `Frame.CallOutputs` needs: the field is new, so nothing written before this
// feature existed could ever carry a value in it, and this is what proves the
// executor treats that absence exactly like any other RunState a prior
// interpreter wrote — correctly, rather than merely not crashing.
//
// It also pins the bug this shape found once already: a callee's own scope
// starts from CallScope's freshly allocated map, and stashing it into
// Frame.CallOutputs when a run suspends mid-callee round-trips through
// Continue-As-New's wire converter — which drops an *empty* map entirely,
// since protobuf has no wire representation for one. A resume that trusted the
// round-tripped value without re-checking wrote into that nil map the moment
// the callee's own first step tried to record its output: a panic Temporal's
// test environment retries into what looks indistinguishable from a hang
// rather than a failure invariant 9 would have caught cleanly.
func TestCallSuspendsMidCalleeAndCarriesItsOutputs(t *testing.T) {
	// A callee with two steps and nothing to do with the network, so the only
	// thing that decides where Continue-As-New lands is the step budget: a
	// for_each (which suspends between iterations, transparently through the
	// call) and a step reading the loop's own `results` — carried the whole
	// way only if Frame.CallOutputs survived the handover.
	callee := &v1.Workflow{
		Name:    "callee-can",
		Profile: v1.CurrentProfile,
		Steps: []*v1.Node{
			{
				Id: "batches",
				Kind: &v1.Node_ForEach{ForEach: &v1.ForEach{
					Items:    v1.NewLiteralList("a", "b"),
					Iterator: "item",
					Body:     []*v1.Node{logStep("checked", "ok")},
				}},
			},
			logStep("summary", "done"),
		},
	}
	wf := &v1.Workflow{
		Name:    "call-can",
		Profile: v1.CurrentProfile,
		Steps: []*v1.Node{
			{
				Id:   "provision",
				Kind: &v1.Node_Call{Call: &v1.Call{Workflow: callee}},
			},
			gatedOn(logStep("after", "caller resumed"), "has(steps.provision)"),
		},
	}

	env := budgetEnv(t)
	env.ExecuteWorkflow(engine.Run, &v1.RunState{Workflow: wf, StepsBudget: 1})
	require.True(t, env.IsWorkflowCompleted())

	err := env.GetWorkflowError()
	require.Error(t, err, "a budget of one step, three steps deep through a call, finished in one segment")

	var continued *workflow.ContinueAsNewError
	require.ErrorAs(t, err, &continued, "did not continue as new: %v", err)

	var next v1.RunState
	require.NoError(t, converter.GetDefaultDataConverter().FromPayload(continued.Input.GetPayloads()[0], &next))

	// The suspension landed inside the callee's loop, not merely between the
	// caller's own two top-level steps — which is the whole property under
	// test: a call is transparent to suspension, so the boundary the budget
	// finds is wherever the *true* first step is, three levels down.
	frames := next.GetFrames()
	require.GreaterOrEqual(t, len(frames), 3,
		"expected a frame per level down to the loop iteration, got %d", len(frames))

	// Resume from the carried state, the way the real Continue-As-New would —
	// and, since a budget of one forces a handover at every remaining step
	// too, drive it through however many more segments it takes, the way a
	// real worker picking each one up in turn would.
	state := &next
	var out v1.Workflow_StepOutputs
	for segment := 0; ; segment++ {
		require.Less(t, segment, 10, "did not converge in a reasonable number of segments")

		seg := budgetEnv(t)
		seg.ExecuteWorkflow(engine.Run, state)
		require.True(t, seg.IsWorkflowCompleted())

		err := seg.GetWorkflowError()
		if err == nil {
			require.NoError(t, seg.GetWorkflowResult(&out))
			break
		}

		var again *workflow.ContinueAsNewError
		require.ErrorAs(t, err, &again, "segment %d failed rather than continuing: %v", segment, err)

		state = &v1.RunState{}
		require.NoError(t, converter.GetDefaultDataConverter().FromPayload(again.Input.GetPayloads()[0], state))
	}

	// `after`'s condition names `steps.provision`, which is what keeps
	// compaction from pruning the call's own output on a later handover the
	// way it would an ordinary step's — so both must still be present.
	require.Contains(t, out.GetStepValues(), "after",
		"the caller did not resume past the call")
	require.Contains(t, out.GetStepValues(), "provision",
		"the call's own step is missing from the finished run's outputs")

	// The two drivers must agree, which is invariant 3 stated once more: a run
	// forced through several Continue-As-New segments is still the same
	// workload `flow run local` would execute in one.
	local, err := v1.Run(t.Context(), wf)
	require.NoError(t, err)
	require.Empty(t, cmp.Diff(local, &out, protocmp.Transform()))
}

// TestCallVarsSurviveMidCalleeSuspend is the suspend join for `Frame.call_vars`:
// a callee whose own `vars:` are read *before* the loop iteration a budget of
// one suspends inside, and again *after* it resumes — including in the
// callee's own declared outputs, evaluated in the segment that finishes it.
//
// This is the shape that would show a driver re-evaluating the callee's vars
// on resume instead of carrying the first segment's answer: the loop body
// before the suspend and the step after it read the identical expression, so
// if the two segments disagreed about what `vars.prefix` was — the failure
// mode invariant 4 names for the top-level `vars:` this mirrors — the run's
// own outputs would contradict themselves rather than merely being wrong.
func TestCallVarsSurviveMidCalleeSuspend(t *testing.T) {
	callee := &v1.Workflow{
		Name:    "callee-vars-can",
		Profile: v1.CurrentProfile,
		Vars:    map[string]*v1.Value{"prefix": v1.NewLiteral("eu-")},
		Steps: []*v1.Node{
			{
				Id: "batches",
				Kind: &v1.Node_ForEach{ForEach: &v1.ForEach{
					Items:    v1.NewLiteralList("a", "b"),
					Iterator: "item",
					// Read before the budget's suspend point: the first iteration
					// runs, consumes the budget, and the second resumes in a later
					// segment — so this is the read a re-evaluation could disagree
					// with itself about.
					Body: []*v1.Node{gatedOn(logStep("checked", "ok"), `(vars.prefix + item) in ["eu-a", "eu-b"]`)},
				}},
			},
			// Read again, after the loop has resumed and finished — the second of
			// the pair that has to agree.
			gatedOn(logStep("summary", "done"), `vars.prefix == "eu-"`),
		},
		DeclaredOutputs: []*v1.OutputDeclaration{
			{Name: "region", Value: v1.NewExpr(`vars.prefix + "west"`)},
		},
	}
	wf := &v1.Workflow{
		Name:    "call-vars-can",
		Profile: v1.CurrentProfile,
		Steps: []*v1.Node{
			{
				Id:   "provision",
				Kind: &v1.Node_Call{Call: &v1.Call{Workflow: callee}},
			},
			gatedOn(logStep("after", "caller resumed"), "has(steps.provision)"),
		},
	}

	// A count of how many times the callee's vars were actually evaluated,
	// across every segment — the sharpest version of this test, because it is
	// the one assertion a driver that recomputed on every resume instead of
	// carrying the first segment's answer could not pass by accident. A run
	// spanning several segments must still evaluate the callee's `vars:`
	// exactly once, the same guarantee `RunState.Vars` gives the top level's.
	var evaluations int

	state := &v1.RunState{Workflow: wf, StepsBudget: 1}
	var out v1.Workflow_StepOutputs
	for segment := 0; ; segment++ {
		require.Less(t, segment, 10, "did not converge in a reasonable number of segments")

		testSuite := &testsuite.WorkflowTestSuite{}
		seg := testSuite.NewTestWorkflowEnvironment()
		seg.RegisterWorkflow(engine.Run)
		seg.OnActivity(engine.Task, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(engine.Task)
		seg.OnActivity(engine.TaskWithPrev, mock.Anything, mock.Anything, mock.Anything).Return(engine.TaskWithPrev)
		seg.OnActivity(engine.TaskInScope, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(engine.TaskInScope)
		seg.OnActivity(engine.WorkflowVars, mock.Anything, mock.Anything).
			Run(func(mock.Arguments) { evaluations++ }).
			Return(engine.WorkflowVars)
		seg.ExecuteWorkflow(engine.Run, state)
		require.True(t, seg.IsWorkflowCompleted())

		err := seg.GetWorkflowError()
		if err == nil {
			require.NoError(t, seg.GetWorkflowResult(&out))
			break
		}

		var again *workflow.ContinueAsNewError
		require.ErrorAs(t, err, &again, "segment %d failed rather than continuing: %v", segment, err)

		state = &v1.RunState{}
		require.NoError(t, converter.GetDefaultDataConverter().FromPayload(again.Input.GetPayloads()[0], state))

		// Direct inspection of the mechanism, not just the eventual answer: the
		// carried state itself has to hold the callee's evaluated vars at
		// whichever frame stands inside the call, so a later segment has
		// something to restore rather than a reason to recompute. Checked at
		// least once, on the first handover, where the call is freshest.
		if segment == 0 {
			var found bool
			for _, frame := range state.GetFrames() {
				if prefix, ok := frame.GetCallVars()["prefix"]; ok {
					found = true
					require.Equal(t, "eu-", prefix.GetLiteral().GetStringValue(),
						"the carried call_vars holds the wrong value for the callee's own var")
				}
			}
			require.True(t, found, "no frame carried the callee's evaluated vars across the handover")
		}
	}

	require.Contains(t, out.GetStepValues(), "after", "the caller did not resume past the call")
	require.Equal(t, "eu-west",
		out.GetStepValues()["provision"].GetNamedValues()["region"].GetLiteral().GetStringValue(),
		"the callee's declared output did not see the vars its own steps saw")

	// The decision itself: evaluated once for the whole call, carried across
	// every segment after — never once per segment, which is what a driver
	// that trusted re-evaluation instead of `Frame.call_vars` would do.
	require.Equal(t, 1, evaluations,
		"the callee's vars were evaluated more than once across the call's segments; "+
			"they should be evaluated once and carried, exactly like the top level's")

	// Both drivers must agree — the same run in one segment locally.
	local, err := v1.Run(t.Context(), wf)
	require.NoError(t, err)
	require.Empty(t, cmp.Diff(local, &out, protocmp.Transform()))
}

// resumedPosition returns where a carried state says the run continues from, in
// whichever of the two spellings it uses.
//
// A run started before frames existed carries only next_step, and resumeFrames
// translates one into the other; a test that read only frames would report a correctly
// carried older state as a run resuming from the beginning.
func resumedPosition(st *v1.RunState) []int32 {
	if frames := st.GetFrames(); len(frames) > 0 {
		out := make([]int32, 0, len(frames))
		for _, frame := range frames {
			out = append(out, frame.GetNextNode())
		}

		return out
	}
	if st.GetNextStep() > 0 {
		return []int32{st.GetNextStep()}
	}

	return nil
}

// TestRunWorkflowLog covers the `log` task in the durable driver.
//
// The route differs from the local driver's in a way this is the only check on: a log
// step's outputs cross the wire as a proto message and are written into a map on the
// far side, so an empty message and an absent one are one deserialization apart.
func TestRunWorkflowLog(t *testing.T) {
	for _, test := range conformance.LogCases() {
		t.Run(test.Name, func(t *testing.T) {
			testSuite := &testsuite.WorkflowTestSuite{}
			env := testSuite.NewTestWorkflowEnvironment()
			env.RegisterWorkflow(engine.Run)
			env.OnActivity(engine.Task, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(engine.Task)
			env.OnActivity(engine.TaskInScope, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(engine.TaskInScope)
			env.OnActivity(engine.WorkflowVars, mock.Anything, mock.Anything).Return(engine.WorkflowVars)

			env.ExecuteWorkflow(engine.Run, &v1.RunState{Workflow: test.Workflow})
			require.True(t, env.IsWorkflowCompleted())
			require.NoError(t, env.GetWorkflowError())

			var out v1.Workflow_StepOutputs
			require.NoError(t, env.GetWorkflowResult(&out))
			require.Empty(t, cmp.Diff(test.ExpectedOutputs, &out, protocmp.Transform()))
		})
	}
}

// TestRunWorkflowVars covers `vars:` in the durable driver.
//
// The same cases the local driver runs. Here they exercise a route the local driver
// does not have: the workflow's block is evaluated by the WorkflowVars activity rather
// than in workflow code, because a profile pins which functions exist and not how
// cel-go implements them — so evaluating it inline would be a replay divergence waiting
// on a dependency bump. A step's block is evaluated in workflow code, alongside that
// step's expression inputs, by swapping the executor's scope; a nested executor built
// from the wrong one is a divergence only these can see.
func TestRunWorkflowVars(t *testing.T) {
	baseURL := conformance.NewHTTPServer(t)
	for _, test := range conformance.VarsCases(baseURL) {
		t.Run(test.Name, func(t *testing.T) {
			testSuite := &testsuite.WorkflowTestSuite{}
			env := testSuite.NewTestWorkflowEnvironment()
			env.RegisterWorkflow(engine.Run)
			env.OnActivity(engine.Task, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(engine.Task)
			env.OnActivity(engine.TaskInScope, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(engine.TaskInScope)
			env.OnActivity(engine.WorkflowVars, mock.Anything, mock.Anything).Return(engine.WorkflowVars)

			env.ExecuteWorkflow(engine.Run, &v1.RunState{Workflow: test.Workflow})
			require.True(t, env.IsWorkflowCompleted())
			require.NoError(t, env.GetWorkflowError())

			var out v1.Workflow_StepOutputs
			require.NoError(t, env.GetWorkflowResult(&out))
			require.Empty(t, cmp.Diff(test.ExpectedOutputs, &out, protocmp.Transform()))
		})
	}
}

// TestRunWorkflowInputsAndOutputs runs the shared `inputs:`/`outputs:` cases
// against the durable driver.
//
// The arguments are bound the way the server binds them — [v1.BindRunInputs], once,
// before the workflow is started — and then handed to the run in `RunState`, which
// is exactly what `server.Run` does. The engine deliberately does not re-derive
// them: re-applying a default at the top of every segment would let a declaration
// edited between deploys change an argument underneath a run in flight, so what a
// segment reads is what the submission established.
//
// The declared outputs are the other half, and the durable route to them is the
// longer one: they are evaluated in workflow code after the last step, and the
// result crosses Temporal's payload converter as part of the run's completion.
func TestRunWorkflowInputsAndOutputs(t *testing.T) {
	baseURL := conformance.NewHTTPServer(t)
	for _, test := range conformance.InputOutputCases(baseURL) {
		t.Run(test.Name, func(t *testing.T) {
			inputs, err := v1.BindRunInputs(test.Workflow, test.Inputs)
			require.NoError(t, err, "the submission was refused")
			require.NoError(t, v1.CheckSubmissionSize(test.Workflow, inputs))

			testSuite := &testsuite.WorkflowTestSuite{}
			env := testSuite.NewTestWorkflowEnvironment()
			env.RegisterWorkflow(engine.Run)
			env.OnActivity(engine.Task, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(engine.Task)
			env.OnActivity(engine.TaskInScope, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(engine.TaskInScope)
			env.OnActivity(engine.WorkflowVars, mock.Anything, mock.Anything).Return(engine.WorkflowVars)

			env.ExecuteWorkflow(engine.Run, &v1.RunState{Workflow: test.Workflow, Inputs: inputs})
			require.True(t, env.IsWorkflowCompleted())
			require.NoError(t, env.GetWorkflowError())

			var out v1.Workflow_StepOutputs
			require.NoError(t, env.GetWorkflowResult(&out))
			require.Empty(t, cmp.Diff(test.ExpectedOutputs, &out, protocmp.Transform()))
		})
	}
}

// TestRunWorkflowValue is the durable half of `value:`. See the local driver's
// TestRunWorkflowValue, which runs the identical [conformance.ValueCases].
//
// The answer, the name it is recorded under, and what a skipped one leaves behind
// all have to hold here exactly as they do locally, because each is decided by the
// one function both drivers call ([v1.EvalValueNode]) at the one position both
// drivers evaluate it in: workflow code, in written order. Nothing is scheduled,
// so no activity mock stands between this and the answer, which is the whole
// reason a value replays rather than being remembered.
// TestRunWorkflowInterpolation is the durable half of #413's interpolation. See
// the local driver's TestRunWorkflowInterpolation, which runs the identical
// [conformance.InterpolationCases].
//
// What it holds is the `string()` around every fence, since the interpolation
// itself is gone by the time either driver reads the workflow. A durable run
// evaluates that conversion in workflow code on replay, so a rendering that
// depended on anything outside the expression — a local zone, a locale, the
// moment it ran — would differ from the local driver's and, worse, from its own
// earlier replay.
func TestRunWorkflowInterpolation(t *testing.T) {
	for _, test := range conformance.InterpolationCases() {
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
			require.NoError(t, env.GetWorkflowError())

			var out v1.Workflow_StepOutputs
			require.NoError(t, env.GetWorkflowResult(&out))
			require.Empty(t, cmp.Diff(test.ExpectedOutputs, &out, protocmp.Transform()))
		})
	}
}

func TestRunWorkflowValue(t *testing.T) {
	for _, test := range conformance.ValueCases() {
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

			if test.ExpectFailure {
				require.Error(t, env.GetWorkflowError(),
					"reading a value that never ran was expected to fail the run")
				return
			}
			require.NoError(t, env.GetWorkflowError())

			var out v1.Workflow_StepOutputs
			require.NoError(t, env.GetWorkflowResult(&out))
			require.Empty(t, cmp.Diff(test.ExpectedOutputs, &out, protocmp.Transform()))
		})
	}
}

// TestRunWorkflowWebhookTrigger covers a declared `triggers:` webhook against
// the durable driver, pairing the local run of the identical
// [conformance.WebhookTriggerCases].
//
// The route differs in the way that makes the pairing worth having: here the
// declaration crosses the wire inside [v1.RunState.Workflow], is written to
// history, and is carried through every Continue-As-New — so a driver that read
// it, or a compaction that dropped it into a run that then behaved differently,
// would show up here and nowhere else.
func TestRunWorkflowWebhookTrigger(t *testing.T) {
	runTriggerCases(t, conformance.WebhookTriggerCases())
}

// TestRunWorkflowWebhookDelivery covers a run started by a delivery against the
// durable driver, pairing the local run of the identical
// [conformance.WebhookDeliveryCases].
//
// Here the mapped inputs cross the wire inside [v1.RunState.Inputs] and are
// written to history, which is where a value that only *looked* like an integer
// would stop looking like one.
func TestRunWorkflowWebhookDelivery(t *testing.T) {
	for _, test := range conformance.WebhookDeliveryCases() {
		require.NotNil(t, test.Inputs, "the delivery did not bind, so there is nothing to run")
	}

	runTriggerCases(t, conformance.WebhookDeliveryCases())
}

// TestRunWorkflowTriggerContext covers reading `trigger` against the durable
// driver, pairing the local run of the identical [conformance.TriggerContextCases].
//
// The route is what makes the pairing worth having. Here the context is a field
// of [v1.RunState]: it crosses the wire, is written to history, is handed to an
// activity inside the compacted [v1.Scope], and is carried across every
// Continue-As-New. Locally it is a value on a context. A field dropped anywhere
// along that path shows up here and nowhere else.
func TestRunWorkflowTriggerContext(t *testing.T) {
	runTriggerCases(t, conformance.TriggerContextCases())
}

// runTriggerCases runs one trigger corpus, so the three above cannot drift in how
// they run what they were given.
func runTriggerCases(t *testing.T, cases []conformance.Case) {
	t.Helper()

	for _, test := range cases {
		t.Run(test.Name, func(t *testing.T) {
			inputs, err := v1.BindRunInputs(test.Workflow, test.Inputs)
			require.NoError(t, err, "the submission was refused")

			testSuite := &testsuite.WorkflowTestSuite{}
			env := testSuite.NewTestWorkflowEnvironment()
			env.RegisterWorkflow(engine.Run)
			env.OnActivity(engine.Task, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(engine.Task)
			env.OnActivity(engine.TaskInScope, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(engine.TaskInScope)
			env.OnActivity(engine.WorkflowVars, mock.Anything, mock.Anything).Return(engine.WorkflowVars)

			env.ExecuteWorkflow(engine.Run, &v1.RunState{
				Workflow: test.Workflow,
				Inputs:   inputs,

				// Where the durable driver's half of a trigger context lives: in
				// the state message, not on a context value, which is the
				// difference the shared set exists to prove does not matter.
				Trigger: test.Trigger,
			})
			require.True(t, env.IsWorkflowCompleted())

			if test.ExpectFailure {
				require.Error(t, env.GetWorkflowError(), "the case expected the run to fail")
				return
			}
			require.NoError(t, env.GetWorkflowError())

			var out v1.Workflow_StepOutputs
			require.NoError(t, env.GetWorkflowResult(&out))
			require.Empty(t, cmp.Diff(test.ExpectedOutputs, &out, protocmp.Transform()))
		})
	}
}

// TestRunWorkflowSwitch covers `switch:` against the durable driver, pairing
// TestRunWorkflowValue's local run of the identical [conformance.SwitchCases].
//
// The discriminant evaluates in workflow code, so no activity mock stands
// between this and the branch taken — which is the property under test: which
// case a value takes, what the record says, and that an unresolvable
// discriminant fails rather than defaulting are all decided by the one
// [v1.SelectSwitchCase] both drivers call.
// TestRunWorkflowAsync covers `async:` on the durable driver, where the
// concurrency is real.
//
// The local driver runs the identical [conformance.AsyncCases]. What differs beneath
// them is the whole reason the set is shared: here each async step is a
// coroutine scheduling its own activities, and the joins are channel receives,
// where locally the work has already happened and the join only publishes it.
// A disagreement about where an output becomes visible, or where a failure is
// heard, would show up here and nowhere else.
func TestRunWorkflowAsync(t *testing.T) {
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

			if test.ExpectFailure {
				require.Error(t, env.GetWorkflowError(), "the case expected the run to fail")

				return
			}
			require.NoError(t, env.GetWorkflowError())

			var out v1.Workflow_StepOutputs
			require.NoError(t, env.GetWorkflowResult(&out))
			if test.ExpectedOutputsPredicate != nil {
				require.True(t, test.ExpectedOutputsPredicate(&out), "outputs predicate failed: %v", &out)

				return
			}
			require.Empty(t, cmp.Diff(test.ExpectedOutputs, &out, protocmp.Transform()))
		})
	}
}

func TestRunWorkflowSwitch(t *testing.T) {
	for _, test := range conformance.SwitchCases() {
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

			if test.ExpectFailure {
				require.Error(t, env.GetWorkflowError(), "the case expected the run to fail")
				return
			}
			require.NoError(t, env.GetWorkflowError())

			var out v1.Workflow_StepOutputs
			require.NoError(t, env.GetWorkflowResult(&out))
			if test.ExpectedOutputsPredicate != nil {
				require.True(t, test.ExpectedOutputsPredicate(&out), "outputs predicate failed: %v", &out)
				return
			}
			require.Empty(t, cmp.Diff(test.ExpectedOutputs, &out, protocmp.Transform()))
		})
	}
}

// TestRunWorkflowInputsRefused is the negative direction, against the durable
// driver's own submit boundary.
//
// Nothing is executed, because nothing should be: the point of checking at submit
// is that a run which would be wrong never starts. What this pins is that the
// durable path refuses the same submissions the local one does, in the same words —
// which is only true while both go through one function, and is the first thing to
// stop being true if either grows a check of its own.
func TestRunWorkflowInputsRefused(t *testing.T) {
	for _, test := range conformance.InputRefusalCases() {
		t.Run(test.Name, func(t *testing.T) {
			_, err := v1.BindRunInputs(test.Workflow, test.Inputs)
			require.Error(t, err, "the submission was accepted")
			require.Contains(t, err.Error(), test.Contains)
		})
	}
}

// TestRunWorkflowVarsSecretRefused is the durable driver's half of the same
// negative direction: a specification whose `vars:` hold a secret reference is
// refused at this driver's submit boundary too, in the same words, because both
// reach [v1.BindRunInputs] (#169).
//
// Nothing is executed, because nothing should be — a workflow whose first act would
// be to evaluate a var holding a reference must not have a first act.
func TestRunWorkflowVarsSecretRefused(t *testing.T) {
	for _, test := range conformance.VarsSecretRefusalCases() {
		t.Run(test.Name, func(t *testing.T) {
			_, err := v1.BindRunInputs(test.Workflow, test.Inputs)
			require.Error(t, err, "the submission was accepted")
			require.Contains(t, err.Error(), test.Contains)
		})
	}
}

// TestInputsAndDeclaredOutputsSurviveContinueAsNew is the durable half of this
// feature that no shared case can reach: the local driver never suspends.
//
// Two things have to survive the handover, and they fail differently. The
// arguments have to be carried in `RunState`, or a later segment resolves
// `${inputs.region}` against nothing — which is a run that succeeded for two steps
// and then failed on a value it was started with. And an output the *declared
// outputs* reference has to survive compaction, which walks the remaining steps to
// decide what a resumed segment can still need: the block is evaluated after the
// last step, so a reference in it belongs to no step at all, and a walk that only
// asks the steps prunes exactly the output the run is about to be judged by. Both
// only bite after a suspend, and the run fails at the one moment there is nothing
// left to retry.
func TestInputsAndDeclaredOutputsSurviveContinueAsNew(t *testing.T) {
	t.Parallel()

	spec := &v1.Workflow{
		Name:    "carries-inputs",
		Profile: v1.CurrentProfile,
		DeclaredInputs: []*v1.InputDeclaration{
			{Name: "region", Type: v1.InputDeclaration_TYPE_STRING, Required: true},
		},
		DeclaredOutputs: []*v1.OutputDeclaration{
			// Reaches back to the first step, which is the reference compaction is
			// most likely to prune: nothing after it mentions the step.
			{Name: "waited", Value: v1.NewExpr("!steps.gate.timed_out")},
			{Name: "where", Value: v1.NewExpr("inputs.region")},
		},
		Steps: []*v1.Node{
			{
				Id:   "gate",
				Kind: &v1.Node_Wait{Wait: &v1.Wait{Kind: &v1.Wait_Duration{Duration: durationpb.New(0)}}},
			},
			logStep("one", "1"),
			logStep("two", "2"),
		},
	}

	inputs, err := v1.BindRunInputs(spec, map[string]*v1.Value{"region": v1.NewLiteral("eu-west-1")})
	require.NoError(t, err)

	// A budget of one forces a suspend after the wait, so everything the outputs
	// need has to cross a handover.
	first := newWaitEnv(t)
	first.ExecuteWorkflow(engine.Run, &v1.RunState{Workflow: spec, Inputs: inputs, StepsBudget: 1})
	require.True(t, first.IsWorkflowCompleted())

	suspended := first.GetWorkflowError()
	require.Error(t, suspended, "the run did not suspend, so this test proves nothing")

	var continueAsNew *workflow.ContinueAsNewError
	require.ErrorAs(t, suspended, &continueAsNew)

	var carried v1.RunState
	require.NoError(t,
		converter.GetDefaultDataConverter().FromPayloads(continueAsNew.Input, &carried),
		"could not read the state the suspended run carried")

	require.Equal(t, "eu-west-1", carried.GetInputs()["region"].GetLiteral().GetStringValue(),
		"the run's arguments were not carried across the suspend")
	require.Contains(t, carried.GetOutputs().GetStepValues(), "gate",
		"compaction dropped the output a declared output reads, so the resumed run cannot answer")

	outputs, runs := resumeToCompletion(t, &carried)
	require.Greater(t, runs, 1, "the run did not suspend again, so the carry was only tested once")

	require.True(t, outputs.GetRunOutputs().GetValues()["waited"].GetLiteral().GetBoolValue(),
		"the wait's own output did not survive to the moment the outputs were evaluated")
	require.Equal(t, "eu-west-1",
		outputs.GetRunOutputs().GetValues()["where"].GetLiteral().GetStringValue(),
		"an argument the run was started with did not reach the outputs it is reported in")
}

// TestARunStateWrittenBeforeInputsExistedStillRuns is the cross-version read
// invariant 10 asks for, in the direction that actually happens: an old writer and
// a new reader.
//
// `RunState` is a wire contract between interpreter versions — one writes it at
// Continue-As-New and a different one reads it back — so a run suspended by a
// worker that had never heard of `inputs` or `run_outputs` has to resume here
// without either. Absent must read as "this run has no arguments", which is exactly
// what that run is, rather than as anything needing a compatibility arm.
//
// Written as stored ProtoJSON rather than as a Go value with fields left unset,
// because that is what is actually in a history: the two differ precisely when a
// field's absence and its zero value are not the same thing, which is the case this
// is about.
func TestARunStateWrittenBeforeInputsExistedStillRuns(t *testing.T) {
	t.Parallel()

	stored := []byte(`{"workflow":{"name":"old","profile":"2026.1","steps":[` +
		`{"id":"a","task":{"name":"log","inputs":{"message":{"literal":{"stringValue":"hello"}}}}}` +
		`]},"stepsBudget":100}`)

	payload, err := converter.NewProtoJSONPayloadConverter().ToPayload(&v1.RunState{})
	require.NoError(t, err)
	payload.Data = stored

	var state v1.RunState
	require.NoError(t, converter.NewProtoJSONPayloadConverter().FromPayload(payload, &state),
		"a state written before these fields existed no longer decodes")

	require.Empty(t, state.GetInputs(), "an absent field decoded as something")
	require.Nil(t, state.GetRunOutputs(), "an absent field decoded as something")

	env := newWaitEnv(t)
	env.ExecuteWorkflow(engine.Run, &state)
	require.True(t, env.IsWorkflowCompleted())
	require.NoError(t, env.GetWorkflowError(),
		"a run suspended by a worker that predates inputs cannot be resumed by this one")

	var out v1.Workflow_StepOutputs
	require.NoError(t, env.GetWorkflowResult(&out))
	require.Contains(t, out.GetStepValues(), "a")
	require.Nil(t, out.GetRunOutputs(),
		"a workflow declaring no outputs reported a result rather than nothing")
}

// TestRunWorkflowResponseScope runs the response-scope cases against the durable
// driver.
//
// The names these read reach the task by three different routes, and the durable
// driver takes a longer version of each: the workflow's vars are evaluated in the
// WorkflowVars activity and carried across Continue-As-New, a step's own vars are
// evaluated in workflow code and swapped into the executor's scope, and a loop's
// iterator is copied into a per-iteration scope by hand — and then the whole scope
// crosses the payload converter on its way to TaskInScope, which is where these
// expressions are finally evaluated. Every one of those is somewhere the bindings
// could be dropped again after being restored here.
func TestRunWorkflowResponseScope(t *testing.T) {
	baseURL := conformance.NewHTTPServer(t)
	for _, test := range conformance.ResponseScopeCases(baseURL) {
		t.Run(test.Name, func(t *testing.T) {
			testSuite := &testsuite.WorkflowTestSuite{}
			env := testSuite.NewTestWorkflowEnvironment()
			env.RegisterWorkflow(engine.Run)
			env.OnActivity(engine.Task, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(engine.Task)
			env.OnActivity(engine.TaskInScope, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(engine.TaskInScope)
			env.OnActivity(engine.WorkflowVars, mock.Anything, mock.Anything).Return(engine.WorkflowVars)

			env.ExecuteWorkflow(engine.Run, &v1.RunState{Workflow: test.Workflow, Trigger: test.Trigger})
			require.True(t, env.IsWorkflowCompleted())
			require.NoError(t, env.GetWorkflowError())

			var out v1.Workflow_StepOutputs
			require.NoError(t, env.GetWorkflowResult(&out))
			require.Empty(t, cmp.Diff(test.ExpectedOutputs, &out, protocmp.Transform()))
		})
	}
}

// TestRunWorkflowOutputShaping runs the shared shaping cases against the durable
// driver.
//
// The durable half of the pair, and the half where the encoding matters most: a
// shaped mapping is a structure inside the specification, so it crosses the
// payload converter on its way to the activity that evaluates it, and one entry
// of it reads an earlier step's output — which compaction is free to prune the
// moment nothing appears to reference it.
func TestRunWorkflowOutputShaping(t *testing.T) {
	baseURL := conformance.NewHTTPServer(t)
	for _, test := range conformance.OutputShapingCases(baseURL) {
		t.Run(test.Name, func(t *testing.T) {
			runWorkflow(t, test.Workflow, test.ExpectedOutputs)
		})
	}
}

// TestRunWorkflowZeroValues runs the shared zero-value cases against the durable
// driver, which nothing did.
//
// Every other set in `tests` had two callers — one per driver, which is the whole
// reason the package exists — and this one had one. It was also the worst set to be
// missing, because what it asserts is that a legitimately empty value survives a
// round trip, and the round trip is longer here: an empty string crosses Temporal's
// payload converter twice per step on its way into and out of an activity, and the
// local driver's does not exist. A conversion that dropped a zero value would be
// invisible to the driver that was running these.
func TestRunWorkflowZeroValues(t *testing.T) {
	baseURL := conformance.NewHTTPServer(t)
	for _, test := range conformance.ZeroValueCases(baseURL) {
		t.Run(test.Name, func(t *testing.T) {
			runWorkflow(t, test.Workflow, test.ExpectedOutputs)
		})
	}
}

// TestRunWorkflowTaskOutputElementBound is the durable half of the remaining
// #204 gap — see the local driver's identically-named test. A task's result
// is bounded at [v1.Task.EvalInScope], which the durable driver reaches
// through the `Task` activity exactly as the local driver reaches it through
// `runStepAttempt`, so the same cases have to fail (and succeed) the same way
// here.
func TestRunWorkflowTaskOutputElementBound(t *testing.T) {
	baseURL := conformance.NewHTTPServer(t)
	for _, test := range conformance.TaskOutputElementBoundCases(baseURL) {
		t.Run(test.Name, func(t *testing.T) {
			testSuite := &testsuite.WorkflowTestSuite{}
			// At the bound on purpose, so the ten thousand elements are
			// real work the workflow goroutine does in one task (#431).
			env := atABound(testSuite.NewTestWorkflowEnvironment())
			env.RegisterWorkflow(engine.Run)
			env.OnActivity(engine.Task, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(engine.Task)
			env.OnActivity(engine.TaskInScope, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(engine.TaskInScope)
			env.OnActivity(engine.WorkflowVars, mock.Anything, mock.Anything).Return(engine.WorkflowVars)

			env.ExecuteWorkflow(engine.Run, &v1.RunState{Workflow: test.Workflow})
			require.True(t, env.IsWorkflowCompleted())

			err := env.GetWorkflowError()
			if test.ExpectFailure {
				require.Error(t, err, "a task result past the element bound must be refused")
				require.Contains(t, err.Error(), "10000",
					"the refusal must name the bound it reached")
				return
			}
			require.NoError(t, err)

			var out v1.Workflow_StepOutputs
			require.NoError(t, env.GetWorkflowResult(&out))
			require.True(t, test.ExpectedOutputsPredicate(&out), "unexpected outputs: %v", &out)
		})
	}
}

// TestRunWorkflowForEachResultsBound is the durable half of #229's byte bound
// for the `for_each` construct — see the local driver's identically-named test.
//
// A `for_each`'s accumulated `results` are bounded in bytes exactly as a
// `loop:`'s are, through the shared [v1.MaxLoopResultsBytes]. What this half adds
// over the local one is the concurrent path: with `max_parallel:` set, the
// durable driver runs iterations with bounded fan-out that land out of order, so
// the bound is checked at the join over the completed iterations in input order —
// a genuinely different code path from the sequential accumulation, which must
// nonetheless reach the identical verdict. Both the sequential and the concurrent
// over-bound cases must fail here, and the just-under case must succeed.
func TestRunWorkflowForEachResultsBound(t *testing.T) {
	baseURL := conformance.NewHTTPServer(t)
	for _, test := range conformance.ForEachResultsBoundCases(baseURL) {
		t.Run(test.Name, func(t *testing.T) {
			testSuite := &testsuite.WorkflowTestSuite{}
			// Accumulating results right up against the byte bound is the
			// point of these cases, so the same raise applies (#431).
			env := atABound(testSuite.NewTestWorkflowEnvironment())
			env.RegisterWorkflow(engine.Run)
			env.OnActivity(engine.Task, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(engine.Task)
			env.OnActivity(engine.TaskInScope, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(engine.TaskInScope)
			env.OnActivity(engine.WorkflowVars, mock.Anything, mock.Anything).Return(engine.WorkflowVars)

			env.ExecuteWorkflow(engine.Run, &v1.RunState{Workflow: test.Workflow})
			require.True(t, env.IsWorkflowCompleted())

			err := env.GetWorkflowError()
			if test.ExpectFailure {
				require.Error(t, err, "a for_each past the results byte bound must be refused")
				require.Contains(t, err.Error(), "byte limit",
					"the refusal must name the bound it reached")
				return
			}
			require.NoError(t, err)

			var out v1.Workflow_StepOutputs
			require.NoError(t, env.GetWorkflowResult(&out))
			require.True(t, test.ExpectedOutputsPredicate(&out), "unexpected outputs: %v", &out)
		})
	}
}

// TestRunWorkflowForEachTripCount is the durable half of the `for_each`
// trip-count ceiling, see the local driver's identically-named test.
//
// The durable driver reaches [v1.CheckForEachItems] at the same point the local
// one does, immediately after [v1.ResolveItems] and before an iteration is
// scheduled, so a list past the ceiling costs no activity here either: the run
// fails outright rather than fanning out and stopping part way. The at-ceiling
// case asserts the full trip count was run, which is what makes this a claim the
// bound is reached rather than only that it is not exceeded.
func TestRunWorkflowForEachTripCount(t *testing.T) {
	for _, test := range conformance.ForEachTripCountCases() {
		t.Run(test.Name, func(t *testing.T) {
			// The at-ceiling case runs a thousand iterations on purpose, which
			// is exactly the work the trip-count design sized to stay under the
			// detector, and exactly what contention takes back (#431).
			env := atABound(budgetEnv(t))

			env.ExecuteWorkflow(engine.Run, &v1.RunState{Workflow: test.Workflow})
			require.True(t, env.IsWorkflowCompleted())

			err := env.GetWorkflowError()
			if test.ExpectFailure {
				require.Error(t, err, "a for_each past the trip-count ceiling must be refused")
				// The same three things the local driver's half asserts, in the
				// same sentence: a message an author's tooling matches on must
				// not depend on where the workload ran.
				require.Contains(t, err.Error(), `step "fan"`,
					"the refusal must name the step")
				require.Contains(t, err.Error(), strconv.Itoa(v1.MaxForEachItems+1),
					"the refusal must name the count observed")
				require.Contains(t, err.Error(), strconv.Itoa(v1.MaxForEachItems),
					"the refusal must name the ceiling it reached")
				return
			}
			require.NoError(t, err)

			var out v1.Workflow_StepOutputs
			require.NoError(t, env.GetWorkflowResult(&out))
			require.True(t, test.ExpectedOutputsPredicate(&out), "unexpected outputs: %v", &out)
		})
	}
}

// TestRunWorkflowAtomicBlockBound is the durable half of the
// suspension-opaque fan-out ceiling, [v1.MaxAtomicBlockActivities]; see the
// local driver's identically-named test.
//
// The durable driver is the one with history to protect: a `for_each` with
// `max_parallel:` above one, or one inside a `parallel:` branch, runs with no
// Continue-As-New seam, so its items × body product accumulates in a single
// execution against Temporal's 51,200-event termination limit — and a
// termination skips the compensation log. [v1.CheckAtomicBlockActivities]
// refuses the product before any iteration is dispatched, at the same point
// [v1.CheckForEachItems] already runs, so a refusal costs no activity. The
// at-ceiling cases assert the full trip count ran, which is what makes this a
// claim the bound is reached rather than only not exceeded.
func TestRunWorkflowAtomicBlockBound(t *testing.T) {
	for _, test := range conformance.ForEachAtomicBlockCases() {
		t.Run(test.Name, func(t *testing.T) {
			// The at-ceiling cases evaluate ten thousand `if:` guards inside
			// single workflow tasks on purpose — that product is the point —
			// which is exactly the stretch the boundary deadlock budget
			// exists for (#431).
			env := atABound(budgetEnv(t))

			env.ExecuteWorkflow(engine.Run, &v1.RunState{Workflow: test.Workflow})
			require.True(t, env.IsWorkflowCompleted())

			err := env.GetWorkflowError()
			if test.ExpectFailure {
				require.Error(t, err, "a for_each past the atomic-activity ceiling must be refused")
				// The same pieces the local driver's half asserts, in the same
				// sentence: the step, the item count, the per-iteration count
				// and the ceiling must not depend on where the workload ran.
				require.Contains(t, err.Error(), `step "fan"`,
					"the refusal must name the step")
				for _, want := range conformance.AtomicBlockRefusalSubstrings() {
					require.Contains(t, err.Error(), want,
						"the refusal must name the counts and the ceiling")
				}
				return
			}
			require.NoError(t, err)

			var out v1.Workflow_StepOutputs
			require.NoError(t, env.GetWorkflowResult(&out))
			require.True(t, test.ExpectedOutputsPredicate(&out), "unexpected outputs: %v", &out)
		})
	}
}

// TestAConcurrentForEachCountsItsBodyStepsAgainstTheBudget pins the
// `processed` copy-back at the concurrent join: a `max_parallel > 1` loop
// used to advance the step budget by one however many body steps its workers
// ran, so the between-siblings seam after a concurrent loop fired only on the
// history hint.
//
// The workflow runs a concurrent for_each of three iterations, two real
// activities each, then one more top-level step, under a budget of five. With
// the copy-back the loop contributes its six body steps plus itself — over
// budget, so the seam after the loop suspends and the run continues as new
// with a position past the loop. Without it the loop counts as one step and
// the run completes in a single segment, which is exactly how this test fails
// when the copy-back is reverted.
func TestAConcurrentForEachCountsItsBodyStepsAgainstTheBudget(t *testing.T) {
	baseURL := conformance.NewHTTPServer(t)
	env := budgetEnv(t)

	get := func(id string) *v1.Node {
		return &v1.Node{
			Id: id,
			Kind: &v1.Node_Task{Task: &v1.Task{
				Name: "http",
				Inputs: map[string]*v1.Value{
					"method": v1.NewLiteral("GET"),
					"url":    v1.NewLiteral(baseURL + "/status/200"),
				},
			}},
		}
	}

	wf := &v1.Workflow{
		Name: "concurrent-budget",
		Steps: []*v1.Node{
			{
				Id: "fan",
				Kind: &v1.Node_ForEach{ForEach: &v1.ForEach{
					Items:       v1.NewExpr("[0, 1, 2]"),
					MaxParallel: 2,
					Body:        []*v1.Node{get("first"), get("second")},
				}},
			},
			get("after"),
		},
	}

	env.ExecuteWorkflow(engine.Run, &v1.RunState{Workflow: wf, StepsBudget: 5})
	require.True(t, env.IsWorkflowCompleted())

	err := env.GetWorkflowError()
	require.Error(t, err,
		"a concurrent loop that ran six body steps under a budget of five finished in one segment, "+
			"so the join is not copying worker step counts back into the budget")

	var continued *workflow.ContinueAsNewError
	require.ErrorAs(t, err, &continued,
		"the run failed instead of continuing as new: %v", err)

	// The seam that fired is the one after the loop, so the next segment
	// resumes past it rather than re-running the fan-out.
	var next v1.RunState
	require.NoError(t, converter.GetDefaultDataConverter().
		FromPayload(continued.Input.GetPayloads()[0], &next))
	require.NotEmpty(t, resumedPosition(&next),
		"the next segment resumes from the beginning, so the loop would run twice")
}

// TestAConcurrentForEachCountsFailedIterationsAgainstTheBudget is the failing
// half of the copy-back claim above: an attempted step is history whether or
// not it succeeded, and whether or not its failure is tolerated *above* it.
// runNodes used to return before its `processed++` on a propagating failure,
// so a concurrent iteration whose first body step failed copied a count of
// zero back to the join — and a loop marked `continue_on_error:` whose every
// iteration failed advanced the budget by one however many activities it had
// scheduled, which is the history protection bypassed by failing.
//
// Three iterations of one permanently-failing task under a budget of three,
// tolerated at the loop step: the attempted steps plus the loop itself are
// four, so the seam after the loop suspends. Reverting either the counting of
// a failed attempt in runNodes or the join's copy-back completes the run in
// one segment and turns this red.
func TestAConcurrentForEachCountsFailedIterationsAgainstTheBudget(t *testing.T) {
	baseURL := conformance.NewHTTPServer(t)
	env := budgetEnv(t)

	wf := &v1.Workflow{
		Name: "concurrent-budget-failing",
		Steps: []*v1.Node{
			{
				Id:     "fan",
				Policy: &v1.StepPolicy{ContinueOnError: true},
				Kind: &v1.Node_ForEach{ForEach: &v1.ForEach{
					Items:       v1.NewExpr("[0, 1, 2]"),
					MaxParallel: 2,
					Body: []*v1.Node{
						{
							Id: "fails",
							Kind: &v1.Node_Task{Task: &v1.Task{
								Name: "http",
								Inputs: map[string]*v1.Value{
									"method": v1.NewLiteral("GET"),
									"url":    v1.NewLiteral(baseURL + "/status/404"),
								},
							}},
						},
					},
				}},
			},
			{
				Id: "after",
				Kind: &v1.Node_Task{Task: &v1.Task{
					Name: "http",
					Inputs: map[string]*v1.Value{
						"method": v1.NewLiteral("GET"),
						"url":    v1.NewLiteral(baseURL + "/status/200"),
					},
				}},
			},
		},
	}

	env.ExecuteWorkflow(engine.Run, &v1.RunState{Workflow: wf, StepsBudget: 3})
	require.True(t, env.IsWorkflowCompleted())

	err := env.GetWorkflowError()
	require.Error(t, err,
		"a tolerated all-failing concurrent loop under a budget of three finished in one segment, "+
			"so failed attempts are not being counted against the budget")

	var continued *workflow.ContinueAsNewError
	require.ErrorAs(t, err, &continued,
		"the run failed instead of continuing as new: %v", err)
}

// TestAStepsVarsSurviveContinueAsNew is the same claim as
// [TestABudgetSmallerThanTheWorkflowContinuesAsNew], written the way the language
// actually encourages — and the way that used to lose the value.
//
// `collectNodeRefs` walked a task's inputs and not a step's `vars:`, so the two
// shapes below, which mean the same thing, behaved differently across a handover:
// the reference in an input carried its output forward and the reference in `vars:`
// did not. The second segment then resumed with nothing for `carried` to read out
// of and failed permanently — on a step that had already succeeded.
//
// `examples/http-json` is exactly this shape and teaches it: "a step's own `vars:`
// gives that value a name so the parse is written once rather than at every use."
// Nothing running the examples could see it, because only the durable driver
// continues as new.
func TestAStepsVarsSurviveContinueAsNew(t *testing.T) {
	baseURL := conformance.NewHTTPServer(t)
	env := budgetEnv(t)

	// `b` reads `a.said` through its own `vars:` rather than through a task input,
	// and its input names only the var — so `a`'s output is reachable from nowhere
	// else and carrying it is the only way this can resume.
	reader := &v1.Node{
		Id:   "b",
		Vars: map[string]*v1.Value{"carried": v1.NewExpr("a.said")},
		Kind: &v1.Node_Task{Task: &v1.Task{
			Name: "http",
			Inputs: map[string]*v1.Value{
				"method":  v1.NewLiteral(http.MethodPost),
				"url":     v1.NewLiteral(baseURL + "/echo"),
				"body":    v1.NewExpr("carried"),
				"outputs": v1.NewExpr(`{"said": response.body}`),
			},
		}},
	}

	wf := &v1.Workflow{Name: "vars-across-handover", Steps: []*v1.Node{chained(baseURL)[0], reader}}

	env.ExecuteWorkflow(engine.Run, &v1.RunState{Workflow: wf, StepsBudget: 1})
	require.True(t, env.IsWorkflowCompleted())

	var continued *workflow.ContinueAsNewError
	require.ErrorAs(t, env.GetWorkflowError(), &continued,
		"a run with one step of budget and two steps did not continue as new")

	require.Len(t, continued.Input.GetPayloads(), 1, "the next segment was passed no state")

	var next v1.RunState
	require.NoError(t, converter.GetDefaultDataConverter().
		FromPayload(continued.Input.GetPayloads()[0], &next))

	require.Contains(t, next.GetOutputs().GetStepValues(), "a",
		"the output a later step's `vars:` reads was not carried into the next segment, so "+
			"the resumed run fails on a reference to a step that has already succeeded")
}

// TestRunWorkflowErrorText runs the shared error-text cases against the durable
// driver, which is the side that was getting them wrong.
//
// What a tolerated failure records used to be whatever Temporal handed back: an
// activity envelope carrying scheduled event ids, a worker identity, and the
// classification restated at every level of the cause chain. The event ids vary
// per run, so the value an author's `if:` compares was not only different from
// the local driver's but unstable between runs of this one.
func TestRunWorkflowErrorText(t *testing.T) {
	baseURL := conformance.NewHTTPServer(t)
	for _, test := range conformance.ErrorTextCases(baseURL) {
		t.Run(test.Name, func(t *testing.T) {
			testSuite := &testsuite.WorkflowTestSuite{}
			env := testSuite.NewTestWorkflowEnvironment()
			env.RegisterWorkflow(engine.Run)
			env.OnActivity(engine.Task, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(engine.Task)
			env.OnActivity(engine.TaskInScope, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(engine.TaskInScope)
			env.OnActivity(engine.WorkflowVars, mock.Anything, mock.Anything).Return(engine.WorkflowVars)

			env.ExecuteWorkflow(engine.Run, &v1.RunState{Workflow: test.Workflow})
			require.True(t, env.IsWorkflowCompleted())
			require.NoError(t, env.GetWorkflowError())

			var out v1.Workflow_StepOutputs
			require.NoError(t, env.GetWorkflowResult(&out))
			require.Empty(t, cmp.Diff(test.ExpectedOutputs, &out, protocmp.Transform()))
		})
	}
}

// TestToleratedFailureTextCarriesNoTransportWrapping is the negative direction.
//
// The case above pins the sentence that should be recorded; this pins that none
// of the envelope's vocabulary survives into it. Worth stating separately because
// the two fail differently: a renderer change breaks the case above and reads as
// a deliberate edit, while a regression that reintroduces the wrapping would make
// the recorded value grow a per-run event id — which the exact-match case would
// also catch, but this one names.
func TestToleratedFailureTextCarriesNoTransportWrapping(t *testing.T) {
	baseURL := conformance.NewHTTPServer(t)
	cases := conformance.ErrorTextCases(baseURL)
	require.NotEmpty(t, cases, "no error-text cases, so this asserts nothing")

	for _, test := range cases {
		t.Run(test.Name, func(t *testing.T) {
			testSuite := &testsuite.WorkflowTestSuite{}
			env := testSuite.NewTestWorkflowEnvironment()
			env.RegisterWorkflow(engine.Run)
			env.OnActivity(engine.Task, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(engine.Task)
			env.OnActivity(engine.TaskInScope, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(engine.TaskInScope)
			env.OnActivity(engine.WorkflowVars, mock.Anything, mock.Anything).Return(engine.WorkflowVars)

			env.ExecuteWorkflow(engine.Run, &v1.RunState{Workflow: test.Workflow})
			require.True(t, env.IsWorkflowCompleted())
			require.NoError(t, env.GetWorkflowError())

			var out v1.Workflow_StepOutputs
			require.NoError(t, env.GetWorkflowResult(&out))

			recorded := out.GetStepValues()["flaky"].GetNamedValues()[v1.StepErrorOutput].GetLiteral().GetStringValue()
			require.NotEmpty(t, recorded, "the tolerated step recorded nothing")

			for _, leaked := range []string{
				"activity error", "scheduledEventID", "startedEventID", "identity:",
				"retryable:", "engine: flowstate run failed", "Attempt", "attempt",
			} {
				require.NotContains(t, recorded, leaked,
					"the recorded value carries %q from the transport that delivered it, "+
						"which the local driver has no equivalent of", leaked)
			}
		})
	}
}

// TestRunWorkflowCall is the durable half of `call:` — see the local driver's
// TestRunWorkflowCall. Isolation, argument scope, outputs under the step id, the
// depth bound and a tolerated callee failure all have to hold here exactly as
// they do locally, since every one of them is a rule call.go states once for
// both drivers to reach.
func TestRunWorkflowCall(t *testing.T) {
	for _, test := range conformance.CallCases() {
		t.Run(test.Name, func(t *testing.T) {
			testSuite := &testsuite.WorkflowTestSuite{}
			env := testSuite.NewTestWorkflowEnvironment()
			env.RegisterWorkflow(engine.Run)
			env.OnActivity(engine.Task, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(engine.Task)
			env.OnActivity(engine.TaskInScope, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(engine.TaskInScope)
			env.OnActivity(engine.WorkflowVars, mock.Anything, mock.Anything).Return(engine.WorkflowVars)

			env.ExecuteWorkflow(engine.Run, &v1.RunState{Workflow: test.Workflow})
			require.True(t, env.IsWorkflowCompleted())

			err := env.GetWorkflowError()
			if test.ExpectFailure {
				require.Error(t, err, "the call was expected to be refused")
				return
			}
			require.NoError(t, err)

			var out v1.Workflow_StepOutputs
			require.NoError(t, env.GetWorkflowResult(&out))
			if test.ExpectedOutputsPredicate != nil {
				require.True(t, test.ExpectedOutputsPredicate(&out), "unexpected outputs: %v", &out)
				return
			}
			require.Empty(t, cmp.Diff(test.ExpectedOutputs, &out, protocmp.Transform()))
		})
	}
}

// TestRunWorkflowLoop is the durable half of the `loop:` primitive — see the local
// driver's TestRunWorkflowLoop. The iteration count, the carried state a loop reports
// as its `state` output, and the failure at the ceiling all have to hold here exactly
// as they do locally, since each is a rule loop.go states once for both drivers: the
// ceiling through [v1.LoopMaxIterations], the exhaustion through
// [v1.LoopIterationLimitError], the outputs through [v1.LoopStateOutputs].
func TestRunWorkflowLoop(t *testing.T) {
	for _, test := range conformance.LoopCases() {
		t.Run(test.Name, func(t *testing.T) {
			testSuite := &testsuite.WorkflowTestSuite{}
			env := testSuite.NewTestWorkflowEnvironment()
			env.RegisterWorkflow(engine.Run)
			env.OnActivity(engine.Task, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(engine.Task)
			env.OnActivity(engine.TaskInScope, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(engine.TaskInScope)
			env.OnActivity(engine.WorkflowVars, mock.Anything, mock.Anything).Return(engine.WorkflowVars)

			env.ExecuteWorkflow(engine.Run, &v1.RunState{Workflow: test.Workflow})
			require.True(t, env.IsWorkflowCompleted())

			err := env.GetWorkflowError()
			if test.ExpectFailure {
				require.Error(t, err, "the loop was expected to fail")
				want := test.ExpectedErrorContains
				if want == "" {
					want = "ran its full budget"
				}
				require.Contains(t, err.Error(), want)
				return
			}
			require.NoError(t, err)

			var out v1.Workflow_StepOutputs
			require.NoError(t, env.GetWorkflowResult(&out))
			if test.ExpectedOutputsPredicate != nil {
				require.True(t, test.ExpectedOutputsPredicate(&out), "unexpected outputs: %v", &out)
				return
			}
			require.Empty(t, cmp.Diff(test.ExpectedOutputs, &out, protocmp.Transform()))
		})
	}
}

// TestRunWorkflowToleratedIterationIdentity is the durable half of a tolerated
// iteration failure carrying its `as:` binding (#157's question 3).
//
// The local driver runs the identical [conformance.ToleratedIterationIdentityCases].
// The concurrent case is the one only this driver can actually exercise as
// written — `max_parallel` schedules iterations onto coroutines here where the
// local driver runs them in order regardless — so this caller is what holds
// the concurrent path's failure entries to the sequential answer.
func TestRunWorkflowToleratedIterationIdentity(t *testing.T) {
	baseURL := conformance.NewHTTPServer(t)
	for _, test := range conformance.ToleratedIterationIdentityCases(baseURL) {
		t.Run(test.Name, func(t *testing.T) {
			testSuite := &testsuite.WorkflowTestSuite{}
			env := testSuite.NewTestWorkflowEnvironment()
			env.RegisterWorkflow(engine.Run)
			env.OnActivity(engine.Task, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(engine.Task)
			env.OnActivity(engine.TaskInScope, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(engine.TaskInScope)
			env.OnActivity(engine.WorkflowVars, mock.Anything, mock.Anything).Return(engine.WorkflowVars)

			env.ExecuteWorkflow(engine.Run, &v1.RunState{Workflow: test.Workflow})
			require.True(t, env.IsWorkflowCompleted())
			require.NoError(t, env.GetWorkflowError(),
				"every failure in these cases is tolerated, so the run must complete")

			var out v1.Workflow_StepOutputs
			require.NoError(t, env.GetWorkflowResult(&out))
			require.Empty(t, cmp.Diff(test.ExpectedOutputs, &out, protocmp.Transform()))
		})
	}
}

// TestRunWorkflowToleratedStepFailure is the durable half of the outermost-step
// cases.
//
// This is the side that named the step inside its own recorded value: the
// position was attached where the failure was raised, so a step that tolerated
// its own `items:` or `wait_until:` failure recorded `step "gate": …` under the
// key `gate` while the local driver recorded the sentence alone. The position is
// added on the way out of a step now, which is the only path where it tells a
// reader something the surrounding structure does not.
func TestRunWorkflowToleratedStepFailure(t *testing.T) {
	for _, test := range conformance.ToleratedStepFailureCases() {
		t.Run(test.Name, func(t *testing.T) {
			testSuite := &testsuite.WorkflowTestSuite{}
			env := testSuite.NewTestWorkflowEnvironment()
			env.RegisterWorkflow(engine.Run)
			env.OnActivity(engine.Task, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(engine.Task)
			env.OnActivity(engine.TaskInScope, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(engine.TaskInScope)
			env.OnActivity(engine.WorkflowVars, mock.Anything, mock.Anything).Return(engine.WorkflowVars)

			env.ExecuteWorkflow(engine.Run, &v1.RunState{Workflow: test.Workflow})
			require.True(t, env.IsWorkflowCompleted())
			require.NoError(t, env.GetWorkflowError(),
				"a step allowed to fail stopped the run")

			var out v1.Workflow_StepOutputs
			require.NoError(t, env.GetWorkflowResult(&out))
			require.Empty(t, cmp.Diff(test.ExpectedOutputs, &out, protocmp.Transform()))
		})
	}
}

// TestRunWorkflowNestedErrorText is the durable half of the nested case.
//
// The task case converges because errors.As reaches a TaskError through every
// wrapper. This one has no TaskError to find, so the structural position is part
// of what the failure says — and the durable driver used to read the innermost
// recorded text out of the envelope and drop every wrapper on the way to it.
func TestRunWorkflowNestedErrorText(t *testing.T) {
	for _, test := range conformance.NestedErrorTextCases() {
		t.Run(test.Name, func(t *testing.T) {
			testSuite := &testsuite.WorkflowTestSuite{}
			env := testSuite.NewTestWorkflowEnvironment()
			env.RegisterWorkflow(engine.Run)
			env.OnActivity(engine.Task, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(engine.Task)
			env.OnActivity(engine.TaskInScope, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(engine.TaskInScope)
			env.OnActivity(engine.WorkflowVars, mock.Anything, mock.Anything).Return(engine.WorkflowVars)

			env.ExecuteWorkflow(engine.Run, &v1.RunState{Workflow: test.Workflow})
			require.True(t, env.IsWorkflowCompleted())
			require.NoError(t, env.GetWorkflowError())

			var out v1.Workflow_StepOutputs
			require.NoError(t, env.GetWorkflowResult(&out))
			require.Empty(t, cmp.Diff(test.ExpectedOutputs, &out, protocmp.Transform()))
		})
	}
}

// undoPlaceholderBase is a base URL used only to enumerate the shared saga cases.
//
// `.invalid` is reserved by RFC 2606 and resolves nowhere, so a case list built
// with it and then accidentally *run* fails rather than reaching something real.
const undoPlaceholderBase = "http://undo.invalid"

// TestRunWorkflowUndo is the durable half of the saga cases.
//
// The local driver runs the identical [conformance.UndoCases]. Compensation is where the
// two have the most reason to be written separately — here an undo is an activity
// scheduled by a workflow already on its way to failing — so what an author can see
// about it is exactly what has to be pinned in both places: which steps get undone,
// in what order, what a failing compensation does to the rest, and the sentence the
// failed run reports.
//
// A recording server per case, because what is asserted is a sequence of requests
// and a shared one would make each case depend on which ran before it.
func TestRunWorkflowUndo(t *testing.T) {
	for index, outline := range conformance.UndoCases(undoPlaceholderBase) {
		t.Run(outline.Name, func(t *testing.T) {
			base, recorded := conformance.NewUndoServer(t)
			test := conformance.UndoCases(base)[index]

			testSuite := &testsuite.WorkflowTestSuite{}
			env := testSuite.NewTestWorkflowEnvironment()
			env.RegisterWorkflow(engine.Run)
			env.OnActivity(engine.Task, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(engine.Task)
			env.OnActivity(engine.TaskInScope, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(engine.TaskInScope)
			env.OnActivity(engine.WorkflowVars, mock.Anything, mock.Anything).Return(engine.WorkflowVars)

			env.ExecuteWorkflow(engine.Run, &v1.RunState{Workflow: test.Workflow})
			require.True(t, env.IsWorkflowCompleted())

			err := env.GetWorkflowError()
			if !test.Fails {
				require.NoError(t, err, "the run was expected to succeed")
			} else {
				require.Error(t, err, "the run was expected to fail")
				require.Contains(t, err.Error(), test.Summary,
					"the failure does not carry the account of what was compensated")
			}

			conformance.AssertRecorded(t, test, recorded())
		})
	}
}

// TestRunWorkflowUndoCall is the durable half of the compose-through cases —
// issue #219's decision that a callee's compensations register onto the same
// run-level undo stack a top-level step's would, and undo in reverse across the
// `call:` boundary. The local driver runs the identical [conformance.UndoCallCases].
//
// This is exactly the shape [TestRunWorkflowUndo] exists for, pointed at the one
// case that shape could not previously express: the durable executor shares
// `e.undo` by pointer with the nested executor a call descends into (see
// [executor.runCall]), so what makes this pass is that sharing already being
// correct — the only thing that changed to make this legal was the placement
// check at [v1.CheckUndoPlacement], not how registration reaches the log.
func TestRunWorkflowUndoCall(t *testing.T) {
	for index, outline := range conformance.UndoCallCases(undoPlaceholderBase) {
		t.Run(outline.Name, func(t *testing.T) {
			base, recorded := conformance.NewUndoServer(t)
			test := conformance.UndoCallCases(base)[index]

			testSuite := &testsuite.WorkflowTestSuite{}
			env := testSuite.NewTestWorkflowEnvironment()
			env.RegisterWorkflow(engine.Run)
			env.OnActivity(engine.Task, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(engine.Task)
			env.OnActivity(engine.TaskInScope, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(engine.TaskInScope)
			env.OnActivity(engine.WorkflowVars, mock.Anything, mock.Anything).Return(engine.WorkflowVars)

			env.ExecuteWorkflow(engine.Run, &v1.RunState{Workflow: test.Workflow})
			require.True(t, env.IsWorkflowCompleted())

			err := env.GetWorkflowError()
			if !test.Fails {
				require.NoError(t, err, "the run was expected to succeed")
			} else {
				require.Error(t, err, "the run was expected to fail")
				require.Contains(t, err.Error(), test.Summary,
					"the failure does not carry the account of what was compensated across the call boundary")
			}

			require.Equal(t, test.Recorded, recorded(),
				"the effects that happened, and their order, are not what compensating across a call should have produced")
		})
	}
}

// TestRunWorkflowUndoLoop is the durable half of the loop cases — issue #253's
// decision. The local driver runs the identical [conformance.UndoLoopCases].
//
// The durable executor already shared `e.undo` by pointer with the executor a loop
// iteration descends into, exactly as it does for a call, so what changed to make
// these pass is the placement check ([v1.CheckUndoPlacement]) and the composition
// [v1.UndoScope.IntoLoop] performs — not how a registration reaches the log.
func TestRunWorkflowUndoLoop(t *testing.T) {
	for index, outline := range conformance.UndoLoopCases(undoPlaceholderBase) {
		t.Run(outline.Name, func(t *testing.T) {
			base, recorded := conformance.NewUndoServer(t)
			test := conformance.UndoLoopCases(base)[index]

			testSuite := &testsuite.WorkflowTestSuite{}
			env := testSuite.NewTestWorkflowEnvironment()
			env.RegisterWorkflow(engine.Run)
			env.OnActivity(engine.Task, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(engine.Task)
			env.OnActivity(engine.TaskInScope, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(engine.TaskInScope)
			env.OnActivity(engine.WorkflowVars, mock.Anything, mock.Anything).Return(engine.WorkflowVars)

			env.ExecuteWorkflow(engine.Run, &v1.RunState{Workflow: test.Workflow})
			require.True(t, env.IsWorkflowCompleted())

			err := env.GetWorkflowError()
			if !test.Fails {
				require.NoError(t, err, "the run was expected to succeed")
			} else {
				require.Error(t, err, "the run was expected to fail")
				require.Contains(t, err.Error(), test.Summary,
					"the failure does not name each iteration's compensation in reverse order")
			}

			require.Equal(t, test.Recorded, recorded(),
				"the effects that happened, and their order, are not what unwinding a loop should have produced")
		})
	}
}

// TestRunWorkflowUndoOnCancellation is the durable half of the cancellation cases.
//
// The local driver runs the identical [conformance.UndoCancellationCases]. This is the
// path where the two drivers have the least in common: what a compensation has to
// escape here is a workflow context Temporal has cancelled, and every activity
// scheduled on one is refused before it reaches a worker. So the failure mode this
// guards against is not "the wrong things were undone" — it is a run that reports
// having tried to undo everything and in fact attempted none of it, which the
// `Recorded` assertion catches and a summary assertion alone would not.
//
// The cancellation is delivered by a delayed callback at a minute, which is the
// same instrument `cancel_test.go` uses. Time in the test environment is virtual,
// so the steps run at zero and the run is parked on its hour-long wait long before
// it arrives — deterministic rather than raced.
func TestRunWorkflowUndoOnCancellation(t *testing.T) {
	for index, outline := range conformance.UndoCancellationCases(undoPlaceholderBase) {
		t.Run(outline.Name, func(t *testing.T) {
			base, recorded := conformance.NewUndoServer(t)
			test := conformance.UndoCancellationCases(base)[index]

			testSuite := &testsuite.WorkflowTestSuite{}
			env := testSuite.NewTestWorkflowEnvironment()
			env.RegisterWorkflow(engine.Run)
			env.OnActivity(engine.Task, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(engine.Task)
			env.OnActivity(engine.TaskInScope, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(engine.TaskInScope)
			env.OnActivity(engine.WorkflowVars, mock.Anything, mock.Anything).Return(engine.WorkflowVars)

			// `flow cancel`, arriving while the run is parked.
			env.RegisterDelayedCallback(env.CancelWorkflow, time.Minute)

			env.ExecuteWorkflow(engine.Run, &v1.RunState{Workflow: test.Workflow})
			require.True(t, env.IsWorkflowCompleted(),
				"a cancelled run never stopped, so its compensations never finished")

			err := env.GetWorkflowError()
			require.Error(t, err, "a cancelled run reported success")

			// The distinction the feature rests on. Compensating changes the state of
			// the world, not what the run was: a workload somebody stopped on purpose
			// that starts reporting FAILED sends whoever finds it looking for a fault
			// that never happened. Temporal decides this from the error's type, which
			// is why `compensate` returns a fresh cancellation rather than wrapping.
			var canceled *temporal.CanceledError
			require.ErrorAs(t, err, &canceled,
				"a stopped run stopped reading as cancelled once it compensated: %v", err)

			// The summary rides the cancellation's details, because a cancelled
			// workflow closes with a command whose only payload is that. An operator
			// asking what was cleaned up gets it from there.
			var summary string
			if canceled.HasDetails() {
				require.NoError(t, canceled.Details(&summary))
			}
			require.Equal(t, test.Summary, summary,
				"the cancelled run does not carry the account of what was compensated")

			conformance.AssertCancellationRecorded(t, test, recorded())
		})
	}
}

// TestRunWorkflowUndoPlacement pins the shapes the durable engine refuses.
//
// The local driver refuses the same ones through the same call, which is what
// makes this a driver-agreement case rather than a test of one engine: a shape
// accepted here and refused there — or the reverse — would be a rehearsal that
// disagrees with production about whether a saga is expressible at all.
func TestRunWorkflowUndoPlacement(t *testing.T) {
	for _, test := range conformance.UndoPlacementCases(undoPlaceholderBase) {
		t.Run(test.Name, func(t *testing.T) {
			testSuite := &testsuite.WorkflowTestSuite{}
			env := testSuite.NewTestWorkflowEnvironment()
			env.RegisterWorkflow(engine.Run)
			env.OnActivity(engine.Task, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(engine.Task)
			env.OnActivity(engine.TaskInScope, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(engine.TaskInScope)
			env.OnActivity(engine.WorkflowVars, mock.Anything, mock.Anything).Return(engine.WorkflowVars)

			env.ExecuteWorkflow(engine.Run, &v1.RunState{Workflow: test.Workflow})
			require.True(t, env.IsWorkflowCompleted())

			err := env.GetWorkflowError()
			require.Error(t, err, "a compensation the engine cannot honour was accepted")
			require.Contains(t, err.Error(), "`undo:` is only supported on",
				"the refusal does not say what is wrong with where the compensation is written")
		})
	}
}

// TestPendingCompensationsSurviveContinueAsNew is the case only the durable driver
// can ask, and the one a saga most depends on.
//
// A provisioning workload is exactly the shape that suspends: it does some work,
// crosses the step budget, and fails later — in a segment that replays none of the
// history where the work happened. If the registered compensations did not ride
// `RunState`, the failing segment would have nothing to take back and would report a
// clean failure over a half-built world.
//
// Driven through the executor's own budget seam rather than by hand, with a budget
// of one so the handover happens between the two provisioning steps.
func TestPendingCompensationsSurviveContinueAsNew(t *testing.T) {
	base, recorded := conformance.NewUndoServer(t)
	cases := conformance.UndoCases(base)
	test := cases[0]
	require.Equal(t, "compensations run in reverse order when a later step fails", test.Name,
		"this test is written against the first shared case; the list was reordered")

	// The test environment does not continue a workflow as new for real: it reports
	// the ContinueAsNew error, and the next segment is started from the state it
	// carried. Feeding that state back in is what makes this a test of the carry
	// rather than of one segment.
	state := &v1.RunState{Workflow: test.Workflow, StepsBudget: 1}

	var (
		err       error
		segments  int
		lastError error
	)
	for segments = 0; segments < 10; segments++ {
		testSuite := &testsuite.WorkflowTestSuite{}
		env := testSuite.NewTestWorkflowEnvironment()
		env.RegisterWorkflow(engine.Run)
		env.OnActivity(engine.Task, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(engine.Task)
		env.OnActivity(engine.TaskInScope, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(engine.TaskInScope)
		env.OnActivity(engine.WorkflowVars, mock.Anything, mock.Anything).Return(engine.WorkflowVars)

		env.ExecuteWorkflow(engine.Run, state)
		require.True(t, env.IsWorkflowCompleted())

		err = env.GetWorkflowError()
		var continued *workflow.ContinueAsNewError
		if !errors.As(err, &continued) {
			lastError = err

			break
		}

		next := &v1.RunState{}
		require.NoError(t, converter.GetDefaultDataConverter().FromPayloads(continued.Input, &next))
		state = next
	}

	require.Positive(t, segments,
		"the run never suspended, so this proves nothing the single-segment case did not")
	require.Error(t, lastError, "the run was expected to fail")
	require.Contains(t, lastError.Error(), test.Summary,
		"a run that suspended before it failed did not take back what earlier segments did")
	require.Equal(t, test.Recorded, recorded(),
		"the effects that happened, and their order, are not what compensating should have produced")
}

// TestPendingLoopCompensationsSurviveContinueAsNew is
// [TestPendingCompensationsSurviveContinueAsNew] pointed at the placement #253
// opened, and it is the case the issue named as the one to prove.
//
// A progressive rollout is the workload that suspends by construction: a loop is
// where the durable driver deliberately looks for an iteration boundary to hand
// over at, so a rollout of any length spans several segments. Each segment
// registers the compensation for the iterations it ran and then stops executing
// entirely; the segment that finally fails replays none of their history. If the
// registrations did not ride `RunState.pending_undo`, that segment would unwind
// only its own iterations and report a clean failure over a half-shifted fleet.
//
// It is worth stating what this asserts that #233 does not. `Frame.results` is a
// different structure with a different fate — [v1.LoopResumeResults] drops it on
// the way in when nothing outside the loop can read it — and the undo log is a
// top-level field of `RunState`, appended to and never compacted. The assertion
// below is that a rollout whose `results` may legitimately have been thrown away
// still takes back every iteration, in reverse, across every handover.
//
// `StepsBudget: 1` puts a suspension at every iteration boundary, so the reversal
// under test crosses a Continue-As-New between each pair of entries rather than
// only once somewhere in the middle.
func TestPendingLoopCompensationsSurviveContinueAsNew(t *testing.T) {
	base, recorded := conformance.NewUndoServer(t)
	cases := conformance.UndoLoopCases(base)
	test := cases[0]
	require.Equal(t, "a loop body's compensations undo newest iteration first", test.Name,
		"this test is written against the first shared loop case; the list was reordered")

	state := &v1.RunState{Workflow: test.Workflow, StepsBudget: 1}

	var (
		segments  int
		lastError error
	)
	for segments = 0; segments < 20; segments++ {
		testSuite := &testsuite.WorkflowTestSuite{}
		env := testSuite.NewTestWorkflowEnvironment()
		env.RegisterWorkflow(engine.Run)
		env.OnActivity(engine.Task, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(engine.Task)
		env.OnActivity(engine.TaskInScope, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(engine.TaskInScope)
		env.OnActivity(engine.WorkflowVars, mock.Anything, mock.Anything).Return(engine.WorkflowVars)

		env.ExecuteWorkflow(engine.Run, state)
		require.True(t, env.IsWorkflowCompleted())

		err := env.GetWorkflowError()
		var continued *workflow.ContinueAsNewError
		if !errors.As(err, &continued) {
			lastError = err

			break
		}

		next := &v1.RunState{}
		require.NoError(t, converter.GetDefaultDataConverter().FromPayloads(continued.Input, &next))
		state = next
	}

	// Asserted rather than assumed: a budget that stopped suspending would leave
	// every case below passing for a run that never handed over at all, which is
	// CLAUDE.md's rule about a bound nothing reaches.
	require.Greater(t, segments, 2,
		"the rollout did not suspend between its iterations, so this proves nothing the "+
			"single-segment loop case did not")
	require.Error(t, lastError, "the run was expected to fail")
	require.Contains(t, lastError.Error(), test.Summary,
		"a rollout that suspended between iterations did not take back what earlier segments shifted")
	require.Equal(t, test.Recorded, recorded(),
		"the effects that happened, and their order, are not what unwinding a suspended rollout should have produced")
}
