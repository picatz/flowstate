package engine_test

import (
	"fmt"
	"net/http"
	"testing"

	"github.com/google/go-cmp/cmp"
	v1 "github.com/picatz/flowstate/pkg/flowstate/v1"
	"github.com/picatz/flowstate/pkg/flowstate/v1/engine"
	"github.com/picatz/flowstate/pkg/flowstate/v1/flowfile"
	"github.com/picatz/flowstate/pkg/flowstate/v1/tests"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	"go.temporal.io/sdk/converter"
	"go.temporal.io/sdk/testsuite"
	"go.temporal.io/sdk/workflow"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/testing/protocmp"
)

func runWorkflow(t *testing.T, input *v1.Workflow, expected *v1.Workflow_StepOutputs) {
	t.Helper()

	testSuite := &testsuite.WorkflowTestSuite{}

	env := testSuite.NewTestWorkflowEnvironment()

	env.RegisterWorkflow(engine.Run)
	env.OnActivity(engine.Task, mock.Anything, mock.Anything).Return(engine.Task)
	env.OnActivity(engine.TaskWithPrev, mock.Anything, mock.Anything, mock.Anything).Return(engine.TaskWithPrev)
	env.OnActivity(engine.TaskInScope, mock.Anything, mock.Anything, mock.Anything).Return(engine.TaskInScope)
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
	baseURL := tests.NewHTTPServer(t)
	for _, test := range tests.Workflows(baseURL) {
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
	failedSteps := tests.PolicyCaseFailedSteps()

	for _, test := range tests.PolicyCases() {
		t.Run(test.Name, func(t *testing.T) {
			testSuite := &testsuite.WorkflowTestSuite{}
			env := testSuite.NewTestWorkflowEnvironment()
			env.RegisterWorkflow(engine.Run)
			env.OnActivity(engine.Task, mock.Anything, mock.Anything).Return(engine.Task)
			env.OnActivity(engine.TaskWithPrev, mock.Anything, mock.Anything, mock.Anything).Return(engine.TaskWithPrev)
			env.OnActivity(engine.TaskInScope, mock.Anything, mock.Anything, mock.Anything).Return(engine.TaskInScope)
			env.OnActivity(engine.TaskInScope, mock.Anything, mock.Anything, mock.Anything).Return(engine.TaskInScope)

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

// TestRunWorkflowControlFlow runs the shared loop and parallel cases against the
// durable driver, where iterations and branches are genuinely concurrent.
func TestRunWorkflowControlFlow(t *testing.T) {
	baseURL := tests.NewHTTPServer(t)
	for _, test := range tests.ControlFlowCases(baseURL) {
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
	env.OnActivity(engine.Task, mock.Anything, mock.Anything).Return(engine.Task)
	env.OnActivity(engine.TaskWithPrev, mock.Anything, mock.Anything, mock.Anything).Return(engine.TaskWithPrev)
	env.OnActivity(engine.TaskInScope, mock.Anything, mock.Anything, mock.Anything).Return(engine.TaskInScope)

	return env
}

// TestABudgetThatFitsRunsInOneSegment is the boundary case: exactly enough budget for
// the steps there are.
//
// Worth having on its own, because off-by-one here is a run that suspends when it did
// not need to — correct, invisible, and paying for a Continue-As-New every time.
func TestABudgetThatFitsRunsInOneSegment(t *testing.T) {
	baseURL := tests.NewHTTPServer(t)
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
	baseURL := tests.NewHTTPServer(t)
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
	for _, test := range tests.LogCases() {
		t.Run(test.Name, func(t *testing.T) {
			testSuite := &testsuite.WorkflowTestSuite{}
			env := testSuite.NewTestWorkflowEnvironment()
			env.RegisterWorkflow(engine.Run)
			env.OnActivity(engine.Task, mock.Anything, mock.Anything).Return(engine.Task)
			env.OnActivity(engine.TaskInScope, mock.Anything, mock.Anything, mock.Anything).Return(engine.TaskInScope)
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
	baseURL := tests.NewHTTPServer(t)
	for _, test := range tests.VarsCases(baseURL) {
		t.Run(test.Name, func(t *testing.T) {
			testSuite := &testsuite.WorkflowTestSuite{}
			env := testSuite.NewTestWorkflowEnvironment()
			env.RegisterWorkflow(engine.Run)
			env.OnActivity(engine.Task, mock.Anything, mock.Anything).Return(engine.Task)
			env.OnActivity(engine.TaskInScope, mock.Anything, mock.Anything, mock.Anything).Return(engine.TaskInScope)
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
	baseURL := tests.NewHTTPServer(t)
	for _, test := range tests.ResponseScopeCases(baseURL) {
		t.Run(test.Name, func(t *testing.T) {
			testSuite := &testsuite.WorkflowTestSuite{}
			env := testSuite.NewTestWorkflowEnvironment()
			env.RegisterWorkflow(engine.Run)
			env.OnActivity(engine.Task, mock.Anything, mock.Anything).Return(engine.Task)
			env.OnActivity(engine.TaskInScope, mock.Anything, mock.Anything, mock.Anything).Return(engine.TaskInScope)
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
	baseURL := tests.NewHTTPServer(t)
	for _, test := range tests.ZeroValueCases(baseURL) {
		t.Run(test.Name, func(t *testing.T) {
			runWorkflow(t, test.Workflow, test.ExpectedOutputs)
		})
	}
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
	baseURL := tests.NewHTTPServer(t)
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
	baseURL := tests.NewHTTPServer(t)
	for _, test := range tests.ErrorTextCases(baseURL) {
		t.Run(test.Name, func(t *testing.T) {
			testSuite := &testsuite.WorkflowTestSuite{}
			env := testSuite.NewTestWorkflowEnvironment()
			env.RegisterWorkflow(engine.Run)
			env.OnActivity(engine.Task, mock.Anything, mock.Anything).Return(engine.Task)
			env.OnActivity(engine.TaskInScope, mock.Anything, mock.Anything, mock.Anything).Return(engine.TaskInScope)
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
	baseURL := tests.NewHTTPServer(t)
	cases := tests.ErrorTextCases(baseURL)
	require.NotEmpty(t, cases, "no error-text cases, so this asserts nothing")

	for _, test := range cases {
		t.Run(test.Name, func(t *testing.T) {
			testSuite := &testsuite.WorkflowTestSuite{}
			env := testSuite.NewTestWorkflowEnvironment()
			env.RegisterWorkflow(engine.Run)
			env.OnActivity(engine.Task, mock.Anything, mock.Anything).Return(engine.Task)
			env.OnActivity(engine.TaskInScope, mock.Anything, mock.Anything, mock.Anything).Return(engine.TaskInScope)
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

// TestRunWorkflowNestedErrorText is the durable half of the nested case.
//
// The task case converges because errors.As reaches a TaskError through every
// wrapper. This one has no TaskError to find, so the structural position is part
// of what the failure says — and the durable driver used to read the innermost
// recorded text out of the envelope and drop every wrapper on the way to it.
func TestRunWorkflowNestedErrorText(t *testing.T) {
	for _, test := range tests.NestedErrorTextCases() {
		t.Run(test.Name, func(t *testing.T) {
			testSuite := &testsuite.WorkflowTestSuite{}
			env := testSuite.NewTestWorkflowEnvironment()
			env.RegisterWorkflow(engine.Run)
			env.OnActivity(engine.Task, mock.Anything, mock.Anything).Return(engine.Task)
			env.OnActivity(engine.TaskInScope, mock.Anything, mock.Anything, mock.Anything).Return(engine.TaskInScope)
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
