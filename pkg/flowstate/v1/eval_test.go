package flowstatev1_test

import (
	"context"
	"fmt"
	"slices"
	"testing"
	"time"

	"github.com/google/go-cmp/cmp"
	v1 "github.com/picatz/flowstate/pkg/flowstate/v1"
	"github.com/picatz/flowstate/pkg/flowstate/v1/flowfile"
	"github.com/picatz/flowstate/pkg/flowstate/v1/tests"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/testing/protocmp"

	"github.com/stretchr/testify/require"
)

func runWorkflow(t *testing.T, input *v1.Workflow, expected *v1.Workflow_StepOutputs) {
	t.Helper()

	output, err := v1.Run(t.Context(), input)
	require.NoError(t, err)
	require.NotEmpty(t, output)

	require.True(
		t,
		proto.Equal(expected, output),
		"Expected output does not match actual output:\n%s",
		cmp.Diff(expected, output, protocmp.Transform()),
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

// TestRunWorkflowZeroValues pins that legitimately empty values survive a round
// trip through the task input and output conversion layer.
func TestRunWorkflowZeroValues(t *testing.T) {
	baseURL := tests.NewHTTPServer(t)
	for _, test := range tests.ZeroValueCases(baseURL) {
		t.Run(test.Name, func(t *testing.T) {
			runWorkflow(t, test.Workflow, test.ExpectedOutputs)
		})
	}
}

// TestRunWorkflowControlFlow covers loops and parallel branches in the local
// driver. The engine package runs the same cases against the durable driver.
func TestRunWorkflowControlFlow(t *testing.T) {
	baseURL := tests.NewHTTPServer(t)
	for _, test := range tests.ControlFlowCases(baseURL) {
		t.Run(test.Name, func(t *testing.T) {
			runWorkflow(t, test.Workflow, test.ExpectedOutputs)
		})
	}
}

// TestRunWorkflowLoop covers the `loop:` primitive in the local driver.
//
// The engine package runs the identical [tests.LoopCases] against the durable
// driver, which is what holds the two to one answer about how many iterations a
// loop runs, what state it carries out, and that it fails at its ceiling rather than
// stopping silently — the disagreements invariant 3 exists to catch, and the reason
// the ceiling and the exhaustion error live in one place each (loop.go).
func TestRunWorkflowLoop(t *testing.T) {
	for _, test := range tests.LoopCases() {
		t.Run(test.Name, func(t *testing.T) {
			out, err := v1.Run(t.Context(), test.Workflow)
			if test.ExpectFailure {
				require.Error(t, err, "the loop was expected to fail at its ceiling")
				require.Contains(t, err.Error(), "ran its full budget",
					"a loop that exhausts its budget must say so distinctly")
				return
			}
			require.NoError(t, err)
			require.Empty(t, cmp.Diff(test.ExpectedOutputs, out, protocmp.Transform()))
		})
	}
}

// TestRunWorkflowPolicy covers conditions and per-step policy in the local
// driver. The same cases run against the durable driver in the engine package,
// which is what keeps the two from diverging.
func TestRunWorkflowPolicy(t *testing.T) {
	failedSteps := tests.PolicyCaseFailedSteps()

	for _, test := range tests.PolicyCases() {
		t.Run(test.Name, func(t *testing.T) {
			if test.ExpectedOutputs == nil {
				// Cases whose failure text is engine-specific: assert the shape
				// instead of the exact message.
				out, err := v1.Run(t.Context(), test.Workflow)
				require.NoError(t, err)

				step, ok := failedSteps[test.Name]
				require.True(t, ok, "case with no expected outputs must name its failed step")
				require.Contains(t, out.GetStepValues(), step)
				require.Contains(t, out.GetStepValues()[step].GetNamedValues(), "error",
					"a step tolerated by continue_on_error must record its failure")
				return
			}
			runWorkflow(t, test.Workflow, test.ExpectedOutputs)
		})
	}
}

// TestRunWorkflowWait covers durable waiting in the local driver.
//
// The same cases run against the durable driver in the engine package. Waiting is
// where the two drivers are most different underneath — a timer here is a sleep in
// a process, and there it is state on a server — so it is where holding them to
// one set of expectations matters most.
func TestRunWorkflowWait(t *testing.T) {
	for _, test := range tests.WaitCases() {
		t.Run(test.Name, func(t *testing.T) {
			runWorkflow(t, test.Workflow, test.ExpectedOutputs)
		})
	}
}

// TestNestedValueIsReachableByIndex pins that a nested map survives being carried as a
// value and can still be indexed where it is read.
//
// It used to hold a `cel` step's result. The value now comes from a `vars:` binding,
// which is where a computed one lives since that task retired — same conversion layer,
// same indexing, one fewer step.
func TestNestedValueIsReachableByIndex(t *testing.T) {
	wf := &v1.Workflow{
		Name:    "nested",
		Profile: v1.CurrentProfile,
		Vars: map[string]*v1.Value{
			"nested": v1.NewExpr("{'outer': {'inner': 'val'}}"),
		},
		Steps: []*v1.Node{
			{
				Id:        "pick",
				Condition: v1.NewExpr("vars.nested['outer']['inner'] == 'val'"),
				Kind: &v1.Node_Task{Task: &v1.Task{Name: "log", Inputs: map[string]*v1.Value{
					"message": v1.NewLiteral("found it"),
				}}},
			},
			{
				Id:        "pick_else",
				Condition: v1.NewExpr("vars.nested['outer']['inner'] != 'val'"),
				Kind: &v1.Node_Task{Task: &v1.Task{Name: "log", Inputs: map[string]*v1.Value{
					"message": v1.NewLiteral("wrong value"),
				}}},
			},
		},
	}
	out, err := v1.Run(t.Context(), wf)
	require.NoError(t, err)
	expected := &v1.Workflow_StepOutputs{StepValues: map[string]*v1.Node_Outputs{
		"pick": {},
	}}
	require.Empty(t, cmp.Diff(expected, out, protocmp.Transform()))
}

// TestRunWorkflowVars covers the workflow's `vars:` block in the local driver.
//
// The same cases run against the durable driver in the engine package. That matters
// more here than for most features, because the two drivers reach this state by
// different routes: locally the vars are evaluated in process before the first step,
// durably they are evaluated in an activity and then carried across Continue-As-New.
// Two routes to one observable is the shape that drifts.
func TestRunWorkflowVars(t *testing.T) {
	baseURL := tests.NewHTTPServer(t)
	for _, test := range tests.VarsCases(baseURL) {
		t.Run(test.Name, func(t *testing.T) {
			out, err := v1.Run(t.Context(), test.Workflow)
			require.NoError(t, err)
			require.Empty(t, cmp.Diff(test.ExpectedOutputs, out, protocmp.Transform()))
		})
	}
}

// TestRunWorkflowCall covers `call:` in the local driver.
//
// The same cases run against the durable driver in the engine package — see
// TestRunWorkflowCall there. What is under test is the three rules
// [v1.CallScope], [v1.CallOutputs] and [v1.CheckCallDepth] both drivers reach,
// so a case that only exercised one driver would not tell the two apart from
// one that reaches a divergent path only the other driver walks.
func TestRunWorkflowCall(t *testing.T) {
	for _, test := range tests.CallCases() {
		t.Run(test.Name, func(t *testing.T) {
			out, err := v1.Run(t.Context(), test.Workflow)
			if test.ExpectFailure {
				require.Error(t, err, "the call was expected to be refused")
				return
			}
			require.NoError(t, err)
			if test.ExpectedOutputsPredicate != nil {
				require.True(t, test.ExpectedOutputsPredicate(out), "unexpected outputs: %v", out)
				return
			}
			require.Empty(t, cmp.Diff(test.ExpectedOutputs, out, protocmp.Transform()))
		})
	}
}

// TestRunWorkflowInputsAndOutputs covers `inputs:` and `outputs:` in the local
// driver.
//
// The same cases run against the durable driver in the engine package. Both reach
// them through the submit boundary each driver actually has — here that is
// [v1.RunWithInputs], there it is the check the server performs before starting the
// workflow — which is the pairing that matters: the checking and the defaulting are
// one function, and a driver that skipped it would accept a submission the other
// refuses.
func TestRunWorkflowInputsAndOutputs(t *testing.T) {
	baseURL := tests.NewHTTPServer(t)
	for _, test := range tests.InputOutputCases(baseURL) {
		t.Run(test.Name, func(t *testing.T) {
			out, err := v1.RunWithInputs(t.Context(), test.Workflow, test.Inputs)
			require.NoError(t, err)
			require.Empty(t, cmp.Diff(test.ExpectedOutputs, out, protocmp.Transform()))
		})
	}
}

// TestRunWorkflowInputsRefused is the negative direction of the same corpus: a
// submission that must be refused before anything runs.
//
// Run by both drivers, because "refused" is an observable and the local driver
// exists to predict it. A local run that started work on arguments the server would
// have rejected is a rehearsal that says yes where production says no — and nothing
// about the happy-path cases above could detect it.
func TestRunWorkflowInputsRefused(t *testing.T) {
	for _, test := range tests.InputRefusalCases() {
		t.Run(test.Name, func(t *testing.T) {
			_, err := v1.RunWithInputs(t.Context(), test.Workflow, test.Inputs)
			require.Error(t, err, "the submission was accepted")
			require.Contains(t, err.Error(), test.Contains)
		})
	}
}

// TestRunWorkflowResponseScope covers what an http step's `expect:` and `outputs:`
// can see, in the local driver.
//
// The same cases run against the durable driver in the engine package, and the reason
// is the one the shared package exists for read backwards: what these guard is not a
// difference between the drivers but a difference between two positions in one file,
// and both drivers reach both positions through the same task. A set that ran here
// only would let the durable driver rebuild that activation by hand unobserved.
func TestRunWorkflowResponseScope(t *testing.T) {
	baseURL := tests.NewHTTPServer(t)
	for _, test := range tests.ResponseScopeCases(baseURL) {
		t.Run(test.Name, func(t *testing.T) {
			out, err := v1.Run(t.Context(), test.Workflow)
			require.NoError(t, err)
			require.Empty(t, cmp.Diff(test.ExpectedOutputs, out, protocmp.Transform()))
		})
	}
}

// TestRunWorkflowLog covers the `log` task in the local driver.
//
// What a workflow's *result* can see of a log step is that it ran and produced nothing,
// which is the claim these pin. Where the message went is decided elsewhere and tested
// against a captured logger there.
func TestRunWorkflowLog(t *testing.T) {
	for _, test := range tests.LogCases() {
		t.Run(test.Name, func(t *testing.T) {
			out, err := v1.Run(t.Context(), test.Workflow)
			require.NoError(t, err)
			require.Empty(t, cmp.Diff(test.ExpectedOutputs, out, protocmp.Transform()))
		})
	}
}

// TestRunWorkflowErrorText pins what a tolerated failure records, in the local
// driver. The durable driver runs the same cases in the engine package, and the
// pairing is the whole point: this value used to be a different sentence in each.
func TestRunWorkflowErrorText(t *testing.T) {
	baseURL := tests.NewHTTPServer(t)
	for _, test := range tests.ErrorTextCases(baseURL) {
		t.Run(test.Name, func(t *testing.T) {
			out, err := v1.Run(t.Context(), test.Workflow)
			require.NoError(t, err)
			require.Empty(t, cmp.Diff(test.ExpectedOutputs, out, protocmp.Transform()))
		})
	}
}

// TestRunWorkflowToleratedStepFailure covers a non-task failure tolerated at the
// step that raised it, in the local driver.
//
// The local half is where a step's own `vars:` failing used to abort the whole
// run: the evaluation returned out of runNodes above the `continue_on_error`
// check, so the driver that exists to predict production was stricter than it.
// The engine package runs the identical cases against the durable driver, which
// is the only thing that can say the two now agree.
func TestRunWorkflowToleratedStepFailure(t *testing.T) {
	for _, test := range tests.ToleratedStepFailureCases() {
		t.Run(test.Name, func(t *testing.T) {
			out, err := v1.Run(t.Context(), test.Workflow)
			require.NoError(t, err)
			require.Empty(t, cmp.Diff(test.ExpectedOutputs, out, protocmp.Transform()))
		})
	}
}

func TestRunWorkflowNestedErrorText(t *testing.T) {
	for _, test := range tests.NestedErrorTextCases() {
		t.Run(test.Name, func(t *testing.T) {
			out, err := v1.Run(t.Context(), test.Workflow)
			require.NoError(t, err)
			require.Empty(t, cmp.Diff(test.ExpectedOutputs, out, protocmp.Transform()))
		})
	}
}

// runAuthorityCase installs test's Authority the way the local driver actually
// does it in production — [v1.ContextWithTaskRuntime] on the context a run is
// started with — and runs the case through [v1.Run].
//
// engine/authority_test.go's runAuthorityCase runs the identical [tests.AuthorityCase]
// through worker registration instead. That pairing is #116: before it, secret
// denial and containment were each proven once, by whichever driver's test file
// happened to add them, and nothing compared the two.
func runAuthorityCase(t *testing.T, test tests.AuthorityCase) {
	t.Helper()

	ctx := t.Context()
	if !test.Authority.NoRuntime {
		runtime := v1.TaskRuntime{Broker: test.Authority.Broker(t), Identity: test.Authority.Identity}
		if test.Authority.HasSecrets() {
			runtime.Store = test.Authority.Store(t)
			runtime.Policy = test.Authority.Policy(t)
		}
		ctx = v1.ContextWithTaskRuntime(ctx, runtime)
	}

	out, err := v1.Run(ctx, test.Workflow)
	require.NoError(t, err)
	require.Empty(t, cmp.Diff(test.ExpectedOutputs, out, protocmp.Transform()))

	if test.ContainmentValue != "" {
		tests.AssertNoLeak(t, out, test.ContainmentValue)
	}
}

// TestAuthorityDenial runs the shared fail-closed and policy-denial cases
// against the local driver. The durable driver runs the same cases in
// engine/authority_test.go.
func TestAuthorityDenial(t *testing.T) {
	for _, test := range tests.AuthorityDenialCases() {
		t.Run(test.Name, func(t *testing.T) {
			runAuthorityCase(t, test)
			if test.Authority.ProviderCalls != nil {
				require.Zero(t, test.Authority.ProviderCalls.Load(),
					"the fixture provider resolved a reference the policy should have denied first")
			}
		})
	}
}

// TestAuthorityContainment runs the shared secret and JIT credential
// containment cases against the local driver. The durable driver runs the
// same cases in engine/authority_test.go.
func TestAuthorityContainment(t *testing.T) {
	baseURL := tests.NewHTTPServer(t)
	for _, test := range tests.AuthorityContainmentCases(baseURL) {
		t.Run(test.Name, func(t *testing.T) {
			runAuthorityCase(t, test)
		})
	}
}

// TestRunWorkflowUndo is the local half of the saga cases.
//
// The engine package runs the identical [tests.UndoCases] against the durable
// driver. That pairing is the whole point: compensation is where the two drivers
// have the most reason to be implemented separately — a function call here, an
// activity scheduled by a failing workflow there — and the order compensations run
// in is exactly what a local run exists to rehearse.
//
// A recording server per case rather than one for the whole set, because what is
// asserted is a *sequence* of requests: sharing one would make each case's
// expectation depend on which cases ran before it. The case list is built twice for
// that reason — once to enumerate, once against the server the subtest started.
func TestRunWorkflowUndo(t *testing.T) {
	for index, outline := range tests.UndoCases(undoPlaceholderBase) {
		t.Run(outline.Name, func(t *testing.T) {
			base, recorded := tests.NewUndoServer(t)
			test := tests.UndoCases(base)[index]

			_, err := v1.Run(t.Context(), test.Workflow)
			if !test.Fails {
				require.NoError(t, err, "the run was expected to succeed")
			} else {
				require.Error(t, err, "the run was expected to fail")
				require.Contains(t, err.Error(), test.Summary,
					"the failure does not carry the account of what was compensated")
			}

			require.Equal(t, test.Recorded, recorded(),
				"the effects that happened, and their order, are not what compensating should have produced")
		})
	}
}

// TestRunWorkflowUndoOnCancellation is the local half of the cancellation cases.
//
// The engine package runs the identical [tests.UndoCancellationCases]. This is the
// pairing invariant 3 most needs on this path, because the two drivers do not
// merely implement it differently — they implement it against different
// cancellation *mechanisms*. Locally the scope a compensation must escape is a
// [context.Context]; durably it is a workflow context Temporal has cancelled and
// every activity scheduled on refuses. An implementation that got either wrong
// would report a run that took back nothing, having attempted nothing, which is the
// failure this whole set exists to catch.
//
// The cancellation is delivered when the marker token arrives, which is what makes
// this deterministic rather than timed — see `reaches` in the shared cases for why
// the last compensated step's own token is the wrong signal to use.
func TestRunWorkflowUndoOnCancellation(t *testing.T) {
	for index, outline := range tests.UndoCancellationCases(undoPlaceholderBase) {
		t.Run(outline.Name, func(t *testing.T) {
			base, recorded := tests.NewUndoServer(t)
			test := tests.UndoCancellationCases(base)[index]

			ctx, cancel := context.WithCancel(t.Context())
			defer cancel()

			done := make(chan error, 1)
			go func() {
				_, err := v1.Run(ctx, test.Workflow)
				done <- err
			}()

			require.Eventually(t, func() bool {
				return slices.Contains(recorded(), "z")
			}, 30*time.Second, time.Millisecond,
				"the run never reached the step it was to be cancelled at")

			// `flow cancel`, arriving while the run is parked.
			cancel()

			var err error
			select {
			case err = <-done:
			case <-time.After(30 * time.Second):
				t.Fatal("a cancelled run did not stop, so its compensations never ran")
			}

			require.Error(t, err, "a cancelled run reported success")

			// The distinction the whole feature rests on: a run somebody stopped on
			// purpose still reads as stopped, not as failed. Compensating must not
			// change what the run *is* — see [v1.UndoRunError], which wraps for
			// exactly this reason.
			require.ErrorIs(t, err, context.Canceled,
				"a stopped run stopped reading as cancelled once it compensated: %v", err)

			if test.Summary == "" {
				require.NotContains(t, err.Error(), "compensation ran",
					"a run with nothing registered reported compensating anyway")
			} else {
				require.Contains(t, err.Error(), test.Summary,
					"the cancellation does not carry the account of what was compensated")
			}

			require.Equal(t, test.Recorded, recorded(),
				"the effects that happened, and their order, are not what compensating a cancelled run should have produced")
		})
	}
}

// undoPlaceholderBase is a base URL used only to enumerate the shared saga cases.
//
// `.invalid` is reserved by RFC 2606 and resolves nowhere, so a case list built
// with it and then accidentally *run* fails rather than reaching something real.
const undoPlaceholderBase = "http://undo.invalid"

// TestRunWorkflowUndoPlacement pins the shapes the local engine refuses.
//
// `flow validate` refuses them earlier and with a position, which is where an
// author meets them. This is the backstop for a specification that never came from
// a Flowfile — and it is a driver-agreement case rather than a local one because a
// refusal that held here and not durably would be a rehearsal passing a workload
// production rejects, which is invariant 3 in the other direction.
func TestRunWorkflowUndoPlacement(t *testing.T) {
	base, _ := tests.NewUndoServer(t)

	for _, test := range tests.UndoPlacementCases(base) {
		t.Run(test.Name, func(t *testing.T) {
			_, err := v1.Run(t.Context(), test.Workflow)
			require.Error(t, err, "a compensation the engine cannot honour was accepted")
			require.Contains(t, err.Error(), "`undo:` is only supported on",
				"the refusal does not say what is wrong with where the compensation is written")
		})
	}
}
