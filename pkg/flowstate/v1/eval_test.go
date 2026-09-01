package flowstatev1_test

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"slices"
	"strconv"
	"sync/atomic"
	"testing"
	"time"

	"github.com/google/go-cmp/cmp"
	v1 "github.com/picatz/flowstate/pkg/flowstate/v1"
	"github.com/picatz/flowstate/pkg/flowstate/v1/flowfile"
	"github.com/picatz/flowstate/pkg/flowstate/v1/internal/conformance"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/testing/protocmp"
	"google.golang.org/protobuf/types/known/durationpb"

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

// TestUnavailableTaskCapabilitiesAreRefusedBeforeEffects is the local caller
// of the shared admission case. The registry is run-scoped on purpose: using
// DefaultRegistry here would let a rehearsal claim tasks it cannot dispatch.
func TestUnavailableTaskCapabilitiesAreRefusedBeforeEffects(t *testing.T) {
	var effects atomic.Int32
	tc, effect, missing := conformance.TaskCapabilityAdmissionCase(func() { effects.Add(1) })

	registry := v1.NewRegistry()
	require.NoError(t, registry.Register(effect))
	ctx := v1.NewContextWithRegistry(t.Context(), registry)

	out, err := v1.Run(ctx, tc.Workflow)
	require.Zero(t, effects.Load(), "the first effect ran before capability refusal")
	require.Error(t, err)
	require.Nil(t, out, "admission refusal must have no partial transcript")
	require.ErrorContains(t, err, tc.ExpectedErrorContains)
	for _, name := range missing {
		require.ErrorContains(t, err, name)
	}
}

// TestRunWorkflowZeroValues pins that legitimately empty values survive a round
// trip through the task input and output conversion layer.
func TestRunWorkflowZeroValues(t *testing.T) {
	baseURL := conformance.NewHTTPServer(t)
	for _, test := range conformance.ZeroValueCases(baseURL) {
		t.Run(test.Name, func(t *testing.T) {
			runWorkflow(t, test.Workflow, test.ExpectedOutputs)
		})
	}
}

// TestRunWorkflowControlFlow covers loops and parallel branches in the local
// driver. The engine package runs the same cases against the durable driver.
func TestRunWorkflowControlFlow(t *testing.T) {
	baseURL := conformance.NewHTTPServer(t)
	for _, test := range conformance.ControlFlowCases(baseURL) {
		t.Run(test.Name, func(t *testing.T) {
			runWorkflow(t, test.Workflow, test.ExpectedOutputs)
		})
	}
}

// TestRunWorkflowLoop covers the `loop:` primitive in the local driver.
//
// The engine package runs the identical [conformance.LoopCases] against the durable
// driver, which is what holds the two to one answer about how many iterations a
// loop runs, what state it carries out, and that it fails at its ceiling rather than
// stopping silently — the disagreements invariant 3 exists to catch, and the reason
// the ceiling and the exhaustion error live in one place each (loop.go).
func TestRunWorkflowLoop(t *testing.T) {
	for _, test := range conformance.LoopCases() {
		t.Run(test.Name, func(t *testing.T) {
			out, err := v1.Run(t.Context(), test.Workflow)
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
			if test.ExpectedOutputsPredicate != nil {
				require.True(t, test.ExpectedOutputsPredicate(out), "unexpected outputs: %v", out)
				return
			}
			require.Empty(t, cmp.Diff(test.ExpectedOutputs, out, protocmp.Transform()))
		})
	}
}

// TestRunWorkflowLoopExhaustionTranscript is the local half of what an
// exhausted loop's transcript entry says (#157's question 3): the iterations
// that ran recorded under `results` — tolerated failures naming the state they
// carried — and nothing at all for iterations the spent budget never let
// start. The engine package runs the identical cases against the durable
// driver, which is the only thing that can say the two agree.
func TestRunWorkflowLoopExhaustionTranscript(t *testing.T) {
	for _, test := range conformance.LoopExhaustionTranscriptCases() {
		t.Run(test.Name, func(t *testing.T) {
			out, err := v1.Run(t.Context(), test.Workflow)
			require.Error(t, err, "these loops exhaust their budget on purpose")
			require.Contains(t, err.Error(), "ran its full budget",
				"exhaustion must keep its distinct sentence")

			// Compared whole: an entry for an iteration the loop never ran is
			// as wrong as a missing entry for one it did — the failed/never-
			// attempted line is exactly what this record exists to draw.
			require.Empty(t, cmp.Diff(test.Expected, out, protocmp.Transform()))
		})
	}
}

// TestRunWorkflowToleratedIterationIdentity is the local half of a tolerated
// iteration failure carrying its `as:` binding (#157's question 3): the failed
// entry names its item directly, and a later step's expression can read it,
// instead of reconstructing identity downstream by set subtraction — while a
// successful step that merely declares outputs named `error`/`item` keeps its
// shape untouched. The engine package runs the identical cases against the
// durable driver.
func TestRunWorkflowToleratedIterationIdentity(t *testing.T) {
	baseURL := conformance.NewHTTPServer(t)
	for _, test := range conformance.ToleratedIterationIdentityCases(baseURL) {
		t.Run(test.Name, func(t *testing.T) {
			runWorkflow(t, test.Workflow, test.ExpectedOutputs)
		})
	}
}

// TestRunWorkflowPolicy covers conditions and per-step policy in the local
// driver. The same cases run against the durable driver in the engine package,
// which is what keeps the two from diverging.
func TestRunWorkflowPolicy(t *testing.T) {
	failedSteps := conformance.PolicyCaseFailedSteps()

	for _, test := range conformance.PolicyCases() {
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

// TestRunWorkflowErrorKind pins that a run failing outright in the local
// driver is classified the way [conformance.ErrorKindCases] says it must be. The
// same cases run against the durable driver in
// engine.TestRunWorkflowErrorKind — invariant 3's "shared cases, two verified
// callers" for the classification #241's P2 puts on the wire.
func TestRunWorkflowErrorKind(t *testing.T) {
	baseURL := conformance.NewHTTPServer(t)

	// The timeout case's fixture (#915), in a registry derived from the build's
	// rather than added to it.
	//
	// The other cases need the tasks this build ships — including the
	// loopback-permitting `http` [conformance.NewHTTPServer] has just installed
	// on the default registry — and the fixture must not join them there:
	// TestEveryTaskDescribesItself walks [v1.DefaultRegistry] in parallel with
	// this and holds every task in it to describing its inputs, which a fixture
	// taking none cannot do. Registering and unregistering around this test
	// would leave exactly that window open. Copying is what
	// [v1.NewContextWithRegistry] is for, and the durable caller reaches the
	// same task the only way it can (see engine.TestRunWorkflowErrorKind's own
	// note: an activity cannot see a registry on the workflow's context).
	registry := v1.NewRegistry()
	for _, def := range v1.DefaultRegistry().All() {
		require.NoError(t, registry.Register(def))
	}
	require.NoError(t, registry.Register(conformance.ErrorKindTimeoutTaskDef()))

	for _, tc := range conformance.ErrorKindCases(baseURL) {
		t.Run(tc.Name, func(t *testing.T) {
			_, err := v1.Run(v1.NewContextWithRegistry(t.Context(), registry), tc.Workflow)
			require.Error(t, err, "the case must fail the run outright")
			require.Equal(t, tc.ExpectedKind, v1.ClassifyError(err))
		})
	}
}

// TestRunWorkflowTaskPolicy covers #187 slice 1's task-shape policy in the
// local driver. The same cases run against the durable driver in the engine
// package (TestRunWorkflowTaskPolicyDurable) — verified callers on both,
// which is what invariant 3 asks a shared case set to have, per CLAUDE.md's
// account of [conformance.ZeroValueCases] sitting unreached by one driver for
// months.
//
// Each case installs its own policy as the process-wide default
// ([v1.SetDefaultTaskPolicy]) for the duration of its subtest and restores
// nil afterward — global state, guarded by running one case at a time
// (no t.Parallel here), the same posture [allowLoopback] in conformance.go takes
// for the egress registry swap.
func TestRunWorkflowTaskPolicy(t *testing.T) {
	for _, tc := range conformance.TaskPolicyCases() {
		t.Run(tc.Name, func(t *testing.T) {
			policy, err := tc.Policy.Policy()
			require.NoError(t, err, "every case's policy must itself compile")

			v1.SetDefaultTaskPolicy(policy)
			t.Cleanup(func() { v1.SetDefaultTaskPolicy(nil) })

			// The local driver's route for the case's identity: the same
			// seam `flow run local --as-*` uses, which is the whole of what
			// #295 fixed. A case with no identity sets nil, which is
			// identical to not setting one — the starter-less run.
			ctx := v1.NewContextWithRehearsalIdentity(t.Context(), tc.Identity)

			out, err := v1.Run(ctx, tc.Workflow)

			if tc.DeniedTask != "" {
				require.Error(t, err, "the policy must refuse this dispatch")

				var denied *v1.TaskPolicyDeniedError
				require.True(t, errors.As(err, &denied),
					"the failure must be a *v1.TaskPolicyDeniedError, got: %v", err)
				require.Equal(t, tc.DeniedTask, denied.Task)
				require.Equal(t, tc.DeniedReason, denied.Reason)

				// #652 item 3: the denial names the identity it evaluated,
				// in the same words the durable driver's own copy of this
				// loop asserts. Asserted on the message rather than on
				// [v1.TaskPolicyDeniedError.Identity] because the message is
				// what an author reads, and because it is the only form the
				// durable side can compare — the struct does not survive
				// Temporal's failure conversion.
				require.Contains(t, denied.Error(), tc.DeniedIdentity,
					"the denial must name the identity the rule was evaluated against")

				// A local run *is* a rehearsal, so the message must say so
				// here and must not say so durably. This is the one clause
				// the two drivers are meant to differ on.
				require.True(t, denied.Local,
					"a dispatch denied under the local driver is a rehearsal denial")
				return
			}

			require.NoError(t, err)
			require.Empty(t, cmp.Diff(tc.ExpectedOutputs, out, protocmp.Transform()))
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
	for _, test := range conformance.WaitCases() {
		t.Run(test.Name, func(t *testing.T) {
			// Inputs and ExpectFailure are read here rather than delegated to
			// runWorkflow, which does neither. Both arrived with computed
			// durations: a `sleep:` branching on `inputs.plan` needs the first,
			// and a negative one has to fail the run rather than produce outputs
			// to compare. A caller that ignored them would have run every one of
			// those cases as "expect success with no inputs" and passed on the
			// two that are about failing — which is CLAUDE.md's own note about
			// ZeroValueCases, in a suite that already had both callers.
			// A waiter nobody sends anything to. The durable driver always has a
			// signal channel; locally one has to be installed, and without it a
			// gate bounded by a computed timeout fails on "no signal waiter"
			// instead of lapsing — a difference in the driver, not in the case.
			// Delivering a signal is still driver-specific (see [conformance.WaitCases]);
			// timing one out is not.
			ctx := v1.NewContextWithSignalWaiter(t.Context(), v1.NewLocalSignals())

			out, err := v1.RunWithInputs(ctx, test.Workflow, test.Inputs)
			if test.ExpectFailure {
				require.Error(t, err, "the wait was expected to fail the run")
				return
			}
			require.NoError(t, err)
			require.NotEmpty(t, out)

			require.True(t, proto.Equal(test.ExpectedOutputs, out),
				"Expected output does not match actual output:\n%s",
				cmp.Diff(test.ExpectedOutputs, out, protocmp.Transform()))
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
	baseURL := conformance.NewHTTPServer(t)
	for _, test := range conformance.VarsCases(baseURL) {
		t.Run(test.Name, func(t *testing.T) {
			out, err := v1.Run(t.Context(), test.Workflow)
			require.NoError(t, err)
			require.Empty(t, cmp.Diff(test.ExpectedOutputs, out, protocmp.Transform()))
		})
	}
}

// TestRunWorkflowVarsSecretRefused is the negative direction of the same corpus: a
// specification whose `vars:` hold a secret reference, which must be refused before
// anything runs.
//
// Run by both drivers, for the reason [TestRunWorkflowInputsRefused] is: a local run
// that evaluated a var holding a reference would resolve a secret production refuses
// to, which is a rehearsal saying yes where production says no. Nothing above could
// see it — every case there asserts a var *does* reach the scope a step reads, which
// is exactly what makes a secret in one a leak (#169).
func TestRunWorkflowVarsSecretRefused(t *testing.T) {
	for _, test := range conformance.VarsSecretRefusalCases() {
		t.Run(test.Name, func(t *testing.T) {
			_, err := v1.RunWithInputs(t.Context(), test.Workflow, test.Inputs)
			require.Error(t, err, "the submission was accepted")
			require.Contains(t, err.Error(), test.Contains)
		})
	}
}

// TestRunWorkflowTaskOutputElementBound covers the local driver's half of the
// remaining #204 gap: a task's *result*, not a caller's submitted input,
// carrying more list elements than a later expression can walk cheaply.
//
// The same cases run against the durable driver in the engine package — see
// the identically-named test there. Both reach the bound through the one
// function every task's call funnels through, [v1.Task.EvalInScope], which is
// what invariant 3 asks a shared case to hold the two drivers to.
func TestRunWorkflowTaskOutputElementBound(t *testing.T) {
	baseURL := conformance.NewHTTPServer(t)
	for _, test := range conformance.TaskOutputElementBoundCases(baseURL) {
		t.Run(test.Name, func(t *testing.T) {
			out, err := v1.Run(t.Context(), test.Workflow)
			if test.ExpectFailure {
				require.Error(t, err, "a task result past the element bound must be refused")
				require.Contains(t, err.Error(), "10000",
					"the refusal must name the bound it reached")
				return
			}
			require.NoError(t, err)
			require.True(t, test.ExpectedOutputsPredicate(out), "unexpected outputs: %v", out)
		})
	}
}

// TestRunWorkflowTaskOutputSizeBound covers the local driver's half of #787:
// a single task's result weighing more than Temporal will store as an
// activity result. The local driver has no server to refuse an oversized
// result, so without the shared check it admits silently what the durable
// driver wedges on — the rehearsal disagreeing with production in the worst
// direction.
//
// The same cases run against the durable driver in the engine package — see
// the identically-named test there. Both reach the bound through
// [v1.Task.EvalInScope], the same choke point the element bound uses, which
// is what invariant 3 asks a shared case to hold the two drivers to.
func TestRunWorkflowTaskOutputSizeBound(t *testing.T) {
	baseURL := conformance.NewHTTPServer(t)
	for _, test := range conformance.TaskOutputSizeBoundCases(baseURL) {
		t.Run(test.Name, func(t *testing.T) {
			out, err := v1.Run(t.Context(), test.Workflow)
			if test.ExpectFailure {
				require.Error(t, err, "a task result past the output byte bound must be refused")
				require.Contains(t, err.Error(), `step "fetch"`,
					"the refusal must name the step")
				require.Contains(t, err.Error(), strconv.Itoa(v1.MaxTaskOutputBytes),
					"the refusal must name the bound it reached")
				return
			}
			require.NoError(t, err)
			require.True(t, test.ExpectedOutputsPredicate(out), "unexpected outputs: %v", out)
		})
	}
}

// TestRunWorkflowForEachResultsBound covers the local driver's half of #229's
// byte bound for the `for_each` construct: its accumulated `results` are bounded
// in bytes exactly as a `loop:`'s are, through the shared [v1.MaxLoopResultsBytes]
// and [v1.AccumulateForEachResult].
//
// The same cases run against the durable driver in the engine package — see the
// identically-named test there. Both reach the bound through the one accumulation
// point each driver has for a `for_each`, which is what invariant 3 asks a shared
// case to hold the two to. The local driver runs even the `max_parallel:` case
// sequentially by design, so here that case exercises the ordinary sequential
// accumulation; the durable driver's concurrent path is what the engine half
// covers.
func TestRunWorkflowForEachResultsBound(t *testing.T) {
	baseURL := conformance.NewHTTPServer(t)
	for _, test := range conformance.ForEachResultsBoundCases(baseURL) {
		t.Run(test.Name, func(t *testing.T) {
			out, err := v1.Run(t.Context(), test.Workflow)
			if test.ExpectFailure {
				require.Error(t, err, "a for_each past the results byte bound must be refused")
				require.Contains(t, err.Error(), "byte limit",
					"the refusal must name the bound it reached")
				return
			}
			require.NoError(t, err)
			require.True(t, test.ExpectedOutputsPredicate(out), "unexpected outputs: %v", out)
		})
	}
}

// TestRunWorkflowForEachTripCount covers the local driver's half of the
// `for_each` trip-count ceiling, [v1.MaxForEachItems]: the bound on how many
// items a single `for_each` may iterate, which is the one quantity a `for_each`
// carried no bound on at all.
//
// The same cases run against the durable driver in the engine package, see the
// identically-named test there. Both reach the ceiling through the one function
// each driver calls right after resolving `items:`, [v1.CheckForEachItems],
// which is what invariant 3 asks a shared case to hold the two drivers to.
func TestRunWorkflowForEachTripCount(t *testing.T) {
	for _, test := range conformance.ForEachTripCountCases() {
		t.Run(test.Name, func(t *testing.T) {
			out, err := v1.Run(t.Context(), test.Workflow)
			if test.ExpectFailure {
				require.Error(t, err, "a for_each past the trip-count ceiling must be refused")
				// The whole sentence, not just that it failed: the step it
				// happened in, the count observed, and the ceiling reached.
				require.Contains(t, err.Error(), `step "fan"`,
					"the refusal must name the step")
				require.Contains(t, err.Error(), strconv.Itoa(v1.MaxForEachItems+1),
					"the refusal must name the count observed")
				require.Contains(t, err.Error(), strconv.Itoa(v1.MaxForEachItems),
					"the refusal must name the ceiling it reached")
				return
			}
			require.NoError(t, err)
			require.True(t, test.ExpectedOutputsPredicate(out), "unexpected outputs: %v", out)
		})
	}
}

// TestRunWorkflowAtomicBlockBound covers the local driver's half of the
// suspension-opaque fan-out ceiling, [v1.MaxAtomicBlockActivities]: the bound
// on the items × body-activities product of a `for_each` the durable driver
// runs with no Continue-As-New seam — one declaring `max_parallel:` above
// one, or one written inside a `parallel:` branch.
//
// This driver runs iterations sequentially and has no history to protect; it
// refuses anyway, at the identical pre-dispatch point with the identical
// sentence, because a fan-out the rehearsal admits and production refuses is
// the drivers disagreeing about what the file means. The same cases run
// against the durable driver in the engine package, see the
// identically-named test there.
func TestRunWorkflowAtomicBlockBound(t *testing.T) {
	for _, test := range conformance.ForEachAtomicBlockCases() {
		t.Run(test.Name, func(t *testing.T) {
			out, err := v1.Run(t.Context(), test.Workflow)
			if test.ExpectFailure {
				require.Error(t, err, "a for_each past the atomic-activity ceiling must be refused")
				// The same pieces the durable half asserts, in the same
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
			require.True(t, test.ExpectedOutputsPredicate(out), "unexpected outputs: %v", out)
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
	for _, test := range conformance.CallCases() {
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
	baseURL := conformance.NewHTTPServer(t)
	for _, test := range conformance.InputOutputCases(baseURL) {
		t.Run(test.Name, func(t *testing.T) {
			out, err := v1.RunWithInputs(t.Context(), test.Workflow, test.Inputs)
			if test.ExpectFailure {
				// An output that cannot satisfy its own declaration fails the
				// run, so a case about that has no outputs to compare — only
				// the sentence, which must be the same one the durable driver
				// gives.
				require.Error(t, err, "the run was expected to fail")
				require.Contains(t, err.Error(), test.ExpectedErrorContains)

				return
			}
			require.NoError(t, err)
			require.Empty(t, cmp.Diff(test.ExpectedOutputs, out, protocmp.Transform()))
		})
	}
}

// TestRunWorkflowValue covers `value:` in the local driver.
//
// The engine package runs the identical [conformance.ValueCases] against the durable
// driver. That pairing is the point of the set rather than a habit: a value's
// whole observable behaviour is the answer it computed and the name it computed
// it under, so a driver evaluating it in a different scope, at a different
// moment, or storing it anywhere but `steps.<id>.value` would make a local
// rehearsal quietly wrong about production. One shared [v1.EvalValueNode] is what
// keeps them together; these are what prove it is what both of them reach.
func TestRunWorkflowValue(t *testing.T) {
	for _, test := range conformance.ValueCases() {
		t.Run(test.Name, func(t *testing.T) {
			out, err := v1.RunWithInputs(t.Context(), test.Workflow, test.Inputs)
			if test.ExpectFailure {
				require.Error(t, err, "reading a value that never ran was expected to fail the run")
				return
			}
			require.NoError(t, err)
			require.Empty(t, cmp.Diff(test.ExpectedOutputs, out, protocmp.Transform()))
		})
	}
}

// TestRunWorkflowInterpolation covers a scalar mixing text with ${...} in the
// local driver.
//
// The engine package runs the identical [conformance.InterpolationCases] against the
// durable driver. Interpolation itself is compiled away before either driver
// sees a workflow, which is the design; what the pairing holds is the part that
// is not compiled away, the `string()` each driver's own evaluator runs over
// every fence. See the set's doc for why a rendering that differed between them
// is invariant 3 broken in the direction an author would notice last.
func TestRunWorkflowInterpolation(t *testing.T) {
	for _, test := range conformance.InterpolationCases() {
		t.Run(test.Name, func(t *testing.T) {
			out, err := v1.RunWithInputs(t.Context(), test.Workflow, test.Inputs)
			require.NoError(t, err)
			require.Empty(t, cmp.Diff(test.ExpectedOutputs, out, protocmp.Transform()))
		})
	}
}

// TestRunWorkflowWebhookTrigger covers a declared `triggers:` webhook in the
// local driver.
//
// The engine package runs the identical [conformance.WebhookTriggerCases] against the
// durable driver, and the pairing is the whole point of the set: what a trigger
// declaration does to a run is *nothing*, on both drivers, and a rehearsal that
// disagreed with production about that would be a rehearsal of a different file.
func TestRunWorkflowWebhookTrigger(t *testing.T) {
	for _, test := range conformance.WebhookTriggerCases() {
		t.Run(test.Name, func(t *testing.T) {
			out, err := v1.RunWithInputs(t.Context(), test.Workflow, test.Inputs)
			if test.ExpectFailure {
				require.Error(t, err, "the case expected the run to fail")
				return
			}
			require.NoError(t, err)
			require.Empty(t, cmp.Diff(test.ExpectedOutputs, out, protocmp.Transform()))
		})
	}
}

// TestRunWorkflowTriggerContext covers reading `trigger` in the local driver.
//
// The engine package runs the identical [conformance.TriggerContextCases] against the
// durable driver, and the pairing is the substance of this feature rather than a
// formality: here the context arrives on a context value and there it arrives in
// [v1.RunState], crosses the wire and survives Continue-As-New. Two routes, one
// answer, or `flow run local` is rehearsing a different file.
func TestRunWorkflowTriggerContext(t *testing.T) {
	for _, test := range conformance.TriggerContextCases() {
		t.Run(test.Name, func(t *testing.T) {
			ctx := t.Context()
			if test.Trigger != nil {
				ctx = v1.NewContextWithTrigger(ctx, test.Trigger)
			}

			out, err := v1.RunWithInputs(ctx, test.Workflow, test.Inputs)
			if test.ExpectFailure {
				require.Error(t, err, "the case expected the run to fail")
				return
			}
			require.NoError(t, err)
			require.Empty(t, cmp.Diff(test.ExpectedOutputs, out, protocmp.Transform()))
		})
	}
}

// TestRunWorkflowWebhookDelivery covers a run started by a delivery in the local
// driver.
//
// The engine package runs the identical [conformance.WebhookDeliveryCases] against the
// durable driver. What the pairing is for is the *values*: a delivery's inputs
// come out of a JSON payload rather than off a command line, so they are the one
// set of inputs whose Go types nobody wrote down, and a driver that read a
// payload's number differently would refuse a run its rehearsal accepted.
func TestRunWorkflowWebhookDelivery(t *testing.T) {
	for _, test := range conformance.WebhookDeliveryCases() {
		t.Run(test.Name, func(t *testing.T) {
			require.NotNil(t, test.Inputs, "the delivery did not bind, so there is nothing to run")

			out, err := v1.RunWithInputs(t.Context(), test.Workflow, test.Inputs)
			if test.ExpectFailure {
				require.Error(t, err, "the case expected the run to fail")
				return
			}
			require.NoError(t, err)
			require.Empty(t, cmp.Diff(test.ExpectedOutputs, out, protocmp.Transform()))
		})
	}
}

// TestRunWorkflowSwitch covers `switch:` in the local driver.
//
// The engine package runs the identical [conformance.SwitchCases] against the durable
// driver. The pairing is the point: which branch a value takes, what the record
// says, and that an unresolvable discriminant fails rather than defaulting are
// the whole observable surface of a dispatch, and every one is decided by the
// one [v1.SelectSwitchCase] both drivers call.
func TestRunWorkflowSwitch(t *testing.T) {
	for _, test := range conformance.SwitchCases() {
		t.Run(test.Name, func(t *testing.T) {
			out, err := v1.RunWithInputs(t.Context(), test.Workflow, test.Inputs)
			if test.ExpectFailure {
				require.Error(t, err, "the case expected the run to fail")
				return
			}
			require.NoError(t, err)
			if test.ExpectedOutputsPredicate != nil {
				require.True(t, test.ExpectedOutputsPredicate(out), "outputs predicate failed: %v", out)
				return
			}
			require.Empty(t, cmp.Diff(test.ExpectedOutputs, out, protocmp.Transform()))
		})
	}
}

// TestRunWorkflowAsync covers `async:` in the local driver.
//
// The engine package runs the identical [conformance.AsyncCases] against the durable
// driver, and the pairing is the point rather than a convention: this driver
// runs an async step's work where it is written and holds the result until the
// join, where the durable one genuinely overlaps it — so every observable a
// case names (which output appears where, which failure is heard where, what a
// mention joins) is exactly what could drift between the two.
func TestRunWorkflowAsync(t *testing.T) {
	baseURL := conformance.NewHTTPServer(t)
	for _, test := range conformance.AsyncCases(baseURL) {
		t.Run(test.Name, func(t *testing.T) {
			out, err := v1.RunWithInputs(t.Context(), test.Workflow, test.Inputs)
			if test.ExpectFailure {
				require.Error(t, err, "the case expected the run to fail")

				return
			}
			require.NoError(t, err)
			if test.ExpectedOutputsPredicate != nil {
				require.True(t, test.ExpectedOutputsPredicate(out), "outputs predicate failed: %v", out)

				return
			}
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
	for _, test := range conformance.InputRefusalCases() {
		t.Run(test.Name, func(t *testing.T) {
			_, err := v1.RunWithInputs(t.Context(), test.Workflow, test.Inputs)
			require.Error(t, err, "the submission was accepted")
			require.Contains(t, err.Error(), test.Contains)
		})
	}
}

// TestRunWorkflowOutputShaping covers a shaped `outputs:` mapping in the local
// driver.
//
// Paired with the identically named test in the engine package, per the rule the
// shared package exists for. The mapping form and the older fenced map literal
// take different paths inside the http task, so "they are the same shaping" is a
// claim about execution, and it has to be made on both drivers or it is a claim
// about whichever one happened to be run.
func TestRunWorkflowOutputShaping(t *testing.T) {
	baseURL := conformance.NewHTTPServer(t)
	for _, test := range conformance.OutputShapingCases(baseURL) {
		t.Run(test.Name, func(t *testing.T) {
			out, err := v1.Run(t.Context(), test.Workflow)
			require.NoError(t, err)
			require.Empty(t, cmp.Diff(test.ExpectedOutputs, out, protocmp.Transform()))
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
	baseURL := conformance.NewHTTPServer(t)
	for _, test := range conformance.ResponseScopeCases(baseURL) {
		t.Run(test.Name, func(t *testing.T) {
			ctx := t.Context()
			if test.Trigger != nil {
				ctx = v1.NewContextWithTrigger(ctx, test.Trigger)
			}

			out, err := v1.Run(ctx, test.Workflow)
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
	for _, test := range conformance.LogCases() {
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
	baseURL := conformance.NewHTTPServer(t)
	for _, test := range conformance.ErrorTextCases(baseURL) {
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
	for _, test := range conformance.ToleratedStepFailureCases() {
		t.Run(test.Name, func(t *testing.T) {
			out, err := v1.Run(t.Context(), test.Workflow)
			require.NoError(t, err)
			require.Empty(t, cmp.Diff(test.ExpectedOutputs, out, protocmp.Transform()))
		})
	}
}

// TestRunWorkflowPartialTranscript is the local half of what a failed run hands
// back about what it did (issue #453).
//
// The local driver is the one `flow test` runs on, so this is the half that
// decides whether coverage and `expect.ran` can see into a case that failed on
// purpose. The engine package runs the identical cases against the durable
// driver, which is the only thing that can say the two agree.
func TestRunWorkflowPartialTranscript(t *testing.T) {
	for _, test := range conformance.PartialTranscriptCases() {
		t.Run(test.Name, func(t *testing.T) {
			out, err := v1.Run(t.Context(), test.Workflow)
			require.Error(t, err, "these cases fail on purpose")

			// Compared whole, not as a subset: a transcript carrying a step the
			// run never reached is as wrong as one missing a step it did, and it
			// is the direction that would silently credit coverage for a branch
			// nothing exercised.
			require.Empty(t, cmp.Diff(test.Expected, out, protocmp.Transform()))

			// A failed run has no answer, so it carries none. Asserted apart from
			// the diff above because it is a different claim about the same value:
			// the diff would also pass if `run_outputs` were compared into an
			// expectation that happened to be empty for another reason.
			require.Nil(t, out.GetRunOutputs(),
				"a run that failed produced no declared outputs")
		})
	}
}

func TestRunWorkflowNestedErrorText(t *testing.T) {
	for _, test := range conformance.NestedErrorTextCases() {
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
// engine/authority_test.go's runAuthorityCase runs the identical [conformance.AuthorityCase]
// through worker registration instead. That pairing is #116: before it, secret
// denial and containment were each proven once, by whichever driver's test file
// happened to add them, and nothing compared the two.
func runAuthorityCase(t *testing.T, test conformance.AuthorityCase) {
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
		conformance.AssertNoLeak(t, out, test.ContainmentValue)
	}
}

// TestAuthorityDenial runs the shared fail-closed and policy-denial cases
// against the local driver. The durable driver runs the same cases in
// engine/authority_test.go.
func TestAuthorityDenial(t *testing.T) {
	for _, test := range conformance.AuthorityDenialCases() {
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
	baseURL := conformance.NewHTTPServer(t)
	for _, test := range conformance.AuthorityContainmentCases(baseURL) {
		t.Run(test.Name, func(t *testing.T) {
			runAuthorityCase(t, test)
		})
	}
}

// TestCleartextCredential runs #963 half one's shared cases — the http task
// refusing a bearer secret or a JIT federation credential sent to a
// non-loopback http:// destination, and the same credential succeeding over
// https — against the local driver. The durable driver runs the same cases
// in engine/authority_test.go.
func TestCleartextCredential(t *testing.T) {
	tlsServer := conformance.NewTLSCredentialServer(t)
	tlsServer.InstallTrustingHTTPTask(t)

	for _, test := range conformance.CleartextCredentialCases(tlsServer.URL) {
		t.Run(test.Name, func(t *testing.T) {
			runAuthorityCase(t, test)
			if test.Authority.ProviderCalls != nil {
				require.Zero(t, test.Authority.ProviderCalls.Load(),
					"the fixture secret provider was consulted for a request the cleartext refusal should have stopped first")
			}
			if test.Authority.Federation != nil && test.Authority.Federation.ExchangeCalls != nil {
				require.Zero(t, test.Authority.Federation.ExchangeCalls.Load(),
					"the fixture broker exchanged a credential for a request the cleartext refusal should have stopped first")
			}
		})
	}
}

// TestRunWorkflowUndo is the local half of the saga cases.
//
// The engine package runs the identical [conformance.UndoCases] against the durable
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
	for index, outline := range conformance.UndoCases(undoPlaceholderBase) {
		t.Run(outline.Name, func(t *testing.T) {
			base, recorded := conformance.NewUndoServer(t)
			test := conformance.UndoCases(base)[index]

			_, err := v1.Run(t.Context(), test.Workflow)
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

// TestRunWorkflowUndoCall is the local half of the compose-through cases: a
// callee's compensations must register onto the caller's own undo stack and run
// in reverse across the `call:` boundary exactly as [TestRunWorkflowUndo]'s
// top-level cases do within one level. The engine package runs the identical
// [conformance.UndoCallCases] against the durable driver.
func TestRunWorkflowUndoCall(t *testing.T) {
	for index, outline := range conformance.UndoCallCases(undoPlaceholderBase) {
		t.Run(outline.Name, func(t *testing.T) {
			base, recorded := conformance.NewUndoServer(t)
			test := conformance.UndoCallCases(base)[index]

			_, err := v1.Run(t.Context(), test.Workflow)
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

// TestRunWorkflowUndoLoop is the local half of the loop cases — issue #253's
// decision that a `loop:` body is a place a compensation may be written, and that
// a `call:` from one composes onto the same run-level stack.
//
// What makes it worth running locally as well as durably is what the whole shared
// package is for: an author rehearsing a progressive rollout on a laptop has to be
// told the order production will unwind in. The engine package runs the identical
// [conformance.UndoLoopCases] against the durable driver.
func TestRunWorkflowUndoLoop(t *testing.T) {
	for index, outline := range conformance.UndoLoopCases(undoPlaceholderBase) {
		t.Run(outline.Name, func(t *testing.T) {
			base, recorded := conformance.NewUndoServer(t)
			test := conformance.UndoLoopCases(base)[index]

			_, err := v1.Run(t.Context(), test.Workflow)
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

// TestRunWorkflowUndoOnCancellation is the local half of the cancellation cases.
//
// The engine package runs the identical [conformance.UndoCancellationCases]. This is the
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
	for index, outline := range conformance.UndoCancellationCases(undoPlaceholderBase) {
		t.Run(outline.Name, func(t *testing.T) {
			base, recorded := conformance.NewUndoServer(t)
			test := conformance.UndoCancellationCases(base)[index]

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

			conformance.AssertCancellationRecorded(t, test, recorded())
		})
	}
}

// TestRunWorkflowCancellationCauseIsDistinguishable is issue #520's own
// cancellation case: `context.WithCancelCause` was unused across the tree, so
// every cancelled run read as the same "context canceled" whatever actually
// stopped it. Two runs stopped for two different reasons — a stand-in for a
// CEL cost limit and for [UndoBudget] running out — must produce two
// different failure messages, and `errors.Is(err, context.Canceled)` must
// still hold for both, which is what a caller distinguishing a stopped run
// from a failed one relies on (see [UndoRunError]).
func TestRunWorkflowCancellationCauseIsDistinguishable(t *testing.T) {
	runCancelledWithCause := func(t *testing.T, cause error) error {
		t.Helper()

		base, recorded := conformance.NewUndoServer(t)
		workflow := conformance.UndoCancellationCases(base)[0].Workflow

		ctx, cancel := context.WithCancelCause(t.Context())
		defer cancel(nil)

		done := make(chan error, 1)
		go func() {
			_, err := v1.Run(ctx, workflow)
			done <- err
		}()

		require.Eventually(t, func() bool {
			return slices.Contains(recorded(), "z")
		}, 30*time.Second, time.Millisecond,
			"the run never reached the step it was to be cancelled at")

		cancel(cause)

		select {
		case err := <-done:
			return err
		case <-time.After(30 * time.Second):
			t.Fatal("a cancelled run did not stop")
			return nil
		}
	}

	errA := runCancelledWithCause(t, errors.New("cel cost limit of 1000000 exceeded"))
	errB := runCancelledWithCause(t, errors.New("compensation budget for this cancelled run ran out"))

	require.ErrorIs(t, errA, context.Canceled, "a cancelled run must still read as cancelled")
	require.ErrorIs(t, errB, context.Canceled, "a cancelled run must still read as cancelled")

	require.ErrorContains(t, errA, "cel cost limit of 1000000 exceeded")
	require.ErrorContains(t, errB, "compensation budget for this cancelled run ran out")
	require.NotEqual(t, errA.Error(), errB.Error(),
		"two different cancellation reasons produced the same failure text")
}

// TestRunWorkflowCancellationCauseIsNotDoubled is the doubling the run-level
// fallback ([eval.go]'s cancellation branch) could reintroduce once
// [runStepWithPolicy] started enriching a cancellation with a cause of its
// own: a run cancelled while a step is waiting out its retry backoff already
// has a named cause by the time it reaches the fallback, and the fallback
// must not name it a second time.
//
// The step is made to be waiting on its retry backoff, rather than mid-attempt,
// because that is the deterministic half of "executing or waiting to retry" —
// mid-attempt would race an in-flight HTTP request against the cancellation
// with no signal to synchronize on. A five-second interval gives the
// cancellation, sent the moment the first request is observed to have
// arrived, a wide and reliable window to land inside the sleep.
//
// The exact text is asserted, not merely that "maintenance" appears once:
// `require.Contains` would pass on the doubled "context canceled: maintenance:
// maintenance" just as readily as on the single, correct rendering, since both
// contain the substring once each without overlap being checked.
func TestRunWorkflowCancellationCauseIsNotDoubled(t *testing.T) {
	// Not parallel, for the reason TestAStepWithNoRetryBlockUsesTheSharedDefault
	// gives: reaching a test server means swapping the http task's egress policy
	// process-wide.
	allowLoopback(t)

	requested := make(chan struct{}, 1)
	failing := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		select {
		case requested <- struct{}{}:
		default:
		}
		w.WriteHeader(http.StatusInternalServerError)
	}))
	t.Cleanup(failing.Close)

	workflow := &v1.Workflow{
		Name:    "doubling-probe",
		Profile: v1.CurrentProfile,
		Steps: []*v1.Node{{
			Id: "a",
			Kind: &v1.Node_Task{Task: &v1.Task{Name: "http", Inputs: map[string]*v1.Value{
				"url": v1.NewLiteral(failing.URL),
			}}},
			Policy: &v1.StepPolicy{Retry: &v1.RetryPolicy{
				MaxAttempts:     3,
				InitialInterval: durationpb.New(5 * time.Second),
			}},
		}},
	}

	ctx, cancel := context.WithCancelCause(t.Context())
	defer cancel(nil)

	done := make(chan error, 1)
	go func() {
		_, err := v1.Run(ctx, workflow)
		done <- err
	}()

	select {
	case <-requested:
	case <-time.After(30 * time.Second):
		t.Fatal("the step never made its first request, so cancelling never lands in its retry backoff")
	}

	// `flow cancel`, arriving while the step is asleep between attempts —
	// [runStepWithPolicy]'s own withCancellationCause call already names this
	// cause before the error ever reaches eval's run-level fallback.
	cancel(errors.New("maintenance"))

	var err error
	select {
	case err = <-done:
	case <-time.After(30 * time.Second):
		t.Fatal("a cancelled run did not stop")
	}

	require.Error(t, err, "a cancelled run reported success")
	require.ErrorIs(t, err, context.Canceled,
		"a stopped run stopped reading as cancelled: %v", err)

	require.Equal(t, `step "a": context canceled: maintenance`, err.Error(),
		"the cancellation cause was named more than once")
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
	base, _ := conformance.NewUndoServer(t)

	for _, test := range conformance.UndoPlacementCases(base) {
		t.Run(test.Name, func(t *testing.T) {
			_, err := v1.Run(t.Context(), test.Workflow)
			require.Error(t, err, "a compensation the engine cannot honour was accepted")
			require.Contains(t, err.Error(), "`undo:` is only supported on",
				"the refusal does not say what is wrong with where the compensation is written")
		})
	}
}
