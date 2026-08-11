package server_test

import (
	"context"
	"fmt"
	"strings"
	"testing"
	"time"

	"connectrpc.com/connect"
	"github.com/stretchr/testify/require"
	"go.temporal.io/sdk/client"
	"go.temporal.io/sdk/worker"
	"google.golang.org/protobuf/proto"

	v1 "github.com/picatz/flowstate/pkg/flowstate/v1"
	"github.com/picatz/flowstate/pkg/flowstate/v1/engine"
	"github.com/picatz/flowstate/pkg/flowstate/v1/server"
	"github.com/picatz/flowstate/pkg/flowstate/v1/tests"
)

// A run that cannot continue must fail, and these two tests are about the one
// state it must never end up in instead.
//
// Temporal refuses to store a payload past its blob limit. A Continue-As-New over
// that limit does not fail the run — it fails the *workflow task*, and a failed
// workflow task is retried. Indefinitely. The run reports RUNNING, climbs an
// attempt count nobody is watching, and takes a worker's attention on each try; it
// never completes and it never fails.
//
// That was measured before it was fixed: a 1.2 MiB specification submitted
// successfully, ran its first step, and was on attempt 5 of a workflow task
// forty-five seconds later, with a status any listing would show as healthy.

// bulky returns a step whose *specification* is large: a log line nobody would
// write, carrying a literal of the requested size.
//
// It weighs down the spec and nothing else. A `log` step has no outputs, so a run
// made of these grows exactly as much as the file does — which is the case the
// submit check is for.
func bulky(id string, size int) *v1.Node {
	return &v1.Node{
		Id: id,
		Kind: &v1.Node_Task{Task: &v1.Task{
			Name:   "log",
			Inputs: map[string]*v1.Value{"message": v1.NewLiteral(strings.Repeat(id, size))},
		}},
	}
}

// TestRunRefusesASpecificationTooLargeToExecute covers the moment an author is
// still there to be told.
//
// The size limit is not a schema rule and cannot be one — the schema describes
// what a workflow *is*, and this is about what the substrate underneath will
// store. So it is a separate check, and it has to report InvalidArgument: the
// request is the problem, and CodeInternal would tell a caller to retry something
// that will never succeed.
func TestRunRefusesASpecificationTooLargeToExecute(t *testing.T) {
	t.Parallel()

	temporal, _ := newTemporalNamespace(t)
	flowstate := server.New(temporal)

	spec := &v1.Workflow{
		Name:  "too-large",
		Steps: []*v1.Node{bulky("big", v1.MaxSpecBytes)},
	}

	// Schema-valid, and that is the point: nothing about it is malformed. No
	// length bound in the schema reaches a literal's string value, so this is a
	// workflow the validator is right to accept and the substrate cannot run.
	require.NoError(t, v1.Validate(&v1.RunRequest{Workflow: spec}),
		"the fixture stopped being schema-valid, so it no longer tests what it claims to")
	require.Greater(t, proto.Size(spec), v1.MaxSpecBytes)

	_, err := flowstate.Run(t.Context(), connect.NewRequest(&v1.RunRequest{Workflow: spec}))

	require.Error(t, err, "a specification too large to execute was accepted")
	require.Equal(t, connect.CodeInvalidArgument, connect.CodeOf(err),
		"reported as something other than the caller's problem: %v", err)

	// The message has to say what is wrong and what to do instead. Temporal's own
	// refusal for the same input is "Blob data size exceeds limit.", which is true
	// and leaves an author with nowhere to go.
	require.Contains(t, err.Error(), "step outputs",
		"the refusal does not explain that a run carries more than the specification: %v", err)
}

// TestARunTooLargeToCarryFailsRatherThanWedging is the backstop, and the reason
// the submit check is not enough on its own.
//
// Submission can only weigh the specification. What overflows is the
// specification *plus* everything the run accumulates — every step output a later
// step can still reach — and no check at submit knows how large those will be.
// This one runs where the answer is a fact rather than an estimate.
//
// The assertion is that the run reaches a terminal state at all. Before the fix
// this test does not fail, it hangs, which is exactly the defect: there is no
// outcome to assert against.
func TestARunTooLargeToCarryFailsRatherThanWedging(t *testing.T) {
	t.Parallel()

	temporal, _ := newTemporalNamespace(t)

	// This run's whole subject is a run state growing until it cannot be carried,
	// so every one of its workflow tasks legitimately handles a state near the
	// bound and continues as new, over and over. That is real non-yielding work,
	// and on a machine sharing itself with other suites it crosses the SDK's one
	// second deadlock budget: see [tests.BoundaryDeadlockDetectionTimeout] for why
	// this test raises it and why the raise costs the assertions below nothing.
	startWorkerWithOptions(t, temporal, worker.Options{
		DeadlockDetectionTimeout: tests.BoundaryDeadlockDetectionTimeout,
	})

	// A specification that is small, and a run that is not.
	//
	// This is the shape the submit check structurally cannot catch. A loop carries
	// its `state:` forward in the resume frame across every Continue-As-New, so a
	// state that grows each iteration grows the run while the specification stays
	// exactly the size it was written at. Nothing weighable at submit predicts it.
	//
	// The growth is folded through `update:` from a literal already in the spec,
	// rather than accumulated in `results`: a `for_each`'s and a `loop:`'s `results`
	// are each bounded in bytes at [v1.MaxLoopResultsBytes] (a quarter of the
	// run-state budget, #229 and its `for_each` sibling), so neither `results` field
	// can be the vehicle that carries a run past the *whole* run-state bound — it
	// would hit its own named bound first. A loop's `state:` is deliberately left
	// un-sub-bounded — the aggregate escape hatch #229 kept open — so it is what
	// reaches this backstop now, which is exactly the run-state check this test is
	// about. (This fixture used to grow a `for_each`'s `results`; that path is now
	// bounded before it can wedge, which is a different guarantee proven elsewhere.)
	const chunkBytes = 40 << 10
	big := strings.Repeat("x", chunkBytes)

	spec := &v1.Workflow{
		Name:    "grows-while-running",
		Profile: v1.CurrentProfile,
		Steps: []*v1.Node{{
			Id: "each",
			Kind: &v1.Node_Loop{Loop: &v1.Loop{
				State:   "acc",
				Initial: v1.NewLiteral(""),
				// Each iteration appends a spec-embedded chunk to the carried state,
				// so the state — and thus the run — grows ~40 KiB per iteration while
				// `results` stays empty (the body has no outputs). `until: false`
				// never stops it; the run-state bound does, which is the point.
				Update:        v1.NewExpr(fmt.Sprintf("acc + %q", big)),
				Until:         v1.NewExpr("false"),
				MaxIterations: 1000,
				Body: []*v1.Node{{
					Id: "tick",
					Kind: &v1.Node_Task{Task: &v1.Task{
						Name:   "log",
						Inputs: map[string]*v1.Value{"message": v1.NewLiteral("tick")},
					}},
				}},
			}},
		}},
	}

	require.NoError(t, v1.CheckSpecSize(spec),
		"the fixture is refused at submit, so it never reaches the check it is about")
	require.Less(t, proto.Size(spec), v1.MaxRunStateBytes/4,
		"the fixture only demonstrates anything if the specification itself is small")

	// A budget of one suspends at every iteration boundary, which is what a long
	// loop does on its own; here it just gets there sooner.
	run, err := temporal.ExecuteWorkflow(t.Context(), client.StartWorkflowOptions{
		ID:        "grows-" + t.Name(),
		TaskQueue: engine.RunTaskQueueName,
	}, engine.Run, &v1.RunState{Workflow: spec, StepsBudget: 1})
	require.NoError(t, err)

	// Bounded, because the failure this covers is an absence of any answer. A test
	// that waited forever would reproduce the bug rather than report it.
	ctx, cancel := context.WithTimeout(t.Context(), 90*time.Second)
	defer cancel()

	err = run.Get(ctx, nil)
	require.Error(t, err, "a run that cannot continue as new reported success")
	require.NotErrorIs(t, err, ctx.Err(),
		"the run never reached a terminal state; it is wedged retrying a workflow task")

	require.Contains(t, err.Error(), "continue as new",
		"the failure does not say why the run could not go on: %v", err)
}
