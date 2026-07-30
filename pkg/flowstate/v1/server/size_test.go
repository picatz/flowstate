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

// producing returns a step whose *output* is a large string, built by the step
// rather than written into the file.
//
// This is the shape the submit check cannot see, and it takes a request to make
// now: no task returns a value except `http`, so the size has to come back from
// somewhere. The loopback server generates it rather than echoing it, which keeps
// the specification tiny while the run's state grows — the asymmetry the test is
// about.
//
// Which is worth saying plainly, because it reads like a workaround and is not:
// this is now the *only* way a run's carried state grows at all. A workload that
// reads a large document is an http step, and its response is what it carries.
func producing(id, httpBaseURL string, size int) *v1.Node {
	return &v1.Node{
		Id: id,
		Kind: &v1.Node_Task{Task: &v1.Task{
			Name: "http",
			Inputs: map[string]*v1.Value{
				"method":  v1.NewLiteral("GET"),
				"url":     v1.NewLiteral(fmt.Sprintf("%s/bytes/%d", httpBaseURL, size)),
				"outputs": v1.NewExpr(`{"chunk": response.body}`),
			},
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
	startWorker(t, temporal)
	httpBaseURL := tests.NewHTTPServer(t)

	// A specification that is small, and a run that is not.
	//
	// This is the shape the submit check structurally cannot catch. A loop carries
	// the results of every iteration so far in its resume frame, so the state grows
	// with the number of iterations while the specification stays exactly the size
	// it was written at. Nothing weighable at submit predicts it — the item count
	// can even be an expression, computed at run time from a step's output.
	const iterations = 60
	items := make([]any, iterations)
	for i := range items {
		items[i] = "item"
	}

	spec := &v1.Workflow{
		Name: "grows-while-running",
		Steps: []*v1.Node{{
			Id: "each",
			Kind: &v1.Node_ForEach{ForEach: &v1.ForEach{
				Items:    v1.NewLiteralList(items...),
				Iterator: "one",
				Body:     []*v1.Node{producing("chunk", httpBaseURL, 40<<10)},
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
