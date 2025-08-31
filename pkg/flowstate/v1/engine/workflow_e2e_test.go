package engine_test

import (
	"testing"

	"github.com/google/go-cmp/cmp"
	v1 "github.com/picatz/flowstate/pkg/flowstate/v1"
	"github.com/picatz/flowstate/pkg/flowstate/v1/engine"
	"github.com/stretchr/testify/require"
	"go.temporal.io/sdk/client"
	"go.temporal.io/sdk/testsuite"
	"go.temporal.io/sdk/worker"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/testing/protocmp"
)

// TestRun_E2E_CEL_ContinueAsNew spins up a Temporal dev server + worker and
// verifies that CEL tasks evaluate correctly across Continue-As-New boundaries.
// It also demonstrates that only the minimal previous outputs are required for
// CEL evaluation by successfully evaluating expressions that reference prior
// step outputs after multiple continues.
func TestRun_E2E_CEL_ContinueAsNew(t *testing.T) {
	t.Parallel()

	devServer, err := testsuite.StartDevServer(t.Context(), testsuite.DevServerOptions{
		ClientOptions: &client.Options{},
	})
	require.NoError(t, err)
	t.Cleanup(func() { _ = devServer.Stop() })

	// Start worker and register our workflow + activities
	w := worker.New(devServer.Client(), engine.RunTaskQueueName, worker.Options{})
	w.RegisterWorkflow(engine.Run)
	w.RegisterActivity(engine.Task)
	w.RegisterActivity(engine.TaskWithPrev)
	require.NoError(t, w.Start())
	t.Cleanup(w.Stop)

	// Build a workflow with alternating CEL and echo steps referencing previous
	// outputs so we exercise TaskWithPrev multiple times across continues.
	wf := &v1.Workflow{
		Name: "e2e-cel-continue",
		Steps: []*v1.Node{
			// a: literal echo
			{Id: "a", Kind: &v1.Node_Task{Task: &v1.Task{Name: "echo", Inputs: map[string]*v1.Value{
				"message": v1.NewLiteral("hi"),
			}}}},
			// b: CEL references a.result
			{Id: "b", Kind: &v1.Node_Task{Task: &v1.Task{Name: "cel", Inputs: map[string]*v1.Value{
				"expr": v1.NewLiteral("a.result + '!'"),
			}}}},
			// c: echo references b.result via ${}
			{Id: "c", Kind: &v1.Node_Task{Task: &v1.Task{Name: "echo", Inputs: map[string]*v1.Value{
				"message": v1.NewExpr("b.result"),
			}}}},
			// d: CEL references c.result
			{Id: "d", Kind: &v1.Node_Task{Task: &v1.Task{Name: "cel", Inputs: map[string]*v1.Value{
				"expr": v1.NewLiteral("c.result + '!again'"),
			}}}},
		},
	}

	// Table of step budgets exercises Continue-As-New paths
	testCases := []struct {
		name   string
		budget int32
	}{
		{name: "budget=1", budget: 1},
		{name: "budget=2", budget: 2},
		{name: "budget=4", budget: 4}, // no continue
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			run, err := devServer.Client().ExecuteWorkflow(
				t.Context(),
				client.StartWorkflowOptions{
					ID:        "e2e-cel-continue-" + tc.name,
					TaskQueue: engine.RunTaskQueueName,
				},
				engine.Run,
				&v1.RunState{Workflow: wf, StepsBudget: tc.budget},
			)
			require.NoError(t, err)

			var got v1.Workflow_StepOutputs
			require.NoError(t, run.Get(t.Context(), &got))

			// Expected outputs depend on whether Continue-As-New trimmed prior
			// steps from the final carryover:
			// - budget=1 => final outputs include steps {c, d}
			// - budget=2 => final outputs include steps {b, c, d}
			// - budget>=4 => final outputs include steps {a, b, c, d}
			wantValues := map[string]*v1.Node_Outputs{}
			if tc.budget >= 4 {
				wantValues["a"] = &v1.Node_Outputs{NamedValues: map[string]*v1.Value{"result": v1.NewLiteral("hi")}}
			}
			if tc.budget >= 2 {
				wantValues["b"] = &v1.Node_Outputs{NamedValues: map[string]*v1.Value{"result": v1.NewLiteral("hi!")}}
			}
			// c and d are always present in the final run
			wantValues["c"] = &v1.Node_Outputs{NamedValues: map[string]*v1.Value{"result": v1.NewLiteral("hi!")}}
			wantValues["d"] = &v1.Node_Outputs{NamedValues: map[string]*v1.Value{"result": v1.NewLiteral("hi!!again")}}
			want := &v1.Workflow_StepOutputs{StepValues: wantValues}

			if !proto.Equal(&got, want) {
				t.Fatalf("outputs mismatch\n%s", cmp.Diff(want, &got, protocmp.Transform()))
			}
		})
	}
}
