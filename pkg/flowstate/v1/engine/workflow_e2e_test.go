package engine_test

import (
	"net/http"
	"testing"

	"github.com/google/go-cmp/cmp"
	v1 "github.com/picatz/flowstate/pkg/flowstate/v1"
	"github.com/picatz/flowstate/pkg/flowstate/v1/engine"
	"github.com/picatz/flowstate/pkg/flowstate/v1/tests"
	"github.com/stretchr/testify/require"
	"go.temporal.io/sdk/client"
	"go.temporal.io/sdk/testsuite"
	"go.temporal.io/sdk/worker"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/testing/protocmp"
)

// TestRun_E2E_ExpressionsAcrossContinueAsNew spins up a Temporal dev server and a
// worker, and checks that an expression naming an earlier step still resolves after
// the run has suspended and resumed — repeatedly, and against the *trimmed* carryover
// rather than the whole history.
//
// It used to be named for the `cel` task, which it was never really about: `cel`
// retired at edition v2026.2 and the property survives it unchanged, because what is
// under test is the compaction that decides which prior outputs a suspending run
// carries forward. Carry too little and the resumed run cannot resolve an input it
// needs; carry everything and the point of suspending is lost. The budgets below walk
// the boundary from both sides.
//
// A step's value comes from the loopback echo server now that no task produces one:
// `http` posts a body and gets it back, so each step records a real `said` output the
// next step's expression reads. That the chain is four steps long is what makes the
// trimming visible — the expected sets differ per budget precisely because a step's
// output is dropped once nothing left to run still names it.
func TestRun_E2E_ExpressionsAcrossContinueAsNew(t *testing.T) {
	t.Parallel()

	baseURL := tests.NewHTTPServer(t)

	devServer, err := testsuite.StartDevServer(t.Context(), testsuite.DevServerOptions{
		ClientOptions: &client.Options{},
	})
	require.NoError(t, err)
	t.Cleanup(func() { _ = devServer.Stop() })

	// Start worker and register our workflow + activities
	w := worker.New(devServer.Client(), engine.RunTaskQueueName, worker.Options{})
	engine.Register(w)
	require.NoError(t, w.Start())
	t.Cleanup(w.Stop)

	echoes := func(id, body string) *v1.Node {
		return &v1.Node{
			Id: id,
			Kind: &v1.Node_Task{Task: &v1.Task{
				Name: "http",
				Inputs: map[string]*v1.Value{
					"method":  v1.NewLiteral(http.MethodPost),
					"url":     v1.NewLiteral(baseURL + "/echo"),
					"body":    v1.NewExpr(body),
					"outputs": v1.NewExpr(`{"said": response.body}`),
				},
			}},
		}
	}

	// Each step names the one before it, so every suspend has a reference that has to
	// survive it. The two that append a suffix make a lost carry visible as a wrong
	// string rather than only as a missing one.
	wf := &v1.Workflow{
		Name: "e2e-expressions-continue",
		Steps: []*v1.Node{
			echoes("a", `"hi"`),
			echoes("b", `a.said + "!"`),
			echoes("c", "b.said"),
			echoes("d", `c.said + "!again"`),
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
					ID:        "e2e-expressions-continue-" + tc.name,
					TaskQueue: engine.RunTaskQueueName,
				},
				engine.Run,
				&v1.RunState{Workflow: wf, StepsBudget: tc.budget},
			)
			require.NoError(t, err)

			var got v1.Workflow_StepOutputs
			require.NoError(t, run.Get(t.Context(), &got))

			said := func(value string) *v1.Node_Outputs {
				return &v1.Node_Outputs{NamedValues: map[string]*v1.Value{"said": v1.NewLiteral(value)}}
			}

			// Expected outputs depend on whether Continue-As-New trimmed prior
			// steps from the final carryover:
			// - budget=1 => final outputs include steps {c, d}
			// - budget=2 => final outputs include steps {b, c, d}
			// - budget>=4 => final outputs include steps {a, b, c, d}
			wantValues := map[string]*v1.Node_Outputs{}
			if tc.budget >= 4 {
				wantValues["a"] = said("hi")
			}
			if tc.budget >= 2 {
				wantValues["b"] = said("hi!")
			}
			// c and d are always present in the final run
			wantValues["c"] = said("hi!")
			wantValues["d"] = said("hi!!again")
			want := &v1.Workflow_StepOutputs{StepValues: wantValues}

			if !proto.Equal(&got, want) {
				t.Fatalf("outputs mismatch\n%s", cmp.Diff(want, &got, protocmp.Transform()))
			}
		})
	}
}
