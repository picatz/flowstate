package engine_test

import (
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
	"time"

	v1 "github.com/picatz/flowstate/pkg/flowstate/v1"
	"github.com/picatz/flowstate/pkg/flowstate/v1/engine"
	"github.com/picatz/flowstate/pkg/flowstate/v1/netpolicy"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	"go.temporal.io/sdk/testsuite"
)

// The latency claim, which is the whole point of `async:` and the one thing the
// shared corpus deliberately does not carry.
//
// Every case in `conformance.AsyncCases` is about what an author can *see* — where an
// output appears, where a failure is heard — and both drivers must agree about
// all of it. Whether the work actually overlaps is a claim about this driver's
// scheduler and nothing else: the local driver runs an async step where it is
// written and holds the result, exactly as it runs a `parallel:` block's
// branches in order. So the claim is asserted here, against the real thing.

// TestRunWorkflowAsyncOverlapsLaterWork proves the N-graph runs as an N-graph:
// a step written after an async one runs while that async one is still in
// flight.
//
// The proof is a rendezvous rather than a clock. The async step's request blocks
// until the later step's request has arrived, so the run can only finish if the
// two were genuinely in flight together — under the sequential execution this
// replaces, the async step's handler would wait for a request that cannot be
// made until it returns, and the case would fail on the rendezvous timeout
// rather than on a threshold somebody had to pick. A bounded wait rather than an
// unbounded one so that a regression is a failed assertion here instead of a
// suite that hangs.
func TestRunWorkflowAsyncOverlapsLaterWork(t *testing.T) {
	var (
		mu      sync.Mutex
		arrived = map[string]chan struct{}{
			"slow":  make(chan struct{}),
			"later": make(chan struct{}),
		}
		overlapped bool
	)

	announce := func(name string) {
		mu.Lock()
		defer mu.Unlock()
		select {
		case <-arrived[name]:
		default:
			close(arrived[name])
		}
	}

	mux := http.NewServeMux()
	// The async step: announces itself, then refuses to answer until the step
	// written after it has been dispatched too.
	mux.HandleFunc("/slow", func(w http.ResponseWriter, r *http.Request) {
		announce("slow")
		select {
		case <-arrived["later"]:
			mu.Lock()
			overlapped = true
			mu.Unlock()
		case <-time.After(10 * time.Second):
		}
		w.Header().Set("Content-Type", "text/plain")
		_, _ = w.Write([]byte("slow"))
	})
	// The step written after it, which under written-order execution could not
	// be reached until the handler above had returned.
	mux.HandleFunc("/later", func(w http.ResponseWriter, r *http.Request) {
		announce("later")
		w.Header().Set("Content-Type", "text/plain")
		_, _ = w.Write([]byte("later"))
	})

	srv := httptest.NewServer(mux)
	t.Cleanup(srv.Close)
	allowLoopbackForAsync(t)

	get := func(id, path string, async bool) *v1.Node {
		return &v1.Node{
			Id:    id,
			Async: async,
			Kind: &v1.Node_Task{Task: &v1.Task{
				Name: "http",
				Inputs: map[string]*v1.Value{
					"url":     v1.NewLiteral(srv.URL + path),
					"method":  v1.NewLiteral(http.MethodGet),
					"outputs": v1.NewExpr(`{"said": response.body}`),
				},
			}},
		}
	}

	workflowSpec := &v1.Workflow{
		Name:    "async-overlap",
		Profile: v1.CurrentProfile,
		Steps: []*v1.Node{
			get("slow", "/slow", true),
			get("later", "/later", false),
		},
	}

	testSuite := &testsuite.WorkflowTestSuite{}
	env := atABound(testSuite.NewTestWorkflowEnvironment())
	env.RegisterWorkflow(engine.Run)
	env.OnActivity(engine.Task, mock.Anything, mock.Anything, mock.Anything).Return(engine.Task)
	env.OnActivity(engine.TaskInScope, mock.Anything, mock.Anything, mock.Anything).Return(engine.TaskInScope)
	env.OnActivity(engine.WorkflowVars, mock.Anything, mock.Anything).Return(engine.WorkflowVars)

	env.ExecuteWorkflow(engine.Run, &v1.RunState{Workflow: workflowSpec})
	require.True(t, env.IsWorkflowCompleted())
	require.NoError(t, env.GetWorkflowError())

	mu.Lock()
	defer mu.Unlock()
	require.True(t, overlapped,
		"the step written after an async one did not reach the server while it was still in flight, "+
			"so nothing was overlapped and `async:` bought no latency at all")
}

// allowLoopbackForAsync registers an http task permitting loopback for the
// duration of the test, restoring the original afterwards — the same exemption
// the shared corpus states for itself rather than weakening the shipped default.
func allowLoopbackForAsync(tb testing.TB) {
	tb.Helper()

	policy, err := netpolicy.New(netpolicy.WithAllowLoopback())
	require.NoError(tb, err, "building loopback egress policy")

	registry := v1.DefaultRegistry()
	original, existed := registry.Lookup("http")
	require.NoError(tb, registry.Register(v1.HTTPTaskDef(policy)))
	tb.Cleanup(func() {
		if existed {
			_ = registry.Register(original)
		}
	})
}
