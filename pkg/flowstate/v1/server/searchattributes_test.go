package server_test

import (
	"testing"
	"time"

	"connectrpc.com/connect"
	"github.com/stretchr/testify/require"

	v1 "github.com/picatz/flowstate/pkg/flowstate/v1"
	"github.com/picatz/flowstate/pkg/flowstate/v1/server"
)

// A mock ListWorkflow response can prove that [FlowstateServer.List] decodes a
// search attribute payload the way this package encodes one, but it cannot
// prove the two encodings actually agree — a mock is exactly as wrong as the
// test author, never more. This is the one test in the package that goes
// through Temporal's own visibility store end to end: register, run, and
// read back what actually got indexed.
//
// # Why this is the right place to gate on the dev server
//
// Registration is the part every other search-attribute test in this package
// cannot exercise at all, because [server.EnsureSearchAttributesRegistered]
// is a call to the real operator API — there is no mock for "the server
// accepted this Keyword attribute" that would be testing anything but the
// mock itself.
func TestSearchAttributesAreRegisteredIdempotentlyAndProjected(t *testing.T) {
	temporal, namespace := newTemporalNamespace(t)
	startWorker(t, temporal)

	require.NoError(t, server.EnsureSearchAttributesRegistered(t.Context(), temporal, namespace),
		"registering Flowstate's search attributes against a dev server namespace")

	// Idempotent: a second registration — a second `flow server` process, or
	// this same process restarting — must not be treated as a failure. See
	// [server.EnsureSearchAttributesRegistered]'s own doc for why ALREADY_EXISTS
	// is the expected response here, not an error.
	require.NoError(t, server.EnsureSearchAttributesRegistered(t.Context(), temporal, namespace),
		"registering the same search attributes twice must be a no-op, not an error")

	flowstateServer := server.New(temporal, server.WithSearchAttributesRegistered())

	const workflowName = "search-attribute-probe"

	started, err := flowstateServer.Run(t.Context(), connect.NewRequest(&v1.RunRequest{
		Workflow: &v1.Workflow{
			Name: workflowName,
			Steps: []*v1.Node{
				{
					Id: "a",
					Kind: &v1.Node_Task{Task: &v1.Task{
						Name:   "log",
						Inputs: map[string]*v1.Value{"message": v1.NewLiteral("hello")},
					}},
				},
			},
		},
	}))
	require.NoError(t, err)

	workflowID := started.Msg.GetWorkflowId()
	require.NotEmpty(t, workflowID)

	// Temporal indexes a new execution's search attributes asynchronously, so
	// this polls rather than asserting on the first request — the same shape
	// every other dev-server test in this package uses for "has Temporal
	// caught up yet".
	require.Eventually(t, func() bool {
		resp, err := flowstateServer.List(t.Context(), connect.NewRequest(&v1.ListRequest{
			Filter: `name == "` + workflowName + `"`,
		}))
		if err != nil {
			return false
		}

		for _, run := range resp.Msg.GetRuns() {
			if run.GetWorkflowId() == workflowID {
				// The point of going through the real store: this is
				// [FlowstateServer.List] decoding a payload Temporal itself
				// wrote, which a mocked ListWorkflow response cannot exercise.
				require.Equal(t, workflowName, run.GetName())
				return true
			}
		}
		return false
	}, 30*time.Second, 200*time.Millisecond,
		"a run started with search attributes registered was never found by a `name` filter")
}
