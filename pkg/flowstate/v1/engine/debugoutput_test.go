package engine_test

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"

	v1 "github.com/picatz/flowstate/pkg/flowstate/v1"
	"github.com/picatz/flowstate/pkg/flowstate/v1/engine"
	"github.com/picatz/flowstate/pkg/flowstate/v1/internal/conformance"
)

// TestTheDurableDriverReportsTheCorpussOutputs is the durable half of
// [conformance.OutputCase]: the same programs, run with a session attached,
// give the same account of every step that finished.
func TestTheDurableDriverReportsTheCorpussOutputs(t *testing.T) {
	t.Parallel()

	cases := conformance.OutputCases()
	require.NotEmpty(t, cases, "the corpus is empty, so this asserts nothing")
	for _, test := range cases {
		t.Run(test.Name, func(t *testing.T) {
			t.Parallel()

			spec := proto.CloneOf(test.Workflow)
			spec.Debug = debugSpec(spec.GetName()).GetDebug()

			tl := newTimeline(t)
			const sre = "sre-1@example.com"
			tl.ask(0, sre, &v1.DebugAsk{Verb: v1.DebugVerbPause, Session: "s1", Request: "attach", Lease: 5 * time.Minute})
			tl.ask(2*time.Second, sre, &v1.DebugAsk{Verb: v1.DebugVerbResume, Session: "s1", Request: "on",
				Action: v1.DebugResumeAction_DEBUG_RESUME_ACTION_CONTINUE})

			tl.env.ExecuteWorkflow(engine.Run, &v1.RunState{Workflow: spec})
			require.True(t, tl.env.IsWorkflowCompleted())
			require.NoError(t, tl.env.GetWorkflowError())

			assert.Empty(t, test.Problems(querySnapshot(t, tl.env, "").GetObservations()))
		})
	}
}
