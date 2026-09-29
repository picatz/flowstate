package engine_test

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"

	v1 "github.com/picatz/flowstate/pkg/flowstate/v1"
	"github.com/picatz/flowstate/pkg/flowstate/v1/engine"
	"github.com/picatz/flowstate/pkg/flowstate/v1/internal/conformance"
)

// TestTheDurableDriverWithholdsTheCorpussHeldSensitive is the durable half of
// [conformance.HeldSensitiveCase]: the same program, held at the same place
// inside the callee as the local half, and the same value withheld from the
// same inspection.
func TestTheDurableDriverWithholdsTheCorpussHeldSensitive(t *testing.T) {
	t.Parallel()

	cases := conformance.HeldSensitiveCases()
	require.NotEmpty(t, cases, "the corpus is empty, so this asserts nothing")
	for _, test := range cases {
		t.Run(test.Name, func(t *testing.T) {
			t.Parallel()

			spec := proto.CloneOf(test.Workflow)
			spec.Debug = debugSpec(spec.GetName()).GetDebug()

			tl := newTimeline(t)
			const sre = "sre-1@example.com"
			// Asked before the run starts, so it holds at the first boundary,
			// where the local session holds on entry.
			tl.ask(0, sre, &v1.DebugAsk{Verb: v1.DebugVerbPause, Session: "s1", Request: "attach", Lease: 5 * time.Minute})
			tl.ask(2*time.Second, sre, &v1.DebugAsk{Verb: v1.DebugVerbResume, Session: "s1", Request: "until",
				Action: v1.DebugResumeAction_DEBUG_RESUME_ACTION_RUN_UNTIL, Until: test.Until})
			var held *v1.DebugSnapshot
			var inspected v1.DebugInspectResponse
			tl.env.RegisterDelayedCallback(func() {
				held = querySnapshot(t, tl.env, "")
				encoded, err := tl.env.QueryWorkflow(v1.DebugInspectQuery, &v1.DebugInspectRequest{
					SessionId: "s1", Revision: held.GetRevision(), Expression: test.Expression,
				})
				require.NoError(t, err)
				require.NoError(t, encoded.Get(&inspected))
			}, 3*time.Second)
			tl.ask(4*time.Second, sre, &v1.DebugAsk{Verb: v1.DebugVerbResume, Session: "s1", Request: "on",
				Action: v1.DebugResumeAction_DEBUG_RESUME_ACTION_CONTINUE})

			tl.env.ExecuteWorkflow(engine.Run, &v1.RunState{Workflow: spec})
			require.True(t, tl.env.IsWorkflowCompleted())
			require.NoError(t, tl.env.GetWorkflowError())

			require.Equal(t, v1.DebugRunState_DEBUG_RUN_STATE_HELD, held.GetState())
			require.Equal(t, test.HeldAt, held.GetOccurrence().GetAddress())
			require.Empty(t, inspected.GetError(), "the inspection failed, so this proves nothing")
			encoded, err := protojson.Marshal(&inspected)
			require.NoError(t, err)
			assert.NotContains(t, string(encoded), test.Secret)
		})
	}
}
