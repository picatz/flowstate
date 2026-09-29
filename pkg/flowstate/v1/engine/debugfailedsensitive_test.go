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

// TestTheDurableDriverWithholdsTheCorpussFailedSensitive is the durable half
// of [conformance.FailedSensitiveCase]: the same programs, run with a session
// attached until they fail, report every failed step without the secret.
func TestTheDurableDriverWithholdsTheCorpussFailedSensitive(t *testing.T) {
	t.Parallel()

	cases := conformance.FailedSensitiveCases()
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
			require.Error(t, tl.env.GetWorkflowError())

			failed := map[string]string{}
			for _, observation := range querySnapshot(t, tl.env, "").GetObservations() {
				if observation.GetKind() == v1.DebugObservationKind_DEBUG_OBSERVATION_KIND_FAILED {
					failed[observation.GetStepId()] = observation.GetText()
				}
			}
			for _, step := range test.Failed {
				require.Contains(t, failed, step, "no failure was reported for %s", step)
				assert.Contains(t, failed[step], test.Quoted, "%s's report does not quote its error, so this proves nothing", step)
				assert.NotContains(t, failed[step], test.Secret, "%s's report showed the secret", step)
			}
		})
	}
}
