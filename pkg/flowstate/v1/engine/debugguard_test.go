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

// TestTheDurableDriverExplainsTheCorpussGuards is the durable half of
// [conformance.GuardCase]: the same programs, run with a session attached,
// give the same account of every `if:` that decided against its step.
func TestTheDurableDriverExplainsTheCorpussGuards(t *testing.T) {
	t.Parallel()

	cases := conformance.GuardCases()
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
			if test.Failed != "" {
				require.Error(t, tl.env.GetWorkflowError())
			} else {
				require.NoError(t, tl.env.GetWorkflowError())
			}

			var skipped []string
			failed := map[string]string{}
			observations := querySnapshot(t, tl.env, "").GetObservations()
			for _, observation := range observations {
				switch observation.GetKind() {
				case v1.DebugObservationKind_DEBUG_OBSERVATION_KIND_SKIPPED:
					skipped = append(skipped, observation.GetText())
				case v1.DebugObservationKind_DEBUG_OBSERVATION_KIND_FAILED:
					failed[observation.GetStepId()] = observation.GetText()
				}
			}
			assert.Equal(t, test.Skipped, skipped)
			for _, observation := range observations {
				if test.Secret != "" {
					assert.NotContains(t, observation.GetText(), test.Secret, "%s's account showed the secret", observation.GetStepId())
				}
			}
			if test.Failed == "" {
				assert.Empty(t, failed)
				return
			}
			require.Contains(t, failed, test.Failed, "the condition that could not be evaluated was not reported")
			assert.Contains(t, failed[test.Failed], test.Quoted)
		})
	}
}
