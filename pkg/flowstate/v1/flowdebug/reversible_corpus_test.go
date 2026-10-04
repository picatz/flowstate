package flowdebug_test

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	v1 "github.com/picatz/flowstate/pkg/flowstate/v1"
	"github.com/picatz/flowstate/pkg/flowstate/v1/flowfile"
)

// reversibleCorpus holds the shapes a replay must reproduce stop for stop,
// beyond the loop, parallel block and call of the journey: a failure the run
// tolerates and reads back, steps that depart from written order and join at
// the first read, and a wait.
var reversibleCorpus = map[string]string{
	"a tolerated failure that a later step reads": `edition: v2026.4
name: tolerated
steps:
  - id: before
    value: ${1}
  - id: shrugged
    value: ${1 / 0}
    continue_on_error: true
  - id: echoed
    value: ${steps.shrugged.error}
  - id: after
    value: ${steps.before.value}
`,
	"async steps that join where they are read": `edition: v2026.4
name: joined
steps:
  - id: build
    async: true
    log:
      message: building
  - id: provision
    async: true
    log:
      message: provisioning
  - id: test
    log:
      message: ${"testing " + string(size(steps.build))}
  - id: deploy
    log:
      message: ${"deploying " + string(size(steps.provision))}
`,
	"a sleep between steps": `edition: v2026.4
name: sleeping
steps:
  - id: before
    value: ${1}
  - id: nap
    sleep: 1ms
  - id: after
    value: ${2}
`,
}

// TestBackReproducesEveryShapeInTheCorpus walks each program to its end by
// single steps and goes back to the first stop, one stop at a time, requiring
// each to show exactly what it showed the first time, as the journey's test
// does for its own shapes.
func TestBackReproducesEveryShapeInTheCorpus(t *testing.T) {
	t.Parallel()

	for name, source := range reversibleCorpus {
		t.Run(name, func(t *testing.T) {
			t.Parallel()

			workflow, _, err := flowfile.Parse([]byte(source))
			require.NoError(t, err)
			run := newReversing(t, func(int) *v1.Workflow { return workflow }, nil)

			at := run.first()
			visited := []shown{shownAt(at)}
			for at.GetState() == v1.DebugRunState_DEBUG_RUN_STATE_HELD {
				at = run.move(at, v1.DebugResumeAction_DEBUG_RESUME_ACTION_STEP_IN)
				visited = append(visited, shownAt(at))
			}
			require.GreaterOrEqual(t, len(visited), 4, "the program was meant to have several stops")
			require.Equal(t, v1.DebugRunState_DEBUG_RUN_STATE_COMPLETED, at.GetState(),
				"the program was meant to complete, not fail at some stop on the way: %s", at.GetFailure())

			for i := len(visited) - 2; i >= 0; i-- {
				receipt, snapshot := run.back(0)
				require.Equal(t, appliedStatus, receipt.GetStatus(), "stop %d: %s", i, receipt.GetMessage())
				assert.Equal(t, visited[i], shownAt(snapshot), "stop %d shows something other than the first visit did", i)
			}
			assert.Equal(t, run.launches.started.Load()-1, run.launches.stopped.Load(), "a rewound run was left running")
		})
	}
}
