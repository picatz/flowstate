package flowfile_test

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"

	v1 "github.com/picatz/flowstate/pkg/flowstate/v1"
	"github.com/picatz/flowstate/pkg/flowstate/v1/flowfile"
)

// `step_defaults:` states timeout, total_timeout and retry once. The compiler
// resolves them into each eligible step, so these tests read the resolved
// policies, and the round trip proves `flow fmt` keeps the block factored.
const stepDefaultsSource = `edition: v2026.4
name: bounded
step_defaults:
  timeout: 30s
  retry:
    attempts: 4
    interval: 2s
steps:
  - id: plain
    log:
      message: one
  - id: own_timeout
    timeout: 5s
    log:
      message: two
  - id: no_retry
    retry:
      attempts: 1
    log:
      message: three
  - id: engine_retry
    retry:
    log:
      message: four
  - id: tolerant
    continue_on_error: true
    log:
      message: five
  - id: gate
    value: ${1}
  - id: each
    for_each:
      items: ${[1, 2]}
      as: n
      steps:
        - id: inner
          log:
            message: six
`

func stepPolicy(t *testing.T, wf *v1.Workflow, id string) *v1.StepPolicy {
	t.Helper()

	var found *v1.StepPolicy
	v1.WalkNodes(wf.GetSteps(), v1.Walk{Node: func(node *v1.Node) {
		if node.GetId() == id {
			found = node.GetPolicy()
		}
	}})
	return found
}

func TestStepDefaultsResolveIntoEligibleSteps(t *testing.T) {
	t.Parallel()

	wf, err := flowfile.Unmarshal([]byte(stepDefaultsSource))
	require.NoError(t, err)

	plain := stepPolicy(t, wf, "plain")
	require.NotNil(t, plain)
	assert.Equal(t, 30*time.Second, plain.GetTimeout().AsDuration())
	assert.EqualValues(t, 4, plain.GetRetry().GetMaxAttempts())
	assert.Equal(t, 2*time.Second, plain.GetRetry().GetInitialInterval().AsDuration())

	// Per key: the step's timeout wins and the default retry still applies.
	own := stepPolicy(t, wf, "own_timeout")
	assert.Equal(t, 5*time.Second, own.GetTimeout().AsDuration())
	assert.EqualValues(t, 4, own.GetRetry().GetMaxAttempts())

	// A step's retry replaces the default whole: nothing of interval: 2s survives.
	noRetry := stepPolicy(t, wf, "no_retry")
	assert.EqualValues(t, 1, noRetry.GetRetry().GetMaxAttempts())
	assert.Nil(t, noRetry.GetRetry().GetInitialInterval())
	assert.Equal(t, 30*time.Second, noRetry.GetTimeout().AsDuration())

	// An empty retry: is the engine's own behaviour, the opt-out.
	engine := stepPolicy(t, wf, "engine_retry")
	require.NotNil(t, engine.GetRetry())
	assert.Zero(t, engine.GetRetry().GetMaxAttempts())

	// continue_on_error is not a default and is still the step's own.
	assert.False(t, stepPolicy(t, wf, "plain").GetContinueOnError())
	assert.True(t, stepPolicy(t, wf, "tolerant").GetContinueOnError())
	assert.Equal(t, 30*time.Second, stepPolicy(t, wf, "tolerant").GetTimeout().AsDuration())

	// A step that schedules no single activity takes nothing, and a step under a
	// for_each body takes the defaults.
	assert.Nil(t, stepPolicy(t, wf, "gate"))
	assert.Nil(t, stepPolicy(t, wf, "each"))
	assert.Equal(t, 30*time.Second, stepPolicy(t, wf, "inner").GetTimeout().AsDuration())

	assert.Equal(t, 30*time.Second, wf.GetStepDefaults().GetTimeout().AsDuration())
}

func TestStepDefaultsNoBlockChangesNothing(t *testing.T) {
	t.Parallel()

	wf, err := flowfile.Unmarshal([]byte("edition: v2026.4\nname: plain\nsteps:\n  - id: a\n    log:\n      message: hi\n"))
	require.NoError(t, err)
	assert.Nil(t, wf.GetStepDefaults())
	assert.Nil(t, stepPolicy(t, wf, "a"))
}

func TestStepDefaultsRoundTripStaysFactored(t *testing.T) {
	t.Parallel()

	wf, err := flowfile.Unmarshal([]byte(stepDefaultsSource))
	require.NoError(t, err)

	written, err := flowfile.Marshal(wf)
	require.NoError(t, err)

	// Said once at the top, and not repeated on the step that takes it.
	assert.Equal(t, 1, strings.Count(string(written), "timeout: 30s"), string(written))
	assert.Equal(t, 1, strings.Count(string(written), "attempts: 4"), string(written))
	assert.Contains(t, string(written), "step_defaults:")

	again, err := flowfile.Unmarshal(written)
	require.NoError(t, err)
	assert.True(t, proto.Equal(wf, again), "the written file compiles to a different workflow")

	second, err := flowfile.Marshal(again)
	require.NoError(t, err)
	assert.Equal(t, string(written), string(second), "formatting is not stable")
}

func TestStepDefaultsAreRefusedWhereTheyMeanNothingOrTooMuch(t *testing.T) {
	t.Parallel()

	const steps = "steps:\n  - id: a\n    log:\n      message: hi\n"
	header := "edition: v2026.4\nname: x\n"

	for name, test := range map[string]struct {
		source string
		want   string
	}{
		"continue_on_error is not a default": {
			source: header + "step_defaults:\n  continue_on_error: true\n" + steps,
			want:   "continue_on_error",
		},
		"an empty block": {
			source: header + "step_defaults: {}\n" + steps,
			want:   "declares nothing",
		},
		"an unknown key": {
			source: header + "step_defaults:\n  async: true\n" + steps,
			want:   "async",
		},
		"a total shorter than the timeout beside it": {
			source: header + "step_defaults:\n  timeout: 1m\n  total_timeout: 10s\n" + steps,
			want:   "shorter than timeout",
		},
		"an inherited total shorter than a step's own timeout": {
			source: header + "step_defaults:\n  total_timeout: 10s\nsteps:\n  - id: a\n    timeout: 1m\n    log:\n      message: hi\n",
			want:   "shorter than its timeout",
		},
		"a file with no steps": {
			source: header + "step_defaults:\n  timeout: 5s\ntypes:\n  Port:\n    type: int\n",
			want:   "no `steps:`",
		},
	} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()

			_, err := flowfile.Unmarshal([]byte(test.source))
			require.Error(t, err)
			assert.Contains(t, err.Error(), test.want)
		})
	}
}

func TestStepDefaultsOnAHandBuiltSpecAreHeldToTheGrammar(t *testing.T) {
	t.Parallel()

	wf, err := flowfile.Unmarshal([]byte(stepDefaultsSource))
	require.NoError(t, err)
	assert.Empty(t, flowfile.Validate(wf))

	wf.StepDefaults.ContinueOnError = true
	diagnostics := flowfile.Validate(wf)
	require.NotEmpty(t, diagnostics)
	assert.Contains(t, diagnostics[0].Message, "continue_on_error")
}

// A call is another file: its steps take that file's defaults, not the caller's.
func TestStepDefaultsDoNotCrossACall(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	require.NoError(t, os.WriteFile(filepath.Join(dir, "child.yaml"), []byte(
		"edition: v2026.4\nname: child\nsteps:\n  - id: work\n    log:\n      message: hi\n"), 0o600))
	require.NoError(t, os.WriteFile(filepath.Join(dir, "parent.yaml"), []byte(
		"edition: v2026.4\nname: parent\nstep_defaults:\n  timeout: 30s\nsteps:\n  - id: sub\n    call: ./child.yaml\n  - id: mine\n    log:\n      message: hi\n"), 0o600))

	wf, _, err := flowfile.ParseFile(filepath.Join(dir, "parent.yaml"))
	require.NoError(t, err)

	assert.Equal(t, 30*time.Second, stepPolicy(t, wf, "mine").GetTimeout().AsDuration())
	assert.Nil(t, stepPolicy(t, wf, "sub"))
	assert.Nil(t, wf.GetSteps()[0].GetCall().GetWorkflow().GetSteps()[0].GetPolicy(),
		"the callee's step took the caller's defaults")
}
