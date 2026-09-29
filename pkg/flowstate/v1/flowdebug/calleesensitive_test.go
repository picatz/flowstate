package flowdebug_test

import (
	"strings"
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/encoding/protojson"

	v1 "github.com/picatz/flowstate/pkg/flowstate/v1"
	"github.com/picatz/flowstate/pkg/flowstate/v1/flowdebug"
)

const calleeSecret = "hunter2-callee-only-secret"

// calleeSensitiveFiles is a root that passes a literal to a callee declaring
// it sensitive, and itself declares nothing sensitive.
func calleeSensitiveFiles() map[string]string {
	child := `edition: v2026.3
name: child
inputs:
  api_key:
    type: string
    required: true
    sensitive: true
steps:
  - id: use
    log:
      message: ${"using " + inputs.api_key}
`
	root := `edition: v2026.3
name: parent
steps:
  - id: first
    value: ${1}
  - id: nested
    call: ./child.yaml
    with:
      api_key: ${"` + calleeSecret + `"}
`

	return map[string]string{"main.yaml": root, "child.yaml": child}
}

// TestAHoldInsideACalleeWithholdsItsSensitiveInputs is #2208: a value only a
// callee declares `sensitive: true` is withheld at a hold inside that callee,
// by the typed contract's inspection and the Driver's, as the durable
// driver withholds it — although the session was given no redactor of its
// own, and the root declares nothing sensitive.
func TestAHoldInsideACalleeWithholdsItsSensitiveInputs(t *testing.T) {
	t.Parallel()

	const secret = calleeSecret
	run := startDebugRun(t, "main.yaml", calleeSensitiveFiles(), nil)
	target := flowdebug.Target(run.session)
	at := waitHeld(t, target, 0)
	require.Equal(t, "first", at.GetOccurrence().GetSite().GetPath()[0])

	at = move(t, target, at, v1.DebugResumeAction_DEBUG_RESUME_ACTION_RUN_UNTIL, "nested/use")
	require.Equal(t, "nested(child)/use", at.GetOccurrence().GetAddress())

	for _, expression := range []string{"inputs.api_key", "inputs", `"key: " + inputs.api_key`} {
		inspected, err := target.Inspect(t.Context(), &v1.DebugInspectRequest{
			Revision: at.GetRevision(), Expression: expression, Children: true,
		})
		require.NoError(t, err, expression)
		encoded, err := protojson.Marshal(inspected)
		require.NoError(t, err)
		assert.NotContains(t, string(encoded), secret, "%s showed the callee's sensitive input", expression)
	}

	result, err := flowdebug.NewDriver(run.session).Do(t.Context(), "inspect inputs.api_key")
	require.NoError(t, err)
	assert.NotContains(t, result.Text, secret, "the Driver's inspect showed the callee's sensitive input")
	assert.True(t, strings.Contains(result.Text, "[redacted]") || strings.Contains(result.Text, "withheld"),
		"the prompt's inspect did not say the value was withheld: %s", result.Text)

	final := move(t, target, at, v1.DebugResumeAction_DEBUG_RESUME_ACTION_CONTINUE, "")
	assert.Equal(t, v1.DebugRunState_DEBUG_RUN_STATE_COMPLETED, final.GetState())
	require.NoError(t, <-run.done)
}

// TestARevealedSessionShowsACalleesSensitiveInputs: a front that was
// authorized to show sensitive values (`--reveal-sensitive`, embed's
// RevealSensitive) says so with [flowdebug.Options.RevealSensitive], and a
// hold inside the callee then shows what it withholds otherwise.
func TestARevealedSessionShowsACalleesSensitiveInputs(t *testing.T) {
	t.Parallel()

	run := startDebugRun(t, "main.yaml", calleeSensitiveFiles(), func(opts *flowdebug.Options) { opts.RevealSensitive = true })
	target := flowdebug.Target(run.session)
	at := waitHeld(t, target, 0)
	at = move(t, target, at, v1.DebugResumeAction_DEBUG_RESUME_ACTION_RUN_UNTIL, "nested/use")

	inspected, err := target.Inspect(t.Context(), &v1.DebugInspectRequest{Revision: at.GetRevision(), Expression: "inputs.api_key"})
	require.NoError(t, err)
	assert.Contains(t, inspected.GetValue().GetRendered(), calleeSecret, "an authorized reveal withheld the value")

	move(t, target, at, v1.DebugResumeAction_DEBUG_RESUME_ACTION_CONTINUE, "")
	require.NoError(t, <-run.done)
}

// TestAHoldInsideACalleeWithholdsTheRootsSensitiveInputs: sensitivity belongs
// to a value's origin, so a root's sensitive input passed to a callee under a
// name the callee does not declare sensitive is still withheld at a hold
// there, as the durable driver withholds it.
func TestAHoldInsideACalleeWithholdsTheRootsSensitiveInputs(t *testing.T) {
	t.Parallel()

	const secret = "root-only-sensitive-value"
	root := `edition: v2026.3
name: parent
inputs:
  token:
    type: string
    sensitive: true
    default: ` + secret + `
steps:
  - id: first
    value: ${1}
  - id: nested
    call: ./child.yaml
    with:
      who: ${inputs.token}
`
	run := startDebugRun(t, "main.yaml", map[string]string{"main.yaml": root, "child.yaml": childFlowfile}, nil)
	target := flowdebug.Target(run.session)
	at := waitHeld(t, target, 0)
	at = move(t, target, at, v1.DebugResumeAction_DEBUG_RESUME_ACTION_RUN_UNTIL, "nested/greet")
	require.Equal(t, "nested(child)/greet", at.GetOccurrence().GetAddress())

	inspected, err := target.Inspect(t.Context(), &v1.DebugInspectRequest{Revision: at.GetRevision(), Expression: "inputs.who"})
	require.NoError(t, err)
	encoded, err := protojson.Marshal(inspected)
	require.NoError(t, err)
	assert.NotContains(t, string(encoded), secret, "the root's sensitive input was shown inside the callee")

	move(t, target, at, v1.DebugResumeAction_DEBUG_RESUME_ACTION_CONTINUE, "")
	require.NoError(t, <-run.done)
}

// TestAHoldInsideACalleeWithholdsAShortSensitiveValue: a sensitive container
// with a short descendant — `[7]` — has nothing a substring redactor can
// safely replace, so it is withheld by equality at the value seam, as
// flowtest's own value redactor withholds a root's.
func TestAHoldInsideACalleeWithholdsAShortSensitiveValue(t *testing.T) {
	t.Parallel()

	child := `edition: v2026.3
name: child
inputs:
  codes:
    type: list
    required: true
    sensitive: true
steps:
  - id: use
    log:
      message: hi
`
	root := `edition: v2026.3
name: parent
steps:
  - id: first
    value: ${1}
  - id: nested
    call: ./child.yaml
    with:
      codes: ${[7]}
`
	run := startDebugRun(t, "main.yaml", map[string]string{"main.yaml": root, "child.yaml": child}, nil)
	target := flowdebug.Target(run.session)
	at := waitHeld(t, target, 0)
	at = move(t, target, at, v1.DebugResumeAction_DEBUG_RESUME_ACTION_RUN_UNTIL, "nested/use")

	inspected, err := target.Inspect(t.Context(), &v1.DebugInspectRequest{Revision: at.GetRevision(), Expression: "inputs.codes"})
	require.NoError(t, err)
	assert.NotEqual(t, "[7]", inspected.GetValue().GetRendered(), "a short sensitive value was shown inside the callee")

	move(t, target, at, v1.DebugResumeAction_DEBUG_RESUME_ACTION_CONTINUE, "")
	require.NoError(t, <-run.done)
}

// TestAnArrivalInsideACalleeWithholdsItsSensitiveInputs: text rendered from a
// callee's scope at an arrival — a failure stop's error, a logpoint's
// message, a declined condition's error — withholds what the callee declares
// sensitive, as the hold's inspections do (Codex, #2209).
func TestAnArrivalInsideACalleeWithholdsItsSensitiveInputs(t *testing.T) {
	t.Parallel()

	child := `edition: v2026.3
name: child
inputs:
  api_key:
    type: string
    required: true
    sensitive: true
steps:
  - id: use
    log:
      message: hi
  - id: boom
    value: ${{"a":1}[inputs.api_key]}
`
	root := `edition: v2026.3
name: parent
steps:
  - id: first
    value: ${1}
  - id: nested
    call: ./child.yaml
    with:
      api_key: ${"` + calleeSecret + `"}
`
	run := startDebugRun(t, "main.yaml", map[string]string{"main.yaml": root, "child.yaml": child}, nil)
	target := flowdebug.Target(run.session)
	at := waitHeld(t, target, 0)

	_, err := target.ReplaceBreakpoints(t.Context(), &v1.DebugSetBreakpointsRequest{
		FailureMode: v1.DebugFailureMode_DEBUG_FAILURE_MODE_ALL,
		Breakpoints: []*v1.DebugBreakpoint{
			{Id: "log", Step: "nested/use", LogMessage: "key={inputs.api_key}"},
			{Id: "declined", Step: "nested/use", Condition: `{"a": 1}[inputs.api_key] > 0`},
		},
	})
	require.NoError(t, err)

	at = move(t, target, at, v1.DebugResumeAction_DEBUG_RESUME_ACTION_CONTINUE, "")
	require.Equal(t, v1.DebugStopReason_DEBUG_STOP_REASON_FAILURE, at.GetReason(), at.GetMessage())
	assert.Contains(t, at.GetFailure(), "no such key", "the failure does not quote its error, so this proves nothing: %s", at.GetFailure())
	assert.NotContains(t, at.GetFailure(), calleeSecret, "the failure stop showed the callee's sensitive input")

	// What the arrivals rendered: the logpoint's message and the declined
	// condition's notice and last error. The observer's own FAILED lines are
	// the transcript's rendering, which has no position (#2210).
	var rendered []string
	for _, observation := range at.GetObservations() {
		switch observation.GetKind() {
		case v1.DebugObservationKind_DEBUG_OBSERVATION_KIND_LOG, v1.DebugObservationKind_DEBUG_OBSERVATION_KIND_NOTICE:
			rendered = append(rendered, observation.GetText())
		}
	}
	for _, state := range at.GetBreakpoints() {
		rendered = append(rendered, state.GetLastError())
	}
	joined := strings.Join(rendered, "\n")
	assert.Contains(t, joined, "key=", "the logpoint did not record its message")
	assert.Contains(t, joined, "no such key", "the declined condition's error is not quoted, so this proves nothing")
	assert.NotContains(t, joined, calleeSecret, "an arrival's rendering showed the callee's sensitive input")

	move(t, target, at, v1.DebugResumeAction_DEBUG_RESUME_ACTION_CONTINUE, "")
	require.Error(t, <-run.done)
}

// TestThePromptsInspectInsideACalleeWithholdsItsSensitiveInputs is the #2208
// reproduction on the path it was found on: the prompt's own `inspect`, typed
// as a line at a hold inside the callee — what `flow test --debug`, the
// scripted MCP tool and [flowdebug.Session.Control] feed — rather than the
// typed contract's inspection the Driver asks (exact-head review).
func TestThePromptsInspectInsideACalleeWithholdsItsSensitiveInputs(t *testing.T) {
	t.Parallel()

	var mu sync.Mutex
	var printed strings.Builder
	run := startDebugRun(t, "main.yaml", calleeSensitiveFiles(), func(opts *flowdebug.Options) {
		opts.Emit = func(text string, _ flowdebug.Tone) {
			mu.Lock()
			defer mu.Unlock()
			printed.WriteString(text)
		}
	})
	target := flowdebug.Target(run.session)
	at := waitHeld(t, target, 0)
	at = move(t, target, at, v1.DebugResumeAction_DEBUG_RESUME_ACTION_RUN_UNTIL, "nested/use")
	require.Equal(t, "nested(child)/use", at.GetOccurrence().GetAddress())

	// The last fails, and its error quotes the value it could not find
	// (exact-head review).
	for _, line := range []string{"inspect inputs.api_key", `inspect "k:" + inputs.api_key`, "inspect inputs", `inspect {"a": 1}[inputs.api_key]`} {
		err := run.session.Control(t.Context(), line)
		require.NoError(t, err, line)
	}

	// Read once the run is over, so every answer has been printed.
	move(t, target, at, v1.DebugResumeAction_DEBUG_RESUME_ACTION_CONTINUE, "")
	require.NoError(t, <-run.done)

	mu.Lock()
	out := printed.String()
	mu.Unlock()
	assert.NotContains(t, out, calleeSecret, "the prompt's inspect showed the callee's sensitive input")
	assert.Equal(t, 4, strings.Count(out, "[redacted]"), "each inspection, and the failing one's error, did not say its value was withheld:\n%s", out)
	assert.Contains(t, out, "no such key", "the failing inspection printed no error, so this proves nothing:\n%s", out)
}

// TestAMissedUntilAppliedInsideACalleeWithholdsItsSensitiveInputs: an `until`
// applied at a hold inside a callee can quote a value only that callee
// declares sensitive, and the notice that it was never reached withholds it,
// as the durable driver's does (exact-head review, #2209).
func TestAMissedUntilAppliedInsideACalleeWithholdsItsSensitiveInputs(t *testing.T) {
	t.Parallel()

	var mu sync.Mutex
	var printed strings.Builder
	run := startDebugRun(t, "main.yaml", calleeSensitiveFiles(), func(opts *flowdebug.Options) {
		opts.Emit = func(text string, _ flowdebug.Tone) {
			mu.Lock()
			defer mu.Unlock()
			printed.WriteString(text)
		}
	})
	target := flowdebug.Target(run.session)
	at := waitHeld(t, target, 0)
	at = move(t, target, at, v1.DebugResumeAction_DEBUG_RESUME_ACTION_RUN_UNTIL, "nested/use")
	require.Equal(t, "nested(child)/use", at.GetOccurrence().GetAddress())

	// `use` is not reached again, so the run completes without stopping.
	require.NoError(t, run.session.Control(t.Context(), `until use if inputs.api_key != "`+calleeSecret+`"`))
	require.NoError(t, <-run.done)

	mu.Lock()
	out := printed.String()
	mu.Unlock()
	assert.Contains(t, out, "the run completed without stopping at", "the notice was not said, so this proves nothing:\n%s", out)
	assert.NotContains(t, out, calleeSecret, "the missed-until notice showed the callee's sensitive input")

	final, err := target.Snapshot(t.Context())
	require.NoError(t, err)
	encoded, err := protojson.Marshal(final)
	require.NoError(t, err)
	assert.NotContains(t, string(encoded), calleeSecret, "the recorded notice showed the callee's sensitive input")
}
