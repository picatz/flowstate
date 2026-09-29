package flowdebug_test

import (
	"fmt"
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
	// condition's notice and last error, and every step's account (#2210).
	var rendered []string
	for _, observation := range at.GetObservations() {
		rendered = append(rendered, observation.GetText())
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

	for _, line := range []string{"inspect inputs.api_key", `inspect "k:" + inputs.api_key`, "inspect inputs"} {
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
	assert.Equal(t, 3, strings.Count(out, "[redacted]"), "each inspection did not say its value was withheld:\n%s", out)
}

// TestAStepsAccountWithholdsWhatItsCalleeDeclaresSensitive is #2210: the
// account a session gives of each step as it finishes — printed, and recorded
// as an observation — withholds what the step's workflow declares sensitive,
// and so does the account of each call a callee's failure passes through, and
// the failed run's own message. A revealed session shows them.
func TestAStepsAccountWithholdsWhatItsCalleeDeclaresSensitive(t *testing.T) {
	t.Parallel()

	child := `edition: v2026.3
name: child
inputs:
  api_key:
    type: string
    required: true
    sensitive: true
steps:
  - id: echo
    value: ${inputs.api_key}
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
	for _, reveal := range []bool{false, true} {
		t.Run(fmt.Sprintf("reveal=%t", reveal), func(t *testing.T) {
			t.Parallel()

			var mu sync.Mutex
			var printed strings.Builder
			run := startDebugRun(t, "main.yaml", map[string]string{"main.yaml": root, "child.yaml": child}, func(opts *flowdebug.Options) {
				opts.RevealSensitive = reveal
				opts.Emit = func(text string, _ flowdebug.Tone) {
					mu.Lock()
					defer mu.Unlock()
					printed.WriteString(text)
				}
			})
			target := flowdebug.Target(run.session)
			at := waitHeld(t, target, 0)
			move(t, target, at, v1.DebugResumeAction_DEBUG_RESUME_ACTION_CONTINUE, "")
			runErr := <-run.done
			require.Error(t, runErr)

			final, err := target.Snapshot(t.Context())
			require.NoError(t, err)
			accounts := map[string]string{}
			for _, observation := range final.GetObservations() {
				accounts[observation.GetStepId()] = observation.GetText()
			}
			require.Contains(t, accounts["echo"], "-> value:", "the callee step's value was not recorded, so this proves nothing")
			for _, step := range []string{"boom", "nested"} {
				require.Contains(t, accounts[step], "no such key", "%s's failure was not recorded, so this proves nothing", step)
			}
			mu.Lock()
			out := printed.String()
			mu.Unlock()
			failure := run.session.FailureText(runErr)
			require.Contains(t, failure, "no such key", "the failure says nothing, so this proves nothing")

			shown := []string{out, accounts["echo"], accounts["boom"], accounts["nested"], final.GetMessage(), failure}
			for i, text := range shown {
				if reveal {
					assert.Contains(t, text, calleeSecret, "rendering %d withheld what an authorized reveal shows", i)
				} else {
					assert.NotContains(t, text, calleeSecret, "rendering %d showed the callee's sensitive input", i)
				}
			}
		})
	}
}

// TestALongSensitiveValueInACalleesAccountIsWithheldBeforeTheCap: a step's
// account is capped, and a sensitive value longer than the cap would survive
// it as a prefix no match can find, so the callee's set is applied before
// the cap, as the session's own redactor is (#2210).
func TestALongSensitiveValueInACalleesAccountIsWithheldBeforeTheCap(t *testing.T) {
	t.Parallel()

	secret := strings.Repeat("s3cr3t-", flowdebug.MaxInspectRunes/7+100)
	child := `edition: v2026.3
name: child
inputs:
  api_key:
    type: string
    required: true
    sensitive: true
steps:
  - id: echo
    value: ${inputs.api_key}
`
	root := `edition: v2026.3
name: parent
steps:
  - id: first
    value: ${1}
  - id: nested
    call: ./child.yaml
    with:
      api_key: ${"` + secret + `"}
`
	run := startDebugRun(t, "main.yaml", map[string]string{"main.yaml": root, "child.yaml": child}, nil)
	target := flowdebug.Target(run.session)
	move(t, target, waitHeld(t, target, 0), v1.DebugResumeAction_DEBUG_RESUME_ACTION_CONTINUE, "")
	require.NoError(t, <-run.done)

	final, err := target.Snapshot(t.Context())
	require.NoError(t, err)
	var echo string
	for _, observation := range final.GetObservations() {
		if observation.GetStepId() == "echo" {
			echo = observation.GetText()
		}
	}
	require.Contains(t, echo, "-> value:", "the step's value was not recorded, so this proves nothing")
	assert.NotContains(t, echo, secret[:64], "a prefix of the long sensitive value survived the cap")
}
