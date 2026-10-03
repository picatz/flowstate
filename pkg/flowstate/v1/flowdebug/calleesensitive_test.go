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
	child := `edition: v2026.4
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
	root := `edition: v2026.4
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
	root := `edition: v2026.4
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

	child := `edition: v2026.4
name: child
inputs:
  codes:
    type: list(dyn)
    required: true
    sensitive: true
steps:
  - id: use
    log:
      message: hi
`
	root := `edition: v2026.4
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

	child := `edition: v2026.4
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
	root := `edition: v2026.4
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

// TestAStepsAccountWithholdsWhatItsCalleeDeclaresSensitive is #2210: the
// account a session gives of each step as it finishes — printed, and recorded
// as an observation — withholds what the step's workflow declares sensitive,
// and so does the account of each call a callee's failure passes through, and
// the failed run's own message. A revealed session shows them.
func TestAStepsAccountWithholdsWhatItsCalleeDeclaresSensitive(t *testing.T) {
	t.Parallel()

	child := `edition: v2026.4
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
	root := `edition: v2026.4
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
	child := `edition: v2026.4
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
	root := `edition: v2026.4
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

// TestACallsAccountWithholdsWhatItsCalleeHandsBack: a callee's outputs are
// rendered in the call step's account from the caller's position, and can hand
// back a value only the callee declares sensitive; a short sensitive value
// inside a structured input is caught by value, where no substring match of
// the rendered line can (Codex, #2212).
func TestACallsAccountWithholdsWhatItsCalleeHandsBack(t *testing.T) {
	t.Parallel()

	child := `edition: v2026.4
name: child
inputs:
  api_key:
    type: string
    required: true
    sensitive: true
  codes:
    type: list(dyn)
    required: true
    sensitive: true
steps:
  - id: first_code
    value: ${inputs.codes[0]}
outputs:
  key:
    value: ${inputs.api_key}
`
	root := `edition: v2026.4
name: parent
steps:
  - id: first
    value: ${1}
  - id: nested
    call: ./child.yaml
    with:
      api_key: ${"` + calleeSecret + `"}
      codes: ${[7]}
`
	for _, reveal := range []bool{false, true} {
		t.Run(fmt.Sprintf("reveal=%t", reveal), func(t *testing.T) {
			t.Parallel()

			run := startDebugRun(t, "main.yaml", map[string]string{"main.yaml": root, "child.yaml": child}, func(opts *flowdebug.Options) {
				opts.RevealSensitive = reveal
			})
			target := flowdebug.Target(run.session)
			move(t, target, waitHeld(t, target, 0), v1.DebugResumeAction_DEBUG_RESUME_ACTION_CONTINUE, "")
			require.NoError(t, <-run.done)

			final, err := target.Snapshot(t.Context())
			require.NoError(t, err)
			accounts := map[string]string{}
			for _, observation := range final.GetObservations() {
				accounts[observation.GetStepId()] = observation.GetText()
			}
			require.Contains(t, accounts["nested"], "key:", "the call's outputs were not recorded, so this proves nothing")
			require.Contains(t, accounts["first_code"], "-> value:", "the step's value was not recorded, so this proves nothing")
			if reveal {
				assert.Contains(t, accounts["nested"], calleeSecret, "an authorized reveal withheld what the callee handed back")
				assert.Contains(t, accounts["first_code"], "value: 7", "an authorized reveal withheld the short value")
			} else {
				assert.NotContains(t, accounts["nested"], calleeSecret, "the call's account showed what its callee handed back")
				assert.NotContains(t, accounts["first_code"], "value: 7", "a short sensitive value was shown in its step's account")
			}
		})
	}
}

// TestALaterStepsAccountWithholdsWhatACallHandedBack: once a callee's value
// sits in the caller's scope, as an output the callee does not declare
// sensitive or as a tolerated call's recorded failure, a later step's account
// reads it from the caller's position, and so does a callee the caller passes
// it on to under a plain name (#2213).
func TestALaterStepsAccountWithholdsWhatACallHandedBack(t *testing.T) {
	t.Parallel()

	const failingSecret = "hunter2-failing-callee-secret"
	child := `edition: v2026.4
name: child
inputs:
  api_key:
    type: string
    required: true
    sensitive: true
steps:
  - id: use
    value: ${1}
outputs:
  key:
    value: ${inputs.api_key}
`
	failing := `edition: v2026.4
name: failing
inputs:
  api_key:
    type: string
    required: true
    sensitive: true
steps:
  - id: boom
    value: ${{"a":1}[inputs.api_key]}
`
	leaf := `edition: v2026.4
name: leaf
inputs:
  who:
    type: string
    required: true
steps:
  - id: greet
    value: ${inputs.who}
`
	root := `edition: v2026.4
name: parent
steps:
  - id: nested
    call: ./child.yaml
    with:
      api_key: ${"` + calleeSecret + `"}
  - id: copied
    value: ${"Bearer " + steps.nested.key}
  - id: tolerated
    call: ./failing.yaml
    continue_on_error: true
    with:
      api_key: ${"` + failingSecret + `"}
  - id: echoed
    value: ${steps.tolerated.error}
  - id: passed
    call: ./leaf.yaml
    with:
      who: ${steps.nested.key}
`
	for _, reveal := range []bool{false, true} {
		t.Run(fmt.Sprintf("reveal=%t", reveal), func(t *testing.T) {
			t.Parallel()

			run := startDebugRun(t, "main.yaml", map[string]string{
				"main.yaml": root, "child.yaml": child, "failing.yaml": failing, "leaf.yaml": leaf,
			}, func(opts *flowdebug.Options) {
				opts.RevealSensitive = reveal
			})
			target := flowdebug.Target(run.session)
			move(t, target, waitHeld(t, target, 0), v1.DebugResumeAction_DEBUG_RESUME_ACTION_CONTINUE, "")
			require.NoError(t, <-run.done)

			final, err := target.Snapshot(t.Context())
			require.NoError(t, err)
			accounts := map[string]string{}
			for _, observation := range final.GetObservations() {
				accounts[observation.GetStepId()] = observation.GetText()
			}
			for step, secret := range map[string]string{"copied": calleeSecret, "echoed": failingSecret, "greet": calleeSecret} {
				require.Contains(t, accounts[step], "-> value:", "%s's value was not recorded, so this proves nothing", step)
				if reveal {
					assert.Contains(t, accounts[step], secret, "an authorized reveal withheld %s's value", step)
				} else {
					assert.NotContains(t, accounts[step], secret, "%s's account showed what a call handed back", step)
				}
			}
		})
	}
}

// TestARunsFailureWithholdsWhatACallHandedBack: a caller fails quoting a
// value a call handed back, and the run ends on it: through a step, a later
// step's `if:`, or a declared output. The run's final message and the
// session's account of it are rendered from the root, which declares nothing,
// so the failure carries what the root's position withholds (#2213).
func TestARunsFailureWithholdsWhatACallHandedBack(t *testing.T) {
	t.Parallel()

	child := `edition: v2026.4
name: child
inputs:
  api_key:
    type: string
    required: true
    sensitive: true
steps:
  - id: use
    value: ${1}
outputs:
  key:
    value: ${inputs.api_key}
`
	nested := `edition: v2026.4
name: parent
steps:
  - id: nested
    call: ./child.yaml
    with:
      api_key: ${"` + calleeSecret + `"}
`
	for name, root := range map[string]string{
		"a step": nested + `  - id: boom
    value: ${{"a":1}[steps.nested.key]}
`,
		"a later step's if": nested + `  - id: guarded
    if: ${{"a":true}[steps.nested.key]}
    value: ${1}
`,
		"a declared output": nested + `outputs:
  out:
    value: ${{"a":1}[steps.nested.key]}
`,
	} {
		for _, reveal := range []bool{false, true} {
			t.Run(fmt.Sprintf("%s/reveal=%t", name, reveal), func(t *testing.T) {
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
				move(t, target, waitHeld(t, target, 0), v1.DebugResumeAction_DEBUG_RESUME_ACTION_CONTINUE, "")
				runErr := <-run.done
				require.ErrorContains(t, runErr, calleeSecret, "the run's failure does not quote the handed-back value, so this proves nothing")

				final, err := target.Snapshot(t.Context())
				require.NoError(t, err)
				failure := run.session.FailureText(runErr)
				require.Contains(t, failure, "no such key", "the failure says nothing, so this proves nothing")
				mu.Lock()
				out := printed.String()
				mu.Unlock()

				for i, text := range []string{final.GetMessage(), failure, out} {
					if reveal {
						assert.Contains(t, text, calleeSecret, "rendering %d withheld what an authorized reveal shows", i)
					} else {
						assert.NotContains(t, text, calleeSecret, "rendering %d showed what a call handed back", i)
					}
				}
			})
		}
	}
}
