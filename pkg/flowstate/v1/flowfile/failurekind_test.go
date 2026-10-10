package flowfile_test

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

const failureKindWorkflow = `edition: v2026.4
name: failure-kind
steps:
  - id: fetch
    continue_on_error: true
    http:
      method: GET
      url: https://example.com/
  - id: react
    if: %s
    log:
      message: hi
`

// TestFailureKindLiteralIsChecked pins #1905's validate-time half: a tolerated
// step's `failure.kind` is the closed [v1.ErrorKind] set, so a misspelled
// literal is refused, in either operand order and with `!=`, rather than
// evaluating false on both drivers forever.
func TestFailureKindLiteralIsChecked(t *testing.T) {
	t.Parallel()

	for name, condition := range map[string]string{
		"equals":         `${steps.fetch.failure.kind == "Timeuot"}`,
		"not equals":     `${steps.fetch.failure.kind != "Timeuot"}`,
		"literal first":  `${"Timeuot" == steps.fetch.failure.kind}`,
		"inside a macro": `${[1].exists(x, steps.fetch.failure.kind == "Timeuot")}`,
	} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()

			diagnostics := validateTriggerSource(t, strings.Replace(failureKindWorkflow, "%s", condition, 1))

			var found bool
			for _, d := range diagnostics {
				if strings.Contains(d.Message, `"Timeuot"`) {
					found = true
					assert.Contains(t, d.Message, `did you mean "Timeout"`)
				}
			}
			require.True(t, found, "no diagnostic named the misspelled kind: %v", diagnostics)
		})
	}
}

// TestFailureOutputIsKnownAndKindsPass is the opposite direction: the typed
// `failure` output resolves on a tolerated step, every real kind passes, and a
// non-literal comparand is never judged.
func TestFailureOutputIsKnownAndKindsPass(t *testing.T) {
	t.Parallel()

	for _, condition := range []string{
		`${steps.fetch.failure.kind == "Timeout"}`,
		`${steps.fetch.failure.retryable && steps.fetch.failure.kind != "UpstreamUnknown"}`,
		`${steps.fetch.failure.message == steps.fetch.error}`,
		`${steps.fetch.failure.kind == steps.fetch.failure.message}`,
	} {
		diagnostics := validateTriggerSource(t, strings.Replace(failureKindWorkflow, "%s", condition, 1))
		require.Empty(t, diagnostics, "%s", condition)
	}
}

// TestFailureOutputRequiresTolerance mirrors `error`: the output exists only on
// a step that carries `continue_on_error:`.
func TestFailureOutputRequiresTolerance(t *testing.T) {
	t.Parallel()

	source := strings.Replace(strings.Replace(failureKindWorkflow, "    continue_on_error: true\n", "", 1),
		"%s", `${steps.fetch.failure.kind == "Timeout"}`, 1)

	diagnostics := validateTriggerSource(t, source)
	require.NotEmpty(t, diagnostics)
}

// TestFailureKindOnAnUntoleratedStepIsNotJudged: only a tolerated step's
// `failure` is the engine's, so a call output that is merely named `failure`
// is left alone.
func TestFailureKindOnAnUntoleratedStepIsNotJudged(t *testing.T) {
	t.Parallel()

	source := strings.Replace(strings.Replace(failureKindWorkflow, "    continue_on_error: true\n", "", 1),
		"%s", `${steps.fetch.failure.kind == "network"}`, 1)

	for _, d := range validateTriggerSource(t, source) {
		require.NotContains(t, d.Message, `"network"`, "%v", d)
	}
}

// TestFailureOutputOnShapedAndWaitSteps pins the #2288 review findings: the
// policy's `failure` resolves on a tolerated shaping wait, a successful
// tolerated step that shapes or declares its own `failure` is not judged by the
// closed kind set, and a shaping that omits `failure` still is.
func TestFailureOutputOnShapedAndWaitSteps(t *testing.T) {
	t.Parallel()

	const waits = `edition: v2026.4
name: tolerated-wait
steps:
  - id: gate
    continue_on_error: true
    wait_for_signal:
      name: go
      timeout: 1h
      outputs:
        who: someone
  - id: react
    if: ${has(steps.gate.failure)}
    log:
      message: hi
`
	require.Empty(t, validateTriggerSource(t, waits))

	const shapes = `edition: v2026.4
name: shaped
steps:
  - id: fetch
    continue_on_error: true
    http:
      url: https://example.com/
      outputs:
        failure: '${{"kind": "network"}}'
  - id: react
    if: ${steps.fetch.failure.kind == "network"}
    log:
      message: hi
`
	for _, d := range validateTriggerSource(t, shapes) {
		require.NotContains(t, d.Message, `"network"`, "%v", d)
	}

	const dropped = `edition: v2026.4
name: shaped-without
steps:
  - id: fetch
    continue_on_error: true
    http:
      url: https://example.com/
      outputs:
        code: ${status_code}
  - id: react
    if: ${steps.fetch.failure.kind == "Timeuot"}
    log:
      message: hi
`
	var found bool
	for _, d := range validateTriggerSource(t, dropped) {
		found = found || strings.Contains(d.Message, `"Timeuot"`)
	}
	require.True(t, found)
}
