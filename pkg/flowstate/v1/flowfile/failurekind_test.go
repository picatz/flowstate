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
