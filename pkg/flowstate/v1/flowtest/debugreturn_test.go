package flowtest_test

import (
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/picatz/flowstate/pkg/flowstate/v1/flowdebug"
	"github.com/picatz/flowstate/pkg/flowstate/v1/flowtest"
)

// TestADebuggedCaseReportsItsRunsReturn: a case run under a debugger tells it
// the run has returned, so `flow test --debug` and the scripted MCP tool say
// an `until` the run never reached, as the other fronts do. The verdict is
// left to the case's driver: this changes no session state.
func TestADebuggedCaseReportsItsRunsReturn(t *testing.T) {
	t.Parallel()

	var out strings.Builder
	session, err := flowdebug.New(flowdebug.Options{In: strings.NewReader("until orders[9]/charge\n"), Out: &out})
	require.NoError(t, err)
	t.Cleanup(func() { _ = session.Close() })

	workflow := `edition: v2026.3
name: orders
steps:
  - id: orders
    for_each:
      items: ${[1, 2]}
      as: order
      steps:
        - id: charge
          log:
            message: charged
outputs: {}
`
	tests := `tests:
  - name: charges both
    stubs:
      - task: log
        returns: {}
    expect:
      ran: [orders]
`
	result := flowtest.RunSourceWith(t.Context(), "<submitted>", []byte(workflow), []byte(tests), flowtest.RunOptions{Debugger: session})
	require.Len(t, result.Report.GetCases(), 1, "%v", result.Report)
	require.True(t, result.Report.GetCases()[0].GetPassed(), "%v", result.Report.GetCases()[0])

	assert.Contains(t, out.String(), "the run completed without stopping at `until orders[9]/charge`")
}

// TestTheAutopsyWithholdsACalleesSensitiveInput: the session's redactors are
// the case's posture when the run starts, and the autopsy inspects the
// finished run's scope, which can hold a value only a callee declared
// sensitive, handed back and read by the caller. The autopsy is given what
// the run withheld (Copilot, #2215).
func TestTheAutopsyWithholdsACalleesSensitiveInput(t *testing.T) {
	t.Parallel()

	const secret = "hunter2-callee-only-secret"
	dir := t.TempDir()
	write := func(name, body string) {
		require.NoError(t, os.WriteFile(filepath.Join(dir, name), []byte(body), 0o600))
	}
	write("child.yaml", `edition: v2026.3
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
`)
	write("workflow.yaml", `edition: v2026.3
name: parent
steps:
  - id: nested
    call: ./child.yaml
    with:
      api_key: ${"`+secret+`"}
  - id: echo
    value: ${steps.nested.key}
`)
	write("workflow.test.yaml", `tests:
  - name: expects a failure the run never has
    workflow: ./workflow.yaml
    expect:
      failed: true
`)

	var out strings.Builder
	session, err := flowdebug.New(flowdebug.Options{In: strings.NewReader("continue\ninspect steps.echo.value\nquit\n"), Out: &out})
	require.NoError(t, err)
	t.Cleanup(func() { _ = session.Close() })

	result := flowtest.RunPath(t.Context(), filepath.Join(dir, "workflow.test.yaml"), flowtest.RunOptions{Debugger: session})
	require.Len(t, result.Report.GetCases(), 1, "%v", result.Report)
	require.False(t, result.Report.GetCases()[0].GetPassed(), "the case passed, so there was no autopsy")

	printed := out.String()
	_, autopsy, found := strings.Cut(printed, "autopsy:")
	require.True(t, found, "no autopsy was printed, so this proves nothing:\n%s", printed)
	assert.Contains(t, autopsy, "[redacted]", "the autopsy's inspection did not say it withheld the value:\n%s", autopsy)
	assert.NotContains(t, autopsy, secret, "the autopsy showed a callee's sensitive input")
}
