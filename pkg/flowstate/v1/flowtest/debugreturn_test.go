package flowtest_test

import (
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
