package flowtest_test

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/picatz/flowstate/pkg/flowstate/v1/flowtest"
)

// TestRunSourceCarriesCoverage pins the parity half of issue #931: the bytes
// door — what the flowstate_test MCP tool calls — used to build no coverage
// accumulator at all, so the schema field existed, the CLI filled it, and
// every MCP answer carried `coverage: []` while docs/reference/mcp.md
// promised "the same v1.TestReport `flow test -o json` writes". Coverage now
// attaches in the one shared suite loop, so this door answers with the same
// account. Arm positions are zero here, as the schema documents for a
// workflow submitted as bytes — that part is not a gap, it is the honest
// answer for a source with no file to point into.
func TestRunSourceCarriesCoverage(t *testing.T) {
	t.Parallel()

	report := flowtest.RunSource("<submitted>", []byte(`
edition: v2026.3
name: two
steps:
  - id: yes_branch
    log:
      message: taken
  - id: no_branch
    if: ${false}
    log:
      message: never
outputs: {}
`), []byte(`
tests:
  - name: one branch of two
    workflow: ignored-for-bytes
    stubs:
      - task: log
        returns: {}
    expect:
      ran: [yes_branch]
      skipped: [no_branch]
`))

	require.Empty(t, report.GetRefused())
	require.Len(t, report.GetCases(), 1)
	require.True(t, report.GetCases()[0].GetPassed())

	require.Len(t, report.GetCoverage(), 1, "the bytes door must carry the same coverage account the CLI's does")
	cov := report.GetCoverage()[0]
	require.Equal(t, int32(2), cov.GetStepsTotal())
	require.Equal(t, int32(1), cov.GetStepsReached(),
		"the skipped-by-if step produces no outputs, so coverage counts it unreached — the account a reader acts on")
	require.Contains(t, cov.GetUnreached(), "no_branch")
}
