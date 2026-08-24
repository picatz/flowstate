package flowtest_test

import (
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/picatz/flowstate/pkg/flowstate/v1/flowtest"
)

// [flowtest.Run] with no directory (the Codex finding on #1015): before the
// refusal existed, a relative `workflow:` fell through [filepath.Join]'s
// identity on an empty prefix and resolved against the process working
// directory — the same suite silently running whatever file the caller's cwd
// happened to hold, or failing there, depending on where the test binary ran
// from. The doc always promised a refusal; these pin that the code keeps it.

func TestRunWithNoDirRefusesARelativeWorkflowPath(t *testing.T) {
	t.Parallel()

	file, err := flowtest.LoadSource([]byte(`
tests:
  - name: names a relative workflow
    workflow: ./workflow.yaml
    stubs:
      - task: log
        returns: {}
    expect:
      ran: [hello]
`))
	require.NoError(t, err)

	result := flowtest.Run(t.Context(), file, "", flowtest.RunOptions{Label: "<built>"})
	require.Len(t, result.Report.GetCases(), 1)
	c := result.Report.GetCases()[0]
	require.False(t, c.GetPassed())
	require.Contains(t, c.GetError(), `workflow "./workflow.yaml" is a relative path`)
	require.Contains(t, c.GetError(), "no directory to resolve it against")
}

func TestRunWithNoDirAcceptsAnAbsoluteWorkflowPath(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	workflow := filepath.Join(dir, "workflow.yaml")
	writeFile(t, workflow, `
edition: v2026.3
name: greet
steps:
  - id: hello
    log:
      message: hi
outputs: {}
`)

	file, err := flowtest.LoadSource([]byte(`
tests:
  - name: names an absolute workflow
    workflow: ` + workflow + `
    stubs:
      - task: log
        returns: {}
    expect:
      ran: [hello]
`))
	require.NoError(t, err)

	result := flowtest.Run(t.Context(), file, "", flowtest.RunOptions{Label: "<built>"})
	require.Len(t, result.Report.GetCases(), 1)
	c := result.Report.GetCases()[0]
	require.True(t, c.GetPassed(), "an absolute path needs no directory: %v / %v", c.GetError(), c.GetFailures())
}
