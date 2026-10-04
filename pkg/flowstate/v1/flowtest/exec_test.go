package flowtest_test

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/picatz/flowstate/pkg/flowstate/v1/flowtest"
	"github.com/picatz/flowstate/pkg/flowstate/v1/internal/conformance"
)

// TestAnExecStepIsStubbedAndNeverStarted pins `flow test`'s promise for the one
// task whose escape would be a process on the machine running the tests. A real
// exec policy is installed that would let the step run, and the workflow's step
// would create a marker file if it did: neither a stubbed nor an unstubbed case
// may leave one behind.
//
// Serial: it installs a policy into the process-wide registry.
func TestAnExecStepIsStubbedAndNeverStarted(t *testing.T) {
	root := conformance.ExecRoot(t)
	conformance.InstallExecPolicy(t, root, conformance.ExecCase{})
	marker := filepath.Join(root, "started")

	dir := t.TempDir()
	writeFile(t, dir+"/workflow.yaml", `
edition: v2026.4
name: exec-stubbed
steps:
  - id: build
    exec:
      argv: [sh, -c, "touch started"]
      dir: `+root+`
  - id: report
    if: steps.build.exit_code == 3
    log:
      message: '${"build exited " + string(steps.build.exit_code) + ": " + steps.build.stderr}'
outputs:
  code:
    value: ${steps.build.exit_code}
`)
	report := flowtest.RunFile(writeInline(t, dir, `
tests:
  - name: a stub answers with the program's own outputs, and the next step reads them
    workflow: ./workflow.yaml
    stubs:
      - task: exec
        where: inputs.argv[0] == 'sh'
        returns:
          exit_code: 3
          stdout: ""
          stderr: tests failed
      - task: log
        returns: {}
    expect:
      ran: [build, report]
      outputs: {code: 3}

  - name: no stub, and nothing starts
    workflow: ./workflow.yaml
    expect:
      failed: true
`))

	require.Len(t, report.GetCases(), 2)

	stubbed := report.GetCases()[0]
	require.True(t, stubbed.GetPassed(), "%v / %v", stubbed.GetError(), stubbed.GetFailures())

	unstubbed := report.GetCases()[1]
	require.True(t, unstubbed.GetPassed(), "%v / %v", unstubbed.GetError(), unstubbed.GetFailures())
	require.NotEmpty(t, unstubbed.GetWarnings())
	assert.Contains(t, unstubbed.GetWarnings()[0].GetMessage(), `task "exec"`,
		"the harness must say the exec step ran unstubbed")

	_, err := os.Stat(marker)
	require.ErrorIs(t, err, os.ErrNotExist, "an exec step was started under flow test")
}
