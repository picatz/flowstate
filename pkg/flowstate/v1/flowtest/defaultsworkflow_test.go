package flowtest_test

import (
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/picatz/flowstate/pkg/flowstate/v1/flowtest"
)

// `defaults.workflow:` (#924 slice 1): the one fact every case in the shipped
// corpus restated identically — 151 of 151 cases — stated once at the file
// level, merged by the same one-direction rule `defaults.sender` already
// follows: explicit beats inherited.

func TestDefaultsWorkflowIsInheritedByCasesThatNameNone(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	writeFile(t, filepath.Join(dir, "workflow.yaml"), `
edition: v2026.3
name: greet
steps:
  - id: hello
    log:
      message: hi
outputs: {}
`)
	path := filepath.Join(dir, "workflow.test.yaml")
	writeFile(t, path, `
defaults:
  workflow: ./workflow.yaml
tests:
  - name: inherits the file-level workflow
    stubs:
      - task: log
        returns: {}
    expect:
      ran: [hello]
  - name: so does the second case
    stubs:
      - task: log
        returns: {}
    expect:
      ran: [hello]
      others: skipped
`)

	report := flowtest.RunFile(path)
	require.Empty(t, report.GetRefused())
	require.Len(t, report.GetCases(), 2)
	for _, c := range report.GetCases() {
		require.True(t, c.GetPassed(), "%s: %v / %v", c.GetName(), c.GetError(), c.GetFailures())
	}
}

// TestACasesOwnWorkflowBeatsTheDefault: the merge's one direction, proven by
// two workflows whose step ids tell them apart.
func TestACasesOwnWorkflowBeatsTheDefault(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	writeFile(t, filepath.Join(dir, "workflow.yaml"), `
edition: v2026.3
name: default-target
steps:
  - id: default_step
    log:
      message: hi
outputs: {}
`)
	writeFile(t, filepath.Join(dir, "other.yaml"), `
edition: v2026.3
name: own-target
steps:
  - id: own_step
    log:
      message: hi
outputs: {}
`)
	path := filepath.Join(dir, "workflow.test.yaml")
	writeFile(t, path, `
defaults:
  workflow: ./workflow.yaml
tests:
  - name: names its own and keeps it
    workflow: ./other.yaml
    stubs:
      - task: log
        returns: {}
    expect:
      ran: [own_step]
`)

	report := flowtest.RunFile(path)
	require.Empty(t, report.GetRefused())
	c := report.GetCases()[0]
	require.True(t, c.GetPassed(), "%v / %v", c.GetError(), c.GetFailures())
}

// TestADefaultWorkflowMayNotHoldAnExpression: `defaults:` is a fixture, so
// `defaults.workflow:` takes the same expression refusal every other default
// field does — otherwise `${inputs.target}` loads as a literal path and fails
// later with a file-open error that names nothing an author can act on.
func TestADefaultWorkflowMayNotHoldAnExpression(t *testing.T) {
	t.Parallel()

	_, err := flowtest.Load(writeInline(t, t.TempDir(), `
defaults:
  workflow: ${inputs.target}
tests:
  - name: never gets this far
    expect:
      ran: [x]
`))
	require.Error(t, err)
	require.Contains(t, err.Error(), "defaults.workflow")
	require.Contains(t, err.Error(), "may not hold an expression")
}

// TestNoWorkflowAnywhereIsStillRefused: the default is a value, not a
// loosening — a file with neither spelling keeps the refusal a case with no
// workflow has always earned.
func TestNoWorkflowAnywhereIsStillRefused(t *testing.T) {
	t.Parallel()

	_, err := flowtest.Load(writeInline(t, t.TempDir(), `
tests:
  - name: names nothing
    expect:
      ran: [x]
`))
	require.Error(t, err)
	require.Contains(t, err.Error(), "names no workflow")
}
