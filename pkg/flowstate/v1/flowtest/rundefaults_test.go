package flowtest_test

import (
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/picatz/flowstate/pkg/flowstate/v1/flowtest"
)

// [flowtest.Run] and `defaults:` (the Codex finding on #1015): only Load and
// LoadSource used to fold a file's defaults into its cases, so a [flowtest.File]
// built in Go ran with its Defaults silently ignored — the same logical suite
// behaving differently depending on whether it was constructed or parsed.

// TestRunAppliesDefaultsToAGoBuiltFile: a built file whose only stub lives in
// Defaults passes exactly as its parsed twin would — and the caller's File is
// left untouched, since Run folds into a copy.
func TestRunAppliesDefaultsToAGoBuiltFile(t *testing.T) {
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

	file := &flowtest.File{
		Defaults: &flowtest.Defaults{
			Stubs: []flowtest.Stub{{Task: "log", Returns: map[string]any{}}},
		},
		Tests: []flowtest.Test{{
			Name:     "inherits the file-level stub",
			Workflow: "./workflow.yaml",
			Expect:   flowtest.Expectation{Ran: []string{"hello"}},
		}},
	}

	result := flowtest.Run(t.Context(), file, dir, flowtest.RunOptions{Label: "<built>"})
	require.Len(t, result.Report.GetCases(), 1)
	c := result.Report.GetCases()[0]
	require.True(t, c.GetPassed(),
		"the default stub must reach a Go-built file's case: %v / %v", c.GetError(), c.GetFailures())

	require.Empty(t, file.Tests[0].Stubs, "Run folds defaults into a copy, never into the caller's File")
}

// TestRunDoesNotDoubleMergeALoadedFile pins the idempotency the fix leans on:
// Load already folded this file's defaults, and Run folds again. The probe is
// a `times: 1` default stub against a workflow that invokes its task twice —
// folded once, the stub drains and the second call fails, which is what the
// case expects; folded twice, a duplicated inherited copy would answer the
// second call and the run would succeed, failing the case's `failed: true`.
func TestRunDoesNotDoubleMergeALoadedFile(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	writeFile(t, filepath.Join(dir, "workflow.yaml"), `
edition: v2026.3
name: twice
steps:
  - id: first
    log:
      message: one
  - id: second
    log:
      message: two
outputs: {}
`)

	file, err := flowtest.LoadSource([]byte(`
defaults:
  stubs:
    - task: log
      returns: {}
      times: 1
tests:
  - name: the drained stub fails the second call
    workflow: ./workflow.yaml
    expect:
      failed: true
`))
	require.NoError(t, err)

	result := flowtest.Run(t.Context(), file, dir, flowtest.RunOptions{Label: "<loaded>"})
	require.Len(t, result.Report.GetCases(), 1)
	c := result.Report.GetCases()[0]
	require.True(t, c.GetPassed(),
		"exactly one inherited copy must exist, draining on call two: %v / %v", c.GetError(), c.GetFailures())
}
