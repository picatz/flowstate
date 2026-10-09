package flowtest_test

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/picatz/flowstate/pkg/flowstate/v1/flowtest"
)

// TestACaseNamingAModuleIsRefusedNotRun: a module has no steps to exercise, so a
// case that names one as its workflow fails with the module sentence instead of
// passing against an empty run. Function cases for modules are a later slice;
// until then the refusal is the honest answer.
func TestACaseNamingAModuleIsRefusedNotRun(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	writeFile(t, dir+"/ids.yaml", `edition: v2026.4
name: ids
errors:
  NotFound:
    description: The customer does not exist.
`)
	report := flowtest.RunFile(writeInline(t, dir, `
tests:
  - name: exercises a module
    workflow: ./ids.yaml
    expect:
      ran: []
`))

	require.Empty(t, report.GetRefused())
	require.Len(t, report.GetCases(), 1)
	c := report.GetCases()[0]
	assert.False(t, c.GetPassed(), "a module has nothing to run, so no case against it may pass")
	assert.Contains(t, c.GetError(), "is a module (no steps); import it with use:, don't run it")
}
