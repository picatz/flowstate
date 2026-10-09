package flowtest_test

import (
	"strconv"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/picatz/flowstate/pkg/flowstate/v1/flowtest"
)

const failedHowWorkflow = `
edition: v2026.4
name: refuses
errors:
  TooBig: {}
  TooSmall: {}
inputs:
  n:
    type: int
steps:
  - id: small
    if: ${inputs.n < 0}
    fail:
      error: TooSmall
      message: negative
  - id: guarded
    for_each:
      items: ${[inputs.n]}
      as: n
      steps:
        - id: big
          if: ${n > 10}
          fail:
            error: TooBig
            message: above ten
`

// runFailedHow runs one case whose expect.failed is the given YAML, so each
// test states only the claim and the input.
func runFailedHow(t *testing.T, n int, failed string) (passed bool, diagnostics string, refused string) {
	t.Helper()

	dir := t.TempDir()
	writeFile(t, dir+"/workflow.yaml", failedHowWorkflow)
	writeFile(t, dir+"/workflow.test.yaml", `
tests:
  - name: case
    workflow: ./workflow.yaml
    inputs: {n: `+strconv.Itoa(n)+`}
    expect:
      failed: `+failed+`
`)
	report := flowtest.RunFile(dir + "/workflow.test.yaml")
	if len(report.GetRefused()) > 0 {
		return false, "", report.GetRefused()
	}
	require.Len(t, report.GetCases(), 1)
	c := report.GetCases()[0]
	for _, f := range c.GetFailures() {
		diagnostics += f.GetField() + ": " + f.GetMessage() + "\n"
	}

	return c.GetPassed(), diagnostics + c.GetError(), c.GetError()
}

func TestFailedNamesStepAndError(t *testing.T) {
	t.Parallel()

	t.Run("the right step and error pass", func(t *testing.T) {
		t.Parallel()
		passed, diagnostics, _ := runFailedHow(t, 50, "{step: big, error: TooBig}")
		require.True(t, passed, diagnostics)
	})
	t.Run("either key alone is a claim", func(t *testing.T) {
		t.Parallel()
		passed, diagnostics, _ := runFailedHow(t, -1, "{error: TooSmall}")
		require.True(t, passed, diagnostics)
		passed, diagnostics, _ = runFailedHow(t, -1, "{step: small}")
		require.True(t, passed, diagnostics)
	})
	t.Run("the wrong step fails the case", func(t *testing.T) {
		t.Parallel()
		passed, diagnostics, _ := runFailedHow(t, 50, "{step: guarded, error: TooBig}")
		require.False(t, passed)
		require.Contains(t, diagnostics, `expect.failed.step: expected the run to fail in step "guarded", but it failed in "big"`)
	})
	t.Run("the wrong error fails the case", func(t *testing.T) {
		t.Parallel()
		passed, diagnostics, _ := runFailedHow(t, 50, "{step: big, error: TooSmall}")
		require.False(t, passed)
		require.Contains(t, diagnostics, "expected the run to fail with TooSmall, but it failed with TooBig")
	})
	t.Run("a name nothing declares fails the case and says what exists", func(t *testing.T) {
		t.Parallel()
		passed, diagnostics, _ := runFailedHow(t, 50, "{error: Toobig}")
		require.False(t, passed)
		require.Contains(t, diagnostics, `"Toobig" is neither an error this workflow declares`)
		require.Contains(t, diagnostics, "TooBig, TooSmall")
	})
	t.Run("a run that does not fail fails the claim", func(t *testing.T) {
		t.Parallel()
		passed, diagnostics, _ := runFailedHow(t, 5, "{step: big, error: TooBig}")
		require.False(t, passed)
		require.Contains(t, diagnostics, "expected the run to report failed=true, got failed=false")
	})
	t.Run("bare booleans still mean what they did", func(t *testing.T) {
		t.Parallel()
		passed, diagnostics, _ := runFailedHow(t, 50, "true")
		require.True(t, passed, diagnostics)
		passed, _, _ = runFailedHow(t, 50, "false")
		require.False(t, passed)
	})
	t.Run("an empty or misspelled mapping is refused at load", func(t *testing.T) {
		t.Parallel()
		_, _, refused := runFailedHow(t, 50, "{}")
		require.Contains(t, refused, "names the `step:`")
		_, _, refused = runFailedHow(t, 50, "{erorr: TooBig}")
		require.Contains(t, refused, `"erorr" is neither`)
	})
}

// A case that never bound the secret its workflow reads must not be satisfied
// by the failure that follows, whatever it claims about failing.
func TestUnboundSecretIsNotAssertableFailure(t *testing.T) {
	t.Parallel()

	for _, failed := range []string{"true", "{error: invalid_input}"} {
		dir := t.TempDir()
		writeFile(t, dir+"/workflow.yaml", `
edition: v2026.4
name: bearer-request
steps:
  - id: call
    http:
      url: https://api.example.com/status
      bearer: ${secret('env:TOKEN')}
`)
		writeFile(t, dir+"/workflow.test.yaml", `
tests:
  - name: unbound
    workflow: ./workflow.yaml
    stubs:
      - task: http
        returns: {status_code: 200}
    expect:
      failed: `+failed+`
`)
		report := flowtest.RunFile(dir + "/workflow.test.yaml")
		require.Len(t, report.GetCases(), 1)
		c := report.GetCases()[0]
		require.False(t, c.GetPassed(), "failed: %s", failed)
		require.Contains(t, c.GetError(), "does not bind a secret")
		require.Contains(t, c.GetError(), "env:TOKEN")
	}
}
