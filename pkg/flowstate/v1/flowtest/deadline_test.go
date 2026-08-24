package flowtest_test

import (
	"context"
	"fmt"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/picatz/flowstate/pkg/flowstate/v1/flowtest"
)

// The contract [flowtest.RunSourceContext] documents, and the reason it
// exists: a caller's bound has to stop the *work*, not only the execution.
//
// Most of what a case costs happens before its context is ever consulted —
// compiling its stubs, parsing the workflow again, binding the stubs against
// it — so a deadline that expired during an early case was followed by every
// remaining case's setup. On a serving surface that is hundreds of parses
// after the caller's whole budget is spent, while the exclusive registry lock
// it took is still held (see cmd/flow/mcpserve.go). Reported by Codex on
// picatz/flowstate#807.

// TestRunSourceContextStopsStartingCasesOnceCancelled asserts the contract as
// the report reads it: a case that never started says so, rather than being
// attempted and failing for some incidental reason on the way.
func TestRunSourceContextStopsStartingCasesOnceCancelled(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithCancel(t.Context())
	cancel()

	const workflow = "edition: v2026.3\nname: demo\nsteps:\n- id: hi\n  log:\n    message: hello\n"

	var tests strings.Builder
	tests.WriteString("tests:\n")
	for i := range 50 {
		fmt.Fprintf(&tests, "  - name: case %d\n    stubs:\n      - task: log\n        returns: {}\n"+
			"    expect:\n      failed: false\n", i)
	}

	report := flowtest.RunSourceContext(ctx, "<submitted>", []byte(workflow), []byte(tests.String()))

	require.Empty(t, report.GetRefused(), "the tests document itself is fine; only the context is done")
	require.Len(t, report.GetCases(), 50, "every declared case must still be accounted for")

	for _, c := range report.GetCases() {
		require.False(t, c.GetPassed(), "%s must not be reported as passing: it never ran", c.GetName())
		require.Contains(t, c.GetError(), "not run",
			"%s must say it never started, not report an incidental failure from being attempted", c.GetName())
	}
}

// TestRunSourceRunsEveryCaseWithoutADeadline keeps the test above honest: the
// same file with a live context really does run, so "not run" is a property of
// the cancellation rather than of the file.
func TestRunSourceRunsEveryCaseWithoutADeadline(t *testing.T) {
	t.Parallel()

	const workflow = "edition: v2026.3\nname: demo\nsteps:\n- id: hi\n  log:\n    message: hello\n"
	const tests = "tests:\n  - name: it runs\n    stubs:\n      - task: log\n        returns: {}\n" +
		"    expect:\n      failed: false\n"

	report := flowtest.RunSourceContext(t.Context(), "<submitted>", []byte(workflow), []byte(tests))

	require.Len(t, report.GetCases(), 1)
	require.True(t, report.GetCases()[0].GetPassed(), report.GetCases()[0].GetError())
}
