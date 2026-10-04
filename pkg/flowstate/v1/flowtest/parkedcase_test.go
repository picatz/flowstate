package flowtest_test

import (
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/picatz/flowstate/pkg/flowstate/v1/flowdebug"
	"github.com/picatz/flowstate/pkg/flowstate/v1/flowtest"
)

// TestAParkedCaseDoesNotHoldTheProcessRegistry: a case held at a debugger's
// stop owns its own registry, so a second case runs to its end while the
// first is still parked. A debugging session that replays its run beside the
// one it holds, to step backwards, depends on this: the process-wide
// registry is held for compiling the case and building its own, not for the
// run.
func TestAParkedCaseDoesNotHoldTheProcessRegistry(t *testing.T) {
	t.Parallel()

	const workflow = `edition: v2026.4
name: plugin-wf
steps:
  - id: a
    parked.post:
      channel: general
outputs: {}
`
	const tests = `tests:
  - name: stubs a task this build never registered
    stubs:
      - task: parked.post
        returns:
          ok: true
    expect:
      ran: [a]
`
	session, err := flowdebug.New(flowdebug.Options{In: strings.NewReader(""), Out: &strings.Builder{}, Controlled: true})
	require.NoError(t, err)
	t.Cleanup(func() { _ = session.Close() })

	parked := make(chan flowtest.RunResult, 1)
	go func() {
		parked <- flowtest.RunSourceWith(t.Context(), "<parked>", []byte(workflow), []byte(tests), flowtest.RunOptions{Debugger: session})
	}()
	_, err = session.WaitForPause(t.Context())
	require.NoError(t, err)

	second := make(chan flowtest.RunResult, 1)
	go func() {
		second <- flowtest.RunSourceWith(t.Context(), "<second>", []byte(workflow), []byte(tests), flowtest.RunOptions{})
	}()
	select {
	case result := <-second:
		require.Len(t, result.Report.GetCases(), 1, "%v", result.Report)
		require.True(t, result.Report.GetCases()[0].GetPassed(), "%v", result.Report.GetCases()[0])
	case <-time.After(30 * time.Second):
		t.Fatal("a second case never ran while the first was parked at a stop: the process registry is still held for the run")
	}

	require.NoError(t, session.Close())
	require.Len(t, (<-parked).Report.GetCases(), 1)
}
