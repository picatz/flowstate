package flowtest_test

import (
	"os"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/picatz/flowstate/pkg/flowstate/v1/flowtest"
)

// realClockBackstop is how long a case that is supposed to run on the virtual
// clock may take in real time before this package calls it a regression.
//
// It is deliberately enormous next to what these cases actually take — they
// are milliseconds — and deliberately tiny next to what the regression it
// detects would take. The regression is binary, not gradual: `flow test` runs
// every wait on [v1.VirtualClock], which never sleeps, so a case built around
// a 1-hour timeout or a 24-hour sleep either resolves in the time it takes to
// evaluate a few steps, or it resolves in an hour, or a day. There is no
// mechanism that puts it in between.
//
// So every threshold between "milliseconds" and "one hour" detects exactly the
// same defect, and the only thing a *tight* threshold adds is sensitivity to
// how busy the machine is. A one-second budget here is what actually failed
// under contention — several of these are t.Parallel() cases doing file I/O,
// YAML parsing and CEL compilation, and on a loaded box that is not reliably
// under a second, while still being four orders of magnitude away from the
// regression.
//
// The stronger half of the proof is not this constant at all: it is that these
// cases *pass*. A case asserting that a 1h `wait_for_signal:` lapsed has, by
// passing, shown that an hour of the workflow's own clock elapsed inside the
// test — which under a real clock could not have happened at all.
const realClockBackstop = time.Minute

// TestRunFileBasic exercises the happy path and the "no matching stub"
// failure mode against pkg/flowstate/v1/flowtest/testdata/basic.
func TestRunFileBasic(t *testing.T) {
	t.Parallel()

	report := flowtest.RunFile("testdata/basic/basic.test.yaml")
	require.Empty(t, report.GetRefused())
	require.Len(t, report.GetCases(), 2)

	healthy := report.GetCases()[0]
	require.Equal(t, "a healthy check reports its status", healthy.GetName())
	require.True(t, healthy.GetPassed(), "failures: %v", healthy.GetFailures())

	unmatched := report.GetCases()[1]
	require.Equal(t, "an unmatched stub fails the case with a diagnostic", unmatched.GetName())
	require.True(t, unmatched.GetPassed(), "failures: %v", unmatched.GetFailures())
}

// TestRunFileSleepIsInstant is the proof-of-bite from #155: a workflow that
// sleeps for a day runs, under `flow test`, in well under a second.
func TestRunFileSleepIsInstant(t *testing.T) {
	t.Parallel()

	started := time.Now()
	report := flowtest.RunFile("testdata/sleep/sleep.test.yaml")
	elapsed := time.Since(started)

	require.Empty(t, report.GetRefused())
	require.Len(t, report.GetCases(), 1)
	require.True(t, report.GetCases()[0].GetPassed(), "failures: %v", report.GetCases()[0].GetFailures())
	// The case passing is itself the proof of #155: the workflow's `sleep: 24h`
	// completed, which on a real clock takes a day. See [realClockBackstop] for
	// why the remaining check is wide rather than tight.
	require.Less(t, elapsed, realClockBackstop, "a 24h sleep took %s to test", elapsed)
}

// TestRunFileGate exercises a scripted signal racing a wait's own timeout, in
// both directions: delivered before the deadline, and never delivered at
// all.
func TestRunFileGate(t *testing.T) {
	t.Parallel()

	started := time.Now()
	report := flowtest.RunFile("testdata/gate/gate.test.yaml")
	elapsed := time.Since(started)

	require.Empty(t, report.GetRefused())
	require.Len(t, report.GetCases(), 2)
	for _, c := range report.GetCases() {
		require.True(t, c.GetPassed(), "%s: failures: %v", c.GetName(), c.GetFailures())
	}
	// The second case's timeout is one hour on the workflow's own clock, and
	// that case having *passed* — the gate lapsed, `deploy` was skipped — is
	// what shows the hour elapsed virtually rather than really. See
	// [realClockBackstop] for why the check below is wide rather than tight.
	require.Less(t, elapsed, realClockBackstop,
		"two cases built on a 1h timeout took %s, so the virtual clock is not what they ran on", elapsed)
}

// TestRunFileUndo checks that a stub answering a failure exercises real
// compensation through the real local driver, and that the report's
// `expect.compensated` reads the undo log's own account.
func TestRunFileUndo(t *testing.T) {
	t.Parallel()

	report := flowtest.RunFile("testdata/undo/undo.test.yaml")
	require.Empty(t, report.GetRefused())
	require.Len(t, report.GetCases(), 1)
	require.True(t, report.GetCases()[0].GetPassed(), "failures: %v", report.GetCases()[0].GetFailures())
}

// TestRunFileReportsAFailedExpectation checks that a case whose expectation
// does not hold is reported as failed with a legible diagnostic, rather than
// a passing case or a hard error — this is what an author actually sees when
// they write a wrong assertion.
func TestRunFileReportsAFailedExpectation(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	writeFile(t, dir+"/workflow.yaml", `
edition: v2026.2
name: wrong-output
steps:
  - id: fetch
    log:
      message: hi
outputs:
  greeting:
    value: ${'hi'}
`)
	writeFile(t, dir+"/wrong.test.yaml", `
tests:
  - name: expects the wrong greeting
    workflow: ./workflow.yaml
    stubs:
      - task: log
        returns: {}
    expect:
      outputs:
        greeting: "bye"
`)

	report := flowtest.RunFile(dir + "/wrong.test.yaml")
	require.Empty(t, report.GetRefused())
	require.Len(t, report.GetCases(), 1)

	c := report.GetCases()[0]
	require.False(t, c.GetPassed())
	require.Len(t, c.GetFailures(), 1)
	require.Equal(t, "expect.outputs", c.GetFailures()[0].GetField())
	require.Contains(t, c.GetFailures()[0].GetMessage(), "bye")
}

// TestLoadRejectsAnUnknownField checks that a typo in a test file is reported
// rather than silently ignored — the same rule CLAUDE.md's "diagnostics are a
// feature" states for a Flowfile itself: a misspelled key must be reported,
// not ignored.
func TestLoadRejectsAnUnknownField(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	path := dir + "/typo.test.yaml"
	writeFile(t, path, `
tests:
  - name: a test with a typo
    workflow: ./workflow.yaml
    expectt:
      outputs: {}
`)

	_, err := flowtest.Load(path)
	require.Error(t, err)
}

// TestLoadEnforcesBounds checks that the bounds on stubs, signals, and test
// count are actually reached rather than merely declared — a test file is
// untrusted input like any Flowfile (CLAUDE.md).
func TestLoadEnforcesBounds(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()

	var sb []byte
	sb = append(sb, "tests:\n  - name: too many stubs\n    workflow: ./workflow.yaml\n    stubs:\n"...)
	for i := 0; i < flowtest.MaxStubsPerTest+1; i++ {
		sb = append(sb, "      - task: http\n        returns: {}\n"...)
	}
	path := dir + "/too-many-stubs.test.yaml"
	writeFile(t, path, string(sb))

	_, err := flowtest.Load(path)
	require.Error(t, err)
	require.Contains(t, err.Error(), "stubs")
}

// TestExpectRanIsJudgedAgainstAFailedRunsPartialTranscript is the negative
// direction of issue #453, and the half a coverage assertion cannot reach.
//
// `expect.ran` and `expect.skipped` were skipped outright on a failed run, so a
// case could claim anything at all about which steps ran and be believed — which
// is the same blindness coverage had, on the surface an author is likelier to
// look at. A test that only asserts a *true* claim now passes cannot see that,
// because a harness which still ignores these on failure also reports no failure.
// So the claims here are false on purpose, and each must be reported.
//
// Both directions are wrong in the same file: `after` never ran and is claimed to
// have, `first` ran and is claimed to have been skipped. Asserting only the first
// would pass against a fix that checks `ran:` and forgets `skipped:`.
func TestExpectRanIsJudgedAgainstAFailedRunsPartialTranscript(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	writeFile(t, dir+"/workflow.yaml", `
edition: v2026.2
name: fails-partway
steps:
  - id: first
    log:
      message: ran
  - id: boom
    http:
      url: https://example.com/boom
  - id: after
    log:
      message: unreachable
`)
	writeFile(t, dir+"/x.test.yaml", `
tests:
  - name: the case lies about what ran, and the run failed
    workflow: ./workflow.yaml
    stubs:
      - task: log
        returns: {}
      - task: http
        fails:
          kind: Upstream
          message: upstream said no
    expect:
      failed: true
      ran: [after]
      skipped: [first]
`)

	report := flowtest.RunFile(dir + "/x.test.yaml")
	require.Empty(t, report.GetRefused())
	require.Len(t, report.GetCases(), 1)

	result := report.GetCases()[0]
	require.False(t, result.GetPassed(),
		"a false ran:/skipped: claim about a failed run must be reported, not skipped")

	fields := map[string]string{}
	for _, failure := range result.GetFailures() {
		fields[failure.GetField()] = failure.GetStep()
	}
	require.Equal(t, "after", fields["expect.ran"],
		"the step claimed to have run, and did not, must be named")
	require.Equal(t, "first", fields["expect.skipped"],
		"the step claimed to have been skipped, and did not, must be named")
}

func writeFile(t *testing.T, path, contents string) {
	t.Helper()
	require.NoError(t, os.WriteFile(path, []byte(contents), 0o644))
}
