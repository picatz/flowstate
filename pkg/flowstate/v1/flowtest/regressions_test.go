package flowtest_test

import (
	"fmt"
	"net"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	v1 "github.com/picatz/flowstate/pkg/flowstate/v1"
	"github.com/picatz/flowstate/pkg/flowstate/v1/flowtest"
	"github.com/picatz/flowstate/pkg/flowstate/v1/netpolicy"
)

// This file pins the six findings Codex raised on PR #190 against
// claude/flow-test, one test (or pair) per finding, named after the finding
// so a reader can match a report back to a test.

// TestP1RunErrorWithoutExpectFailedFailsTheCase is P1-1: a case asserting
// something about outputs must not read as passed just because the run that
// was supposed to produce them errored out instead and nothing else was left
// to complain — a green test that should be red is the worst failure mode a
// test framework can have.
func TestP1RunErrorWithoutExpectFailedFailsTheCase(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	writeFile(t, dir+"/workflow.yaml", `
edition: v2026.3
name: crashes
steps:
  - id: fetch
    http:
      method: GET
      url: https://example.internal/status
outputs:
  status:
    value: ${steps.fetch.status}
`)
	// No stub at all for http: the task fails closed (see the P1-2 tests
	// below), the run errors, and this case never said that could happen.
	writeFile(t, dir+"/x.test.yaml", `
tests:
  - name: expects outputs from a workflow that errors
    workflow: ./workflow.yaml
    expect:
      outputs:
        status: 200
`)

	report := flowtest.RunFile(dir + "/x.test.yaml")
	require.Empty(t, report.GetRefused())
	require.Len(t, report.GetCases(), 1)

	c := report.GetCases()[0]
	require.False(t, c.GetPassed(), "a run that errored unexpectedly was reported as a passing case")
	require.NotEmpty(t, c.GetFailures(), "an unexpected failure must itself be reported")
	require.Contains(t, c.GetFailures()[0].GetMessage(), "unexpectedly")
}

// TestP1ExpectedFailureStillPasses is P1-1's other direction: a case that
// explicitly declares `expect.failed: true` must still pass when the run
// fails for the declared reason — the fix must not turn every failing run
// into a failing case regardless of what was asked for.
func TestP1ExpectedFailureStillPasses(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	writeFile(t, dir+"/workflow.yaml", `
edition: v2026.3
name: fails-on-purpose
steps:
  - id: fetch
    http:
      method: GET
      url: https://example.internal/status
`)
	writeFile(t, dir+"/x.test.yaml", `
tests:
  - name: a case that expects the run to fail
    workflow: ./workflow.yaml
    stubs:
      - task: http
        fails:
          kind: Upstream
          message: quota exceeded
    expect:
      failed: true
      error_contains: quota exceeded
`)

	report := flowtest.RunFile(dir + "/x.test.yaml")
	require.Empty(t, report.GetRefused())
	require.Len(t, report.GetCases(), 1)

	c := report.GetCases()[0]
	require.True(t, c.GetPassed(), "failures: %v", c.GetFailures())
}

// TestP2UnstubbedTaskFailsClosedWithoutDialing is P1-2: a task this case
// never stubbed must fail the case rather than run for real — and, proven
// here rather than merely asserted, must never actually reach the network to
// find that out. Loopback is explicitly allowed on the registered `http`
// task for the duration of this test (the same exemption
// pkg/flowstate/v1/internal/conformance states for itself), which is what makes "zero
// connections" evidence of the fix rather than of the ordinary egress
// policy: with loopback allowed, a real dial to the listener below would
// succeed if flow test ever let the real task run.
func TestP2UnstubbedTaskFailsClosedWithoutDialing(t *testing.T) {
	t.Parallel()

	ln, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	t.Cleanup(func() { _ = ln.Close() })

	var connections int32
	go func() {
		for {
			conn, err := ln.Accept()
			if err != nil {
				return
			}
			atomic.AddInt32(&connections, 1)
			_ = conn.Close()
		}
	}()

	policy, err := netpolicy.New(netpolicy.WithAllowLoopback())
	require.NoError(t, err)

	registry := v1.DefaultRegistry()
	original, existed := registry.Lookup("http")
	require.True(t, existed)
	require.NoError(t, registry.Register(v1.HTTPTaskDef(policy)))
	t.Cleanup(func() { _ = registry.Register(original) })

	dir := t.TempDir()
	writeFile(t, dir+"/workflow.yaml", fmt.Sprintf(`
edition: v2026.3
name: unstubbed
steps:
  - id: fetch
    http:
      method: GET
      url: http://%s/
`, ln.Addr().String()))
	writeFile(t, dir+"/x.test.yaml", `
tests:
  - name: no stub declared for http
    workflow: ./workflow.yaml
    expect:
      failed: true
      error_contains: "declares no stub"
`)

	report := flowtest.RunFile(dir + "/x.test.yaml")
	require.Empty(t, report.GetRefused())
	require.Len(t, report.GetCases(), 1)

	c := report.GetCases()[0]
	require.True(t, c.GetPassed(), "failures: %v", c.GetFailures())

	// A real dial, if the fix regressed, would land on this listener well
	// inside this window — 50ms of headroom on a loopback connection that
	// either happens immediately or never does.
	require.Never(t, func() bool {
		return atomic.LoadInt32(&connections) > 0
	}, 50*time.Millisecond, 5*time.Millisecond,
		"flow test dialed the network for a task this case declared no stub for")
}

// TestP1ScriptedSignalAfterTimeoutStillLapses is P1-3: a signal scripted for
// a virtual moment after a wait's own timeout must not be visible to that
// wait — the gate has to lapse on schedule, exactly as it would if nothing
// were ever going to answer it.
//
// Before the fix, [scriptSignals] delivered every signal immediately upon
// being scheduled (in real time, via VirtualClock.Advance called eagerly),
// which put it in [v1.LocalSignals]'s buffer well before the workload's own
// wait_for_signal ever reached it — so a signal timestamped for *after* the
// timeout was, in practice, already there *before* the wait even started.
func TestP1ScriptedSignalAfterTimeoutStillLapses(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	writeFile(t, dir+"/workflow.yaml", `
edition: v2026.3
name: gate
steps:
  - id: approval
    wait_for_signal:
      name: deploy-approved
      timeout: 1h
  - id: deploy
    if: ${!approval.timed_out}
    log:
      message: deploying
`)
	writeFile(t, dir+"/x.test.yaml", `
tests:
  - name: a signal scripted after the timeout does not release the gate
    workflow: ./workflow.yaml
    signals:
      - name: deploy-approved
        at: 2h
        payload:
          approved: true
    stubs:
      - task: log
        returns: {}
    expect:
      ran: [approval]
      skipped: [deploy]
`)

	started := time.Now()
	report := flowtest.RunFile(dir + "/x.test.yaml")
	elapsed := time.Since(started)

	require.Empty(t, report.GetRefused())
	require.Len(t, report.GetCases(), 1)
	c := report.GetCases()[0]
	require.True(t, c.GetPassed(), "failures: %v", c.GetFailures())

	// The passing case is the whole of P1-3's proof, and it is an ordering
	// claim rather than a timing one: the gate lapsed at its 1h timeout and
	// `deploy` was skipped, so the signal scripted for 2h was not visible to a
	// wait that ended at 1h. [v1.VirtualClock] decides that ordering
	// deterministically — it advances only once every registered participant is
	// parked, and then only to the earliest pending deadline, all under one
	// mutex — so which of the two moments comes first is not a race the Go
	// scheduler can lose under load.
	//
	// What *was* load-sensitive here was the check below, when it read
	// `time.Second`: a stopwatch on a t.Parallel() case that writes two files,
	// parses YAML and compiles CEL. See [realClockBackstop].
	require.Less(t, elapsed, realClockBackstop,
		"a 1h timeout and a 2h signal took %s to resolve", elapsed)
}

// TestP1ScriptedSignalsDeliverInTimestampOrder is P1-3's other half: two
// signals scripted out of declaration order must still be delivered in
// timestamp order — the clock's own earliest-deadline-first rule, not
// whichever sender goroutine the Go scheduler happened to run first.
//
// Both waits here are untimed, which is deliberate: an untimed
// `wait_for_signal:` never registers a timer of its own with the clock (see
// [v1.LeaveClockWhile]'s doc), so this also exercises that a scripted
// signal's own timer is what the clock's auto-advance has to run on when the
// workload itself has nothing pending — the case this repo's own P1-3 fix
// had to get right to avoid deadlocking rather than merely reordering.
func TestP1ScriptedSignalsDeliverInTimestampOrder(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	writeFile(t, dir+"/workflow.yaml", `
edition: v2026.3
name: two-gates
steps:
  - id: first
    wait_for_signal:
      name: a
  - id: second
    wait_for_signal:
      name: b
outputs:
  order:
    value: '${steps.first.payload.order + "," + steps.second.payload.order}'
`)
	// Declared in reverse of the timestamp order they must arrive in: "b" is
	// written first but scheduled for 10m, "a" is written second but
	// scheduled for 1m — the workflow waits for "a" first, so this only
	// passes if delivery follows the scheduled instants and not the list.
	writeFile(t, dir+"/x.test.yaml", `
tests:
  - name: signals scripted out of order still arrive in timestamp order
    workflow: ./workflow.yaml
    signals:
      - name: b
        at: 10m
        payload:
          order: second
      - name: a
        at: 1m
        payload:
          order: first
    expect:
      outputs:
        order: "first,second"
`)

	started := time.Now()
	report := flowtest.RunFile(dir + "/x.test.yaml")
	elapsed := time.Since(started)

	require.Empty(t, report.GetRefused())
	require.Len(t, report.GetCases(), 1)
	c := report.GetCases()[0]
	require.True(t, c.GetPassed(), "failures: %v", c.GetFailures())
	// realClockBackstop, not a one-second budget: this case resolves two
	// `wait_for_signal:` gates ten virtual minutes apart on [v1.VirtualClock],
	// which never sleeps, so what this measures is real wall time spent
	// parsing, scheduling and running the case — not the ten minutes it
	// simulates. A tight bound here is exactly the load-sensitive assertion
	// [realClockBackstop]'s own doc comment describes; this case was the one
	// instance in this file that predated that constant and was never moved
	// onto it. See issue #431.
	require.Less(t, elapsed, realClockBackstop,
		"delivering two scripted signals took %s in real time", elapsed)
}

// TestP1LoadRejectsAnAliasBomb is P1-4: an alias-expansion bomb well under
// [flowtest.MaxTestFileBytes] must still be refused, naming the bound, and
// must not actually expand — proven here by elapsed time staying small,
// because a document shaped like this one that *did* expand would not
// return from yaml.Unmarshal on any practical time budget at all.
//
// Nine levels of ten-way branching multiplies out to 10^9 leaf references
// from a document under 2 KiB — the classic billion-laughs shape CLAUDE.md
// names by exactly that name.
func TestP1LoadRejectsAnAliasBomb(t *testing.T) {
	t.Parallel()

	var doc string
	doc += "a0: &a0 [x,x,x,x,x,x,x,x,x,x]\n"
	for i := 1; i <= 8; i++ {
		doc += fmt.Sprintf("a%d: &a%d [", i, i)
		for j := 0; j < 10; j++ {
			if j > 0 {
				doc += ","
			}
			doc += fmt.Sprintf("*a%d", i-1)
		}
		doc += "]\n"
	}
	doc += "tests: *a8\n"

	require.Less(t, len(doc), flowtest.MaxTestFileBytes,
		"the bomb must fit comfortably under the byte bound, or it would only prove that bound catches it")

	dir := t.TempDir()
	path := dir + "/bomb.test.yaml"
	writeFile(t, path, doc)

	started := time.Now()
	_, err := flowtest.Load(path)
	elapsed := time.Since(started)

	require.Error(t, err)
	require.Contains(t, err.Error(), "expanded")
	require.Less(t, elapsed, 2*time.Second,
		"Load took %s; the expansion bound was not enforced before the document was expanded", elapsed)
}

// TestP2PluginTaskStubCompilesAndRuns is P2-1: a stub naming a task this
// build does not otherwise register — a plugin task's dotted name — must be
// usable end to end, including compiling the workflow that calls it, not
// merely dispatchable once compiled.
func TestP2PluginTaskStubCompilesAndRuns(t *testing.T) {
	t.Parallel()
	const taskName = "slack.post"
	_, registeredBefore := v1.DefaultRegistry().Lookup(taskName)
	require.False(t, registeredBefore, "the synthetic task name must be unique to this test")

	dir := t.TempDir()
	writeFile(t, dir+"/workflow.yaml", `
edition: v2026.3
name: plugin-wf
steps:
  - id: a
    slack.post:
      channel: general
`)
	writeFile(t, dir+"/x.test.yaml", `
tests:
  - name: stub a plugin task this build never registered
    workflow: ./workflow.yaml
    stubs:
      - task: slack.post
        returns:
          ok: true
    expect:
      ran: [a]
`)

	report := flowtest.RunFile(dir + "/x.test.yaml")
	require.Empty(t, report.GetRefused())
	require.Len(t, report.GetCases(), 1)
	c := report.GetCases()[0]
	require.True(t, c.GetPassed(), "error: %s failures: %v", c.GetError(), c.GetFailures())
	_, registeredAfter := v1.DefaultRegistry().Lookup(taskName)
	require.False(t, registeredAfter, "a synthetic task must not escape its test case")
}

// TestPluginTaskStubByStepIdCompilesAndRuns is the step-form counterpart of
// TestP2PluginTaskStubCompilesAndRuns: the same plugin task this build never
// registers, stubbed by the step id that invokes it rather than by the task
// name, must compile and run just the same. The concern is that resolving a
// step id to its task needs the compiled workflow, which lands after the parse
// a task-form stub pre-registers a synthetic shape for. It holds because
// [flowfile.Parse] does not reject an unregistered task on its own (a bare or
// dotted unknown name parses; task existence is a separate validation the run
// path does not invoke), so the workflow compiles, the step resolves to
// slack.post, and the case registry answers it.
func TestPluginTaskStubByStepIdCompilesAndRuns(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	writeFile(t, dir+"/workflow.yaml", `
edition: v2026.3
name: plugin-wf
steps:
  - id: a
    slack.post:
      channel: general
`)
	writeFile(t, dir+"/x.test.yaml", `
tests:
  - name: stub a plugin task by the step that invokes it
    workflow: ./workflow.yaml
    stubs:
      - step: a
        returns:
          ok: true
    expect:
      ran: [a]
`)

	report := flowtest.RunFile(dir + "/x.test.yaml")
	require.Empty(t, report.GetRefused())
	require.Len(t, report.GetCases(), 1)
	c := report.GetCases()[0]
	require.True(t, c.GetPassed(), "error: %s failures: %v", c.GetError(), c.GetFailures())
}

// TestP2LargeInt64PrecisionIsNotLost is P2-2: two int64s on either side of
// float64's 2^53 mantissa boundary must compare as different — a comparison
// that round-trips both sides through float64 first would make this pass for
// the wrong reason, which is worse than an assertion that cannot express the
// difference at all.
func TestP2LargeInt64PrecisionIsNotLost(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	writeFile(t, dir+"/workflow.yaml", `
edition: v2026.3
name: big-number
steps:
  - id: a
    log:
      message: hi
outputs:
  n:
    value: ${9007199254740993}
`)
	writeFile(t, dir+"/x.test.yaml", `
tests:
  - name: a value one past the float64 mantissa boundary must not compare equal to its neighbor
    workflow: ./workflow.yaml
    stubs:
      - task: log
        returns: {}
    expect:
      outputs:
        n: 9007199254740992
`)

	report := flowtest.RunFile(dir + "/x.test.yaml")
	require.Empty(t, report.GetRefused())
	require.Len(t, report.GetCases(), 1)

	c := report.GetCases()[0]
	require.False(t, c.GetPassed(),
		"9007199254740992 and 9007199254740993 compared equal — the comparison is losing precision through float64")
	require.Len(t, c.GetFailures(), 1)
	require.Equal(t, "expect.outputs", c.GetFailures()[0].GetField())
}

// TestIsolationHoldsUnderConcurrentRunFile is issue #195's regression: the
// no-network guarantee has to be a property of the run, not of a window in
// which a process-global registry happens to be swapped.
//
// Before the per-case registry ([v1.NewContextWithRegistry]), two RunFile
// calls racing each other could each observe the other's swap window, and what
// escaped was a real task doing real work — a genuine DNS lookup out of an
// http step whose case had stubbed it. The old design could not fix this by
// locking harder: the critical sections were already serialized, and the leak
// happened anyway.
//
// Deliberately runs many cases concurrently and asserts every one of them
// failed *closed* — the unstubbed task refused by name — rather than reaching
// a network. An unreachable-by-design host is used so that a leak fails loudly
// (a DNS error naming the host) instead of hanging.
func TestIsolationHoldsUnderConcurrentRunFile(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	writeFile(t, dir+"/workflow.yaml", `
edition: v2026.3
name: unstubbed
steps:
  - id: fetch
    http:
      method: GET
      url: https://isolation-must-hold.invalid/probe
`)
	writeFile(t, dir+"/x.test.yaml", `
tests:
  - name: an unstubbed task fails closed and never reaches the network
    workflow: ./workflow.yaml
    expect:
      failed: true
      error_contains: "declares no stub"
`)

	const racers = 24
	var wg sync.WaitGroup
	failures := make(chan string, racers)

	for i := 0; i < racers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			report := flowtest.RunFile(dir + "/x.test.yaml")
			if refused := report.GetRefused(); refused != "" {
				failures <- "refused: " + refused
				return
			}
			for _, c := range report.GetCases() {
				if !c.GetPassed() {
					failures <- fmt.Sprintf("case %q failed: %v", c.GetName(), c.GetFailures())
				}
			}
		}()
	}

	wg.Wait()
	close(failures)

	var got []string
	for f := range failures {
		got = append(got, f)
	}
	require.Empty(t, got, "isolation leaked under concurrent RunFile: %v", got)
}
