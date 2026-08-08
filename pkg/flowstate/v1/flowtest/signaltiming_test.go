package flowtest_test

import (
	"strconv"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/picatz/flowstate/pkg/flowstate/v1/flowtest"
)

// This file pins #278: `flow test` can script *when* a signal arrives, not
// merely that it does.
//
// # What was wrong, and why every existing case was blind to it
//
// A bounded `wait_for_signal:` used to hand its clock participation to a
// helper goroutine for the duration of the wait and take it back afterwards
// (`waitForSignalLocally`, pkg/flowstate/v1/wait_local.go). Between the helper
// dropping its slot and the waiting goroutine reclaiming one, a scripted
// signal parked on some far later moment was the [v1.VirtualClock]'s only
// participant — so the clock advanced straight to that moment and delivered
// it. Every `at:` past the first deadline therefore collapsed onto the first
// lapse: `at: 721h` and `at: 3000h` produced identical runs.
//
// The corpus could not see this, and the reason is the reason it shipped:
// every gate case in it scripted a signal for the *first* open wait, which is
// exactly the situation where the collapse is invisible — there is no second
// period for a signal to be wrongly hoisted out of. So the cases below are
// deliberately about the traversal rather than the step. They script for the
// third period and assert the run reached the third period: two lapses, no
// more and no fewer. A test that only asserted "at most three" would be
// satisfied by the collapsed run this file exists to keep out.

// periodsWorkflow is a reminder-shaped loop: one bounded gate per iteration,
// a lapse means "go round again", a delivered signal means "stop". `lapses`
// counts the iterations whose wait actually lapsed, so it *is* the period the
// signal landed in, minus one.
const periodsWorkflow = `
edition: v2026.2
name: periods
steps:
  - id: periods
    loop:
      as: n
      init: ${0}
      update: ${n + 1}
      until: ${!steps.gate.timed_out}
      max_iterations: 20
      steps:
        - id: gate
          wait_for_signal:
            name: go
            timeout: 1h
outputs:
  lapses:
    value: ${steps.periods.state}
  answered:
    value: ${!steps.periods.results[steps.periods.results.size() - 1].gate.timed_out}
`

// TestScriptedSignalLandsInThePeriodItNames is the core of #278: a signal
// scripted for a moment inside the third period is delivered in the third
// period.
//
// Each sub-case names a different period on the *same* workflow, and the
// expected lapse count is asserted exactly. That exactness is the point: the
// defect made every one of these produce one lapse, so a table that pinned
// only "the loop stopped and the signal was seen" would have passed
// throughout. Reaching the third period is the bound this asserts was
// *reached*, not merely not exceeded.
func TestScriptedSignalLandsInThePeriodItNames(t *testing.T) {
	t.Parallel()

	for _, tc := range []struct {
		name       string
		at         string
		wantLapses int
	}{
		// Inside the first period: the gate is answered before it ever
		// lapses, and nothing periodic happens at all. This is the case the
		// old corpus was made of, and it passed before the fix too — it is
		// here as the control, so a regression that broke *this* direction
		// while fixing the others is not mistaken for the fix.
		{name: "the first period", at: "30m", wantLapses: 0},
		// One minute past the first deadline. Under the defect this was
		// indistinguishable from the case above's opposite — delivered the
		// instant the 1h gate lapsed — and it is the tightest statement of
		// "just after" the harness can make.
		{name: "the second period, barely", at: "1h1m", wantLapses: 1},
		{name: "the third period", at: "2h30m", wantLapses: 2},
		// Far out, to show the answer tracks the arithmetic rather than
		// saturating: the ninth period is nine periods away, not "late".
		{name: "the ninth period", at: "8h15m", wantLapses: 8},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			dir := t.TempDir()
			writeFile(t, dir+"/workflow.yaml", periodsWorkflow)
			writeFile(t, dir+"/x.test.yaml", `
tests:
  - name: the signal lands where it was scripted
    workflow: ./workflow.yaml
    signals:
      - name: go
        at: `+tc.at+`
    expect:
      outputs:
        lapses: `+strconv.Itoa(tc.wantLapses)+`
        answered: true
`)

			started := time.Now()
			report := flowtest.RunFile(dir + "/x.test.yaml")
			elapsed := time.Since(started)

			require.Empty(t, report.GetRefused())
			require.Len(t, report.GetCases(), 1)
			c := report.GetCases()[0]
			require.True(t, c.GetPassed(),
				"a signal scripted for %s did not land in the period it names: %v", tc.at, c.GetFailures())

			// The gates here are an hour each and there are up to nine of
			// them, so the case passing at all is the proof they elapsed
			// virtually. See [realClockBackstop] for why this is wide.
			require.Less(t, elapsed, realClockBackstop,
				"a run of hour-long gates took %s, so it did not run on the virtual clock", elapsed)
		})
	}
}

// TestScriptedSignalBeyondEveryDeadlineIsNeverDelivered is the other end of
// the same rule. A signal scripted past the loop's whole budget of periods
// stays queued forever: every gate lapses, the loop exhausts `max_iterations:`
// and the run fails.
//
// Under the defect this run *succeeded* on its second iteration, which is a
// far worse answer than a failure — the author asked for a customer who never
// replies and was told the loop stopped because somebody did.
func TestScriptedSignalBeyondEveryDeadlineIsNeverDelivered(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	writeFile(t, dir+"/workflow.yaml", periodsWorkflow)
	writeFile(t, dir+"/x.test.yaml", `
tests:
  - name: a signal nobody's gate ever reaches
    workflow: ./workflow.yaml
    signals:
      # The loop's budget is twenty one-hour periods, so this moment is one
      # the run cannot reach: it is never delivered to anything.
      - name: go
        at: 500h
    expect:
      failed: true
      error_contains: "ran its full budget of 20 iterations"
`)

	report := flowtest.RunFile(dir + "/x.test.yaml")
	require.Empty(t, report.GetRefused())
	require.Len(t, report.GetCases(), 1)
	require.True(t, report.GetCases()[0].GetPassed(),
		"failures: %v", report.GetCases()[0].GetFailures())
}

// TestAnsweredGateDoesNotDragTheClockToItsUnusedDeadline pins the half of
// #278 that is not about participation at all but about what a wait leaves
// behind.
//
// A gate answered by its signal never fires its own timeout, and that timeout
// stays registered with [v1.VirtualClock] — a deadline the clock will happily
// advance to the moment everything registered is parked. So a gate answered at
// 1h with a 720h timeout used to drag the run's clock to 720h on its way out,
// and every later `at:` in the case was then measured from a moment the
// workflow never reached: the second gate below would see its own signal as
// already past and be answered by it, rather than lapsing.
//
// The assertion is therefore that the *second* gate lapses. That only holds if
// the clock left the first gate at 1h.
func TestAnsweredGateDoesNotDragTheClockToItsUnusedDeadline(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	writeFile(t, dir+"/workflow.yaml", `
edition: v2026.2
name: two-gates
steps:
  - id: first
    wait_for_signal:
      name: a
      # Generous, and deliberately so: this is the deadline the gate never
      # needs, and the one the clock must not inherit.
      timeout: 720h
  - id: second
    wait_for_signal:
      name: b
      timeout: 2h
outputs:
  first_timed_out:
    value: ${steps.first.timed_out}
  second_timed_out:
    value: ${steps.second.timed_out}
`)
	writeFile(t, dir+"/x.test.yaml", `
tests:
  - name: the second gate is measured from where the first one actually ended
    workflow: ./workflow.yaml
    signals:
      - name: a
        at: 1h
      # The first gate ends at 1h and the second runs to 3h, so this moment is
      # two hours beyond the run's reach. It arrives only if the clock was
      # dragged forward by a deadline nothing waited for.
      - name: b
        at: 5h
    expect:
      outputs:
        first_timed_out: false
        second_timed_out: true
`)

	report := flowtest.RunFile(dir + "/x.test.yaml")
	require.Empty(t, report.GetRefused())
	require.Len(t, report.GetCases(), 1)
	require.True(t, report.GetCases()[0].GetPassed(),
		"failures: %v", report.GetCases()[0].GetFailures())
}

// TestSignalDeliveredWhileTheRunIsElsewhereDoesNotStopTheClock is the other
// direction of the same bookkeeping, and the one that fails as a hang rather
// than as a wrong answer.
//
// A scripted signal whose moment arrives while the run is doing something else
// — asleep, here — wakes nobody. Its sender is still a participant of the
// virtual clock, and its departure is what has to let the clock carry on to the
// sleep's own deadline; a departure made quietly on the theory that it woke
// somebody leaves a run parked on a timer with nothing left to move time, and
// `flow test` hangs forever rather than failing. Nothing here reads a wall
// clock as a backstop, so the test's own timeout is the only thing that would
// ever end it.
func TestSignalDeliveredWhileTheRunIsElsewhereDoesNotStopTheClock(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	writeFile(t, dir+"/workflow.yaml", `
edition: v2026.2
name: asleep-when-it-lands
steps:
  - id: nap
    sleep: 5h
  - id: gate
    wait_for_signal:
      name: go
      timeout: 1h
outputs:
  timed_out:
    value: ${steps.gate.timed_out}
`)
	writeFile(t, dir+"/x.test.yaml", `
tests:
  - name: a signal that lands mid-sleep waits in the queue for the gate that follows
    workflow: ./workflow.yaml
    signals:
      # An hour in, which the run spends asleep: nothing is waiting for this
      # when it is delivered, and it is read by the gate four hours later.
      - name: go
        at: 1h
    expect:
      ran: [nap, gate]
      outputs:
        timed_out: false
`)

	started := time.Now()
	report := flowtest.RunFile(dir + "/x.test.yaml")
	elapsed := time.Since(started)

	require.Empty(t, report.GetRefused())
	require.Len(t, report.GetCases(), 1)
	require.True(t, report.GetCases()[0].GetPassed(),
		"failures: %v", report.GetCases()[0].GetFailures())
	require.Less(t, elapsed, realClockBackstop,
		"a 5h sleep with a signal landing inside it took %s", elapsed)
}

// TestAGateAnsweredBeforeItBlocksDoesNotSpendItsTimeout is the delivery that
// arrived first, and it is a statement about the clock rather than about the
// answer.
//
// A signal scripted with no `at:` is delivered at the epoch, before the run has
// reached the gate at all, so the gate never blocks. It must also never spend
// its `timeout:`: a deadline registered by a wait that is not going to wait is
// still a moment a virtual clock with nothing else to hold it back will advance
// straight to, and the run's later moments are then measured from a month it
// never lived through. The second gate lapsing at 3h rather than being answered
// by a signal scripted for 5h is what says it did not.
func TestAGateAnsweredBeforeItBlocksDoesNotSpendItsTimeout(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	writeFile(t, dir+"/workflow.yaml", `
edition: v2026.2
name: answered-early
steps:
  - id: first
    wait_for_signal:
      name: a
      timeout: 720h
  - id: second
    wait_for_signal:
      name: b
      timeout: 2h
outputs:
  first_timed_out:
    value: ${steps.first.timed_out}
  second_timed_out:
    value: ${steps.second.timed_out}
`)
	writeFile(t, dir+"/x.test.yaml", `
tests:
  - name: the gate that never blocked did not spend a month either
    workflow: ./workflow.yaml
    signals:
      # No at: at all, so this is delivered at the epoch, before the run
      # reaches the gate.
      - name: a
      # The run reaches 2h if the first gate cost nothing, so this is three
      # hours out of reach. It arrives only if the first gate's unused month
      # was charged to the clock.
      - name: b
        at: 5h
    expect:
      outputs:
        first_timed_out: false
        second_timed_out: true
`)

	report := flowtest.RunFile(dir + "/x.test.yaml")
	require.Empty(t, report.GetRefused())
	require.Len(t, report.GetCases(), 1)
	require.True(t, report.GetCases()[0].GetPassed(),
		"failures: %v", report.GetCases()[0].GetFailures())
}
