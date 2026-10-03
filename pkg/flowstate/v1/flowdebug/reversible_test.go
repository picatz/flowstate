package flowdebug_test

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	v1 "github.com/picatz/flowstate/pkg/flowstate/v1"
	"github.com/picatz/flowstate/pkg/flowstate/v1/flowdebug"
	"github.com/picatz/flowstate/pkg/flowstate/v1/flowfile"
)

// launches counts what a test launcher started and what ended, so a test can
// prove a rewind leaves no run behind.
type launches struct {
	started, stopped atomic.Int64

	mu     sync.Mutex
	done   []chan struct{}
	finals map[int]v1.DebugRunState
}

// launcher starts workflow under a fresh controlled session each time, the way
// a host that has stubbed its effects would. next, when set, chooses the
// workflow for the nth launch (from zero), to stage a run that is not
// deterministic.
func (l *launches) launcher(workflow func(n int) *v1.Workflow, configure func(*flowdebug.Session)) flowdebug.Launcher {
	return func(ctx context.Context) (*flowdebug.Run, error) {
		n := int(l.started.Add(1)) - 1
		program := workflow(n)
		session, err := flowdebug.New(flowdebug.Options{Controlled: true, Out: &strings.Builder{}, Workflow: program})
		if err != nil {
			return nil, err
		}
		if configure != nil {
			configure(session)
		}
		runCtx, cancel := context.WithCancel(context.Background())
		done := make(chan struct{})
		l.mu.Lock()
		l.done = append(l.done, done)
		l.mu.Unlock()
		go func() {
			defer close(done)
			runCtx = v1.NewContextWithDebugger(runCtx, session)
			runCtx = v1.NewContextWithRunObserver(runCtx, session)
			_, err := v1.RunWithInputs(runCtx, program, nil)
			session.Finished(err)
		}()

		return &flowdebug.Run{Session: session, Stop: func() {
			// Cancelled before the session is released: closing first would let
			// the abandoned run carry on through its remaining steps.
			cancel()
			_ = session.Close()
			<-done
			if final, err := session.Snapshot(context.Background()); err == nil {
				l.mu.Lock()
				if l.finals == nil {
					l.finals = map[int]v1.DebugRunState{}
				}
				l.finals[n] = final.GetState()
				l.mu.Unlock()
			}
			l.stopped.Add(1)
		}}, nil
	}
}

// parseJourney compiles the journey workflow and the child it calls.
func parseJourney(t *testing.T) *v1.Workflow {
	t.Helper()

	dir := t.TempDir()
	for name, text := range map[string]string{"main.yaml": journeyFlowfile, "child.yaml": childFlowfile} {
		require.NoError(t, os.WriteFile(filepath.Join(dir, name), []byte(text), 0o600))
	}
	workflow, _, err := flowfile.ParseFile(filepath.Join(dir, "main.yaml"))
	require.NoError(t, err)

	return workflow
}

// shown is what a person saw at a stop, without its revision, which a rewind is
// meant to change.
type shown struct {
	state        v1.DebugRunState
	reason       v1.DebugStopReason
	address      string
	observations []string
}

func shownAt(snapshot *v1.DebugSnapshot) shown {
	at := shown{state: snapshot.GetState(), reason: snapshot.GetReason(), address: snapshot.GetOccurrence().GetAddress()}
	for _, observation := range snapshot.GetObservations() {
		at.observations = append(at.observations, observation.GetText())
	}

	return at
}

// reversing is one Reversible over the journey, with the helpers a test drives
// it by.
type reversing struct {
	t        *testing.T
	target   *flowdebug.Reversible
	launches *launches
	ids      int
}

func newReversing(t *testing.T, workflow func(n int) *v1.Workflow, configure func(*flowdebug.Session)) *reversing {
	t.Helper()

	l := &launches{}
	target, err := flowdebug.NewReversible(t.Context(), l.launcher(workflow, configure))
	require.NoError(t, err)
	t.Cleanup(target.Stop)

	return &reversing{t: t, target: target, launches: l}
}

func (r *reversing) id(prefix string) string {
	r.ids++

	return fmt.Sprintf("%s-%d", prefix, r.ids)
}

// first is the entry stop.
func (r *reversing) first() *v1.DebugSnapshot {
	r.t.Helper()

	return waitHeld(r.t, r.target, 0)
}

// move resumes from at and waits for the next stop.
func (r *reversing) move(at *v1.DebugSnapshot, action v1.DebugResumeAction) *v1.DebugSnapshot {
	r.t.Helper()

	receipt, err := r.target.Resume(r.t.Context(), &v1.DebugResumeRequest{
		RequestId:        r.id("move"),
		ExpectedRevision: at.GetRevision(),
		Action:           action,
	})
	require.NoError(r.t, err)
	require.Equal(r.t, v1.DebugCommandStatus_DEBUG_COMMAND_STATUS_APPLIED, receipt.GetStatus(), receipt.GetMessage())

	return waitHeld(r.t, r.target, at.GetRevision())
}

// back rewinds and returns the receipt and the stop it left the session at.
func (r *reversing) back(expected uint64) (*v1.DebugReceipt, *v1.DebugSnapshot) {
	r.t.Helper()

	receipt, err := r.target.Back(r.t.Context(), r.id("back"), expected)
	require.NoError(r.t, err)
	snapshot, err := r.target.Snapshot(r.t.Context())
	require.NoError(r.t, err)

	return receipt, snapshot
}

const appliedStatus = v1.DebugCommandStatus_DEBUG_COMMAND_STATUS_APPLIED

// TestBackReachesWhatTheFirstVisitShowed walks the journey (a loop, a parallel
// block and a call) to its end, then goes back stop by stop to the first one.
// Every stop must show exactly what it showed the first time, and every
// revision must be higher than any shown before it, rewind or not.
func TestBackReachesWhatTheFirstVisitShowed(t *testing.T) {
	t.Parallel()

	workflow := parseJourney(t)
	run := newReversing(t, func(int) *v1.Workflow { return workflow }, nil)

	var visited []shown
	var highest uint64
	note := func(snapshot *v1.DebugSnapshot) {
		t.Helper()
		assert.Greater(t, snapshot.GetRevision(), highest, "a revision did not increase")
		highest = snapshot.GetRevision()
	}

	at := run.first()
	note(at)
	visited = append(visited, shownAt(at))
	for at.GetState() == v1.DebugRunState_DEBUG_RUN_STATE_HELD {
		at = run.move(at, v1.DebugResumeAction_DEBUG_RESUME_ACTION_STEP_IN)
		note(at)
		visited = append(visited, shownAt(at))
	}
	require.Greater(t, len(visited), 8, "the journey was meant to have many stops")
	require.Equal(t, v1.DebugRunState_DEBUG_RUN_STATE_COMPLETED, at.GetState())

	for i := len(visited) - 2; i >= 0; i-- {
		receipt, snapshot := run.back(0)
		require.Equal(t, appliedStatus, receipt.GetStatus(), "stop %d: %s", i, receipt.GetMessage())
		assert.Equal(t, visited[i], shownAt(snapshot), "stop %d shows something other than the first visit did", i)
		assert.True(t, snapshot.GetCapabilities().GetReverse(), "the rewound session did not say it can go back")
		note(snapshot)
		assert.Equal(t, snapshot.GetRevision(), receipt.GetRevision())
	}

	// At the first stop there is nowhere left to go, and saying so changes nothing.
	receipt, snapshot := run.back(0)
	assert.Equal(t, v1.DebugCommandStatus_DEBUG_COMMAND_STATUS_REFUSED, receipt.GetStatus())
	assert.Contains(t, receipt.GetMessage(), "first stop")
	assert.Equal(t, visited[0], shownAt(snapshot))

	// And forward again from a rewound stop is the same run: the same stops.
	again := snapshot
	for i := 1; i < len(visited); i++ {
		again = run.move(again, v1.DebugResumeAction_DEBUG_RESUME_ACTION_STEP_IN)
		assert.Equal(t, visited[i], shownAt(again), "stop %d differs when reached again", i)
	}

	// Each rewind started the run again and ended the one it replaced: nothing
	// is left running but the current run.
	assert.Equal(t, run.launches.started.Load()-1, run.launches.stopped.Load(), "a rewound run was left running")
}

// TestBackRefusesAStaleFenceAndAnOldRetry is the revision rule: a request
// fenced to a revision from before the rewind is refused, and a request id that
// was already applied is not applied again, even across the rewind.
func TestBackRefusesAStaleFenceAndAnOldRetry(t *testing.T) {
	t.Parallel()

	workflow := parseJourney(t)
	run := newReversing(t, func(int) *v1.Workflow { return workflow }, nil)

	one := run.first()
	two := run.move(one, v1.DebugResumeAction_DEBUG_RESUME_ACTION_STEP_IN)
	three := run.move(two, v1.DebugResumeAction_DEBUG_RESUME_ACTION_STEP_IN)

	// Fenced to a stop the session is not at: stale, nothing rewound.
	receipt, snapshot := run.back(two.GetRevision())
	assert.Equal(t, v1.DebugCommandStatus_DEBUG_COMMAND_STATUS_STALE, receipt.GetStatus())
	assert.Equal(t, three.GetRevision(), snapshot.GetRevision())
	assert.Equal(t, int64(1), run.launches.started.Load(), "a stale rewind started a run")

	// Fenced to the current stop: applied, and the same id again is a duplicate.
	back := run.id("back-once")
	receipt, err := run.target.Back(t.Context(), back, three.GetRevision())
	require.NoError(t, err)
	require.Equal(t, appliedStatus, receipt.GetStatus(), receipt.GetMessage())
	repeat, err := run.target.Back(t.Context(), back, 0)
	require.NoError(t, err)
	assert.Equal(t, v1.DebugCommandStatus_DEBUG_COMMAND_STATUS_DUPLICATE, repeat.GetStatus())
	assert.Equal(t, receipt.GetRevision(), repeat.GetRevision())
	now, err := run.target.Snapshot(t.Context())
	require.NoError(t, err)
	assert.Equal(t, shownAt(two), shownAt(now), "a retried rewind went back twice")

	// A movement fenced to the pre-rewind stop is stale: it would have moved
	// from a place the session is no longer at.
	stale, err := run.target.Resume(t.Context(), &v1.DebugResumeRequest{
		RequestId:        run.id("late"),
		ExpectedRevision: three.GetRevision(),
		Action:           v1.DebugResumeAction_DEBUG_RESUME_ACTION_STEP_IN,
	})
	require.NoError(t, err)
	assert.Equal(t, v1.DebugCommandStatus_DEBUG_COMMAND_STATUS_STALE, stale.GetStatus())
	after, err := run.target.Snapshot(t.Context())
	require.NoError(t, err)
	assert.Equal(t, now.GetRevision(), after.GetRevision(), "a stale movement advanced the run")

	// A snapshot revision from before the rewind is never reused: waiting past
	// it returns the rewound session, whose revision is newer than everything shown.
	assert.Greater(t, now.GetRevision(), three.GetRevision())
}

// TestBackRefusesWhenTheRunIsNotTheSameRun stages a program that changes
// between launches. The replay must say it diverged, and the run being
// debugged must be exactly where it was, still movable.
func TestBackRefusesWhenTheRunIsNotTheSameRun(t *testing.T) {
	t.Parallel()

	workflow := parseJourney(t)
	other := changedJourney(t)
	run := newReversing(t, func(n int) *v1.Workflow {
		if n == 0 {
			return workflow
		}

		return other
	}, nil)

	one := run.first()
	two := run.move(one, v1.DebugResumeAction_DEBUG_RESUME_ACTION_STEP_IN)
	three := run.move(two, v1.DebugResumeAction_DEBUG_RESUME_ACTION_STEP_IN)

	receipt, snapshot := run.back(0)
	assert.Equal(t, v1.DebugCommandStatus_DEBUG_COMMAND_STATUS_REFUSED, receipt.GetStatus())
	assert.True(t, strings.HasPrefix(receipt.GetMessage(), "diverged:"), receipt.GetMessage())
	assert.Equal(t, three.GetRevision(), snapshot.GetRevision(), "a refused rewind moved the session")
	assert.Equal(t, shownAt(three), shownAt(snapshot))

	// The abandoned replay was released, and the run still moves.
	assert.Equal(t, run.launches.started.Load()-1, run.launches.stopped.Load(), "a diverged replay was left running")
	four := run.move(three, v1.DebugResumeAction_DEBUG_RESUME_ACTION_STEP_IN)
	assert.NotEqual(t, shownAt(three).address, shownAt(four).address)
}

// changedJourney is the journey with its first step renamed, so a replay of it
// shows a different account from the first stop on.
func changedJourney(t *testing.T) *v1.Workflow {
	t.Helper()

	dir := t.TempDir()
	changed := strings.Replace(journeyFlowfile, "- id: start", "- id: begin_here", 1)
	require.NotEqual(t, journeyFlowfile, changed)
	for name, text := range map[string]string{"main.yaml": changed, "child.yaml": childFlowfile} {
		require.NoError(t, os.WriteFile(filepath.Join(dir, name), []byte(text), 0o600))
	}
	parsed, _, err := flowfile.ParseFile(filepath.Join(dir, "main.yaml"))
	require.NoError(t, err)
	return parsed
}

// TestBackRestoresTheBreakpointsASessionSet: a breakpoint set at one stop
// decides where a later `continue` stops, so a rewind must replay it at the
// point it was set and keep it after.
func TestBackRestoresTheBreakpointsASessionSet(t *testing.T) {
	t.Parallel()

	workflow := parseJourney(t)
	run := newReversing(t, func(int) *v1.Workflow { return workflow }, nil)

	one := run.first()
	set, err := run.target.ReplaceBreakpoints(t.Context(), &v1.DebugSetBreakpointsRequest{
		RequestId:   run.id("bp"),
		Breakpoints: []*v1.DebugBreakpoint{{Id: "each/touch", Step: "each/touch"}},
	})
	require.NoError(t, err)
	require.Equal(t, appliedStatus, set.GetReceipt().GetStatus())
	require.True(t, set.GetBreakpoints()[0].GetVerified(), set.GetBreakpoints()[0].GetMessage())

	two := run.move(one, v1.DebugResumeAction_DEBUG_RESUME_ACTION_CONTINUE)
	assert.Equal(t, "each[0]/touch", two.GetOccurrence().GetAddress())
	three := run.move(two, v1.DebugResumeAction_DEBUG_RESUME_ACTION_CONTINUE)
	assert.Equal(t, "each[1]/touch", three.GetOccurrence().GetAddress())

	receipt, snapshot := run.back(0)
	require.Equal(t, appliedStatus, receipt.GetStatus(), receipt.GetMessage())
	assert.Equal(t, "each[0]/touch", snapshot.GetOccurrence().GetAddress())
	require.Len(t, snapshot.GetBreakpoints(), 1, "the rewound session lost its breakpoint")
	assert.Equal(t, "each/touch", snapshot.GetBreakpoints()[0].GetId())

	// And it still decides the next stop.
	again := run.move(snapshot, v1.DebugResumeAction_DEBUG_RESUME_ACTION_CONTINUE)
	assert.Equal(t, "each[1]/touch", again.GetOccurrence().GetAddress())
}

// TestPausingAHeldRunChangesNothingToReplay: asking a held run to hold is
// answered applied and moves nothing, so it must not cost the session its
// rewind.
func TestPausingAHeldRunChangesNothingToReplay(t *testing.T) {
	t.Parallel()

	workflow := parseJourney(t)
	run := newReversing(t, func(int) *v1.Workflow { return workflow }, nil)

	one := run.first()
	two := run.move(one, v1.DebugResumeAction_DEBUG_RESUME_ACTION_STEP_IN)
	pause, err := run.target.Pause(t.Context(), run.id("pause"))
	require.NoError(t, err)
	require.Equal(t, appliedStatus, pause.GetStatus(), pause.GetMessage())

	receipt, snapshot := run.back(0)
	require.Equal(t, appliedStatus, receipt.GetStatus(), receipt.GetMessage())
	assert.Equal(t, shownAt(one), shownAt(snapshot))
	_ = two
}

// TestBackIsBounded: a session that has moved past the limit refuses to
// re-execute through all of it, and says it is unavailable.
func TestBackIsBounded(t *testing.T) {
	t.Parallel()

	items := make([]string, 300)
	for i := range items {
		items[i] = fmt.Sprint(i)
	}
	text := "edition: v2026.3\nname: long\nvars:\n  items: ${[" + strings.Join(items, ", ") + "]}\n" +
		"steps:\n  - id: each\n    for_each:\n      items: ${vars.items}\n      as: item\n      steps:\n" +
		"        - id: touch\n          log:\n            message: ${string(item)}\n"
	path := filepath.Join(t.TempDir(), "long.yaml")
	require.NoError(t, os.WriteFile(path, []byte(text), 0o600))
	workflow, _, err := flowfile.ParseFile(path)
	require.NoError(t, err)
	run := newReversing(t, func(int) *v1.Workflow { return workflow }, nil)

	at := run.first()
	for range flowdebug.MaxReversibleMoves + 1 {
		at = run.move(at, v1.DebugResumeAction_DEBUG_RESUME_ACTION_STEP_IN)
	}
	receipt, snapshot := run.back(0)
	assert.Equal(t, v1.DebugCommandStatus_DEBUG_COMMAND_STATUS_REFUSED, receipt.GetStatus())
	assert.Contains(t, receipt.GetMessage(), "unavailable")
	assert.Equal(t, at.GetRevision(), snapshot.GetRevision())
	assert.Equal(t, int64(1), run.launches.started.Load(), "an unavailable rewind started a run")
}

// TestRepeatedBackAndForthLeaksNothing: every rewind ends the run it
// replaced, and the goroutines of all of them have returned once the last is
// stopped.
func TestRepeatedBackAndForthLeaksNothing(t *testing.T) {
	t.Parallel()

	workflow := parseJourney(t)
	run := newReversing(t, func(int) *v1.Workflow { return workflow }, nil)

	at := run.first()
	for range 12 {
		forward := run.move(at, v1.DebugResumeAction_DEBUG_RESUME_ACTION_STEP_IN)
		_, back := run.back(0)
		assert.Equal(t, shownAt(at), shownAt(back))
		at = run.move(back, v1.DebugResumeAction_DEBUG_RESUME_ACTION_STEP_IN)
		assert.Equal(t, shownAt(forward), shownAt(at), "the same stop shows differently the second time")
	}
	run.target.Stop()

	run.launches.mu.Lock()
	done := append([]chan struct{}(nil), run.launches.done...)
	run.launches.mu.Unlock()
	require.Len(t, done, int(run.launches.started.Load()))
	for i, c := range done {
		select {
		case <-c:
		case <-time.After(10 * time.Second):
			t.Fatalf("run %d of %d never returned", i, len(done))
		}
	}
	assert.Equal(t, run.launches.started.Load(), run.launches.stopped.Load())
}

// TestARewoundRunDoesNotCarryOn: the run a rewind replaces is cancelled where it
// stands. Released the other way round it would finish its remaining steps,
// repeating every effect they have, after the person asked to go back.
func TestARewoundRunDoesNotCarryOn(t *testing.T) {
	t.Parallel()

	workflow := parseJourney(t)
	run := newReversing(t, func(int) *v1.Workflow { return workflow }, nil)

	at := run.first()
	for range 3 {
		at = run.move(at, v1.DebugResumeAction_DEBUG_RESUME_ACTION_STEP_IN)
	}
	receipt, _ := run.back(0)
	require.Equal(t, appliedStatus, receipt.GetStatus(), receipt.GetMessage())

	run.launches.mu.Lock()
	defer run.launches.mu.Unlock()
	final, ok := run.launches.finals[0]
	require.True(t, ok, "the replaced run was not stopped")
	assert.NotEqual(t, v1.DebugRunState_DEBUG_RUN_STATE_COMPLETED, final, "the replaced run was let finish")
}
