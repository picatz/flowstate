package flowdebug_test

import (
	"cmp"
	"context"
	"sync"
	"testing"
	"testing/synctest"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	v1 "github.com/picatz/flowstate/pkg/flowstate/v1"
	"github.com/picatz/flowstate/pkg/flowstate/v1/flowdebug"
)

// TestTheDriverSpeaksThePromptsVocabularyToATarget drives a real local run
// through the command lines `flow debug attach` and the MCP sessions accept,
// which reach the target through nothing but its typed contract.
func TestTheDriverSpeaksThePromptsVocabularyToATarget(t *testing.T) {
	t.Parallel()

	run := startDebugRun(t, "main.yaml", map[string]string{"main.yaml": journeyFlowfile, "child.yaml": childFlowfile}, nil)
	waitHeld(t, run.session, 0)
	driver := flowdebug.NewDriver(run.session)

	do := func(line string) *flowdebug.DriveResult {
		t.Helper()
		result, err := driver.Do(t.Context(), line)
		require.NoError(t, err, line)

		return result
	}

	assert.Contains(t, do("status").Text, "held at start")

	armed := do("break each/touch if item == 2")
	assert.Contains(t, armed.Text, "breakpoint at each/touch")
	assert.Contains(t, do("log touch saw {item}").Text, "breakpoint at touch")
	assert.Contains(t, do("break nowhere").Text, "not armed: nowhere")

	stop := do("continue")
	require.NotNil(t, stop.Snapshot)
	assert.Equal(t, "each[1]/touch", stop.Snapshot.GetOccurrence().GetAddress())
	assert.Contains(t, stop.Text, "breakpoint")
	assert.Equal(t, "2\n", do("inspect item").Text)
	assert.Contains(t, do("expand [1, [2, 3]]").Text, "list")
	assert.Contains(t, do("bt").Text, "iteration 1")
	assert.Contains(t, do("scope").Text, "vars: item")
	assert.Contains(t, do("breakpoints").Text, "each/touch  hits 1")

	out := do("finish")
	assert.Equal(t, "checks", out.Snapshot.GetOccurrence().GetAddress())

	detached := do("detach")
	assert.Equal(t, v1.DebugCommandStatus_DEBUG_COMMAND_STATUS_APPLIED, detached.Receipt.GetStatus())
	require.NoError(t, <-run.done)

	_, err := driver.Do(t.Context(), "frobnicate")
	require.Error(t, err)
}

// TestAFreshDriverKeepsTheBreakpointsItFinds is the `flow debug do` case:
// each invocation is a new driver over the same session, and the target
// replaces the whole set at once. A fresh driver adopts what is installed,
// from the definitions the snapshot reports, so its line adds to the set
// rather than replacing it — conditions included.
func TestAFreshDriverKeepsTheBreakpointsItFinds(t *testing.T) {
	t.Parallel()

	run := startDebugRun(t, "main.yaml", map[string]string{"main.yaml": journeyFlowfile, "child.yaml": childFlowfile}, nil)
	at := waitHeld(t, run.session, 0)

	do := func(line string) *flowdebug.DriveResult {
		t.Helper()
		// A new driver per line, as `flow debug do` makes one.
		result, err := flowdebug.NewDriver(run.session).Do(t.Context(), line)
		require.NoError(t, err, line)

		return result
	}
	ids := func() []string {
		t.Helper()
		snapshot, err := run.session.Snapshot(t.Context())
		require.NoError(t, err)
		var ids []string
		for _, state := range snapshot.GetBreakpoints() {
			ids = append(ids, state.GetId())
		}

		return ids
	}

	do("break each/touch if item == 2")
	do("break done")
	assert.ElementsMatch(t, []string{"each/touch", "done"}, ids(), "a fresh driver dropped a breakpoint it found")
	do("log checks at the checks")
	do("catch uncaught")
	assert.ElementsMatch(t, []string{"each/touch", "done", "log checks"}, ids())
	do("delete done")
	assert.ElementsMatch(t, []string{"each/touch", "log checks"}, ids())
	// A logpoint is deleted by the id it was set under.
	do("delete log checks")
	assert.ElementsMatch(t, []string{"each/touch"}, ids(), "a logpoint could not be deleted")

	// The adopted breakpoint kept its condition: the run passes item 1.
	stop := do("continue")
	assert.Equal(t, "each[1]/touch", stop.Snapshot.GetOccurrence().GetAddress())
	assert.Greater(t, stop.Snapshot.GetRevision(), at.GetRevision())

	cleared := do("clear")
	assert.Equal(t, "no breakpoints\n", cleared.Text)
	assert.Empty(t, ids())
}

// TestADriverRefusesToDropABreakpointItCannotRebuild: a target that reports a
// breakpoint without its definition (a server older than the field) cannot
// have it resent, so a line that would drop it is refused unless it names it.
func TestADriverRefusesToDropABreakpointItCannotRebuild(t *testing.T) {
	t.Parallel()

	target := &scriptedTarget{snapshot: &v1.DebugSnapshot{
		Revision: 1, State: v1.DebugRunState_DEBUG_RUN_STATE_HELD,
		Breakpoints: []*v1.DebugBreakpointState{{Id: "old", Verified: true}},
	}}
	_, err := flowdebug.NewDriver(target).Do(t.Context(), "break build")
	require.Error(t, err)
	assert.Contains(t, err.Error(), "old")
	assert.Empty(t, target.modes, "nothing was sent")

	_, err = flowdebug.NewDriver(target).Do(t.Context(), "delete old")
	require.NoError(t, err, "a line naming the breakpoint may replace it")
	_, err = flowdebug.NewDriver(target).Do(t.Context(), "clear")
	require.NoError(t, err)
}

// TestABreakKeepsAnotherClientsBreakpointOnTheSameStep: the typed contract
// allows several breakpoints on one step under different ids, so `break build`
// replaces only the breakpoint the driver owns under id `build`, and keeps a
// conditional one an editor set on the same step as `dap-7`.
func TestABreakKeepsAnotherClientsBreakpointOnTheSameStep(t *testing.T) {
	t.Parallel()

	theirs := &v1.DebugBreakpoint{Id: "dap-7", Step: "build", Condition: "attempt > 2"}
	target := &scriptedTarget{snapshot: &v1.DebugSnapshot{
		Revision: 1, State: v1.DebugRunState_DEBUG_RUN_STATE_HELD,
		Breakpoints: []*v1.DebugBreakpointState{{Id: "dap-7", Verified: true, Definition: theirs}},
	}}
	_, err := flowdebug.NewDriver(target).Do(t.Context(), "break build")
	require.NoError(t, err)
	require.Len(t, target.sets, 1)
	ids := make([]string, 0, 2)
	for _, bp := range target.sets[0] {
		ids = append(ids, bp.GetId())
	}
	assert.ElementsMatch(t, []string{"dap-7", "build"}, ids, "break dropped another client's breakpoint on its step")
}

// TestADetachedDriverChangesNothing: once a detach is accepted the session is
// over. A durable pause after it would attach the run anew, so a line that
// would change the session is refused before it reaches the target, and a
// read still answers.
func TestADetachedDriverChangesNothing(t *testing.T) {
	t.Parallel()

	target := &scriptedTarget{snapshot: &v1.DebugSnapshot{Revision: 1, State: v1.DebugRunState_DEBUG_RUN_STATE_HELD}}
	driver := flowdebug.NewDriver(target)
	_, err := driver.Do(t.Context(), "detach")
	require.NoError(t, err)
	sent := len(target.requests)

	for _, line := range []string{"pause", "next", "break build", "clear"} {
		_, err := driver.Do(t.Context(), line)
		require.Error(t, err, "%s was accepted after the session was detached", line)
	}
	assert.Len(t, target.requests, sent, "a refused line reached the target")
	_, err = driver.Do(t.Context(), "status")
	assert.NoError(t, err, "a read was refused after a detach")
}

// honestTarget is a [scriptedTarget] whose WaitSnapshot waits for a revision
// past the one asked about, as a real target's does.
type honestTarget struct{ *scriptedTarget }

func (h honestTarget) WaitSnapshot(ctx context.Context, after uint64) (*v1.DebugSnapshot, error) {
	h.mu.Lock()
	snapshot := h.snapshot
	h.mu.Unlock()
	if snapshot.GetRevision() > after {
		return snapshot, nil
	}
	<-ctx.Done()

	return nil, ctx.Err()
}

// TestAPauseOfAHeldRunAnswersAtOnce: a run already held answers a pause at the
// revision it is held at, without moving, so the stop it is at is the answer,
// not one to wait for past it until the wait runs out.
func TestAPauseOfAHeldRunAnswersAtOnce(t *testing.T) {
	t.Parallel()

	synctest.Test(t, func(t *testing.T) {
		target := honestTarget{&scriptedTarget{snapshot: &v1.DebugSnapshot{Revision: 1, State: v1.DebugRunState_DEBUG_RUN_STATE_HELD}}}
		driver := flowdebug.NewDriver(target)
		driver.Wait = time.Minute

		start := time.Now()
		result, err := driver.Do(t.Context(), "pause")
		require.NoError(t, err)
		assert.Equal(t, v1.DebugRunState_DEBUG_RUN_STATE_HELD, result.Snapshot.GetState())
		assert.Zero(t, time.Since(start), "a pause of a held run waited for a stop past the one it is at")
	})
}

// TestADetachIsNeverStaleUnlessPinned: a detach lets the run go wherever it
// has got to, so a driver sends it without the revision it last read — a stop
// landing in between must not leave the run held. A refused detach leaves the
// driver able to try again.
func TestADetachIsNeverStaleUnlessPinned(t *testing.T) {
	t.Parallel()

	target := &scriptedTarget{snapshot: &v1.DebugSnapshot{Revision: 4, State: v1.DebugRunState_DEBUG_RUN_STATE_RUNNING}}
	target.resumeStatus = v1.DebugCommandStatus_DEBUG_COMMAND_STATUS_REFUSED
	driver := flowdebug.NewDriver(target)
	_, err := driver.Do(t.Context(), "detach")
	require.NoError(t, err)
	assert.Zero(t, target.expected, "a detach was pinned to the revision the driver read")

	target.resumeStatus = v1.DebugCommandStatus_DEBUG_COMMAND_STATUS_UNSPECIFIED
	_, err = driver.Do(t.Context(), "detach")
	require.NoError(t, err, "a refused detach left the driver refusing to try again")

	_, err = flowdebug.NewDriver(target).DoWith(t.Context(), "detach", flowdebug.DoOptions{ExpectedRevision: 4})
	require.NoError(t, err)
	assert.Equal(t, uint64(4), target.expected, "a caller's pinned revision was dropped")
}

// TestAFreshDriverLeavesTheFailureModeAlone: a driver that has not been told
// a mode by `catch` must not reset the one the session has.
func TestAFreshDriverLeavesTheFailureModeAlone(t *testing.T) {
	t.Parallel()

	target := &scriptedTarget{snapshot: &v1.DebugSnapshot{Revision: 1, State: v1.DebugRunState_DEBUG_RUN_STATE_HELD}}
	driver := flowdebug.NewDriver(target)

	_, err := driver.Do(t.Context(), "break build")
	require.NoError(t, err)
	_, err = driver.Do(t.Context(), "catch all")
	require.NoError(t, err)
	_, err = driver.Do(t.Context(), "delete build")
	require.NoError(t, err)

	assert.Equal(t, []v1.DebugFailureMode{
		v1.DebugFailureMode_DEBUG_FAILURE_MODE_UNSPECIFIED,
		v1.DebugFailureMode_DEBUG_FAILURE_MODE_ALL,
		v1.DebugFailureMode_DEBUG_FAILURE_MODE_ALL,
	}, target.modes)
}

// TestAMovementsWaitEndsEvenWhenTheTargetStopsAnswering: when the wait for
// the next stop runs out, the read that reports "still running" has a bound
// of its own. A remote that stops answering must not hold the caller forever.
func TestAMovementsWaitEndsEvenWhenTheTargetStopsAnswering(t *testing.T) {
	t.Parallel()

	synctest.Test(t, func(t *testing.T) {
		target := &scriptedTarget{snapshot: &v1.DebugSnapshot{Revision: 1, State: v1.DebugRunState_DEBUG_RUN_STATE_HELD}}
		driver := flowdebug.NewDriver(target)
		driver.Wait = time.Second

		// Accepted, and then the target hangs on every read.
		target.hangAfterResume = true
		start := time.Now()
		_, err := driver.Do(t.Context(), "next")
		require.Error(t, err)
		assert.ErrorIs(t, err, context.DeadlineExceeded)
		assert.Contains(t, err.Error(), "had not stopped")
		assert.Equal(t, time.Second+5*time.Second, time.Since(start), "the wait and the read are each bounded")
	})
}

// TestAMovementEndsAtTheCallersDeadline: a caller's deadline that passes
// before the driver's own wait is the caller's error, answered at once — not a
// "still running" read that outlives it.
func TestAMovementEndsAtTheCallersDeadline(t *testing.T) {
	t.Parallel()

	synctest.Test(t, func(t *testing.T) {
		target := &scriptedTarget{snapshot: &v1.DebugSnapshot{Revision: 1, State: v1.DebugRunState_DEBUG_RUN_STATE_HELD}}
		driver := flowdebug.NewDriver(target)
		driver.Wait = time.Minute

		// Accepted, and then the target hangs on every read.
		target.hangAfterResume = true
		ctx, cancel := context.WithTimeout(t.Context(), time.Second)
		defer cancel()
		start := time.Now()
		_, err := driver.Do(ctx, "next")
		require.ErrorIs(t, err, context.DeadlineExceeded)
		assert.NotContains(t, err.Error(), "had not stopped", "the caller's deadline was read as the driver's wait")
		assert.Equal(t, time.Second, time.Since(start), "the command outlived its caller's deadline")
	})
}

// TestDoWithCarriesTheCallersRequestIDToTheTarget: a caller's retry key is the
// target's request id, so a target that remembers receipts answers a retry
// instead of acting twice.
func TestDoWithCarriesTheCallersRequestIDToTheTarget(t *testing.T) {
	t.Parallel()

	target := &scriptedTarget{snapshot: &v1.DebugSnapshot{Revision: 1, State: v1.DebugRunState_DEBUG_RUN_STATE_HELD}}
	driver := flowdebug.NewDriver(target)

	for _, line := range []string{"next", "pause", "break build"} {
		_, err := driver.DoWith(t.Context(), line, flowdebug.DoOptions{RequestID: "retry-" + line})
		require.NoError(t, err, line)
	}
	assert.Equal(t, []string{"retry-next", "retry-pause", "retry-break build"}, target.requests)

	// A line meant for another revision is refused before it reaches the
	// target; a movement carries the revision to the target instead.
	stale, err := driver.DoWith(t.Context(), "inspect 1", flowdebug.DoOptions{ExpectedRevision: 7})
	require.NoError(t, err)
	assert.Equal(t, v1.DebugCommandStatus_DEBUG_COMMAND_STATUS_STALE, stale.Receipt.GetStatus())
	_, err = driver.DoWith(t.Context(), "next", flowdebug.DoOptions{ExpectedRevision: 7})
	require.NoError(t, err)
	assert.Equal(t, uint64(7), target.expected)
}

// scriptedTarget is a [flowdebug.Target] that holds at one snapshot, records
// what it is sent, and can be made to hang on every read once resumed.
type scriptedTarget struct {
	mu              sync.Mutex
	snapshot        *v1.DebugSnapshot
	hangAfterResume bool
	// resumeStatus, when set, is the status every resume is answered with.
	resumeStatus v1.DebugCommandStatus
	hang         bool
	requests     []string
	modes        []v1.DebugFailureMode
	sets         [][]*v1.DebugBreakpoint
	expected     uint64
}

func (s *scriptedTarget) read(ctx context.Context) (*v1.DebugSnapshot, error) {
	s.mu.Lock()
	hang, snapshot := s.hang, s.snapshot
	s.mu.Unlock()
	if hang {
		<-ctx.Done()

		return nil, ctx.Err()
	}

	return snapshot, nil
}

func (s *scriptedTarget) Snapshot(ctx context.Context) (*v1.DebugSnapshot, error) { return s.read(ctx) }

func (s *scriptedTarget) WaitSnapshot(ctx context.Context, _ uint64) (*v1.DebugSnapshot, error) {
	return s.read(ctx)
}

func (s *scriptedTarget) Resume(_ context.Context, req *v1.DebugResumeRequest) (*v1.DebugReceipt, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.requests = append(s.requests, req.GetRequestId())
	s.expected = req.GetExpectedRevision()
	s.hang = s.hangAfterResume

	return &v1.DebugReceipt{RequestId: req.GetRequestId(), Status: cmp.Or(s.resumeStatus, v1.DebugCommandStatus_DEBUG_COMMAND_STATUS_APPLIED), Revision: 1}, nil
}

func (s *scriptedTarget) Pause(_ context.Context, id string) (*v1.DebugReceipt, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.requests = append(s.requests, id)

	return &v1.DebugReceipt{RequestId: id, Status: v1.DebugCommandStatus_DEBUG_COMMAND_STATUS_APPLIED, Revision: 1}, nil
}

func (s *scriptedTarget) ReplaceBreakpoints(_ context.Context, req *v1.DebugSetBreakpointsRequest) (*v1.DebugSetBreakpointsResponse, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.requests = append(s.requests, req.GetRequestId())
	s.modes = append(s.modes, req.GetFailureMode())
	s.sets = append(s.sets, req.GetBreakpoints())

	return &v1.DebugSetBreakpointsResponse{Receipt: &v1.DebugReceipt{Status: v1.DebugCommandStatus_DEBUG_COMMAND_STATUS_APPLIED}}, nil
}

func (s *scriptedTarget) Inspect(context.Context, *v1.DebugInspectRequest) (*v1.DebugInspectResponse, error) {
	return &v1.DebugInspectResponse{}, nil
}

func (s *scriptedTarget) Close() error { return nil }
