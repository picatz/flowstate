package flowdebug_test

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"

	v1 "github.com/picatz/flowstate/pkg/flowstate/v1"
	"github.com/picatz/flowstate/pkg/flowstate/v1/flowdebug"
	"github.com/picatz/flowstate/pkg/flowstate/v1/flowfile"
)

// The typed contract, driven the way every surface drives it: through
// [flowdebug.Target], against the real local driver.

// journeyFlowfile exercises every kind of nesting a stop can be inside.
const journeyFlowfile = `edition: v2026.3
name: journey
vars:
  items: ${[1, 2, 3]}
steps:
  - id: start
    log:
      message: begin
  - id: each
    for_each:
      items: ${vars.items}
      as: item
      steps:
        - id: touch
          log:
            message: ${"item %d".format([item])}
  - id: checks
    parallel:
      - steps:
          - id: left
            log:
              message: left
      - steps:
          - id: right
            log:
              message: right
  - id: nested
    call: ./child.yaml
    with:
      who: world
  - id: price
    value: '${{"total": 40, "lines": [1, 2, 3], "customer": {"name": "ada"}}}'
  - id: done
    log:
      message: done
`

const childFlowfile = `edition: v2026.3
name: child
inputs:
  who:
    type: string
steps:
  - id: greet
    log:
      message: ${"hello " + inputs.who}
  - id: wave
    log:
      message: bye
`

// debugRun is one debugged local run.
type debugRun struct {
	session *flowdebug.Session
	done    chan error
}

// startDebugRun compiles files (the first is the root), and runs it under a
// controlled session on its own goroutine.
func startDebugRun(t *testing.T, root string, files map[string]string, configure func(*flowdebug.Options), before ...func(*flowdebug.Session)) *debugRun {
	t.Helper()

	dir := t.TempDir()
	for name, text := range files {
		require.NoError(t, os.WriteFile(filepath.Join(dir, name), []byte(text), 0o600))
	}
	workflow, _, err := flowfile.ParseFile(filepath.Join(dir, root))
	require.NoError(t, err)

	return startDebugWorkflow(t, workflow, configure, before...)
}

// startDebugWorkflow runs workflow under a new session; before, if given, runs
// on the session ahead of the run, so that what it installs is in place for
// the very first stop.
func startDebugWorkflow(t *testing.T, workflow *v1.Workflow, configure func(*flowdebug.Options), before ...func(*flowdebug.Session)) *debugRun {
	t.Helper()

	opts := flowdebug.Options{Controlled: true, Out: &strings.Builder{}, Workflow: workflow}
	if configure != nil {
		configure(&opts)
	}
	session, err := flowdebug.New(opts)
	require.NoError(t, err)
	for _, prepare := range before {
		prepare(session)
	}

	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(func() {
		_ = session.Close()
		cancel()
	})

	run := &debugRun{session: session, done: make(chan error, 1)}
	go func() {
		runCtx := v1.NewContextWithDebugger(ctx, session)
		runCtx = v1.NewContextWithRunObserver(runCtx, session)
		_, err := v1.RunWithInputs(runCtx, workflow, nil)
		session.Finished(err)
		run.done <- err
	}()

	return run
}

// waitHeld waits for the session to hold after revision after.
func waitHeld(t *testing.T, target flowdebug.Target, after uint64) *v1.DebugSnapshot {
	t.Helper()

	ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
	defer cancel()
	for {
		snapshot, err := target.WaitSnapshot(ctx, after)
		require.NoError(t, err)
		switch snapshot.GetState() {
		case v1.DebugRunState_DEBUG_RUN_STATE_HELD:
			return snapshot
		case v1.DebugRunState_DEBUG_RUN_STATE_COMPLETED, v1.DebugRunState_DEBUG_RUN_STATE_FAILED,
			v1.DebugRunState_DEBUG_RUN_STATE_DETACHED:
			return snapshot
		}
		after = snapshot.GetRevision()
	}
}

// move resumes and waits for the next stop.
func move(t *testing.T, target flowdebug.Target, from *v1.DebugSnapshot, action v1.DebugResumeAction, until string) *v1.DebugSnapshot {
	t.Helper()

	receipt, err := target.Resume(t.Context(), &v1.DebugResumeRequest{
		RequestId:        "r-" + from.GetOccurrence().GetAddress() + "-" + action.String(),
		ExpectedRevision: from.GetRevision(),
		Action:           action,
		Until:            until,
	})
	require.NoError(t, err)
	require.Equal(t, v1.DebugCommandStatus_DEBUG_COMMAND_STATUS_APPLIED, receipt.GetStatus(), receipt.GetMessage())

	return waitHeld(t, target, receipt.GetRevision())
}

func TestTypedSteppingIsCallAware(t *testing.T) {
	t.Parallel()

	run := startDebugRun(t, "main.yaml", map[string]string{"main.yaml": journeyFlowfile, "child.yaml": childFlowfile}, nil)
	target := flowdebug.Target(run.session)

	at := waitHeld(t, target, 0)
	require.Equal(t, v1.DebugRunState_DEBUG_RUN_STATE_HELD, at.GetState())
	assert.Equal(t, v1.DebugStopReason_DEBUG_STOP_REASON_ENTRY, at.GetReason())
	assert.Equal(t, "start", at.GetOccurrence().GetAddress())

	// Into the loop: step-in stops at the container, then inside its body.
	at = move(t, target, at, v1.DebugResumeAction_DEBUG_RESUME_ACTION_STEP_IN, "")
	assert.Equal(t, "each", at.GetOccurrence().GetAddress())
	at = move(t, target, at, v1.DebugResumeAction_DEBUG_RESUME_ACTION_STEP_IN, "")
	assert.Equal(t, "each[0]/touch", at.GetOccurrence().GetAddress())
	assert.Equal(t, []string{"each", "touch"}, at.GetOccurrence().GetSite().GetPath())
	require.Len(t, at.GetFrames(), 2, "a stop inside a loop has the step's frame and the iteration's")
	assert.True(t, at.GetFrames()[0].GetScoped())
	assert.Contains(t, at.GetFrames()[1].GetLabel(), "iteration 0")

	// Over, inside the body: the next iteration's arrival is the same depth.
	at = move(t, target, at, v1.DebugResumeAction_DEBUG_RESUME_ACTION_STEP_OVER, "")
	assert.Equal(t, "each[1]/touch", at.GetOccurrence().GetAddress())

	// Out: leaves the loop, stopping at the next step at the top level.
	at = move(t, target, at, v1.DebugResumeAction_DEBUG_RESUME_ACTION_STEP_OUT, "")
	assert.Equal(t, "checks", at.GetOccurrence().GetAddress())

	// Over a parallel runs every branch without stopping in one.
	at = move(t, target, at, v1.DebugResumeAction_DEBUG_RESUME_ACTION_STEP_OVER, "")
	assert.Equal(t, "nested", at.GetOccurrence().GetAddress())

	// Into a call: the callee's step, addressed through its caller.
	at = move(t, target, at, v1.DebugResumeAction_DEBUG_RESUME_ACTION_STEP_IN, "")
	assert.Equal(t, "nested(child)/greet", at.GetOccurrence().GetAddress())
	assert.Equal(t, "child", at.GetOccurrence().GetSite().GetWorkflow())
	assert.Equal(t, []string{"greet"}, at.GetOccurrence().GetSite().GetPath())
	assert.Contains(t, at.GetFrames()[1].GetLabel(), `nested (call "child")`)

	// Out of the call.
	at = move(t, target, at, v1.DebugResumeAction_DEBUG_RESUME_ACTION_STEP_OUT, "")
	assert.Equal(t, "price", at.GetOccurrence().GetAddress())

	receipt, err := target.Resume(t.Context(), &v1.DebugResumeRequest{
		RequestId: "finish", Action: v1.DebugResumeAction_DEBUG_RESUME_ACTION_CONTINUE,
	})
	require.NoError(t, err)
	require.Equal(t, v1.DebugCommandStatus_DEBUG_COMMAND_STATUS_APPLIED, receipt.GetStatus())
	require.NoError(t, <-run.done)

	final := waitHeld(t, target, receipt.GetRevision())
	assert.Equal(t, v1.DebugRunState_DEBUG_RUN_STATE_COMPLETED, final.GetState())
}

func TestParallelBranchesAreAddressedByBranch(t *testing.T) {
	t.Parallel()

	run := startDebugRun(t, "main.yaml", map[string]string{"main.yaml": journeyFlowfile, "child.yaml": childFlowfile}, nil)
	target := flowdebug.Target(run.session)

	at := waitHeld(t, target, 0)
	at = move(t, target, at, v1.DebugResumeAction_DEBUG_RESUME_ACTION_RUN_UNTIL, "checks#1/right")
	assert.Equal(t, v1.DebugStopReason_DEBUG_STOP_REASON_UNTIL, at.GetReason())
	assert.Equal(t, "checks#1/right", at.GetOccurrence().GetAddress())
	assert.Contains(t, at.GetFrames()[1].GetLabel(), "branch 1")
}

func TestRichBreakpoints(t *testing.T) {
	t.Parallel()

	run := startDebugRun(t, "main.yaml", map[string]string{"main.yaml": journeyFlowfile, "child.yaml": childFlowfile}, nil)
	target := flowdebug.Target(run.session)
	at := waitHeld(t, target, 0)

	response, err := target.ReplaceBreakpoints(t.Context(), &v1.DebugSetBreakpointsRequest{
		RequestId: "bp1",
		Breakpoints: []*v1.DebugBreakpoint{
			{Id: "cond", Step: "each/touch", Condition: "item == 2"},
			{Id: "log", Step: "touch", LogMessage: "saw {item} of {size(vars.items)}"},
			{Id: "hit", Step: "touch", HitCondition: "== 3"},
			{Id: "nope", Step: "nowhere"},
			{Id: "badhit", Step: "touch", HitCondition: "often"},
			{Id: "badcond", Step: "touch", Condition: "item +"},
		},
	})
	require.NoError(t, err)
	states := map[string]*v1.DebugBreakpointState{}
	for _, state := range response.GetBreakpoints() {
		states[state.GetId()] = state
	}
	assert.True(t, states["cond"].GetVerified())
	require.Len(t, states["cond"].GetSites(), 1)
	assert.Equal(t, []string{"each", "touch"}, states["cond"].GetSites()[0].GetPath())
	assert.True(t, states["log"].GetVerified())
	assert.False(t, states["nope"].GetVerified(), "a target nothing declares must not be armed")
	assert.False(t, states["badhit"].GetVerified())
	assert.False(t, states["badcond"].GetVerified(), "a condition that does not parse must not be armed")

	at = move(t, target, at, v1.DebugResumeAction_DEBUG_RESUME_ACTION_CONTINUE, "")
	assert.Equal(t, v1.DebugStopReason_DEBUG_STOP_REASON_BREAKPOINT, at.GetReason())
	assert.Equal(t, "each[1]/touch", at.GetOccurrence().GetAddress())
	assert.Equal(t, []string{"cond"}, at.GetBreakpointIds())

	at = move(t, target, at, v1.DebugResumeAction_DEBUG_RESUME_ACTION_CONTINUE, "")
	assert.Equal(t, "each[2]/touch", at.GetOccurrence().GetAddress())
	assert.Equal(t, []string{"hit"}, at.GetBreakpointIds(), "the hit condition admits the third arrival only")

	var logs []string
	for _, observation := range at.GetObservations() {
		if observation.GetKind() == v1.DebugObservationKind_DEBUG_OBSERVATION_KIND_LOG {
			logs = append(logs, observation.GetText())
		}
	}
	assert.Equal(t, []string{"saw 1 of 3", "saw 2 of 3", "saw 3 of 3"}, logs,
		"a logpoint records every arrival and never stops")
}

func TestBreakpointReplacementIsAtomic(t *testing.T) {
	t.Parallel()

	run := startDebugRun(t, "main.yaml", map[string]string{"main.yaml": journeyFlowfile, "child.yaml": childFlowfile}, nil)
	target := flowdebug.Target(run.session)
	waitHeld(t, target, 0)

	_, err := target.ReplaceBreakpoints(t.Context(), &v1.DebugSetBreakpointsRequest{
		Breakpoints: []*v1.DebugBreakpoint{{Id: "a", Step: "done"}},
	})
	require.NoError(t, err)

	tooMany := make([]*v1.DebugBreakpoint, flowdebug.MaxBreakpoints+1)
	for i := range tooMany {
		tooMany[i] = &v1.DebugBreakpoint{Step: "done"}
	}
	_, err = target.ReplaceBreakpoints(t.Context(), &v1.DebugSetBreakpointsRequest{Breakpoints: tooMany})
	require.Error(t, err)

	at, err := target.Snapshot(t.Context())
	require.NoError(t, err)
	at = move(t, target, at, v1.DebugResumeAction_DEBUG_RESUME_ACTION_CONTINUE, "")
	assert.Equal(t, "done", at.GetOccurrence().GetAddress(), "a refused replacement left the previous set armed")
}

func TestFailureStopsOnceWhereTheFailureWasRaised(t *testing.T) {
	t.Parallel()

	const failing = `edition: v2026.3
name: failing
steps:
  - id: outer
    for_each:
      items: ${[0]}
      as: i
      steps:
        - id: boom
          value: ${[1][5 + i]}
`
	run := startDebugRun(t, "main.yaml", map[string]string{"main.yaml": failing}, nil)
	target := flowdebug.Target(run.session)
	at := waitHeld(t, target, 0)

	_, err := target.ReplaceBreakpoints(t.Context(), &v1.DebugSetBreakpointsRequest{
		FailureMode: v1.DebugFailureMode_DEBUG_FAILURE_MODE_UNCAUGHT,
	})
	require.NoError(t, err)

	at = move(t, target, at, v1.DebugResumeAction_DEBUG_RESUME_ACTION_CONTINUE, "")
	require.Equal(t, v1.DebugStopReason_DEBUG_STOP_REASON_FAILURE, at.GetReason())
	assert.Equal(t, "outer[0]/boom", at.GetOccurrence().GetAddress())
	assert.NotEmpty(t, at.GetFailure())

	inspected, err := target.Inspect(t.Context(), &v1.DebugInspectRequest{
		Revision: at.GetRevision(), Expression: "steps.boom.error",
	})
	require.NoError(t, err)
	assert.Empty(t, inspected.GetError())
	assert.Equal(t, "string", inspected.GetValue().GetType())

	final := move(t, target, at, v1.DebugResumeAction_DEBUG_RESUME_ACTION_CONTINUE, "")
	assert.Equal(t, v1.DebugRunState_DEBUG_RUN_STATE_FAILED, final.GetState(),
		"the same failure propagating through the loop must not stop again")
	require.Error(t, <-run.done)
}

func TestInspectIsTypedPagedAndRevisioned(t *testing.T) {
	t.Parallel()

	run := startDebugRun(t, "main.yaml", map[string]string{"main.yaml": journeyFlowfile, "child.yaml": childFlowfile}, nil)
	target := flowdebug.Target(run.session)
	at := waitHeld(t, target, 0)
	at = move(t, target, at, v1.DebugResumeAction_DEBUG_RESUME_ACTION_RUN_UNTIL, "done")

	roots, err := target.Inspect(t.Context(), &v1.DebugInspectRequest{Revision: at.GetRevision()})
	require.NoError(t, err)
	var groups []string
	for _, child := range roots.GetChildren() {
		groups = append(groups, child.GetName())
	}
	assert.Contains(t, groups, "steps")

	value, err := target.Inspect(t.Context(), &v1.DebugInspectRequest{
		Revision: at.GetRevision(), Expression: "steps.price.value", Children: true, Limit: 2,
	})
	require.NoError(t, err)
	require.Empty(t, value.GetError())
	assert.Equal(t, "map", value.GetValue().GetType())
	assert.EqualValues(t, 3, value.GetValue().GetChildren())
	assert.EqualValues(t, 3, value.GetTotal())
	require.Len(t, value.GetChildren(), 2, "the page is bounded by the limit")
	assert.Equal(t, "customer", value.GetChildren()[0].GetName())
	assert.Equal(t, "steps.price.value.customer", value.GetChildren()[0].GetValue().GetExpression())

	child, err := target.Inspect(t.Context(), &v1.DebugInspectRequest{
		Revision: at.GetRevision(), Expression: value.GetChildren()[1].GetValue().GetExpression(), Children: true,
	})
	require.NoError(t, err)
	assert.Equal(t, "list", child.GetValue().GetType())
	require.Len(t, child.GetChildren(), 3)
	assert.Equal(t, "int", child.GetChildren()[0].GetValue().GetType())

	failed, err := target.Inspect(t.Context(), &v1.DebugInspectRequest{Revision: at.GetRevision(), Expression: "nope.nothing"})
	require.NoError(t, err)
	assert.NotEmpty(t, failed.GetError())

	_, err = target.Inspect(t.Context(), &v1.DebugInspectRequest{Revision: at.GetRevision() - 1, Expression: "1"})
	require.ErrorIs(t, err, flowdebug.ErrStaleRevision)
}

func TestResumeIsRetrySafeAndRevisionChecked(t *testing.T) {
	t.Parallel()

	run := startDebugRun(t, "main.yaml", map[string]string{"main.yaml": journeyFlowfile, "child.yaml": childFlowfile}, nil)
	target := flowdebug.Target(run.session)
	at := waitHeld(t, target, 0)

	request := &v1.DebugResumeRequest{
		RequestId: "once", ExpectedRevision: at.GetRevision(), Action: v1.DebugResumeAction_DEBUG_RESUME_ACTION_STEP_IN,
	}
	first, err := target.Resume(t.Context(), request)
	require.NoError(t, err)
	require.Equal(t, v1.DebugCommandStatus_DEBUG_COMMAND_STATUS_APPLIED, first.GetStatus())
	next := waitHeld(t, target, first.GetRevision())
	assert.Equal(t, "each", next.GetOccurrence().GetAddress())

	again, err := target.Resume(t.Context(), request)
	require.NoError(t, err)
	assert.Equal(t, v1.DebugCommandStatus_DEBUG_COMMAND_STATUS_DUPLICATE, again.GetStatus())
	assert.Equal(t, first.GetRevision(), again.GetRevision())

	still, err := target.Snapshot(t.Context())
	require.NoError(t, err)
	assert.Equal(t, "each", still.GetOccurrence().GetAddress(), "a retried command must not advance twice")

	stale, err := target.Resume(t.Context(), &v1.DebugResumeRequest{
		RequestId: "late", ExpectedRevision: at.GetRevision(), Action: v1.DebugResumeAction_DEBUG_RESUME_ACTION_STEP_IN,
	})
	require.NoError(t, err)
	assert.Equal(t, v1.DebugCommandStatus_DEBUG_COMMAND_STATUS_STALE, stale.GetStatus())

	unknown, err := target.Resume(t.Context(), &v1.DebugResumeRequest{
		RequestId: "nowhere", Action: v1.DebugResumeAction_DEBUG_RESUME_ACTION_RUN_UNTIL, Until: "missing",
	})
	require.NoError(t, err)
	assert.Equal(t, v1.DebugCommandStatus_DEBUG_COMMAND_STATUS_REFUSED, unknown.GetStatus())
}

// TestPauseStopsAtTheNextBoundary uses a directly constructed program with no
// source at all: debugging never depends on a frontend.
func TestPauseStopsAtTheNextBoundary(t *testing.T) {
	t.Parallel()

	release := make(chan struct{})
	started := make(chan struct{}, 1)
	registry := v1.NewRegistry()
	for _, def := range v1.DefaultRegistry().All() {
		require.NoError(t, registry.Register(def))
	}
	require.NoError(t, registry.Register(v1.TaskDef{Name: "slow", Fn: func(ctx context.Context, _ map[string]*v1.Value, _ *v1.Scope) (*v1.Node_Outputs, error) {
		started <- struct{}{}
		select {
		case <-release:
		case <-ctx.Done():
			return nil, ctx.Err()
		}

		return &v1.Node_Outputs{}, nil
	}}))

	workflow := &v1.Workflow{
		Name:    "ir-only",
		Profile: v1.CurrentProfile,
		Steps: []*v1.Node{
			{Id: "work", Kind: &v1.Node_Task{Task: &v1.Task{Name: "slow"}}},
			{Id: "after", Kind: &v1.Node_Task{Task: &v1.Task{Name: "log", Inputs: map[string]*v1.Value{"message": v1.NewLiteral("x")}}}},
		},
	}

	session, err := flowdebug.New(flowdebug.Options{Controlled: true, Workflow: workflow})
	require.NoError(t, err)
	t.Cleanup(func() { _ = session.Close() })
	done := make(chan error, 1)
	go func() {
		ctx := v1.NewContextWithRegistry(context.Background(), registry)
		ctx = v1.NewContextWithDebugger(ctx, session)
		_, err := v1.RunWithInputs(ctx, workflow, nil)
		session.Finished(err)
		done <- err
	}()

	at := waitHeld(t, session, 0)
	receipt, err := session.Resume(t.Context(), &v1.DebugResumeRequest{
		RequestId: "go", Action: v1.DebugResumeAction_DEBUG_RESUME_ACTION_CONTINUE, ExpectedRevision: at.GetRevision(),
	})
	require.NoError(t, err)
	require.Equal(t, v1.DebugCommandStatus_DEBUG_COMMAND_STATUS_APPLIED, receipt.GetStatus())
	<-started

	paused, err := session.Pause(t.Context(), "pause")
	require.NoError(t, err)
	assert.Equal(t, v1.DebugCommandStatus_DEBUG_COMMAND_STATUS_PENDING, paused.GetStatus(),
		"a pause is pending until the run reaches a boundary; the task under way keeps running")
	snapshot, err := session.Snapshot(t.Context())
	require.NoError(t, err)
	assert.Equal(t, v1.DebugRunState_DEBUG_RUN_STATE_PAUSE_REQUESTED, snapshot.GetState())

	close(release)
	held := waitHeld(t, session, paused.GetRevision())
	assert.Equal(t, v1.DebugStopReason_DEBUG_STOP_REASON_PAUSE, held.GetReason())
	assert.Equal(t, "after", held.GetOccurrence().GetAddress())
	assert.Equal(t, "ir-only", held.GetOccurrence().GetSite().GetWorkflow())

	require.NoError(t, session.Close())
	require.NoError(t, <-done, "closing the session detaches; it never ends the run")
	final, err := session.Snapshot(t.Context())
	require.NoError(t, err)
	assert.Equal(t, v1.DebugRunState_DEBUG_RUN_STATE_DETACHED, final.GetState())
}

func TestTargetGrammar(t *testing.T) {
	t.Parallel()

	for _, bad := range []string{"", "a/", "a[x]/b", "a[1]", "a(b/c", "a b"} {
		_, err := v1.ParseDebugTarget(bad)
		assert.Error(t, err, bad)
	}

	occurrence := v1.NewDebugOccurrence("child", []*v1.DebugSegment{
		{Kind: v1.DebugSegmentKind_DEBUG_SEGMENT_KIND_CALL, StepId: "nested", Callee: "child"},
		{Kind: v1.DebugSegmentKind_DEBUG_SEGMENT_KIND_ITERATION, StepId: "pages", Index: 2},
	}, "page", "log")
	assert.Equal(t, "nested(child)/pages[2]/page", occurrence.GetAddress())

	for text, want := range map[string]bool{
		"page":                        true,
		"pages/page":                  true,
		"pages[2]/page":               true,
		"pages[1]/page":               false,
		"pages#2/page":                false,
		"nested/pages/page":           true,
		"nested(child)/pages[2]/page": true,
		"nested(other)/pages/page":    false,
		"other/page":                  false,
	} {
		target, err := v1.ParseDebugTarget(text)
		require.NoError(t, err, text)
		assert.Equal(t, want, target.Matches(occurrence), text)
		assert.Equal(t, text, target.String())
	}
}

func TestLineBreakpointsResolveThroughTheSourceMap(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	root := filepath.Join(dir, "main.yaml")
	require.NoError(t, os.WriteFile(root, []byte(journeyFlowfile), 0o600))
	require.NoError(t, os.WriteFile(filepath.Join(dir, "child.yaml"), []byte(childFlowfile), 0o600))
	workflow, positions, err := flowfile.ParseFile(root)
	require.NoError(t, err)
	sourceMap := flowfile.SourceMap(root, []byte(journeyFlowfile), workflow, positions)

	// A map for another program is refused, not used.
	other := proto.CloneOf(sourceMap)
	other.IrDigest = "sha256:" + strings.Repeat("0", 64)
	_, err = flowdebug.New(flowdebug.Options{Controlled: true, Workflow: workflow, SourceMap: other})
	require.Error(t, err, "a source map for another program was accepted")
	_, err = flowdebug.New(flowdebug.Options{Controlled: true, SourceMap: sourceMap})
	require.Error(t, err, "a source map was accepted with no program to check it against")

	run := startDebugWorkflow(t, workflow, func(opts *flowdebug.Options) { opts.SourceMap = sourceMap })
	target := flowdebug.Target(run.session)
	at := waitHeld(t, target, 0)
	assert.Equal(t, sourceMap.GetIrDigest(), at.GetIrDigest(), "the snapshot does not name the program its source map is bound to")
	require.True(t, at.GetCapabilities().GetSourceBreakpoints())
	require.NotNil(t, at.GetFrames()[0].GetSource(), "the entry frame names where its step is written")
	assert.EqualValues(t, 6, at.GetFrames()[0].GetSource().GetRange().GetStartLine())

	response, err := target.ReplaceBreakpoints(t.Context(), &v1.DebugSetBreakpointsRequest{Breakpoints: []*v1.DebugBreakpoint{
		// Line 15 is inside `touch`'s `log:` block, nested in `each`.
		{Id: "line", Line: &v1.DebugSourceLine{Uri: "file://" + root, Line: 15}},
		{Id: "blank", Line: &v1.DebugSourceLine{Uri: root, Line: 2}},
		{Id: "elsewhere", Line: &v1.DebugSourceLine{Uri: "/nowhere.yaml", Line: 3}},
	}})
	require.NoError(t, err)
	require.True(t, response.GetBreakpoints()[0].GetVerified(), response.GetBreakpoints()[0].GetMessage())
	assert.Equal(t, []string{"each", "touch"}, response.GetBreakpoints()[0].GetSites()[0].GetPath())
	assert.False(t, response.GetBreakpoints()[1].GetVerified(), "a line holding no step resolves to nothing, not a guess")
	assert.False(t, response.GetBreakpoints()[2].GetVerified())

	at = move(t, target, at, v1.DebugResumeAction_DEBUG_RESUME_ACTION_CONTINUE, "")
	assert.Equal(t, "each[0]/touch", at.GetOccurrence().GetAddress())
	assert.Equal(t, []string{"line"}, at.GetBreakpointIds())
}

// TestTheTypedSurfaceIsRedacted withholds one step name and reads every typed
// place a name is printed: the stop's occurrence and frames, and the sites a
// breakpoint resolved to.
func TestTheTypedSurfaceIsRedacted(t *testing.T) {
	t.Parallel()

	// Installed before the run starts: a pause keeps the redactor it began
	// under, so one installed later would not reach the entry stop.
	run := startDebugRun(t, "main.yaml", map[string]string{"main.yaml": journeyFlowfile, "child.yaml": childFlowfile}, nil,
		func(session *flowdebug.Session) {
			session.SetRedactor(func(text string) string { return strings.ReplaceAll(text, "touch", "[redacted]") })
		})
	target := flowdebug.Target(run.session)
	at := waitHeld(t, target, 0)

	response, err := target.ReplaceBreakpoints(t.Context(), &v1.DebugSetBreakpointsRequest{
		RequestId:   "bp",
		Breakpoints: []*v1.DebugBreakpoint{{Id: "at", Step: "touch"}},
	})
	require.NoError(t, err)
	require.Len(t, response.GetBreakpoints(), 1)
	state := response.GetBreakpoints()[0]
	require.True(t, state.GetVerified(), state.GetMessage())
	require.NotEmpty(t, state.GetSites())
	for _, site := range state.GetSites() {
		assert.NotContains(t, strings.Join(site.GetPath(), "/"), "touch", "a breakpoint's site printed a withheld name")
	}

	at = move(t, target, at, v1.DebugResumeAction_DEBUG_RESUME_ACTION_CONTINUE, "")
	require.Equal(t, v1.DebugStopReason_DEBUG_STOP_REASON_BREAKPOINT, at.GetReason())
	encoded, err := protojson.Marshal(at)
	require.NoError(t, err)
	assert.NotContains(t, string(encoded), "touch", "the stop printed a withheld name")
	assert.Contains(t, at.GetOccurrence().GetAddress(), "[redacted]")

	// The reply is the caller's: changing it leaves the session's own sites
	// as they were.
	state.GetSites()[0].Path[0] = "mutated"
	again, err := target.ReplaceBreakpoints(t.Context(), &v1.DebugSetBreakpointsRequest{
		RequestId:   "bp-again",
		Breakpoints: []*v1.DebugBreakpoint{{Id: "at", Step: "touch"}},
	})
	require.NoError(t, err)
	assert.NotEqual(t, "mutated", again.GetBreakpoints()[0].GetSites()[0].GetPath()[0])
}

// TestDetachEndsTheSessionNotTheRun: once detached, the session is over, and
// neither a pause nor a new breakpoint can take the run back.
func TestDetachEndsTheSessionNotTheRun(t *testing.T) {
	t.Parallel()

	run := startDebugRun(t, "main.yaml", map[string]string{"main.yaml": journeyFlowfile, "child.yaml": childFlowfile}, nil)
	target := flowdebug.Target(run.session)
	at := waitHeld(t, target, 0)

	receipt, err := target.Resume(t.Context(), &v1.DebugResumeRequest{
		RequestId: "leave", ExpectedRevision: at.GetRevision(), Action: v1.DebugResumeAction_DEBUG_RESUME_ACTION_DETACH,
	})
	require.NoError(t, err)
	require.Equal(t, v1.DebugCommandStatus_DEBUG_COMMAND_STATUS_APPLIED, receipt.GetStatus(), receipt.GetMessage())

	detached := waitHeld(t, target, at.GetRevision())
	assert.Equal(t, v1.DebugRunState_DEBUG_RUN_STATE_DETACHED, detached.GetState())

	paused, err := target.Pause(t.Context(), "again")
	require.NoError(t, err)
	assert.Equal(t, v1.DebugCommandStatus_DEBUG_COMMAND_STATUS_ENDED, paused.GetStatus(), "a detached session was paused again")

	rearmed, err := target.ReplaceBreakpoints(t.Context(), &v1.DebugSetBreakpointsRequest{
		RequestId:   "rearm",
		Breakpoints: []*v1.DebugBreakpoint{{Id: "at", Step: "touch"}},
	})
	require.NoError(t, err)
	assert.Equal(t, v1.DebugCommandStatus_DEBUG_COMMAND_STATUS_ENDED, rearmed.GetReceipt().GetStatus())
	assert.Empty(t, rearmed.GetBreakpoints(), "a detached session armed a breakpoint")

	select {
	case err := <-run.done:
		require.NoError(t, err, "the detached run did not finish on its own")
	case <-time.After(10 * time.Second):
		t.Fatal("the detached run never finished")
	}
}
