package flowdap_test

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	v1 "github.com/picatz/flowstate/pkg/flowstate/v1"
	"github.com/picatz/flowstate/pkg/flowstate/v1/flowdap"
	"github.com/picatz/flowstate/pkg/flowstate/v1/flowdebug"
	"github.com/picatz/flowstate/pkg/flowstate/v1/flowfile"
)

// An editor's session over a real local run launched by the adapter itself,
// with the program's source map: what a person actually gets from `flow dap`.

const richFlowfile = `edition: v2026.3
name: rich
steps:
  - id: start
    log:
      message: begin
  - id: each
    for_each:
      items: ${[1, 2, 3, 4]}
      as: item
      steps:
        - id: touch
          log:
            message: ${"item %d".format([item])}
  - id: price
    value: '${{"total": 40, "lines": [1, 2, 3], "customer": {"name": "ada"}}}'
  - id: boom
    value: ${[1][5]}
  - id: after
    log:
      message: never
`

// launched is an adapter whose launch compiles and runs richFlowfile.
func launched(t *testing.T) (*client, string, <-chan error) {
	t.Helper()

	dir := t.TempDir()
	program := filepath.Join(dir, "rich.yaml")
	require.NoError(t, os.WriteFile(program, []byte(richFlowfile), 0o600))

	c := newClient(t)
	t.Cleanup(func() { _ = c.Close() })

	finished := make(chan error, 1)
	var server *flowdap.Server
	server = flowdap.NewServer(nil, c, flowdap.WithLaunch(func(ctx context.Context, args flowdap.LaunchArguments) (*flowdap.Launch, error) {
		source, err := os.ReadFile(args.Program)
		if err != nil {
			return nil, err
		}
		workflow, positions, err := flowfile.ParseAt(source, args.Program)
		if err != nil {
			return nil, err
		}
		sourceMap := flowfile.SourceMap(args.Program, source, workflow, positions)
		session, err := flowdebug.New(flowdebug.Options{
			Controlled: true, Workflow: workflow, SourceMap: sourceMap,
			Emit: func(text string, _ flowdebug.Tone) { server.Output(text) },
		})
		if err != nil {
			return nil, err
		}
		runCtx, cancel := context.WithCancel(context.Background())
		t.Cleanup(cancel)

		return &flowdap.Launch{
			Target:    session,
			SourceMap: sourceMap,
			Start: func() {
				ctx := v1.NewContextWithDebugger(runCtx, session)
				ctx = v1.NewContextWithRunObserver(ctx, session)
				_, err := v1.RunWithInputs(ctx, workflow, nil)
				session.Finished(err)
				if err != nil {
					server.Exited(1)
				}
				_ = session.Close()
				server.Finished()
				finished <- err
			},
			Terminate: cancel,
		}, nil
	}))
	go func() { _ = server.Serve(t.Context()) }()

	return c, program, finished
}

func body(message map[string]any) map[string]any {
	b, _ := message["body"].(map[string]any)

	return b
}

func TestAnEditorGetsTheWholeDebugger(t *testing.T) {
	t.Parallel()

	c, program, finished := launched(t)

	c.send(1, "initialize", map[string]any{"adapterID": "flowstate"})
	caps := body(c.await("response", "initialize"))
	for _, name := range []string{"supportsConditionalBreakpoints", "supportsHitConditionalBreakpoints", "supportsLogPoints", "supportsTerminateRequest"} {
		assert.Equal(t, true, caps[name], "a local session does %s, so the adapter must say so", name)
	}
	assert.NotEmpty(t, caps["exceptionBreakpointFilters"])
	assert.NotEqual(t, true, caps["supportsStepBack"], "nothing here can step back")
	c.await("event", "initialized")

	c.send(2, "launch", map[string]any{"program": program})
	c.await("response", "launch")

	// Line 13 is inside `touch`, in the loop body: a conditional line
	// breakpoint there, and a line that holds no step.
	c.send(3, "setBreakpoints", map[string]any{
		"source": map[string]any{"path": program},
		"breakpoints": []map[string]any{
			{"line": 13, "condition": "item == 3"},
			{"line": 2},
		},
	})
	lines := body(c.await("response", "setBreakpoints"))["breakpoints"].([]any)
	require.Len(t, lines, 2)
	assert.Equal(t, true, lines[0].(map[string]any)["verified"], lines[0])
	assert.Equal(t, false, lines[1].(map[string]any)["verified"], "a line with no step on it verifies nothing")

	c.send(4, "setFunctionBreakpoints", map[string]any{"breakpoints": []map[string]any{
		{"name": "each/touch", "hitCondition": "== 1"},
	}})
	functions := body(c.await("response", "setFunctionBreakpoints"))["breakpoints"].([]any)
	assert.Equal(t, true, functions[0].(map[string]any)["verified"], functions[0])

	c.send(5, "setExceptionBreakpoints", map[string]any{"filters": []string{"uncaught"}})
	assert.Equal(t, true, c.await("response", "setExceptionBreakpoints")["success"])

	c.send(6, "configurationDone", nil)
	c.await("response", "configurationDone")
	entry := c.await("event", "stopped")
	assert.Equal(t, "entry", body(entry)["reason"])

	// The entry frame names its source line.
	c.send(7, "stackTrace", map[string]any{"threadId": 1})
	frames := body(c.await("response", "stackTrace"))["stackFrames"].([]any)
	first := frames[0].(map[string]any)
	assert.EqualValues(t, 4, first["line"])
	assert.Equal(t, program, first["source"].(map[string]any)["path"])

	// Continue: the hit-counted function breakpoint fires at the first
	// iteration, then the conditional line breakpoint at the third.
	c.send(8, "continue", map[string]any{"threadId": 1})
	c.await("response", "continue")
	stop := c.await("event", "stopped")
	assert.Equal(t, "breakpoint", body(stop)["reason"])
	c.send(9, "evaluate", map[string]any{"expression": "item", "frameId": 1})
	assert.Equal(t, "1", body(c.await("response", "evaluate"))["result"])

	c.send(10, "continue", map[string]any{"threadId": 1})
	c.await("response", "continue")
	c.await("event", "stopped")
	c.send(11, "evaluate", map[string]any{"expression": "item", "frameId": 1})
	evaluated := body(c.await("response", "evaluate"))
	assert.Equal(t, "3", evaluated["result"])
	assert.Equal(t, "int", evaluated["type"])

	// The stack is honest about where the stop is: the step, then the
	// iteration it is inside.
	c.send(12, "stackTrace", map[string]any{"threadId": 1})
	frames = body(c.await("response", "stackTrace"))["stackFrames"].([]any)
	require.Len(t, frames, 2)
	assert.Contains(t, frames[1].(map[string]any)["name"], "iteration 2")
	assert.Equal(t, "subtle", frames[1].(map[string]any)["presentationHint"])

	// Step out of the loop to `price`, then over it, and expand its value.
	c.send(13, "stepOut", map[string]any{"threadId": 1})
	c.await("response", "stepOut")
	c.await("event", "stopped")
	c.send(14, "next", map[string]any{"threadId": 1})
	c.await("response", "next")
	c.await("event", "stopped")

	c.send(15, "evaluate", map[string]any{"expression": "steps.price.value", "frameId": 1})
	price := body(c.await("response", "evaluate"))
	assert.Equal(t, "map", price["type"])
	reference := price["variablesReference"].(float64)
	require.NotZero(t, reference)
	c.send(16, "variables", map[string]any{"variablesReference": reference})
	children := body(c.await("response", "variables"))["variables"].([]any)
	require.Len(t, children, 3)
	lines0 := children[1].(map[string]any)
	assert.Equal(t, "lines", lines0["name"])
	assert.Equal(t, "list", lines0["type"])
	assert.Equal(t, "steps.price.value.lines", lines0["evaluateName"])
	c.send(17, "variables", map[string]any{"variablesReference": lines0["variablesReference"]})
	elements := body(c.await("response", "variables"))["variables"].([]any)
	require.Len(t, elements, 3)
	assert.Equal(t, "int", elements[0].(map[string]any)["type"])

	// Continue into the failing step: an exception stop, where the failure
	// is readable, and then the run fails.
	c.send(18, "continue", map[string]any{"threadId": 1})
	c.await("response", "continue")
	failed := c.await("event", "stopped")
	assert.Equal(t, "exception", body(failed)["reason"])
	assert.NotEmpty(t, body(failed)["text"])
	c.send(19, "evaluate", map[string]any{"expression": "steps.boom.error", "frameId": 1})
	assert.Equal(t, true, c.await("response", "evaluate")["success"])

	// A handle from the previous stop answers nothing now.
	c.send(20, "variables", map[string]any{"variablesReference": reference})
	assert.Empty(t, body(c.await("response", "variables"))["variables"])

	c.send(21, "continue", map[string]any{"threadId": 1})
	c.await("response", "continue")
	exited := c.await("event", "exited")
	assert.EqualValues(t, 1, body(exited)["exitCode"])
	select {
	case err := <-finished:
		require.Error(t, err)
	case <-time.After(20 * time.Second):
		t.Fatal("the run did not finish")
	}
}

func TestPauseStopsARunningRunAtItsNextStep(t *testing.T) {
	t.Parallel()

	c, program, _ := launched(t)
	c.send(1, "initialize", map[string]any{"adapterID": "flowstate"})
	c.await("response", "initialize")
	c.send(2, "launch", map[string]any{"program": program, "stopOnEntry": false})
	c.await("response", "launch")
	// Paused before anything runs: the pause is pending until the first
	// boundary, which is where the run then stops.
	c.send(3, "pause", map[string]any{"threadId": 1})
	assert.Equal(t, true, c.await("response", "pause")["success"])
	c.send(4, "configurationDone", nil)
	c.await("response", "configurationDone")
	stop := c.await("event", "stopped")
	assert.Equal(t, "pause", body(stop)["reason"])

	c.send(5, "disconnect", map[string]any{"terminateDebuggee": true})
	c.await("response", "disconnect")
}

// TestBreakpointsSetBeforeLaunchAreAppliedWhenItHappens is the order an editor
// may use: configuration after `initialized`, which can precede `launch`.
func TestBreakpointsSetBeforeLaunchAreAppliedWhenItHappens(t *testing.T) {
	t.Parallel()

	c, program, _ := launched(t)
	c.send(1, "initialize", map[string]any{"adapterID": "flowstate"})
	c.await("response", "initialize")
	c.await("event", "initialized")

	c.send(2, "setFunctionBreakpoints", map[string]any{"breakpoints": []map[string]any{{"name": "price"}}})
	pending := body(c.await("response", "setFunctionBreakpoints"))["breakpoints"].([]any)[0].(map[string]any)
	assert.Equal(t, false, pending["verified"], "nothing is running yet, so nothing is verified yet")
	id := pending["id"]
	require.NotNil(t, id)

	c.send(3, "launch", map[string]any{"program": program})
	c.await("response", "launch")
	changed := body(c.await("event", "breakpoint"))
	assert.Equal(t, "changed", changed["reason"])
	assert.Equal(t, id, changed["breakpoint"].(map[string]any)["id"])
	assert.Equal(t, true, changed["breakpoint"].(map[string]any)["verified"])

	c.send(4, "configurationDone", nil)
	c.await("response", "configurationDone")
	c.await("event", "stopped")
	c.send(5, "continue", map[string]any{"threadId": 1})
	c.await("response", "continue")
	assert.Equal(t, "breakpoint", body(c.await("event", "stopped"))["reason"])
}

func TestAMalformedBreakpointRequestKeepsTheInstalledSet(t *testing.T) {
	t.Parallel()

	c, program, _ := launched(t)
	c.send(1, "initialize", map[string]any{"adapterID": "flowstate"})
	c.await("response", "initialize")
	c.send(2, "launch", map[string]any{"program": program})
	c.await("response", "launch")

	c.send(3, "setFunctionBreakpoints", map[string]any{"breakpoints": []map[string]any{{"name": "price"}}})
	c.await("response", "setFunctionBreakpoints")
	for i, malformed := range []any{
		map[string]any{"breakpoints": []any{nil}},
		map[string]any{"breakpoints": []any{map[string]any{"condition": "true"}}},
		map[string]any{},
		"nonsense",
	} {
		c.send(4+i, "setFunctionBreakpoints", malformed)
		response := c.await("response", "setFunctionBreakpoints")
		assert.Equal(t, false, response["success"], "malformed request %d was accepted", i)
		assert.NotContains(t, response["message"], "true", "the refusal echoed what was submitted")
	}

	c.send(20, "configurationDone", nil)
	c.await("response", "configurationDone")
	c.await("event", "stopped")
	c.send(21, "continue", map[string]any{"threadId": 1})
	c.await("response", "continue")
	stop := c.await("event", "stopped")
	assert.Equal(t, "breakpoint", body(stop)["reason"], "the set installed before the refused requests still fired")
}

func TestAttachNarrowsWhatTheEditorIsOffered(t *testing.T) {
	t.Parallel()

	c := newClient(t)
	t.Cleanup(func() { _ = c.Close() })

	remote := &fakeRemote{snapshot: &v1.DebugSnapshot{
		Revision:     1,
		State:        v1.DebugRunState_DEBUG_RUN_STATE_RUNNING,
		Capabilities: v1.DurableDebugCapabilities(),
	}}
	// Built as `flow dap` builds it, able to launch as well as attach: what it
	// offers after an attach is the attached run's, not the launch it could
	// have made.
	server := flowdap.NewServer(nil, c,
		flowdap.WithLaunch(func(context.Context, flowdap.LaunchArguments) (*flowdap.Launch, error) {
			return nil, errors.New("this test only attaches")
		}),
		flowdap.WithAttach(func(context.Context, flowdap.AttachArguments) (*flowdap.Attachment, error) {
			return &flowdap.Attachment{Target: remote}, nil
		}))
	go func() { _ = server.Serve(t.Context()) }()

	c.send(1, "initialize", map[string]any{"adapterID": "flowstate"})
	initialized := body(c.await("response", "initialize"))
	assert.Equal(t, true, initialized["supportsTerminateRequest"], "before an attach, an adapter that can launch offers termination")

	// Configured before the backend is known, as an editor does: an exception
	// filter the durable driver does not offer must not take the rest down.
	c.send(2, "setExceptionBreakpoints", map[string]any{"filters": []string{"uncaught"}})
	c.await("response", "setExceptionBreakpoints")
	c.send(3, "setFunctionBreakpoints", map[string]any{"breakpoints": []map[string]any{{"name": "deploy"}}})
	c.await("response", "setFunctionBreakpoints")

	c.send(4, "attach", map[string]any{"workflowId": "wf-1"})
	c.await("response", "attach")
	dropped := body(c.await("event", "output"))
	assert.Contains(t, dropped["output"], "exception filter was dropped")
	caps := body(c.await("event", "capabilities"))["capabilities"].(map[string]any)
	assert.Equal(t, false, caps["supportsLogPoints"], "the durable driver has no logpoints, so the editor must not offer them")
	assert.Empty(t, caps["exceptionBreakpointFilters"])
	assert.Equal(t, true, caps["supportsConditionalBreakpoints"])
	assert.Equal(t, false, caps["supportsTerminateRequest"], "an attached run is not the adapter's to end")
	changed := body(c.await("event", "breakpoint"))["breakpoint"].(map[string]any)
	assert.Equal(t, true, changed["verified"], "the function breakpoint set before the attach was not applied: %v", changed)

	c.send(6, "setExceptionBreakpoints", map[string]any{"filters": []string{"all"}})
	assert.Equal(t, false, c.await("response", "setExceptionBreakpoints")["success"],
		"a failure stop the backend cannot make must be refused, not ignored")

	c.send(7, "terminate", map[string]any{})
	terminated := c.await("response", "terminate")
	assert.Equal(t, false, terminated["success"], "ending a run the adapter did not start was reported as done")
	assert.True(t, remote.closed, "a refused terminate still detaches the session")
}

// fakeRemote is a Target that reports one snapshot and records Close.
type fakeRemote struct {
	snapshot *v1.DebugSnapshot
	closed   bool
}

func (f *fakeRemote) Snapshot(context.Context) (*v1.DebugSnapshot, error) { return f.snapshot, nil }

func (f *fakeRemote) WaitSnapshot(ctx context.Context, _ uint64) (*v1.DebugSnapshot, error) {
	<-ctx.Done()

	return nil, ctx.Err()
}

func (f *fakeRemote) Resume(context.Context, *v1.DebugResumeRequest) (*v1.DebugReceipt, error) {
	return &v1.DebugReceipt{Status: v1.DebugCommandStatus_DEBUG_COMMAND_STATUS_REFUSED}, nil
}

func (f *fakeRemote) Pause(context.Context, string) (*v1.DebugReceipt, error) {
	return &v1.DebugReceipt{Status: v1.DebugCommandStatus_DEBUG_COMMAND_STATUS_PENDING}, nil
}

func (f *fakeRemote) ReplaceBreakpoints(_ context.Context, req *v1.DebugSetBreakpointsRequest) (*v1.DebugSetBreakpointsResponse, error) {
	if mode := req.GetFailureMode(); mode != v1.DebugFailureMode_DEBUG_FAILURE_MODE_NONE && mode != 0 {
		return &v1.DebugSetBreakpointsResponse{Receipt: &v1.DebugReceipt{Status: v1.DebugCommandStatus_DEBUG_COMMAND_STATUS_UNSUPPORTED}}, nil
	}

	states := make([]*v1.DebugBreakpointState, 0, len(req.GetBreakpoints()))
	for _, bp := range req.GetBreakpoints() {
		states = append(states, &v1.DebugBreakpointState{Id: bp.GetId(), Verified: true})
	}

	return &v1.DebugSetBreakpointsResponse{
		Receipt:     &v1.DebugReceipt{Status: v1.DebugCommandStatus_DEBUG_COMMAND_STATUS_APPLIED},
		Breakpoints: states,
	}, nil
}

func (f *fakeRemote) Inspect(context.Context, *v1.DebugInspectRequest) (*v1.DebugInspectResponse, error) {
	return nil, flowdebug.ErrNotPaused
}

func (f *fakeRemote) Close() error {
	f.closed = true

	return nil
}

// TestAnUnreadableAttachedRunIsLetGoAndTheEditorTold: when a durable run
// cannot be read, the adapter retries, and if it stays unreadable it detaches,
// releasing the lease that would otherwise keep the run held, and ends the
// editor's session with a failure rather than leaving it waiting in silence.
func TestAnUnreadableAttachedRunIsLetGoAndTheEditorTold(t *testing.T) {
	restore := flowdap.ShortenWatchRetries(time.Millisecond)
	t.Cleanup(restore)

	c := newClient(t)
	t.Cleanup(func() { _ = c.Close() })

	remote := &unreadableRemote{fakeRemote: fakeRemote{snapshot: &v1.DebugSnapshot{
		Revision: 1, State: v1.DebugRunState_DEBUG_RUN_STATE_RUNNING, Capabilities: v1.DurableDebugCapabilities(),
	}}}
	server := flowdap.NewServer(nil, c, flowdap.WithAttach(func(context.Context, flowdap.AttachArguments) (*flowdap.Attachment, error) {
		return &flowdap.Attachment{Target: remote}, nil
	}))
	go func() { _ = server.Serve(t.Context()) }()

	c.send(1, "initialize", map[string]any{"adapterID": "flowstate"})
	c.await("response", "initialize")
	c.send(2, "attach", map[string]any{"workflowId": "wf-1"})
	c.await("response", "attach")
	c.send(3, "configurationDone", nil)
	c.await("response", "configurationDone")

	exited := body(c.await("event", "exited"))
	assert.EqualValues(t, 1, exited["exitCode"], "an abandoned run was reported as a clean exit")
	assert.True(t, remote.closed, "the adapter kept the session, and so the lease, of a run it could not read")
	assert.GreaterOrEqual(t, remote.reads.Load(), int32(2), "the adapter gave up without retrying")
}

// unreadableRemote is a fakeRemote whose every wait fails.
type unreadableRemote struct {
	fakeRemote
	reads atomic.Int32
}

func (u *unreadableRemote) WaitSnapshot(context.Context, uint64) (*v1.DebugSnapshot, error) {
	u.reads.Add(1)

	return nil, errors.New("the server did not answer")
}

// TestADetachedLaunchIsWaitedFor is the adapter keeping the process a detached
// run needs: a disconnect without terminateDebuggee ends Serve, and Wait holds
// until the run it let go of has returned rather than exiting under it.
func TestADetachedLaunchIsWaitedFor(t *testing.T) {
	t.Parallel()

	c := newClient(t)
	t.Cleanup(func() { _ = c.Close() })

	release, started := make(chan struct{}), make(chan struct{})
	var terminated atomic.Bool
	server := flowdap.NewServer(nil, c, flowdap.WithLaunch(func(context.Context, flowdap.LaunchArguments) (*flowdap.Launch, error) {
		session, err := flowdebug.New(flowdebug.Options{Controlled: true})
		if err != nil {
			return nil, err
		}

		return &flowdap.Launch{
			Target:    session,
			Start:     func() { close(started); <-release },
			Terminate: func() { terminated.Store(true) },
		}, nil
	}))
	served := make(chan error, 1)
	go func() { served <- server.Serve(t.Context()) }()

	c.send(1, "initialize", map[string]any{"adapterID": "flowstate"})
	c.await("response", "initialize")
	c.send(2, "launch", map[string]any{"program": "detached.yaml", "stopOnEntry": false})
	c.await("response", "launch")
	c.send(3, "configurationDone", nil)
	c.await("response", "configurationDone")
	<-started

	c.send(4, "disconnect", map[string]any{"terminateDebuggee": false})
	c.await("response", "disconnect")
	require.NoError(t, <-served)
	assert.False(t, terminated.Load(), "a disconnect that did not ask to terminate ended the run")

	waited := make(chan struct{})
	go func() { server.Wait(); close(waited) }()
	select {
	case <-waited:
		t.Fatal("Wait returned while the detached run was still going, so the adapter would exit under it")
	case <-time.After(200 * time.Millisecond):
	}

	close(release)
	select {
	case <-waited:
	case <-time.After(20 * time.Second):
		t.Fatal("Wait did not return once the detached run had")
	}
}

// TestAClientThatVanishesDetachesTheRun is the other way a client leaves: its
// stream ends with no disconnect while the run is paused. The session detaches
// as a disconnect would, so the run finishes rather than waiting forever for
// a command, and an adapter waiting on it can exit.
func TestAClientThatVanishesDetachesTheRun(t *testing.T) {
	t.Parallel()

	c, program, finished := launched(t)

	c.send(1, "initialize", map[string]any{"adapterID": "flowstate"})
	c.await("response", "initialize")
	c.send(2, "launch", map[string]any{"program": program, "stopOnEntry": true})
	c.await("response", "launch")
	c.send(3, "configurationDone", nil)
	c.await("response", "configurationDone")
	c.await("event", "stopped")

	require.NoError(t, c.Close())

	select {
	case <-finished:
	case <-time.After(20 * time.Second):
		t.Fatal("the run stayed paused after its client's stream ended, so the adapter never exits")
	}
	assert.Zero(t, c.late.Load(), "the adapter wrote to a client that had gone, which on stdio is a "+
		"broken-pipe write that kills the process under the run it detached")
}
