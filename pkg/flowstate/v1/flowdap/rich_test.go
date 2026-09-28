package flowdap_test

import (
	"context"
	"errors"
	"fmt"
	"net/url"
	"os"
	"path/filepath"
	"slices"
	"strings"
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

	return launchedAt(t, "rich.yaml")
}

// launchedAt is [launched] with the program at name under a fresh directory.
func launchedAt(t *testing.T, name string) (*client, string, <-chan error) {
	t.Helper()

	program := filepath.Join(t.TempDir(), name)
	require.NoError(t, os.MkdirAll(filepath.Dir(program), 0o700))
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
	// A message already in flight when the stream ended may still be
	// written; that race is the process's SIGPIPE handling to absorb. What
	// the detached run does next — its steps, its end — happens only after
	// the adapter has hung up, and none of it may be written.
	c.lateMu.Lock()
	late := slices.Clone(c.lateEvents)
	c.lateMu.Unlock()
	assert.NotContains(t, late, "terminated", "the adapter wrote the detached run's end to a client that had gone")
	assert.NotContains(t, late, "exited", "the adapter wrote the detached run's end to a client that had gone")
}

// TestAZeroBasedClientGetsItsOwnLineNumbers is an editor that initializes with
// linesStartAt1 and columnsStartAt1 false: its breakpoints are read, and its
// frames and answers written, in its coordinates rather than DAP's default.
func TestAZeroBasedClientGetsItsOwnLineNumbers(t *testing.T) {
	t.Parallel()

	c, program, _ := launched(t)

	c.send(1, "initialize", map[string]any{"adapterID": "flowstate", "linesStartAt1": false, "columnsStartAt1": false})
	c.await("response", "initialize")
	c.await("event", "initialized")
	c.send(2, "launch", map[string]any{"program": program})
	c.await("response", "launch")

	// Line 12 is the 1-based 13 inside `touch`; line 0 is the file's first
	// line, which holds no step but is a line this client can name.
	c.send(3, "setBreakpoints", map[string]any{
		"source":      map[string]any{"path": program},
		"breakpoints": []map[string]any{{"line": 12}, {"line": 0}},
	})
	answer := c.await("response", "setBreakpoints")
	require.Equal(t, true, answer["success"], "a zero-based client's first line was refused as malformed")
	lines := body(answer)["breakpoints"].([]any)
	require.Len(t, lines, 2)
	assert.Equal(t, true, lines[0].(map[string]any)["verified"], lines[0])
	assert.EqualValues(t, 12, lines[0].(map[string]any)["line"])
	assert.Equal(t, false, lines[1].(map[string]any)["verified"])
	assert.EqualValues(t, 0, lines[1].(map[string]any)["line"])

	c.send(4, "configurationDone", nil)
	c.await("response", "configurationDone")
	c.await("event", "stopped")

	// The entry step is written on the 1-based line 4.
	c.send(5, "stackTrace", map[string]any{"threadId": 1})
	first := body(c.await("response", "stackTrace"))["stackFrames"].([]any)[0].(map[string]any)
	assert.EqualValues(t, 3, first["line"])
	assert.GreaterOrEqual(t, first["column"].(float64), float64(0))

	c.send(6, "continue", map[string]any{"threadId": 1})
	c.await("response", "continue")
	c.await("event", "stopped")
	c.send(7, "stackTrace", map[string]any{"threadId": 1})
	frame := body(c.await("response", "stackTrace"))["stackFrames"].([]any)[0].(map[string]any)
	// The stop is reported where its step, `touch`, is written: the 1-based
	// line 12, whose zero-based number is 11.
	assert.EqualValues(t, 11, frame["line"], "the stop is reported in the wrong client coordinates")
}

// TestVariableHandlesAreReusedAndBoundedWithinAStop is an editor refreshing
// the same value at one stop, and one asking for more distinct values than a
// stop holds references for: the first reuses its reference, and the second
// stops being handed new ones rather than growing the table without end.
func TestVariableHandlesAreReusedAndBoundedWithinAStop(t *testing.T) {
	t.Parallel()

	c, program, _ := launched(t)

	c.send(1, "initialize", map[string]any{"adapterID": "flowstate"})
	c.await("response", "initialize")
	c.await("event", "initialized")
	c.send(2, "launch", map[string]any{"program": program})
	c.await("response", "launch")
	c.send(3, "configurationDone", nil)
	c.await("response", "configurationDone")
	c.await("event", "stopped")

	seq := 4
	evaluate := func(expression string) float64 {
		c.send(seq, "evaluate", map[string]any{"expression": expression, "frameId": 1})
		seq++
		answer := c.await("response", "evaluate")
		require.Equal(t, true, answer["success"], answer)

		return body(answer)["variablesReference"].(float64)
	}

	first := evaluate("[1, 2]")
	require.NotZero(t, first)
	assert.Equal(t, first, evaluate("[1, 2]"), "the same value at the same stop was issued a second reference")

	for i := range flowdap.MaxVariableHandles - 1 {
		require.NotZero(t, evaluate(fmt.Sprintf("[%d]", i)), "reference %d was refused below the bound", i)
	}
	assert.Zero(t, evaluate("[-1]"), "a stop handed out more references than it may hold")
	assert.Equal(t, first, evaluate("[1, 2]"), "a reference already issued stopped answering at the bound")
}

// TestATerminatedLaunchReportsItsEnd is the protocol's order for a terminate:
// the response, then the run's `terminated` and `exited` once it has stopped,
// and only then the client's `disconnect`. An adapter that stopped listening at
// the terminate would leave the client waiting for events that never come.
func TestATerminatedLaunchReportsItsEnd(t *testing.T) {
	t.Parallel()

	c, program, finished := launched(t)

	c.send(1, "initialize", map[string]any{"adapterID": "flowstate"})
	c.await("response", "initialize")
	c.await("event", "initialized")
	c.send(2, "launch", map[string]any{"program": program})
	c.await("response", "launch")
	c.send(3, "configurationDone", nil)
	c.await("response", "configurationDone")
	c.await("event", "stopped")

	c.send(4, "terminate", map[string]any{})
	require.Equal(t, true, c.await("response", "terminate")["success"])
	c.await("event", "terminated")
	assert.EqualValues(t, 1, body(c.await("event", "exited"))["exitCode"], "a terminated run reported success")
	select {
	case <-finished:
	case <-time.After(20 * time.Second):
		t.Fatal("the terminated run did not end")
	}

	c.send(5, "disconnect", map[string]any{})
	assert.Equal(t, true, c.await("response", "disconnect")["success"],
		"the adapter stopped answering after the terminate")
}

// TestATerminateBeforeConfigurationReportsTheEnd is a client that terminates a
// launch it never configured: no run was started to report its own end, so the
// adapter reports it, rather than leave the client waiting for `terminated`.
func TestATerminateBeforeConfigurationReportsTheEnd(t *testing.T) {
	t.Parallel()

	c, program, _ := launched(t)

	c.send(1, "initialize", map[string]any{"adapterID": "flowstate"})
	c.await("response", "initialize")
	c.await("event", "initialized")
	c.send(2, "launch", map[string]any{"program": program})
	c.await("response", "launch")

	c.send(3, "terminate", map[string]any{})
	require.Equal(t, true, c.await("response", "terminate")["success"])
	c.await("event", "terminated")
	assert.EqualValues(t, 1, body(c.await("event", "exited"))["exitCode"])
}

// TestBreakpointRequestsAreBoundedAtTheEdge is the adapter refusing, before it
// keeps anything, what a breakpoint request could otherwise make it hold or
// misplace: a missing array read as "clear this source", a line past the
// uint32 the source map speaks wrapping onto a small one, and text across
// sources that each request's own bounds would allow to accumulate.
func TestBreakpointRequestsAreBoundedAtTheEdge(t *testing.T) {
	t.Parallel()

	c, program, _ := launched(t)

	c.send(1, "initialize", map[string]any{"adapterID": "flowstate"})
	c.await("response", "initialize")
	c.await("event", "initialized")
	c.send(2, "launch", map[string]any{"program": program})
	c.await("response", "launch")

	seq := 3
	set := func(path string, breakpoints any) map[string]any {
		arguments := map[string]any{"source": map[string]any{"path": path}}
		if breakpoints != nil {
			arguments["breakpoints"] = breakpoints
		}
		c.send(seq, "setBreakpoints", arguments)
		seq++

		return c.await("response", "setBreakpoints")
	}

	require.Equal(t, true, set(program, []map[string]any{{"line": 13}})["success"])
	assert.Equal(t, false, set(program, nil)["success"], "a request with no breakpoints array cleared the source")
	kept := set(program, []map[string]any{{"line": 13}})
	assert.Equal(t, true, body(kept)["breakpoints"].([]any)[0].(map[string]any)["verified"],
		"the malformed request disturbed the installed set")

	c.send(seq, "setExceptionBreakpoints", map[string]any{})
	seq++
	assert.Equal(t, false, c.await("response", "setExceptionBreakpoints")["success"],
		"a request with no filters array cleared the failure stops")

	assert.Equal(t, false, set(program, []map[string]any{{"line": int64(1)<<32 + 13}})["success"],
		"a line past 2^32 was taken, and would have been set on line 13")

	assert.Equal(t, false, set("/"+strings.Repeat("p", 4096)+".yaml", []map[string]any{{"line": 1}})["success"],
		"a source path past the contract's bound was kept")

	// A path is carried by every breakpoint's identity, so a long one across
	// many breakpoints is charged each time, not once.
	many := make([]map[string]any, 0, 600)
	for i := range 600 {
		many = append(many, map[string]any{"line": i + 1})
	}
	amplified := set("/"+strings.Repeat("a", 4000)+".yaml", many)
	assert.Equal(t, false, amplified["success"], "a long path repeated across many breakpoints was charged once")
	assert.Contains(t, amplified["message"], "at most")

	condition := "true" + strings.Repeat(" ", 60<<10)
	refused := false
	for i := range flowdap.MaxBreakpointBytes/len(condition) + 2 {
		answer := set(fmt.Sprintf("/elsewhere-%d.yaml", i), []map[string]any{{"line": 1, "condition": condition}})
		if answer["success"] == false {
			assert.Contains(t, answer["message"], "at most")
			refused = true

			break
		}
	}
	assert.True(t, refused, "breakpoint text across sources grew past the adapter's bound")
}

// TestVariableHandlesAreBoundedByTheirText is a client spending the handle
// table's bytes rather than its count: long, distinct expressions stop being
// handed references once their text reaches the bound.
func TestVariableHandlesAreBoundedByTheirText(t *testing.T) {
	t.Parallel()

	c, program, _ := launched(t)

	c.send(1, "initialize", map[string]any{"adapterID": "flowstate"})
	c.await("response", "initialize")
	c.await("event", "initialized")
	c.send(2, "launch", map[string]any{"program": program})
	c.await("response", "launch")
	c.send(3, "configurationDone", nil)
	c.await("response", "configurationDone")
	c.await("event", "stopped")

	padding := strings.Repeat(" ", 60<<10)
	refused := false
	for i := range flowdap.MaxVariableHandleBytes/len(padding) + 2 {
		c.send(4+i, "evaluate", map[string]any{"expression": fmt.Sprintf("[%d%s]", i, padding), "frameId": 1})
		answer := c.await("response", "evaluate")
		require.Equal(t, true, answer["success"], answer)
		if body(answer)["variablesReference"].(float64) == 0 {
			refused = true

			break
		}
	}
	assert.True(t, refused, "long expressions kept being handed references past the byte bound")
}

// pendingRemote is a durable target that accepts a breakpoint replacement for
// its next step boundary, then reports it applied in the next snapshot.
type pendingRemote struct {
	fakeRemote
	applied chan *v1.DebugSnapshot
	// ended makes the run end without stopping, having installed nothing.
	ended bool
}

func (p *pendingRemote) ReplaceBreakpoints(_ context.Context, req *v1.DebugSetBreakpointsRequest) (*v1.DebugSetBreakpointsResponse, error) {
	states := make([]*v1.DebugBreakpointState, 0, len(req.GetBreakpoints()))
	verified := make([]*v1.DebugBreakpointState, 0, len(req.GetBreakpoints()))
	for _, bp := range req.GetBreakpoints() {
		// The installed set's state under the reused id: the old definition,
		// already verified, which says nothing of the replacement.
		states = append(states, &v1.DebugBreakpointState{Id: bp.GetId(), Verified: true})
		verified = append(verified, &v1.DebugBreakpointState{Id: bp.GetId(), Verified: true})
	}
	// First a snapshot of the run still moving, which shows the set it had —
	// here, none — and says nothing of the replacement; then its next hold,
	// with the replacement installed.
	p.applied <- &v1.DebugSnapshot{Revision: 2, State: v1.DebugRunState_DEBUG_RUN_STATE_RUNNING}
	if p.ended {
		// Ending moves no revision: the run completes at the one it had,
		// still listing the old set under the reused slot ids.
		p.applied <- &v1.DebugSnapshot{Revision: 1, State: v1.DebugRunState_DEBUG_RUN_STATE_COMPLETED, Breakpoints: verified}
	} else {
		p.applied <- &v1.DebugSnapshot{Revision: 3, State: v1.DebugRunState_DEBUG_RUN_STATE_HELD, Breakpoints: verified}
	}

	return &v1.DebugSetBreakpointsResponse{
		Receipt:     &v1.DebugReceipt{Status: v1.DebugCommandStatus_DEBUG_COMMAND_STATUS_PENDING, Revision: 1},
		Breakpoints: states,
	}, nil
}

func (p *pendingRemote) WaitSnapshot(ctx context.Context, after uint64) (*v1.DebugSnapshot, error) {
	select {
	case snapshot := <-p.applied:
		return snapshot, nil
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}

// TestAPendingBreakpointIsReportedOnceApplied is a durable run inside a long
// step: it accepts a breakpoint for its next boundary, and the editor hears
// the breakpoint verified once a snapshot shows it applied, rather than
// showing unverified a breakpoint that will stop the run.
func TestAPendingBreakpointIsReportedOnceApplied(t *testing.T) {
	t.Parallel()

	c := newClient(t)
	t.Cleanup(func() { _ = c.Close() })

	remote := &pendingRemote{
		fakeRemote: fakeRemote{snapshot: &v1.DebugSnapshot{
			Revision: 1, State: v1.DebugRunState_DEBUG_RUN_STATE_RUNNING, Capabilities: v1.DurableDebugCapabilities(),
		}},
		applied: make(chan *v1.DebugSnapshot, 2),
	}
	server := flowdap.NewServer(nil, c, flowdap.WithAttach(func(context.Context, flowdap.AttachArguments) (*flowdap.Attachment, error) {
		return &flowdap.Attachment{Target: remote}, nil
	}))
	go func() { _ = server.Serve(t.Context()) }()

	c.send(1, "initialize", map[string]any{"adapterID": "flowstate"})
	c.await("response", "initialize")
	c.send(2, "attach", map[string]any{"workflowId": "wf-1"})
	c.await("response", "attach")

	c.send(3, "setFunctionBreakpoints", map[string]any{"breakpoints": []map[string]any{{"name": "deploy"}}})
	set := body(c.await("response", "setFunctionBreakpoints"))["breakpoints"].([]any)[0].(map[string]any)
	require.Equal(t, false, set["verified"], "a breakpoint the run has not applied was reported verified")

	c.send(4, "configurationDone", nil)
	c.await("response", "configurationDone")
	changed := body(c.await("event", "breakpoint"))["breakpoint"].(map[string]any)
	assert.Equal(t, true, changed["verified"], "the applied breakpoint was never reported")
	assert.Equal(t, set["id"], changed["id"], "the change named a different breakpoint")
}

// TestEndingServeDetachesTheTarget is an embedding process that cancels the
// adapter while it waits on its client: the session detaches, as it does for
// a client that goes, rather than leave a durable target renewing its lease.
func TestEndingServeDetachesTheTarget(t *testing.T) {
	t.Parallel()

	c := newClient(t)
	t.Cleanup(func() { _ = c.Close() })

	remote := &fakeRemote{snapshot: &v1.DebugSnapshot{
		Revision: 1, State: v1.DebugRunState_DEBUG_RUN_STATE_RUNNING, Capabilities: v1.DurableDebugCapabilities(),
	}}
	server := flowdap.NewServer(nil, c, flowdap.WithAttach(func(context.Context, flowdap.AttachArguments) (*flowdap.Attachment, error) {
		return &flowdap.Attachment{Target: remote}, nil
	}))
	ctx, cancel := context.WithCancel(t.Context())
	served := make(chan error, 1)
	go func() { served <- server.Serve(ctx) }()

	c.send(1, "initialize", map[string]any{"adapterID": "flowstate"})
	c.await("response", "initialize")
	c.send(2, "attach", map[string]any{"workflowId": "wf-1"})
	c.await("response", "attach")

	c.send(3, "attach", map[string]any{"workflowId": "wf-2"})
	assert.Equal(t, false, c.await("response", "attach")["success"], "a second attach replaced the first target")

	cancel()
	select {
	case err := <-served:
		assert.ErrorIs(t, err, context.Canceled)
	case <-time.After(20 * time.Second):
		t.Fatal("Serve did not return once its context ended")
	}
	assert.True(t, remote.closed, "the target was left attached after Serve's context ended")
}

// TestARefusedSecondLaunchChangesNothing is a client that launches twice
// before configuring: the second is refused, and its options do not reach the
// first launch — which still stops on entry as it asked.
func TestARefusedSecondLaunchChangesNothing(t *testing.T) {
	t.Parallel()

	c, program, _ := launched(t)

	c.send(1, "initialize", map[string]any{"adapterID": "flowstate"})
	c.await("response", "initialize")
	c.await("event", "initialized")
	c.send(2, "launch", map[string]any{"program": program})
	require.Equal(t, true, c.await("response", "launch")["success"])
	c.send(3, "launch", map[string]any{"program": program, "stopOnEntry": false})
	require.Equal(t, false, c.await("response", "launch")["success"], "a second launch was taken")

	c.send(4, "configurationDone", nil)
	c.await("response", "configurationDone")
	assert.Equal(t, "entry", body(c.await("event", "stopped"))["reason"],
		"the refused launch's stopOnEntry reached the first launch")
}

// TestAPendingBreakpointTheRunNeverInstalledIsSettledAtItsEnd is a replacement
// the run applies while it keeps moving and never stops for — a breakpoint on
// a step it refuses, say — so its next revision is its end: the editor hears
// the breakpoint was not installed rather than seeing it pending for good.
func TestAPendingBreakpointTheRunNeverInstalledIsSettledAtItsEnd(t *testing.T) {
	t.Parallel()

	c := newClient(t)
	t.Cleanup(func() { _ = c.Close() })

	remote := &pendingRemote{
		fakeRemote: fakeRemote{snapshot: &v1.DebugSnapshot{
			Revision: 1, State: v1.DebugRunState_DEBUG_RUN_STATE_RUNNING, Capabilities: v1.DurableDebugCapabilities(),
		}},
		applied: make(chan *v1.DebugSnapshot, 2),
		ended:   true,
	}
	server := flowdap.NewServer(nil, c, flowdap.WithAttach(func(context.Context, flowdap.AttachArguments) (*flowdap.Attachment, error) {
		return &flowdap.Attachment{Target: remote}, nil
	}))
	go func() { _ = server.Serve(t.Context()) }()

	c.send(1, "initialize", map[string]any{"adapterID": "flowstate"})
	c.await("response", "initialize")
	c.send(2, "attach", map[string]any{"workflowId": "wf-1"})
	c.await("response", "attach")
	c.send(3, "setFunctionBreakpoints", map[string]any{"breakpoints": []map[string]any{{"name": "deplyo"}}})
	set := body(c.await("response", "setFunctionBreakpoints"))["breakpoints"].([]any)[0].(map[string]any)
	c.send(4, "configurationDone", nil)
	c.await("response", "configurationDone")

	changed := body(c.await("event", "breakpoint"))["breakpoint"].(map[string]any)
	assert.Equal(t, set["id"], changed["id"])
	assert.Equal(t, false, changed["verified"], "a breakpoint the run never installed was reported verified")
	assert.NotEmpty(t, changed["message"])
}

// TestAURIClientGetsURIPaths is an editor that initializes with pathFormat
// "uri", on a program whose path needs escaping: the frames it is sent name
// their source as a file URI, and the URI it sends its breakpoints under names
// the same document.
func TestAURIClientGetsURIPaths(t *testing.T) {
	t.Parallel()

	c, program, _ := launchedAt(t, "my flows/rich #1.yaml")
	uri := (&url.URL{Scheme: "file", Path: program}).String()
	require.Contains(t, uri, "%20", "the path needs no escaping, so the test proves nothing")

	c.send(1, "initialize", map[string]any{"adapterID": "flowstate", "pathFormat": "uri"})
	c.await("response", "initialize")
	c.await("event", "initialized")
	c.send(2, "launch", map[string]any{"program": program})
	c.await("response", "launch")

	c.send(3, "setBreakpoints", map[string]any{
		"source":      map[string]any{"path": uri},
		"breakpoints": []map[string]any{{"line": 13}},
	})
	set := body(c.await("response", "setBreakpoints"))["breakpoints"].([]any)[0].(map[string]any)
	assert.Equal(t, true, set["verified"], "a breakpoint under the program's URI named no source: %v", set)

	c.send(4, "configurationDone", nil)
	c.await("response", "configurationDone")
	c.await("event", "stopped")

	c.send(5, "stackTrace", map[string]any{"threadId": 1})
	first := body(c.await("response", "stackTrace"))["stackFrames"].([]any)[0].(map[string]any)
	path, _ := first["source"].(map[string]any)["path"].(string)
	assert.Equal(t, uri, path, "a URI client was not sent the program's URI")
}

// TestBreakpointsInAModifiedSourceAreNotBound is an editor that changed the
// Flowfile after launch: its lines are not the compiled program's, so its
// breakpoints are answered unverified rather than bound through a source map
// of bytes that no longer match, and the set already installed stands.
func TestBreakpointsInAModifiedSourceAreNotBound(t *testing.T) {
	t.Parallel()

	c, program, _ := launched(t)

	c.send(1, "initialize", map[string]any{"adapterID": "flowstate"})
	c.await("response", "initialize")
	c.await("event", "initialized")
	c.send(2, "launch", map[string]any{"program": program})
	c.await("response", "launch")

	c.send(3, "setBreakpoints", map[string]any{"source": map[string]any{"path": program}, "breakpoints": []map[string]any{{"line": 13}}})
	require.Equal(t, true, body(c.await("response", "setBreakpoints"))["breakpoints"].([]any)[0].(map[string]any)["verified"])

	c.send(4, "setBreakpoints", map[string]any{
		"source": map[string]any{"path": program}, "breakpoints": []map[string]any{{"line": 5}}, "sourceModified": true,
	})
	modified := body(c.await("response", "setBreakpoints"))["breakpoints"].([]any)[0].(map[string]any)
	assert.Equal(t, false, modified["verified"], "a breakpoint in an edited file was bound through the old lines")
	assert.Contains(t, modified["message"], "changed")

	// The installed breakpoint still stops the run.
	c.send(5, "configurationDone", nil)
	c.await("response", "configurationDone")
	c.await("event", "stopped")
	c.send(6, "continue", map[string]any{"threadId": 1})
	c.await("response", "continue")
	assert.Equal(t, "breakpoint", body(c.await("event", "stopped"))["reason"], "the set installed before the edit was dropped")
}
