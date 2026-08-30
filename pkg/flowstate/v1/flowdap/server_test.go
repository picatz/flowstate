package flowdap_test

import (
	"encoding/json"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	v1 "github.com/picatz/flowstate/pkg/flowstate/v1"
	"github.com/picatz/flowstate/pkg/flowstate/v1/flowdap"
	"github.com/picatz/flowstate/pkg/flowstate/v1/flowdebug"
)

// Driving the adapter the way an editor does, because that is the only thing
// that says whether the mapping works. A test that called the mapping
// functions directly would prove the translation and not the conversation.

// client is a DAP client made of two channels, standing in for the framed
// stream the command supplies.
//
// Deliberately not a pipe with real framing: the framing is `lsp`'s, bounded
// and fuzzed there, and reproducing it here would test that package's work
// while adding a place for this one's tests to hang.
type client struct {
	t *testing.T

	toAdapter   chan json.RawMessage
	fromAdapter chan map[string]any

	closeOnce sync.Once
	closed    chan struct{}
}

func newClient(t *testing.T) *client {
	return &client{
		t:           t,
		toAdapter:   make(chan json.RawMessage, 64),
		fromAdapter: make(chan map[string]any, 256),
		closed:      make(chan struct{}),
	}
}

func (c *client) ReadObject(v any) error {
	select {
	case raw := <-c.toAdapter:
		return json.Unmarshal(raw, v)
	case <-c.closed:
		return assert.AnError
	}
}

func (c *client) WriteObject(v any) error {
	encoded, err := json.Marshal(v)
	if err != nil {
		return err
	}

	var decoded map[string]any
	if err := json.Unmarshal(encoded, &decoded); err != nil {
		return err
	}

	select {
	case c.fromAdapter <- decoded:
	case <-c.closed:
	}

	return nil
}

func (c *client) Close() error {
	c.closeOnce.Do(func() { close(c.closed) })

	return nil
}

// send writes one request, numbering it as a client does.
func (c *client) send(seq int, command string, arguments any) {
	c.t.Helper()

	request := map[string]any{"seq": seq, "type": "request", "command": command}
	if arguments != nil {
		request["arguments"] = arguments
	}

	encoded, err := json.Marshal(request)
	require.NoError(c.t, err)

	select {
	case c.toAdapter <- encoded:
	case <-time.After(10 * time.Second):
		c.t.Fatalf("the adapter never read the %q request", command)
	}
}

// next returns the very next message, whatever it is.
//
// For the claims that are about *order*. [client.await] skips to what it is
// looking for, which is right for a stopped event racing an unrelated response
// and blind to a sequence that has to hold.
func (c *client) next() map[string]any {
	c.t.Helper()

	select {
	case got := <-c.fromAdapter:
		return got
	case <-time.After(20 * time.Second):
		c.t.Fatal("the adapter said nothing")

		return nil
	}
}

// await returns the next message of a kind, skipping the rest.
//
// Skipping rather than asserting on order, because a stopped event and a
// response to an unrelated request are genuinely concurrent — the movement runs
// on its own goroutine, which is what keeps a client's UI from freezing.
func (c *client) await(kind, name string) map[string]any {
	c.t.Helper()

	deadline := time.After(20 * time.Second)
	for {
		select {
		case got := <-c.fromAdapter:
			if got["type"] != kind {
				continue
			}
			if kind == "response" && got["command"] != name {
				continue
			}
			if kind == "event" && got["event"] != name {
				continue
			}

			return got

		case <-deadline:
			c.t.Fatalf("the adapter never sent a %s %q", kind, name)

			return nil
		}
	}
}

// walked is a session under an adapter, with a run stepping through ids.
func walked(t *testing.T, steps ...string) (*client, *flowdebug.Session, <-chan error) {
	t.Helper()

	session, err := flowdebug.New(flowdebug.Options{Controlled: true})
	require.NoError(t, err)
	t.Cleanup(func() { _ = session.Close() })

	c := newClient(t)
	t.Cleanup(func() { _ = c.Close() })

	server := flowdap.NewServer(session, c)
	go func() { _ = server.Serve(t.Context()) }()

	scope := v1.NewScope(v1.CurrentProfile, &v1.Workflow_StepOutputs{
		StepValues: map[string]*v1.Node_Outputs{},
	})

	finished := make(chan error, 1)
	go func() {
		// Started only once the client has finished configuring, which is the
		// ordering DAP itself specifies and the reason Launched exists.
		<-server.Launched()

		for _, id := range steps {
			node := &v1.Node{Id: id, Kind: &v1.Node_Value{Value: v1.NewExpr("1")}}
			if err := session.BeforeStep(t.Context(), node, scope); err != nil {
				finished <- err

				return
			}
		}
		finished <- nil
	}()

	return c, session, finished
}

// called is a real local run stopped first at a call and then in its callee.
// It exists separately from walked because a call chain is engine state: a
// test that assembled frames at the adapter would prove the divergence this
// package is forbidden to introduce.
func called(t *testing.T) (*client, <-chan error) {
	t.Helper()

	session, err := flowdebug.New(flowdebug.Options{Controlled: true})
	require.NoError(t, err)
	t.Cleanup(func() { _ = session.Close() })

	c := newClient(t)
	t.Cleanup(func() { _ = c.Close() })

	server := flowdap.NewServer(session, c)
	go func() { _ = server.Serve(t.Context()) }()

	finished := make(chan error, 1)
	go func() {
		<-server.Launched()
		_, runErr := v1.Run(v1.NewContextWithDebugger(t.Context(), session), &v1.Workflow{
			Name: "caller",
			Steps: []*v1.Node{{
				Id: "invoke",
				Kind: &v1.Node_Call{Call: &v1.Call{Workflow: &v1.Workflow{
					Name:  "callee",
					Steps: []*v1.Node{{Id: "inside", Kind: &v1.Node_Value{Value: v1.NewLiteral(1)}}},
				}}},
			}},
		})
		finished <- runErr
	}()

	return c, finished
}

func TestStackTraceUsesTheCoresCallChain(t *testing.T) {
	t.Parallel()

	c, finished := called(t)
	c.send(1, "initialize", map[string]any{"adapterID": "flowstate"})
	c.await("response", "initialize")
	c.await("event", "initialized")
	c.send(2, "launch", map[string]any{})
	c.await("response", "launch")
	c.send(3, "configurationDone", nil)
	c.await("response", "configurationDone")
	c.await("event", "stopped")

	// Step over the caller's call boundary and stop at the callee's first
	// step. The adapter asks the session for the resulting chain; it does not
	// infer one from the inventory or from ids.
	c.send(4, "next", map[string]any{"threadId": 1})
	c.await("response", "next")
	c.await("event", "stopped")
	c.send(5, "stackTrace", map[string]any{"threadId": 1})
	trace := c.await("response", "stackTrace")
	frames := trace["body"].(map[string]any)["stackFrames"].([]any)
	require.Len(t, frames, 2)
	assert.Contains(t, frames[0].(map[string]any)["name"], "callee.inside")
	assert.Contains(t, frames[1].(map[string]any)["name"], "caller.invoke")

	// The chain knows the caller, but only the innermost frame is paused. An
	// empty caller scope is the honest answer; reusing the callee's scope would
	// put correct values under the wrong frame.
	c.send(6, "scopes", map[string]any{"frameId": 2})
	callerScopes := c.await("response", "scopes")
	assert.Empty(t, callerScopes["body"].(map[string]any)["scopes"])

	c.send(7, "continue", map[string]any{"threadId": 1})
	c.await("response", "continue")
	select {
	case err := <-finished:
		require.NoError(t, err)
	case <-time.After(20 * time.Second):
		t.Fatal("the called run did not finish")
	}
}

// TestAnEditorCanStepARun is the conversation, end to end: initialize,
// configure, stop, step, and read where the run is.
func TestAnEditorCanStepARun(t *testing.T) {
	t.Parallel()

	c, _, finished := walked(t, "build", "test", "deploy")

	c.send(1, "initialize", map[string]any{"adapterID": "flowstate"})

	// Read in order, not skipped to. The `initialized` event is what tells a
	// client it may start sending breakpoints, and the specification has it
	// follow the response rather than precede it — a client that sees it first
	// is being told to configure a conversation it has not been told the terms
	// of.
	initialize := c.next()
	require.Equal(t, "response", initialize["type"],
		"the adapter spoke before answering initialize, and what it said was %q",
		initialize["event"])
	require.Equal(t, "initialize", initialize["command"])
	assert.Equal(t, true, initialize["success"])

	body, _ := initialize["body"].(map[string]any)
	assert.Equal(t, true, body["supportsFunctionBreakpoints"],
		"a client told this adapter has no function breakpoints will never send the only "+
			"kind it can honour")
	assert.Equal(t, true, body["supportsConfigurationDoneRequest"])

	initialized := c.next()
	require.Equal(t, "event", initialized["type"])
	require.Equal(t, "initialized", initialized["event"])

	c.send(2, "launch", map[string]any{})
	c.await("response", "launch")

	c.send(3, "configurationDone", nil)
	c.await("response", "configurationDone")

	// The run starts and stops at its first step, and the adapter says so
	// without being asked to. A client waits for exactly this before it will
	// enable a step button.
	entry := c.await("event", "stopped")
	assert.Equal(t, "entry", entry["body"].(map[string]any)["reason"],
		"the first stop is not one anybody asked for, and DAP has a word for that")

	c.send(4, "next", map[string]any{"threadId": 1})
	c.await("response", "next")

	stopped := c.await("event", "stopped")
	stoppedBody, _ := stopped["body"].(map[string]any)
	assert.Equal(t, "step", stoppedBody["reason"])
	assert.Equal(t, float64(1), stoppedBody["threadId"])

	// Where it stopped, as a client's call-stack pane asks.
	c.send(5, "stackTrace", map[string]any{"threadId": 1})
	trace := c.await("response", "stackTrace")
	frames := trace["body"].(map[string]any)["stackFrames"].([]any)
	require.Len(t, frames, 1, "a run has one position, so it has one frame")
	assert.Contains(t, frames[0].(map[string]any)["name"], "test",
		"the frame does not name the step the run is stopped at")

	// And letting it go finishes the run.
	c.send(6, "continue", map[string]any{"threadId": 1})
	c.await("response", "continue")

	select {
	case err := <-finished:
		require.NoError(t, err)
	case <-time.After(20 * time.Second):
		t.Fatal("the run did not finish after the client continued it")
	}
}

// TestTheRunDoesNotStartBeforeTheClientHasConfigured is the ordering DAP
// specifies and the one a debugger is useless without.
//
// Breakpoints arrive *after* launch, so a run started at the launch request is
// already past the step somebody set one on — and the person watches a session
// that will never stop, with no way to tell that from a workflow that simply
// did not reach it.
func TestTheRunDoesNotStartBeforeTheClientHasConfigured(t *testing.T) {
	t.Parallel()

	session, err := flowdebug.New(flowdebug.Options{Controlled: true})
	require.NoError(t, err)
	t.Cleanup(func() { _ = session.Close() })

	c := newClient(t)
	t.Cleanup(func() { _ = c.Close() })

	server := flowdap.NewServer(session, c)
	go func() { _ = server.Serve(t.Context()) }()

	c.send(1, "initialize", map[string]any{"adapterID": "flowstate"})
	c.await("response", "initialize")
	c.await("event", "initialized")

	// A message flowing the wrong way is not a request and is not answered:
	// responses and events are what the adapter sends, and replying to one
	// would be inventing a conversation. The next thing said must therefore be
	// the answer to the *next* request, which is why this is read in order
	// rather than skipped to — an invented reply is exactly what skipping hides.
	c.toAdapter <- json.RawMessage(`{"seq":99,"type":"event","event":"invented"}`)

	// Nor is a request nobody implements answered as though it were: a client
	// probing for a capability is owed a no, and silence reads as a hang.
	c.send(2, "goToTargets", map[string]any{})
	unknown := c.next()
	require.Equal(t, "goToTargets", unknown["command"],
		"the adapter answered a message that was flowing the other way, with %q",
		unknown["command"])
	assert.Equal(t, false, unknown["success"])
	assert.Contains(t, unknown["message"], "goToTargets",
		"a refusal that does not name what was refused leaves a client guessing")

	// A client asks for a stack speculatively, and a run that is not stopped
	// has no frames — which is an answer rather than a failure.
	c.send(3, "stackTrace", map[string]any{"threadId": 1})
	speculative := c.await("response", "stackTrace")
	require.Equal(t, true, speculative["success"],
		"asking where a run is before it is stopped failed the request, which a client "+
			"does routinely")
	assert.Empty(t, speculative["body"].(map[string]any)["stackFrames"])

	c.send(4, "launch", map[string]any{})
	c.await("response", "launch")

	select {
	case <-server.Launched():
		t.Fatal("the run was released to start before the client had configured, so a " +
			"breakpoint set after launch would be set on a step already run")
	case <-time.After(200 * time.Millisecond):
	}

	c.send(5, "configurationDone", nil)
	c.await("response", "configurationDone")

	select {
	case <-server.Launched():
	case <-time.After(10 * time.Second):
		t.Fatal("configurationDone did not release the run")
	}
}

// TestLaunchAloneNeverStartsTheRun is the ordering, from the side that a
// previous version of this adapter got wrong.
//
// `supportsConfigurationDoneRequest` is a field of the *adapter's*
// `Capabilities` response, not of `InitializeRequestArguments`. An adapter that
// reads it out of the initialize *request* therefore finds it absent from every
// real client, concludes none of them can configure, and releases every one of
// them at `launch` — the exact premature start this ordering exists to prevent,
// arrived at by way of the mechanism meant to prevent it.
//
// The client below sends what a real one sends: an `adapterID` and the client
// capabilities that do exist. Nothing releases the run but `configurationDone`.
func TestLaunchAloneNeverStartsTheRun(t *testing.T) {
	t.Parallel()

	session, err := flowdebug.New(flowdebug.Options{Controlled: true})
	require.NoError(t, err)
	t.Cleanup(func() { _ = session.Close() })

	c := newClient(t)
	t.Cleanup(func() { _ = c.Close() })

	server := flowdap.NewServer(session, c)
	go func() { _ = server.Serve(t.Context()) }()

	// What VS Code actually sends. Note what is *not* here, and cannot be:
	// there is no client capability by that name to send.
	c.send(1, "initialize", map[string]any{
		"clientID":                     "vscode",
		"adapterID":                    "flowstate",
		"linesStartAt1":                true,
		"columnsStartAt1":              true,
		"supportsRunInTerminalRequest": true,
		"supportsProgressReporting":    true,
	})
	c.await("response", "initialize")
	c.await("event", "initialized")

	c.send(2, "launch", map[string]any{"program": "/tmp/workflow.yaml"})
	c.await("response", "launch")

	// The breakpoints a real client sends here, *after* launch, which is the
	// whole reason the run must not have started.
	c.send(3, "setFunctionBreakpoints", map[string]any{
		"breakpoints": []map[string]any{{"name": "deploy"}},
	})
	c.await("response", "setFunctionBreakpoints")

	select {
	case <-server.Launched():
		t.Fatal("a client that sent launch and then its breakpoints had the run started " +
			"underneath it, so a breakpoint set here is set on a step that may already " +
			"have run")
	case <-time.After(200 * time.Millisecond):
	}

	c.send(4, "configurationDone", nil)
	c.await("response", "configurationDone")

	select {
	case <-server.Launched():
	case <-time.After(10 * time.Second):
		t.Fatal("configurationDone did not release the run")
	}
}

// TestTheExitCodeSaysWhetherTheRunWorked keeps an editor from reporting a
// success it did not see.
//
// A client reads the `exited` event to decide what the debuggee did. Left at
// the zero value it says every run succeeded — a validation refusal, a failed
// step and a missing `program` alike (Codex, #1124).
//
// Recorded before the session closes, because the close is what releases a
// movement that learns the run is over and reports it: whichever path gets
// there first has to report the same code.
func TestTheExitCodeSaysWhetherTheRunWorked(t *testing.T) {
	t.Parallel()

	for _, want := range []int{0, 1} {
		session, err := flowdebug.New(flowdebug.Options{Controlled: true})
		require.NoError(t, err)

		c := newClient(t)
		server := flowdap.NewServer(session, c)

		server.Exited(want)
		server.Finished()

		exited := c.await("event", "exited")
		assert.Equal(t, float64(want), exited["body"].(map[string]any)["exitCode"],
			"the run ended with %d and the client was told otherwise", want)

		_ = session.Close()
		_ = c.Close()
	}

	// And a code recorded after the end has been reported changes nothing,
	// which is what the ordering above is protecting.
	session, err := flowdebug.New(flowdebug.Options{Controlled: true})
	require.NoError(t, err)
	t.Cleanup(func() { _ = session.Close() })

	c := newClient(t)
	t.Cleanup(func() { _ = c.Close() })

	server := flowdap.NewServer(session, c)
	server.Finished()
	server.Exited(1)

	exited := c.await("event", "exited")
	assert.Equal(t, float64(0), exited["body"].(map[string]any)["exitCode"],
		"a code set after the exit was reported rewrote what the client had already been told")
}

// TestTheFirstStopIsAnnouncedWithoutBeingAskedFor is what makes an editor's
// buttons work.
//
// A DAP client considers the target *running* after launch and waits to be told
// it stopped before enabling movement. An adapter that emits `stopped` only
// from a movement it was asked for is therefore one no client will ever ask to
// move: the person watches a run that is, as far as the editor can tell, still
// going, with every button greyed out.
//
// This went unnoticed because the tests sent `continue` straight after
// configuring, which is a thing no client does — so the run was always moved by
// something that did not need to be told where it was (Codex, #1124). Nothing
// is sent here after `configurationDone` at all.
func TestTheFirstStopIsAnnouncedWithoutBeingAskedFor(t *testing.T) {
	t.Parallel()

	c, _, finished := walked(t, "build", "test")

	c.send(1, "initialize", map[string]any{"adapterID": "flowstate"})
	c.await("response", "initialize")
	c.await("event", "initialized")

	c.send(2, "configurationDone", nil)
	c.await("response", "configurationDone")

	// Nothing further is sent. The stop has to arrive on its own.
	stopped := c.await("event", "stopped")
	body := stopped["body"].(map[string]any)
	assert.Equal(t, "entry", body["reason"],
		"the arrival stop is not one anybody asked for, and `entry` is DAP's word for it")
	assert.Equal(t, float64(1), body["threadId"])

	// And it is a real position, not a placeholder: the frame names the step
	// the run is actually held at.
	c.send(3, "stackTrace", map[string]any{"threadId": 1})
	frames := c.await("response", "stackTrace")["body"].(map[string]any)["stackFrames"].([]any)
	require.Len(t, frames, 1)
	assert.Contains(t, frames[0].(map[string]any)["name"], "build",
		"the entry stop was announced somewhere other than where the run is held")

	c.send(4, "continue", map[string]any{"threadId": 1})
	c.await("response", "continue")
	<-finished
}

// TestTheEntryStopIsAnnouncedEvenWhenAClientMovesFirst is the entry stop's own
// race, and the reason movement waits for it.
//
// Two things report a pause: the announcement on arrival, and a movement. A
// client that moves before hearing the first stop lets them race for it — and
// the movement wins by *consuming* the pause, so the announcement then finds
// either a later pause or, on a run with nothing after it, no pause at all. The
// arrival is never reported and the person is never told where the run began.
//
// A conforming client waits for `stopped` before it will move, so it never
// produces this ordering. That is exactly why the guarantee has to be the
// adapter's rather than the client's: a promise kept only by well-behaved peers
// is not a promise, and this repository's rule is that a bound nothing reaches
// is a bound nothing tests.
//
// One step, so that a movement which got there first leaves no second pause to
// find, which is what turns a mislabelled stop into a missing one. Iterations,
// because a window is a probability: the shutdown race on #1122 needed two
// thousand attempts before it showed twice in three runs, and two hundred
// showed none.
func TestTheEntryStopIsAnnouncedEvenWhenAClientMovesFirst(t *testing.T) {
	t.Parallel()

	for attempt := range 60 {
		session, err := flowdebug.New(flowdebug.Options{Controlled: true})
		require.NoError(t, err)

		c := newClient(t)

		server := flowdap.NewServer(session, c)
		go func() { _ = server.Serve(t.Context()) }()

		scope := v1.NewScope(v1.CurrentProfile, &v1.Workflow_StepOutputs{
			StepValues: map[string]*v1.Node_Outputs{},
		})

		ran := make(chan struct{})
		go func() {
			defer close(ran)
			<-server.Launched()

			node := &v1.Node{Id: "only", Kind: &v1.Node_Value{Value: v1.NewExpr("1")}}
			_ = session.BeforeStep(t.Context(), node, scope)

			// The run is over, and this package cannot see that for itself —
			// so the owner says so, which is what releases the movement still
			// waiting for a stop that is not coming.
			_ = session.Close()
			server.Finished()
		}()

		c.send(1, "initialize", map[string]any{"adapterID": "flowstate"})
		c.await("response", "initialize")
		c.await("event", "initialized")

		// Configured and moved in the same breath: the ordering a conforming
		// client never produces, and the one the guarantee is for.
		c.send(2, "configurationDone", nil)
		c.send(3, "next", map[string]any{"threadId": 1})

		stops := 0
		for {
			message := c.next()
			if message["event"] == "stopped" {
				stops++
			}
			if message["event"] == "terminated" {
				break
			}
		}

		require.Equal(t, 1, stops,
			"attempt %d: the run reached one pause and the client was told about it %d "+
				"times — a movement that arrived first consumed the arrival, so nobody "+
				"was ever told where the run began", attempt, stops)

		<-ran
		_ = c.Close()
	}
}

// TestAScopeAddressIsGoodForOnePauseOnly is why the adapter forgets its
// references at every stop.
//
// A `variablesReference` is an address a client holds onto: it asks for scopes
// once, then asks for variables as a person expands a pane. If the address
// outlived the pause it was minted for, an editor still showing the previous
// stop's tree would repaint it with the *current* run position's values — every
// row plausible, none of them an answer to the question the pane is asking.
//
// So a stale one is not known, and an unknown one is empty. The client asks for
// scopes again after each stop, which is DAP's own order.
func TestAScopeAddressIsGoodForOnePauseOnly(t *testing.T) {
	t.Parallel()

	c, _, finished := walked(t, "build", "test", "deploy")

	c.send(1, "initialize", map[string]any{"adapterID": "flowstate"})
	c.await("response", "initialize")
	c.await("event", "initialized")
	c.send(2, "configurationDone", nil)
	c.await("response", "configurationDone")

	// The entry stop, which arrives without being asked for. See
	// [TestTheFirstStopIsAnnouncedWithoutBeingAskedFor].
	c.await("event", "stopped")

	c.send(3, "next", map[string]any{"threadId": 1})
	c.await("response", "next")
	c.await("event", "stopped")

	c.send(4, "scopes", map[string]any{"frameId": 1})
	first := c.await("response", "scopes")
	groups := first["body"].(map[string]any)["scopes"].([]any)
	require.NotEmpty(t, groups, "a paused run offered no scopes at all")

	stale := groups[0].(map[string]any)["variablesReference"].(float64)
	require.NotZero(t, stale,
		"DAP reserves reference zero for a value with no children, so a scope handed out "+
			"with zero is one a client will never ask about")

	// The address answers now, while the pause it belongs to is the one the run
	// is in.
	c.send(5, "variables", map[string]any{"variablesReference": stale})
	require.NotEmpty(t, c.await("response", "variables")["body"].(map[string]any)["variables"],
		"the address minted for this pause did not answer during it")

	c.send(6, "next", map[string]any{"threadId": 1})
	c.await("response", "next")
	c.await("event", "stopped")

	// And not after it.
	c.send(7, "variables", map[string]any{"variablesReference": stale})
	after := c.await("response", "variables")
	assert.Empty(t, after["body"].(map[string]any)["variables"],
		"an address from the previous pause was answered with this pause's values, so a "+
			"pane a client has not refreshed shows the wrong run position's scope")

	// A fresh address for the same group does answer, so what was refused was
	// the staleness and not the group.
	c.send(8, "scopes", map[string]any{"frameId": 1})
	second := c.await("response", "scopes")
	fresh := second["body"].(map[string]any)["scopes"].([]any)[0].(map[string]any)["variablesReference"].(float64)
	assert.NotEqual(t, stale, fresh, "the same address was handed out for a different pause")

	c.send(9, "variables", map[string]any{"variablesReference": fresh})
	assert.NotEmpty(t, c.await("response", "variables")["body"].(map[string]any)["variables"])

	c.send(10, "continue", map[string]any{"threadId": 1})
	c.await("response", "continue")
	<-finished
}

// TestAHugeScopeIsBoundedAndSaysWhatItDropped reaches the bound rather than
// only staying under it.
//
// [flowdap.MaxScopeVariables] exists because rendering a name costs an
// evaluation and an editor repaints a variables pane freely, so a run inside a
// long loop naming thousands of steps is real work done on every repaint. A
// bound nothing reaches is a bound nothing tests — and the half that matters
// here is the last row: what is dropped is *said*, because a pane that simply
// stops is one a person reads as the whole scope.
func TestAHugeScopeIsBoundedAndSaysWhatItDropped(t *testing.T) {
	t.Parallel()

	const steps = flowdap.MaxScopeVariables + 3

	session, err := flowdebug.New(flowdebug.Options{Controlled: true})
	require.NoError(t, err)
	t.Cleanup(func() { _ = session.Close() })

	c := newClient(t)
	t.Cleanup(func() { _ = c.Close() })

	server := flowdap.NewServer(session, c)
	go func() { _ = server.Serve(t.Context()) }()

	values := make(map[string]*v1.Node_Outputs, steps)
	for i := range steps {
		values[fmt.Sprintf("step%04d", i)] = &v1.Node_Outputs{}
	}
	scope := v1.NewScope(v1.CurrentProfile, &v1.Workflow_StepOutputs{StepValues: values})

	finished := make(chan error, 1)
	go func() {
		<-server.Launched()

		// Two, so that stepping off the first lands on a second pause rather
		// than on the end of a run this test never releases.
		for _, id := range []string{"first", "second"} {
			node := &v1.Node{Id: id, Kind: &v1.Node_Value{Value: v1.NewExpr("1")}}
			if err := session.BeforeStep(t.Context(), node, scope); err != nil {
				finished <- err

				return
			}
		}
		finished <- nil
	}()

	c.send(1, "initialize", map[string]any{"adapterID": "flowstate"})
	c.await("response", "initialize")
	c.await("event", "initialized")
	c.send(2, "configurationDone", nil)
	c.await("response", "configurationDone")

	// The entry stop, which arrives without being asked for. See
	// [TestTheFirstStopIsAnnouncedWithoutBeingAskedFor].
	c.await("event", "stopped")

	c.send(3, "next", map[string]any{"threadId": 1})
	c.await("response", "next")
	c.await("event", "stopped")

	c.send(4, "scopes", map[string]any{"frameId": 1})
	var reference float64
	for _, group := range c.await("response", "scopes")["body"].(map[string]any)["scopes"].([]any) {
		if group.(map[string]any)["name"] == "steps" {
			reference = group.(map[string]any)["variablesReference"].(float64)
		}
	}
	require.NotZero(t, reference, "a run with %d steps offered no steps scope", steps)

	c.send(5, "variables", map[string]any{"variablesReference": reference})
	rendered := c.await("response", "variables")["body"].(map[string]any)["variables"].([]any)

	require.Len(t, rendered, flowdap.MaxScopeVariables+1,
		"the bound was not reached, so nothing here tests it — %d names produced %d rows",
		steps, len(rendered))

	last := rendered[len(rendered)-1].(map[string]any)
	assert.Contains(t, last["value"], "3 more",
		"the pane stops without saying it stopped, which a person reads as the whole scope")

	// The scope listing itself is not narrowed: a client asking what the run
	// can name gets every name, and only the rendering is bounded.
	names, err := session.Scope()
	require.NoError(t, err)
	for _, group := range names {
		if group.Group == "steps" {
			assert.Len(t, group.Names, steps,
				"the bound on rendering leaked into what the run reports it can name")
		}
	}

	c.send(6, "continue", map[string]any{"threadId": 1})
	c.await("response", "continue")
	<-finished
}

// TestARefusedBreakpointSetVerifiesNothing is the fail-closed direction.
//
// A client sends the whole set each time one changes, and the session refuses
// an oversized set *whole* — so no entry may come back verified. The
// alternative is a person watching for stops at breakpoints the session never
// took, which is the same silence a breakpoint exists to break.
func TestARefusedBreakpointSetVerifiesNothing(t *testing.T) {
	t.Parallel()

	// No run: setting breakpoints is a question about the future, so it is
	// answerable before anything has started — which is exactly when a client
	// asks, and the reason the session takes them directly rather than as a
	// command a pause has to deliver.
	session, err := flowdebug.New(flowdebug.Options{Controlled: true})
	require.NoError(t, err)
	t.Cleanup(func() { _ = session.Close() })

	c := newClient(t)
	t.Cleanup(func() { _ = c.Close() })

	server := flowdap.NewServer(session, c)
	go func() { _ = server.Serve(t.Context()) }()

	c.send(1, "initialize", map[string]any{"adapterID": "flowstate"})
	c.await("response", "initialize")
	c.await("event", "initialized")

	// One past what a session holds, so the set is refused rather than trimmed.
	asked := make([]map[string]any, 0, flowdebug.MaxBreakpoints+1)
	for i := range flowdebug.MaxBreakpoints + 1 {
		asked = append(asked, map[string]any{"name": fmt.Sprintf("step%04d", i)})
	}

	c.send(2, "setFunctionBreakpoints", map[string]any{"breakpoints": asked})
	answer := c.await("response", "setFunctionBreakpoints")
	assert.Equal(t, true, answer["success"],
		"failing the request hides the reason where a client shows breakpoint state")

	points := answer["body"].(map[string]any)["breakpoints"].([]any)
	require.Len(t, points, len(asked), "a client is owed one answer per breakpoint it sent")
	for i, point := range points {
		entry := point.(map[string]any)
		require.Equal(t, false, entry["verified"],
			"breakpoint %d claims to be set from a set the session refused whole", i)
		require.NotEmpty(t, entry["message"], "breakpoint %d was refused without saying why", i)
	}

	// And nothing was left behind: the refusal happened before the session's
	// set was touched.
	assert.NoError(t, session.SetBreakpoints([]string{"build"}),
		"the session was left in a state that will not take a breakpoint")

	// An empty name is refused on its own, without taking the rest down.
	c.send(3, "setFunctionBreakpoints", map[string]any{
		"breakpoints": []map[string]any{{"name": "  "}, {"name": "build"}},
	})
	mixed := c.await("response", "setFunctionBreakpoints")["body"].(map[string]any)["breakpoints"].([]any)
	require.Len(t, mixed, 2)
	assert.Equal(t, false, mixed[0].(map[string]any)["verified"])
	assert.Equal(t, true, mixed[1].(map[string]any)["verified"],
		"one unusable name took a usable one with it")
}

// TestDisconnectingReleasesTheRun keeps an editor's close button from stranding
// a workflow.
//
// The run is parked inside the debugger with nobody left to press continue, so
// a `disconnect` that only stopped answering would leave it held until its
// context expired — a workflow suspended by a debugger that is gone.
func TestDisconnectingReleasesTheRun(t *testing.T) {
	t.Parallel()

	c, _, finished := walked(t, "build", "test", "deploy")

	c.send(1, "initialize", map[string]any{"adapterID": "flowstate"})
	c.await("response", "initialize")
	c.await("event", "initialized")
	c.send(2, "configurationDone", nil)
	c.await("response", "configurationDone")

	// The entry stop, which arrives without being asked for. See
	// [TestTheFirstStopIsAnnouncedWithoutBeingAskedFor].
	c.await("event", "stopped")

	c.send(3, "next", map[string]any{"threadId": 1})
	c.await("response", "next")
	c.await("event", "stopped")

	c.send(4, "disconnect", map[string]any{})
	c.await("response", "disconnect")

	select {
	case err := <-finished:
		assert.NoError(t, err, "the released run did not finish cleanly")
	case <-time.After(20 * time.Second):
		t.Fatal("the run stayed held after the client disconnected, so an editor's close " +
			"button strands the workflow it was debugging")
	}
}

// TestTheRunEndsOnceEvenWhenTwoThingsSeeItEnd is the once-guard, from the
// direction that makes it necessary.
//
// Two things learn a run is over: whoever owns it, watching it return, and a
// movement outstanding at the time, which meets [flowdebug.ErrRunOver]. Both
// say so. A client told twice puts its session away twice, which in an editor
// is a second debug toolbar tearing down a session that is already gone.
func TestTheRunEndsOnceEvenWhenTwoThingsSeeItEnd(t *testing.T) {
	t.Parallel()

	session, err := flowdebug.New(flowdebug.Options{Controlled: true})
	require.NoError(t, err)
	t.Cleanup(func() { _ = session.Close() })

	c := newClient(t)
	t.Cleanup(func() { _ = c.Close() })

	server := flowdap.NewServer(session, c)

	var both sync.WaitGroup
	both.Add(2)
	for range 2 {
		go func() {
			defer both.Done()
			server.Finished()
		}()
	}
	both.Wait()

	terminated := 0
	for {
		select {
		case message := <-c.fromAdapter:
			if message["event"] == "terminated" {
				terminated++
			}

			continue
		default:
		}

		break
	}

	assert.Equal(t, 1, terminated,
		"the client was told the run ended %d times, so an editor tears down a session "+
			"that is already gone", terminated)
}

// TestAnExpressionThatDoesNotCompileFailsTheRequestNotTheSession is what a
// debug console is for.
//
// Somebody asking questions will ask some that do not parse, exactly as at the
// prompt. DAP's way of putting the message beside what was typed is an
// unsuccessful *response* — so the request fails, the session does not, and the
// next question is answered.
func TestAnExpressionThatDoesNotCompileFailsTheRequestNotTheSession(t *testing.T) {
	t.Parallel()

	c, _, finished := walked(t, "build", "test")

	c.send(1, "initialize", map[string]any{"adapterID": "flowstate"})
	c.await("response", "initialize")
	c.await("event", "initialized")
	c.send(2, "configurationDone", nil)
	c.await("response", "configurationDone")

	// The entry stop, which arrives without being asked for. See
	// [TestTheFirstStopIsAnnouncedWithoutBeingAskedFor].
	c.await("event", "stopped")

	c.send(3, "next", map[string]any{"threadId": 1})
	c.await("response", "next")
	c.await("event", "stopped")

	c.send(4, "evaluate", map[string]any{"expression": "steps.[", "context": "repl"})
	broken := c.await("response", "evaluate")
	assert.Equal(t, false, broken["success"])
	assert.NotEmpty(t, broken["message"], "a failed evaluation said nothing about why")

	// The session is still answering, which is the half a person notices.
	c.send(5, "evaluate", map[string]any{"expression": "1 + 1", "context": "repl"})
	fine := c.await("response", "evaluate")
	require.Equal(t, true, fine["success"], "one bad expression ended the console: %v", fine["message"])
	assert.Equal(t, "2", fine["body"].(map[string]any)["result"])

	c.send(6, "continue", map[string]any{"threadId": 1})
	c.await("response", "continue")
	<-finished
}
