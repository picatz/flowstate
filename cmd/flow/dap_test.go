package main

import (
	"bufio"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/picatz/flowstate/pkg/flowstate/v1/flowfile"
)

// `flow dap` driven the way an editor drives it: a real binary, real
// Content-Length framing, a real workflow.
//
// The package tests underneath this one prove the mapping. This proves the
// *reachability* — that a capability which is complete and tested is also
// something a person can point an editor at, which this repository treats as
// the difference between a feature and scaffolding. Nothing below imports
// flowdap; it speaks the protocol.

// dapConn is one framed conversation with the adapter's stdio.
type dapConn struct {
	t   *testing.T
	in  io.Writer
	out *bufio.Reader
	seq int
}

// send writes one request, framed as the protocol requires.
func (c *dapConn) send(command string, arguments any) {
	c.t.Helper()

	c.seq++
	request := map[string]any{"seq": c.seq, "type": "request", "command": command}
	if arguments != nil {
		request["arguments"] = arguments
	}

	body, err := json.Marshal(request)
	require.NoError(c.t, err)

	_, err = fmt.Fprintf(c.in, "Content-Length: %d\r\n\r\n%s", len(body), body)
	require.NoError(c.t, err)
}

// read returns the next message, decoding the frame the same way.
func (c *dapConn) read() map[string]any {
	c.t.Helper()

	length := 0
	for {
		line, err := c.out.ReadString('\n')
		require.NoError(c.t, err, "the adapter closed its stream mid-frame")

		trimmed := strings.TrimRight(line, "\r\n")
		if trimmed == "" {
			break
		}
		if name, value, ok := strings.Cut(trimmed, ":"); ok && strings.EqualFold(strings.TrimSpace(name), "Content-Length") {
			length, err = strconv.Atoi(strings.TrimSpace(value))
			require.NoError(c.t, err)
		}
	}
	require.NotZero(c.t, length, "a frame arrived with no Content-Length")

	body := make([]byte, length)
	_, err := io.ReadFull(c.out, body)
	require.NoError(c.t, err)

	var message map[string]any
	require.NoError(c.t, json.Unmarshal(body, &message), "the adapter wrote a frame that is not JSON: %s", body)

	return message
}

// await returns the next message of a kind, skipping the rest — a stopped
// event and an unrelated response are genuinely concurrent here, because the
// movement runs on its own goroutine so a client's UI never freezes.
func (c *dapConn) await(kind, name string) map[string]any {
	c.t.Helper()

	for range 100 {
		got := c.read()
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
	}

	c.t.Fatalf("the adapter never sent a %s %q", kind, name)

	return nil
}

// TestFlowDAPStepsARealWorkflowForAnEditor is the reachability proof.
func TestFlowDAPStepsARealWorkflowForAnEditor(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	workflow := filepath.Join(dir, "workflow.yaml")
	require.NoError(t, os.WriteFile(workflow, []byte(`
edition: v2026.3
name: staged
steps:
  - id: build
    value: "'web.tar.gz'"
  - id: test
    value: "'3 passed'"
  - id: deploy
    value: "'shipped'"
outputs: {}
`), 0o600))

	cmd := flowBinaryCommand(buildFlowBinary(t), "dap")
	stdin, err := cmd.StdinPipe()
	require.NoError(t, err)
	stdout, err := cmd.StdoutPipe()
	require.NoError(t, err)
	require.NoError(t, cmd.Start())
	t.Cleanup(func() {
		_ = stdin.Close()
		_ = cmd.Process.Kill()
		_ = cmd.Wait()
	})

	conn := &dapConn{t: t, in: stdin, out: bufio.NewReader(stdout)}

	conn.send("initialize", map[string]any{"adapterID": "flowstate"})
	initialize := conn.await("response", "initialize")
	require.Equal(t, true, initialize["success"])
	assert.Equal(t, true, initialize["body"].(map[string]any)["supportsFunctionBreakpoints"])

	conn.await("event", "initialized")

	// What to run, exactly as a launch configuration names it.
	conn.send("launch", map[string]any{"program": workflow})
	conn.await("response", "launch")

	// A breakpoint on a step id, which is the only kind this adapter can keep.
	conn.send("setFunctionBreakpoints", map[string]any{
		"breakpoints": []map[string]any{{"name": "deploy"}},
	})
	set := conn.await("response", "setFunctionBreakpoints")
	points := set["body"].(map[string]any)["breakpoints"].([]any)
	require.Len(t, points, 1)
	assert.Equal(t, true, points[0].(map[string]any)["verified"])

	conn.send("configurationDone", nil)
	conn.await("response", "configurationDone")

	// The run starts and announces where it stopped, with nothing asked of it.
	// A client waits for exactly this before it will enable a step button, so
	// an adapter that stayed silent here is one an editor never drives.
	entry := conn.await("event", "stopped")
	assert.Equal(t, "entry", entry["body"].(map[string]any)["reason"])

	// Continuing from there lands on the breakpoint.
	conn.send("continue", map[string]any{"threadId": 1})
	conn.await("response", "continue")
	conn.await("event", "stopped")

	conn.send("stackTrace", map[string]any{"threadId": 1})
	trace := conn.await("response", "stackTrace")
	frames := trace["body"].(map[string]any)["stackFrames"].([]any)
	require.Len(t, frames, 1)
	assert.Contains(t, frames[0].(map[string]any)["name"], "deploy",
		"the run did not stop at the step the editor's breakpoint named")

	// What the earlier steps produced, read through the debug console.
	conn.send("evaluate", map[string]any{"expression": "steps.build.value", "context": "repl"})
	evaluated := conn.await("response", "evaluate")
	require.Equal(t, true, evaluated["success"], "evaluate failed: %v", evaluated["message"])
	assert.Contains(t, evaluated["body"].(map[string]any)["result"], "web.tar.gz")

	// And the variables pane, which is the scope listing rendered.
	conn.send("scopes", map[string]any{"frameId": 1})
	scopes := conn.await("response", "scopes")
	groups := scopes["body"].(map[string]any)["scopes"].([]any)
	require.NotEmpty(t, groups, "a paused run offered no scopes, so a variables pane is empty")

	var stepsReference float64
	for _, group := range groups {
		if group.(map[string]any)["name"] == "steps" {
			stepsReference = group.(map[string]any)["variablesReference"].(float64)
		}
	}
	require.NotZero(t, stepsReference, "the paused run did not offer its steps as a scope")

	conn.send("variables", map[string]any{"variablesReference": stepsReference})
	variables := conn.await("response", "variables")
	rendered := variables["body"].(map[string]any)["variables"].([]any)

	names := make([]string, 0, len(rendered))
	for _, entry := range rendered {
		names = append(names, entry.(map[string]any)["name"].(string))
	}
	assert.Contains(t, names, "build")
	assert.Contains(t, names, "test",
		"the variables pane does not show what the run has produced")

	// Letting it go ends the run, and the adapter says so rather than leaving
	// the editor's session open on a workflow that has finished.
	conn.send("continue", map[string]any{"threadId": 1})
	conn.await("response", "continue")
	conn.await("event", "terminated")
}

// TestFlowDAPValidatesBeforeItRunsAnything is the side effect somebody cannot
// take back.
//
// A Flowfile can parse and still be wrong — a step missing a required input is
// this one, and it parses clean because the shape is legal and only the task's
// own rules refuse it. A run started on such a file performs every step *before*
// the bad one and then fails, and under this adapter that is an unstubbed local
// run, so those steps are real. Every other verb that executes a Flowfile goes
// through `loadWorkflow` for exactly this reason, and this one reached past it
// (Codex, #1124).
//
// What the assertion turns on is that a `stopped` event *is* a run under way:
// this debugger stops before every step, so a stop means the engine is at a
// boundary and the person's next `continue` performs the step behind it. So the
// claim is that the adapter says why nothing ran, and never says where it
// stopped.
//
// The first fixture written for this proved nothing — an unknown task key is
// refused by the parser, so it failed identically with the fix reverted. It was
// the mutation that said so, which is the only reason this test is worth
// anything.
func TestFlowDAPValidatesBeforeItRunsAnything(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	workflow := filepath.Join(dir, "workflow.yaml")
	require.NoError(t, os.WriteFile(workflow, []byte(`
edition: v2026.3
name: partial
steps:
  - id: first
    log:
      message: "'this step would run'"
  - id: second
    http: {}
outputs: {}
`), 0o600))

	// It parses. Only the task's own rules refuse it, which is what makes this
	// a validation test rather than a parse test.
	_, _, err := flowfile.ParseFile(workflow)
	require.NoError(t, err, "the fixture is refused by the parser, so it cannot test validation")

	diagnostics, err := flowfile.ValidateSourceFile(workflow)
	require.NoError(t, err)
	require.NotEmpty(t, diagnostics, "the fixture validates, so there is nothing for this to catch")

	cmd := flowBinaryCommand(buildFlowBinary(t), "dap")
	stdin, err := cmd.StdinPipe()
	require.NoError(t, err)
	stdout, err := cmd.StdoutPipe()
	require.NoError(t, err)
	require.NoError(t, cmd.Start())
	t.Cleanup(func() {
		_ = stdin.Close()
		_ = cmd.Process.Kill()
		_ = cmd.Wait()
	})

	conn := &dapConn{t: t, in: stdin, out: bufio.NewReader(stdout)}

	conn.send("initialize", map[string]any{"adapterID": "flowstate"})
	conn.await("response", "initialize")
	conn.await("event", "initialized")

	conn.send("launch", map[string]any{"program": workflow})
	conn.await("response", "launch")

	conn.send("configurationDone", nil)
	conn.await("response", "configurationDone")

	// The refusal reaches the debug console, which is the only place a person
	// is looking — the adapter's own standard output is the protocol stream.
	var said strings.Builder
	for range 30 {
		message := conn.read()

		require.NotEqual(t, "stopped", message["event"],
			"the adapter stopped a run on a workflow that cannot finish, so the person's "+
				"next continue performs the earlier steps for a file that was never going "+
				"to work")

		if message["event"] != "output" {
			continue
		}

		said.WriteString(message["body"].(map[string]any)["output"].(string))
		if strings.Contains(said.String(), "http") {
			break
		}
	}

	assert.Contains(t, said.String(), `requires input "url"`,
		"the console was not told why nothing ran:\n\n%s", said.String())

	// And the editor is told the run failed. A client reads the `exited` event
	// to decide what the debuggee did, so a zero here says a workflow that was
	// refused before it started succeeded — the console message says otherwise
	// and nothing machine-readable agrees with it.
	exited := conn.await("event", "exited")
	assert.Equal(t, float64(1), exited["body"].(map[string]any)["exitCode"],
		"a workflow refused by validation reported a clean exit")
}

// TestFlowDAPAtATerminalSaysWhatItIs keeps `flow dap` from reading as a hang.
//
// It speaks nothing until a client writes to it, which at a terminal is
// silence — the same trap `flow lsp` was given a banner for (#398). The banner
// goes to stderr so a real editor's pipe never sees it and the protocol stream
// on stdout stays a protocol stream.
func TestFlowDAPAtATerminalSaysWhatItIs(t *testing.T) {
	t.Parallel()

	assert.Contains(t, dapBanner, "Debug Adapter Protocol")
	assert.Contains(t, dapBanner, "flow run local --debug",
		"the banner does not point a person at the debugger meant for a terminal")

	// Not written when stdin is a pipe, which is every editor.
	var piped strings.Builder
	writeStdioBanner(&piped, false, dapBanner)
	assert.Empty(t, piped.String(),
		"the banner reached a client's stream, where it is not a frame and cannot be parsed")

	var interactive strings.Builder
	writeStdioBanner(&interactive, true, dapBanner)
	assert.Equal(t, dapBanner, interactive.String())
}
