package main

import (
	"bufio"
	"encoding/json"
	"fmt"
	"io"
	"maps"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	v1 "github.com/picatz/flowstate/pkg/flowstate/v1"
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
	t    *testing.T
	in   io.Writer
	out  *bufio.Reader
	seq  int
	seen strings.Builder
}

const dapSensitiveValue = "s3cr3t-value-nothing-may-print"

func TestFlowDAPRefusesSensitiveWorkflowWithoutReveal(t *testing.T) {
	dir := t.TempDir()
	workflow := filepath.Join(dir, "workflow.yaml")
	require.NoError(t, os.WriteFile(workflow, []byte(`edition: v2026.4
name: `+dapSensitiveValue+`
inputs:
  token:
    type: string
    sensitive: true
    default: `+dapSensitiveValue+`
steps:
  - id: first
    value: "'hello'"
outputs: {}
`), 0o600))

	stdout, stderr := flowDAPRefusal(t, workflow)
	require.NotContains(t, stdout, dapSensitiveValue)
	require.NotContains(t, stderr, dapSensitiveValue)
}

func TestFlowDAPRefusesSensitiveEmbeddedWorkflowWithoutReveal(t *testing.T) {
	dir := t.TempDir()
	callee := filepath.Join(dir, "callee.yaml")
	require.NoError(t, os.WriteFile(callee, []byte(`edition: v2026.4
name: sensitive-callee
inputs:
  token:
    type: string
    sensitive: true
    default: `+dapSensitiveValue+`
steps:
  - id: inside
    value: ${inputs.token}
outputs: {}
`), 0o600))
	workflow := filepath.Join(dir, "workflow.yaml")
	require.NoError(t, os.WriteFile(workflow, []byte(`edition: v2026.4
name: ordinary-caller
steps:
  - id: called
    call: ./callee.yaml
outputs: {}
`), 0o600))

	stdout, stderr := flowDAPRefusal(t, workflow)
	require.NotContains(t, stdout, dapSensitiveValue)
	require.NotContains(t, stderr, dapSensitiveValue)
}

func TestFlowDAPWithholdsDiagnosticsForAnInvalidWorkflow(t *testing.T) {
	dir := t.TempDir()
	workflow := filepath.Join(dir, "workflow.yaml")
	require.NoError(t, os.WriteFile(workflow, []byte(`edition: v2026.4
name: invalid-sensitive-probe
steps:
  - id: first
    value: '${["`+dapSensitiveValue+`"]'
outputs: {}
`), 0o600))

	stdout, stderr := flowDAPRefusal(t, workflow)
	require.Contains(t, stdout, "workflow diagnostics withheld")
	require.NotContains(t, stdout, dapSensitiveValue)
	require.NotContains(t, stderr, dapSensitiveValue)
}

func TestFlowDAPReportsMissingWorkflowWithoutReveal(t *testing.T) {
	missing := filepath.Join(t.TempDir(), "missing-workflow.yaml")

	stdout, stderr := flowDAPFailedLaunch(t, missing, "missing-workflow.yaml")
	require.Contains(t, stdout, "missing-workflow.yaml")
	require.NotContains(t, stdout, "workflow diagnostics withheld")
	require.NotContains(t, stdout, "--reveal-sensitive")
	require.Empty(t, stderr)
}

func TestFlowDAPRevealsSensitiveWorkflowOnlyWhenExplicitlyRequested(t *testing.T) {
	dir := t.TempDir()
	workflow := filepath.Join(dir, "workflow.yaml")
	require.NoError(t, os.WriteFile(workflow, []byte(`edition: v2026.4
name: sensitive-probe
inputs:
  token:
    type: string
    sensitive: true
    default: `+dapSensitiveValue+`
steps:
  - id: first
    value: "'hello'"
outputs: {}
`), 0o600))

	for _, test := range []struct {
		name       string
		commandArg []string
		launchArg  map[string]any
	}{
		{"adapter flag", []string{"--reveal-sensitive"}, map[string]any{}},
		{"launch configuration", nil, map[string]any{"revealSensitive": true}},
	} {
		t.Run(test.name, func(t *testing.T) {
			args := append([]string{"dap"}, test.commandArg...)
			cmd := flowBinaryCommand(buildFlowBinary(t), args...)
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
			launch := map[string]any{"program": workflow}
			maps.Copy(launch, test.launchArg)
			conn.send("launch", launch)
			conn.await("response", "launch")
			conn.send("configurationDone", nil)
			conn.await("response", "configurationDone")
			conn.await("event", "stopped")

			conn.send("evaluate", map[string]any{"expression": "inputs.token", "frameId": 1})
			evaluated := conn.await("response", "evaluate")
			require.Equal(t, true, evaluated["success"])
			require.Contains(t, evaluated["body"].(map[string]any)["result"], dapSensitiveValue)
		})
	}
}

func flowDAPRefusal(t *testing.T, workflow string) (stdoutText, stderrText string) {
	t.Helper()
	stdoutText, stderrText = flowDAPFailedLaunch(t, workflow, "--reveal-sensitive")
	require.Contains(t, stdoutText, "--reveal-sensitive")
	require.Contains(t, stdoutText, "revealSensitive")
	return stdoutText, stderrText
}

func flowDAPFailedLaunch(t *testing.T, workflow, messageFragment string) (stdoutText, stderrText string) {
	t.Helper()

	cmd := flowBinaryCommand(buildFlowBinary(t), "dap")
	stdin, err := cmd.StdinPipe()
	require.NoError(t, err)
	stdout, err := cmd.StdoutPipe()
	require.NoError(t, err)
	var stderr strings.Builder
	cmd.Stderr = &stderr
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
	// A refused launch is answered as the protocol says an editor expects: a
	// failed launch response carrying the reason, and no debuggee to exit.
	conn.send("launch", map[string]any{"program": workflow})
	launched := conn.await("response", "launch")
	require.Equal(t, false, launched["success"], "the refused workflow launched")
	require.Contains(t, launched["message"], messageFragment)

	conn.send("disconnect", nil)
	conn.await("response", "disconnect")
	_ = stdin.Close()
	require.NoError(t, cmd.Wait(), "the adapter did not end cleanly after a refused launch")

	return conn.seen.String(), stderr.String()
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
	c.seen.Write(body)

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
edition: v2026.4
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

// TestFlowDAPAcceptsAPluginTask proves the editor front reaches the same launched
// plugin registry and worker-side secret runtime as `flow run local
// --plugin-dir`. The example plugin contributes both the task and the example:
// provider; resolving through it proves the adapter did not launch the host with
// a nil registry or execute without installing the resulting task runtime.
func TestFlowDAPAcceptsAPluginTask(t *testing.T) {
	dir := t.TempDir()
	workflow := filepath.Join(dir, "workflow.yaml")
	policy := filepath.Join(dir, "auth.yaml")
	t.Setenv("EXAMPLE_SECRET_API_KEY", "dap-test-token")
	require.NoError(t, os.WriteFile(policy, []byte(`issuers:
  - name: local
    actions: []
    issuer: https://issuer.example
    audiences: [flowstate]
    algorithms: [RS256]
secrets:
  allow: ['true']
`), 0o600))
	require.NoError(t, os.WriteFile(workflow, []byte(`edition: v2026.4
name: plugin-debug
steps:
  - id: hello
    example.greet:
      greeting: Hello
      name: debugger
      token: ${secret('example:api-key')}
outputs: {}
`), 0o600))

	cmd := flowBinaryCommand(buildFlowBinary(t), "dap",
		"--plugin-dir", buildExamplePluginDir(t),
		"--auth-policy", policy)
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
	entry := conn.await("event", "stopped")
	assert.Equal(t, "entry", entry["body"].(map[string]any)["reason"],
		"the plugin-backed workflow did not reach a debuggable run")

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
edition: v2026.4
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

	// Reveal authorizes the invalid file's source diagnostics. The default DAP
	// posture withholds them because no valid specification exists to classify.
	cmd := flowBinaryCommand(buildFlowBinary(t), "dap", "--reveal-sensitive")
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

	// The refusal is the launch's own failed response: an editor shows its
	// message where the person is looking, and success=false is the
	// machine-readable answer that nothing ran. A launch that succeeded here
	// would let the person's next continue perform the earlier steps for a file
	// that was never going to work.
	conn.send("launch", map[string]any{"program": workflow})
	launched := conn.await("response", "launch")
	require.Equal(t, false, launched["success"], "a workflow refused by validation launched")
	assert.Contains(t, launched["message"], `requires input "url"`,
		"the editor was not told why nothing ran")
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

// TestFlowDAPRefusesAPolicyItCannotLoad is the fail-closed half of giving this
// adapter the deployment policy flags (#1119).
//
// `flow dap` runs the workflow its client names, with this operator's secret
// providers and plugins behind it, so it is a real local execution surface. It
// took neither --egress-policy nor --task-policy, which meant a rehearsal here
// ran under the permissive defaults while the worker it is rehearsing enforced
// an operator's file — a Flowfile could resolve an allowed secret and send it
// to a public endpoint an egress policy would have refused.
//
// The claim is the order as much as the refusal: both policies load before any
// plugin process starts and before a client can name a program, so a policy
// that cannot be read refuses the adapter rather than leaving it serving under
// the defaults.
func TestFlowDAPRefusesAPolicyItCannotLoad(t *testing.T) {
	t.Parallel()

	for _, flag := range []string{"egress-policy", "task-policy"} {
		t.Run(flag, func(t *testing.T) {
			t.Parallel()

			cmd := newDAPCommand()
			missing := filepath.Join(t.TempDir(), "absent.yaml")
			require.NoError(t, cmd.Flags().Set(flag, missing))

			// No stdin is wired, so reaching the protocol server at all would
			// block rather than return: an error naming the file is therefore
			// also evidence that nothing was served.
			err := runDAP(cmd, nil)
			require.Error(t, err, "a policy file that cannot be read was accepted")
			require.Contains(t, err.Error(), missing)
		})
	}
}

// TestFlowDAPLaunchCarriesTheRunsInputs: a launch configuration's `inputs`
// are the run's arguments, bound before anything runs. A launch missing a
// required one is refused, saying where it goes, rather than accepted into a
// run that can only fail; one carrying it runs with it. And a launch that
// does not stop on entry never narrates the entry stop it did not make.
func TestFlowDAPLaunchCarriesTheRunsInputs(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	workflow := filepath.Join(dir, "workflow.yaml")
	require.NoError(t, os.WriteFile(workflow, []byte(`
edition: v2026.4
name: released
inputs:
  release:
    type: string
    required: true
steps:
  - id: tagged
    value: "${'v' + inputs.release}"
  - id: done
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
	conn.await("response", "initialize")
	conn.await("event", "initialized")

	conn.send("launch", map[string]any{"program": workflow})
	refused := conn.await("response", "launch")
	require.Equal(t, false, refused["success"], "a launch missing a required input was accepted")
	assert.Contains(t, refused["message"], `"release"`)
	assert.Contains(t, refused["message"], "`inputs` object of the launch configuration",
		"the refusal did not say where the input goes")

	conn.send("launch", map[string]any{"program": workflow, "stopOnEntry": false,
		"inputs": map[string]any{"release": "2026.9"}})
	launched := conn.await("response", "launch")
	require.Equal(t, true, launched["success"], "the launch carrying its input was refused: %v", launched["message"])

	conn.send("setFunctionBreakpoints", map[string]any{"breakpoints": []map[string]any{{"name": "done"}}})
	conn.await("response", "setFunctionBreakpoints")
	conn.send("configurationDone", nil)
	conn.await("response", "configurationDone")

	stopped := conn.await("event", "stopped")
	assert.Equal(t, "breakpoint", stopped["body"].(map[string]any)["reason"], "the first stop was not the breakpoint")

	conn.send("evaluate", map[string]any{"expression": "steps.tagged.value", "context": "repl"})
	evaluated := conn.await("response", "evaluate")
	require.Equal(t, true, evaluated["success"], "evaluate failed: %v", evaluated["message"])
	assert.Contains(t, evaluated["body"].(map[string]any)["result"], "v2026.9", "the run did not see its input")

	conn.send("continue", map[string]any{"threadId": 1})
	conn.await("response", "continue")
	conn.await("event", "terminated")
	assert.NotContains(t, conn.seen.String(), "break at tagged",
		"the console narrated an entry stop the editor was never sent")
}

// TestFlowDAPStepsBackThroughARealWorkflow: a launch that asks for `reverse`
// offers stepBack and reverseContinue over the real binary, each lands on a
// stop the editor was shown before, the run still finishes from there, and a
// launch that does not ask never offers them.
func TestFlowDAPStepsBackThroughARealWorkflow(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	workflow := filepath.Join(dir, "workflow.yaml")
	require.NoError(t, os.WriteFile(workflow, []byte(`
edition: v2026.4
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

	start := func(t *testing.T, env ...string) *dapConn {
		t.Helper()

		cmd := flowBinaryCommand(buildFlowBinary(t), "dap")
		cmd.Env = append(cmd.Environ(), env...)
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
		assert.Equal(t, false, initialize["body"].(map[string]any)["supportsStepBack"],
			"stepping back is offered only once a launch has asked for it")
		conn.await("event", "initialized")

		return conn
	}
	frame := func(conn *dapConn) string {
		conn.send("stackTrace", map[string]any{"threadId": 1})
		frames := conn.await("response", "stackTrace")["body"].(map[string]any)["stackFrames"].([]any)
		require.NotEmpty(t, frames)

		return frames[0].(map[string]any)["name"].(string)
	}

	t.Run("reverse", func(t *testing.T) {
		t.Parallel()

		conn := start(t)
		conn.send("launch", map[string]any{"program": workflow, "reverse": true})
		conn.await("response", "launch")
		offered := conn.await("event", "capabilities")
		assert.Equal(t, true, offered["body"].(map[string]any)["capabilities"].(map[string]any)["supportsStepBack"])
		conn.send("configurationDone", nil)
		conn.await("response", "configurationDone")
		conn.await("event", "stopped")
		assert.Contains(t, frame(conn), "build")

		conn.send("stepIn", map[string]any{"threadId": 1})
		conn.await("response", "stepIn")
		conn.await("event", "stopped")
		conn.send("stepIn", map[string]any{"threadId": 1})
		conn.await("response", "stepIn")
		conn.await("event", "stopped")
		assert.Contains(t, frame(conn), "deploy")

		conn.send("stepBack", map[string]any{"threadId": 1})
		back := conn.await("response", "stepBack")
		require.Equal(t, true, back["success"], "%v", back["message"])
		conn.await("event", "stopped")
		assert.Contains(t, frame(conn), "test")

		conn.send("reverseContinue", map[string]any{"threadId": 1})
		reversed := conn.await("response", "reverseContinue")
		require.Equal(t, true, reversed["success"], "%v", reversed["message"])
		conn.await("event", "stopped")
		assert.Contains(t, frame(conn), "build")

		// The rewound run is the run: it carries on to its end.
		conn.send("continue", map[string]any{"threadId": 1})
		conn.await("response", "continue")
		conn.await("event", "terminated")
	})

	// Breakpoints set before the run starts are part of what a replay repeats,
	// so reverse-continue lands on the stop one decided rather than the first.
	t.Run("reverse continue to a breakpoint", func(t *testing.T) {
		t.Parallel()

		conn := start(t)
		conn.send("launch", map[string]any{"program": workflow, "reverse": true})
		conn.await("response", "launch")
		conn.send("setFunctionBreakpoints", map[string]any{"breakpoints": []map[string]any{{"name": "test"}}})
		conn.await("response", "setFunctionBreakpoints")
		conn.send("configurationDone", nil)
		conn.await("response", "configurationDone")
		conn.await("event", "stopped")
		conn.send("continue", map[string]any{"threadId": 1})
		conn.await("response", "continue")
		hit := conn.await("event", "stopped")
		require.Equal(t, "breakpoint", hit["body"].(map[string]any)["reason"])
		assert.Contains(t, frame(conn), "test")
		conn.send("stepIn", map[string]any{"threadId": 1})
		conn.await("response", "stepIn")
		conn.await("event", "stopped")
		assert.Contains(t, frame(conn), "deploy")

		conn.send("reverseContinue", map[string]any{"threadId": 1})
		reversed := conn.await("response", "reverseContinue")
		require.Equal(t, true, reversed["success"], "%v", reversed["message"])
		stopped := conn.await("event", "stopped")
		assert.Equal(t, "breakpoint", stopped["body"].(map[string]any)["reason"])
		assert.Contains(t, frame(conn), "test")

		// And once more: no breakpoint before that one, so the first stop.
		conn.send("reverseContinue", map[string]any{"threadId": 1})
		reversed = conn.await("response", "reverseContinue")
		require.Equal(t, true, reversed["success"], "%v", reversed["message"])
		conn.await("event", "stopped")
		assert.Contains(t, frame(conn), "build")
	})

	t.Run("reverse and running past the entry stop disagree", func(t *testing.T) {
		t.Parallel()

		conn := start(t)
		conn.send("launch", map[string]any{"program": workflow, "reverse": true, "stopOnEntry": false})
		refused := conn.await("response", "launch")
		assert.Equal(t, false, refused["success"])
		assert.Contains(t, refused["message"], "stopOnEntry")
	})

	t.Run("not asked for", func(t *testing.T) {
		t.Parallel()

		conn := start(t)
		conn.send("launch", map[string]any{"program": workflow})
		conn.await("response", "launch")
		conn.send("configurationDone", nil)
		conn.await("response", "configurationDone")
		conn.await("event", "stopped")
		conn.send("stepBack", map[string]any{"threadId": 1})
		refused := conn.await("response", "stepBack")
		assert.Equal(t, false, refused["success"])
		assert.Contains(t, refused["message"], `"reverse": true`)
	})

	// A program that does not repeat itself: its second request is refused. The
	// replay ends early, the adapter says so, and nothing it did is reported as
	// the session's own.
	t.Run("a replay that ends early", func(t *testing.T) {
		t.Parallel()

		var requests atomic.Int64
		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			if requests.Add(1) > 1 {
				w.WriteHeader(http.StatusBadRequest)
			}
		}))
		t.Cleanup(server.Close)
		flaky := filepath.Join(dir, "flaky.yaml")
		require.NoError(t, os.WriteFile(flaky, []byte(`
edition: v2026.4
name: flaky
steps:
  - id: ask
    http:
      url: `+server.URL+`
      expect: ${response.status_code == 200}
  - id: after
    value: "'done'"
  - id: last
    value: "'done'"
outputs: {}
`), 0o600))

		conn := start(t, v1.AllowLoopbackEgressEnv+"="+v1.AllowLoopbackEgressValue)
		conn.send("launch", map[string]any{"program": flaky, "reverse": true})
		conn.await("response", "launch")
		conn.send("configurationDone", nil)
		conn.await("response", "configurationDone")
		conn.await("event", "stopped")
		for range 2 {
			conn.send("stepIn", map[string]any{"threadId": 1})
			conn.await("response", "stepIn")
			conn.await("event", "stopped")
		}
		require.Contains(t, frame(conn), "last", "the first run did not get past its request")

		// Going back to "after" repeats the request, which is now refused.
		conn.send("stepBack", map[string]any{"threadId": 1})
		refused := conn.await("response", "stepBack")
		require.Equal(t, false, refused["success"], "%v", refused)
		assert.Contains(t, refused["message"], "diverged")

		// The session is where it was, and still moves.
		assert.Contains(t, frame(conn), "last")
		conn.send("continue", map[string]any{"threadId": 1})
		conn.await("response", "continue")
		conn.await("event", "terminated")
		exited := conn.await("event", "exited")
		assert.Equal(t, float64(0), exited["body"].(map[string]any)["exitCode"],
			"a replay's failure was reported as the run's")
	})

	t.Run("terminate", func(t *testing.T) {
		t.Parallel()

		conn := start(t)
		conn.send("launch", map[string]any{"program": workflow, "reverse": true})
		conn.await("response", "launch")
		conn.send("configurationDone", nil)
		conn.await("response", "configurationDone")
		conn.await("event", "stopped")
		conn.send("stepIn", map[string]any{"threadId": 1})
		conn.await("response", "stepIn")
		conn.await("event", "stopped")
		conn.send("stepBack", map[string]any{"threadId": 1})
		conn.await("response", "stepBack")
		conn.await("event", "stopped")

		// Ending a rewound run is reported like ending any other.
		conn.send("terminate", map[string]any{})
		conn.await("response", "terminate")
		conn.await("event", "terminated")
	})
}
