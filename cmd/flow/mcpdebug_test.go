package main

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/modelcontextprotocol/go-sdk/mcp"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/encoding/protojson"

	flowmcp "github.com/picatz/flowstate/cmd/flow/internal/mcp"
	v1 "github.com/picatz/flowstate/pkg/flowstate/v1"
	"github.com/picatz/flowstate/pkg/flowstate/v1/flowdebug"
)

// The debugger over MCP (#928 slice 3): the same session the CLI drives with a
// console, driven by a submitted script. These are the wiring and the
// refusals — the session's own verbs are tested in flowdebug, and the case
// semantics in flowtest.

// debugAnswer is the tool's document, as an agent decodes it.
type debugAnswer struct {
	Session []struct {
		Text string `json:"text"`
		Tone string `json:"tone"`
	} `json:"session"`
	Script []string        `json:"script"`
	Report json.RawMessage `json:"report"`
	Note   string          `json:"note"`
}

// transcript joins the session's text, which is what a claim about "the
// session said" is actually about.
func (a debugAnswer) transcript() string {
	var b strings.Builder
	for _, f := range a.Session {
		b.WriteString(f.Text)
	}

	return b.String()
}

// tonesOf collects the tones a fragment containing substr was emitted with.
func (a debugAnswer) tonesOf(substr string) []string {
	var tones []string
	for _, f := range a.Session {
		if strings.Contains(f.Text, substr) {
			tones = append(tones, f.Tone)
		}
	}

	return tones
}

const debugWorkflow = `edition: v2026.3
name: debugged
inputs:
  release:
    type: string
    required: true
steps:
  - id: build
    log:
      message: ${'building ' + inputs.release}
  - id: ship
    log:
      message: shipped
outputs: {}
`

func callDebug(t *testing.T, session *mcp.ClientSession, args map[string]any) (*mcp.CallToolResult, debugAnswer) {
	t.Helper()

	result, err := session.CallTool(t.Context(), &mcp.CallToolParams{
		Name:      flowmcp.DebugToolName,
		Arguments: args,
	})
	require.NoError(t, err)
	require.NotEmpty(t, result.Content)

	text := result.Content[0].(*mcp.TextContent).Text

	var answer debugAnswer
	if err := json.Unmarshal([]byte(text), &answer); err != nil {
		// A refusal before the session is bare text; the tests for those read it.
		return result, debugAnswer{}
	}

	return result, answer
}

// TestTheDebugToolStepsAndInspects is the whole loop an agent runs: hold the
// run, ask what a step produced, ask what an expression evaluates to, and get
// the verdict anyway.
func TestTheDebugToolStepsAndInspects(t *testing.T) {
	t.Parallel()

	session := connectMCP(t, defaultLocalRunPosture())

	result, answer := callDebug(t, session, map[string]any{
		"workflow": debugWorkflow,
		"tests": `tests:
  - name: it ships
    inputs:
      release: "2026.9.0"
    stubs:
      - task: log
        returns: {}
    expect:
      ran: [build, ship]
`,
		"commands": []any{"step", "scope", "inspect inputs.release", "info", "continue"},
	})
	require.False(t, result.IsError, "the case passes: %s", result.Content[0].(*mcp.TextContent).Text)

	joined := answer.transcript()
	assert.Contains(t, joined, `break at build (task "log")`, "the run is held before its first step")
	assert.Contains(t, joined, "steps: build", "`scope` lists what the paused run can name")
	assert.Contains(t, joined, `"2026.9.0"`, "`inspect` answers from the run's own scope")
	assert.Contains(t, joined, "break at ship", "`step` advanced to the next boundary")

	assert.Equal(t, []string{"step", "scope", "inspect inputs.release", "info", "continue"}, answer.Script,
		"the script is what a caller re-sends to go further")
	assert.Contains(t, answer.tonesOf("break at build"), "break",
		"a stop carries the tone a terminal would colour it")

	// The verdict is the ordinary one, and this tool did not change it.
	var report struct {
		Cases []struct {
			Name   string `json:"name"`
			Passed bool   `json:"passed"`
		} `json:"cases"`
	}
	require.NoError(t, json.Unmarshal(answer.Report, &report))
	require.Len(t, report.Cases, 1)
	assert.True(t, report.Cases[0].Passed)
	assert.Equal(t, "it ships", report.Cases[0].Name)
}

// TestTheDebugToolReachesTheAutopsy: a failing case is held open once more
// after the verdict, which is where an agent asks *why* — the reason this
// tool exists beside flowstate_test rather than instead of it.
func TestTheDebugToolReachesTheAutopsy(t *testing.T) {
	t.Parallel()

	session := connectMCP(t, defaultLocalRunPosture())

	result, answer := callDebug(t, session, map[string]any{
		"workflow": debugWorkflow,
		"tests": `tests:
  - name: claims the wrong thing
    inputs:
      release: "2026.9.0"
    stubs:
      - task: log
        returns: {}
    expect:
      ran: [build, ship]
      check:
        - 1 == 2
`,
		"commands": []any{"continue", "inspect 'the run itself was fine'", "quit"},
	})
	require.True(t, result.IsError, "a failing case must be flagged, debugger or not")

	joined := answer.transcript()
	assert.Contains(t, joined, "autopsy: the case failed 1 expectation(s)")
	assert.Contains(t, joined, "check failed: 1 == 2")
	assert.Contains(t, joined, "the run itself was fine", "the autopsy answers inspections")
	assert.Contains(t, answer.tonesOf("check failed"), "danger", "a failure arrives as one")
}

// TestTheDebugToolRefusesAnAmbiguousCase: a session drives one run, so a
// document with two cases and no `case` is a refusal that names them — never
// a session that silently debugs the first.
func TestTheDebugToolRefusesAnAmbiguousCase(t *testing.T) {
	t.Parallel()

	session := connectMCP(t, defaultLocalRunPosture())

	tests := `tests:
  - name: first
    inputs:
      release: a
    stubs:
      - task: log
        returns: {}
    expect:
      ran: [build, ship]
  - name: second
    inputs:
      release: b
    stubs:
      - task: log
        returns: {}
    expect:
      ran: [build, ship]
`

	result, _ := callDebug(t, session, map[string]any{
		"workflow": debugWorkflow,
		"tests":    tests,
		"commands": []any{"continue"},
	})
	require.True(t, result.IsError)
	text := result.Content[0].(*mcp.TextContent).Text
	assert.Contains(t, text, "declares 2 cases")
	assert.Contains(t, text, `"first", "second"`)
	assert.Contains(t, text, "Name one with `case`")

	// And with the name, it drives exactly that one.
	named, answer := callDebug(t, session, map[string]any{
		"workflow": debugWorkflow,
		"tests":    tests,
		"case":     "second",
		"commands": []any{"inspect inputs.release", "continue"},
	})
	require.False(t, named.IsError, named.Content[0].(*mcp.TextContent).Text)
	assert.Contains(t, answer.transcript(), `"b"`, "the named case is the one that ran")

	var report struct {
		Cases []struct {
			Name string `json:"name"`
		} `json:"cases"`
	}
	require.NoError(t, json.Unmarshal(answer.Report, &report))
	require.Len(t, report.Cases, 1, "the other case was filtered out, not run")
	assert.Equal(t, "second", report.Cases[0].Name)
}

// TestTheDebugToolRefusesAnUnknownCase names what the document does declare,
// the diagnostics standard this repo holds itself to.
func TestTheDebugToolRefusesAnUnknownCase(t *testing.T) {
	t.Parallel()

	session := connectMCP(t, defaultLocalRunPosture())

	result, _ := callDebug(t, session, map[string]any{
		"workflow": debugWorkflow,
		"tests": `tests:
  - name: the only one
    inputs:
      release: a
    stubs:
      - task: log
        returns: {}
    expect:
      ran: [build, ship]
`,
		"case":     "a case that is not there",
		"commands": []any{"continue"},
	})
	require.True(t, result.IsError)
	text := result.Content[0].(*mcp.TextContent).Text
	assert.Contains(t, text, `no case is named "a case that is not there"`)
	assert.Contains(t, text, `"the only one"`)
}

// TestAnExhaustedScriptFinishesTheRun is what makes a scripted session safe on
// a surface with no console: a script that runs out is not a held run.
func TestAnExhaustedScriptFinishesTheRun(t *testing.T) {
	t.Parallel()

	session := connectMCP(t, defaultLocalRunPosture())

	result, answer := callDebug(t, session, map[string]any{
		"workflow": debugWorkflow,
		"tests": `tests:
  - name: it ships
    inputs:
      release: "2026.9.0"
    stubs:
      - task: log
        returns: {}
    expect:
      ran: [build, ship]
`,
		// One command, two steps: the script is exhausted at the first
		// boundary and the run must finish on its own.
		"commands": []any{"step"},
	})
	require.False(t, result.IsError, result.Content[0].(*mcp.TextContent).Text)
	assert.Contains(t, answer.transcript(), "no more commands",
		"the session must say it continued unattended")

	var report struct {
		Cases []struct {
			Passed bool `json:"passed"`
		} `json:"cases"`
	}
	require.NoError(t, json.Unmarshal(answer.Report, &report))
	require.Len(t, report.Cases, 1)
	assert.True(t, report.Cases[0].Passed, "the run finished and its expectations held")
}

// TestTheDebugToolRefusesAnUnboundedScript: the argument an untrusted caller
// submits is bounded, and the refusal names the number rather than truncating
// a script into a session answering questions nobody asked.
func TestTheDebugToolRefusesAnUnboundedScript(t *testing.T) {
	t.Parallel()

	session := connectMCP(t, defaultLocalRunPosture())

	commands := make([]any, maxDebugCommands+1)
	for i := range commands {
		commands[i] = "scope"
	}

	result, _ := callDebug(t, session, map[string]any{
		"workflow": debugWorkflow,
		"tests": `tests:
  - name: it ships
    inputs:
      release: a
    stubs:
      - task: log
        returns: {}
    expect:
      ran: [build, ship]
`,
		"commands": commands,
	})
	require.True(t, result.IsError)
	assert.Contains(t, result.Content[0].(*mcp.TextContent).Text,
		fmt.Sprintf("at most %d commands", maxDebugCommands))
}

// TestTheDebugToolRefusesAMultiLineCommand: each entry is one command, so a
// smuggled newline is a refusal rather than two commands the caller did not
// account for.
func TestTheDebugToolRefusesAMultiLineCommand(t *testing.T) {
	t.Parallel()

	session := connectMCP(t, defaultLocalRunPosture())

	result, _ := callDebug(t, session, map[string]any{
		"workflow": debugWorkflow,
		"tests": `tests:
  - name: it ships
    inputs:
      release: a
    stubs:
      - task: log
        returns: {}
    expect:
      ran: [build, ship]
`,
		"commands": []any{"step\nquit"},
	})
	require.True(t, result.IsError)
	assert.Contains(t, result.Content[0].(*mcp.TextContent).Text, "contains a line break")
}

// TestTheDebugToolRefusesAnEmptyScript points at the tool that already runs a
// case unattended, rather than quietly being a second spelling of it.
func TestTheDebugToolRefusesAnEmptyScript(t *testing.T) {
	t.Parallel()

	session := connectMCP(t, defaultLocalRunPosture())

	result, _ := callDebug(t, session, map[string]any{
		"workflow": debugWorkflow,
		"tests": `tests:
  - name: it ships
    expect:
      failed: false
`,
		"commands": []any{},
	})
	require.True(t, result.IsError)
	assert.Contains(t, result.Content[0].(*mcp.TextContent).Text, flowmcp.TestToolName)
}

// TestTheDebugToolRefusesUnknownArguments: the schema says
// additionalProperties:false and the handler has to mean it.
func TestTheDebugToolRefusesUnknownArguments(t *testing.T) {
	t.Parallel()

	session := connectMCP(t, defaultLocalRunPosture())

	result, _ := callDebug(t, session, map[string]any{
		"workflow": debugWorkflow,
		"tests": `tests:
  - name: it ships
    expect:
      failed: false
`,
		"commands":  []any{"continue"},
		"breakpont": "build",
	})
	require.True(t, result.IsError)
	assert.Contains(t, result.Content[0].(*mcp.TextContent).Text, "breakpont")
}

// TestTheDebugToolCannotResolveASecret is the property inherited rather than
// re-implemented: an inspection goes through the run's own activation, which
// refuses a secret reference wherever it appears.
func TestTheDebugToolCannotResolveASecret(t *testing.T) {
	t.Parallel()

	session := connectMCP(t, defaultLocalRunPosture())

	_, answer := callDebug(t, session, map[string]any{
		"workflow": debugWorkflow,
		"tests": `tests:
  - name: it ships
    inputs:
      release: a
    secrets:
      "env:TOKEN": hunter2-swordfish
    stubs:
      - task: log
        returns: {}
    expect:
      ran: [build, ship]
`,
		"commands": []any{"inspect secret('env:TOKEN')", "continue"},
	})

	joined := answer.transcript()

	// Both halves, because the absence alone would pass on an empty
	// transcript — the vacuity this repository keeps catching. The session
	// really ran, the inspection really was attempted, and what came back is
	// a refusal rather than a value.
	require.Contains(t, joined, `break at build`, "the session did not run at all")
	assert.Contains(t, joined, "evaluate expression",
		"the inspection must be refused, not silently answered")
	assert.NotContains(t, joined, "hunter2-swordfish",
		"an inspection resolved a secret, which no expression in a run may do")
}

// TestTheDebugAnswerIsBoundedByItsRenderedSize is the arithmetic the first cut
// got wrong (Codex, #1109): every part of this document is bounded on its own
// — the transcript where it is collected, the report by its own ladder, the
// script at the door — and the encoded total is still not any of those. Only
// the encoded length is the length.
//
// Against the renderer rather than through a call, deliberately. Reaching this
// collision through a submitted workflow means fighting three *other* bounds
// that fire first (a Flowfile's own byte limit, CEL's expression size, the
// transcript's collection cap), and a test that spends its effort on the
// fixture ends up proving whichever bound it tripped over. The subject here is
// the assembly, so the assembly is what it calls.
func TestTheDebugAnswerIsBoundedByItsRenderedSize(t *testing.T) {
	t.Parallel()

	// A transcript right at its own bound, a report right at its own, and a
	// script the caller is entitled to have echoed: each legal, and together
	// over the cap.
	transcript := &debugTranscript{}
	for transcript.bytes < maxDebugTranscriptBytes-1024 {
		transcript.add(strings.Repeat("x", 512)+"\n", flowdebug.ToneInfo)
	}
	require.Zero(t, transcript.dropped, "the transcript must be within its own bound, not over it")

	report := &v1.TestReport{File: "<submitted>", Cases: []*v1.TestCase{{
		Name: "it runs",
		Failures: []*v1.Diagnostic{{
			Field:   "expect.outputs",
			Message: strings.Repeat("y", 160<<10),
		}},
	}}}

	script := []string{"step", "inspect steps.build.ok", "continue"}

	encoded, err := renderDebugResult(report, transcript, script, false)
	require.NoError(t, err)

	assert.LessOrEqual(t, len(encoded), flowmcp.MaxResultBytes,
		"the rendered answer escaped the cap: %d bytes", len(encoded))

	var answer debugAnswer
	require.NoError(t, json.Unmarshal(encoded, &answer))

	// The ladder ran, and gave up the cheapest thing first: the script is what
	// the caller sent, so it is the one part they already have.
	assert.Nil(t, answer.Script, "the answer fit without the ladder, so this proves nothing about it")
	assert.Contains(t, answer.Note, "exceeded",
		"an answer the ladder reduced must say the cap is why")

	// Bounded, and still an answer: a document that fits by carrying nothing
	// would satisfy the assertion above and be useless.
	require.NotEmpty(t, answer.Report, "the verdict is the floor and must survive")
	assert.NotEmpty(t, answer.Session, "the transcript is what a debug call is for")
}

// TestTheDebugAnswerKeepsTheVerdictWhenNothingElseFits is the floor: a
// transcript that cannot be reduced enough gives way entirely, and what
// survives is the one thing this call shares with the flowstate_test call the
// caller could have made instead.
func TestTheDebugAnswerKeepsTheVerdictWhenNothingElseFits(t *testing.T) {
	t.Parallel()

	transcript := &debugTranscript{}
	for transcript.bytes < maxDebugTranscriptBytes-1024 {
		transcript.add(strings.Repeat("x", 512)+"\n", flowdebug.ToneInfo)
	}

	// A report that survives its own ladder at nearly the whole allowance —
	// under the cap, so renderTestResult returns it untouched — leaves room
	// for nothing else.
	report := &v1.TestReport{File: "<submitted>", Cases: []*v1.TestCase{{
		Name: "it runs",
		Failures: []*v1.Diagnostic{{
			Field:   "expect.outputs",
			Message: strings.Repeat("y", 240<<10),
		}},
	}}}

	encoded, err := renderDebugResult(report, transcript, []string{"continue"}, false)
	require.NoError(t, err)

	var answer debugAnswer
	require.NoError(t, json.Unmarshal(encoded, &answer))

	assert.Empty(t, answer.Session, "the transcript is the last thing to go, and it went")
	require.NotEmpty(t, answer.Report, "the verdict never goes")
	assert.Contains(t, answer.Note, "transcript was dropped entirely")

	// Every rung's note, not just the settled rung's (Codex, #1109): the rungs
	// are cumulative, so a document that lost the script *and* the transcript
	// has to say both — otherwise `script: null` arrives with no explanation
	// for it anywhere.
	assert.Contains(t, answer.Note, "accepted script was dropped",
		"the script went too, and the note is where a caller learns why")
}

// TestTheDebugToolRefusesAnOversizedScript bounds the argument itself, which
// the per-command bound does not: a hundred commands each just under
// MaxCommandBytes is megabytes of input the answer would echo back.
func TestTheDebugToolRefusesAnOversizedScript(t *testing.T) {
	t.Parallel()

	session := connectMCP(t, defaultLocalRunPosture())

	// Ten commands, each well under the per-command bound, together over the
	// script bound — so this can only be refused by the total.
	commands := make([]any, 10)
	for i := range commands {
		commands[i] = "inspect '" + strings.Repeat("y", maxDebugScriptBytes/8) + "'"
	}

	result, _ := callDebug(t, session, map[string]any{
		"workflow": debugWorkflow,
		"tests": `tests:
  - name: it ships
    expect:
      failed: false
`,
		"commands": commands,
	})
	require.True(t, result.IsError)
	assert.Contains(t, result.Content[0].(*mcp.TextContent).Text,
		fmt.Sprintf("at most %d across all of its commands", maxDebugScriptBytes))
}

// TestTheDebugToolWithholdsASensitiveInput (Codex, #1109): a debugger is a
// reveal — it narrates each step's values and `inspect` reaches whatever is in
// scope — so a value the transcript and the check witnesses withhold must not
// come back through the session instead. Both paths, since the leak was on
// both: an inspection asking for it directly, and the step account that would
// carry it incidentally.
func TestTheDebugToolWithholdsASensitiveInput(t *testing.T) {
	t.Parallel()

	session := connectMCP(t, defaultLocalRunPosture())

	_, answer := callDebug(t, session, map[string]any{
		"workflow": `edition: v2026.3
name: secretive
inputs:
  token:
    type: string
    required: true
    sensitive: true
steps:
  - id: echo
    value: ${inputs.token}
outputs: {}
`,
		"tests": `tests:
  - name: it runs
    inputs:
      token: hunter2-swordfish
    expect:
      ran: [echo]
`,
		"commands": []any{"inspect inputs.token", "continue"},
	})

	joined := answer.transcript()

	// Both halves: the session really ran and really answered, and what it
	// answered is not the value. The absence alone would pass on an empty
	// transcript.
	require.Contains(t, joined, "break at echo", "the session did not run at all")
	assert.NotContains(t, joined, "hunter2-swordfish",
		"a declared-sensitive input reached the answer through the debug session")
	assert.Contains(t, joined, "[redacted]",
		"the value should render as the marker the transcript already uses")
}

// TestTheDebugToolHonoursRequestCancellation (Codex, #1109): on the stdio
// surface no timeout is configured, and the first cut rooted the run at
// context.Background() — so a client that cancelled a call could not stop it.
// A `continue` into a wait with no timeout and no scripted signal is a legal
// Flowfile that never completes, and the run would have outlived the request
// that asked for it.
//
// Against the handler rather than through a client session, because the claim
// is about the context the handler runs the case under, and cancelling a live
// MCP request from the client side is the SDK's plumbing rather than this
// tool's behaviour.
func TestTheDebugToolHonoursRequestCancellation(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithCancel(t.Context())

	done := make(chan *mcp.CallToolResult, 1)
	go func() {
		result, err := debugToolHandler(0)(ctx, &mcp.CallToolRequest{
			Params: &mcp.CallToolParamsRaw{
				Arguments: json.RawMessage(`{
					"workflow": "edition: v2026.3\nname: parked\nsteps:\n- id: gate\n  wait_for_signal:\n    name: approve\n",
					"tests": "tests:\n  - name: it waits\n    expect:\n      failed: false\n",
					"commands": ["continue"]
				}`),
			},
		})
		require.NoError(t, err)
		done <- result
	}()

	// The run parks: nothing signals the gate and the virtual clock has no
	// deadline to advance to.
	select {
	case <-done:
		t.Fatal("the case completed, so this proves nothing about cancellation — the fixture must park")
	case <-time.After(250 * time.Millisecond):
	}

	cancel()

	select {
	case result := <-done:
		require.NotNil(t, result)
	case <-time.After(10 * time.Second):
		t.Fatal("cancelling the request did not stop the run: it is rooted at a context the caller cannot reach")
	}
}

// TestTheDebugToolBoundsTheCaseArgument (Codex, #1109): an unknown-case
// refusal quotes what was asked for, which is right — and that puts a
// caller-controlled string into an error returned outside the answer ladder.
// Both doors: the argument itself, and the file's own case names echoed back
// beside it.
func TestTheDebugToolBoundsTheCaseArgument(t *testing.T) {
	t.Parallel()

	session := connectMCP(t, defaultLocalRunPosture())

	tests := `tests:
  - name: it ships
    expect:
      failed: false
`

	t.Run("the argument", func(t *testing.T) {
		t.Parallel()

		result, _ := callDebug(t, session, map[string]any{
			"workflow": debugWorkflow,
			"tests":    tests,
			"case":     strings.Repeat("n", 300<<10),
			"commands": []any{"continue"},
		})
		require.True(t, result.IsError)

		text := result.Content[0].(*mcp.TextContent).Text
		assert.LessOrEqual(t, len(text), flowmcp.MaxResultBytes,
			"the refusal echoed the argument and became an oversized answer: %d bytes", len(text))
		assert.Contains(t, text, fmt.Sprintf("at most %d", maxDebugCaseBytes))
	})

	t.Run("the file's own names", func(t *testing.T) {
		t.Parallel()

		// A case whose *name* is enormous, so the refusal listing what the
		// document declares is the door instead.
		result, _ := callDebug(t, session, map[string]any{
			"workflow": debugWorkflow,
			"tests":    "tests:\n  - name: \"" + strings.Repeat("m", 300<<10) + "\"\n    expect:\n      failed: false\n",
			"case":     "not-there",
			"commands": []any{"continue"},
		})
		require.True(t, result.IsError)

		text := result.Content[0].(*mcp.TextContent).Text
		assert.LessOrEqual(t, len(text), flowmcp.MaxResultBytes,
			"the refusal echoed a declared name whole: %d bytes", len(text))
	})
}

// TestQuitCannotSatisfyAnExpectedFailure (Codex, #1109): `quit` ends the run
// wherever it stands, and an abandoned run returns an error like any other —
// so a case declaring `expect.failed: true` was *satisfied* by the debugger's
// own error and passed without ever reaching the failure it named. That is a
// debugger turning a red case green, which is the one thing it must never do.
func TestQuitCannotSatisfyAnExpectedFailure(t *testing.T) {
	t.Parallel()

	session := connectMCP(t, defaultLocalRunPosture())

	result, answer := callDebug(t, session, map[string]any{
		"workflow": `edition: v2026.3
name: eventually-fails
steps:
  - id: first
    log:
      message: one
  - id: boom
    value: ${1 / 0}
outputs: {}
`,
		"tests": `tests:
  - name: it fails at boom
    stubs:
      - task: log
        returns: {}
    expect:
      failed: true
`,
		// Abandoned at the very first boundary, long before `boom`.
		"commands": []any{"quit"},
	})

	require.True(t, result.IsError,
		"the case passed on a run the debugger abandoned before it reached the failure it expected")

	var report struct {
		Cases []struct {
			Passed bool   `json:"passed"`
			Error  string `json:"error"`
		} `json:"cases"`
	}
	require.NoError(t, json.Unmarshal(answer.Report, &report))
	require.Len(t, report.Cases, 1)
	assert.False(t, report.Cases[0].Passed)
	assert.Contains(t, report.Cases[0].Error, "ended this run before it finished",
		"the case must say it has no verdict, rather than reporting one it never reached")
}

// reportRenderingJustUnderTheCap builds a v1.TestReport whose own rendering
// settles just under the whole surface cap, leaving nothing for a document to
// carry it in.
//
// Every message stays under maxTestFailureMessageBytes, so the report ladder's
// message-capping rung is a no-op and its floor — per-case verdicts with the
// diagnostics dropped — is the only thing that can make this smaller. That is
// the shape the wrapper's budget has to survive: a report the debug ladder
// cannot shrink by any rung of its own.
func reportRenderingJustUnderTheCap(t *testing.T) *v1.TestReport {
	t.Helper()

	// Few failures, each heavy, and the weight in `value` rather than in
	// `message`. Both halves are load-bearing.
	//
	// `message` is what the report ladder's second rung caps, so weight there
	// is weight that rung takes back; `value` — the literal at fault, which a
	// compared value's size decides — is not capped by any rung, so the report
	// arrives at rung 0 or at its floor and nothing in between.
	//
	// Few, because protojson writes every field of a Diagnostic under
	// EmitUnpopulated and puts a space after some separators — and *whether*
	// it does is decided per process. Sixty-odd failures is a thousand such
	// separators, which is more slack than the wrapper this test is about
	// weighs, so a fixture built out of small failures cannot sit tight
	// against the cap in both regimes. Eight can.
	const chunk = 32 << 10

	// No `<`, `>` or `&` anywhere: json.Marshal escapes each to six bytes
	// inside a RawMessage, which would put the measurements below out by five
	// apiece.
	report := &v1.TestReport{File: "submitted", Cases: []*v1.TestCase{{Name: "it runs"}}}
	fill := func(size int) *v1.Diagnostic {
		one := &v1.Diagnostic{Field: "expect.outputs", Value: strings.Repeat("y", size)}
		holder := report.GetCases()[0]
		holder.Failures = append(holder.Failures, one)

		return one
	}

	// The *unreduced* encoding, deliberately, and this is the trap the first
	// cut of this fixture fell into: renderTestResult never answers over the
	// cap — that is its whole job — so a search asking it whether the report
	// fits is satisfied by the collapsed floor and grows the report forever.
	// Rung 0 is what is being sized here, and it has to keep fitting, or the
	// report arrives already reduced and the debug floor has room to spare.
	measure := func() int {
		encoded, err := v1.MarshalSchemaJSON(report, false)
		require.NoError(t, err)

		return len(encoded)
	}

	// What the report costs *inside* the answer, which is not the same number:
	// json.Marshal compacts a RawMessage, so protojson's spacing is spent
	// before the bytes land. This is the one the wrapper competes with.
	embedded := func() int {
		encoded, err := v1.MarshalSchemaJSON(report, false)
		require.NoError(t, err)

		var compact bytes.Buffer
		require.NoError(t, json.Compact(&compact, encoded))

		return compact.Len()
	}

	const margin = 512

	for measure()+chunk+margin < flowmcp.MaxResultBytes {
		fill(chunk)
	}

	// Searched rather than computed, because a failure's rendered cost is not
	// its value's length: EmitUnpopulated writes every field of the message.
	// The ceiling matches the loop's margin above — the gap it leaves is at
	// most chunk+margin, and a ceiling under that cannot close it.
	last := fill(0)
	low, high := 0, chunk+margin
	for low < high {
		middle := (low + high + 1) / 2
		last.Value = strings.Repeat("y", middle)
		if measure() <= flowmcp.MaxResultBytes {
			low = middle
		} else {
			high = middle - 1
		}
	}
	last.Value = strings.Repeat("y", low)

	rendered, err := renderTestResult(report)
	require.NoError(t, err)
	require.Equal(t, measure(), len(rendered),
		"no rung of the report's own ladder may have fired, or the debug floor inherits "+
			"a report that already had room to spare")

	require.Greater(t, embedded(), flowmcp.MaxResultBytes-margin,
		"the fixture must leave less than the wrapper weighs, or it tests nothing")

	return report
}

// TestTheDebugAnswerReservesRoomForItsOwnWrapper is the arithmetic every rung
// above the floor cannot fix (Codex, #1109).
//
// A report that renders just under the cap survives every debug rung untouched
// — dropping the script and the transcript does not shrink a report — and the
// floor then adds the object, its keys and the notes saying what left. The cap
// is a promise about the whole answer, so those bytes have to come out of the
// report's budget rather than being spent twice.
func TestTheDebugAnswerReservesRoomForItsOwnWrapper(t *testing.T) {
	t.Parallel()

	report := reportRenderingJustUnderTheCap(t)

	transcript := &debugTranscript{}
	for transcript.bytes < maxDebugTranscriptBytes-1024 {
		transcript.add(strings.Repeat("x", 512)+"\n", flowdebug.ToneInfo)
	}

	encoded, err := renderDebugResult(report, transcript, []string{"continue"}, false)
	require.NoError(t, err)

	assert.LessOrEqual(t, len(encoded), flowmcp.MaxResultBytes,
		"the answer carrying the report exceeded the cap the report alone respected")

	// And it is still an answer: a floor that fits by being undecodable, or by
	// dropping the verdict, would satisfy the line above and be worthless.
	var answer debugAnswer
	require.NoError(t, json.Unmarshal(encoded, &answer))
	require.NotEmpty(t, answer.Report, "the verdict never goes")

	var carried v1.TestReport
	require.NoError(t, protojson.Unmarshal(answer.Report, &carried))
	require.Len(t, carried.GetCases(), 1)
	assert.Equal(t, "it runs", carried.GetCases()[0].GetName(),
		"the case's verdict is what a caller came for")
}

// TestTheDebugToolRefusesADuplicateCaseName: a document may declare two cases
// with one name, and `case` names a name rather than a case — so the predicate
// selects both, one command stream drives two runs, and the transcript names
// the same case twice with no way to tell which said what (Codex, #1109).
func TestTheDebugToolRefusesADuplicateCaseName(t *testing.T) {
	t.Parallel()

	session := connectMCP(t, defaultLocalRunPosture())

	result, _ := callDebug(t, session, map[string]any{
		"workflow": debugWorkflow,
		"tests": `tests:
  - name: it ships
    expect:
      failed: false
  - name: it ships
    expect:
      failed: false
`,
		"case":     "it ships",
		"commands": []any{"continue"},
	})

	require.True(t, result.IsError, "two cases under one name cannot be one session")
	text := result.Content[0].(*mcp.TextContent).Text
	assert.Contains(t, text, `2 of this document's cases are named "it ships"`)
	assert.Contains(t, text, "Give them distinct names")
}

// TestTheDebugToolDrivesABranchingWorkflow: `parallel:` is where a debugged
// run has more than one thread of written order, and every branch's account
// has to reach one transcript.
//
// It is a wiring test and says nothing about concurrency: the local driver
// runs branches sequentially (pkg/flowstate/v1/eval.go), so nothing here
// produces the concurrent callbacks [v1.RunObserver] permits. The session's
// own claim to survive those is tested where it is made, in flowdebug's
// TestConcurrentCallbacksAreSerialized.
func TestTheDebugToolDrivesABranchingWorkflow(t *testing.T) {
	t.Parallel()

	session := connectMCP(t, defaultLocalRunPosture())

	result, answer := callDebug(t, session, map[string]any{
		"workflow": `edition: v2026.3
name: branching
steps:
  - id: fan
    parallel:
      - steps:
          - id: left
            log:
              message: left
      - steps:
          - id: right
            log:
              message: right
      - steps:
          - id: middle
            log:
              message: middle
outputs: {}
`,
		"tests": `tests:
  - name: all three branches run
    stubs:
      - task: log
        returns: {}
    expect:
      ran: [left, right, middle]
`,
		"commands": []any{"continue"},
	})
	require.False(t, result.IsError, result.Content[0].(*mcp.TextContent).Text)

	joined := answer.transcript()
	for _, branch := range []string{"left", "right", "middle"} {
		assert.Contains(t, joined, "  "+branch+" ",
			"every branch's account has to reach the transcript")
	}
}

// TestTheDebugAnswerBoundsARefusedDocument (Codex, #1109): a `tests` document
// the loader will not read at all produces no cases and one `refused` string —
// and that string quotes what it refused, out of a document that may be a
// megabyte.
//
// Nothing in the debug ladder shrinks a report, and the report ladder carried
// `refused` whole on every rung including its floor, so the answer's size was
// the submitted document's to choose. That is the attacker-controlled resource
// escaping its bound, which is the one thing these ladders exist to prevent.
func TestTheDebugAnswerBoundsARefusedDocument(t *testing.T) {
	t.Parallel()

	// A refusal the size of the document that caused it.
	report := &v1.TestReport{
		File:    "submitted",
		Refused: strings.Repeat("y", 900<<10),
	}

	transcript := &debugTranscript{}
	transcript.add("break at first\n", flowdebug.ToneBreak)

	encoded, err := renderDebugResult(report, transcript, []string{"continue"}, false)
	require.NoError(t, err)

	assert.LessOrEqual(t, len(encoded), flowmcp.MaxResultBytes,
		"a refused document chose the size of the answer refusing it")

	var answer debugAnswer
	require.NoError(t, json.Unmarshal(encoded, &answer))

	var carried v1.TestReport
	require.NoError(t, protojson.Unmarshal(answer.Report, &carried))
	assert.Contains(t, carried.GetRefused(), "truncated, exceeded",
		"the refusal must say it was cut, not just arrive short")
	assert.NotEmpty(t, carried.GetRefused(), "and a refusal with nothing in it is not a diagnostic")
}

// TestTheDebugAnswerBoundsFiveHundredCaseNames is the same escape by the other
// door: the report floor keeps every case's name, and how many names there are
// is the document's choice — flowtest.MaxTestsPerFile is 500 and a case name
// has no length of its own, so a megabyte of names is an ordinary submitted
// document (Codex, #1109, generalising the refused-report finding).
func TestTheDebugAnswerBoundsFiveHundredCaseNames(t *testing.T) {
	t.Parallel()

	report := &v1.TestReport{File: "submitted"}
	for i := range 500 {
		report.Cases = append(report.Cases, &v1.TestCase{
			Name:   fmt.Sprintf("case-%03d-%s", i, strings.Repeat("y", 2<<10)),
			Passed: false,
			Error:  "it did not pass",
		})
	}

	transcript := &debugTranscript{}
	transcript.add("break at first\n", flowdebug.ToneBreak)

	encoded, err := renderDebugResult(report, transcript, []string{"continue"}, false)
	require.NoError(t, err)

	assert.LessOrEqual(t, len(encoded), flowmcp.MaxResultBytes,
		"five hundred long case names carried the answer past the cap")

	var answer debugAnswer
	require.NoError(t, json.Unmarshal(encoded, &answer))

	var carried v1.TestReport
	require.NoError(t, protojson.Unmarshal(answer.Report, &carried))
	require.Len(t, carried.GetCases(), 500, "every verdict survives; only the names are cut")
	assert.Contains(t, carried.GetCases()[0].GetName(), "case-000",
		"and each name keeps the part that identifies it")
}

// TestTheReportFloorBoundsFiveHundredNamesAndErrors is the case the first
// budget missed (Codex, #1109): the floor emits *two* strings per case, and
// the share was divided as though it emitted one, so a document with long
// names and long errors alike got twice its allotment. `capText` also appended
// its truncation sentence past the share it was given, which across a thousand
// strings is another forty kilobytes nothing accounted for.
//
// Asked of renderTestResultWithin rather than of the debug answer, and that
// distinction is the whole reason this test exists as written: the debug floor
// converges, so it *rescues* an over-budget report by handing the ladder a
// smaller limit and asking again — a version of this driven through
// renderDebugResult passes against the bug. `flowstate_test` has no such loop.
// Its floor is the answer, and the answer went out oversized.
func TestTheReportFloorBoundsFiveHundredNamesAndErrors(t *testing.T) {
	t.Parallel()

	report := &v1.TestReport{File: "submitted"}
	for i := range 500 {
		report.Cases = append(report.Cases, &v1.TestCase{
			Name:   fmt.Sprintf("case-%03d-%s", i, strings.Repeat("y", 1<<10)),
			Passed: false,
			Error:  fmt.Sprintf("error-%03d-%s", i, strings.Repeat("z", 1<<10)),
		})
	}

	encoded, err := renderTestResultWithin(report, flowmcp.MaxResultBytes)
	require.NoError(t, err)

	assert.LessOrEqual(t, len(encoded), flowmcp.MaxResultBytes,
		"both strings of five hundred cases carried the report past the cap")

	var carried v1.TestReport
	require.NoError(t, protojson.Unmarshal(encoded, &carried))
	require.Len(t, carried.GetCases(), 500, "every verdict survives; the strings are what gave way")
	assert.Contains(t, carried.GetCases()[0].GetName(), "case-000",
		"and each keeps the part that identifies it")
	assert.Contains(t, carried.GetCases()[0].GetError(), "error-000")
}

// TestCapTextKeepsItsOwnPromise: a caller that divides a budget between
// strings and caps each at its share is relying on this returning at most that
// many bytes. It was returning the share plus a sentence.
func TestCapTextKeepsItsOwnPromise(t *testing.T) {
	t.Parallel()

	for _, max := range []int{16, 64, 128, 4096} {
		got := capText(strings.Repeat("y", 100<<10), max)
		assert.LessOrEqual(t, len(got), max, "capText(_, %d) overran its own bound", max)
	}

	assert.Equal(t, "short", capText("short", 64), "what fits is returned untouched")
}

// TestTheDebugToolCompletes is the parity claim, end to end.
//
// The completion built for the prompt was reachable only through a terminal's
// tab key, because `SetCompleter` is installed on a console and a console
// exists only where both streams are terminals. So the surface agents drive
// this project through could not ask what may be written at a paused run,
// though the answer is a pure function of a scope the session already holds.
//
// That is the same shape as every other gap this session's review kept finding
// — a capability complete on one front and unreachable from another — and the
// fix is a command rather than a second mechanism, so the two fronts share one
// answer and one renderer.
func TestTheDebugToolCompletes(t *testing.T) {
	t.Parallel()

	session := connectMCP(t, defaultLocalRunPosture())

	result, answer := callDebug(t, session, map[string]any{
		"workflow": debugWorkflow,
		"tests": `tests:
  - name: it ships
    inputs:
      release: "2026.9.0"
    stubs:
      - task: log
        returns: {}
    expect:
      ran: [build, ship]
`,
		"commands": []any{"step", "complete inspect ", "complete inspect steps.", "complete break ", "continue"},
	})
	require.False(t, result.IsError, "the case passes: %s", result.Content[0].(*mcp.TextContent).Text)

	joined := answer.transcript()

	assert.Contains(t, joined, "steps.        step outputs",
		"the roots a bare expression may name, with the detail a terminal shows beside them")
	assert.Contains(t, joined, "inputs.       run inputs",
		"including the run's own arguments, since this run was started with some")
	assert.Contains(t, joined, "join          strings",
		"and the profile's own functions, which is what makes this worth asking rather than guessing")
	assert.Contains(t, joined, "a step that has run",
		"`steps.` answers from the paused run's own outputs")
	assert.Contains(t, joined, "a step this run may reach",
		"and `break ` from the step inventory")

	assert.Equal(t,
		[]string{"step", "complete inspect ", "complete inspect steps.", "complete break ", "continue"},
		answer.Script,
		"a completion is recorded like any other question, so the script still replays the session")
}
