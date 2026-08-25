package main

import (
	"encoding/json"
	"fmt"
	"strings"
	"testing"

	"github.com/modelcontextprotocol/go-sdk/mcp"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	flowmcp "github.com/picatz/flowstate/cmd/flow/internal/mcp"
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
