package main

import (
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/modelcontextprotocol/go-sdk/mcp"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	flowmcp "github.com/picatz/flowstate/cmd/flow/internal/mcp"
)

// testAnswer is the v1.TestReport an agent reads, by field name — the same
// discipline [runLocalAnswer] follows above and for the same reason: what is
// under test is the document a caller receives, not the Go type behind it.
type testAnswer struct {
	File    string `json:"file"`
	Refused string `json:"refused"`
	Cases   []struct {
		Name     string `json:"name"`
		Passed   bool   `json:"passed"`
		Error    string `json:"error"`
		Failures []struct {
			Message string `json:"message"`
			Field   string `json:"field"`
			Step    string `json:"step"`
			Value   string `json:"value"`
		} `json:"failures"`
	} `json:"cases"`
}

// callTest calls flowstate_test and decodes its answer.
func callTest(t *testing.T, session *mcp.ClientSession, args map[string]any) (*mcp.CallToolResult, testAnswer) {
	t.Helper()

	result, err := session.CallTool(t.Context(), &mcp.CallToolParams{
		Name:      flowmcp.TestToolName,
		Arguments: args,
	})
	require.NoError(t, err)
	require.NotEmpty(t, result.Content)

	text := result.Content[0].(*mcp.TextContent).Text

	var answer testAnswer
	if err := json.Unmarshal([]byte(text), &answer); err != nil {
		require.NoError(t, err, "the tool's answer is not a JSON document: %s", text)
	}

	return result, answer
}

// TestTheTestToolRunsAPassingCase closes the loop #241 found missing: an
// agent authoring an `http:` workflow over MCP could previously prove only
// that it parsed. This proves it behaves — stubbing an http response and
// checking the workflow's declared outputs and step presence — without a
// server, without egress, and without a *.test.yaml ever touching disk.
func TestTheTestToolRunsAPassingCase(t *testing.T) {
	t.Parallel()

	session := connectMCP(t, defaultLocalRunPosture())

	result, answer := callTest(t, session, map[string]any{
		"workflow": `edition: v2026.3
name: basic
inputs:
  tenant:
    type: string
    required: true
steps:
  - id: fetch
    http:
      method: GET
      url: https://example.internal/status
  - id: announce
    log:
      message: ${'tenant ' + inputs.tenant + ' checked'}
outputs:
  status:
    value: ${steps.fetch.status}
`,
		"tests": `tests:
  - name: a healthy check reports its status
    inputs:
      tenant: acme
    stubs:
      - task: http
        where: inputs.url == "https://example.internal/status"
        returns:
          status: 200
      - task: log
        returns: {}
    expect:
      outputs:
        status: 200
      ran: [fetch, announce]
`,
	})
	require.False(t, result.IsError, "a passing suite reported an error: %s",
		result.Content[0].(*mcp.TextContent).Text)

	require.Len(t, answer.Cases, 1)
	assert.True(t, answer.Cases[0].Passed, "the case did not pass: %+v", answer.Cases[0])
	assert.Empty(t, answer.Cases[0].Failures)
}

// TestTheTestToolReportsAFailingCaseHonestly is the [runLocalToolHandler]
// discipline applied here: a model that cannot tell a suite that failed from
// one that passed will report success, so IsError and Passed must agree, and
// the diagnostic must say what did not hold.
func TestTheTestToolReportsAFailingCaseHonestly(t *testing.T) {
	t.Parallel()

	session := connectMCP(t, defaultLocalRunPosture())

	result, answer := callTest(t, session, map[string]any{
		"workflow": `edition: v2026.3
name: basic
steps:
  - id: greet
    log:
      message: hello
outputs:
  greeting:
    value: ${"hello"}
`,
		"tests": `tests:
  - name: expects the wrong output
    stubs:
      - task: log
        returns: {}
    expect:
      outputs:
        greeting: goodbye
`,
	})
	require.True(t, result.IsError,
		"a case that did not meet its expectation was reported as if it passed")

	require.Len(t, answer.Cases, 1)
	assert.False(t, answer.Cases[0].Passed)
	require.NotEmpty(t, answer.Cases[0].Failures, "a failed case reported no diagnostic to correct against")
	assert.Contains(t, answer.Cases[0].Failures[0].Message, "goodbye",
		"the diagnostic does not say what the case actually expected")
}

// TestTheTestToolRefusesAnUnstubbedTask is `flow test`'s central promise
// (#155), proven over MCP: a task this case never bothered to stub does not
// fall through to a real implementation, it fails the case, naming the task.
func TestTheTestToolRefusesAnUnstubbedTask(t *testing.T) {
	t.Parallel()

	session := connectMCP(t, defaultLocalRunPosture())

	result, answer := callTest(t, session, map[string]any{
		"workflow": `edition: v2026.3
name: basic
steps:
  - id: fetch
    http:
      url: https://example.internal/status
`,
		"tests": `tests:
  - name: no stub for http at all
    expect:
      failed: true
`,
	})
	require.False(t, result.IsError, "expect.failed: true should be satisfied here: %s",
		result.Content[0].(*mcp.TextContent).Text)

	require.Len(t, answer.Cases, 1)
	assert.True(t, answer.Cases[0].Passed)
}

// TestTheTestToolStubsMakeNoRequest is the load-bearing safety claim in
// [flowmcp.TestToolDescription]: a stubbed task never invokes a real implementation,
// so an http step never reaches the network regardless of what this process
// was started with. Proven against a real listener rather than argued: the
// workflow names a live loopback server capable of answering, the test
// stubs the request anyway, and the server must see nothing.
func TestTheTestToolStubsMakeNoRequest(t *testing.T) {
	t.Parallel()

	var requests int
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		requests++
		w.WriteHeader(http.StatusTeapot)
	}))
	defer srv.Close()

	// defaultLocalRunPosture denies all egress, so a *real* http step to this
	// server would fail closed regardless of stubbing — which would prove
	// nothing about stubbing specifically. The point here is that the case
	// passes and the server sees nothing despite naming a server that would
	// gladly answer if actually reached.
	session := connectMCP(t, defaultLocalRunPosture())

	result, answer := callTest(t, session, map[string]any{
		"workflow": fmt.Sprintf(`edition: v2026.3
name: reaches-out
steps:
  - id: fetch
    http:
      url: %s
outputs:
  status:
    value: ${steps.fetch.status}
`, srv.URL),
		"tests": `tests:
  - name: the http step is answered by the stub, not the server
    stubs:
      - task: http
        returns:
          status: 200
    expect:
      outputs:
        status: 200
`,
	})
	require.False(t, result.IsError, "the stubbed case did not pass: %s",
		result.Content[0].(*mcp.TextContent).Text)
	require.Len(t, answer.Cases, 1)
	assert.True(t, answer.Cases[0].Passed)

	assert.Equal(t, 0, requests,
		"a stubbed http task reached the real server; stubbing is supposed to replace the effect entirely")
}

// TestTheTestToolNeedsNoEgressPolicy is the other half of the same claim: the
// tool works with no egress policy configured at all, which
// flowstate_run_local — asked to do the same thing without a stub — refuses
// by default. The contrast is the proof that flowstate_test's safety does not
// come from a policy this process happens to have, but from never reaching a
// real task in the first place.
func TestTheTestToolNeedsNoEgressPolicy(t *testing.T) {
	t.Parallel()

	posture := defaultLocalRunPosture()
	require.NoError(t, applyMCPEgressPolicy(posture))
	session := connectMCP(t, posture)

	workflow := `edition: v2026.3
name: exfiltrate
steps:
  - id: fetch
    http:
      url: https://example.com/
`

	// flowstate_run_local denies this outright under the same posture.
	runResult, runAnswer := callRunLocal(t, session, map[string]any{"source": workflow})
	require.True(t, runResult.IsError)
	assert.Contains(t, runAnswer.Run.Error.Message, "denied by egress policy")

	// flowstate_test, with the same task stubbed, needs none of that.
	testResult, testAns := callTest(t, session, map[string]any{
		"workflow": workflow,
		"tests": `tests:
  - name: stubbed, so egress never enters into it
    stubs:
      - task: http
        returns:
          status: 200
    expect:
      failed: false
`,
	})
	require.False(t, testResult.IsError, "a stubbed case needed an egress policy: %s",
		testResult.Content[0].(*mcp.TextContent).Text)
	require.Len(t, testAns.Cases, 1)
	assert.True(t, testAns.Cases[0].Passed)
}

// TestTheTestToolRequiresWorkflowAndTests mirrors
// [TestTheRunLocalToolNeedsASource]: a refusal names what to pass rather than
// an opaque decode error.
func TestTheTestToolRequiresWorkflowAndTests(t *testing.T) {
	t.Parallel()

	session := connectMCP(t, defaultLocalRunPosture())

	result, err := session.CallTool(t.Context(), &mcp.CallToolParams{
		Name:      flowmcp.TestToolName,
		Arguments: map[string]any{"tests": "tests:\n  - name: x\n    expect: {}"},
	})
	require.NoError(t, err)
	require.True(t, result.IsError)
	assert.Contains(t, result.Content[0].(*mcp.TextContent).Text, "workflow is required")

	result, err = session.CallTool(t.Context(), &mcp.CallToolParams{
		Name:      flowmcp.TestToolName,
		Arguments: map[string]any{"workflow": "edition: v2026.3\nname: x\nsteps: []"},
	})
	require.NoError(t, err)
	require.True(t, result.IsError)
	assert.Contains(t, result.Content[0].(*mcp.TextContent).Text, "tests is required")
}

// TestTheTestToolRefusesUnknownArguments is the mirror of
// [TestTheRunLocalToolRefusesUnknownArguments]: a misspelled argument is
// refused rather than silently dropped.
func TestTheTestToolRefusesUnknownArguments(t *testing.T) {
	t.Parallel()

	session := connectMCP(t, defaultLocalRunPosture())

	result, err := session.CallTool(t.Context(), &mcp.CallToolParams{
		Name: flowmcp.TestToolName,
		Arguments: map[string]any{
			"workflow": "edition: v2026.3\nname: x\nsteps: []",
			"tests":    "tests:\n  - name: x\n    expect: {}",
			"vars":     map[string]any{"oops": true},
		},
	})
	require.NoError(t, err)
	require.True(t, result.IsError)
	assert.Contains(t, result.Content[0].(*mcp.TextContent).Text, "do not match")
}

// TestTheTestToolRefusesUnparseableSources reports a refused *.test.yaml as
// `refused` rather than as a case, and flags the tool result, matching
// [flowtest.RunFile]'s own handling of a file that does not parse.
func TestTheTestToolRefusesUnparseableSources(t *testing.T) {
	t.Parallel()

	session := connectMCP(t, defaultLocalRunPosture())

	result, answer := callTest(t, session, map[string]any{
		"workflow": "edition: v2026.3\nname: x\nsteps: []",
		"tests":    "tests: [{name: x, stubs: [{task: http, returns: {}, fails: {message: no}}]}]",
	})
	require.True(t, result.IsError, "a *.test.yaml declaring both returns and fails validated clean")
	assert.Empty(t, answer.Cases)
	assert.Contains(t, answer.Refused, "declares both returns and fails")
}

// TestTheTestToolAnswerIsBounded is [TestTheRunLocalAnswerIsBounded]'s own
// shape, on the part of a TestReport a case actually controls: a mismatch's
// diagnostic message, built by comparing whatever `expect.outputs` named
// against whatever the run — here, a stub's own `returns:` — produced.
//
// The bound is asserted reached, not merely respected (CLAUDE.md): the
// unbounded message is built first and checked to actually exceed
// flowmcp.MaxResultBytes, so a cap that silently never engaged would fail
// this test rather than pass it by accident.
func TestTheTestToolAnswerIsBounded(t *testing.T) {
	t.Parallel()

	huge := strings.Repeat("x", 2<<20) // 2 MiB, well past flowmcp.MaxResultBytes.
	require.Greater(t, len(huge), flowmcp.MaxResultBytes,
		"the fixture is not actually large enough to force renderTestResult to shrink anything")

	session := connectMCP(t, defaultLocalRunPosture())

	result, answer := callTest(t, session, map[string]any{
		"workflow": fmt.Sprintf(`edition: v2026.3
name: basic
steps:
  - id: greet
    log:
      message: hello
outputs:
  greeting:
    value: "%s"
`, huge),
		"tests": `tests:
  - name: expects the wrong output
    stubs:
      - task: log
        returns: {}
    expect:
      outputs:
        greeting: goodbye
`,
	})
	require.True(t, result.IsError)

	encoded := result.Content[0].(*mcp.TextContent).Text
	assert.LessOrEqual(t, len(encoded), flowmcp.MaxResultBytes,
		"a case's own comparison spent %d bytes of a model's context", len(encoded))

	require.Len(t, answer.Cases, 1)
	assert.True(t, len(answer.Cases) > 0, "trimming took the case list with it")

	// Reached, not just respected: the huge value must actually have been
	// removed from what came back, not merely fit under the cap by luck.
	assert.NotContains(t, encoded, huge,
		"the oversized value survived into the bounded answer")
}
