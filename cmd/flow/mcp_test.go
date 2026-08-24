package main

import (
	"encoding/json"
	"fmt"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
	"unicode/utf8"

	"connectrpc.com/connect"
	"github.com/modelcontextprotocol/go-sdk/mcp"
	"github.com/spf13/cobra"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/reflect/protoreflect"
	"google.golang.org/protobuf/reflect/protoregistry"

	v1 "github.com/picatz/flowstate/pkg/flowstate/v1"
	"github.com/picatz/flowstate/pkg/flowstate/v1/flowstatev1connect"
	"github.com/picatz/flowstate/pkg/flowstate/v1/protodoc"
	"github.com/picatz/flowstate/pkg/flowstate/v1/server"

	flowmcp "github.com/picatz/flowstate/cmd/flow/internal/mcp"
)

// TestToolsMatchTheServiceDescriptor holds the tool list to the service
// descriptor, in both directions.
//
// The dispatch table in flowmcp.WorkflowServiceMethods is written out — Go cannot range
// over connect's typed methods — so what keeps it honest is this: an RPC added
// to the schema without a row here fails, and a row naming an RPC the schema no
// longer has fails. The same pattern as the README's command table, for the
// same reason: a hand-kept list is fine exactly as long as a test holds it to
// the source of truth.
//
// The surface is no longer only RPCs, and the pin says so explicitly rather than
// being loosened to accommodate it. flowstate_run_local is the local driver and
// deliberately has no RPC behind it, so the assertion is now: every RPC has a
// tool, and every tool that is not an RPC's is one this test names. A tool added
// without a line here still fails, which is the direction that matters — a
// surface that quietly grows is one nobody reviews.
func TestToolsMatchTheServiceDescriptor(t *testing.T) {
	t.Parallel()

	table := map[string]bool{}
	for _, m := range flowmcp.WorkflowServiceMethods() {
		require.False(t, table[m.Name], "the dispatch table lists %q twice", m.Name)
		table[m.Name] = true

		// The schema each tool advertises is the schema of the RPC's own request
		// message; a row pointing at the wrong descriptor would advertise fields
		// the handler then refuses.
		require.NotNil(t, m.Input, "%q has no input descriptor", m.Name)
	}

	names := serviceMethodNames(t)
	require.NotEmpty(t, names, "the service declares no methods; the lookup is broken")

	for name := range names {
		assert.True(t, table[name],
			"the schema declares rpc %s and `flow mcp` serves no tool for it; add a row "+
				"to flowmcp.WorkflowServiceMethods", name)
	}
	for name := range table {
		assert.True(t, names[name],
			"the dispatch table lists %q, which the service no longer declares", name)
	}

	// The other direction, against what a client actually sees rather than
	// against the table: registration is where a tool becomes real.
	registered := registeredToolNames(t)

	for name := range names {
		assert.True(t, registered[flowmcp.ToolName(name)],
			"rpc %s has a dispatch row but no registered tool", name)
	}
	for name := range registered {
		if documentedLocalTools[name] {
			continue
		}

		assert.True(t, names[rpcNameOfTool(name)],
			"`flow mcp` serves %q, which is neither an RPC's tool nor one of the documented "+
				"local tools (%v); add it to documentedLocalTools deliberately, with the reason "+
				"it is not a service method", name, documentedLocalToolNames())
	}
	for name := range documentedLocalTools {
		assert.True(t, registered[name],
			"%q is documented as a local tool and nothing registers it", name)
	}
}

// documentedLocalTools names every tool on this surface that is not the
// projection of an RPC.
//
// There is one, and it should stay hard to add to. A tool with no service method
// behind it is a capability that exists only here — no Connect client, no CLI
// verb, nothing else to compare its behavior against — so each one is a
// deliberate exception rather than a category.
//
// flowstate_run_local: the local driver, executing in this process. Not an RPC
// because a server executing a submitted workflow in-process is a different
// product, and inventing a service method for it would be that product's first
// half.
//
// flowstate_test: flowtest.RunSource, executing in this process the same way
// run_local does. Not an RPC for the identical reason, and not folded into
// run_local's own tool because the two answer different questions with
// different documents — a GetResponse's transcript against a v1.TestReport's
// verdicts — and a single tool choosing between them by which arguments were
// set is the shape [runLocalArguments]'s own doc comment already rejected for
// `vars`: an implicit mode a caller has to infer.
var documentedLocalTools = map[string]bool{
	flowmcp.RunLocalToolName: true,
	flowmcp.TestToolName:     true,
}

func documentedLocalToolNames() []string {
	names := make([]string, 0, len(documentedLocalTools))
	for name := range documentedLocalTools {
		names = append(names, name)
	}

	return names
}

// rpcNameOfTool inverts flowmcp.ToolName, so a registered tool can be matched back to
// the method it claims to serve.
func rpcNameOfTool(tool string) string {
	var b strings.Builder
	for _, word := range strings.Split(strings.TrimPrefix(tool, flowmcp.ToolPrefix), "_") {
		if word == "" {
			continue
		}
		b.WriteString(strings.ToUpper(word[:1]))
		b.WriteString(word[1:])
	}

	return b.String()
}

// TestEveryToolHasADescription keeps the prose complete.
//
// Read off the registered tools rather than off the RPC set, because the RPCs
// are only one half: a local tool shipping mute would have been invisible to a
// service-shaped version of this test.
func TestEveryToolHasADescription(t *testing.T) {
	t.Parallel()

	for name := range serviceMethodNames(t) {
		assert.NotEmpty(t, flowmcp.ToolDescription(name),
			"rpc %s has no description; a mute tool is one a model cannot choose", name)
	}

	for _, tool := range registeredTools(t) {
		assert.NotEmpty(t, tool.Description,
			"tool %s has no description; a mute tool is one a model cannot choose", tool.Name)
		assert.NotNil(t, tool.InputSchema,
			"tool %s advertises no input schema", tool.Name)
	}
}

// TestEveryToolDescriptionComesFromTheSchema is the half of #424 that a
// non-empty check cannot see.
//
// "Not empty" is satisfied by a sentence typed here beside the code, which is
// precisely the arrangement this slice retired: prose about an RPC written twice,
// where the copy next to the Go is the one that stays behind when the schema
// moves. So the assertion is provenance rather than presence. Every RPC tool's
// description must *begin* with the leading comment its RPC carries in
// proto/flowstate/v1/flowstate.proto, byte for byte, which no hand-written
// string can satisfy by accident.
//
// Begin with rather than equal, because a tool may add a note about this surface
// after it (see mcpToolNotes). Those are the declared exception, and the shape of
// the check keeps them honest in the one way that matters: a note can only be
// appended, so nothing here can quietly replace what the schema says.
func TestEveryToolDescriptionComesFromTheSchema(t *testing.T) {
	t.Parallel()

	for name := range serviceMethodNames(t) {
		comment, ok := protodoc.Method(flowmcp.WorkflowServiceName, protoreflect.Name(name))
		require.True(t, ok,
			"rpc %s carries no leading comment in the schema; write one in "+
				"proto/flowstate/v1/flowstate.proto, which is where this surface's prose lives", name)
		require.NotEmpty(t, comment)

		assert.True(t, strings.HasPrefix(flowmcp.ToolDescription(name), comment),
			"flowstate_%s's description does not start with %s's own schema comment; a description "+
				"written beside this code is a second copy of what the schema already says", name, name)
	}
}

// TestTheRunLocalToolDescribesWhatItDoesNotProve.
//
// The description is the only thing a model reads before deciding to trust a
// result, and this tool's result is a rehearsal: the run had no durability, no
// compaction, and its egress was whatever this process was started with. A
// description that omitted any of that would be accurate about what happened and
// misleading about what it means.
func TestTheRunLocalToolDescribesWhatItDoesNotProve(t *testing.T) {
	t.Parallel()

	description := flowmcp.RunLocalTool().Description

	for _, claim := range []string{
		"no server",
		"no Temporal",
		"denied",
		"secret",
		"Continue-As-New",
		"durab",
	} {
		assert.Contains(t, description, claim,
			"the run_local description does not mention %q, which is something an agent "+
				"reading its result needs to know", claim)
	}
}

// serviceMethodNames reads the service's methods from the compiled-in schema —
// the same registry the tools' input schemas come from.
func serviceMethodNames(t *testing.T) map[string]bool {
	t.Helper()

	desc, err := protoregistry.GlobalFiles.FindDescriptorByName("flowstate.v1.WorkflowService")
	require.NoError(t, err)

	service, ok := desc.(protoreflect.ServiceDescriptor)
	require.True(t, ok, "flowstate.v1.WorkflowService is not a service descriptor")

	names := map[string]bool{}
	methods := service.Methods()
	for i := 0; i < methods.Len(); i++ {
		names[string(methods.Get(i).Name())] = true
	}

	return names
}

// registeredTools asks a connected client what the server serves.
//
// Over the protocol rather than off the registration code, because what a tool
// list means is what a client receives.
func registeredTools(t *testing.T) []*mcp.Tool {
	t.Helper()

	session := connectMCP(t, defaultLocalRunPosture())

	result, err := session.ListTools(t.Context(), &mcp.ListToolsParams{})
	require.NoError(t, err)

	return result.Tools
}

func registeredToolNames(t *testing.T) map[string]bool {
	t.Helper()

	names := map[string]bool{}
	for _, tool := range registeredTools(t) {
		names[tool.Name] = true
	}

	return names
}

// mcpDepsFor builds the flowmcp.Deps a real server construction needs, from
// posture: the one seam cmd/flow/internal/mcp takes back from this package,
// exercised here with the same redaction `flow mcp` itself wires in mcp.go.
func mcpDepsFor(posture *cobra.Command) flowmcp.Deps {
	return flowmcp.Deps{
		Redact: func(response *v1.GetResponse) *v1.GetResponse {
			return redactGetResponse(response, nil, revealSensitiveRequested(posture))
		},
	}
}

// mcpExtraToolsFor builds the two tools that are not RPCs, the same pair
// runMCP registers, so a test connects to the identical tool set an agent
// does.
func mcpExtraToolsFor(posture *cobra.Command) []flowmcp.ToolRegistration {
	return []flowmcp.ToolRegistration{
		{Tool: flowmcp.RunLocalTool(), Handler: runLocalToolHandler(posture)},
		{Tool: flowmcp.TestTool(), Handler: testToolHandler()},
	}
}

// connectMCP stands the server up over an in-memory transport and returns a
// connected client session.
//
// posture is the flag set a run_local call executes under, which is the whole of
// what a test is choosing when it calls this: everything else is the one
// registration an agent connects to.
func connectMCP(t *testing.T, posture *cobra.Command) *mcp.ClientSession {
	t.Helper()

	srv := flowmcp.NewServer("test")

	flowmcp.AddCapabilities(srv, server.New(nil), func() flowstatev1connect.WorkflowServiceClient {
		t.Error("a local tool dialed the server")

		return nil
	}, mcpDepsFor(posture), mcpExtraToolsFor(posture)...)

	serverTransport, clientTransport := mcp.NewInMemoryTransports()

	go func() { _ = srv.Run(t.Context(), serverTransport) }()

	client := mcp.NewClient(&mcp.Implementation{Name: "test", Version: "test"}, nil)
	session, err := client.Connect(t.Context(), clientTransport, nil)
	require.NoError(t, err)
	t.Cleanup(func() { _ = session.Close() })

	return session
}

// TestTheValidateToolAnswersOverTheProtocol is the functional half: a real MCP
// client, over an in-memory transport, calling the tool an agent would call.
func TestTheValidateToolAnswersOverTheProtocol(t *testing.T) {
	t.Parallel()

	session := connectMCP(t, defaultLocalRunPosture())

	result, err := session.CallTool(t.Context(), &mcp.CallToolParams{
		Name: flowmcp.ToolName("Validate"),
		Arguments: map[string]any{
			"files": []map[string]any{{
				"name": "broken.yaml",
				// base64 of an invalid Flowfile; SourceFile.source is bytes.
				"source": []byte("edition: v2026.3\nname: x\nsteps:\n  - id: a\n    nope:\n      x: y\n"),
			}},
		},
	})
	require.NoError(t, err)
	require.False(t, result.IsError, "the tool reported an error: %v", result.Content)

	text := result.Content[0].(*mcp.TextContent).Text

	// The answer is the protojson of the RPC's own response message — the
	// report arrives under the same field a Connect caller would read it from,
	// because it is the same message.
	var response struct {
		Report struct {
			Files []struct {
				File        string `json:"file"`
				Diagnostics []struct {
					Line    int    `json:"line"`
					Message string `json:"message"`
				} `json:"diagnostics"`
			} `json:"files"`
		} `json:"report"`
	}
	require.NoError(t, json.Unmarshal([]byte(text), &response),
		"the tool's answer is not the protojson of a ValidateResponse: %s", text)

	files := response.Report.Files
	require.Len(t, files, 1)
	require.NotEmpty(t, files[0].Diagnostics,
		"an invalid file validated clean over the protocol")
	assert.NotZero(t, files[0].Diagnostics[0].Line,
		"the diagnostic lost its position crossing the protocol")
	assert.Contains(t, files[0].Diagnostics[0].Message, "nope",
		"the diagnostic does not name what the author wrote")
}

// runLocalAnswer is the tool's document, as a caller reads it.
//
// Written out rather than decoded into the schema types on purpose: what is
// under test is the *document* an agent receives, so reading it the way an agent
// would — by field name, through encoding/json — is what proves the names are
// there. Unmarshalling into a GetResponse would pass on bytes no third party
// could address.
type runLocalAnswer struct {
	Run struct {
		Status string `json:"status"`
		Error  struct {
			Message string `json:"message"`
			Kind    string `json:"kind"`
		} `json:"error"`
		Outputs struct {
			StepValues map[string]struct {
				NamedValues map[string]struct {
					Literal map[string]any `json:"literal"`
				} `json:"namedValues"`
			} `json:"stepValues"`
		} `json:"outputs"`
		// The run's answer, beside the transcript above, read by name for the
		// reason the rest of this struct is: what an agent addresses is the
		// field, not the Go type behind it.
		RunOutputs struct {
			Values map[string]struct {
				Literal map[string]any `json:"literal"`
			} `json:"values"`
		} `json:"runOutputs"`
	} `json:"run"`
	Logs []struct {
		Level   string `json:"level"`
		Message string `json:"message"`
	} `json:"logs"`
	Note string `json:"note"`
}

// callRunLocal calls the tool and decodes its answer.
func callRunLocal(t *testing.T, session *mcp.ClientSession, args map[string]any) (*mcp.CallToolResult, runLocalAnswer) {
	t.Helper()

	result, err := session.CallTool(t.Context(), &mcp.CallToolParams{
		Name:      flowmcp.RunLocalToolName,
		Arguments: args,
	})
	require.NoError(t, err)
	require.NotEmpty(t, result.Content)

	text := result.Content[0].(*mcp.TextContent).Text

	// A failed run still answers with the document — the status and the reason
	// are the two things a model needs to decide what to do next — so this
	// decodes whatever came back. Only a refusal before the run (a diagnostic) is
	// bare text, and the tests for those read the text.
	var answer runLocalAnswer
	if err := json.Unmarshal([]byte(text), &answer); err != nil && !result.IsError {
		require.NoError(t, err, "the tool's answer is not a JSON document: %s", text)
	}

	return result, answer
}

// TestTheRunLocalToolExecutesAWorkflow closes the loop the surface used to
// dead-end at: a file an agent could write, executed, with its outputs and its
// narration coming back as data.
func TestTheRunLocalToolExecutesAWorkflow(t *testing.T) {
	t.Parallel()

	session := connectMCP(t, defaultLocalRunPosture())

	result, answer := callRunLocal(t, session, map[string]any{
		"source": `edition: v2026.3
name: offline
vars:
  who: world
steps:
  - id: greet
    log:
      message: ${"hello %s".format([vars.who])}
    vars:
      greeting: ${"hello %s".format([vars.who])}
`,
	})
	require.False(t, result.IsError, "a valid workflow reported an error: %s",
		result.Content[0].(*mcp.TextContent).Text)

	assert.Equal(t, "STATUS_COMPLETED", answer.Run.Status)

	_, ok := answer.Run.Outputs.StepValues["greet"]
	assert.True(t, ok,
		"the run reported nothing for the step it ran, so an agent cannot tell it ran: %+v",
		answer.Run.Outputs)

	// The narration is data rather than a stream, because stdout on this surface
	// is the protocol — a `log:` step writing there would corrupt the session it
	// is reporting into.
	require.NotEmpty(t, answer.Logs, "a log: step emitted nothing an agent can read")
	assert.Equal(t, "hello world", answer.Logs[0].Message,
		"the message the step composed did not survive into the answer")
	assert.Equal(t, "INFO", answer.Logs[0].Level)
}

// TestTheRunLocalToolAnswersAGate rehearses an approval gate, which is the thing
// an author most wants to exercise before production and least wants to first
// meet there.
func TestTheRunLocalToolAnswersAGate(t *testing.T) {
	t.Parallel()

	session := connectMCP(t, defaultLocalRunPosture())

	result, answer := callRunLocal(t, session, map[string]any{
		"source": `edition: v2026.3
name: gated
steps:
  - id: approval
    wait_for_signal:
      name: deploy-approved
      timeout: 30s

  - id: deploy
    if: ${steps.approval.payload.approved}
    log:
      message: deploying
`,
		"signals": map[string]any{
			"deploy-approved": map[string]any{"approved": true, "by": "someone@example.com"},
		},
	})
	require.False(t, result.IsError, "the gated workflow failed: %s",
		result.Content[0].(*mcp.TextContent).Text)

	assert.Equal(t, "STATUS_COMPLETED", answer.Run.Status)

	approval, ok := answer.Run.Outputs.StepValues["approval"]
	require.True(t, ok, "the wait reported no outputs: %+v", answer.Run.Outputs)
	assert.Equal(t, false, approval.NamedValues["timed_out"].Literal["boolValue"],
		"a gate answered up front reported as timed out")
	assert.NotEmpty(t, approval.NamedValues["payload"].Literal,
		"the payload supplied in the tool's signals did not reach the waiting step")

	require.NotEmpty(t, answer.Logs, "the step behind the gate did not run")
	assert.Equal(t, "deploying", answer.Logs[0].Message)
}

// TestTheRunLocalToolTakesInputs closes the same loop for a workflow that takes
// arguments: an agent authoring a parameterized file can supply them and read
// back what the run answered with, without a server.
//
// The arguments go in as JSON of the declared types, which is why this is not
// simply `flow run local --input` in another costume: an agent composing a call
// already has a document, and the coercion the CLI performs from shell words has
// nothing to do here.
func TestTheRunLocalToolTakesInputs(t *testing.T) {
	t.Parallel()

	session := connectMCP(t, defaultLocalRunPosture())

	result, answer := callRunLocal(t, session, map[string]any{
		"source": `edition: v2026.3
name: parameterized
inputs:
  service:
    type: string
    required: true
  replicas:
    type: int
    default: 2
outputs:
  placed:
    value: ${inputs.service}
  replicas:
    value: ${inputs.replicas}
steps:
  - id: plan
    log:
      message: ${'planning ' + inputs.service}
`,
		"inputs": map[string]any{"service": "checkout", "replicas": 5},
	})
	require.False(t, result.IsError, "a parameterized workflow failed: %s",
		result.Content[0].(*mcp.TextContent).Text)

	assert.Equal(t, "STATUS_COMPLETED", answer.Run.Status)
	require.NotEmpty(t, answer.Logs, "the step did not run")
	assert.Equal(t, "planning checkout", answer.Logs[0].Message,
		"the argument did not reach the step that reads it")

	// The answer, in the field a durable run reports it in — which is what makes
	// this tool's document the one flowstate_get answers with.
	require.NotNil(t, answer.Run.RunOutputs.Values, "the run reported no declared outputs")
	assert.Equal(t, "checkout",
		answer.Run.RunOutputs.Values["placed"].Literal["stringValue"])

	// A whole number sent as JSON stays a whole number: protojson writes an int64
	// as a string, and a float would have come back under doubleValue instead.
	assert.Equal(t, "5", answer.Run.RunOutputs.Values["replicas"].Literal["int64Value"],
		"an int input arrived as something other than an int")
}

// TestTheRunLocalToolRedactsSensitiveOutputs is the MCP half of the gap PR #205
// left: an agent's context is an untrusted-consumer surface exactly like a
// terminal, so a value the submitted source declared `sensitive: true` must not
// come back in the tool's answer any more than `flow run local` prints it. The
// source is right here in the call, so redaction is precise: the sensitive name
// is withheld and the other declared output renders unchanged.
func TestTheRunLocalToolRedactsSensitiveOutputs(t *testing.T) {
	t.Parallel()

	session := connectMCP(t, defaultLocalRunPosture())

	const secret = "sk-live-0123456789abcdef"

	result, answer := callRunLocal(t, session, map[string]any{
		"source": fmt.Sprintf(`edition: v2026.3
name: has-a-secret
outputs:
  token:
    value: ${"%s"}
    sensitive: true
  region:
    value: ${"us-east-1"}
steps:
  - id: noop
    log:
      message: done
`, secret),
	})
	require.False(t, result.IsError, "the workflow failed: %s", result.Content[0].(*mcp.TextContent).Text)

	rawText := result.Content[0].(*mcp.TextContent).Text
	require.NotContains(t, rawText, secret,
		"the actual secret string must be absent from the rendered bytes, not merely covered by a marker")

	require.Equal(t, "[redacted: token]", answer.Run.RunOutputs.Values["token"].Literal["stringValue"])
	require.Equal(t, "us-east-1", answer.Run.RunOutputs.Values["region"].Literal["stringValue"],
		"a value the source did not mark sensitive must render unchanged")
}

// TestTheRunLocalToolRevealSensitiveShowsValues checks the escape hatch on this
// surface: --reveal-sensitive on the `flow mcp` process, decided once at
// start-up, shows what the tool would otherwise withhold.
func TestTheRunLocalToolRevealSensitiveShowsValues(t *testing.T) {
	t.Parallel()

	posture := defaultLocalRunPosture()
	require.NoError(t, posture.Flags().Set(revealSensitiveFlagName, "true"))

	session := connectMCP(t, posture)

	const secret = "sk-live-0123456789abcdef"

	_, answer := callRunLocal(t, session, map[string]any{
		"source": fmt.Sprintf(`edition: v2026.3
name: has-a-secret
outputs:
  token:
    value: ${"%s"}
    sensitive: true
steps:
  - id: noop
    log:
      message: done
`, secret),
	})

	require.Equal(t, secret, answer.Run.RunOutputs.Values["token"].Literal["stringValue"])
}

// TestTheRunLocalToolRedactsAStepComputedSensitiveOutput is the Codex finding on
// PR #212, exercised end to end through this tool: `outputs.token.value:
// ${steps.fetch.payload.token}` with `sensitive: true` withholds `token` at the
// name it surfaces under, and the same raw value used to still ship in the
// clear one line down, in `outputs.stepValues.fetch` — the transcript this same
// tool result carries beside the answer. `wait_for_signal` supplies the step
// output here rather than the http task, so the test needs no network: the
// signal's payload is exactly as caller-controlled as an HTTP response body is,
// which is what makes it the same shape as the bug report's `${steps.fetch.token}`.
func TestTheRunLocalToolRedactsAStepComputedSensitiveOutput(t *testing.T) {
	t.Parallel()

	session := connectMCP(t, defaultLocalRunPosture())

	const secret = "sk-live-0123456789abcdef"

	result, answer := callRunLocal(t, session, map[string]any{
		"source": `edition: v2026.3
name: secret-from-a-step
outputs:
  token:
    value: ${steps.fetch.payload.token}
    sensitive: true
  region:
    value: ${"us-east-1"}
steps:
  - id: fetch
    wait_for_signal:
      name: credentials-ready
      timeout: 30s
`,
		"signals": map[string]any{
			"credentials-ready": map[string]any{"token": secret},
		},
	})
	require.False(t, result.IsError, "the workflow failed: %s", result.Content[0].(*mcp.TextContent).Text)

	rawText := result.Content[0].(*mcp.TextContent).Text
	require.NotContains(t, rawText, secret,
		"the raw value must be absent from the whole tool result, including the step transcript — "+
			"not only from the name it surfaced under as a declared output")

	require.Equal(t, "[redacted: token]", answer.Run.RunOutputs.Values["token"].Literal["stringValue"])
	require.Equal(t, "us-east-1", answer.Run.RunOutputs.Values["region"].Literal["stringValue"],
		"a value the source did not mark sensitive must render unchanged")

	fetched, ok := answer.Run.Outputs.StepValues["fetch"]
	require.True(t, ok, "the step still ran and the transcript should say so: %+v", answer.Run.Outputs)
	require.NotEqual(t, secret, fmt.Sprintf("%v", fetched.NamedValues["payload"].Literal),
		"the step's own transcript entry must not carry the raw value either")
}

// TestTheRunLocalToolRefusesArgumentsTheSourceDoesNotDeclare is the negative
// direction, and the one that decides whether `inputs` is a contract or a hole:
// the names a call may pass are the names the submitted source declared, and the
// refusal is the binder's — the same text the server gives a caller of Run.
func TestTheRunLocalToolRefusesArgumentsTheSourceDoesNotDeclare(t *testing.T) {
	t.Parallel()

	session := connectMCP(t, defaultLocalRunPosture())

	const source = `edition: v2026.3
name: parameterized
inputs:
  service:
    type: string
    required: true
steps:
  - id: plan
    log:
      message: ${'planning ' + inputs.service}
`

	for _, test := range []struct {
		name   string
		inputs map[string]any
		says   string
	}{
		{
			name:   "a name the source does not declare",
			inputs: map[string]any{"service": "checkout", "regoin": "eu-west-1"},
			says:   "is not declared",
		},
		{
			name:   "a required argument left out",
			inputs: map[string]any{},
			says:   "is required",
		},
		{
			name:   "a value of the wrong type",
			inputs: map[string]any{"service": 3},
			says:   "declared string",
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()

			args := map[string]any{"source": source}
			if len(test.inputs) > 0 {
				args["inputs"] = test.inputs
			}

			result, _ := callRunLocal(t, session, args)
			require.True(t, result.IsError, "the call was accepted: %v", result.Content)
			assert.Contains(t, result.Content[0].(*mcp.TextContent).Text, test.says,
				"the refusal does not say what is wrong with the call")
		})
	}
}

// TestTheRunToolCarriesInputsThroughItsDerivedSchema is the *other* run tool, and
// the claim being checked is that nothing had to be written for it.
//
// flowstate_run submits a RunRequest, whose schema is derived from the message
// descriptor — so `inputs` became callable on the day the field was added, with
// no line in this repository naming it. That is the whole argument for deriving
// the surface, and it is worth an assertion precisely because the alternative
// failure is silent: a tool that quietly cannot express a field the RPC takes.
func TestTheRunToolCarriesInputsThroughItsDerivedSchema(t *testing.T) {
	t.Parallel()

	var request protoreflect.MessageDescriptor
	for _, method := range flowmcp.WorkflowServiceMethods() {
		if method.Name == "Run" {
			request = method.Input
		}
	}
	require.NotNil(t, request, "the service declares no Run method")

	schema := flowmcp.SchemaForMessage(request)

	properties, ok := schema["properties"].(map[string]any)
	require.True(t, ok, "the derived schema has no properties")

	inputs, ok := properties["inputs"].(map[string]any)
	require.True(t, ok,
		"flowstate_run's schema has no `inputs`, so an agent cannot start a parameterized "+
			"run: %v", properties)
	assert.Equal(t, "object", inputs["type"],
		"RunRequest.inputs is a map, which protojson writes as an object")

	// Derived rather than described: the field's own documentation lives in the
	// schema, and what this asserts is that the *shape* an agent must send —
	// values, each a Value message — survived the projection.
	held, ok := inputs["additionalProperties"].(map[string]any)
	require.True(t, ok, "the map's value has no schema, so an agent has nothing to send")
	assert.Equal(t, "object", held["type"],
		"an argument's value is a Value message, which protojson writes as an object")
}

// TestTheRunLocalToolReportsDiagnostics: the reason errors come back as tool
// results rather than protocol errors is that a model can act on them. A
// diagnostic without a position is one it cannot act on.
func TestTheRunLocalToolReportsDiagnostics(t *testing.T) {
	t.Parallel()

	session := connectMCP(t, defaultLocalRunPosture())

	result, _ := callRunLocal(t, session, map[string]any{
		"source": "edition: v2026.3\nname: x\nsteps:\n  - id: a\n    nope:\n      x: y\n",
	})
	require.True(t, result.IsError, "an invalid Flowfile executed without complaint")

	text := result.Content[0].(*mcp.TextContent).Text
	assert.Contains(t, text, "nope", "the diagnostic does not name what the author wrote")
	assert.Regexp(t, `\d+:\d+:`, text,
		"the diagnostic carries no line:column, so an agent cannot find what to fix: %s", text)
}

// TestTheRunLocalToolRefusesEgressByDefault is the negative direction, and the
// one that matters.
//
// The tenancy lesson generalizes: asserting that a permitted thing works is a
// functionality test wearing a security test's clothes. What has to be proved
// here is that a workflow a model composed *cannot* make this process fetch a
// URL when nobody turned egress on — so the assertion is on the run failing, at
// a public address, with no server of ours anywhere near it.
//
// It goes through applyMCPEgressPolicy rather than constructing a policy,
// because the posture under test is the one `flow mcp` starts with.
func TestTheRunLocalToolRefusesEgressByDefault(t *testing.T) {
	posture := defaultLocalRunPosture()
	require.NoError(t, applyMCPEgressPolicy(posture))

	session := connectMCP(t, posture)

	result, answer := callRunLocal(t, session, map[string]any{
		"source": `edition: v2026.3
name: exfiltrate
steps:
  - id: fetch
    http:
      url: https://example.com/
`,
	})
	require.True(t, result.IsError,
		"a run reached a non-loopback URL with no egress policy configured: %s",
		result.Content[0].(*mcp.TextContent).Text)

	assert.Equal(t, "STATUS_FAILED", answer.Run.Status)
	// The whole phrase, because "failed" is not the assertion: a sandbox with no
	// route to the internet would fail this run too, and a test satisfied by that
	// would pass on a machine where egress was wide open.
	assert.Contains(t, answer.Run.Error.Message, "denied by egress policy",
		"the run failed for some reason other than the egress policy denying it, so this "+
			"proves nothing about the policy: %s", answer.Run.Error.Message)

	// #241's P2, at the surface an agent actually reads: the MCP tool result, not
	// the Go message the tool marshals from. A policy denial is permanent — an
	// agent that retried on seeing PolicyDenied would be retrying a request the
	// same policy will refuse again — so this is exactly the answer that has to
	// arrive as data rather than be parsed out of the sentence above.
	assert.Equal(t, v1.ErrorKindPolicyDenied.String(), answer.Run.Error.Kind,
		"the MCP tool result did not carry the run's ErrorKind")
}

// TestTheRunLocalAnswerIsBounded.
//
// The outputs are whatever the submitted workflow chose to produce, and the
// consumer is a context window. What is asserted is both directions of the
// bound: the answer comes in under it, and it is still a JSON document that
// says what it dropped — cutting the bytes at the limit would satisfy the first
// half while making the answer unreadable, which is the failure this shape
// exists to avoid.
func TestTheRunLocalAnswerIsBounded(t *testing.T) {
	t.Parallel()

	outputs := &v1.Workflow_StepOutputs{StepValues: map[string]*v1.Node_Outputs{}}
	for i := range 64 {
		outputs.StepValues[fmt.Sprintf("step_%d", i)] = &v1.Node_Outputs{
			NamedValues: map[string]*v1.Value{
				"body": v1.NewValue(strings.Repeat("x", 32<<10)),
			},
		}
	}

	response := localRun(outputs, nil, nil, time.Now(), time.Now())

	encoded, err := renderRunLocalResult(response, []runLocalLogRecord{{Level: "INFO", Message: "hi"}})
	require.NoError(t, err)
	assert.LessOrEqual(t, len(encoded), flowmcp.MaxResultBytes,
		"a run's outputs are the workflow's choice, and this one spent %d bytes of a model's context",
		len(encoded))

	var answer runLocalAnswer
	require.NoError(t, json.Unmarshal(encoded, &answer),
		"the bounded answer is not parseable, which makes a large run indistinguishable from a broken tool")

	assert.NotEmpty(t, answer.Note, "the answer was trimmed and does not say so")
	assert.Equal(t, "STATUS_COMPLETED", answer.Run.Status,
		"trimming took the status with it; the two things worth keeping are what happened and why")
}

// TestTheRunLocalAnswerIsBoundedByItsDeclaredOutputs is the direction the test
// above cannot see.
//
// It grows the step transcript, which the third shrinking step drops. A run's
// *declared* outputs are carried in a different field, and were left untouched
// by that drop and then returned with no further size check — so a single
// `outputs:` expression building a large string spent a model's context
// regardless of the cap. The workflow chooses that expression, which is what
// makes it the same untrusted-consumer problem as the transcript rather than a
// smaller one.
func TestTheRunLocalAnswerIsBoundedByItsDeclaredOutputs(t *testing.T) {
	t.Parallel()

	outputs := &v1.Workflow_StepOutputs{
		StepValues: map[string]*v1.Node_Outputs{
			"build": {NamedValues: map[string]*v1.Value{"body": v1.NewValue("small")}},
		},
		RunOutputs: &v1.RunOutputs{Values: map[string]*v1.Value{
			"report": v1.NewValue(strings.Repeat("x", 2<<20)),
		}},
	}

	encoded, err := renderRunLocalResult(
		localRun(outputs, nil, nil, time.Now(), time.Now()),
		[]runLocalLogRecord{{Level: "INFO", Message: "hi"}},
	)
	require.NoError(t, err)

	assert.LessOrEqual(t, len(encoded), flowmcp.MaxResultBytes,
		"a declared output is the workflow's choice too, and this one spent %d bytes of a model's context",
		len(encoded))

	var answer runLocalAnswer
	require.NoError(t, json.Unmarshal(encoded, &answer),
		"the bounded answer is not parseable, which makes a large run indistinguishable from a broken tool")

	assert.Empty(t, answer.Run.RunOutputs.Values,
		"the declared outputs are what carried the answer past the cap and are still in it")
	assert.NotEmpty(t, answer.Note, "the answer was trimmed and does not say so")
	assert.Equal(t, "STATUS_COMPLETED", answer.Run.Status,
		"trimming took the status with it; the two things worth keeping are what happened and why")
}

// TestADeclaredOutputThatFitsSurvivesTheTranscript pins the shrinking *order*.
//
// The transcript is commentary; the declared outputs are what the workflow said
// it answers with. A run whose transcript alone exceeds the cap must lose the
// transcript and keep the answer — dropping both at once would be a simpler
// implementation and a worse one.
func TestADeclaredOutputThatFitsSurvivesTheTranscript(t *testing.T) {
	t.Parallel()

	outputs := &v1.Workflow_StepOutputs{
		StepValues: map[string]*v1.Node_Outputs{},
		RunOutputs: &v1.RunOutputs{Values: map[string]*v1.Value{
			"release": v1.NewValue("2026.8.1"),
		}},
	}
	for i := range 64 {
		outputs.StepValues[fmt.Sprintf("step_%d", i)] = &v1.Node_Outputs{
			NamedValues: map[string]*v1.Value{"body": v1.NewValue(strings.Repeat("x", 32<<10))},
		}
	}

	encoded, err := renderRunLocalResult(
		localRun(outputs, nil, nil, time.Now(), time.Now()),
		[]runLocalLogRecord{{Level: "INFO", Message: "hi"}},
	)
	require.NoError(t, err)
	assert.LessOrEqual(t, len(encoded), flowmcp.MaxResultBytes)

	var answer runLocalAnswer
	require.NoError(t, json.Unmarshal(encoded, &answer))

	require.Contains(t, answer.Run.RunOutputs.Values, "release",
		"the transcript was what did not fit, and the run's own answer went with it")
	assert.Empty(t, answer.Run.Outputs.StepValues, "the transcript is still here, so nothing was actually dropped")
}

// TestARunThatFitsIsNotTrimmed is the other side of that bound: an ordinary run
// keeps its outputs and its logs, so the trimming path cannot quietly become the
// normal one.
func TestARunThatFitsIsNotTrimmed(t *testing.T) {
	t.Parallel()

	outputs := &v1.Workflow_StepOutputs{StepValues: map[string]*v1.Node_Outputs{
		"greet": {NamedValues: map[string]*v1.Value{"body": v1.NewValue("hello")}},
	}}

	encoded, err := renderRunLocalResult(
		localRun(outputs, nil, nil, time.Now(), time.Now()),
		[]runLocalLogRecord{{Level: "INFO", Message: "hi"}},
	)
	require.NoError(t, err)

	var answer runLocalAnswer
	require.NoError(t, json.Unmarshal(encoded, &answer))

	assert.Empty(t, answer.Note)
	assert.Len(t, answer.Logs, 1)
	assert.Equal(t, "hello",
		answer.Run.Outputs.StepValues["greet"].NamedValues["body"].Literal["stringValue"])
}

// TestRunLocalLogsAreByteBounded covers the single-record direction of the
// collector's bound. A record count cannot constrain one workflow-controlled
// message or field, and retaining a short slice of either would still retain
// its large backing allocation.
func TestRunLocalLogsAreByteBounded(t *testing.T) {
	t.Parallel()

	logs := newRunLocalLogs()
	record := slog.NewRecord(time.Now(), slog.LevelInfo, strings.Repeat("界", 1<<20), 0)
	record.Add("body", strings.Repeat("x", 2<<20))
	require.NoError(t, logs.Handle(t.Context(), record))

	records := logs.records()
	require.Len(t, records, 1)
	assert.LessOrEqual(t, len(records[0].Message), maxRunLocalLogBytes)
	assert.True(t, utf8.ValidString(records[0].Message), "the byte cap split a UTF-8 rune")

	held := len(records[0].Message)
	for key, value := range records[0].Fields {
		held += len(key) + len(value)
	}
	assert.LessOrEqual(t, held, maxRunLocalLogBytes,
		"one log record retained %d bytes despite the collector's byte budget", held)
}

// TestTheRunLocalToolRefusesUnknownArguments.
//
// The schema says additionalProperties:false and the handler has to mean it: an
// argument invented by a model and silently dropped is a tool that reports
// success for something it did not do — and the plausible invention here is
// exactly the dangerous one, a caller trying to widen its own egress.
func TestTheRunLocalToolRefusesUnknownArguments(t *testing.T) {
	t.Parallel()

	session := connectMCP(t, defaultLocalRunPosture())

	result, err := session.CallTool(t.Context(), &mcp.CallToolParams{
		Name: flowmcp.RunLocalToolName,
		Arguments: map[string]any{
			"source":        "edition: v2026.3\nname: x\nsteps:\n- id: a\n  log:\n    message: hi\n",
			"egress_policy": "/tmp/anything.yaml",
		},
	})

	// Either the SDK refuses it against the advertised schema or the handler
	// does; what must not happen is the run proceeding as though the argument had
	// not been sent.
	if err != nil {
		return
	}
	require.True(t, result.IsError,
		"an unknown argument was accepted: %v", result.Content)
}

// TestTheRunLocalToolNeedsASource keeps the required field required at the
// handler, not only in the schema a client may or may not enforce.
func TestTheRunLocalToolNeedsASource(t *testing.T) {
	t.Parallel()

	session := connectMCP(t, defaultLocalRunPosture())

	result, err := session.CallTool(t.Context(), &mcp.CallToolParams{
		Name:      flowmcp.RunLocalToolName,
		Arguments: map[string]any{"source": "   "},
	})
	require.NoError(t, err)
	assert.True(t, result.IsError, "a blank source executed")
}

// TestRunLocalFlagsMirrorRunLocal holds the MCP process's posture to the CLI's.
//
// The two are the same driver, so a lever that governs one and not the other is
// a rehearsal that differs from the rehearsal — the shape of every driver
// disagreement this repository has found. The comparison is by name and default
// against the real `flow run local` command, so adding a flag there without
// considering it here fails, which is the direction that goes wrong silently.
func TestRunLocalFlagsMirrorRunLocal(t *testing.T) {
	t.Parallel()

	runLocal := findCommand(t, "run local")
	mcpCmd := findCommand(t, "mcp")

	// The flags `flow run local` takes that decide what a run may reach, as
	// opposed to how its answer is printed or how it is signalled from a shell.
	reach := []string{
		"egress-policy",
		"secret-env", "secret-dir", "secret-env-namespace",
		"secret-dir-namespaced", "secret-require-namespace",
		"as-subject", "as-issuer", "as-namespace", "as-deployment", "as-claim",
		"auth-policy", "identity-key",
	}

	for _, name := range reach {
		local := runLocal.Flags().Lookup(name)
		require.NotNil(t, local, "`flow run local` no longer declares --%s; this list is stale", name)

		served := mcpCmd.Flags().Lookup(name)
		require.NotNil(t, served,
			"`flow mcp` does not declare --%s, so flowstate_run_local rehearses under a "+
				"different policy surface than `flow run local` does", name)

		assert.Equal(t, local.DefValue, served.DefValue,
			"--%s defaults differently on `flow mcp` than on `flow run local`", name)
	}
}

// findCommand walks the real CLI tree, so what is compared is what ships.
func findCommand(t *testing.T, path string) *cobra.Command {
	t.Helper()

	cmd, _, err := newRootCommand().Find(strings.Fields(path))
	require.NoError(t, err)
	require.Equal(t, strings.Fields(path)[len(strings.Fields(path))-1], cmd.Name(),
		"`flow %s` was not found; the command tree moved", path)

	return cmd
}

// connectRemoteMCP is [connectMCP] for the tools that are a real RPC rather than
// the local driver: it wires the server's remote client at a fake WorkflowService
// instead of erroring the moment one dials out.
func connectRemoteMCP(t *testing.T, posture *cobra.Command, fake *fakeWorkflowService) *mcp.ClientSession {
	t.Helper()

	address := serveFake(t, fake)

	srv := mcp.NewServer(&mcp.Implementation{Name: "flowstate", Version: "test"}, nil)
	flowmcp.AddCapabilities(srv, server.New(nil), func() flowstatev1connect.WorkflowServiceClient {
		return newWorkflowServiceClient(serverFlags{address: address})
	}, mcpDepsFor(posture), mcpExtraToolsFor(posture)...)

	serverTransport, clientTransport := mcp.NewInMemoryTransports()
	go func() { _ = srv.Run(t.Context(), serverTransport) }()

	client := mcp.NewClient(&mcp.Implementation{Name: "test", Version: "test"}, nil)
	session, err := client.Connect(t.Context(), clientTransport, nil)
	require.NoError(t, err)
	t.Cleanup(func() { _ = session.Close() })

	return session
}

// TestTheGetToolFailsClosedWithNoSpecification is the generic RPC surface's own
// version of the gap PR #205 left: a `flowstate_get` call addresses a run by id
// alone, over the same dispatch every RPC shares, and there is no workflow
// specification anywhere in reach to say which of its declared outputs are
// sensitive. CLAUDE.md's fail-closed rule applies exactly as it does to `flow
// get`: every declared output is withheld, and the actual secret string must be
// absent from the rendered bytes.
func TestTheGetToolFailsClosedWithNoSpecification(t *testing.T) {
	const secret = "sk-live-0123456789abcdef"

	fake := &fakeWorkflowService{
		getResponse: &v1.GetResponse{
			WorkflowId: "flowstate-workflow-3f7c",
			Status:     v1.RunResponse_STATUS_COMPLETED,
			RunOutputs: &v1.RunOutputs{Values: map[string]*v1.Value{
				"token": v1.NewLiteral(secret),
			}},
		},
	}

	session := connectRemoteMCP(t, defaultLocalRunPosture(), fake)

	result, err := session.CallTool(t.Context(), &mcp.CallToolParams{
		Name:      flowmcp.ToolName("Get"),
		Arguments: map[string]any{"workflowId": "flowstate-workflow-3f7c"},
	})
	require.NoError(t, err)
	require.False(t, result.IsError)

	text := result.Content[0].(*mcp.TextContent).Text
	require.NotContains(t, text, secret,
		"the actual secret string must be absent from the tool's answer, not merely covered by a marker")
	require.Contains(t, text, "[redacted: token]")
}

// TestTheGetToolRevealsWithTheServerFlag checks the escape hatch on the generic
// RPC surface: --reveal-sensitive on the `flow mcp` process shows what
// flowstate_get would otherwise withhold, decided once at start-up rather than
// per call — a client speaking to this over stdio never gets to choose it.
func TestTheGetToolRevealsWithTheServerFlag(t *testing.T) {
	const secret = "sk-live-0123456789abcdef"

	fake := &fakeWorkflowService{
		getResponse: &v1.GetResponse{
			WorkflowId: "flowstate-workflow-3f7c",
			Status:     v1.RunResponse_STATUS_COMPLETED,
			RunOutputs: &v1.RunOutputs{Values: map[string]*v1.Value{
				"token": v1.NewLiteral(secret),
			}},
		},
	}

	posture := defaultLocalRunPosture()
	require.NoError(t, posture.Flags().Set(revealSensitiveFlagName, "true"))

	session := connectRemoteMCP(t, posture, fake)

	result, err := session.CallTool(t.Context(), &mcp.CallToolParams{
		Name:      flowmcp.ToolName("Get"),
		Arguments: map[string]any{"workflowId": "flowstate-workflow-3f7c"},
	})
	require.NoError(t, err)
	require.False(t, result.IsError)

	require.Contains(t, result.Content[0].(*mcp.TextContent).Text, secret)
}

// remoteOnlyTaskName is a task name that never appears in this binary's own
// registry. It stands in for a plugin task a real deployment might have and
// this build has never heard of — the fact TestTheGetCatalogToolDispatchesToAnAddressedDeployment
// needs in order to tell "the deployment answered" from "the in-process
// server answered" apart, rather than merely that a tool call returned
// something.
const remoteOnlyTaskName = "flowstate-test-remote-only-plugin-task"

// connectMCPWithDeps is [connectMCP] with the full [flowmcp.Deps] under the
// caller's control, for TestTheGetCatalogToolDispatchesToAnAddressedDeployment
// and its unreachable-deployment sibling — both need Deps.RemoteCatalogAddress
// set to something [connectMCP] and [connectRemoteMCP] have no way to pass.
func connectMCPWithDeps(t *testing.T, posture *cobra.Command, remote func() flowstatev1connect.WorkflowServiceClient, deps flowmcp.Deps) *mcp.ClientSession {
	t.Helper()

	srv := mcp.NewServer(&mcp.Implementation{Name: "flowstate", Version: "test"}, nil)
	flowmcp.AddCapabilities(srv, server.New(nil), remote, deps, mcpExtraToolsFor(posture)...)

	serverTransport, clientTransport := mcp.NewInMemoryTransports()
	go func() { _ = srv.Run(t.Context(), serverTransport) }()

	client := mcp.NewClient(&mcp.Implementation{Name: "test", Version: "test"}, nil)
	session, err := client.Connect(t.Context(), clientTransport, nil)
	require.NoError(t, err)
	t.Cleanup(func() { _ = session.Close() })

	return session
}

// TestTheGetCatalogToolDispatchesToAnAddressedDeployment is the joint #439
// fixes: an in-process answer and a remote answer that each work standing
// alone is exactly what shipped the defect, so what has to be proved is that
// configuring an address changes *which* catalog comes back, distinguishably.
//
// The in-process server's own catalog is asserted not to contain
// remoteOnlyTaskName first, so the assertion that the tool's answer does
// contain it is not vacuously true of any non-empty catalog.
func TestTheGetCatalogToolDispatchesToAnAddressedDeployment(t *testing.T) {
	local := server.New(nil)
	localResp, err := local.GetCatalog(t.Context(), connect.NewRequest(&v1.GetCatalogRequest{}))
	require.NoError(t, err)
	for _, task := range localResp.Msg.GetCatalog().GetTasks() {
		require.NotEqual(t, remoteOnlyTaskName, task.GetName(),
			"the fixture task name collides with this binary's own registry; pick another")
	}

	fake := &fakeWorkflowService{
		getCatalogResponse: &v1.GetCatalogResponse{
			Catalog: &v1.TaskCatalog{
				Tasks: []*v1.TaskDescription{{Name: remoteOnlyTaskName, Summary: "a plugin task only the deployment has"}},
			},
		},
	}
	address := serveFake(t, fake)

	posture := defaultLocalRunPosture()
	session := connectMCPWithDeps(t, posture, func() flowstatev1connect.WorkflowServiceClient {
		return newWorkflowServiceClient(serverFlags{address: address})
	}, flowmcp.Deps{
		Redact:               func(r *v1.GetResponse) *v1.GetResponse { return r },
		RemoteCatalogAddress: address,
	})

	result, err := session.CallTool(t.Context(), &mcp.CallToolParams{
		Name:      flowmcp.ToolName("GetCatalog"),
		Arguments: map[string]any{},
	})
	require.NoError(t, err)
	require.False(t, result.IsError, "the tool reported an error: %v", result.Content)

	text := result.Content[0].(*mcp.TextContent).Text
	require.Contains(t, text, remoteOnlyTaskName,
		"the deployment's catalog was configured with --address but the tool answered with "+
			"something else, which is #439's regression: %s", text)
	require.NotNil(t, fake.gotGetCatalog, "the addressed deployment was never asked for its catalog")
}

// TestTheGetCatalogToolRefusesAnUnreachableDeployment is the fail-closed half
// of the same decision: when --address names a deployment and that
// deployment cannot be reached, the tool must refuse rather than silently
// falling back to this binary's own build — a silent fallback here would be
// the identical defect one level up, an answer that looks authoritative and
// is not.
func TestTheGetCatalogToolRefusesAnUnreachableDeployment(t *testing.T) {
	// Stood up and immediately closed: an address nothing answers at, without
	// depending on any particular port being free or reserved.
	dead := httptest.NewServer(http.HandlerFunc(func(http.ResponseWriter, *http.Request) {}))
	address := dead.URL
	dead.Close()

	posture := defaultLocalRunPosture()
	session := connectMCPWithDeps(t, posture, func() flowstatev1connect.WorkflowServiceClient {
		return newWorkflowServiceClient(serverFlags{address: address})
	}, flowmcp.Deps{
		Redact:               func(r *v1.GetResponse) *v1.GetResponse { return r },
		RemoteCatalogAddress: address,
	})

	result, err := session.CallTool(t.Context(), &mcp.CallToolParams{
		Name:      flowmcp.ToolName("GetCatalog"),
		Arguments: map[string]any{},
	})
	require.NoError(t, err)
	require.True(t, result.IsError,
		"an unreachable deployment must be refused, not answered from this binary's own build")

	text := result.Content[0].(*mcp.TextContent).Text
	assert.Contains(t, text, address,
		"the refusal does not name which deployment was asked: %s", text)
}

// TestRemoteCatalogAddressForRespectsExplicitAddress pins the decision
// [remoteCatalogAddressFor] makes: local by default (nothing else stood up
// works, per CLAUDE.md's "a capability is not done until it is reachable"),
// remote only once an operator has named a deployment either way --address
// can be named.
func TestRemoteCatalogAddressForRespectsExplicitAddress(t *testing.T) {
	newCmd := func() *cobra.Command {
		cmd := &cobra.Command{Use: "mcp"}
		addServerFlags(cmd)
		return cmd
	}

	t.Run("default", func(t *testing.T) {
		cmd := newCmd()
		require.NoError(t, cmd.ParseFlags(nil))
		assert.Equal(t, "", remoteCatalogAddressFor(cmd, serverFlagsOf(cmd)),
			"with nothing configured, GetCatalog must answer locally")
	})

	t.Run("explicit flag", func(t *testing.T) {
		cmd := newCmd()
		require.NoError(t, cmd.ParseFlags([]string{"--address", "deploy.example:9233"}))
		assert.Equal(t, "deploy.example:9233", remoteCatalogAddressFor(cmd, serverFlagsOf(cmd)),
			"an explicit --address must be dispatched to")
	})

	t.Run("environment variable", func(t *testing.T) {
		t.Setenv("FLOWSTATE_ADDRESS", "deploy.example:9233")
		cmd := newCmd()
		require.NoError(t, cmd.ParseFlags(nil))
		assert.Equal(t, "deploy.example:9233", remoteCatalogAddressFor(cmd, serverFlagsOf(cmd)),
			"FLOWSTATE_ADDRESS names a deployment exactly as --address does")
	})
}
