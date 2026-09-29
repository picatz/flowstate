package main

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"

	"connectrpc.com/connect"
	"github.com/modelcontextprotocol/go-sdk/mcp"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"

	flowmcp "github.com/picatz/flowstate/cmd/flow/internal/mcp"
	v1 "github.com/picatz/flowstate/pkg/flowstate/v1"
	"github.com/picatz/flowstate/pkg/flowstate/v1/flowfile"
	"github.com/picatz/flowstate/pkg/flowstate/v1/flowstatev1connect"
)

// sensitiveSubjectWorkflow gates a signal on a subject read from a
// `sensitive:` input, so an argument that is not `<issuer>#<subject>` is
// refused before the run starts, in a sentence quoting what it resolved to.
const sensitiveSubjectWorkflow = `edition: v2026.3
name: sensitive-subject
inputs:
  approver:
    type: string
    required: true
    sensitive: true
signals:
  approved:
    distinct_from_starter: true
    allow:
      - subject: ${inputs.approver}
steps:
  - id: wait
    wait_for_signal:
      name: approved
      timeout: 1s
`

// sensitiveSubject is the argument neither surface may quote: a bare subject,
// with a quote in it so the refusal's `%q` spelling differs from the raw one.
const sensitiveSubject = `approver-"lead"@corp.example`

// TestASubjectRefusalWithholdsASensitiveInput is #2100 on `flow run local`:
// the refusal still says what was wrong, without the value.
func TestASubjectRefusalWithholdsASensitiveInput(t *testing.T) {
	// Not t.Parallel(): runLocal shares the process-wide registry, as the
	// other run-local tests say.
	_, stderr, err := runLocal(t, sensitiveSubjectWorkflow, "--input", "approver="+sensitiveSubject)
	require.Error(t, err, "a bare subject is refused")

	for _, text := range []string{err.Error(), stderr} {
		assert.NotContains(t, text, "lead", "the sensitive input reached the refusal")
	}
	assert.Contains(t, err.Error(), v1.SensitiveMarker, "a redaction, not a withholding")
	assert.Contains(t, err.Error(), "<issuer>#<subject>", "the refusal no longer says what was wrong")

	_, _, revealed := runLocal(t, sensitiveSubjectWorkflow, "--input", "approver="+sensitiveSubject, "--reveal-sensitive")
	require.Error(t, revealed)
	assert.Contains(t, revealed.Error(), "lead", "--reveal-sensitive did not show the value")
}

// TestTheRunLocalToolSubjectRefusalWithholdsASensitiveInput is #2100 on the
// MCP `flowstate_run_local` tool.
func TestTheRunLocalToolSubjectRefusalWithholdsASensitiveInput(t *testing.T) {
	t.Parallel()

	result, _ := callRunLocal(t, connectMCP(t, defaultLocalRunPosture()), map[string]any{
		"source": sensitiveSubjectWorkflow,
		"inputs": map[string]any{"approver": sensitiveSubject},
	})
	require.True(t, result.IsError, "a bare subject is refused")

	text := result.Content[0].(*mcp.TextContent).Text
	assert.NotContains(t, text, "lead", "the sensitive input reached the tool result")
	assert.Contains(t, text, v1.SensitiveMarker, "a redaction, not a withholding")
	assert.Contains(t, text, "<issuer>#<subject>", "the refusal no longer says what was wrong")
}

// subjectResolvingServer refuses a submission the way the server does when a
// gate's `subject:` does not resolve to `<issuer>#<subject>`: InvalidArgument,
// quoting the resolver's own refusal.
type subjectResolvingServer struct {
	flowstatev1connect.UnimplementedWorkflowServiceHandler
}

func (subjectResolvingServer) refuse(ctx context.Context, workflow *v1.Workflow, inputs map[string]*v1.Value) error {
	bound, err := v1.BindRunInputs(workflow, inputs)
	if err != nil {
		return connect.NewError(connect.CodeInvalidArgument, err)
	}
	if _, err := v1.ResolveSignalPolicySubjects(ctx, workflow, bound); err != nil {
		return connect.NewError(connect.CodeInvalidArgument, err)
	}

	return connect.NewError(connect.CodeUnimplemented, nil)
}

func (s subjectResolvingServer) Run(ctx context.Context, req *connect.Request[v1.RunRequest]) (*connect.Response[v1.RunResponse], error) {
	return nil, s.refuse(ctx, req.Msg.GetWorkflow(), req.Msg.GetInputs())
}

func (s subjectResolvingServer) SignalWithStart(ctx context.Context, req *connect.Request[v1.SignalWithStartRequest]) (*connect.Response[v1.SignalWithStartResponse], error) {
	return nil, s.refuse(ctx, req.Msg.GetWorkflow(), req.Msg.GetInputs())
}

func (s subjectResolvingServer) CreateSchedule(ctx context.Context, req *connect.Request[v1.CreateScheduleRequest]) (*connect.Response[v1.CreateScheduleResponse], error) {
	return nil, s.refuse(ctx, req.Msg.GetWorkflow(), req.Msg.GetInputs())
}

// TestADurableSubjectRefusalWithholdsASensitiveInput is #2100 on the durable
// driver's submitting commands: the server's refusal quotes what the gate's
// subject resolved to, and `flow run` and `flow schedule create` print it as
// `flow run local` does.
func TestADurableSubjectRefusalWithholdsASensitiveInput(t *testing.T) {
	t.Parallel()

	mux := http.NewServeMux()
	mux.Handle(flowstatev1connect.NewWorkflowServiceHandler(subjectResolvingServer{}))
	server := httptest.NewServer(mux)
	t.Cleanup(server.Close)

	scheduled := sensitiveSubjectWorkflow + `triggers:
  schedule:
    every: 1h
`
	for name, command := range map[string][]string{
		"flow run":             {"run", writeWorkflowFile(t, sensitiveSubjectWorkflow)},
		"flow schedule create": {"schedule", "create", writeWorkflowFile(t, scheduled)},
	} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()

			res := runFlow(t, append(command, "--input", "approver="+sensitiveSubject, "--address", server.URL)...)
			require.Error(t, res.Err)
			require.Contains(t, res.Err.Error(), "<issuer>#<subject>", "the server did not refuse the subject, so this proves nothing")
			assert.NotContains(t, res.Output(), "lead", "the sensitive input reached the output")
			assert.NotContains(t, res.Err.Error(), "lead", "the sensitive input reached the returned error")
		})
	}
}

// TestTheRunToolSubjectRefusalWithholdsASensitiveInput is #2100 on the MCP
// tools that submit a run: the server's refusal of the submission quotes what
// the gate's subject resolved to, and the tool result withholds it as `flow
// run` does, against the workflow and arguments the request carried.
func TestTheRunToolSubjectRefusalWithholdsASensitiveInput(t *testing.T) {
	t.Parallel()

	mux := http.NewServeMux()
	mux.Handle(flowstatev1connect.NewWorkflowServiceHandler(subjectResolvingServer{}))
	server := httptest.NewServer(mux)
	t.Cleanup(server.Close)

	workflow, err := flowfile.Unmarshal([]byte(sensitiveSubjectWorkflow))
	require.NoError(t, err)
	inputs := map[string]*v1.Value{"approver": v1.NewLiteral(sensitiveSubject)}
	for rpc, request := range map[string]proto.Message{
		"Run":             &v1.RunRequest{Workflow: workflow, Inputs: inputs},
		"SignalWithStart": &v1.SignalWithStartRequest{EntityKey: "order-1", Name: "approved", Workflow: workflow, Inputs: inputs},
	} {
		t.Run(rpc, func(t *testing.T) {
			t.Parallel()

			encoded, err := protojson.Marshal(request)
			require.NoError(t, err)
			var arguments map[string]any
			require.NoError(t, json.Unmarshal(encoded, &arguments))

			flags := serverFlags{address: server.URL}
			posture := defaultLocalRunPosture()
			session := connectMCPWithDeps(t, posture, func() flowstatev1connect.WorkflowServiceClient {
				return newWorkflowServiceClient(flags)
			}, flowmcp.Deps{
				Redact:           func(r *v1.GetResponse) *v1.GetResponse { return r },
				DecorateRPCError: mcpRPCErrorDecorator(posture, flags, true),
			})
			result, err := session.CallTool(t.Context(), &mcp.CallToolParams{Name: flowmcp.ToolName(rpc), Arguments: arguments})
			require.NoError(t, err)
			require.True(t, result.IsError, "the server refused the subject")

			text := result.Content[0].(*mcp.TextContent).Text
			require.Contains(t, text, "<issuer>#<subject>", "the server did not refuse the subject, so this proves nothing")
			assert.NotContains(t, text, "lead", "the sensitive input reached the tool result")
		})
	}
}

// TestAnUnreachableServerIsNamedWhateverASensitiveInputHolds: with no server
// to refuse anything, nothing quotes an argument, so the refusal that names
// the address it dialed is not redacted, even where a sensitive input's value
// is short enough to appear in that address.
func TestAnUnreachableServerIsNamedWhateverASensitiveInputHolds(t *testing.T) {
	t.Parallel()

	dead := httptest.NewServer(http.HandlerFunc(func(http.ResponseWriter, *http.Request) {}))
	address := dead.URL
	dead.Close()

	const shortSensitive = `edition: v2026.3
name: short-sensitive
inputs:
  pin:
    type: int
    sensitive: true
    default: 1
triggers:
  schedule:
    every: 1h
steps:
  - id: hi
    log:
      message: hi
`
	for name, command := range map[string][]string{
		"flow run":             {"run", writeWorkflowFile(t, shortSensitive)},
		"flow schedule create": {"schedule", "create", writeWorkflowFile(t, shortSensitive)},
	} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()

			res := runFlow(t, append(command, "--address", address)...)
			require.Error(t, res.Err)
			assert.Contains(t, res.Err.Error(), address, "the address dialed was redacted out of the refusal")
		})
	}
}

// busyServer answers a submission unavailable, with detail quoting the
// argument it was sent: a server's own answer, which connect spells like a
// failure to reach one.
type busyServer struct {
	flowstatev1connect.UnimplementedWorkflowServiceHandler
}

func (busyServer) Run(_ context.Context, req *connect.Request[v1.RunRequest]) (*connect.Response[v1.RunResponse], error) {
	approver := req.Msg.GetInputs()["approver"].GetLiteral().GetStringValue()

	return nil, connect.NewError(connect.CodeUnavailable, errors.New("admission is busy; retry "+approver+" later"))
}

// TestAnUnavailableAnswerFromAServerIsRedacted: unavailable is also what a
// server that answered can say, with detail of its own quoting an argument, so
// only a failure to reach one is left unredacted; `flow run` and the MCP
// `flowstate_run` tool redact a server's unavailable answer as any other.
func TestAnUnavailableAnswerFromAServerIsRedacted(t *testing.T) {
	t.Parallel()

	mux := http.NewServeMux()
	mux.Handle(flowstatev1connect.NewWorkflowServiceHandler(busyServer{}))
	server := httptest.NewServer(mux)
	t.Cleanup(server.Close)

	t.Run("flow run", func(t *testing.T) {
		t.Parallel()

		res := runFlow(t, "run", writeWorkflowFile(t, sensitiveSubjectWorkflow), "--input", "approver="+sensitiveSubject, "--address", server.URL)
		require.Error(t, res.Err)
		require.Contains(t, res.Err.Error(), "admission is busy", "the server's answer was not the one reported, so this proves nothing")
		assert.NotContains(t, res.Output(), "lead")
		assert.NotContains(t, res.Err.Error(), "lead", "a server's unavailable answer quoted a sensitive input")
	})

	t.Run("flowstate_run", func(t *testing.T) {
		t.Parallel()

		workflow, err := flowfile.Unmarshal([]byte(sensitiveSubjectWorkflow))
		require.NoError(t, err)
		encoded, err := protojson.Marshal(&v1.RunRequest{Workflow: workflow,
			Inputs: map[string]*v1.Value{"approver": v1.NewLiteral(sensitiveSubject)}})
		require.NoError(t, err)
		var arguments map[string]any
		require.NoError(t, json.Unmarshal(encoded, &arguments))

		flags := serverFlags{address: server.URL}
		posture := defaultLocalRunPosture()
		session := connectMCPWithDeps(t, posture, func() flowstatev1connect.WorkflowServiceClient {
			return newWorkflowServiceClient(flags)
		}, flowmcp.Deps{
			Redact:           func(r *v1.GetResponse) *v1.GetResponse { return r },
			DecorateRPCError: mcpRPCErrorDecorator(posture, flags, true),
		})
		result, err := session.CallTool(t.Context(), &mcp.CallToolParams{Name: flowmcp.ToolName("Run"), Arguments: arguments})
		require.NoError(t, err)
		require.True(t, result.IsError)

		text := result.Content[0].(*mcp.TextContent).Text
		require.Contains(t, text, "admission is busy", "the server's answer was not the one reported, so this proves nothing")
		assert.NotContains(t, text, "lead", "a server's unavailable answer quoted a sensitive input")
		assert.Contains(t, text, "--address/FLOWSTATE_ADDRESS", "a redacted unavailable answer lost the tool's remedy")
	})
}
