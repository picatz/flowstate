package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
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

// sensitiveSubjectWorkflow declares a `sensitive:` input beside a signal gate
// (a predicate may not read a sensitive input, so it names the starter instead).
// The refusals below are a server's, quoting that input in
// its own words: redacting what a server quotes is the CLI's and the MCP
// tools' job whatever the server was refusing, and the fixture is what gives
// the argument something to be redacted from.
const sensitiveSubjectWorkflow = `edition: v2026.4
name: sensitive-subject
inputs:
  approver:
    type: string
    required: true
    sensitive: true
signals:
  approved:
    allow: ${sender.identity.principal != run.identity.principal}
steps:
  - id: wait
    wait_for_signal:
      name: approved
      timeout: 1s
`

// sensitiveSubject is the argument neither surface may quote: it has a quote in
// it so a refusal's `%q` spelling differs from the raw one.
const sensitiveSubject = `approver-"lead"@corp.example`

// subjectResolvingServer refuses a submission with InvalidArgument, quoting the
// argument it was sent the way a server's own detail can.
type subjectResolvingServer struct {
	flowstatev1connect.UnimplementedWorkflowServiceHandler
}

func (subjectResolvingServer) refuse(_ context.Context, _ *v1.Workflow, inputs map[string]*v1.Value) error {
	approver := inputs["approver"].GetLiteral().GetStringValue()

	return connect.NewError(connect.CodeInvalidArgument,
		fmt.Errorf("cannot admit approver %q: want an <issuer>#<subject> principal", approver))
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
// driver's submitting commands: the server's refusal quotes the argument, and `flow run` and `flow schedule create` print it as
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
// tools that submit a run: the server's refusal of the submission quotes the
// argument, and the tool result withholds it as `flow
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

	const shortSensitive = `edition: v2026.4
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
