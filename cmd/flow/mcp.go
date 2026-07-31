package main

import (
	"context"
	"fmt"
	"strings"

	"connectrpc.com/connect"
	"github.com/modelcontextprotocol/go-sdk/mcp"
	"github.com/spf13/cobra"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protoreflect"
	"google.golang.org/protobuf/reflect/protoregistry"

	v1 "github.com/picatz/flowstate/pkg/flowstate/v1"
	"github.com/picatz/flowstate/pkg/flowstate/v1/flowstatev1connect"
	"github.com/picatz/flowstate/pkg/flowstate/v1/server"
)

// The agent surface is the machine surface, taken seriously.
//
// `flow mcp` serves the control plane to a model the way `--output json` serves
// it to a pipe: the same schema messages, projected. Every WorkflowService RPC
// becomes one tool, discovered by walking the service descriptor rather than
// kept in a list — so an RPC added to the schema is a tool the day the code is
// regenerated, and there is no tool list to fall behind the engine. docs/DSL.md
// wrote this down as a rule before the surface existed: MCP is generated, not
// written.
//
// Two tools answer locally and the rest speak to a server, split by what the
// method needs. Validate and GetCatalog touch no run and no tenant — the
// server's own handlers take a nil Temporal client, which is the proof — so
// they run in-process and an agent gets a working authoring loop with nothing
// else stood up. The lifecycle verbs address durable runs, which only a server
// has; without --address they explain that rather than failing opaquely.

// mcpToolPrefix namespaces the tools, since a client may aggregate servers.
const mcpToolPrefix = "flowstate_"

// mcpDescriptions is the one hand-written thing on this surface: a sentence per
// RPC for the model to choose tools by.
//
// Descriptions are prose for a reader, which is the one thing a descriptor does
// not carry at runtime — protoc-gen-go strips source comments, so deriving
// these would mean shipping a descriptor set alongside the binary for a
// sentence each. Hand-written is acceptable exactly because the *set* is not:
// TestEveryToolHasADescription holds this map to the service descriptor in both
// directions, so a method added without a sentence fails the build rather than
// shipping mute.
var mcpDescriptions = map[string]string{
	"Run":       "Submit a compiled workflow specification to run durably. Returns ids to watch it by; it does not wait. Compile a Flowfile first with flowstate_validate to check it.",
	"Get":       "Report a run's status, timing, current position, and its outputs once finished.",
	"Signal":    "Deliver a named signal to a run waiting for one — how an approval reaches a workload.",
	"List":      "List the caller's runs, paged. A short or empty page with a nextPageToken is not the end of the listing; keep paging.",
	"Cancel":    "Ask a run to stop, letting it clean up on the way out.",
	"Terminate": "Stop a run immediately, running none of its cleanup. Prefer cancel.",
	"Validate":  "Check Flowfile YAML sources and report positioned diagnostics without executing anything. Pure and safe to loop on; answers locally, no server needed.",
	"GetCatalog": "What this build can execute: every task with its typed inputs and outputs, and every CEL function an expression may call. " +
		"Read this before writing a Flowfile. Answers locally, no server needed.",
}

// runMCP implements the mcp sub-command.
func runMCP(cmd *cobra.Command, args []string) error {
	flags := serverFlagsOf(cmd)

	// Constructed lazily and at most once, so the local tools never dial and the
	// remote ones share a client.
	var remote flowstatev1connect.WorkflowServiceClient
	remoteClient := func() flowstatev1connect.WorkflowServiceClient {
		if remote == nil {
			remote = newWorkflowServiceClient(flags)
		}

		return remote
	}

	// The local half is the server's own handlers over no Temporal client: one
	// implementation of Validate, whoever calls it. See server/validate.go for
	// why a nil client is safe for exactly these two methods.
	local := server.New(nil)

	srv := mcp.NewServer(&mcp.Implementation{Name: "flowstate", Version: version}, nil)

	return serveMCPTools(cmd.Context(), srv, local, remoteClient)
}

// serveMCPTools registers one tool per RPC and runs the server on stdio.
func serveMCPTools(
	ctx context.Context,
	srv *mcp.Server,
	local *server.FlowstateServer,
	remote func() flowstatev1connect.WorkflowServiceClient,
) error {
	addMCPTools(srv, local, remote)

	return srv.Run(ctx, &mcp.StdioTransport{})
}

// addMCPTools is the one registration, shared with the tests so what they
// exercise is what an agent connects to — two registration sites would be the
// two-copies defect this repository keeps refinding, on a new surface.
func addMCPTools(
	srv *mcp.Server,
	local *server.FlowstateServer,
	remote func() flowstatev1connect.WorkflowServiceClient,
) {
	for _, method := range workflowServiceMethods() {
		srv.AddTool(&mcp.Tool{
			Name:        mcpToolName(method.name),
			Description: mcpDescriptions[method.name],
			InputSchema: schemaForMessage(method.input),
		}, mcpHandler(method, local, remote))
	}
}

// mcpToolName renders an RPC name as a tool name: GetCatalog becomes
// flowstate_get_catalog, which is the casing MCP tools conventionally use.
func mcpToolName(rpc string) string {
	var b strings.Builder
	b.WriteString(mcpToolPrefix)
	for i, r := range rpc {
		if r >= 'A' && r <= 'Z' {
			if i > 0 {
				b.WriteByte('_')
			}
			r += 'a' - 'A'
		}
		b.WriteRune(r)
	}

	return b.String()
}

// serviceMethod is one RPC, as the tool derivation needs it.
type serviceMethod struct {
	name  string
	input protoreflect.MessageDescriptor
	call  func(ctx context.Context, local *server.FlowstateServer,
		remote func() flowstatev1connect.WorkflowServiceClient, in proto.Message) (proto.Message, error)
}

// workflowServiceMethods enumerates the service.
//
// The names and shapes come from the descriptor — asserted against it by test,
// in both directions — while the dispatch is written out, because Go generics
// cannot rank over connect's typed methods without reflection that would cost
// more clarity than these lines do. A method added to the service without a row
// here fails TestEveryRPCIsATool.
func workflowServiceMethods() []serviceMethod {
	return []serviceMethod{
		{
			name:  "Validate",
			input: (&v1.ValidateRequest{}).ProtoReflect().Descriptor(),
			call: func(ctx context.Context, local *server.FlowstateServer, _ func() flowstatev1connect.WorkflowServiceClient, in proto.Message) (proto.Message, error) {
				resp, err := local.Validate(ctx, connect.NewRequest(in.(*v1.ValidateRequest)))
				if err != nil {
					return nil, err
				}

				return resp.Msg, nil
			},
		},
		{
			name:  "GetCatalog",
			input: (&v1.GetCatalogRequest{}).ProtoReflect().Descriptor(),
			call: func(ctx context.Context, local *server.FlowstateServer, _ func() flowstatev1connect.WorkflowServiceClient, in proto.Message) (proto.Message, error) {
				resp, err := local.GetCatalog(ctx, connect.NewRequest(in.(*v1.GetCatalogRequest)))
				if err != nil {
					return nil, err
				}

				return resp.Msg, nil
			},
		},
		{
			name:  "Run",
			input: (&v1.RunRequest{}).ProtoReflect().Descriptor(),
			call: func(ctx context.Context, _ *server.FlowstateServer, remote func() flowstatev1connect.WorkflowServiceClient, in proto.Message) (proto.Message, error) {
				resp, err := remote().Run(ctx, connect.NewRequest(in.(*v1.RunRequest)))
				if err != nil {
					return nil, err
				}

				return resp.Msg, nil
			},
		},
		{
			name:  "Get",
			input: (&v1.GetRequest{}).ProtoReflect().Descriptor(),
			call: func(ctx context.Context, _ *server.FlowstateServer, remote func() flowstatev1connect.WorkflowServiceClient, in proto.Message) (proto.Message, error) {
				resp, err := remote().Get(ctx, connect.NewRequest(in.(*v1.GetRequest)))
				if err != nil {
					return nil, err
				}

				return resp.Msg, nil
			},
		},
		{
			name:  "Signal",
			input: (&v1.SignalRequest{}).ProtoReflect().Descriptor(),
			call: func(ctx context.Context, _ *server.FlowstateServer, remote func() flowstatev1connect.WorkflowServiceClient, in proto.Message) (proto.Message, error) {
				resp, err := remote().Signal(ctx, connect.NewRequest(in.(*v1.SignalRequest)))
				if err != nil {
					return nil, err
				}

				return resp.Msg, nil
			},
		},
		{
			name:  "List",
			input: (&v1.ListRequest{}).ProtoReflect().Descriptor(),
			call: func(ctx context.Context, _ *server.FlowstateServer, remote func() flowstatev1connect.WorkflowServiceClient, in proto.Message) (proto.Message, error) {
				resp, err := remote().List(ctx, connect.NewRequest(in.(*v1.ListRequest)))
				if err != nil {
					return nil, err
				}

				return resp.Msg, nil
			},
		},
		{
			name:  "Cancel",
			input: (&v1.CancelRequest{}).ProtoReflect().Descriptor(),
			call: func(ctx context.Context, _ *server.FlowstateServer, remote func() flowstatev1connect.WorkflowServiceClient, in proto.Message) (proto.Message, error) {
				resp, err := remote().Cancel(ctx, connect.NewRequest(in.(*v1.CancelRequest)))
				if err != nil {
					return nil, err
				}

				return resp.Msg, nil
			},
		},
		{
			name:  "Terminate",
			input: (&v1.TerminateRequest{}).ProtoReflect().Descriptor(),
			call: func(ctx context.Context, _ *server.FlowstateServer, remote func() flowstatev1connect.WorkflowServiceClient, in proto.Message) (proto.Message, error) {
				resp, err := remote().Terminate(ctx, connect.NewRequest(in.(*v1.TerminateRequest)))
				if err != nil {
					return nil, err
				}

				return resp.Msg, nil
			},
		},
	}
}

// mcpHandler adapts one RPC into a tool handler.
//
// Arguments arrive as JSON and leave as protojson of the response message —
// the same bytes `--output json` prints, from the same schema, which is what
// keeps this surface from being a second dialect.
func mcpHandler(
	method serviceMethod,
	local *server.FlowstateServer,
	remote func() flowstatev1connect.WorkflowServiceClient,
) mcp.ToolHandler {
	return func(ctx context.Context, req *mcp.CallToolRequest) (*mcp.CallToolResult, error) {
		in := newMessage(method.input)

		// DiscardUnknown stays false on purpose: the schema advertised
		// additionalProperties false, and honouring a field the schema does not
		// have would make the tool "work" while doing something other than what
		// was asked.
		if raw := req.Params.Arguments; len(raw) > 0 {
			if err := protojson.Unmarshal(raw, in); err != nil {
				return toolError(fmt.Errorf("arguments do not match %s: %w", method.input.FullName(), err)), nil
			}
		}

		out, err := method.call(ctx, local, remote, in)
		if err != nil {
			return toolError(err), nil
		}

		encoded, err := protojson.MarshalOptions{EmitUnpopulated: true}.Marshal(out)
		if err != nil {
			return toolError(err), nil
		}

		return &mcp.CallToolResult{
			Content: []mcp.Content{&mcp.TextContent{Text: string(encoded)}},
		}, nil
	}
}

// toolError reports a failure as the tool's result rather than a protocol
// error, which is what lets a model read the reason and correct itself.
func toolError(err error) *mcp.CallToolResult {
	return &mcp.CallToolResult{
		IsError: true,
		Content: []mcp.Content{&mcp.TextContent{Text: err.Error()}},
	}
}

// newMessage constructs an empty message for a descriptor.
func newMessage(md protoreflect.MessageDescriptor) proto.Message {
	mt, err := protoregistry.GlobalTypes.FindMessageByName(md.FullName())
	if err != nil {
		// Unreachable for the compiled-in schema; loud if it ever is not.
		panic("flow mcp: no type for " + string(md.FullName()))
	}

	return mt.New().Interface()
}
