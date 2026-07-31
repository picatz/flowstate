package main

import (
	"encoding/json"
	"testing"

	"github.com/modelcontextprotocol/go-sdk/mcp"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/reflect/protoreflect"
	"google.golang.org/protobuf/reflect/protoregistry"

	"github.com/picatz/flowstate/pkg/flowstate/v1/flowstatev1connect"
	"github.com/picatz/flowstate/pkg/flowstate/v1/server"
)

// TestToolsMatchTheServiceDescriptor holds the tool list to the service
// descriptor, in both directions.
//
// The dispatch table in workflowServiceMethods is written out — Go cannot range
// over connect's typed methods — so what keeps it honest is this: an RPC added
// to the schema without a row here fails, and a row naming an RPC the schema no
// longer has fails. The same pattern as the README's command table, for the
// same reason: a hand-kept list is fine exactly as long as a test holds it to
// the source of truth.
func TestToolsMatchTheServiceDescriptor(t *testing.T) {
	t.Parallel()

	table := map[string]bool{}
	for _, m := range workflowServiceMethods() {
		require.False(t, table[m.name], "the dispatch table lists %q twice", m.name)
		table[m.name] = true

		// The schema each tool advertises is the schema of the RPC's own request
		// message; a row pointing at the wrong descriptor would advertise fields
		// the handler then refuses.
		require.NotNil(t, m.input, "%q has no input descriptor", m.name)
	}

	names := serviceMethodNames(t)
	require.NotEmpty(t, names, "the service declares no methods; the lookup is broken")

	for name := range names {
		assert.True(t, table[name],
			"the schema declares rpc %s and `flow mcp` serves no tool for it; add a row "+
				"to workflowServiceMethods", name)
	}
	for name := range table {
		assert.True(t, names[name],
			"the dispatch table lists %q, which the service no longer declares", name)
	}
}

// TestEveryToolHasADescription keeps the one hand-written map complete.
func TestEveryToolHasADescription(t *testing.T) {
	t.Parallel()

	names := serviceMethodNames(t)

	for name := range names {
		assert.NotEmpty(t, mcpDescriptions[name],
			"rpc %s has no description; a mute tool is one a model cannot choose", name)
	}
	for name := range mcpDescriptions {
		assert.True(t, names[name],
			"mcpDescriptions describes %q, which the service does not declare", name)
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

// TestTheValidateToolAnswersOverTheProtocol is the functional half: a real MCP
// client, over an in-memory transport, calling the tool an agent would call.
func TestTheValidateToolAnswersOverTheProtocol(t *testing.T) {
	t.Parallel()

	srv := mcp.NewServer(&mcp.Implementation{Name: "flowstate", Version: "test"}, nil)

	addMCPTools(srv, server.New(nil), func() flowstatev1connect.WorkflowServiceClient {
		t.Fatal("a local tool dialed the server")

		return nil
	})

	serverTransport, clientTransport := mcp.NewInMemoryTransports()

	serverDone := make(chan error, 1)
	go func() { serverDone <- srv.Run(t.Context(), serverTransport) }()

	client := mcp.NewClient(&mcp.Implementation{Name: "test", Version: "test"}, nil)
	session, err := client.Connect(t.Context(), clientTransport, nil)
	require.NoError(t, err)
	defer session.Close()

	result, err := session.CallTool(t.Context(), &mcp.CallToolParams{
		Name: mcpToolName("Validate"),
		Arguments: map[string]any{
			"files": []map[string]any{{
				"name": "broken.yaml",
				// base64 of an invalid Flowfile; SourceFile.source is bytes.
				"source": []byte("edition: v2026.2\nname: x\nsteps:\n  - id: a\n    nope:\n      x: y\n"),
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
