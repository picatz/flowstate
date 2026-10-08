package main

import (
	"context"
	"net/http"
	"net/http/httptest"
	"os"
	"testing"

	"connectrpc.com/connect"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/encoding/protojson"

	v1 "github.com/picatz/flowstate/pkg/flowstate/v1"
	"github.com/picatz/flowstate/pkg/flowstate/v1/flowstatev1connect"
)

// catalogServer answers GetCatalog with a fixed plugin catalog and refuses every
// submission, so a pass is never mistaken for a run.
type catalogServer struct {
	flowstatev1connect.UnimplementedWorkflowServiceHandler

	plugins *v1.PluginCatalog
}

func (s catalogServer) GetCatalog(context.Context, *connect.Request[v1.GetCatalogRequest]) (*connect.Response[v1.GetCatalogResponse], error) {
	return connect.NewResponse(&v1.GetCatalogResponse{Catalog: v1.Catalog(), Plugins: s.plugins}), nil
}

func (catalogServer) Run(context.Context, *connect.Request[v1.RunRequest]) (*connect.Response[v1.RunResponse], error) {
	return nil, connect.NewError(connect.CodeInvalidArgument, assert.AnError)
}

// TestRunValidatesAgainstTheDeploymentsPluginCatalog is #1548: `flow run`
// submits to a deployment, so the plugin tasks that deployment advertises are
// tasks the client accepts, with no plugin flag and no plugin on the machine.
// A deployment that lacks the plugin leaves the client's refusal as it was.
func TestRunValidatesAgainstTheDeploymentsPluginCatalog(t *testing.T) {
	bin := buildFlowBinary(t)

	raw, err := os.ReadFile(pluginCatalogFor(t, bin))
	require.NoError(t, err)

	var advertised v1.PluginCatalog
	require.NoError(t, protojson.Unmarshal(raw, &advertised))

	serve := func(t *testing.T, plugins *v1.PluginCatalog) string {
		t.Helper()

		mux := http.NewServeMux()
		mux.Handle(flowstatev1connect.NewWorkflowServiceHandler(catalogServer{plugins: plugins}))
		server := httptest.NewServer(mux)
		t.Cleanup(server.Close)

		return server.URL
	}

	t.Run("the deployment has the plugin", func(t *testing.T) {
		output, err := runFlowCapturing(t, bin, "run", exampleGreetWorkflow, "--address", serve(t, &advertised))
		require.Error(t, err, "the stub server refuses every submission, so a pass is a mistake:\n%s", output)
		assert.NotContains(t, output, "no plugin task",
			"the client refused a plugin task the deployment advertises:\n%s", output)
		assert.Contains(t, output, "starting plugin-greeting",
			"the file never reached the server:\n%s", output)
	})

	t.Run("the deployment lacks it", func(t *testing.T) {
		output, err := runFlowCapturing(t, bin, "run", exampleGreetWorkflow, "--address", serve(t, nil))
		require.Error(t, err)
		assert.Contains(t, output, "no plugin task",
			"a plugin the deployment does not have was accepted:\n%s", output)
	})
}
