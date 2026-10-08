package main

import (
	"context"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
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
		assert.Contains(t, output, "checked against the deployment's catalog (1 plugin(s))",
			"the answer does not name the deployment as its source:\n%s", output)
		assert.NotContains(t, output, "no plugin task",
			"the client refused a plugin task the deployment advertises:\n%s", output)
		assert.Contains(t, output, "starting plugin-greeting",
			"the file never reached the server:\n%s", output)
	})

	t.Run("the deployment lacks it", func(t *testing.T) {
		output, err := runFlowCapturing(t, bin, "run", exampleGreetWorkflow, "--address", serve(t, nil))
		require.Error(t, err)
		assert.Contains(t, output, "checked against the deployment's catalog (0 plugin(s))",
			"the refusal does not name the deployment as its source:\n%s", output)
		assert.Contains(t, output, "no plugin task",
			"a plugin the deployment does not have was accepted:\n%s", output)
	})
}

// TestRunSubmitsACompiledSpecification is the compile-then-submit path of
// #1548: what `flow compile --output json` wrote is what `flow run --spec`
// submits, with no plugin flag and no catalog fetch, and a file that is not a
// specification is refused naming `flow compile`.
func TestRunSubmitsACompiledSpecification(t *testing.T) {
	bin := buildFlowBinary(t)
	catalog := pluginCatalogFor(t, bin)

	compiled, err := runFlowCapturing(t, bin, "compile", "--"+pluginCatalogFlag, catalog, "--output", "json", exampleGreetWorkflow)
	require.NoError(t, err, "compiling the example:\n%s", compiled)

	specPath := filepath.Join(t.TempDir(), "spec.json")
	require.NoError(t, os.WriteFile(specPath, []byte(compiled), 0o600))

	mux := http.NewServeMux()
	mux.Handle(flowstatev1connect.NewWorkflowServiceHandler(catalogServer{}))
	server := httptest.NewServer(mux)
	t.Cleanup(server.Close)

	t.Run("a compiled specification reaches the server", func(t *testing.T) {
		output, err := runFlowCapturing(t, bin, "run", "--spec", specPath, "--address", server.URL)
		require.Error(t, err, "the stub server refuses every submission:\n%s", output)
		assert.Contains(t, output, "starting plugin-greeting",
			"the specification never reached the server:\n%s", output)
	})

	t.Run("a Flowfile is not a specification", func(t *testing.T) {
		output, err := runFlowCapturing(t, bin, "run", "--spec", exampleGreetWorkflow, "--address", server.URL)
		require.Error(t, err)
		assert.Contains(t, output, "not a compiled specification",
			"a Flowfile passed as a specification was not named as one:\n%s", output)
		assert.NotContains(t, output, "starting plugin-greeting")
	})
}
