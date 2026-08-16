package server

import (
	"context"
	"testing"

	"connectrpc.com/connect"
	"github.com/stretchr/testify/require"

	v1 "github.com/picatz/flowstate/pkg/flowstate/v1"
)

// The submission side of the plugin contract: every path that brings durable
// work into existence pins it, and pins it against the deployment rather than
// against anything the caller said.
//
// Internal because the interesting assertions are about the shared function and
// about `CreateSchedule` refusing before it has spoken to Temporal at all, which
// is what lets this run without one.

func testCatalog(plugins ...*v1.PluginDescription) *v1.PluginCatalog {
	return &v1.PluginCatalog{Plugins: plugins}
}

func installedPlugin(name, version string) *v1.PluginDescription {
	return &v1.PluginDescription{
		Name:               name,
		Version:            version,
		ProtocolVersion:    2,
		TaskSchemaDigest:   "sha256:schema",
		DistributionDigest: "sha256:binary",
		ClaimsDigest:       "sha256:claims",
	}
}

// requiringWorkflow is a runnable workflow that needs one plugin.
func requiringWorkflow(name, minimum string) *v1.Workflow {
	return &v1.Workflow{
		Name:               "needs-" + name,
		PluginRequirements: []*v1.PluginRequirement{{Name: name, MinimumVersion: minimum}},
		Steps: []*v1.Node{{
			Id: "say",
			Kind: &v1.Node_Task{Task: &v1.Task{
				Name:   "log",
				Inputs: map[string]*v1.Value{"message": v1.NewLiteral("hello")},
			}},
		}},
	}
}

// TestSubmissionIsPinnedAgainstTheDeployment covers the path `Run` and
// `SignalWithStart` share.
func TestSubmissionIsPinnedAgainstTheDeployment(t *testing.T) {
	t.Parallel()

	s := &FlowstateServer{pluginCatalog: testCatalog(installedPlugin("slack", "v2.1.0"))}

	wf := requiringWorkflow("slack", "v2.0.0")
	_, err := s.validateSubmission(wf, nil)
	require.NoError(t, err)
	require.Len(t, wf.GetResolvedPlugins(), 1)
	require.Equal(t, "sha256:binary", wf.GetResolvedPlugins()[0].GetDistributionDigest())
}

// TestSubmissionRefusesAPluginTheDeploymentLacks is the negative direction, and
// it is also the case that was broken for every server: with no catalog
// installed, this refusal is what a deployment holding the plugin used to
// produce.
func TestSubmissionRefusesAPluginTheDeploymentLacks(t *testing.T) {
	t.Parallel()

	for _, tc := range []struct {
		name    string
		catalog *v1.PluginCatalog
		want    string
	}{
		{"no plugins configured", nil, "not installed"},
		{"a different plugin", testCatalog(installedPlugin("pagerduty", "v1.0.0")), "not installed"},
		{"too old", testCatalog(installedPlugin("slack", "v2.0.0")), "below the v2.5.0"},
		{"the wrong major", testCatalog(installedPlugin("slack", "v3.0.0")), "different contract"},
		{"the wrong major, downwards", testCatalog(installedPlugin("slack", "v1.0.0")), "different contract"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			// v2.5.0 rather than v2.0.0 so the "too old" case is a floor that is
			// not met rather than a major mismatch, which is a different refusal.
			s := &FlowstateServer{pluginCatalog: tc.catalog}
			_, err := s.validateSubmission(requiringWorkflow("slack", "v2.5.0"), nil)

			require.ErrorContains(t, err, tc.want)
			require.Equal(t, connect.CodeInvalidArgument, connect.CodeOf(err),
				"an unsatisfiable requirement is the caller's to fix")
		})
	}
}

// TestSubmissionDiscardsACallerSuppliedSelection is the forged-tuple direction at
// the boundary a client actually reaches.
//
// `resolved_plugins` is documented as the control plane's own selection. A client
// can set it on the wire, so the server has to overwrite it, including for a
// specification that requires nothing, which is where a conditional resolution
// left it untouched.
func TestSubmissionDiscardsACallerSuppliedSelection(t *testing.T) {
	t.Parallel()

	s := &FlowstateServer{pluginCatalog: testCatalog(installedPlugin("slack", "v2.1.0"))}

	forged := []*v1.ResolvedPlugin{{
		Name: "slack", Version: "v2.1.0", ProtocolVersion: 99,
		TaskSchemaDigest: "sha256:forged", DistributionDigest: "sha256:forged",
	}}

	t.Run("with no requirements to resolve", func(t *testing.T) {
		wf := requiringWorkflow("slack", "v2.0.0")
		wf.PluginRequirements = nil
		wf.ResolvedPlugins = forged

		_, err := s.validateSubmission(wf, nil)
		require.NoError(t, err)
		require.Empty(t, wf.GetResolvedPlugins(), "the caller's selection survived a submission")
	})

	t.Run("with a requirement of its own", func(t *testing.T) {
		wf := requiringWorkflow("slack", "v2.0.0")
		wf.ResolvedPlugins = forged

		_, err := s.validateSubmission(wf, nil)
		require.NoError(t, err)
		require.Equal(t, "sha256:binary", wf.GetResolvedPlugins()[0].GetDistributionDigest())
		require.Equal(t, uint32(2), wf.GetResolvedPlugins()[0].GetProtocolVersion())
	})
}

// TestCreateScheduleResolvesPluginsBeforeTemporal covers the third creation path,
// which had its own submission pipeline and never pinned anything.
//
// The server here has no Temporal client at all, which is the point: the refusal
// has to come from the pinning step, before the handler has gone anywhere. A
// schedule that persisted unpinned would fire at three in the morning against
// whichever plugin happened to be present.
func TestCreateScheduleResolvesPluginsBeforeTemporal(t *testing.T) {
	t.Parallel()

	s := &FlowstateServer{pluginCatalog: testCatalog(installedPlugin("pagerduty", "v1.0.0"))}

	wf := requiringWorkflow("slack", "v2.0.0")
	wf.Triggers = &v1.Triggers{Schedule: &v1.ScheduleTrigger{Cron: []string{"0 * * * *"}}}

	_, err := s.CreateSchedule(context.Background(), connect.NewRequest(&v1.CreateScheduleRequest{
		Name:     "nightly",
		Workflow: wf,
	}))

	require.ErrorContains(t, err, `required plugin "slack" is not installed`)
	require.Equal(t, connect.CodeInvalidArgument, connect.CodeOf(err))
}

// TestGetCatalogReportsTheDeploymentsPlugins closes the loop for a client: what
// it may require is what a submission requiring it will resolve against.
func TestGetCatalogReportsTheDeploymentsPlugins(t *testing.T) {
	t.Parallel()

	installed := installedPlugin("slack", "v2.1.0")
	s := &FlowstateServer{pluginCatalog: testCatalog(installed)}

	resp, err := s.GetCatalog(context.Background(), connect.NewRequest(&v1.GetCatalogRequest{}))
	require.NoError(t, err)
	require.Len(t, resp.Msg.GetPlugins().GetPlugins(), 1)
	require.Equal(t, "slack", resp.Msg.GetPlugins().GetPlugins()[0].GetName())
	require.Equal(t, "sha256:binary", resp.Msg.GetPlugins().GetPlugins()[0].GetDistributionDigest())

	// A copy, so a client's response cannot be a handle on what the server pins
	// every later submission against.
	resp.Msg.GetPlugins().GetPlugins()[0].Name = "impostor"
	require.Equal(t, "slack", s.pluginCatalog.GetPlugins()[0].GetName())

	// And a deployment without plugins says so rather than omitting the field's
	// meaning: nothing may be required here.
	stock := &FlowstateServer{}
	resp, err = stock.GetCatalog(context.Background(), connect.NewRequest(&v1.GetCatalogRequest{}))
	require.NoError(t, err)
	require.Empty(t, resp.Msg.GetPlugins().GetPlugins())
	require.NotNil(t, resp.Msg.GetCatalog(), "the built-in catalog is still answered")
}
