package server_test

import (
	"fmt"
	"testing"

	"connectrpc.com/connect"
	"github.com/stretchr/testify/require"
	"go.temporal.io/sdk/client"
	"go.temporal.io/sdk/testsuite"

	v1 "github.com/picatz/flowstate/pkg/flowstate/v1"
	"github.com/picatz/flowstate/pkg/flowstate/v1/server"
	"github.com/picatz/flowstate/pkg/flowstate/v1/temporalclient"
)

// mapper describes a tenancy mapping without building a trust policy.
//
// The three answers it can give are the three the pool's contract distinguishes,
// and confusing the last two is a silent tenancy failure rather than an error —
// which is why they are tested separately rather than together.
type mapper struct {
	// mapping is Flowstate namespace to Temporal namespace. Empty means this
	// deployment maps nothing.
	mapping map[string]string
}

func (m mapper) TemporalNamespace(namespace string) (string, bool, error) {
	if len(m.mapping) == 0 {
		// Maps nothing: the caller should use the configured namespace. Not an
		// error, and the difference matters.
		return "", false, nil
	}

	mapped, ok := m.mapping[namespace]
	if !ok {
		// Maps namespaces but not this one, and has no default. A configuration
		// gap, not a licence to put this tenant somewhere arbitrary.
		return "", false, fmt.Errorf("no Temporal namespace configured for %q", namespace)
	}

	return mapped, true, nil
}

func (m mapper) TemporalNamespaces() []string {
	namespaces := make([]string, 0, len(m.mapping))
	for _, mapped := range m.mapping {
		namespaces = append(namespaces, mapped)
	}
	return namespaces
}

// newPooledServer starts a dev server and returns a Flowstate server routing
// through a pool built over the given mapping.
func newPooledServer(t *testing.T, callerNamespace string, mapping map[string]string) *server.FlowstateServer {
	t.Helper()

	devServer, err := testsuite.StartDevServer(t.Context(), testsuite.DevServerOptions{
		ClientOptions: &client.Options{Logger: &testingLogger{t: t}},
	})
	require.NoError(t, err)
	t.Cleanup(func() { _ = devServer.Stop() })

	pool, err := temporalclient.NewPool(t.Context(), temporalclient.Config{
		Address:   devServer.FrontendHostPort(),
		Namespace: "default",
	}, mapper{mapping: mapping})
	require.NoError(t, err)
	t.Cleanup(pool.Close)

	return server.New(devServer.Client(),
		server.WithNamespace(callerNamespace),
		server.WithNamespacePool(pool),
	)
}

// TestUnroutableTenantIsRefusedNotRedirected is the test for the failure mode that
// would not announce itself.
//
// When a deployment maps namespaces and has no entry for this tenant, falling back
// to the configured client would place one tenant's runs in another tenant's
// namespace. They would start, they would execute, and nothing anywhere would say
// so. So the refusal is the feature, and this asserts the run is refused rather
// than accepted.
func TestUnroutableTenantIsRefusedNotRedirected(t *testing.T) {
	// The deployment maps team-a, and this caller is team-b.
	flowstate := newPooledServer(t, "team-b", map[string]string{"team-a": "default"})

	_, err := flowstate.Run(t.Context(), connect.NewRequest(&v1.RunRequest{
		Workflow: gatedWorkflow(),
	}))

	require.Error(t, err, "a run for an unroutable tenant was accepted; it went somewhere, and nothing said where")

	// A configuration gap an operator fixes, not an internal error and not the
	// caller's fault.
	require.Equal(t, connect.CodeFailedPrecondition, connect.CodeOf(err))
	require.Contains(t, err.Error(), "team-b",
		"the refusal does not name the tenant that could not be routed")

	// And it does not describe the deployment's tenancy to someone outside it.
	require.NotContains(t, err.Error(), "team-a",
		"the refusal names another tenant's namespace")
}

// TestRoutableTenantRuns is the positive direction, so the check above is not
// passing merely because everything is refused.
func TestRoutableTenantRuns(t *testing.T) {
	flowstate := newPooledServer(t, "team-a", map[string]string{"team-a": "default"})

	started, err := flowstate.Run(t.Context(), connect.NewRequest(&v1.RunRequest{
		Workflow: gatedWorkflow(),
	}))
	require.NoError(t, err, "a routable tenant could not start a run")
	require.NotEmpty(t, started.Msg.GetWorkflowId())

	// And it is addressable afterwards, which means authorization used the same
	// namespace the run was placed in.
	_, err = flowstate.Get(t.Context(), connect.NewRequest(&v1.GetRequest{
		WorkflowId: started.Msg.GetWorkflowId(),
	}))
	require.NoError(t, err, "a run could be started but not read back through the same mapping")
}

// TestMappingNothingStillRuns checks the zero-configuration path, which is the
// common case and the one that must not need a tenancy described to work.
//
// It is the reason "maps nothing" has to be distinguishable from "maps, but not
// this one": treating the first as an error would mean every single-tenant
// deployment had to describe a tenancy it does not have.
func TestMappingNothingStillRuns(t *testing.T) {
	flowstate := newPooledServer(t, "", nil)

	started, err := flowstate.Run(t.Context(), connect.NewRequest(&v1.RunRequest{
		Workflow: gatedWorkflow(),
	}))
	require.NoError(t, err, "a deployment that maps nothing could not run anything")
	require.NotEmpty(t, started.Msg.GetWorkflowId())
}
