package server_test

import (
	"testing"

	"connectrpc.com/connect"
	"github.com/stretchr/testify/require"

	"github.com/picatz/flowstate/pkg/flowstate/v1/server"
	"github.com/picatz/flowstate/pkg/flowstate/v1/temporalclient"
)

// TestWithTemporalNamespaceIsNotWithNamespace pins the two apart at the surface a
// deployment configures them through.
//
// One word, two boundaries: [server.WithNamespace] names the *Flowstate tenant* a
// caller with no identity of their own is treated as, and every authorization
// decision about a run compares against it; [server.WithTemporalNamespace] names
// the *Temporal namespace* this process's client is dialed for. Conflating them
// has happened here before — WithNamespace was documented as the Temporal
// namespace for long enough to need correcting — and the version that would land
// now is an accessor answering with a tenant name, which a raw Temporal request
// would carry to a namespace of that name or to none.
func TestWithTemporalNamespaceIsNotWithNamespace(t *testing.T) {
	t.Parallel()

	t.Run("the two are recorded separately", func(t *testing.T) {
		t.Parallel()

		flowstate := mustNew(t, nil,
			server.WithNamespace("team-a"),
			server.WithTemporalNamespace("flowstate-prod"),
		)

		got, err := flowstate.TemporalNamespaceForTest("team-a")
		require.NoError(t, err)
		require.Equal(t, "flowstate-prod", got)
		require.NotEqual(t, "team-a", got,
			"the Flowstate tenant is not the Temporal namespace; answering with it would "+
				"address a raw request at a namespace named after a tenant")
	})

	t.Run("a Flowstate tenant alone does not supply a Temporal namespace", func(t *testing.T) {
		t.Parallel()

		// The direction that matters: a deployment that named its tenant and
		// nothing else has not named a Temporal namespace, and must not be
		// answered as though it had.
		flowstate := mustNew(t, nil, server.WithNamespace("team-a"))

		got, err := flowstate.TemporalNamespaceForTest("team-a")
		require.Error(t, err)
		require.Empty(t, got)
	})
}

// TestWithTemporalNamespaceRefusesTheEmptyNamespace pins the option's own
// fail-closed check.
//
// There is no deployment the empty Temporal namespace is right for:
// [temporalclient.Config.Options] resolves a non-empty one for every
// configuration. So a caller passing "" holds an unresolved value — almost
// certainly `cfg.Namespace`, which is an override that is empty on every
// deployment configured by profile or environment — and accepting it would put an
// empty namespace on a request that fails at the far end of an RPC, naming
// Temporal rather than this deployment's configuration.
func TestWithTemporalNamespaceRefusesTheEmptyNamespace(t *testing.T) {
	t.Parallel()

	_, err := server.New(nil, server.WithTemporalNamespace(""))
	require.Error(t, err, "the empty Temporal namespace must be refused at startup, not carried into a request")
	require.Contains(t, err.Error(), "WithTemporalNamespace",
		"the message should name the option an operator has to fix")
}

// TestTemporalNamespaceForWithoutAPool covers the single-namespace deployment,
// which is every deployment that does not map tenants onto namespaces of their
// own.
func TestTemporalNamespaceForWithoutAPool(t *testing.T) {
	t.Parallel()

	t.Run("the recorded namespace serves every caller", func(t *testing.T) {
		t.Parallel()

		// One namespace, so there is nothing to route between and every tenant
		// resolves to it. That is the answer rather than a fallback.
		flowstate := mustNew(t, nil, server.WithTemporalNamespace("flowstate-prod"))

		for _, tenant := range []string{"", "team-a", "team-b"} {
			got, err := flowstate.TemporalNamespaceForTest(tenant)
			require.NoError(t, err)
			require.Equal(t, "flowstate-prod", got)
		}
	})

	t.Run("a server nobody told refuses rather than answering with nothing", func(t *testing.T) {
		t.Parallel()

		// The `server.New(nil)` shape `flow mcp` builds — no Temporal client, so
		// nothing to name a namespace for — and any deployment that forgot the
		// option. Answering "" would hand a caller an empty namespace to put in
		// a raw request field.
		flowstate := mustNew(t, nil)

		got, err := flowstate.TemporalNamespaceForTest("team-a")
		require.Error(t, err)
		require.Empty(t, got)
		require.Equal(t, connect.CodeFailedPrecondition, connect.CodeOf(err),
			"a deployment an operator has to fix, not an internal error and not the caller's fault")
		require.Contains(t, err.Error(), "WithTemporalNamespace",
			"the message should name what is missing")
	})
}

// newNamespaceRecordingPooledServer returns a server that both routes through a
// pool and has a single Temporal namespace recorded on it, so a test can tell
// which of the two answered.
//
// The recorded namespace is deliberately not one the pool can produce: if the
// accessor ever reads it on a pooled deployment, the value it returns says so.
func newNamespaceRecordingPooledServer(
	t *testing.T, recorded string, routed ...string,
) (*server.FlowstateServer, string) {
	t.Helper()

	temporal, namespace := newTemporalNamespace(t)

	mapping := make(map[string]string, len(routed))
	for _, name := range routed {
		mapping[name] = namespace
	}

	pool, err := temporalclient.NewPool(t.Context(), temporalclient.Config{
		Address:   devServer.FrontendHostPort(),
		Namespace: namespace,
	}, mapper{mapping: mapping}, nil)
	require.NoError(t, err)
	t.Cleanup(pool.Close)

	return mustNew(t, temporal,
		server.WithTemporalNamespace(recorded),
		server.WithNamespacePool(pool),
	), namespace
}

// TestTemporalNamespaceForWithAPoolRefusesAnUnroutableTenant is the test for the
// failure that would not announce itself, one level up from
// TestUnroutableTenantIsRefusedNotRedirected.
//
// That test asserts an unroutable tenant's *run* is refused rather than placed in
// somebody else's namespace. This asserts the same of the tenant's *namespace*:
// handing back the process's own when the pool refuses would let a caller address
// a raw Temporal request at a namespace holding another tenant's histories, and
// the request would succeed. Reads leak as thoroughly as writes do, and more
// quietly.
func TestTemporalNamespaceForWithAPoolRefusesAnUnroutableTenant(t *testing.T) {
	// The deployment maps team-a and names no default, and this process is
	// pointed at a namespace of its own.
	const recorded = "flowstate-the-process-namespace"

	flowstate, routedNamespace := newNamespaceRecordingPooledServer(t, recorded, "team-a")

	t.Run("a routed tenant is answered from the pool, not from the recording", func(t *testing.T) {
		got, err := flowstate.TemporalNamespaceForTest("team-a")
		require.NoError(t, err)
		require.Equal(t, routedNamespace, got,
			"a pooled deployment's per-tenant answer must win over the single recorded namespace")
		require.NotEqual(t, recorded, got)
	})

	t.Run("an unroutable tenant is refused, never given the process's namespace", func(t *testing.T) {
		got, err := flowstate.TemporalNamespaceForTest("team-b")
		require.Error(t, err,
			"a tenant this deployment cannot place was told a namespace; it would read what is in it")
		require.Empty(t, got)
		require.NotEqual(t, recorded, got,
			"falling back to the process's own namespace is the tenancy breach, reached by reading")
		require.NotEqual(t, routedNamespace, got,
			"nor may team-b learn the namespace team-a's histories are in")
		require.Equal(t, connect.CodeFailedPrecondition, connect.CodeOf(err),
			"a configuration gap an operator fixes")
	})
}
