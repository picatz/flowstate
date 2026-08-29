package server_test

import (
	"testing"

	"connectrpc.com/connect"
	"github.com/stretchr/testify/require"
	"go.temporal.io/sdk/client"

	"github.com/picatz/flowstate/pkg/flowstate/v1/server"
	"github.com/picatz/flowstate/pkg/flowstate/v1/temporalclient"
)

// serverNamespace narrows the paired accessor to the namespace, for a test whose
// subject is which namespace a caller resolves to rather than the pairing.
//
// The pairing has its own test below. The client is dropped here rather than in
// the package, because the server deliberately offers no way to get a namespace
// without the client it belongs to — see
// [server.FlowstateServer.clientAndTemporalNamespaceFor].
func serverNamespace(t *testing.T, s *server.FlowstateServer, tenant string) (string, error) {
	t.Helper()

	_, namespace, err := s.ClientAndTemporalNamespaceForTest(tenant)
	return namespace, err
}

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

		got, err := serverNamespace(t, flowstate, "team-a")
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

		got, err := serverNamespace(t, flowstate, "team-a")
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

// TestNamingTheTemporalNamespaceWithoutAPool covers the single-namespace deployment,
// which is every deployment that does not map tenants onto namespaces of their
// own.
func TestNamingTheTemporalNamespaceWithoutAPool(t *testing.T) {
	t.Parallel()

	t.Run("the recorded namespace serves every caller", func(t *testing.T) {
		t.Parallel()

		// One namespace, so there is nothing to route between and every tenant
		// resolves to it. That is the answer rather than a fallback.
		flowstate := mustNew(t, nil, server.WithTemporalNamespace("flowstate-prod"))

		for _, tenant := range []string{"", "team-a", "team-b"} {
			got, err := serverNamespace(t, flowstate, tenant)
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

		got, err := serverNamespace(t, flowstate, "team-a")
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

// TestNamingTheTemporalNamespaceWithAPoolRefusesAnUnroutableTenant is the test for the
// failure that would not announce itself, one level up from
// TestUnroutableTenantIsRefusedNotRedirected.
//
// That test asserts an unroutable tenant's *run* is refused rather than placed in
// somebody else's namespace. This asserts the same of the tenant's *namespace*:
// handing back the process's own when the pool refuses would let a caller address
// a raw Temporal request at a namespace holding another tenant's histories, and
// the request would succeed. Reads leak as thoroughly as writes do, and more
// quietly.
func TestNamingTheTemporalNamespaceWithAPoolRefusesAnUnroutableTenant(t *testing.T) {
	// The deployment maps team-a and names no default, and this process is
	// pointed at a namespace of its own.
	const recorded = "flowstate-the-process-namespace"

	flowstate, routedNamespace := newNamespaceRecordingPooledServer(t, recorded, "team-a")

	t.Run("a routed tenant is answered from the pool, not from the recording", func(t *testing.T) {
		got, err := serverNamespace(t, flowstate, "team-a")
		require.NoError(t, err)
		require.Equal(t, routedNamespace, got,
			"a pooled deployment's per-tenant answer must win over the single recorded namespace")
		require.NotEqual(t, recorded, got)
	})

	t.Run("an unroutable tenant is refused, never given the process's namespace", func(t *testing.T) {
		got, err := serverNamespace(t, flowstate, "team-b")
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

// TestTheServerHandsBackAClientAndItsOwnNamespace is the server-side half of the
// pairing property, so the fix does not stop at the temporalclient boundary.
//
// A raw Temporal request needs both halves and needs them to be halves of one
// answer: the client's connection decides which cluster is spoken to, the
// namespace in the request field decides whose history is read. A server that
// reassembled the pair out of two accessors would reintroduce the mismatch the
// pool was just fixed to prevent, and the mismatch is a legal request that
// succeeds against the wrong tenant.
func TestTheServerHandsBackAClientAndItsOwnNamespace(t *testing.T) {
	const recorded = "flowstate-the-process-namespace"

	flowstate, routedNamespace := newNamespaceRecordingPooledServer(t, recorded, "team-a")

	t.Run("pooled, the client and namespace are the pool's answer for this tenant", func(t *testing.T) {
		temporal, namespace, err := flowstate.ClientAndTemporalNamespaceForTest("team-a")
		require.NoError(t, err)
		require.NotNil(t, temporal, "a namespace with no client beside it cannot address anything")
		require.Equal(t, routedNamespace, namespace)

		// The client this server would route a run through, taken the ordinary
		// way, has to be the one that came back beside the namespace. Two
		// accessors answering separately is exactly the defect.
		routed, err := flowstate.ClientForTest("team-a")
		require.NoError(t, err)
		require.Equal(t, routed, temporal,
			"the paired accessor named a namespace beside a client that routing would not have used")
	})

	t.Run("pool-less, both halves come from this server's own configuration", func(t *testing.T) {
		t.Parallel()

		// nil is a legal client here and the assertion is about the pairing, not
		// about the client being usable: what must not happen is a namespace
		// coming back beside a client from some other resolution.
		single := mustNew(t, nil, server.WithTemporalNamespace("flowstate-prod"))

		temporal, namespace, err := single.ClientAndTemporalNamespaceForTest("team-a")
		require.NoError(t, err)
		require.Equal(t, "flowstate-prod", namespace)

		routed, err := single.ClientForTest("team-a")
		require.NoError(t, err)
		require.Equal(t, routed, temporal)
	})

	t.Run("a refusal carries neither half", func(t *testing.T) {
		temporal, namespace, err := flowstate.ClientAndTemporalNamespaceForTest("team-b")
		require.Error(t, err)
		require.Empty(t, namespace)
		require.Nil(t, temporal,
			"a refused tenant handed a client is a tenant that can still reach a cluster")
	})
}

// TestEveryDeploymentShapeCanNameItsTemporalNamespace asserts the partition is
// total rather than asserting each half separately.
//
// `flow server` records a namespace in one of two ways — [server.WithTemporalNamespace]
// on a deployment with no tenancy mapping, [server.WithNamespacePool] on one with
// it — and those are complementary branches of one condition in `runServer`. The
// half nothing else checks is that they *cover* the cases: a deployment that fell
// between them would build a server that cannot name where it reads from, and
// since nothing consumes the accessor yet, the first thing to notice would be the
// follow-up that does.
//
// Both shapes are listed here, and the list is asserted non-empty outside the
// loop, so a shape moved out of it is a failure rather than a smaller sweep.
func TestEveryDeploymentShapeCanNameItsTemporalNamespace(t *testing.T) {
	const recorded = "flowstate-the-process-namespace"

	pooled, _ := newNamespaceRecordingPooledServer(t, recorded, "team-a")

	shapes := map[string]*server.FlowstateServer{
		"no tenancy mapping: WithTemporalNamespace": mustNew(t, nil,
			server.WithTemporalNamespace("flowstate-prod")),
		"a tenancy mapping: WithNamespacePool": pooled,
	}

	require.NotEmpty(t, shapes, "every claim below is inside the loop over this table")

	for name, flowstate := range shapes {
		t.Run(name, func(t *testing.T) {
			namespace, err := serverNamespace(t, flowstate, "team-a")
			require.NoError(t, err,
				"a deployment `flow server` can build could not name the Temporal namespace it "+
					"reads from; the two ways of recording one do not cover this shape")
			require.NotEmpty(t, namespace)
		})
	}
}

// shiftingMapper cycles through Temporal namespaces, one per lookup, so that two
// consecutive lookups never agree.
//
// It is the server-side copy of the temporalclient package's mapper of the same
// shape, and it has to exist here rather than be shared because the tenancy
// mapper is an interface each package's tests describe for themselves. What it is
// for: a stable mapper cannot see the defect this file's pairing test forbids,
// because two lookups of a frozen map agree and the reassembled pair comes out
// right by luck. Measured, not assumed — with the stable `mapper` above, a server
// that called For and ForWithNamespace separately passed.
type shiftingMapper struct {
	namespaces []string
	calls      int
}

func (m *shiftingMapper) TemporalNamespace(string) (string, bool, error) {
	mapped := m.namespaces[m.calls%len(m.namespaces)]
	m.calls++
	return mapped, true, nil
}

func (m *shiftingMapper) TemporalNamespaces() []string { return m.namespaces }

func (m *shiftingMapper) FlowstateNamespaces(string) []string { return []string{"team-a"} }

// TestTheServerNeverReassemblesThePairFromTwoLookups is the server-side half of
// the property, written against a mapper that answers differently every call
// because that is the only way to see the defect.
//
// The fix in [temporalclient.Pool.ForWithNamespace] is worth nothing if the server
// takes the client from one call and the namespace from another: the mismatch
// simply moves up a package. And a mismatch is not a failure anybody sees — a raw
// Temporal request carries its namespace as a request field, so a client connected
// for one namespace addressed at another is a legal request that succeeds against
// the wrong tenant's history.
func TestTheServerNeverReassemblesThePairFromTwoLookups(t *testing.T) {
	// Two real namespaces, because NewPool dials and verifies each one the mapper
	// can select.
	temporal, first := newTemporalNamespace(t)
	_, second := newTemporalNamespace(t)

	pool, err := temporalclient.NewPool(t.Context(), temporalclient.Config{
		Address:   devServer.FrontendHostPort(),
		Namespace: first,
	}, &shiftingMapper{namespaces: []string{first, second}}, nil)
	require.NoError(t, err)
	t.Cleanup(pool.Close)

	// The true pairing, learned from the accessor whose atomicity the
	// temporalclient package's own tests pin. Each call is one resolution, so
	// each observation is a genuine pair; collecting several under a cycling
	// mapper yields the client for both namespaces.
	pairs := map[string]client.Client{}
	for range 4 {
		cl, namespace, err := pool.ForWithNamespace("team-a")
		require.NoError(t, err)
		pairs[namespace] = cl
	}
	require.Len(t, pairs, 2,
		"the mapper was supposed to cycle; against a mapper that does not, this test cannot "+
			"tell a paired answer from a reassembled one and proves nothing")

	flowstate := mustNew(t, temporal, server.WithNamespacePool(pool))

	// Several times, because the mapper is moving under each one and a pairing
	// that held once could be the cycle lining up rather than the property.
	for range 4 {
		cl, namespace, err := flowstate.ClientAndTemporalNamespaceForTest("team-a")
		require.NoError(t, err)
		require.NotNil(t, cl)
		require.Equal(t, pairs[namespace], cl,
			"the server handed back a client dialed for one namespace beside the name of "+
				"another; a raw request built from this pair reads the wrong tenant's history "+
				"and succeeds")
	}
}
