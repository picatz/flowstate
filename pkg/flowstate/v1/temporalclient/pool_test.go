package temporalclient

import (
	"errors"
	"testing"

	"github.com/stretchr/testify/require"
	"go.temporal.io/sdk/client"
)

// errNoMapping stands in for auth.ErrNoTemporalNamespace, so this package's tests
// need no dependency on the one that parses policies.
var errNoMapping = errors.New("no temporal mapping")

// fakeMapper describes a tenancy mapping without a trust policy.
type fakeMapper struct {
	// temporal maps a Flowstate namespace to a Temporal one.
	temporal map[string]string

	// fallback is returned for a namespace with no entry. Empty means a namespace
	// with no entry is an error, which is the fail-closed configuration.
	fallback string

	// mapsNothing makes the mapper report that this deployment maps nothing, which
	// is the zero-configuration path.
	mapsNothing bool
}

func (m fakeMapper) TemporalNamespace(namespace string) (string, bool, error) {
	if m.mapsNothing {
		return "", false, nil
	}
	if mapped, ok := m.temporal[namespace]; ok {
		return mapped, true, nil
	}
	if m.fallback != "" {
		return m.fallback, true, nil
	}
	return "", false, errNoMapping
}

func (m fakeMapper) TemporalNamespaces() []string {
	if m.mapsNothing {
		return nil
	}
	seen := map[string]bool{}
	var out []string
	if m.fallback != "" {
		seen[m.fallback] = true
		out = append(out, m.fallback)
	}
	for _, mapped := range m.temporal {
		if !seen[mapped] {
			seen[mapped] = true
			out = append(out, mapped)
		}
	}
	return out
}

func (m fakeMapper) FlowstateNamespaces(temporalNamespace string) []string {
	if m.mapsNothing {
		return nil
	}
	var out []string
	if m.fallback == temporalNamespace {
		out = append(out, "")
	}
	for namespace, mapped := range m.temporal {
		if mapped == temporalNamespace {
			out = append(out, namespace)
		}
	}
	return out
}

// TestPoolRoutingWithoutDialing exercises For's decisions against a pre-populated
// pool.
//
// Routing is the part with the security consequence and it does not need a Temporal
// server to test, so it is tested on its own. Building the pool by hand is
// deliberate: NewPool dials, and a unit test that needs a cluster is a test that
// stops being run.
func TestPoolRoutingWithoutDialing(t *testing.T) {
	t.Parallel()

	// Distinct non-nil values, so the assertions can tell which client came back.
	var configured, teamA, shared client.Client = stubClient{name: "configured"},
		stubClient{name: "team-a"},
		stubClient{name: "shared"}

	t.Run("a deployment that maps nothing uses the configured client", func(t *testing.T) {
		t.Parallel()

		pool := &Pool{
			mapper:      fakeMapper{mapsNothing: true},
			fallback:    configured,
			byNamespace: map[string]client.Client{},
		}

		got, err := pool.For("anything")
		require.NoError(t, err)
		require.Equal(t, configured, got,
			"false with no error means nothing is mapped, so the configured namespace is the answer")
	})

	t.Run("a nil mapper is valid and uses the configured client", func(t *testing.T) {
		t.Parallel()

		pool := &Pool{fallback: configured, byNamespace: map[string]client.Client{}}

		got, err := pool.For("anything")
		require.NoError(t, err)
		require.Equal(t, configured, got)
	})

	t.Run("a mapped namespace gets its own client", func(t *testing.T) {
		t.Parallel()

		pool := &Pool{
			mapper:      fakeMapper{temporal: map[string]string{"team-a": "flowstate-team-a"}},
			fallback:    configured,
			byNamespace: map[string]client.Client{"flowstate-team-a": teamA},
		}

		got, err := pool.For("team-a")
		require.NoError(t, err)
		require.Equal(t, teamA, got)
	})

	t.Run("an unmapped namespace is refused rather than placed arbitrarily", func(t *testing.T) {
		t.Parallel()

		// The property that matters. Falling back to the configured client here
		// would put one tenant's runs in another tenant's namespace, which is a
		// tenancy boundary failing quietly.
		pool := &Pool{
			mapper:      fakeMapper{temporal: map[string]string{"team-a": "flowstate-team-a"}},
			fallback:    configured,
			byNamespace: map[string]client.Client{"flowstate-team-a": teamA},
		}

		got, err := pool.For("team-b")
		require.Error(t, err, "a deployment that maps namespaces must refuse one it has no entry for")
		require.Nil(t, got)
		require.ErrorIs(t, err, errNoMapping)
	})

	t.Run("a default catches namespaces without their own entry", func(t *testing.T) {
		t.Parallel()

		pool := &Pool{
			mapper: fakeMapper{
				temporal: map[string]string{"team-a": "flowstate-team-a"},
				fallback: "flowstate-shared",
			},
			fallback: configured,
			byNamespace: map[string]client.Client{
				"flowstate-team-a": teamA,
				"flowstate-shared": shared,
			},
		}

		got, err := pool.For("team-b")
		require.NoError(t, err)
		require.Equal(t, shared, got, "a configured default is a deliberate choice, unlike the process's own namespace")
	})

	t.Run("a mapping that grew after startup is an error, not a lazy dial", func(t *testing.T) {
		t.Parallel()

		// The pool holds no client for the mapped namespace. Dialing here would
		// move a connection attempt onto the request path, which is the cost the
		// pool exists to avoid.
		pool := &Pool{
			mapper:      fakeMapper{temporal: map[string]string{"team-a": "flowstate-team-a"}},
			fallback:    configured,
			byNamespace: map[string]client.Client{},
		}

		_, err := pool.For("team-a")
		require.Error(t, err)
		require.Contains(t, err.Error(), "restart", "the message should tell an operator what to do")
	})
}

// TestNewPoolRefusesAnUnregisteredNamespace is the negative direction of #769: a
// tenancy mapping naming a Temporal namespace nobody registered must fail
// construction, naming the tenant and the namespace, rather than dialing cleanly
// and leaving the failure for that tenant's first submit.
//
// Dialing alone cannot catch this — [client.DialContext]'s eager check is
// GetSystemInfo, a cluster-scoped RPC with no namespace argument, so it succeeds
// against a namespace nobody registered. This is why the test needs the shared dev
// server rather than a fake: the claim under test is what a real cluster answers
// when asked to describe a namespace it does not have, and a fake mapper cannot
// stand in for that answer.
func TestNewPoolRefusesAnUnregisteredNamespace(t *testing.T) {
	t.Parallel()

	// This test never registers a namespace, so it never calls
	// [newTemporalNamespace] — the one place the -short skip otherwise lives.
	// Repeated here rather than routed through a helper: skipping late, after
	// touching the nil devServer left by TestMain under -short, is the panic
	// this guards, not a smaller version of it.
	if testing.Short() {
		t.Skip("skipping: needs the shared Temporal dev server, not started under -short; CI runs the full suite")
	}

	// Never registered on this package's dev server, and named after the test so
	// a namespace left behind by a crash (there should be none — NewPool never
	// registers one) says which test it came from.
	missing := "unregistered-" + namespaceNameFor(t)

	pool, err := NewPool(t.Context(), Config{
		Address: devServer.FrontendHostPort(),
	}, fakeMapper{temporal: map[string]string{"team-a": missing}}, nil)

	require.Error(t, err, "a mapping naming a namespace the cluster does not have must fail construction")
	require.Nil(t, pool)
	require.Contains(t, err.Error(), missing,
		"the error should name the namespace an operator has to register or fix")
	require.Contains(t, err.Error(), "team-a",
		"the error should name the tenant the mapping routes to the missing namespace")
}

// TestNewPoolAcceptsARegisteredMapping is the positive direction: a mapping naming
// a namespace the cluster actually has still constructs, so the existence check
// costs nothing to a deployment whose configuration is correct.
func TestNewPoolAcceptsARegisteredMapping(t *testing.T) {
	t.Parallel()

	namespace := newTemporalNamespace(t)

	pool, err := NewPool(t.Context(), Config{
		Address: devServer.FrontendHostPort(),
	}, fakeMapper{temporal: map[string]string{"team-a": namespace}}, nil)
	require.NoError(t, err, "a mapping naming a namespace the cluster has must construct cleanly")
	require.NotNil(t, pool)
	t.Cleanup(pool.Close)

	cl, err := pool.For("team-a")
	require.NoError(t, err)
	require.NotNil(t, cl, "the pool must hold a usable client for the namespace it verified")
}

func TestPoolCloseIsIdempotent(t *testing.T) {
	t.Parallel()

	fallback := &countingClient{}
	scoped := &countingClient{}

	pool := &Pool{
		fallback:    fallback,
		byNamespace: map[string]client.Client{"flowstate-team-a": scoped},
	}

	pool.Close()
	pool.Close()

	require.Equal(t, 1, fallback.closes, "a deferred Close plus an error path that already cleaned up must not double-close")
	require.Equal(t, 1, scoped.closes)
}

// stubClient is a distinguishable client.Client.
//
// The interface is embedded rather than implemented: routing is all that is under
// test, so any other method being reached is a nil-pointer panic naming the method,
// which is more informative than a stub that quietly returns nothing.
type stubClient struct {
	client.Client

	name string
}

func (stubClient) Close() {}

// countingClient records how many times it was closed.
type countingClient struct {
	client.Client

	closes int
}

func (c *countingClient) Close() { c.closes++ }
