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
