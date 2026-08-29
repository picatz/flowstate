package temporalclient

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.temporal.io/sdk/client"
)

// writeProfile writes a Temporal TOML configuration naming one profile and
// returns its path, for a Config that names a namespace the way an ordinary
// deployment does.
//
// A file rather than TEMPORAL_NAMESPACE, and that choice is the reason these
// tests can run in parallel: t.Setenv refuses a parallel test, and a package-wide
// environment mutation would reach every other test in this package that resolves
// a Config — including TestConfigOptionsDefaults, whose whole claim is about an
// unconfigured environment. The two sources are the same source as far as
// [Config.Options] is concerned: both arrive through envconfig, and neither is
// Config.Namespace, which is the distinction under test.
func writeProfile(t *testing.T, profile, namespace string) string {
	t.Helper()

	path := filepath.Join(t.TempDir(), "temporal.toml")
	body := "[profile." + profile + "]\nnamespace = \"" + namespace + "\"\n"
	require.NoError(t, os.WriteFile(path, []byte(body), 0o600),
		"writing the TOML profile this test configures a namespace through")

	return path
}

// poolNamespace is [Pool.ForWithNamespace] narrowed to the namespace, for a test
// whose subject is which namespace a tenant resolves to rather than the pairing.
//
// It takes both halves from one call because that is the only way to get a
// namespace out of a Pool — there is deliberately no accessor for the namespace
// alone, see ForWithNamespace's doc — and it drops the client here rather than in
// the package, so a test reading like a claim about namespaces still cannot spell
// the defect the pairing exists to prevent.
func poolNamespace(t *testing.T, p *Pool, tenant string) (string, error) {
	t.Helper()

	_, namespace, err := p.ForWithNamespace(tenant)
	return namespace, err
}

// TestConfigOptionsResolvesANamespaceTheOverrideFieldDoesNotCarry pins the
// distinction every other test in this file rests on: Config.Namespace is an
// override, and it is empty on a deployment that configured a namespace.
//
// Reading Config.Namespace to learn "which namespace is this" is therefore wrong
// in the common case rather than in an exotic one — a TOML profile or
// TEMPORAL_NAMESPACE is how the Temporal ecosystem configures this, so the field
// is empty on every deployment that used either, and the value that field does not
// carry is exactly the one a client gets dialed for.
func TestConfigOptionsResolvesANamespaceTheOverrideFieldDoesNotCarry(t *testing.T) {
	t.Parallel()

	const configured = "flowstate-from-a-profile"

	t.Run("a profile supplies the namespace the override field leaves empty", func(t *testing.T) {
		t.Parallel()

		cfg := Config{ConfigFile: writeProfile(t, "staging", configured), Profile: "staging"}
		require.Empty(t, cfg.Namespace,
			"the premise: this deployment configured a namespace and never touched the override field")

		opts, err := cfg.Options()
		require.NoError(t, err)
		require.Equal(t, configured, opts.Namespace)

		// Spelled out rather than left implicit, because these are the two
		// wrong answers a reader of Config.Namespace would get: "" from the
		// field itself, or DefaultNamespace from a re-derivation that treated
		// the empty field as "nothing was configured".
		require.NotEmpty(t, opts.Namespace)
		require.NotEqual(t, DefaultNamespace, opts.Namespace)
	})

	t.Run("the override field wins when it is set", func(t *testing.T) {
		t.Parallel()

		cfg := Config{
			ConfigFile: writeProfile(t, "staging", configured),
			Profile:    "staging",
			Namespace:  "flowstate-from-a-flag",
		}

		opts, err := cfg.Options()
		require.NoError(t, err)
		require.Equal(t, "flowstate-from-a-flag", opts.Namespace,
			"an explicit value must still displace a profile, which is what lets a flag override one")
	})

	t.Run("an unconfigured deployment resolves the default, never the empty namespace", func(t *testing.T) {
		t.Parallel()

		// The invariant [Pool.fallbackNamespace] relies on: Options never
		// resolves "", so a pool built from any Config has a namespace to
		// record.
		opts, err := Config{ConfigFile: filepath.Join(t.TempDir(), "absent.toml")}.Options()
		require.NoError(t, err, "a missing configuration file is the common case, not an error")
		require.Equal(t, DefaultNamespace, opts.Namespace)
		require.NotEmpty(t, opts.Namespace)
	})
}

// TestNewPoolRecordsTheResolvedNamespaceNotTheOverride is the negative direction
// of the test above, at the place the value is kept.
//
// A pool that recorded cfg.Namespace would record "" here, and an empty namespace
// in a raw Temporal request field fails at the far end of an RPC with a message
// about Temporal rather than about this deployment — which is worse than not
// having the value at all, because it fails somewhere nobody is looking. The
// namespace is configured through a profile and deliberately never assigned to
// cfg.Namespace, so a pool reading the override field cannot pass.
func TestNewPoolRecordsTheResolvedNamespaceNotTheOverride(t *testing.T) {
	t.Parallel()

	// Registered so that the namespace named here is a real one on the shared
	// dev server, and skips under -short before touching the nil devServer.
	namespace := newTemporalNamespace(t)

	cfg := Config{
		// The address is overridden and the namespace is not, which is the
		// ordinary `flow server` shape: a flag names where, configuration names
		// which.
		Address:    devServer.FrontendHostPort(),
		ConfigFile: writeProfile(t, "default", namespace),
	}
	require.Empty(t, cfg.Namespace, "the premise: nothing sets the override field")

	// No mapper: the zero-configuration path, where the recorded namespace is the
	// whole answer for every tenant.
	pool, err := NewPool(t.Context(), cfg, nil, nil)
	require.NoError(t, err)
	t.Cleanup(pool.Close)

	got, err := poolNamespace(t, pool, "any-tenant")
	require.NoError(t, err)
	require.Equal(t, namespace, got,
		"the pool must record the namespace its client was dialed for, not the override that was never set")
	require.NotEmpty(t, got,
		"recording cfg.Namespace would record the empty namespace on every profile-configured deployment")
	require.NotEqual(t, DefaultNamespace, got,
		"nor may it re-derive an answer as though nothing had been configured")
}

// TestASecondResolutionCanDisagreeWithTheDial is the premise behind carrying the
// namespace out of the dial instead of resolving the configuration again.
//
// [Config.Options] is not a pure function of its receiver: it reads the process
// environment and a TOML file off disk on every call. So "resolve it again when
// you need it" is not a way of asking the same question twice — it is a way of
// asking a *different* question that usually happens to have the same answer. A
// deployment where it does not gets a namespace recorded beside a client that is
// not connected to it, and every raw request built from that pairing succeeds
// against the wrong namespace.
//
// The window is not theoretical in `flow server`: between the dial and the point
// the namespace is needed, plugin subprocesses start, secret providers load and a
// webhook receiver compiles Flowfiles. This test makes the divergence happen
// rather than arguing about whether it can, so that the tests and comments
// forbidding a second resolution are guarding something real.
func TestASecondResolutionCanDisagreeWithTheDial(t *testing.T) {
	t.Parallel()

	namespace := newTemporalNamespace(t)

	path := writeProfile(t, "default", namespace)
	cfg := Config{Address: devServer.FrontendHostPort(), ConfigFile: path}

	cl, dialed, err := DialWithNamespace(t.Context(), cfg)
	require.NoError(t, err)
	t.Cleanup(cl.Close)
	require.Equal(t, namespace, dialed)

	// The configuration changes under the running process, which is all it takes:
	// nothing here touches cfg, and cfg is still the value every later call would
	// resolve.
	require.NoError(t, os.WriteFile(path,
		[]byte("[profile.default]\nnamespace = \"somebody-elses-namespace\"\n"), 0o600))

	second, err := cfg.Options()
	require.NoError(t, err)
	require.Equal(t, "somebody-elses-namespace", second.Namespace,
		"the premise: resolving the same Config again answers with whatever the source says now")
	require.NotEqual(t, dialed, second.Namespace,
		"a second resolution disagreed with the dial, which is the divergence a caller "+
			"that re-resolves would silently adopt")

	// And the dial's own answer is unmoved, because it was taken once and kept.
	// A caller carrying this value describes the client it has; a caller
	// re-resolving describes a client nobody dialed.
	require.Equal(t, namespace, dialed)
}

// TestPoolNamespaceLookupFailsClosed writes the direction a tenancy test has
// to write: not that a tenant reaches its own namespace, but that it cannot reach
// anyone else's, and that a deployment which cannot place a tenant refuses instead
// of choosing.
//
// The consequence is a wrong answer rather than a crash. A tenant handed another
// tenant's Temporal namespace would use it to address a perfectly legal raw
// request, which would succeed, and nothing anywhere would say whose history had
// been read.
func TestPoolNamespaceLookupFailsClosed(t *testing.T) {
	t.Parallel()

	var configured, teamA, shared client.Client = stubClient{name: "configured"},
		stubClient{name: "team-a"},
		stubClient{name: "shared"}

	// The fail-closed deployment: it maps team-a and names no default, so it has
	// no answer for anybody else and must say so.
	separated := &Pool{
		mapper:            fakeMapper{temporal: map[string]string{"team-a": "flowstate-team-a"}},
		fallback:          configured,
		fallbackNamespace: "flowstate-configured",
		byNamespace:       map[string]client.Client{"flowstate-team-a": teamA},
	}

	t.Run("an unmapped tenant is refused rather than given the configured namespace", func(t *testing.T) {
		t.Parallel()

		got, err := poolNamespace(t, separated, "team-b")
		require.Error(t, err, "a deployment that maps namespaces must refuse a tenant it has no entry for")
		require.ErrorIs(t, err, errNoMapping)
		require.Empty(t, got, "a refusal must carry no namespace, least of all a usable one")
	})

	t.Run("an unmapped tenant is never told another tenant's namespace", func(t *testing.T) {
		t.Parallel()

		got, _ := poolNamespace(t, separated, "team-b")
		require.NotEqual(t, "flowstate-team-a", got,
			"team-b must not learn team-a's namespace; a raw request naming it would read team-a's histories")
		require.NotEqual(t, separated.fallbackNamespace, got,
			"nor the process's own namespace, which is where an accessor that fell back would send it")
	})

	t.Run("a mapped tenant gets its own namespace and no other", func(t *testing.T) {
		t.Parallel()

		got, err := poolNamespace(t, separated, "team-a")
		require.NoError(t, err)
		require.Equal(t, "flowstate-team-a", got)
	})

	t.Run("two mapped tenants never resolve to each other", func(t *testing.T) {
		t.Parallel()

		pool := &Pool{
			mapper: fakeMapper{temporal: map[string]string{
				"team-a": "flowstate-team-a",
				"team-b": "flowstate-team-b",
			}},
			fallback:          configured,
			fallbackNamespace: "flowstate-configured",
			byNamespace: map[string]client.Client{
				"flowstate-team-a": teamA,
				"flowstate-team-b": shared,
			},
		}

		a, err := poolNamespace(t, pool, "team-a")
		require.NoError(t, err)
		b, err := poolNamespace(t, pool, "team-b")
		require.NoError(t, err)

		require.NotEqual(t, a, b, "two separated tenants sharing an answer is the separation not existing")
		require.Equal(t, "flowstate-team-a", a)
		require.Equal(t, "flowstate-team-b", b)
	})

	t.Run("a deployment that maps nothing answers with the configured namespace", func(t *testing.T) {
		t.Parallel()

		// Not a fallback but the answer: there is one namespace, and that is
		// it. The claim worth making is that it is the *configured* one rather
		// than empty, which is what a pool that never recorded it would give.
		pool := &Pool{
			mapper:            fakeMapper{mapsNothing: true},
			fallback:          configured,
			fallbackNamespace: "flowstate-configured",
			byNamespace:       map[string]client.Client{},
		}

		got, err := poolNamespace(t, pool, "any-tenant")
		require.NoError(t, err)
		require.Equal(t, "flowstate-configured", got)
		require.NotEmpty(t, got)
	})

	t.Run("a nil mapper answers with the configured namespace", func(t *testing.T) {
		t.Parallel()

		pool := &Pool{
			fallback:          configured,
			fallbackNamespace: "flowstate-configured",
			byNamespace:       map[string]client.Client{},
		}

		got, err := poolNamespace(t, pool, "any-tenant")
		require.NoError(t, err)
		require.Equal(t, "flowstate-configured", got)
	})

	t.Run("a mapping that grew after startup is refused, not answered from the mapper", func(t *testing.T) {
		t.Parallel()

		// The mapper knows where team-a goes and the pool holds no client for
		// it. Answering from the mapper alone would name a namespace this
		// process never dialed or verified, which is the half-configured state
		// [Pool.For] refuses for exactly the same reason.
		pool := &Pool{
			mapper:            fakeMapper{temporal: map[string]string{"team-a": "flowstate-team-a"}},
			fallback:          configured,
			fallbackNamespace: "flowstate-configured",
			byNamespace:       map[string]client.Client{},
		}

		got, err := poolNamespace(t, pool, "team-a")
		require.Error(t, err)
		require.Empty(t, got)
		require.Contains(t, err.Error(), "restart", "the message should tell an operator what to do")
	})
}

// shiftingMapper answers with a different Temporal namespace every time it is
// asked, which is what a stateful [NamespaceMapper] looks like from this package.
//
// Nothing in the tree maps this way today. That is the point: NamespaceMapper is
// an interface, so "does an implementation answer the same way twice" is not a
// property this package may assume, and a guarantee that holds only against a
// mapper that happens to be a frozen map is a habit rather than a guarantee. A
// mapper that reloads a policy on a signal, or reads a tenant's routing from a
// store, is an ordinary thing for somebody to write against this interface.
type shiftingMapper struct {
	// namespaces are cycled, one per call, forever.
	//
	// Cycling rather than shifting once and settling, and that is the difference
	// between a test that catches the defect and one that catches it sometimes:
	// a mapper that settles gives two *consecutive* lookups the same answer
	// after the first couple of calls, so a caller resolving twice would start
	// agreeing with itself again and the mismatch would stop being observable.
	namespaces []string

	// calls counts what has been handed out. Not atomic: these tests call
	// sequentially, and a data race here would be a defect in the test.
	calls int
}

func (m *shiftingMapper) TemporalNamespace(string) (string, bool, error) {
	mapped := m.namespaces[m.calls%len(m.namespaces)]
	m.calls++
	return mapped, true, nil
}

func (m *shiftingMapper) TemporalNamespaces() []string { return m.namespaces }

func (m *shiftingMapper) FlowstateNamespaces(string) []string { return []string{"team-a"} }

// TestAnEmptyMappedNamespaceIsRefused closes the one way a mapper can obtain a
// client that does not match the name it is handed out beside.
//
// `Config.Namespace` is an *override*, so leaving it empty is how a caller says
// "use whatever is configured", and [Config.Options] then substitutes the
// profile's namespace or the default. A mapper naming "" therefore does not get
// a client dialed for "" — it gets one dialed for somewhere else, recorded under
// the empty key. A tenant routed there would be handed that client beside the
// name "", which is exactly the pairing [Pool.ForWithNamespace] promises cannot
// happen (Codex, #1139).
//
// Refused in both places, and the second is not redundant. [NamespaceMapper] is
// an interface and nothing here decides whether an implementation is stateful,
// so a mapper that answered honestly at startup may answer "" on the request
// path — the same reasoning that made the pairing one lookup rather than two.
func TestAnEmptyMappedNamespaceIsRefused(t *testing.T) {
	t.Parallel()

	// Registered so the configured namespace is a real one, and so this skips
	// under -short before touching the shared dev server.
	configured := newTemporalNamespace(t)
	dialable := newTemporalNamespace(t)

	cfg := Config{
		Address:    devServer.FrontendHostPort(),
		ConfigFile: writeProfile(t, "default", configured),
	}

	t.Run("at startup, where an operator can fix it", func(t *testing.T) {
		// The empty name comes *after* a real one, so a client has genuinely
		// been dialed by the time the refusal fires. That ordering is what makes
		// the `pool.Close()` on this path matter rather than decorate.
		//
		// The closure itself is not asserted, and I would rather say so than
		// imply it: nothing in this package observes whether a client was shut,
		// and a mutation removing the Close survives every test here. What is
		// asserted is that no pool comes back — so a caller cannot use one — and
		// the Close is there for consistency with the dial-failure path a few
		// lines up, which has always closed what it had already opened.
		mapper := &shiftingMapper{namespaces: []string{dialable, ""}}

		pool, err := NewPool(t.Context(), cfg, mapper, nil)

		require.Error(t, err, "a mapping naming no namespace was dialed anyway")
		assert.Nil(t, pool, "a refused pool must not be returned holding open clients")
		assert.Contains(t, err.Error(), "empty Temporal namespace",
			"the refusal does not say what is wrong with the mapping")
	})

	t.Run("on the request path, where a mapper may have changed its mind", func(t *testing.T) {
		// Honest at startup and empty afterwards: the list names a real
		// namespace so the pool builds, and the per-tenant lookup then answers
		// with nothing. A startup-only check passes this and hands out the
		// mismatch.
		mapper := &emptyingMapper{dialable: dialable}

		pool, err := NewPool(t.Context(), cfg, mapper, nil)
		require.NoError(t, err, "the premise: this mapping is dialable at startup")
		t.Cleanup(pool.Close)

		_, _, err = pool.ForWithNamespace("team-a")

		require.Error(t, err, "a tenant was routed to a client dialed for another namespace")
		assert.Contains(t, err.Error(), "empty Temporal namespace")
	})
}

// emptyingMapper names a real namespace to dial and then resolves every tenant
// to nothing, which is the shape a startup-only check cannot see.
type emptyingMapper struct {
	dialable string
}

func (m *emptyingMapper) TemporalNamespace(string) (string, bool, error) { return "", true, nil }

func (m *emptyingMapper) TemporalNamespaces() []string { return []string{m.dialable} }

func (m *emptyingMapper) FlowstateNamespaces(string) []string { return []string{"team-a"} }

// TestTheClientAndNamespaceComeFromOneResolution is the test the pairing exists
// for, and it is written against a mapper that changes its mind because a test
// against a stable one passes whether the pairing holds or not.
//
// The defect it forbids: a caller assembling a pair out of two lookups. With a
// mapper that answers differently the second time, that caller ends up holding a
// client dialed for namespace A beside the string "namespace B" — and because a
// raw Temporal request carries its namespace as a request field, using that pair
// is a legal, successful request that reads the wrong tenant's history. Nothing
// fails, nothing logs, and the boundary is gone.
//
// [Pool.ForWithNamespace] can only be wrong here if it resolves twice, so this
// fails the moment somebody reintroduces the two-wrapper shape that made
// [Pool.resolve]'s atomicity stop at its own return statement (Codex, #1139).
func TestTheClientAndNamespaceComeFromOneResolution(t *testing.T) {
	t.Parallel()

	var first, second client.Client = stubClient{name: "first"}, stubClient{name: "second"}

	newPool := func() *Pool {
		return &Pool{
			mapper:            &shiftingMapper{namespaces: []string{"flowstate-first", "flowstate-second"}},
			fallback:          stubClient{name: "configured"},
			fallbackNamespace: "flowstate-configured",
			byNamespace: map[string]client.Client{
				"flowstate-first":  first,
				"flowstate-second": second,
			},
		}
	}

	t.Run("the mapper really does change its answer", func(t *testing.T) {
		t.Parallel()

		// Asserted rather than assumed. A shiftingMapper that had stopped
		// shifting would make every claim below pass against the defect, which
		// is the vacuous shape this whole file is written to avoid.
		mapper := &shiftingMapper{namespaces: []string{"flowstate-first", "flowstate-second"}}

		one, _, err := mapper.TemporalNamespace("team-a")
		require.NoError(t, err)
		two, _, err := mapper.TemporalNamespace("team-a")
		require.NoError(t, err)

		require.NotEqual(t, one, two, "the premise: this mapper answers differently on its second call")
	})

	t.Run("one call hands back a client and the namespace it is dialed for", func(t *testing.T) {
		t.Parallel()

		pool := newPool()

		cl, namespace, err := pool.ForWithNamespace("team-a")
		require.NoError(t, err)

		require.Equal(t, pool.byNamespace[namespace], cl,
			"the client and the namespace came from different resolutions: this pair addresses "+
				"one tenant's cluster connection at another tenant's namespace, and Temporal "+
				"would answer it")
	})

	t.Run("every later call is self-consistent too, however the mapper moves", func(t *testing.T) {
		t.Parallel()

		// The traversal rather than the step. A pairing that held on the first
		// call and broke on the fourth is still a broken pairing, and the
		// mapper is shifting under every one of these.
		pool := newPool()

		seen := map[string]bool{}
		const calls = 6
		for range calls {
			cl, namespace, err := pool.ForWithNamespace("team-a")
			require.NoError(t, err)
			require.Equal(t, pool.byNamespace[namespace], cl,
				"a later call assembled a mismatched pair")
			seen[namespace] = true
		}

		require.Len(t, seen, 2,
			"the mapper was supposed to move between two namespaces across these calls; if it "+
				"only ever named one, this test proved nothing about pairing under change")
	})
}

// TestPoolForAndForWithNamespaceNeverDisagree is the property that makes the
// accessor safe to add at all.
//
// Two answers to "which namespace" that could disagree would submit a tenant's
// runs through one and address a raw request about them at the other, and both
// would succeed. So for every input, either both refuse or both answer — and when
// they answer, the namespace named is the one holding the client returned.
func TestPoolForAndForWithNamespaceNeverDisagree(t *testing.T) {
	t.Parallel()

	var configured, teamA, shared client.Client = stubClient{name: "configured"},
		stubClient{name: "team-a"},
		stubClient{name: "shared"}

	// byNamespace with the fallback's own namespace included, so an answer can
	// always be checked against the client held for it.
	pools := map[string]*Pool{
		"maps nothing": {
			mapper:            fakeMapper{mapsNothing: true},
			fallback:          configured,
			fallbackNamespace: "flowstate-configured",
			byNamespace:       map[string]client.Client{},
		},
		"nil mapper": {
			fallback:          configured,
			fallbackNamespace: "flowstate-configured",
			byNamespace:       map[string]client.Client{},
		},
		"separated, no default": {
			mapper:            fakeMapper{temporal: map[string]string{"team-a": "flowstate-team-a"}},
			fallback:          configured,
			fallbackNamespace: "flowstate-configured",
			byNamespace:       map[string]client.Client{"flowstate-team-a": teamA},
		},
		"separated, with a default": {
			mapper: fakeMapper{
				temporal: map[string]string{"team-a": "flowstate-team-a"},
				fallback: "flowstate-shared",
			},
			fallback:          configured,
			fallbackNamespace: "flowstate-configured",
			byNamespace: map[string]client.Client{
				"flowstate-team-a": teamA,
				"flowstate-shared": shared,
			},
		},
		"mapping grew after startup": {
			mapper:            fakeMapper{temporal: map[string]string{"team-a": "flowstate-team-a"}},
			fallback:          configured,
			fallbackNamespace: "flowstate-configured",
			byNamespace:       map[string]client.Client{},
		},
	}

	tenants := []string{"", "team-a", "team-b", "team-a-suffix"}

	// The corpus is asserted non-empty outside the loop: every claim below is
	// inside two of them, and a table that lost its rows would prove nothing
	// while staying green.
	require.NotEmpty(t, pools)
	require.NotEmpty(t, tenants)

	for name, pool := range pools {
		for _, tenant := range tenants {
			t.Run(name+"/"+tenant, func(t *testing.T) {
				only, clientErr := pool.For(tenant)
				cl, namespace, pairErr := pool.ForWithNamespace(tenant)

				if clientErr != nil {
					require.Error(t, pairErr,
						"For refused this tenant and ForWithNamespace answered; an accessor that "+
							"answers where routing refuses hands out another tenant's namespace")
					require.Empty(t, namespace)
					require.Nil(t, cl)
					return
				}

				require.NoError(t, pairErr,
					"For placed this tenant and ForWithNamespace could not name where")
				require.NotEmpty(t, namespace)
				require.Equal(t, only, cl,
					"the two entry points routed one tenant to two different clients")

				// The namespace named must hold the client handed back. For the
				// fallback there is no map entry, so the check is that the
				// fallback's recorded namespace is what was named.
				if held, ok := pool.byNamespace[namespace]; ok {
					require.Equal(t, held, cl,
						"the namespace named is not the one holding the client returned")
					return
				}
				require.Equal(t, pool.fallbackNamespace, namespace,
					"an answer naming no mapped namespace must be the fallback's own")
				require.Equal(t, pool.fallback, cl)
			})
		}
	}
}
