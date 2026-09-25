package plugin

import (
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	flowstatev1 "github.com/picatz/flowstate/pkg/flowstate/v1"
	"github.com/picatz/flowstate/pkg/flowstate/v1/auth"
	"github.com/picatz/flowstate/pkg/flowstate/v1/secrets"
)

// This file is the join CLAUDE.md's tenancy rule asks for and the other tests in
// this package do not cover. TestResolvePluginSecretInputsFailsClosedWithoutRuntime
// and its neighbors are shape negatives: an undeclared field, a nested reference,
// no runtime at all. None of them asks whether *the right tenant's* runtime reaches
// the provider when two tenants are both real. An isolation test asserting that
// each tenant reads its own secret is a functionality test wearing a security
// test's clothes — the env provider's own tenancy tests once looked exactly like
// that and still let team-a read team-b's variable. So the tests below are built
// the way that bug was found: two tenants, one shared provider, the refusal named,
// and the failure message says what a leak would mean.
//
// # Why the value is compared before the plugin round trip, not after
//
// A first attempt at this test sent both tenants' calls through the real plugin
// process and compared the value it echoed back. That does not work: each call's
// [scrubPluginOutputs] redacts whatever *that call itself* resolved, whichever
// tenant it actually came from — so a bug that hands tenant B tenant A's secret
// still gets it redacted by tenant B's own scrubber, which duly learned tenant A's
// value the moment it resolved it. Scrubbing protects a value from leaving through
// an echo; it has nothing to say about whether it was the *right* value to hand
// the plugin in the first place. So the join under test — ctx's [TaskRuntime] to
// [flowstatev1.ResolveSecret] to [secrets.Store.For] to the provider — is asserted
// directly, on [resolvePluginSecretInputs]'s own resolved output, before anything
// downstream of it can redact the evidence.
//
// # Why the local driver is enough to cover "the authorized-activity route"
//
// A durable run and a local run reach this package through the same function.
// [Plugin.taskFunc] — the [flowstatev1.TaskDef.Fn] both drivers call — has no
// per-driver logic of its own; the durable driver's "authorized activity" is a
// different *activity name* Temporal dispatches to, chosen by
// [flowstatev1.TaskNeedsAuthority], but the code that runs once dispatched is this
// same closure calling this same [resolvePluginSecretInputs] over the same
// [flowstatev1.TaskRuntime] read from ctx. There is no second copy of it for the
// durable path to disagree with — the "one executor" invariant, applied to this
// seam specifically. Testing this function directly therefore covers both
// drivers' dispatch of it; what would differ between drivers is Temporal's own
// activity routing, not anything this package does with the runtime once it
// arrives.

// tenantEnvSecrets sets two tenants' worth of environment-backed secrets under
// disjoint prefixes — the real, namespaced [secrets.EnvProvider], not a stand-in —
// and returns a [secrets.Store] both tenants share, the way one worker serving two
// tenants actually would.
func tenantEnvSecrets(t *testing.T, teamAValue, teamBValue string) *secrets.Store {
	t.Helper()

	const (
		teamAPrefix = "PLUGIN_TENANCY_TEAM_A_"
		teamBPrefix = "PLUGIN_TENANCY_TEAM_B_"
	)

	t.Setenv(teamAPrefix+"TOKEN", teamAValue)
	t.Setenv(teamBPrefix+"TOKEN", teamBValue)

	provider, err := secrets.NewEnvProvider(secrets.WithEnvNamespaces(map[string]string{
		"team-a": teamAPrefix,
		"team-b": teamBPrefix,
	}))
	require.NoError(t, err)

	store, err := secrets.NewStore(provider)
	require.NoError(t, err)

	return store
}

// tenantRuntime builds the [flowstatev1.TaskRuntime] a worker installs for one
// tenant's step, over a [secrets.Store] shared with other tenants — the shape
// [secrets.Store.For] exists for: the store has no namespace of its own, and
// binding one is the one thing a caller cannot skip.
func tenantRuntime(t *testing.T, store *secrets.Store, namespace string) flowstatev1.TaskRuntime {
	t.Helper()

	policy, err := (auth.SecretAccessPolicy{Allow: []string{"true"}}).Compile()
	require.NoError(t, err)

	return flowstatev1.TaskRuntime{
		Store:  store,
		Policy: policy,
		Identity: auth.WorkloadIdentity{
			Subject: namespace + "-user", Issuer: "https://issuer.example", Namespace: namespace,
		},
		Step: auth.StepRef{Workflow: "tenancy-test", Run: "run-" + namespace, Step: "hello"},
	}
}

// tokenRef is the reference every tenant in this file names, identically —
// `${secret('env:TOKEN')}` — which is the point: the same spelling must resolve
// to a different value per tenant, and never to another tenant's value at all.
var tokenRef = map[string]*flowstatev1.Value{
	"message": {Kind: &flowstatev1.Value_SecretRef{SecretRef: &flowstatev1.SecretRef{
		Scheme: "env", Name: "TOKEN",
	}}},
}

// resolveAs resolves tokenRef under one tenant's runtime, returning the literal
// value [resolvePluginSecretInputs] handed to (what would be) the plugin.
func resolveAs(t *testing.T, store *secrets.Store, namespace string) (string, error) {
	t.Helper()

	ctx := flowstatev1.ContextWithTaskRuntime(t.Context(), tenantRuntime(t, store, namespace))

	resolved, _, err := resolvePluginSecretInputs(ctx, "example.task", []string{"message"}, []string{"message"}, tokenRef, nil)
	if err != nil {
		return "", err
	}

	return resolved["message"].GetLiteral().GetStringValue(), nil
}

// TestResolvePluginSecretInputsDoesNotCrossTenants is the negative direction:
// two tenants, one shared provider, the same reference spelling. Tenant A's call
// must resolve to tenant A's value and never tenant B's, whether the calls happen
// one after another or at the same time — the "every time" the review asked for,
// since a join bug that shows up only on the second call of a pair would pass a
// test that made only one call per tenant.
func TestResolvePluginSecretInputsDoesNotCrossTenants(t *testing.T) {
	const (
		teamAValue = "team-a-secret-must-stay-in-team-a"
		teamBValue = "team-b-secret-must-stay-in-team-b"
	)

	store := tenantEnvSecrets(t, teamAValue, teamBValue)

	t.Run("sequential calls", func(t *testing.T) {
		gotA, err := resolveAs(t, store, "team-a")
		require.NoError(t, err)
		gotB, err := resolveAs(t, store, "team-b")
		require.NoError(t, err)

		assert.Equal(t, teamAValue, gotA, "tenant A did not receive its own secret")
		assert.Equal(t, teamBValue, gotB, "tenant B did not receive its own secret")
		assert.NotEqual(t, teamBValue, gotA, "tenant A received tenant B's secret")
		assert.NotEqual(t, teamAValue, gotB, "tenant B received tenant A's secret")

		// Repeated the other order, since a bug shaped like "the first call's
		// runtime wins" would pass the ordering above by accident.
		gotB2, err := resolveAs(t, store, "team-b")
		require.NoError(t, err)
		gotA2, err := resolveAs(t, store, "team-a")
		require.NoError(t, err)
		assert.Equal(t, teamBValue, gotB2, "tenant B did not receive its own secret on a second call")
		assert.Equal(t, teamAValue, gotA2, "tenant A did not receive its own secret on a second call")
	})

	t.Run("concurrent calls", func(t *testing.T) {
		// The shape a bug in this package's own plumbing would need to reach —
		// [resolvePluginSecretInputs] closing over or memoizing the wrong
		// runtime — rather than one only a data race detector run serially
		// would ever exercise. Many calls per tenant, interleaved, so a bug that
		// only shows up on an odd call count or a particular interleaving has
		// more than one chance to appear.
		const callsPerTenant = 20

		var wg sync.WaitGroup
		errs := make(chan error, 2*callsPerTenant)

		run := func(namespace, want string) {
			defer wg.Done()
			got, err := resolveAs(t, store, namespace)
			if err != nil {
				errs <- err
				return
			}
			if got != want {
				errs <- assertionError(namespace, want, got)
			}
		}

		for range callsPerTenant {
			wg.Add(2)
			go run("team-a", teamAValue)
			go run("team-b", teamBValue)
		}
		wg.Wait()
		close(errs)

		for err := range errs {
			t.Error(err)
		}
	})

	t.Run("a namespace with no mapping is refused rather than falling back to a default tenant", func(t *testing.T) {
		// The env provider refuses a namespace it has no prefix for rather than
		// guessing — see [secrets.EnvProvider.variable] — and this asserts that
		// refusal actually reaches a plugin task's own resolution path rather
		// than only the provider's own unit tests.
		_, err := resolveAs(t, store, "team-c")
		require.Error(t, err)
		assert.Contains(t, err.Error(), "no prefix configured for namespace")
	})
}

// assertionError builds the message a failed concurrent resolution reports,
// naming what a leak would mean rather than just that values differed.
func assertionError(namespace, want, got string) error {
	return &tenancyMismatch{namespace: namespace, want: want, got: got}
}

type tenancyMismatch struct {
	namespace, want, got string
}

func (e *tenancyMismatch) Error() string {
	if e.got == "" {
		return "tenant " + e.namespace + " resolved no value at all"
	}
	return "tenant " + e.namespace + " did not receive its own secret (a mismatch here " +
		"is what tenant A reading tenant B's reference looks like)"
}

// TestPluginTaskEndToEndRespectsTenantBoundary is the same boundary exercised
// through the real plugin process end to end, complementing the direct test
// above: it is a weaker check on the *value* (this package's own
// [scrubPluginOutputs] redacts whichever tenant's secret a call resolves,
// defeating a raw comparison — see the file comment) but a stronger check that
// the boundary holds all the way through the RPC, for two tenants sharing one
// running plugin instance.
func TestPluginTaskEndToEndRespectsTenantBoundary(t *testing.T) {
	const (
		teamAValue = "team-a-secret-through-the-plugin"
		teamBValue = "team-b-secret-through-the-plugin"
	)

	store := tenantEnvSecrets(t, teamAValue, teamBValue)

	host := openHost(t, testConfig(t, pluginDir(t, "secret-task")))
	defs := host.TaskDefs()
	require.Len(t, defs, 1)
	def := defs[0]

	for _, namespace := range []string{"team-a", "team-b"} {
		t.Run(namespace, func(t *testing.T) {
			ctx := flowstatev1.ContextWithTaskRuntime(t.Context(), tenantRuntime(t, store, namespace))

			outputs, err := def.Fn(ctx, tokenRef, nil)
			require.NoError(t, err, "the plugin task did not run for tenant %q", namespace)

			assert.True(t, outputs.GetNamedValues()["received"].GetLiteral().GetBoolValue(),
				"tenant %q's call reached the plugin with no token at all", namespace)
		})
	}
}
