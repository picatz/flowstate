package flowstatev1_test

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"

	v1 "github.com/picatz/flowstate/pkg/flowstate/v1"
	"github.com/picatz/flowstate/pkg/flowstate/v1/auth"
	"github.com/picatz/flowstate/pkg/flowstate/v1/plugin"
	"github.com/picatz/flowstate/pkg/flowstate/v1/tests"
)

// runPluginIdentityLocal installs a plugin task's context the way the local
// driver's production seam does — cmd/flow/secrets.go's withLocalTaskRuntime —
// and runs [tests.PluginIdentityStep] against it, registered on a private
// [v1.Registry] rather than the process-global one so this test needs no
// coordination with anything else that registers a task for the life of the
// binary.
func runPluginIdentityLocal(t *testing.T, identity auth.WorkloadIdentity) (subject, namespace string, present bool) {
	t.Helper()

	registry := v1.NewRegistry()
	require.NoError(t, registry.Register(tests.PluginIdentityTaskDef(false)))

	ctx := v1.NewContextWithRegistry(context.Background(), registry)

	// The two calls withLocalTaskRuntime makes, in the order it makes them:
	// the wire identity first, unconditionally, then the TaskRuntime a step
	// needing secret authority reads. See cmd/flow/secrets.go.
	ctx = plugin.NewContextWithIdentity(ctx, v1.ProtoWorkloadIdentity(identity))
	ctx = v1.ContextWithTaskRuntime(ctx, v1.TaskRuntime{Identity: identity})

	out, err := v1.Run(ctx, tests.PluginIdentityStep("plugin-identity-local", "call"))
	require.NoError(t, err)

	values := out.GetStepValues()["call"].GetNamedValues()
	return values["subject"].GetLiteral().GetStringValue(),
		values["namespace"].GetLiteral().GetStringValue(),
		values["present"].GetLiteral().GetBoolValue()
}

// TestPluginTaskObservesCallerLocal is one of the two driver callers #235
// asks for: a plugin task run under the local driver observes the run's
// authenticated identity, because withLocalTaskRuntime installs it on every
// step's context rather than only on the ones that also resolve a secret.
// TestPluginTaskObservesCallerDurable in engine/plugin_identity_test.go is
// the other.
func TestPluginTaskObservesCallerLocal(t *testing.T) {
	subject, namespace, present := runPluginIdentityLocal(t, auth.WorkloadIdentity{
		Subject: "svc-reader", Issuer: "https://issuer.example", Namespace: "team-a",
	})

	require.True(t, present, "the plugin task's context carried no identity at all")
	require.Equal(t, "svc-reader", subject)
	require.Equal(t, "team-a", namespace)
}

// TestPluginTaskCallerNotStickyAcrossRunsLocal is the tenancy direction and
// the closure trap #235's fix guidance calls out by name: a plugin's TaskDef
// and its Fn closure are built once, at load, and identity is per run — so
// running the same registered task twice, back to back, under two different
// identities must not leak one run's caller into the other's. A context value
// installed at load time (the closure-at-load trap) would make the second run
// see the first run's caller, or a caller that never changes at all.
func TestPluginTaskCallerNotStickyAcrossRunsLocal(t *testing.T) {
	firstSubject, firstNamespace, _ := runPluginIdentityLocal(t, auth.WorkloadIdentity{
		Subject: "svc-a", Issuer: "https://issuer.example", Namespace: "team-a",
	})
	secondSubject, secondNamespace, _ := runPluginIdentityLocal(t, auth.WorkloadIdentity{
		Subject: "svc-b", Issuer: "https://issuer.example", Namespace: "team-b",
	})

	require.Equal(t, "svc-a", firstSubject)
	require.Equal(t, "team-a", firstNamespace)
	require.Equal(t, "svc-b", secondSubject)
	require.Equal(t, "team-b", secondNamespace)
	require.NotEqual(t, firstNamespace, secondNamespace,
		"two runs with different identities must each see their own namespace, not a value stuck from the first run")
}

// TestPluginTaskCallerExplicitlyEmptyLocal is the negative shape: a run with
// no identity established sends a plugin task a present-but-empty caller
// rather than crashing or fabricating one. [v1.ProtoWorkloadIdentity] never
// returns nil, so `present` is true and every field reads empty — matching
// what a plugin-side [plugin.IdentityFromContext] and the SDK's
// sdk.CallerFromContext both see when nobody authenticated the run.
func TestPluginTaskCallerExplicitlyEmptyLocal(t *testing.T) {
	subject, namespace, present := runPluginIdentityLocal(t, auth.WorkloadIdentity{})

	require.True(t, present, "an unestablished identity must still cross as an explicit empty caller, not as no context value at all")
	require.Empty(t, subject)
	require.Empty(t, namespace)
}
