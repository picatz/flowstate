package flowstatev1_test

import (
	"sync"
	"testing"

	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"

	v1 "github.com/picatz/flowstate/pkg/flowstate/v1"
	"github.com/picatz/flowstate/pkg/flowstate/v1/internal/conformance"
)

func compositionFixture(t *testing.T) (*v1.Workflow, []v1.TaskDef, *v1.PluginCatalog, *v1.Workflow_StepOutputs, *[]string) {
	t.Helper()
	var mu sync.Mutex
	ledger := []string{}
	wf, defs, catalog, expected, err := conformance.CompositionPrototype(func(event string) {
		mu.Lock()
		defer mu.Unlock()
		ledger = append(ledger, event)
	})
	require.NoError(t, err)
	return wf, defs, catalog, expected, &ledger
}

func resolveComposition(t *testing.T, wf *v1.Workflow, catalog *v1.PluginCatalog, registry *v1.Registry) {
	t.Helper()
	require.NoError(t, v1.ResolveCapabilityBindings(wf, map[string]string{
		"billing": "billing-environment-stable", "support": "support-environment-stable",
	}, catalog))
	require.NoError(t, v1.ResolvePlugins(wf, catalog))
	require.NoError(t, v1.ResolveTaskCapabilities(wf, registry))
}

func TestCapabilityParametersBindAndForwardWithoutChangingTheLibrary(t *testing.T) {
	wf, defs, catalog, expected, ledger := compositionFixture(t)
	beforeBilling := proto.Clone(wf.GetSteps()[0].GetCall().GetWorkflow().GetSteps()[0].GetCall().GetWorkflow()).(*v1.Workflow)
	beforeSupport := proto.Clone(wf.GetSteps()[1].GetCall().GetWorkflow().GetSteps()[0].GetCall().GetWorkflow()).(*v1.Workflow)
	require.True(t, proto.Equal(beforeBilling, beforeSupport), "the two call sites did not start with the same library")

	registry := v1.NewRegistry()
	for _, def := range defs {
		require.NoError(t, registry.Register(def))
	}
	resolveComposition(t, wf, catalog, registry)

	billingLeaf := wf.GetSteps()[0].GetCall().GetWorkflow().GetSteps()[0].GetCall().GetWorkflow()
	supportLeaf := wf.GetSteps()[1].GetCall().GetWorkflow().GetSteps()[0].GetCall().GetWorkflow()
	require.Equal(t, "billing-environment.provision", billingLeaf.GetSteps()[0].GetTask().GetName())
	require.Equal(t, "support-environment.provision", supportLeaf.GetSteps()[0].GetTask().GetName())
	require.Equal(t, catalog.GetCapabilityBindings()[0].GetBindingId(), billingLeaf.GetResolvedCapabilityBindings()[0].GetBindingId())
	require.Equal(t, catalog.GetCapabilityBindings()[0].GetRevision(), billingLeaf.GetResolvedCapabilityBindings()[0].GetRevision())

	out, err := v1.Run(v1.NewContextWithRegistry(t.Context(), registry), wf)
	require.NoError(t, err)
	require.True(t, proto.Equal(expected, out))
	require.ElementsMatch(t, []string{
		"billing-environment:provision", "billing-environment:release",
		"support-environment:provision", "support-environment:release",
	}, *ledger)
}

func TestCapabilityBindingFailuresAreAtomicAndFailClosed(t *testing.T) {
	t.Run("missing catalog", func(t *testing.T) {
		err := v1.ResolveCapabilityBindings(&v1.Workflow{Name: "empty"}, nil, nil)
		require.ErrorContains(t, err, "claims schema version 0")
	})

	t.Run("missing root selection", func(t *testing.T) {
		wf, _, catalog, _, _ := compositionFixture(t)
		before := proto.Clone(wf).(*v1.Workflow)
		err := v1.ResolveCapabilityBindings(wf, map[string]string{"billing": "billing-environment-stable"}, catalog)
		require.ErrorContains(t, err, `capability "support", which was not bound`)
		require.True(t, proto.Equal(before, wf), "a refused resolution changed the workflow")
	})

	t.Run("missing forwarding grant", func(t *testing.T) {
		wf, _, catalog, _, _ := compositionFixture(t)
		delete(wf.GetSteps()[0].GetCall().CapabilityArguments, "environment")
		err := v1.ResolveCapabilityBindings(wf, map[string]string{
			"billing": "billing-environment-stable", "support": "support-environment-stable",
		}, catalog)
		require.ErrorContains(t, err, `capability "environment" is not explicitly forwarded`)
	})

	t.Run("undeclared ambient capability", func(t *testing.T) {
		wf, _, catalog, _, _ := compositionFixture(t)
		leaf := wf.GetSteps()[0].GetCall().GetWorkflow().GetSteps()[0].GetCall().GetWorkflow()
		leaf.GetSteps()[0].GetTask().Name = "audit.write"
		err := v1.ResolveCapabilityBindings(wf, map[string]string{
			"billing": "billing-environment-stable", "support": "support-environment-stable",
		}, catalog)
		require.ErrorContains(t, err, `uses undeclared capability "audit"`)
	})

	t.Run("contract schema mismatch", func(t *testing.T) {
		wf, _, catalog, _, _ := compositionFixture(t)
		catalog.GetPlugins()[0].GetTasks()[0].Summary = "changed contract"
		err := v1.ResolveCapabilityBindings(wf, map[string]string{
			"billing": "billing-environment-stable", "support": "support-environment-stable",
		}, catalog)
		require.ErrorContains(t, err, "provider contract digest")
	})

	t.Run("stale binding revision", func(t *testing.T) {
		wf, _, catalog, _, _ := compositionFixture(t)
		catalog.GetCapabilityBindings()[0].TrustTier++
		err := v1.ResolveCapabilityBindings(wf, map[string]string{
			"billing": "billing-environment-stable", "support": "support-environment-stable",
		}, catalog)
		require.ErrorContains(t, err, "revision is")
		require.ErrorContains(t, err, "want")
	})

	t.Run("plugin requirements cannot smuggle authority through a wrapper", func(t *testing.T) {
		wf, _, catalog, _, _ := compositionFixture(t)
		wrapper := wf.GetSteps()[0].GetCall().GetWorkflow()
		wrapper.PluginRequirements = []*v1.PluginRequirement{{Name: "audit", MinimumVersion: "v1.0.0"}}
		err := v1.ResolveCapabilityBindings(wf, map[string]string{
			"billing": "billing-environment-stable", "support": "support-environment-stable",
		}, catalog)
		require.ErrorContains(t, err, "receives explicit capabilities and also declares `plugins:`")
	})

	t.Run("legacy callee keeps its direct plugin task", func(t *testing.T) {
		wf, _, catalog, _, _ := compositionFixture(t)
		legacy := &v1.Workflow{
			Name:               "legacy-library",
			PluginRequirements: []*v1.PluginRequirement{{Name: "audit", MinimumVersion: "v1.0.0"}},
			Steps: []*v1.Node{{
				Id: "write", Kind: &v1.Node_Task{Task: &v1.Task{Name: "audit.write"}},
			}},
		}
		wf.Steps = append(wf.Steps, &v1.Node{
			Id: "legacy", Kind: &v1.Node_Call{Call: &v1.Call{Workflow: legacy}},
		})

		err := v1.ResolveCapabilityBindings(wf, map[string]string{
			"billing": "billing-environment-stable", "support": "support-environment-stable",
		}, catalog)

		require.NoError(t, err)
		require.Equal(t, "audit.write", legacy.GetSteps()[0].GetTask().GetName())
	})

	t.Run("legacy callee cannot use an undeclared plugin task", func(t *testing.T) {
		wf, _, catalog, _, _ := compositionFixture(t)
		legacy := &v1.Workflow{
			Name: "legacy-library",
			Steps: []*v1.Node{{
				Id: "write", Kind: &v1.Node_Task{Task: &v1.Task{Name: "audit.write"}},
			}},
		}
		wf.Steps = append(wf.Steps, &v1.Node{
			Id: "legacy", Kind: &v1.Node_Call{Call: &v1.Call{Workflow: legacy}},
		})

		err := v1.ResolveCapabilityBindings(wf, map[string]string{
			"billing": "billing-environment-stable", "support": "support-environment-stable",
		}, catalog)

		require.ErrorContains(t, err, `uses undeclared capability "audit"`)
	})

	t.Run("unknown claims schema", func(t *testing.T) {
		wf, _, catalog, _, _ := compositionFixture(t)
		catalog.ClaimsSchemaVersion = v1.CurrentClaimsSchemaVersion + 1
		err := v1.ResolveCapabilityBindings(wf, map[string]string{
			"billing": "billing-environment-stable", "support": "support-environment-stable",
		}, catalog)
		require.ErrorContains(t, err, "claims schema version")
	})
}

func TestCapabilityContractDigestIgnoresProviderQualifierButNotClaims(t *testing.T) {
	left := []*v1.TaskDescription{{Name: "left.provision", SecretInputs: []string{"token"}}}
	right := []*v1.TaskDescription{{Name: "right.provision", SecretInputs: []string{"token"}}}
	leftDigest, err := v1.CapabilityContractDigest(left)
	require.NoError(t, err)
	rightDigest, err := v1.CapabilityContractDigest(right)
	require.NoError(t, err)
	require.Equal(t, leftDigest, rightDigest)

	right[0].SecretInputs = nil
	rightDigest, err = v1.CapabilityContractDigest(right)
	require.NoError(t, err)
	require.NotEqual(t, leftDigest, rightDigest, "a security-claim change did not change the contract")

	right[0].SecretInputs = []string{"token"}
	right[0].InputDescriptor = []byte("provider-specific descriptor encoding")
	right[0].InputMessage = "provider.v1.Request"
	rightDigest, err = v1.CapabilityContractDigest(right)
	require.NoError(t, err)
	require.Equal(t, leftDigest, rightDigest, "raw descriptor identity changed the stable contract")

	_, err = v1.CapabilityContractDigest([]*v1.TaskDescription{{Name: ".provision"}})
	require.ErrorContains(t, err, "empty qualifier")
}
