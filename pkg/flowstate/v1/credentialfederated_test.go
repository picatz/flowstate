package flowstatev1_test

import (
	"testing"

	"github.com/stretchr/testify/require"

	v1 "github.com/picatz/flowstate/pkg/flowstate/v1"
	"github.com/picatz/flowstate/pkg/flowstate/v1/internal/conformance"
)

// A credential's declaration decides which kind of reference binds it: a
// federated credential takes a credential reference and a stored one a secret
// reference, neither standing in for the other. The same rule holds at the
// binding, at a step's own input and (in the plugin package) at dispatch; these
// are the first two, asked of a registry that holds the fixture plugins
// ([registerBoundCredentialTask] installs both).

func federatedWorkflow(binding *v1.Value, steps ...*v1.Node) *v1.Workflow {
	credentials := map[string]*v1.Value(nil)
	if binding != nil {
		credentials = map[string]*v1.Value{conformance.FederatedCredentialName: binding}
	}

	return &v1.Workflow{
		Name:               "federated",
		Profile:            v1.CurrentProfile,
		PluginRequirements: []*v1.PluginRequirement{conformance.FederatedCredentialRequirement(credentials)},
		Steps:              steps,
	}
}

func federatedStep(id string, inputs map[string]*v1.Value) *v1.Node {
	return &v1.Node{Id: id, Kind: &v1.Node_Task{Task: &v1.Task{Name: conformance.FederatedCredentialTaskName, Inputs: inputs}}}
}

func TestAFederatedBindingExpandsAsACredentialReference(t *testing.T) {
	registerBoundCredentialTask(t)

	wf := federatedWorkflow(v1.NewCredentialRef("partner"), federatedStep("a", map[string]*v1.Value{"note": v1.NewLiteral("x")}))
	require.NoError(t, v1.BindPluginCredentials(wf, v1.DefaultRegistry()))
	require.NoError(t, v1.CheckInputClaims(wf, v1.DefaultRegistry()))

	require.Equal(t, "partner", wf.GetSteps()[0].GetTask().GetInputs()["token"].GetCredentialRef().GetTarget())
}

func TestABindingOfTheWrongKindIsRefusedWithoutEchoingIt(t *testing.T) {
	registerBoundCredentialTask(t)

	secret := &v1.Value{Kind: &v1.Value_SecretRef{SecretRef: &v1.SecretRef{Scheme: "env", Name: "LEAKY_NAME"}}}
	for _, test := range []struct {
		name string
		wf   *v1.Workflow
		want string
	}{
		{
			name: "a stored secret bound to a federated credential",
			wf:   federatedWorkflow(secret, federatedStep("a", map[string]*v1.Value{"note": v1.NewLiteral("x")})),
			want: `plugin "federated" credential "partner_token": is declared federated`,
		},
		{
			name: "a credential reference bound to a stored credential",
			wf:   boundWorkflowWith(v1.NewCredentialRef("LEAKY_TARGET")),
			want: `plugin "bound" credential "api_token": is not declared federated`,
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			err := v1.BindPluginCredentials(test.wf, v1.DefaultRegistry())
			require.ErrorContains(t, err, test.want)
			require.NotContains(t, err.Error(), "LEAKY")
		})
	}
}

func boundWorkflowWith(binding *v1.Value) *v1.Workflow {
	return &v1.Workflow{
		Name:    "bound",
		Profile: v1.CurrentProfile,
		PluginRequirements: []*v1.PluginRequirement{conformance.BoundCredentialRequirement(map[string]*v1.Value{
			conformance.BoundCredentialName: binding,
		})},
		Steps: []*v1.Node{useStep("a", map[string]*v1.Value{"note": v1.NewLiteral("x")})},
	}
}

// Binding a federated credential is not what makes a step's own reference
// acceptable: an override is held to the declaration, with or without a binding.
func TestAStepsOwnReferenceIsHeldToTheDeclaration(t *testing.T) {
	registerBoundCredentialTask(t)

	stored := &v1.Value{Kind: &v1.Value_SecretRef{SecretRef: &v1.SecretRef{Scheme: "env", Name: "T"}}}
	for _, test := range []struct {
		name string
		wf   *v1.Workflow
		want string
	}{
		{
			name: "a stored secret in a federated credential's input",
			wf:   federatedWorkflow(nil, federatedStep("a", map[string]*v1.Value{"token": stored})),
			want: `step "a": task "federated.use" input "token" receives the plugin's federated credential "partner_token"`,
		},
		{
			name: "a stored credential's input given a credential reference",
			wf: &v1.Workflow{Name: "x", Profile: v1.CurrentProfile, Steps: []*v1.Node{
				useStep("a", map[string]*v1.Value{"token": v1.NewCredentialRef("partner")}),
			}},
			want: `input "token" receives the plugin's credential "api_token", which is not federated`,
		},
		{
			name: "a federated credential's input given a literal",
			wf:   federatedWorkflow(nil, federatedStep("a", map[string]*v1.Value{"token": v1.NewLiteral("plain-text")})),
			want: `requires input "token" to be a whole secret reference`,
		},
		{
			name: "a federated credential's input given an expression",
			wf:   federatedWorkflow(nil, federatedStep("a", map[string]*v1.Value{"token": v1.NewExpr("inputs.token")})),
			want: `requires input "token" to be a whole secret reference`,
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			require.NoError(t, v1.BindPluginCredentials(test.wf, v1.DefaultRegistry()))
			err := v1.CheckInputClaims(test.wf, v1.DefaultRegistry())
			require.ErrorContains(t, err, test.want)
			require.NotContains(t, err.Error(), "plain-text")
		})
	}
}

// A registry that was never told a credential is federated holds it to the stored
// kind, which is the closed direction: unlisted means not federated.
func TestACredentialNoTaskListsAsFederatedIsAStoredSecretOnly(t *testing.T) {
	def := conformance.FederatedCredentialTaskDef()
	require.True(t, v1.CredentialFederated(def, conformance.FederatedCredentialName))

	def.FederatedCredentials = nil
	require.False(t, v1.CredentialFederated(def, conformance.FederatedCredentialName))
	require.False(t, v1.CredentialReferenceMatches(false, v1.NewCredentialRef("partner")))
	require.False(t, v1.CredentialReferenceMatches(true, &v1.Value{Kind: &v1.Value_SecretRef{SecretRef: &v1.SecretRef{Scheme: "env", Name: "T"}}}))
	require.False(t, v1.CredentialReferenceMatches(true, nil))
	require.False(t, v1.CredentialReferenceMatches(false, nil))
}

func TestFederatedCredentialsOfReadsTheDeclarationForTheClaimedCredentialsOnly(t *testing.T) {
	claims := []v1.InputClaim{{Name: "token", Credential: "a"}, {Name: "other", Credential: "b"}, {Name: "plain"}}
	declarations := []*v1.CredentialDeclaration{
		{Name: "a", Federated: true}, {Name: "b"}, {Name: "c", Federated: true},
	}

	require.Equal(t, []string{"a"}, v1.FederatedCredentialsOf(claims, declarations))
	require.Empty(t, v1.FederatedCredentialsOf(nil, declarations))
	require.Empty(t, v1.FederatedCredentialsOf(claims, nil))
}
