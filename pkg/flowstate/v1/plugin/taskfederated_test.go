package plugin

import (
	"context"
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	pluginv1 "github.com/picatz/flowstate/pkg/flowstate/plugin/v1"
	flowstatev1 "github.com/picatz/flowstate/pkg/flowstate/v1"
	"github.com/picatz/flowstate/pkg/flowstate/v1/audit"
	"github.com/picatz/flowstate/pkg/flowstate/v1/auth"
)

// A credential input takes the kind of reference its plugin's declaration says,
// and the host checks it at dispatch: a specification can reach a worker without
// the compiler or the server's admission in front of it, and the worker is the
// last place the wrong kind can be refused before it resolves anything.

func federatedDeclarations(federated ...string) []*flowstatev1.CredentialDeclaration {
	out := credentialDeclarations("api", "other")
	for _, d := range out {
		d.Federated = d.Name == "api" && len(federated) > 0
	}

	return out
}

func TestATaskDefCarriesTheFederatedDeclarationOfTheCredentialsItClaims(t *testing.T) {
	t.Parallel()

	for _, tc := range []struct {
		name      string
		declared  []*flowstatev1.CredentialDeclaration
		claims    string
		federated []string
	}{
		{name: "a federated credential", declared: federatedDeclarations("api"), claims: "api", federated: []string{"api"}},
		{name: "a stored credential", declared: federatedDeclarations(), claims: "api"},
		{name: "a stored credential beside a federated one", declared: federatedDeclarations("api"), claims: "other"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			p := &Plugin{name: "example", manifest: &pluginv1.PluginManifest{Credentials: tc.declared}}
			def, err := p.taskDef(credentialTask("post", tc.claims), Config{}.withDefaults())
			require.NoError(t, err)
			assert.Equal(t, tc.federated, def.FederatedCredentials)
		})
	}
}

func claimed(federated bool) map[string]inputCredential {
	return map[string]inputCredential{"api_key": {name: "api", federated: federated}}
}

var storedSecret = flowstatev1.Value{Kind: &flowstatev1.Value_SecretRef{SecretRef: &flowstatev1.SecretRef{Scheme: "env", Name: "LEAKY_NAME"}}}

func TestDispatchRefusesTheWrongKindOfReferenceForACredentialInput(t *testing.T) {
	t.Parallel()

	secretRef := &storedSecret
	credentialRef := flowstatev1.NewCredentialRef("LEAKY_TARGET")

	for _, tc := range []struct {
		name      string
		federated bool
		value     *flowstatev1.Value
		want      string
	}{
		{name: "a stored secret in a federated credential's input", federated: true, value: secretRef, want: "federated credential"},
		{name: "a credential reference in a stored credential's input", value: credentialRef, want: "not federated"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			exchanger := &bearerExchanger{}
			ctx := flowstatev1.ContextWithTaskRuntime(t.Context(), credentialRuntime(t, exchanger, "true"))

			resolved, scrubber, err := resolvePluginSecretInputs(ctx, "example.task", []string{"api_key"}, []string{"api_key"},
				claimed(tc.federated), map[string]*flowstatev1.Value{"api_key": tc.value}, nil)
			require.Error(t, err)
			assert.Nil(t, resolved)
			assert.Nil(t, scrubber)
			assert.Contains(t, err.Error(), tc.want)
			assert.NotContains(t, err.Error(), "LEAKY", "the refusal named the reference written")
			assert.Zero(t, exchanger.calls, "a refused reference never reaches the broker")

			var taskErr *flowstatev1.TaskError
			require.ErrorAs(t, err, &taskErr)
			assert.Equal(t, flowstatev1.ErrorKindInvalidInput, taskErr.Kind)
			assert.False(t, taskErr.Retryable())
		})
	}
}

// The credential and task the policy reads are the claim's, not anything the
// spec says, and a rule pinned to them decides at the very seam the value comes
// out of.
func TestDispatchNamesTheTaskAndCredentialToTheAssumptionPolicy(t *testing.T) {
	t.Parallel()

	for _, tc := range []struct {
		name string
		rule string
		want bool
	}{
		{name: "pinned to the claimed credential and task", rule: `task == "example.task" && credential.plugin == "example" && credential.name == "api"`, want: true},
		{name: "pinned to another credential", rule: `credential.name == "other"`},
		{name: "pinned to another plugin", rule: `credential.plugin == "slack"`},
		{name: "pinned to another task", rule: `task == "example.other"`},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			exchanger := &bearerExchanger{}
			ctx := flowstatev1.ContextWithTaskRuntime(t.Context(), credentialRuntime(t, exchanger, tc.rule))

			resolved, _, err := resolvePluginSecretInputs(ctx, "example.task", []string{"api_key"}, []string{"api_key"},
				claimed(true), map[string]*flowstatev1.Value{"api_key": flowstatev1.NewCredentialRef("anthropic")}, nil)
			if tc.want {
				require.NoError(t, err)
				assert.Equal(t, mintedPluginCredential, resolved["api_key"].GetLiteral().GetStringValue())

				return
			}
			require.Error(t, err)
			assert.Nil(t, resolved)
			assert.Zero(t, exchanger.calls, "a refused use never mints")
		})
	}
}

func TestDispatchNamesTheTaskAndCredentialToTheSecretPolicy(t *testing.T) {
	store := tenantEnvSecrets(t, "team-a-secret", "team-b-secret")

	for _, tc := range []struct {
		name  string
		allow string
		deny  string
		want  bool
	}{
		{name: "pinned to the claimed credential and task", allow: `secret.name == "TOKEN" && credential.plugin == "example" && credential.name == "api" && task == "example.task"`, want: true},
		{name: "a different credential name", allow: `credential.name == "other"`},
		{name: "denied by the task", allow: "true", deny: `task == "example.task"`},
		{name: "denied by the credential", allow: "true", deny: `credential.plugin == "example" && credential.name == "api"`},
		{name: "a deny on another task leaves it alone", allow: "true", deny: `task == "example.delete"`, want: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			rules := auth.SecretAccessPolicy{Allow: []string{tc.allow}}
			if tc.deny != "" {
				rules.Deny = []string{tc.deny}
			}
			policy, err := rules.Compile()
			require.NoError(t, err)

			runtime := tenantRuntime(t, store, "team-a")
			runtime.Policy = policy
			ctx := flowstatev1.ContextWithTaskRuntime(t.Context(), runtime)

			resolved, _, err := resolvePluginSecretInputs(ctx, "example.task", []string{"message"}, []string{"message"},
				map[string]inputCredential{"message": {name: "api"}}, tokenRef, nil)
			if tc.want {
				require.NoError(t, err)
				assert.Equal(t, "team-a-secret", resolved["message"].GetLiteral().GetStringValue())

				return
			}
			require.Error(t, err)
			assert.Nil(t, resolved)
			assert.Contains(t, err.Error(), "denied by secret access policy")
		})
	}
}

type captureSink struct {
	mu      sync.Mutex
	records []*flowstatev1.AuditRecord
}

func (s *captureSink) Emit(_ context.Context, record *flowstatev1.AuditRecord) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.records = append(s.records, record)

	return nil
}

// The trail names what the policy was asked about: the task and the declared
// credential, and nothing that is a reference or a value.
func TestTheAuditRecordNamesTheTaskAndCredential(t *testing.T) {
	store := tenantEnvSecrets(t, "team-a-secret", "team-b-secret")
	sink := &captureSink{}
	recorder, err := audit.NewRecorder(audit.WithoutStderr(), audit.WithEmitter(sink), audit.Required())
	require.NoError(t, err)

	ctx := flowstatev1.ContextWithTaskRuntime(t.Context(), tenantRuntime(t, store, "team-a"))
	ctx = flowstatev1.NewContextWithEnforcementAuditor(ctx, recorder)

	_, _, err = resolvePluginSecretInputs(ctx, "example.task", []string{"message"}, []string{"message"},
		map[string]inputCredential{"message": {name: "api"}}, tokenRef, nil)
	require.NoError(t, err)

	require.Len(t, sink.records, 1)
	record := sink.records[0]
	assert.Equal(t, "example.task", record.GetTask())
	assert.Equal(t, "example/api", record.GetCredential())
	assert.Equal(t, flowstatev1.AuditEnforcementPoint_AUDIT_ENFORCEMENT_POINT_SECRET_ACCESS, record.GetEnforcementPoint())
	assert.Equal(t, "env:TOKEN", record.GetResourceKey())
}

// A registry rebuilt from a catalog holds a binding to the declaration a running
// plugin's registry does: the federated flag rides on the plugin's credentials,
// not on a task, and is read back onto the tasks that claim them.
func TestACatalogCarriesTheFederatedDeclarationOntoTheTasksThatClaimIt(t *testing.T) {
	t.Parallel()

	p := &Plugin{name: "example", manifest: &pluginv1.PluginManifest{Credentials: federatedDeclarations("api")}}
	post, err := p.taskDef(credentialTask("post", "api"), Config{}.withDefaults())
	require.NoError(t, err)
	spare, err := p.taskDef(credentialTask("update", "other"), Config{}.withDefaults())
	require.NoError(t, err)

	catalog := catalogOf(t, "example", post, spare)
	catalog.Plugins[0].Credentials = federatedDeclarations("api")

	defs, err := TaskDefsFromCatalog(catalog, Config{})
	require.NoError(t, err)
	require.Len(t, defs, 2)
	assert.Equal(t, post.FederatedCredentials, defs[0].FederatedCredentials)
	assert.Equal(t, []string{"api"}, defs[0].FederatedCredentials)
	assert.Empty(t, defs[1].FederatedCredentials, "a task claiming a stored credential was marked federated")

	// A catalog that drops the flag is a registry that holds the credential to
	// the stored kind: unlisted is not federated.
	catalog.Plugins[0].Credentials = federatedDeclarations()
	defs, err = TaskDefsFromCatalog(catalog, Config{})
	require.NoError(t, err)
	assert.Empty(t, defs[0].FederatedCredentials)
}
