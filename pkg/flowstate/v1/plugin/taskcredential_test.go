package plugin

import (
	"context"
	"testing"
	"time"

	"github.com/picatz/jose/pkg/jwa"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	flowstatev1 "github.com/picatz/flowstate/pkg/flowstate/v1"
	"github.com/picatz/flowstate/pkg/flowstate/v1/auth"
	"github.com/picatz/flowstate/pkg/flowstate/v1/principal"
	"github.com/picatz/flowstate/pkg/flowstate/v1/secrets"
)

// A credential reference is minted for a task's declared secret input and
// nowhere else. These pin the refusals first, in the negative direction, and
// then the one input it is resolved for.

const mintedPluginCredential = "minted-plugin-credential-value"

// bearerExchanger hands back one fixed bearer credential, counting its mints.
type bearerExchanger struct {
	kind  auth.CredentialType
	calls int
}

func (e *bearerExchanger) Name() string { return "fixture" }

func (e *bearerExchanger) Requirement() auth.Requirement {
	return auth.Requirement{Audience: "https://resource.example"}
}

func (e *bearerExchanger) Exchange(context.Context, auth.Assertion) (auth.Credential, error) {
	e.calls++
	return auth.NewCredential(cmpKind(e.kind), time.Now().Add(time.Hour), map[string]string{
		auth.CredentialAccessToken: mintedPluginCredential,
	})
}

func cmpKind(kind auth.CredentialType) auth.CredentialType {
	if kind == "" {
		return auth.CredentialBearer
	}
	return kind
}

// credentialRuntime is a worker runtime whose broker federates one target,
// "anthropic", under the given assumption allow rules.
func credentialRuntime(t *testing.T, exchanger *bearerExchanger, allow ...string) flowstatev1.TaskRuntime {
	t.Helper()

	key, err := auth.GenerateSigningKey("k", jwa.ES256)
	require.NoError(t, err)
	issuer, err := auth.NewIssuer("https://flowstate.example", key)
	require.NoError(t, err)

	broker, err := auth.NewBroker(issuer,
		auth.WithTarget("anthropic", exchanger),
		auth.WithAssumeAllowRules(allow...),
	)
	require.NoError(t, err)

	return flowstatev1.TaskRuntime{
		Broker: broker,
		Identity: auth.WorkloadIdentity{
			Subject: "test-user", Issuer: "https://issuer.example", Namespace: "test",
		},
		Step: auth.StepRef{Workflow: "test-workflow", Run: "test-run", Step: "hello"},
	}
}

func TestResolvePluginSecretInputsRefusesACredentialReference(t *testing.T) {
	t.Parallel()

	ref := flowstatev1.NewCredentialRef("anthropic")

	for name, test := range map[string]struct {
		inputs map[string]*flowstatev1.Value
		want   string
	}{
		"whole, in an undeclared input": {
			inputs: map[string]*flowstatev1.Value{"other": ref},
			want:   "did not declare",
		},
		"nested in a mapping, in a declared input": {
			inputs: map[string]*flowstatev1.Value{"api_key": flowstatev1.NewStructureMap(map[string]*flowstatev1.Value{
				"inner": ref,
			})},
			want: "nested inside a list or a mapping",
		},
		"nested in a list, in an undeclared input": {
			inputs: map[string]*flowstatev1.Value{"other": flowstatev1.NewStructureList(ref)},
			want:   "nested inside a list or a mapping",
		},
	} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()

			exchanger := &bearerExchanger{}
			ctx := flowstatev1.ContextWithTaskRuntime(t.Context(), credentialRuntime(t, exchanger, "true"))

			resolved, scrubber, err := resolvePluginSecretInputs(
				ctx, "example.task", []string{"api_key"}, nil, nil, test.inputs, nil)
			require.Error(t, err)
			assert.Nil(t, resolved, "nothing is handed to the plugin")
			assert.Nil(t, scrubber)
			assert.Contains(t, err.Error(), "credential reference")
			assert.Contains(t, err.Error(), test.want)
			assert.Zero(t, exchanger.calls, "a refused position never mints")

			var taskErr *flowstatev1.TaskError
			require.ErrorAs(t, err, &taskErr)
			assert.Equal(t, flowstatev1.ErrorKindInvalidInput, taskErr.Kind)
			assert.False(t, taskErr.Retryable())
		})
	}
}

func TestResolvePluginSecretInputsMintsADeclaredCredential(t *testing.T) {
	t.Parallel()

	exchanger := &bearerExchanger{}
	ctx := flowstatev1.ContextWithTaskRuntime(t.Context(), credentialRuntime(t, exchanger, `target == "anthropic" && workload.step == "hello"`))

	var registered []secrets.Secret
	resolved, scrubber, err := resolvePluginSecretInputs(ctx, "example.task", []string{"api_key"}, []string{"api_key"}, nil,
		map[string]*flowstatev1.Value{
			"api_key": flowstatev1.NewCredentialRef("anthropic"),
			"message": flowstatev1.NewValue("hello"),
		}, func(s secrets.Secret) { registered = append(registered, s) })
	require.NoError(t, err)

	// The plugin receives the credential as the string a stored secret would be.
	assert.Equal(t, mintedPluginCredential, resolved["api_key"].GetLiteral().GetStringValue())
	assert.Equal(t, "hello", resolved["message"].GetLiteral().GetStringValue())
	assert.Equal(t, 1, exchanger.calls)

	// The scrubber covers it, so a plugin that echoes it back cannot put it in
	// an output.
	assert.NotContains(t, scrubber.Scrub("key="+mintedPluginCredential), mintedPluginCredential)
	err = scrubPluginOutputs(scrubber, &flowstatev1.Node_Outputs{NamedValues: map[string]*flowstatev1.Value{
		"echo": flowstatev1.NewValue("key=" + mintedPluginCredential),
	}})
	require.NoError(t, err)

	// And the host's log scrubber is told about it too.
	require.Len(t, registered, 1)
	assert.Equal(t, mintedPluginCredential, registered[0].Reveal())
}

func TestResolvePluginSecretInputsDeniesACredentialTheAssumptionPolicyRefuses(t *testing.T) {
	t.Parallel()

	for name, test := range map[string]struct {
		runtime func(*testing.T, *bearerExchanger) flowstatev1.TaskRuntime
		target  string
		want    string
	}{
		"a rule that names another step": {
			runtime: func(t *testing.T, e *bearerExchanger) flowstatev1.TaskRuntime {
				return credentialRuntime(t, e, `workload.step == "other"`)
			},
			target: "anthropic",
			want:   "denied by assumption policy",
		},
		"no allow rule": {
			runtime: func(t *testing.T, e *bearerExchanger) flowstatev1.TaskRuntime {
				return credentialRuntime(t, e)
			},
			target: "anthropic",
			want:   "no allow rule",
		},
		"a target the deployment does not federate": {
			runtime: func(t *testing.T, e *bearerExchanger) flowstatev1.TaskRuntime {
				return credentialRuntime(t, e, "true")
			},
			target: "unknown",
			want:   "unknown credential target",
		},
		"a worker with no broker": {
			runtime: func(t *testing.T, e *bearerExchanger) flowstatev1.TaskRuntime {
				runtime := credentialRuntime(t, e, "true")
				runtime.Broker = nil
				return runtime
			},
			target: "anthropic",
			want:   "federation is not configured",
		},
	} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()

			exchanger := &bearerExchanger{}
			ctx := flowstatev1.ContextWithTaskRuntime(t.Context(), test.runtime(t, exchanger))

			resolved, scrubber, err := resolvePluginSecretInputs(ctx, "example.task", []string{"api_key"}, nil, nil,
				map[string]*flowstatev1.Value{"api_key": flowstatev1.NewCredentialRef(test.target)}, nil)
			require.Error(t, err)
			assert.Nil(t, resolved)
			assert.Nil(t, scrubber)
			assert.Contains(t, err.Error(), test.want)
			assert.NotContains(t, err.Error(), mintedPluginCredential)
			assert.Zero(t, exchanger.calls, "a refused request mints nothing")

			var taskErr *flowstatev1.TaskError
			require.ErrorAs(t, err, &taskErr)
			assert.Equal(t, flowstatev1.ErrorKindPolicyDenied, taskErr.Kind)
		})
	}
}

// A credential that is not one string, an AWS session, cannot be a plugin's
// secret input: it has to sign a request, and flattening it would hand a plugin
// a value nothing accepts.
func TestResolvePluginSecretInputsRefusesACredentialWithNoSingleToken(t *testing.T) {
	t.Parallel()

	exchanger := &bearerExchanger{kind: auth.CredentialAWSSession}
	ctx := flowstatev1.ContextWithTaskRuntime(t.Context(), credentialRuntime(t, exchanger, "true"))

	resolved, _, err := resolvePluginSecretInputs(ctx, "example.task", []string{"api_key"}, nil, nil,
		map[string]*flowstatev1.Value{"api_key": flowstatev1.NewCredentialRef("anthropic")}, nil)
	require.Error(t, err)
	assert.Nil(t, resolved)
	assert.Contains(t, err.Error(), "no single token")
	assert.NotContains(t, err.Error(), mintedPluginCredential)
}

// A delegated caller is refused the mint outright: the assertion would have no
// act claim, so it would say the delegator acted alone.
func TestResolvePluginSecretInputsRefusesADelegatedCaller(t *testing.T) {
	t.Parallel()

	exchanger := &bearerExchanger{}
	runtime := credentialRuntime(t, exchanger, "true")
	runtime.Identity.Actors = []principal.Actor{{Issuer: "https://agents.example.com", Subject: "secret-bot"}}
	ctx := flowstatev1.ContextWithTaskRuntime(t.Context(), runtime)

	_, _, err := resolvePluginSecretInputs(ctx, "example.task", []string{"api_key"}, nil, nil,
		map[string]*flowstatev1.Value{"api_key": flowstatev1.NewCredentialRef("anthropic")}, nil)
	require.ErrorIs(t, err, auth.ErrDelegatedCaller)
	assert.Zero(t, exchanger.calls)
}

func TestScrubPluginOutputsRefusesACredentialReference(t *testing.T) {
	t.Parallel()

	for name, value := range map[string]*flowstatev1.Value{
		"bare":   flowstatev1.NewCredentialRef("anthropic"),
		"nested": flowstatev1.NewStructureList(flowstatev1.NewCredentialRef("anthropic")),
	} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()

			err := scrubPluginOutputs(secrets.NewScrubber(), &flowstatev1.Node_Outputs{
				NamedValues: map[string]*flowstatev1.Value{"leaked": value},
			})
			require.Error(t, err)
			assert.Contains(t, err.Error(), `"leaked"`)
			assert.Contains(t, err.Error(), "credential reference")
		})
	}
}
