package flowstatev1

import (
	"context"
	"crypto/ed25519"
	"crypto/rand"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/picatz/flowstate/pkg/flowstate/v1/auth"
	"github.com/picatz/flowstate/pkg/flowstate/v1/secrets"
	"github.com/stretchr/testify/require"
)

type testExchanger struct{ token string }

func (e testExchanger) Name() string { return "test-sts" }
func (e testExchanger) Requirement() auth.Requirement {
	return auth.Requirement{Audience: "https://resource.example"}
}
func (e testExchanger) Exchange(context.Context, auth.Assertion) (auth.Credential, error) {
	return auth.NewCredential(auth.CredentialBearer, time.Now().Add(time.Hour), map[string]string{"access_token": e.token})
}

func testBroker(t *testing.T, token string) *auth.Broker {
	t.Helper()
	_, private, err := ed25519.GenerateKey(rand.Reader)
	require.NoError(t, err)
	key, err := auth.NewSigningKey("test", private)
	require.NoError(t, err)
	issuer, err := auth.NewIssuer("https://flowstate.example", key)
	require.NoError(t, err)
	broker, err := auth.NewBroker(issuer,
		auth.WithTarget("partner-api", testExchanger{token: token}),
		auth.WithAssumeAllowRules("true"))
	require.NoError(t, err)
	return broker
}

type fixedSecretProvider struct{ value string }

func (p fixedSecretProvider) Scheme() string { return "env" }
func (p fixedSecretProvider) Resolve(_ context.Context, req secrets.Request) (secrets.Secret, error) {
	return secrets.NewSecret(req.Ref, p.value), nil
}

func testTaskRuntime(t *testing.T, material string) TaskRuntime {
	t.Helper()
	store, err := secrets.NewStore(fixedSecretProvider{value: material})
	require.NoError(t, err)
	policy, err := (auth.SecretAccessPolicy{Allow: []string{"true"}}).Compile()
	require.NoError(t, err)
	return TaskRuntime{
		Store:  store,
		Policy: policy,
		Identity: auth.WorkloadIdentity{
			Subject: "test-user", Issuer: "https://issuer.example", Namespace: "test",
		},
		Step: auth.StepRef{Workflow: "test-workflow", Run: "test-run", Step: "fetch"},
	}
}

func TestHTTPBearerResolvesOnlyAtExecution(t *testing.T) {
	const material = "secret-that-must-not-enter-an-output"
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		require.Equal(t, "Bearer "+material, req.Header.Get("Authorization"))
		w.Header().Set("X-Reflected-Authorization", "Bearer "+material)
		_, _ = w.Write([]byte("reflected: " + material))
	}))
	defer server.Close()

	ctx := ContextWithTaskRuntime(t.Context(), testTaskRuntime(t, material))
	out, err := taskFuncHTTP(testEgressPolicy(t))(ctx, map[string]*Value{
		"url":    NewValue(server.URL),
		"bearer": {Kind: &Value_SecretRef{SecretRef: &SecretRef{Scheme: "env", Name: "API_TOKEN"}}},
	}, nil)
	require.NoError(t, err)
	require.NotContains(t, out.String(), material)
	require.Contains(t, out.String(), secrets.Redacted)
}

func TestHTTPBearerFailsClosedWithoutRuntime(t *testing.T) {
	_, err := taskFuncHTTP(testEgressPolicy(t))(t.Context(), map[string]*Value{
		"url":    NewValue("https://example.com"),
		"bearer": {Kind: &Value_SecretRef{SecretRef: &SecretRef{Scheme: "env", Name: "API_TOKEN"}}},
	}, nil)
	require.ErrorContains(t, err, "secret access is not configured")
}

func TestHTTPBearerPolicyRunsBeforeProvider(t *testing.T) {
	runtime := testTaskRuntime(t, "must-not-resolve")
	denied, err := (auth.SecretAccessPolicy{Deny: []string{"true"}, Allow: []string{"true"}}).Compile()
	require.NoError(t, err)
	runtime.Policy = denied

	_, err = taskFuncHTTP(testEgressPolicy(t))(ContextWithTaskRuntime(t.Context(), runtime), map[string]*Value{
		"url":    NewValue("https://example.com"),
		"bearer": {Kind: &Value_SecretRef{SecretRef: &SecretRef{Scheme: "env", Name: "API_TOKEN"}}},
	}, nil)
	require.ErrorIs(t, err, auth.ErrSecretDenied)
}

func TestHTTPCredentialIsMintedAndContainedInsideExecution(t *testing.T) {
	const material = "jit-token-that-must-not-enter-history"
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		require.Equal(t, "Bearer "+material, req.Header.Get("Authorization"))
		_, _ = w.Write([]byte("reflected " + material))
	}))
	defer server.Close()

	runtime := testTaskRuntime(t, "unused")
	runtime.Broker = testBroker(t, material)
	out, err := taskFuncHTTP(testEgressPolicy(t))(ContextWithTaskRuntime(t.Context(), runtime), map[string]*Value{
		"url":        NewValue(server.URL),
		"credential": NewValue("partner-api"),
	}, nil)
	require.NoError(t, err)
	require.NotContains(t, out.String(), material)
	require.Contains(t, out.String(), secrets.Redacted)
}
