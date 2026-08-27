package flowstatev1

import (
	"context"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"

	"github.com/picatz/flowstate/pkg/flowstate/v1/auth"
	"github.com/picatz/flowstate/pkg/flowstate/v1/netpolicy"
	"github.com/picatz/flowstate/pkg/flowstate/v1/secrets"
	"github.com/stretchr/testify/require"
)

// countingSecretProvider records how many times it was consulted, so a test
// can assert a denied request never reached it — the ordering half of #963's
// design ("check before minting"), not just the outcome half.
type countingSecretProvider struct {
	value    string
	resolves atomic.Int64
}

func (p *countingSecretProvider) Scheme() string { return "env" }
func (p *countingSecretProvider) Resolve(_ context.Context, req secrets.Request) (secrets.Secret, error) {
	p.resolves.Add(1)
	return secrets.NewSecret(req.Ref, p.value), nil
}

// Test_httpTask_egressCredentials is the reachability proof for #963 half two:
// a Flowfile-expressible `credentials`-scoped egress rule actually gates the
// http task, in both directions, on the same host — the design's own worked
// example (`credentials && !(host in [...])`).
func Test_httpTask_egressCredentials(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))
	t.Cleanup(server.Close)

	// The server is loopback, not "partner-a.example.com" — deliberately
	// outside the allowlist, so a credentialed request to it is exactly what
	// the rule exists to refuse.
	policy, err := netpolicy.New(
		netpolicy.WithAllowLoopback(),
		netpolicy.WithDenyRules(`credentials && !(host in ["partner-a.example.com"])`),
	)
	require.NoError(t, err)
	fn := taskFuncHTTP(policy)

	provider := &countingSecretProvider{value: "secret-material"}
	store, err := secrets.NewStore(provider)
	require.NoError(t, err)
	authPolicy, err := (auth.SecretAccessPolicy{Allow: []string{"true"}}).Compile()
	require.NoError(t, err)
	runtime := TaskRuntime{
		Store:  store,
		Policy: authPolicy,
		Identity: auth.WorkloadIdentity{
			Subject: "test-user", Issuer: "https://issuer.example", Namespace: "test",
		},
		Step: auth.StepRef{Workflow: "test-workflow", Run: "test-run", Step: "fetch"},
	}
	ctx := ContextWithTaskRuntime(t.Context(), runtime)

	t.Run("a bare request reaches the same host", func(t *testing.T) {
		out, err := fn(ctx, map[string]*Value{
			"url": NewValue(server.URL),
		}, nil)
		require.NoError(t, err)
		require.NotNil(t, out)
		require.Equal(t, int64(0), provider.resolves.Load(), "an uncredentialed request never touches the secret provider")
	})

	t.Run("a credentialed request to the same host is denied, before the secret backend is read", func(t *testing.T) {
		_, err := fn(ctx, map[string]*Value{
			"url":    NewValue(server.URL),
			"bearer": {Kind: &Value_SecretRef{SecretRef: &SecretRef{Scheme: "env", Name: "API_TOKEN"}}},
		}, nil)

		var taskErr *TaskError
		require.ErrorAs(t, err, &taskErr)
		require.Equal(t, ErrorKindPolicyDenied, taskErr.Kind)
		require.Equal(t, int64(0), provider.resolves.Load(),
			"the preflight must deny before ResolveSecret ever reaches the backend")
	})
}

// Test_httpTask_egressCredentials_allowlistedHost is the positive half of the
// same rule against a host the allowlist does name, proving the rule is a
// genuine gate rather than an unconditional refusal of every credentialed
// request.
func Test_httpTask_egressCredentials_allowlistedHost(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		require.Equal(t, "Bearer secret-material", req.Header.Get("Authorization"))
		w.WriteHeader(http.StatusOK)
	}))
	t.Cleanup(server.Close)

	policy, err := netpolicy.New(
		netpolicy.WithAllowLoopback(),
		netpolicy.WithDenyRules(`credentials && !(host in ["127.0.0.1"])`),
	)
	require.NoError(t, err)
	fn := taskFuncHTTP(policy)

	provider := &countingSecretProvider{value: "secret-material"}
	store, err := secrets.NewStore(provider)
	require.NoError(t, err)
	authPolicy, err := (auth.SecretAccessPolicy{Allow: []string{"true"}}).Compile()
	require.NoError(t, err)
	runtime := TaskRuntime{
		Store:  store,
		Policy: authPolicy,
		Identity: auth.WorkloadIdentity{
			Subject: "test-user", Issuer: "https://issuer.example", Namespace: "test",
		},
		Step: auth.StepRef{Workflow: "test-workflow", Run: "test-run", Step: "fetch"},
	}
	ctx := ContextWithTaskRuntime(t.Context(), runtime)

	out, err := fn(ctx, map[string]*Value{
		"url":    NewValue(server.URL),
		"bearer": {Kind: &Value_SecretRef{SecretRef: &SecretRef{Scheme: "env", Name: "API_TOKEN"}}},
	}, nil)
	require.NoError(t, err)
	require.NotNil(t, out)
	require.Equal(t, int64(1), provider.resolves.Load(), "the allowlisted host must still resolve its credential exactly once")
}

// Test_taskCarriesCredential pins the single predicate the cleartext refusal
// and the credentials egress fact both read (#963), so the two halves cannot
// drift on what counts as a credential.
func Test_taskCarriesCredential(t *testing.T) {
	partnerAPI := "partner-api"

	require.False(t, taskCarriesCredential(&Task_HTTP_Inputs{}))
	require.True(t, taskCarriesCredential(&Task_HTTP_Inputs{
		Bearer: &Value{Kind: &Value_SecretRef{SecretRef: &SecretRef{Scheme: "env", Name: "API_TOKEN"}}},
	}))
	require.True(t, taskCarriesCredential(&Task_HTTP_Inputs{Credential: &partnerAPI}))
}
