package auth_test

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/picatz/jose/pkg/jwa"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/picatz/flowstate/pkg/flowstate/v1/auth"
	"github.com/picatz/flowstate/pkg/flowstate/v1/authtest"
	"github.com/picatz/flowstate/pkg/flowstate/v1/principal"
)

func TestSubjectAt(t *testing.T) {
	t.Parallel()

	identity := testIdentity()
	ref := testStepRef()

	for name, test := range map[string]struct {
		identity auth.WorkloadIdentity
		level    auth.SubjectLevel
		want     string
	}{
		"unset is the step":      {identity, "", "flowstate:acme/prod/deploy-service/push-image"},
		"step":                   {identity, auth.SubjectLevelStep, "flowstate:acme/prod/deploy-service/push-image"},
		"workflow drops step":    {identity, auth.SubjectLevelWorkflow, "flowstate:acme/prod/deploy-service/_any"},
		"deployment drops both":  {identity, auth.SubjectLevelDeployment, "flowstate:acme/prod/_any/_any"},
		"local rehearsal marker": {auth.NewLocalWorkloadIdentity("s", "i", "acme", "prod", nil), auth.SubjectLevelWorkflow, "flowstate:_local/acme/prod/deploy-service/_any"},
		"unset deployment": {
			auth.WorkloadIdentity{Subject: "s", Issuer: "i", Namespace: "acme"},
			auth.SubjectLevelDeployment, "flowstate:acme/_default/_any/_any",
		},
	} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()

			got, err := test.identity.SubjectAt(ref, test.level)
			require.NoError(t, err)
			assert.Equal(t, test.want, got)
		})
	}

	// The same arity as the full subject, so a pattern written for it lines up.
	full, err := identity.SubjectFor(ref)
	require.NoError(t, err)
	reduced, err := identity.SubjectAt(ref, auth.SubjectLevelDeployment)
	require.NoError(t, err)
	assert.Equal(t, len(splitSubject(full)), len(splitSubject(reduced)))
}

func splitSubject(subject string) []string {
	var parts []string
	start := 0
	for i := range len(subject) {
		if subject[i] == '/' {
			parts = append(parts, subject[start:i])
			start = i + 1
		}
	}
	return append(parts, subject[start:])
}

func TestSubjectAtRefusals(t *testing.T) {
	t.Parallel()

	identity := testIdentity()

	for name, test := range map[string]struct {
		ref   auth.StepRef
		level auth.SubjectLevel
	}{
		"a misspelled level, never read as the default": {testStepRef(), "Workflow"},
		"an unknown level":                          {testStepRef(), "namespace"},
		"a step named like a dropped component":     {auth.StepRef{Workflow: "w", Step: "_any"}, auth.SubjectLevelWorkflow},
		"a workflow named like a dropped component": {auth.StepRef{Workflow: "_any", Step: "s"}, auth.SubjectLevelDeployment},
		"a component spanning two levels":           {auth.StepRef{Workflow: "a/b", Step: "s"}, auth.SubjectLevelDeployment},
	} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()

			subject, err := identity.SubjectAt(test.ref, test.level)
			require.ErrorIs(t, err, auth.ErrInvalidIdentity)
			assert.Empty(t, subject)
		})
	}
}

func TestParseFederationPolicySubjectLevel(t *testing.T) {
	t.Parallel()

	const head = `
issuer: https://flowstate.example.com
targets:
  - name: azure
`
	const body = `
    client_credentials:
      token_url: https://login.microsoftonline.com/00000000-0000-0000-0000-000000000000/oauth2/v2.0/token
      client_id: 11111111-1111-1111-1111-111111111111
      audience: api://AzureADTokenExchange
`

	for _, level := range []auth.SubjectLevel{auth.SubjectLevelStep, auth.SubjectLevelWorkflow, auth.SubjectLevelDeployment} {
		t.Run("accepts "+string(level), func(t *testing.T) {
			t.Parallel()

			policy, err := auth.ParseFederationPolicy([]byte(head + "    subject_level: " + string(level) + body))
			require.NoError(t, err)
			assert.Equal(t, level, policy.Targets[0].SubjectLevel)
		})
	}

	t.Run("unset stays unset", func(t *testing.T) {
		t.Parallel()

		policy, err := auth.ParseFederationPolicy([]byte(head + body))
		require.NoError(t, err)
		assert.Empty(t, policy.Targets[0].SubjectLevel)
	})

	for name, level := range map[string]string{
		"a misspelling":         "workflows",
		"another case":          "Workflow",
		"a level that is a key": "namespace",
	} {
		t.Run("refuses "+name, func(t *testing.T) {
			t.Parallel()

			_, err := auth.ParseFederationPolicy([]byte(head + "    subject_level: " + level + body))
			require.Error(t, err)
			require.ErrorIs(t, err, auth.ErrInvalidPolicy)
		})
	}

	t.Run("a broker refuses a level for a target it does not hold", func(t *testing.T) {
		t.Parallel()

		key, err := auth.GenerateSigningKey("k", jwa.ES256)
		require.NoError(t, err)
		issuer, err := auth.NewIssuer("https://flowstate.example.com", key)
		require.NoError(t, err)

		_, err = auth.NewBroker(issuer, auth.WithTargetSubjectLevel("ghost", auth.SubjectLevelWorkflow))
		require.ErrorIs(t, err, auth.ErrInvalidPolicy)
		assert.Contains(t, err.Error(), "ghost")

		_, err = auth.NewBroker(issuer,
			auth.WithTarget("real", &subjectEchoExchanger{}),
			auth.WithTargetSubjectLevel("real", "tenant"))
		require.ErrorIs(t, err, auth.ErrInvalidPolicy)
	})
}

// subjectEchoExchanger is a relying party that hands back the subject it was
// asked about as the bearer credential, and counts how many it minted.
type subjectEchoExchanger struct {
	calls atomic.Int32
}

func (e *subjectEchoExchanger) Name() string { return "echo" }

func (e *subjectEchoExchanger) Requirement() auth.Requirement {
	return auth.Requirement{Audience: "https://resource.example"}
}

func (e *subjectEchoExchanger) Exchange(_ context.Context, assertion auth.Assertion) (auth.Credential, error) {
	e.calls.Add(1)
	return auth.NewCredential(auth.CredentialBearer, time.Now().Add(time.Hour), map[string]string{
		auth.CredentialAccessToken: assertion.Subject,
	})
}

func newEchoBroker(t *testing.T, level auth.SubjectLevel, allow string) (*auth.Broker, *subjectEchoExchanger) {
	t.Helper()

	key, err := auth.GenerateSigningKey("k", jwa.ES256)
	require.NoError(t, err)
	issuer, err := auth.NewIssuer("https://flowstate.example.com", key, auth.WithDeclaredClaims("repository"))
	require.NoError(t, err)

	exchanger := &subjectEchoExchanger{}
	options := []auth.BrokerOption{
		auth.WithTarget("echo", exchanger),
		auth.WithAssumeAllowRules(allow),
	}
	if level != "" {
		options = append(options, auth.WithTargetSubjectLevel("echo", level))
	}
	broker, err := auth.NewBroker(issuer, options...)
	require.NoError(t, err)

	return broker, exchanger
}

// The level changes what the relying party reads and not what Flowstate decides:
// a rule that names the step still gates the step, at every level.
func TestSubjectLevelLeavesTheAssumptionPolicyPerStep(t *testing.T) {
	t.Parallel()

	for _, level := range []auth.SubjectLevel{"", auth.SubjectLevelStep, auth.SubjectLevelWorkflow, auth.SubjectLevelDeployment} {
		t.Run("level "+string(level), func(t *testing.T) {
			t.Parallel()

			broker, exchanger := newEchoBroker(t, level, `target == "echo" && workload.step == "push-image"`)

			credential, err := broker.Credential(t.Context(), testIdentity(), testStepRef(), "echo")
			require.NoError(t, err)
			token, _ := credential.Bearer()

			want, err := testIdentity().SubjectAt(testStepRef(), level)
			require.NoError(t, err)
			assert.Equal(t, want, token, "the relying party sees the subject at the configured level")

			// The rule's `workload.subject` is the step's, whatever the level.
			other := auth.StepRef{Workflow: "deploy-service", Run: "run-1", Step: "notify"}
			_, err = broker.Credential(t.Context(), testIdentity(), other, "echo")
			require.ErrorIs(t, err, auth.ErrAssumeDenied)
			assert.Equal(t, int32(1), exchanger.calls.Load(), "a refused step mints nothing")
		})
	}
}

// Two callers share a workflow-level subject, so the subject cannot be what keeps
// their credentials apart: the cache has to, or a credential minted for one
// caller is served to a run acting for another.
func TestSubjectLevelDoesNotShareCredentialsAcrossCallers(t *testing.T) {
	t.Parallel()

	broker, exchanger := newEchoBroker(t, auth.SubjectLevelDeployment, "true")

	first := testIdentity()
	second := testIdentity()
	second.Subject = "repo:other/org:ref:refs/heads/main"

	// Different workflows too, so the two share nothing but the subject.
	refA := auth.StepRef{Workflow: "deploy-service", Run: "run-a", Step: "push-image"}
	refB := auth.StepRef{Workflow: "other-flow", Run: "run-b", Step: "other-step"}

	a, err := broker.Credential(t.Context(), first, refA, "echo")
	require.NoError(t, err)
	b, err := broker.Credential(t.Context(), second, refB, "echo")
	require.NoError(t, err)

	tokenA, _ := a.Bearer()
	tokenB, _ := b.Bearer()
	assert.Equal(t, tokenA, tokenB, "one deployment-level subject")
	assert.Equal(t, "flowstate:acme/prod/_any/_any", tokenA)
	assert.Equal(t, int32(2), exchanger.calls.Load(), "each caller minted its own credential")

	// The same caller again is served from cache, so the cache is working and
	// the two mints above were not an accident of it being off.
	_, err = broker.Credential(t.Context(), first, refA, "echo")
	require.NoError(t, err)
	assert.Equal(t, int32(2), exchanger.calls.Load())
}

func TestBrokerRefusesADelegatedCallerAtEveryLevel(t *testing.T) {
	t.Parallel()

	for _, level := range []auth.SubjectLevel{"", auth.SubjectLevelWorkflow, auth.SubjectLevelDeployment} {
		broker, exchanger := newEchoBroker(t, level, "true")

		delegated := testIdentity()
		delegated.Actors = []principal.Actor{{Issuer: "https://agents.example.com", Subject: "bot"}}

		_, err := broker.Credential(t.Context(), delegated, testStepRef(), "echo")
		require.ErrorIs(t, err, auth.ErrDelegatedCaller, "level %q", level)

		_, err = broker.Token(t.Context(), delegated, testStepRef(), "echo")
		require.ErrorIs(t, err, auth.ErrDelegatedCaller, "level %q", level)

		assert.Zero(t, exchanger.calls.Load())
	}
}

func TestBrokerTokenRefusesWhatIsNotABearer(t *testing.T) {
	t.Parallel()

	broker, _ := newEchoBroker(t, "", "true")

	token, err := broker.Token(t.Context(), testIdentity(), testStepRef(), "echo")
	require.NoError(t, err)
	assert.Equal(t, "flowstate:acme/prod/deploy-service/push-image", token)

	_, err = broker.Token(t.Context(), testIdentity(), testStepRef(), "ghost")
	require.ErrorIs(t, err, auth.ErrUnknownTarget)

	key, err := auth.GenerateSigningKey("k", jwa.ES256)
	require.NoError(t, err)
	issuer, err := auth.NewIssuer("https://flowstate.example.com", key, auth.WithDeclaredClaims("repository"))
	require.NoError(t, err)
	session, err := auth.NewBroker(issuer,
		auth.WithTarget("aws", sessionExchanger{}),
		auth.WithAssumeAllowRules("true"))
	require.NoError(t, err)

	_, err = session.Token(t.Context(), testIdentity(), testStepRef(), "aws")
	var failed *auth.AssumptionFailedError
	require.ErrorAs(t, err, &failed, "decided, then failed: the policy permitted it")
	assert.Contains(t, err.Error(), "no single token")
}

type sessionExchanger struct{}

func (sessionExchanger) Name() string { return "aws-session" }

func (sessionExchanger) Requirement() auth.Requirement {
	return auth.Requirement{Audience: "sts.amazonaws.com"}
}

func (sessionExchanger) Exchange(context.Context, auth.Assertion) (auth.Credential, error) {
	return auth.NewCredential(auth.CredentialAWSSession, time.Now().Add(time.Hour), map[string]string{
		"access_key_id": "AKIA", "secret_access_key": "secret", "session_token": "token",
	})
}

// TestAzureFederatedCredentialAcceptsAWorkflowLevelSubject is the reason the level
// exists. An Azure federated identity credential holds one exact issuer, one
// exact subject and an audience, and an application has few of them, so a
// subject per step cannot be configured. The relying party here is a stand-in for
// Entra's token endpoint: it verifies the RFC 7523 client assertion against the
// keys Flowstate publishes and then matches the credential exactly, as Entra
// does.
func TestAzureFederatedCredentialAcceptsAWorkflowLevelSubject(t *testing.T) {
	t.Parallel()

	const (
		audience = "api://AzureADTokenExchange"
		clientID = "11111111-1111-1111-1111-111111111111"
	)

	clock := authtest.NewClock(referenceTime)

	var (
		mu      sync.RWMutex
		handler http.Handler
	)
	identityServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		mu.RLock()
		current := handler
		mu.RUnlock()
		current.ServeHTTP(w, r)
	}))
	t.Cleanup(identityServer.Close)

	// The federated identity credential configured on the app registration.
	type fic struct{ issuer, subject, audience string }
	var configured fic

	entra := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		require.NoError(t, r.ParseForm())

		verifier, err := auth.NewOIDCVerifier(
			auth.Policy{Issuers: []auth.TrustedIssuer{{
				Actions:   []string{},
				Name:      "flowstate",
				Issuer:    configured.issuer,
				Audiences: []string{configured.audience},
			}}},
			auth.WithClock(clock.Now),
			auth.WithEgressPolicy(authtest.EgressPolicy()),
		)
		require.NoError(t, err)

		reply := func(status int, body map[string]any) {
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(status)
			require.NoError(t, json.NewEncoder(w).Encode(body))
		}

		principal, err := verifier.Verify(r.Context(), r.PostForm.Get("client_assertion"))
		if err != nil {
			reply(http.StatusBadRequest, map[string]any{"error": "invalid_client", "error_description": "AADSTS700021: client assertion is not signed by a trusted issuer"})
			return
		}

		// Exact string, no wildcard and no prefix: this is Entra's matching.
		if principal.Subject != configured.subject {
			reply(http.StatusBadRequest, map[string]any{"error": "invalid_client", "error_description": "AADSTS700213: no matching federated identity record"})
			return
		}
		reply(http.StatusOK, map[string]any{"access_token": "entra-token", "token_type": "Bearer", "expires_in": 3600})
	}))
	t.Cleanup(entra.Close)

	build := func(t *testing.T, levelLine, allow string) *auth.Broker {
		t.Helper()

		policy, err := auth.ParseFederationPolicy([]byte(`
issuer: ` + identityServer.URL + `
tenants: [acme]
declared_claims: [repository]
allow:
  - '` + allow + `'
targets:
  - name: azure
` + levelLine + `
    client_credentials:
      token_url: ` + entra.URL + `/oauth2/v2.0/token
      client_id: ` + clientID + `
      audience: ` + audience + `
`))
		require.NoError(t, err)

		key, err := auth.GenerateSigningKey("2026-10", jwa.ES256)
		require.NoError(t, err)

		broker, err := policy.Broker(key,
			auth.WithFederationClock(clock.Now),
			auth.WithFederationEgressPolicy(authtest.EgressPolicy()),
			auth.WithFederationTenant("acme"))
		require.NoError(t, err)

		mu.Lock()
		handler = http.StripPrefix("/tenants/acme", broker.Issuer().Handler())
		mu.Unlock()

		return broker
	}

	const workflowOnly = `target == "azure" && workload.workflow == "deploy-service"`

	configured = fic{
		issuer:   identityServer.URL + "/tenants/acme",
		subject:  "flowstate:acme/prod/deploy-service/_any",
		audience: audience,
	}

	// Subtests share the one identity endpoint, so they run in order.
	t.Run("two steps of the workflow both match the one credential", func(t *testing.T) {
		broker := build(t, "    subject_level: workflow", workflowOnly)

		for _, step := range []string{"push-image", "notify"} {
			ref := auth.StepRef{Workflow: "deploy-service", Run: "run-1", Step: step}
			credential, err := broker.Credential(t.Context(), testIdentity(), ref, "azure")
			require.NoError(t, err, "step %s", step)

			token, ok := credential.Bearer()
			require.True(t, ok)
			assert.Equal(t, "entra-token", token)
		}
	})

	t.Run("a step of another workflow matches nothing", func(t *testing.T) {
		ref := auth.StepRef{Workflow: "other-flow", Run: "run-1", Step: "push-image"}

		// Flowstate's own rule refuses it before anything is minted.
		strict := build(t, "    subject_level: workflow", workflowOnly)
		_, err := strict.Credential(t.Context(), testIdentity(), ref, "azure")
		require.ErrorIs(t, err, auth.ErrAssumeDenied)

		// With Flowstate's rule relaxed, Entra's exact match still refuses it:
		// the subject names a different workflow.
		relaxed := build(t, "    subject_level: workflow", `target == "azure"`)
		_, err = relaxed.Credential(t.Context(), testIdentity(), ref, "azure")
		require.ErrorIs(t, err, auth.ErrExchangeFailed)
		assert.Contains(t, err.Error(), "AADSTS700213")
	})

	t.Run("the default subject is the client id and matches no workflow credential", func(t *testing.T) {
		broker := build(t, "", workflowOnly)

		ref := auth.StepRef{Workflow: "deploy-service", Run: "run-1", Step: "push-image"}
		_, err := broker.Credential(t.Context(), testIdentity(), ref, "azure")
		require.ErrorIs(t, err, auth.ErrExchangeFailed)
		assert.Contains(t, err.Error(), "AADSTS")
	})

	t.Run("an explicit step level is the per-step subject and does not match", func(t *testing.T) {
		broker := build(t, "    subject_level: step", workflowOnly)

		ref := auth.StepRef{Workflow: "deploy-service", Run: "run-1", Step: "push-image"}
		_, err := broker.Credential(t.Context(), testIdentity(), ref, "azure")
		require.ErrorIs(t, err, auth.ErrExchangeFailed)
		assert.Contains(t, err.Error(), "AADSTS700213")
	})

	t.Run("a deployment-level credential matches every workflow of the deployment", func(t *testing.T) {
		configured.subject = "flowstate:acme/prod/_any/_any"
		broker := build(t, "    subject_level: deployment", workflowOnly)

		ref := auth.StepRef{Workflow: "deploy-service", Run: "run-1", Step: "push-image"}
		_, err := broker.Credential(t.Context(), testIdentity(), ref, "azure")
		require.NoError(t, err)
	})
}

// A workflow or step named like a dropped component is refused by SubjectFor
// itself, so the unlevelled minting path cannot collide with a levelled one.
func TestSubjectForRefusesTheReservedComponent(t *testing.T) {
	t.Parallel()

	identity := testIdentity()

	for name, ref := range map[string]auth.StepRef{
		"step":     {Workflow: "w", Step: "_any"},
		"workflow": {Workflow: "_any", Step: "s"},
	} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()

			subject, err := identity.SubjectFor(ref)
			require.ErrorIs(t, err, auth.ErrInvalidIdentity)
			assert.Empty(t, subject)
		})
	}
}
