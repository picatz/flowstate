package auth_test

import (
	"net/http"
	"strings"
	"testing"

	"github.com/picatz/flowstate/pkg/flowstate/v1/auth"
	"github.com/picatz/flowstate/pkg/flowstate/v1/authtest"
	"github.com/picatz/jose/pkg/jwt"
	"github.com/stretchr/testify/require"
)

// TestLocalIdentitySubjectDiffersFromServerAttested is the negative direction
// of gap 1 in #243: `flow run local --identity-key <prod key> --as-namespace
// acme --as-deployment prod` must not mint an assertion byte-indistinguishable
// from a server-attested run. A trust policy an operator wrote for the
// server-attested subject must not match what a local rehearsal mints, on any
// relying party that compares the subject by prefix or by exact match.
func TestLocalIdentitySubjectDiffersFromServerAttested(t *testing.T) {
	ref := auth.StepRef{Workflow: "deploy-service", Run: "run-1", Step: "push-image"}

	serverAttested := auth.IdentityFromPrincipal(
		auth.Principal{Subject: "repo:acme/payments:ref:refs/heads/main", Issuer: "https://token.actions.githubusercontent.com"},
		"acme", "prod",
	)
	serverSubject, err := serverAttested.SubjectFor(ref)
	require.NoError(t, err)
	require.Equal(t, "flowstate:acme/prod/deploy-service/push-image", serverSubject)

	// The same subject a local operator can dial in with --identity-key,
	// --as-subject, --as-issuer, --as-namespace and --as-deployment.
	local := auth.NewLocalWorkloadIdentity(
		"repo:acme/payments:ref:refs/heads/main", "https://token.actions.githubusercontent.com",
		"acme", "prod", nil,
	)
	localSubject, err := local.SubjectFor(ref)
	require.NoError(t, err)

	require.NotEqual(t, serverSubject, localSubject,
		"a local rehearsal identity must mint a subject that differs from a server-attested one built from the same fields")
	require.Equal(t, "flowstate:_local/acme/prod/deploy-service/push-image", localSubject)

	// A trust policy written for the server-attested subject, expressed the two
	// ways a relying party actually compares one: exact match, and prefix
	// match at a separator (the shape #243 documents as the correct one).
	require.NotEqual(t, serverSubject, localSubject, "exact match must not admit the local rehearsal")
	require.False(t, strings.HasPrefix(localSubject, serverSubject+"/"),
		"a prefix rule scoped to the server-attested namespace must not match the local rehearsal's subject")
}

// TestLocalComponentCannotBeForgedFromNamespace checks that "_local" cannot be
// reached by any operator-chosen namespace claim, the same way [defaultComponent]
// cannot: [auth.ValidateNamespace] forbids the underscore that both reserved
// components begin with, so a namespace literally spelled "_local" is refused
// before it can collide with the marker [NewLocalWorkloadIdentity] sets.
func TestLocalComponentCannotBeForgedFromNamespace(t *testing.T) {
	identity := auth.WorkloadIdentity{
		Subject: "s", Issuer: "https://idp.example.com",
		Namespace: "_local", Deployment: "prod",
	}

	_, err := identity.SubjectFor(auth.StepRef{Workflow: "deploy", Step: "push"})
	require.ErrorIs(t, err, auth.ErrInvalidIdentity,
		"a namespace claim of \"_local\" must be refused, not accepted as if it were the reserved marker")
}

// TestRunModeClaimCannotBeCarried checks that "run_mode" is reserved the same
// way "namespace", "sub", and every other claim an [Issuer] sets itself is: a
// carried claim of that name is refused by [WorkloadIdentity.Validate], which
// is the same check that already stops --as-claim from setting "namespace" or
// "sub". There is no second check to write for "run_mode" — it goes on the
// existing reservedClaims list [Issuer.mintFor] and [WorkloadIdentity.Validate]
// both already consult.
func TestRunModeClaimCannotBeCarried(t *testing.T) {
	identity := auth.NewLocalWorkloadIdentity(
		"s", "https://idp.example.com", "acme", "prod",
		map[string]string{"run_mode": "server"},
	)

	err := identity.Validate()
	require.ErrorIs(t, err, auth.ErrInvalidIdentity)
	require.ErrorContains(t, err, "run_mode")
}

// TestRunModeClaimReflectsConstructor checks that a minted assertion's
// "run_mode" claim is driven by which constructor built the identity, not by
// anything the caller supplied — the belt to the subject's braces, for a
// relying party that can read claims (GCP, Anthropic, OpenAI) even though AWS
// STS cannot.
func TestRunModeClaimReflectsConstructor(t *testing.T) {
	clock := authtest.NewClock(referenceTime)
	issuer, _ := newIssuer(t, clock)

	t.Run("server-attested", func(t *testing.T) {
		assertion, err := issuer.Mint(t.Context(), testIdentity(), testStepRef(), "https://as.example.com")
		require.NoError(t, err)

		token, err := jwt.Parse(assertion.Token())
		require.NoError(t, err)

		mode, err := token.Claims.Get("run_mode")
		require.NoError(t, err)
		require.Equal(t, "server", mode)
	})

	t.Run("local rehearsal", func(t *testing.T) {
		local := auth.NewLocalWorkloadIdentity(
			testIdentity().Subject, testIdentity().Issuer, testIdentity().Namespace, testIdentity().Deployment, nil,
		)

		assertion, err := issuer.Mint(t.Context(), local, testStepRef(), "https://as.example.com")
		require.NoError(t, err)

		token, err := jwt.Parse(assertion.Token())
		require.NoError(t, err)

		mode, err := token.Claims.Get("run_mode")
		require.NoError(t, err)
		require.Equal(t, "local", mode)

		require.Equal(t, "flowstate:_local/acme/prod/deploy-service/push-image", assertion.Subject)
	})
}

// TestLocalRunAssumptionPolicyIsUnchanged checks rehearsal fidelity: a local
// run's namespace claim and the workload attributes an assumption rule
// evaluates against must be identical to a server-attested run's, so a local
// rehearsal exercises Flowstate's own assumption policy exactly as production
// would. Only the subject gains the "_local" segment — the CEL rule below
// matches on workload.namespace, which #243 requires stay unchanged, and the
// same rule is what decides whether the credential request is allowed at all.
func TestLocalRunAssumptionPolicyIsUnchanged(t *testing.T) {
	clock := authtest.NewClock(referenceTime)
	issuer, _ := newIssuer(t, clock)

	party := newRelyingParty(t, func(w http.ResponseWriter, r *http.Request, body recordedRequest) {
		writeJSON(t, w, http.StatusOK, map[string]any{
			"access_token": "downstream-token",
			"token_type":   "Bearer",
			"expires_in":   3600,
		})
	})

	exchanger, err := auth.NewTokenExchanger(auth.TokenExchangeConfig{
		TokenURL: party.url + "/token",
		Audience: "https://as.example.com",
		Clock:    clock.Now,
	})
	require.NoError(t, err)

	broker, err := auth.NewBroker(issuer,
		auth.WithTarget("aws-prod", exchanger),
		auth.WithAssumeAllowRules(`workload.namespace == "acme" && workload.deployment == "prod"`),
		auth.WithBrokerClock(clock.Now),
	)
	require.NoError(t, err)

	server := auth.IdentityFromPrincipal(
		auth.Principal{Subject: "repo:acme/payments:ref:refs/heads/main", Issuer: "https://token.actions.githubusercontent.com"},
		"acme", "prod",
	)
	local := auth.NewLocalWorkloadIdentity(
		"repo:acme/payments:ref:refs/heads/main", "https://token.actions.githubusercontent.com", "acme", "prod", nil,
	)

	// Both identities satisfy the same assumption rule, evaluated against the
	// same namespace and deployment: a local rehearsal is not exempted from
	// the policy, and does not fail it either, only because it is local.
	_, err = broker.Credential(t.Context(), server, testStepRef(), "aws-prod")
	require.NoError(t, err, "the server-attested identity must satisfy the rule")

	_, err = broker.Credential(t.Context(), local, testStepRef(), "aws-prod")
	require.NoError(t, err, "the local identity must satisfy the identical rule identically — rehearsal fidelity")
}
