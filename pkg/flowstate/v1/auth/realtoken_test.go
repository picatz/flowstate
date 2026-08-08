package auth_test

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/url"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/picatz/jose/pkg/jwa"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/picatz/flowstate/pkg/flowstate/v1/auth"
)

// This file is the one test in the package that reaches the network, and it is
// the only one that runs against an issuer nobody here controls. Everything
// else in ci_federation_test.go mints its own tokens from an authtest.Issuer,
// which proves this package's reading of the specification; this proves the
// deployment path, against the provider an operator actually configures.
//
// It runs only inside a GitHub Actions job that asked for `id-token: write`,
// because that permission is what makes the runner's token endpoint answer.
// Everywhere else it skips.

// ciFederationAudience is the audience this test asks the runner to mint a
// token for.
//
// It is deliberately not "flowstate" and not the repository owner's URL. A
// token addressed to a name only this test uses cannot be replayed against a
// deployment that trusts "flowstate", and the default audience a job gets when
// it names none is the owner URL, which the stub tests already show being
// refused. Choosing a distinct value here also means the assertion that the
// audience matched is an assertion about this request rather than about a
// default that happened to line up.
const ciFederationAudience = "flowstate-ci-federation-test"

// githubActionsIssuer is the issuer identifier GitHub Actions puts in the "iss"
// claim of the tokens its runners mint, and the base URL its discovery document
// is published under.
const githubActionsIssuer = "https://token.actions.githubusercontent.com"

// maxTokenResponseBytes bounds the runner's response before it is read into
// memory. A token is a couple of kilobytes; anything approaching this is not a
// token, and an unbounded read of a response body is the mistake this repository
// keeps a rule about.
const maxTokenResponseBytes = 1 << 20 // 1 MiB

// TestRealCITokenVerifies verifies a token minted by GitHub Actions' real OIDC
// provider, discovered live, against a policy of the shape an operator would
// write to federate a repository into a deployment.
//
// The offline coverage of the same ground is in ci_federation_test.go, which
// runs everywhere and does not skip. This test adds exactly one thing that a
// stand-in issuer cannot: that the discovery document, key set, algorithm, and
// claim names the real provider serves today are the ones this package expects.
func TestRealCITokenVerifies(t *testing.T) {
	requestURL := os.Getenv("ACTIONS_ID_TOKEN_REQUEST_URL")
	requestToken := os.Getenv("ACTIONS_ID_TOKEN_REQUEST_TOKEN")
	if requestURL == "" || requestToken == "" {
		// The one environment that must not take this exit is the federation CI
		// job itself, which exists to run the live check and sets the flag below
		// to say so. A skip there is how a permission regression or a renamed
		// endpoint variable turns the gate green while verifying nothing, which
		// is the fail-open shape wearing a skip message.
		if os.Getenv("FLOWSTATE_REQUIRE_REAL_TOKEN") != "" {
			t.Fatal("this job requires the live token check (FLOWSTATE_REQUIRE_REAL_TOKEN is set) and the runner's " +
				"OIDC token endpoint is absent; the job has lost id-token: write, or the endpoint variables were " +
				"renamed, and either way the gate was about to pass without verifying anything")
		}
		t.Skip("no GitHub Actions OIDC token endpoint in the environment (ACTIONS_ID_TOKEN_REQUEST_URL and ACTIONS_ID_TOKEN_REQUEST_TOKEN are set only inside a job granted id-token: write), so there is no real issuer to verify against here; the offline coverage of the same policy shapes is TestCIIssuedTokenVerifies and its neighbours in ci_federation_test.go, which run everywhere")
	}

	// Read from the environment rather than hard-coded, so a fork running this
	// job against its own repository pins its own claim and passes for the same
	// reason this repository does, instead of passing because the assertion was
	// loose.
	repository := os.Getenv("GITHUB_REPOSITORY")
	require.NotEmpty(t, repository, "GITHUB_REPOSITORY is set by every GitHub Actions job; its absence beside a token endpoint means this is not the environment this test can reason about")

	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	rawToken := requestCIToken(ctx, t, requestURL, requestToken, ciFederationAudience)

	policy := auth.Policy{
		Issuers: []auth.TrustedIssuer{{
			Name:       "github-actions",
			Issuer:     githubActionsIssuer,
			Audiences:  []string{ciFederationAudience},
			Algorithms: []jwa.Algorithm{jwa.RS256},
			Require: []auth.ClaimRule{
				auth.RequireClaim("repository", repository),
			},
			Role:      "deployer",
			Namespace: "ci",
			// Minted seconds ago by this test. Ten minutes is slack for a slow
			// runner and clock drift, not a lifetime an operator would grant.
			MaxTokenAge: 10 * time.Minute,
		}},
	}

	// No WithHTTPClient, no WithClock, no stubbed key set: discovery, the key
	// set fetch, and the lifetime check all run the way they run in a
	// deployment. Network to the issuer is a precondition of this job.
	verifier, err := auth.NewOIDCVerifier(policy)
	require.NoError(t, err)

	principal, err := verifier.Verify(ctx, rawToken)
	require.NoError(t, err, "a token this repository's own job minted, for an audience this policy names, must verify")

	assert.Equal(t, githubActionsIssuer, principal.Issuer)
	assert.Equal(t, "github-actions", principal.IssuerName)
	assert.Equal(t, "deployer", principal.Role)
	assert.Equal(t, "ci", principal.Namespace)
	assert.True(t, principal.HasAudience(ciFederationAudience), "audience %q missing from %v", ciFederationAudience, principal.Audience)

	// The subject names a repository and what it was doing, never a person. Its
	// tail differs by event (a ref for a push, "pull_request" for a pull
	// request, an environment for a deployment), so only the part a policy can
	// rely on is asserted.
	assert.True(t, strings.HasPrefix(principal.Subject, "repo:"+repository+":"),
		"subject %q should start with repo:%s:", principal.Subject, repository)

	// The claim the policy rule matched is on the principal afterwards, which is
	// what an authorization decision downstream reads.
	claimed, ok := principal.StringClaim("repository")
	require.True(t, ok, "the repository claim is missing from the verified principal")
	assert.Equal(t, repository, claimed)

	// The claims real policies pin beyond the repository, asserted for shape
	// rather than exact value because their values are event-dependent: `ref`
	// differs between a push and a pull request, and `job_workflow_ref` carries
	// the triggering ref in its tail. What is stable, and what a policy relies
	// on, is that each is present, non-empty, and shaped as documented. Without
	// these, the provider could rename or drop the claims the offline tests and
	// the documented policies require, and this gate would stay green.
	ref, ok := principal.StringClaim("ref")
	require.True(t, ok, "the ref claim is missing; policies pinning a branch cannot be written against this provider anymore")
	assert.True(t, strings.HasPrefix(ref, "refs/"), "ref %q is no longer a fully qualified git ref", ref)

	workflowRef, ok := principal.StringClaim("job_workflow_ref")
	require.True(t, ok, "the job_workflow_ref claim is missing; policies pinning a reusable workflow cannot be written against this provider anymore")
	assert.True(t, strings.HasPrefix(workflowRef, repository+"/"),
		"job_workflow_ref %q no longer starts with the repository, so a policy pinning it would pin the wrong thing", workflowRef)

	runnerEnv, ok := principal.StringClaim("runner_environment")
	require.True(t, ok, "the runner_environment claim is missing; policies distinguishing hosted from self-hosted runners cannot be written anymore")
	assert.Contains(t, []string{"github-hosted", "self-hosted"}, runnerEnv,
		"runner_environment %q is outside the documented vocabulary", runnerEnv)

	t.Run("a verifier pinning another repository refuses the same token", func(t *testing.T) {
		// The negative direction, against the real token rather than a minted
		// one: proof that the rule is doing the refusing, not the signature and
		// not the audience. The repository named here cannot be the one running
		// this job, because it is this repository's name with a suffix no
		// repository name may contain.
		other := repository + "/not-this-one"

		verifier, err := auth.NewOIDCVerifier(auth.Policy{
			Issuers: []auth.TrustedIssuer{{
				Name:       "github-actions-elsewhere",
				Issuer:     githubActionsIssuer,
				Audiences:  []string{ciFederationAudience},
				Algorithms: []jwa.Algorithm{jwa.RS256},
				Require: []auth.ClaimRule{
					auth.RequireClaim("repository", other),
				},
				Namespace: "ci",
			}},
		})
		require.NoError(t, err)

		_, err = verifier.Verify(ctx, rawToken)
		require.Error(t, err)
		assert.ErrorIs(t, err, auth.ErrClaimMismatch)
	})
}

// requestCIToken asks the runner's token endpoint for an OIDC token addressed
// to the given audience, the way any job with id-token: write does.
//
// The endpoint and the request token both come from the environment the runner
// sets; the audience is ours. The response is a JSON object with one field.
func requestCIToken(ctx context.Context, t *testing.T, endpoint, requestToken, audience string) string {
	t.Helper()

	target, err := url.Parse(endpoint)
	require.NoError(t, err, "ACTIONS_ID_TOKEN_REQUEST_URL is not a URL")

	// The endpoint already carries an api-version query parameter, so the
	// audience is added to whatever is there rather than replacing it.
	query := target.Query()
	query.Set("audience", audience)
	target.RawQuery = query.Encode()

	request, err := http.NewRequestWithContext(ctx, http.MethodGet, target.String(), nil)
	require.NoError(t, err)
	request.Header.Set("Authorization", "Bearer "+requestToken)
	request.Header.Set("Accept", "application/json")

	response, err := http.DefaultClient.Do(request)
	require.NoError(t, err, "requesting an OIDC token from the runner")
	defer func() { _ = response.Body.Close() }()

	body, err := io.ReadAll(io.LimitReader(response.Body, maxTokenResponseBytes))
	require.NoError(t, err)
	require.Equal(t, http.StatusOK, response.StatusCode, "runner refused to mint a token: %s", string(body))

	var minted struct {
		Value string `json:"value"`
	}
	require.NoError(t, json.Unmarshal(body, &minted), "runner response is not the JSON object this endpoint documents")
	require.NotEmpty(t, minted.Value, "runner returned an empty token")

	// Never logged, never written to a file, and never put in a failure
	// message: it is a bearer credential for as long as it lives, even one
	// scoped to an audience nothing trusts.
	return minted.Value
}
