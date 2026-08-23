package credentialsource_test

import (
	"strings"
	"testing"
	"time"

	"github.com/picatz/jose/pkg/jwa"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/picatz/flowstate/pkg/flowstate/v1/authtest"
	"github.com/picatz/flowstate/pkg/flowstate/v1/credentialsource"
)

// staticJWT mints a token carrying exactly the given claims, signed with a
// throwaway key.
//
// The signature is never checked by this package — the server does that on
// arrival, against its trust policy — so any key producing a well-formed JWT
// exercises the same path a real platform's token would. What the tests below
// vary is the claims, because the claims are the whole of what a static source
// reads.
func staticJWT(t *testing.T, claims map[string]any) string {
	t.Helper()

	key := authtest.GenerateKey("stub-ci-key", jwa.RS256)

	return key.Sign(
		map[string]any{"typ": "JWT", "alg": "RS256", "kid": key.ID()},
		claims,
	)
}

// gitLabClaims returns the claim set GitLab documents an ID token carrying,
// with the audience and expiry the caller wants to test.
//
// See gitlab.go for the documentation this shape is read from; the claim
// *names* are pinned against a verifying trust policy in
// pkg/flowstate/v1/auth/ci_federation_vendors_test.go, which is where a change
// in what GitLab mints should fail. Here they are only realistic filling
// around the two claims this package actually reads.
func gitLabClaims(audience any, expiresAt time.Time) map[string]any {
	return map[string]any{
		"iss":            "https://gitlab.com",
		"sub":            "project_path:acme/infra:ref_type:branch:ref:main",
		"aud":            audience,
		"exp":            expiresAt.Unix(),
		"iat":            expiresAt.Add(-5 * time.Minute).Unix(),
		"project_path":   "acme/infra",
		"namespace_path": "acme",
		"ref":            "main",
		"ref_type":       "branch",
	}
}

// terraformCloudClaims is the same for an HCP Terraform workload identity
// token.
func terraformCloudClaims(audience any, expiresAt time.Time) map[string]any {
	return map[string]any{
		"iss":                         "https://app.terraform.io",
		"sub":                         "organization:acme:project:infra:workspace:prod:run_phase:apply",
		"aud":                         audience,
		"exp":                         expiresAt.Unix(),
		"iat":                         expiresAt.Add(-30 * time.Minute).Unix(),
		"terraform_organization_name": "acme",
		"terraform_project_name":      "infra",
		"terraform_workspace_name":    "prod",
		"terraform_run_phase":         "apply",
	}
}

// staticSourceCase is one reading of one environment: what is set, what the
// source is asked for, and whether a token may come back.
type staticSourceCase struct {
	name string

	// env is set for the duration of the case, unset variables included so a
	// case says the whole of what it assumes rather than inheriting it.
	env map[string]string

	// audience the caller names, or "" for no audience check.
	audience string

	// wantToken is the raw token the source must return, or "" when the case
	// must refuse. A case never both refuses and returns: the negative
	// direction this table exists for is that a refusal comes back with the
	// zero Token, never with a credential the source could not justify.
	wantToken string

	// wantErrContains are substrings the refusal must name. They are the
	// diagnostic itself under test — a refusal saying only "unusable" tells
	// an author nothing about which line of their job configuration is wrong.
	wantErrContains []string
}

// runStaticSourceCases drives one table against a source built by build.
func runStaticSourceCases(t *testing.T, cases []staticSourceCase, build func(t *testing.T, audience string) credentialsource.Source) {
	t.Helper()

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			for name, value := range tc.env {
				t.Setenv(name, value)
			}

			token, err := build(t, tc.audience).Token(t.Context())

			if tc.wantToken == "" {
				require.Error(t, err)
				assert.ErrorIs(t, err, credentialsource.ErrSourceUnusable)
				assert.True(t, token.IsZero(), "a refusing source must return no token, got %s", token)
				for _, want := range tc.wantErrContains {
					assert.Contains(t, err.Error(), want)
				}
				return
			}

			require.NoError(t, err)
			raw, ok := token.Bearer()
			require.True(t, ok)
			assert.Equal(t, tc.wantToken, raw)
			assert.False(t, token.ExpiresAt.IsZero(),
				"a static CI source reads exp, so the token it returns must know when it dies")
		})
	}
}

func TestGitLabSource(t *testing.T) {
	now := time.Date(2026, 8, 22, 12, 0, 0, 0, time.UTC)
	clock := func() time.Time { return now }

	good := staticJWT(t, gitLabClaims("https://flowstate.example.com", now.Add(10*time.Minute)))
	expired := staticJWT(t, gitLabClaims("https://flowstate.example.com", now.Add(-time.Second)))
	elsewhere := staticJWT(t, gitLabClaims("https://vault.example.com", now.Add(10*time.Minute)))
	multi := staticJWT(t, gitLabClaims(
		[]string{"https://vault.example.com", "https://flowstate.example.com"}, now.Add(10*time.Minute)))
	noExpiry := staticJWT(t, map[string]any{
		"iss": "https://gitlab.com",
		"sub": "project_path:acme/infra:ref_type:branch:ref:main",
		"aud": "https://flowstate.example.com",
		"iat": now.Unix(),
	})

	const inJob = "GITLAB_CI"

	runStaticSourceCases(t, []staticSourceCase{
		{
			name:      "presents the declared ID token",
			env:       map[string]string{inJob: "true", credentialsource.DefaultGitLabIDTokenEnvVar: good},
			audience:  "https://flowstate.example.com",
			wantToken: good,
		},
		{
			name:      "presents it with no audience check when none was asked for",
			env:       map[string]string{inJob: "true", credentialsource.DefaultGitLabIDTokenEnvVar: elsewhere},
			wantToken: elsewhere,
		},
		{
			name:      "accepts an audience array that contains the one named",
			env:       map[string]string{inJob: "true", credentialsource.DefaultGitLabIDTokenEnvVar: multi},
			audience:  "https://flowstate.example.com",
			wantToken: multi,
		},
		{
			name:            "refuses when the job declared no ID token",
			env:             map[string]string{inJob: "true", credentialsource.DefaultGitLabIDTokenEnvVar: ""},
			audience:        "https://flowstate.example.com",
			wantErrContains: []string{"id_tokens", credentialsource.DefaultGitLabIDTokenEnvVar, ".gitlab-ci.yml"},
		},
		{
			name:            "refuses outside a GitLab job, and says so",
			env:             map[string]string{inJob: "", credentialsource.DefaultGitLabIDTokenEnvVar: ""},
			audience:        "https://flowstate.example.com",
			wantErrContains: []string{"GITLAB_CI", "does not look like a GitLab CI job"},
		},
		{
			name: "refuses the removed CI_JOB_JWT_V2 rather than borrowing it",
			env: map[string]string{
				inJob: "true",
				credentialsource.DefaultGitLabIDTokenEnvVar: "",
				credentialsource.LegacyGitLabJWTEnvVar:      good,
			},
			audience:        "https://flowstate.example.com",
			wantErrContains: []string{"CI_JOB_JWT_V2", "removed in GitLab 17.0", "id_tokens"},
		},
		{
			name:            "refuses an expired token instead of presenting it",
			env:             map[string]string{inJob: "true", credentialsource.DefaultGitLabIDTokenEnvVar: expired},
			audience:        "https://flowstate.example.com",
			wantErrContains: []string{"expired at", "issued once"},
		},
		{
			name:            "refuses a token addressed to another relying party",
			env:             map[string]string{inJob: "true", credentialsource.DefaultGitLabIDTokenEnvVar: elsewhere},
			audience:        "https://flowstate.example.com",
			wantErrContains: []string{"https://vault.example.com", "aud:", ".gitlab-ci.yml"},
		},
		{
			name:            "refuses a token with no expiry, because it cannot tell whether it is good",
			env:             map[string]string{inJob: "true", credentialsource.DefaultGitLabIDTokenEnvVar: noExpiry},
			audience:        "https://flowstate.example.com",
			wantErrContains: []string{`no "exp" claim`},
		},
		{
			name:            "refuses a variable that does not hold a JWT",
			env:             map[string]string{inJob: "true", credentialsource.DefaultGitLabIDTokenEnvVar: "not-a-jwt"},
			wantErrContains: []string{"does not hold a JWT"},
		},
		{
			name: "refuses a variable far larger than any token",
			env: map[string]string{
				inJob: "true",
				credentialsource.DefaultGitLabIDTokenEnvVar: strings.Repeat("a", credentialsource.MaxEnvTokenBytes+1),
			},
			wantErrContains: []string{"which is not a token"},
		},
	}, func(t *testing.T, audience string) credentialsource.Source {
		t.Helper()
		source, err := credentialsource.NewGitLabSource(
			credentialsource.WithGitLabAudience(audience),
			credentialsource.WithGitLabClock(clock),
		)
		require.NoError(t, err)
		return source
	})
}

// TestGitLabSource_NamedIDToken covers the part of the mechanism GitLab leaves
// to the author: the variable's name is the `id_tokens:` key they chose, so a
// job that already calls it something else must be able to say so.
func TestGitLabSource_NamedIDToken(t *testing.T) {
	now := time.Now()
	token := staticJWT(t, gitLabClaims("https://flowstate.example.com", now.Add(10*time.Minute)))

	t.Setenv("GITLAB_CI", "true")
	t.Setenv("DEPLOY_ID_TOKEN", token)

	source, err := credentialsource.NewGitLabSource(credentialsource.WithGitLabEnvVar("DEPLOY_ID_TOKEN"))
	require.NoError(t, err)

	got, err := source.Token(t.Context())
	require.NoError(t, err)
	raw, ok := got.Bearer()
	require.True(t, ok)
	assert.Equal(t, token, raw)

	// And the default variable is not consulted as a fallback: a job that
	// names one ID token has not consented to any other being presented. A
	// fresh source, because the one above is holding a token it may still
	// legitimately serve from cache.
	t.Setenv("DEPLOY_ID_TOKEN", "")
	t.Setenv(credentialsource.DefaultGitLabIDTokenEnvVar, token)

	source, err = credentialsource.NewGitLabSource(credentialsource.WithGitLabEnvVar("DEPLOY_ID_TOKEN"))
	require.NoError(t, err)

	_, err = source.Token(t.Context())
	require.Error(t, err)
	assert.ErrorIs(t, err, credentialsource.ErrSourceUnusable)
	assert.Contains(t, err.Error(), "DEPLOY_ID_TOKEN")
}

func TestTerraformCloudSource(t *testing.T) {
	now := time.Date(2026, 8, 22, 12, 0, 0, 0, time.UTC)
	clock := func() time.Time { return now }

	good := staticJWT(t, terraformCloudClaims("https://flowstate.example.com", now.Add(20*time.Minute)))
	expired := staticJWT(t, terraformCloudClaims("https://flowstate.example.com", now.Add(-time.Minute)))
	elsewhere := staticJWT(t, terraformCloudClaims("https://aws.example.com", now.Add(20*time.Minute)))

	const inRun = "TFC_RUN_ID"

	runStaticSourceCases(t, []staticSourceCase{
		{
			name:      "presents the run's workload identity token",
			env:       map[string]string{inRun: "run-abc", credentialsource.DefaultTerraformCloudTokenEnvVar: good},
			audience:  "https://flowstate.example.com",
			wantToken: good,
		},
		{
			name:     "refuses when the workspace set no audience variable",
			env:      map[string]string{inRun: "run-abc", credentialsource.DefaultTerraformCloudTokenEnvVar: ""},
			audience: "https://flowstate.example.com",
			wantErrContains: []string{
				"TFC_WORKLOAD_IDENTITY_AUDIENCE",
				"which is what makes HCP Terraform mint one",
			},
		},
		{
			name:            "refuses outside an HCP Terraform run, and says so",
			env:             map[string]string{inRun: "", credentialsource.DefaultTerraformCloudTokenEnvVar: ""},
			audience:        "https://flowstate.example.com",
			wantErrContains: []string{"TFC_RUN_ID", "does not look like an HCP Terraform run"},
		},
		{
			name:            "refuses a token whose run phase has already timed out",
			env:             map[string]string{inRun: "run-abc", credentialsource.DefaultTerraformCloudTokenEnvVar: expired},
			audience:        "https://flowstate.example.com",
			wantErrContains: []string{"expired at", "issued once"},
		},
		{
			name:            "refuses a token minted for another relying party",
			env:             map[string]string{inRun: "run-abc", credentialsource.DefaultTerraformCloudTokenEnvVar: elsewhere},
			audience:        "https://flowstate.example.com",
			wantErrContains: []string{"https://aws.example.com", "TFC_WORKLOAD_IDENTITY_AUDIENCE"},
		},
	}, func(t *testing.T, audience string) credentialsource.Source {
		t.Helper()
		source, err := credentialsource.NewTerraformCloudSource(
			credentialsource.WithTerraformCloudAudience(audience),
			credentialsource.WithTerraformCloudClock(clock),
		)
		require.NoError(t, err)
		return source
	})
}

// TestTerraformCloudTaggedEnvVar covers the tagged form a workspace uses when
// it needs tokens for more than one relying party.
func TestTerraformCloudTaggedEnvVar(t *testing.T) {
	assert.Equal(t, "TFC_WORKLOAD_IDENTITY_TOKEN_FLOWSTATE", credentialsource.TerraformCloudTaggedEnvVar("FLOWSTATE"))
	assert.Equal(t, "TFC_WORKLOAD_IDENTITY_TOKEN_FLOWSTATE", credentialsource.TerraformCloudTaggedEnvVar("flowstate"),
		"HCP Terraform's variable names are upper case, and an operator who wrote the tag in lower case "+
			"should not be told the token is missing")
	assert.Equal(t, credentialsource.DefaultTerraformCloudTokenEnvVar, credentialsource.TerraformCloudTaggedEnvVar(""))
}

func TestTerraformCloudSource_Tagged(t *testing.T) {
	now := time.Now()
	token := staticJWT(t, terraformCloudClaims("https://flowstate.example.com", now.Add(20*time.Minute)))

	t.Setenv("TFC_RUN_ID", "run-abc")
	t.Setenv("TFC_WORKLOAD_IDENTITY_TOKEN_FLOWSTATE", token)

	source, err := credentialsource.NewTerraformCloudSource(credentialsource.WithTerraformCloudTag("flowstate"))
	require.NoError(t, err)

	got, err := source.Token(t.Context())
	require.NoError(t, err)
	raw, ok := got.Bearer()
	require.True(t, ok)
	assert.Equal(t, token, raw)

	// The refusal for a tagged source names the tagged audience variable, not
	// the untagged one, because that is the workspace setting to add. A fresh
	// source, because the one above may still serve its cached token.
	t.Setenv("TFC_WORKLOAD_IDENTITY_TOKEN_FLOWSTATE", "")

	source, err = credentialsource.NewTerraformCloudSource(credentialsource.WithTerraformCloudTag("flowstate"))
	require.NoError(t, err)

	_, err = source.Token(t.Context())
	require.Error(t, err)
	assert.Contains(t, err.Error(), "TFC_WORKLOAD_IDENTITY_AUDIENCE_FLOWSTATE")
}

// TestStaticSourceRefreshMargin covers what the margin buys a source that
// cannot mint: a parsed token is served from memory until it is within the
// margin of expiring, and re-read from the environment after that — so a
// value replaced in place is noticed, and one that has died is refused rather
// than presented.
func TestStaticSourceRefreshMargin(t *testing.T) {
	start := time.Date(2026, 8, 22, 12, 0, 0, 0, time.UTC)
	now := start
	clock := func() time.Time { return now }

	first := staticJWT(t, gitLabClaims("flowstate", start.Add(5*time.Minute)))
	second := staticJWT(t, gitLabClaims("flowstate", start.Add(30*time.Minute)))

	t.Setenv("GITLAB_CI", "true")
	t.Setenv(credentialsource.DefaultGitLabIDTokenEnvVar, first)

	source, err := credentialsource.NewGitLabSource(
		credentialsource.WithGitLabAudience("flowstate"),
		credentialsource.WithGitLabClock(clock),
		credentialsource.WithGitLabRefreshMargin(time.Minute),
	)
	require.NoError(t, err)

	token, err := source.Token(t.Context())
	require.NoError(t, err)
	raw, _ := token.Bearer()
	assert.Equal(t, first, raw)

	t.Run("serves the cached token while it is outside the margin", func(t *testing.T) {
		// Far enough out that a re-read is not due. Emptying the variable
		// proves the cache answered: a re-read here would refuse.
		now = start.Add(time.Minute)
		t.Setenv(credentialsource.DefaultGitLabIDTokenEnvVar, "")

		token, err := source.Token(t.Context())
		require.NoError(t, err)
		raw, _ := token.Bearer()
		assert.Equal(t, first, raw)
	})

	t.Run("re-reads inside the margin and picks up a replaced value", func(t *testing.T) {
		now = start.Add(4*time.Minute + 30*time.Second)
		t.Setenv(credentialsource.DefaultGitLabIDTokenEnvVar, second)

		token, err := source.Token(t.Context())
		require.NoError(t, err)
		raw, _ := token.Bearer()
		assert.Equal(t, second, raw, "inside the margin the variable is read again, not served from cache")
	})

	t.Run("refuses rather than serving a cached token past its expiry", func(t *testing.T) {
		// The cached token is the long-lived second one; wind past its expiry
		// with the variable still holding it. Nothing may come back.
		now = start.Add(31 * time.Minute)

		_, err := source.Token(t.Context())
		require.Error(t, err)
		assert.ErrorIs(t, err, credentialsource.ErrSourceUnusable)
	})
}
