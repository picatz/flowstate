package credentialsource

import (
	"os"
	"strings"
	"time"
)

// How an HCP Terraform (formerly Terraform Cloud) run gets an OIDC token, and
// where this package's assumptions come from.
//
// Like GitLab and unlike GitHub Actions, there is nothing to ask: setting the
// workspace variable TFC_WORKLOAD_IDENTITY_AUDIENCE causes the run to be given
// a workload identity token in TFC_WORKLOAD_IDENTITY_TOKEN before it starts. A
// workspace that needs tokens for several relying parties sets tagged pairs —
// TFC_WORKLOAD_IDENTITY_AUDIENCE_<TAG> produces
// TFC_WORKLOAD_IDENTITY_TOKEN_<TAG> — where the tag is letters, numbers and
// underscores, and may not be the reserved word TYPE.
//
// Sources, read 2026-08-22:
// https://developer.hashicorp.com/terraform/cloud-docs/workspaces/dynamic-provider-credentials/manual-generation
// https://developer.hashicorp.com/terraform/cloud-docs/workspaces/dynamic-provider-credentials/workload-identity-tokens
//
// From the second page, for the claim shape pinned in
// pkg/flowstate/v1/auth/ci_federation_vendors_test.go: the issuer is the full
// URL of the HCP Terraform or Terraform Enterprise instance that signed the
// token ("https://app.terraform.io" for HCP Terraform), the subject of a
// workspace run is
// "organization:{org}:project:{project}:workspace:{workspace}:run_phase:{phase}",
// and a run token expires at the timeout of the run phase it was issued for.
// The run_phase claim is the useful one for a trust policy: it is what lets
// `plan` carry a read-only role and `apply` a writing one from an ordinary
// claim rule.

// Environment variables this source knows about.
const (
	// DefaultTerraformCloudTokenEnvVar holds the token for the untagged
	// audience — the one TFC_WORKLOAD_IDENTITY_AUDIENCE produces.
	DefaultTerraformCloudTokenEnvVar = "TFC_WORKLOAD_IDENTITY_TOKEN"

	// TerraformCloudTokenEnvVarPrefix is what a tagged token's variable name
	// starts with; the tag follows. See [TerraformCloudTaggedEnvVar].
	TerraformCloudTokenEnvVarPrefix = DefaultTerraformCloudTokenEnvVar + "_"

	// envTerraformCloudAudience is the *input* variable an operator sets on
	// the workspace to make HCP Terraform mint a token at all. Never read for
	// its value — a workspace variable is not visible as an environment
	// variable to the run in the way the token is — only named in
	// diagnostics, because it is the thing that is missing when the token is.
	envTerraformCloudAudience = "TFC_WORKLOAD_IDENTITY_AUDIENCE"

	// envTerraformRunID is set in every HCP Terraform run, and is used only to
	// tell "not running in HCP Terraform" apart from "running there with no
	// audience variable set".
	envTerraformRunID = "TFC_RUN_ID"
)

// TerraformCloudTaggedEnvVar returns the token variable for a tagged
// audience: tag "FLOWSTATE" gives TFC_WORKLOAD_IDENTITY_TOKEN_FLOWSTATE.
//
// The tag is upper-cased because HCP Terraform's variable names are, and
// because an operator who wrote the tag in lower case in their workspace
// configuration would otherwise get "unset or empty" for a token that is
// sitting right there under a different case.
func TerraformCloudTaggedEnvVar(tag string) string {
	tag = strings.TrimSpace(tag)
	if tag == "" {
		return DefaultTerraformCloudTokenEnvVar
	}
	return TerraformCloudTokenEnvVarPrefix + strings.ToUpper(tag)
}

// TerraformCloudOption configures a [Source] built by
// [NewTerraformCloudSource].
type TerraformCloudOption func(*staticTokenSource)

// WithTerraformCloudTag reads the tagged token variable for tag rather than
// the untagged [DefaultTerraformCloudTokenEnvVar].
func WithTerraformCloudTag(tag string) TerraformCloudOption {
	return func(s *staticTokenSource) {
		if strings.TrimSpace(tag) != "" {
			s.variable = TerraformCloudTaggedEnvVar(tag)
		}
	}
}

// WithTerraformCloudEnvVar names the token variable outright, for a caller
// that has one and would rather not have this package derive it.
func WithTerraformCloudEnvVar(variable string) TerraformCloudOption {
	return func(s *staticTokenSource) {
		if variable != "" {
			s.variable = variable
		}
	}
}

// WithTerraformCloudAudience gives the audience the workspace's
// TFC_WORKLOAD_IDENTITY_AUDIENCE variable is expected to name.
//
// As with GitLab this is only ever a check: HCP Terraform bound the audience
// when it minted the token, before the run started. Empty means no check.
func WithTerraformCloudAudience(audience string) TerraformCloudOption {
	return func(s *staticTokenSource) { s.audience = audience }
}

// WithTerraformCloudClock overrides the clock used to decide whether the token
// needs re-reading. Exists for tests.
func WithTerraformCloudClock(clock func() time.Time) TerraformCloudOption {
	return func(s *staticTokenSource) { s.clock = clock }
}

// WithTerraformCloudRefreshMargin overrides [DefaultStaticRefreshMargin].
func WithTerraformCloudRefreshMargin(margin time.Duration) TerraformCloudOption {
	return func(s *staticTokenSource) { s.margin = margin }
}

// NewTerraformCloudSource returns a [Source] that presents the workload
// identity token an HCP Terraform run was given.
//
// The variable is read on every call once the cached copy is within its
// refresh margin of expiring, and an expired, unparseable or wrongly-addressed
// token is refused rather than presented. See this file's commentary above for
// the mechanism and its documented source.
func NewTerraformCloudSource(opts ...TerraformCloudOption) (Source, error) {
	s := &staticTokenSource{
		name:     SourceTerraformCloud,
		variable: DefaultTerraformCloudTokenEnvVar,
		clock:    time.Now,
		margin:   DefaultStaticRefreshMargin,
	}
	for _, opt := range opts {
		opt(s)
	}

	s.absentHint = func() string { return terraformCloudAbsentHint(s.variable) }
	s.audienceHint = "HCP Terraform binds the audience when it mints the token, so this can only be fixed " +
		"on the workspace, by changing " + envTerraformCloudAudience + " (or the tagged variable this run " +
		"used) or the audience Flowstate was told to expect"

	return s, nil
}

// terraformCloudAbsentHint says what a missing token most likely means.
func terraformCloudAbsentHint(variable string) string {
	if os.Getenv(envTerraformRunID) == "" {
		return "and " + envTerraformRunID + " is not set either, so this does not look like an HCP Terraform " +
			"run; the terraform-cloud source only works inside one"
	}

	audienceVar := envTerraformCloudAudience
	if tag, ok := strings.CutPrefix(variable, TerraformCloudTokenEnvVarPrefix); ok {
		audienceVar = envTerraformCloudAudience + "_" + tag
	}

	return "this run was given no workload identity token by that name; set the workspace variable " +
		audienceVar + " to the Flowstate server this run authenticates to, which is what makes HCP " +
		"Terraform mint one"
}
