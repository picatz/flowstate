package credentialsource

import (
	"os"
	"time"
)

// How a GitLab CI job gets an OIDC token, and where this package's
// assumptions come from.
//
// GitLab has no equivalent of the GitHub Actions token endpoint. A job asks
// for a token declaratively, with the `id_tokens:` keyword, and GitLab mints
// it before the job starts and exposes it as an environment variable named by
// the key the author chose:
//
//	deploy:
//	  id_tokens:
//	    FLOWSTATE_ID_TOKEN:
//	      aud: https://flowstate.example.com
//	  script:
//	    - flow run deploy.yaml --credential-source gitlab
//
// Two consequences follow, and both shape this file. The variable's name is
// the author's choice, so this package cannot know it — it documents a
// convention ([DefaultGitLabIDTokenEnvVar]) and lets a caller name another.
// And the audience is bound in the job definition, once, before the script
// runs: there is no way to ask for a token addressed to somewhere else, so a
// `--audience` that disagrees with the job's `aud:` is a configuration error
// that can only be fixed in .gitlab-ci.yml.
//
// Source, read 2026-08-22:
// https://docs.gitlab.com/ci/secrets/id_token_authentication/
//
// From that page, for the claim shape pinned in
// pkg/flowstate/v1/auth/ci_federation_vendors_test.go: the issuer is the
// GitLab instance's URL ("https://gitlab.com" on GitLab.com), the default
// subject is "project_path:{group}/{project}:ref_type:{type}:ref:{name}",
// and the token expires at the job's timeout, or five minutes after issue
// when the job sets no timeout.

// Environment variables this source knows about.
const (
	// DefaultGitLabIDTokenEnvVar is the `id_tokens:` key this package
	// recommends and reads unless told otherwise.
	//
	// GitLab lets the author name it, so nothing about this string is forced
	// by the platform — it is a convention, chosen so that a job configured by
	// following Flowstate's documentation works with no further wiring, and so
	// that a job that already mints ID tokens for something else (Vault, a
	// cloud provider) does not have its token silently borrowed for a
	// different audience.
	DefaultGitLabIDTokenEnvVar = "FLOWSTATE_ID_TOKEN"

	// LegacyGitLabJWTEnvVar is the pre-`id_tokens:` variable. GitLab
	// deprecated CI_JOB_JWT, CI_JOB_JWT_V1 and CI_JOB_JWT_V2 and removed all
	// three in GitLab 17.0
	// (https://about.gitlab.com/blog/2024/04/10/a-guide-to-the-high-impact-breaking-changes-in-gitlab-17-0/).
	//
	// This source never reads it. It is named only so that a job still relying
	// on it gets told what happened rather than "unset or empty": on a GitLab
	// version old enough to still set it, silently accepting it would produce
	// a token whose audience nobody chose.
	LegacyGitLabJWTEnvVar = "CI_JOB_JWT_V2"

	// envGitLabCI is set to "true" in every GitLab CI job. Used only to tell
	// "you are not on GitLab" apart from "you are, and the id_tokens: keyword
	// is missing", which are different mistakes with different fixes.
	envGitLabCI = "GITLAB_CI"
)

// GitLabOption configures a [Source] built by [NewGitLabSource].
type GitLabOption func(*staticTokenSource)

// WithGitLabEnvVar names the `id_tokens:` key the job declares, when it is
// not [DefaultGitLabIDTokenEnvVar].
func WithGitLabEnvVar(variable string) GitLabOption {
	return func(s *staticTokenSource) {
		if variable != "" {
			s.variable = variable
		}
	}
}

// WithGitLabAudience gives the audience the job's `id_tokens: aud:` is
// expected to name.
//
// Unlike [NewGitHubActionsSource], where the audience is a request parameter,
// this is only ever a check: GitLab bound the audience when it minted the
// token, and a mismatch is reported so an author sees the disagreement here
// rather than as an unexplained refusal from the server. Empty means no check
// — the token is presented with whatever audience it carries, and the server
// decides.
func WithGitLabAudience(audience string) GitLabOption {
	return func(s *staticTokenSource) { s.audience = audience }
}

// WithGitLabClock overrides the clock used to decide whether the token needs
// re-reading. Exists for tests.
func WithGitLabClock(clock func() time.Time) GitLabOption {
	return func(s *staticTokenSource) { s.clock = clock }
}

// WithGitLabRefreshMargin overrides [DefaultStaticRefreshMargin].
func WithGitLabRefreshMargin(margin time.Duration) GitLabOption {
	return func(s *staticTokenSource) { s.margin = margin }
}

// NewGitLabSource returns a [Source] that presents the OIDC ID token a GitLab
// CI job's `id_tokens:` keyword put in the environment.
//
// The variable is read on every call once the cached copy is within its
// refresh margin of expiring, and an expired, unparseable or wrongly-addressed
// token is refused rather than presented. See this file's package-level
// commentary for the mechanism and its documented source.
func NewGitLabSource(opts ...GitLabOption) (Source, error) {
	s := &staticTokenSource{
		name:     SourceGitLab,
		variable: DefaultGitLabIDTokenEnvVar,
		clock:    time.Now,
		margin:   DefaultStaticRefreshMargin,
	}
	for _, opt := range opts {
		opt(s)
	}

	s.absentHint = func() string { return gitLabAbsentHint(s.variable) }
	s.audienceHint = "GitLab binds the audience when it mints the token, so this can only be fixed in " +
		".gitlab-ci.yml, by changing the job's `id_tokens:` `aud:` or the audience Flowstate was told to expect"

	return s, nil
}

// gitLabAbsentHint says what a missing ID token most likely means. The three
// cases have three different fixes, and "unset or empty" distinguishes none of
// them.
func gitLabAbsentHint(variable string) string {
	if os.Getenv(envGitLabCI) != "true" {
		return "and " + envGitLabCI + " is not set either, so this does not look like a GitLab CI job; " +
			"the gitlab source only works inside one"
	}
	if os.Getenv(LegacyGitLabJWTEnvVar) != "" {
		return "but " + LegacyGitLabJWTEnvVar + " is — that variable was removed in GitLab 17.0 and is " +
			"deliberately not read, because its audience is not one this job chose. Declare an ID token " +
			"instead: `id_tokens: {" + variable + ": {aud: <the Flowstate server>}}`"
	}
	return "this job declares no ID token by that name; add " +
		"`id_tokens: {" + variable + ": {aud: <the Flowstate server>}}` to the job in .gitlab-ci.yml"
}
