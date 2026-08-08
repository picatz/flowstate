package flowstatev1_test

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"

	v1 "github.com/picatz/flowstate/pkg/flowstate/v1"
)

// These tests answer the second half of "can a CI-issued identity be used": not
// whether the token verifies (that is `auth`'s side, in
// `auth/ci_federation_test.go`), but whether the identity it becomes can be
// named by the two policy surfaces a workflow author and an operator write,
// which are a signal policy's `allow:` rules and a task-shape policy's CEL
// rules.
//
// The identity below is the one `IdentityFromPrincipal` derives from a token a
// CI platform minted, with GitHub Actions as the concrete shape: the subject is
// the platform's own "repo:<owner>/<name>:ref:refs/heads/<branch>", and the
// claims are the ones an operator named with `--identity-claim`.

// ciIdentity is what a run started by a CI job acts as.
func ciIdentity() *v1.WorkloadIdentity {
	return &v1.WorkloadIdentity{
		Subject:    "repo:octo-org/octo-repo:ref:refs/heads/main",
		Issuer:     "https://token.actions.githubusercontent.com",
		Namespace:  "platform",
		Deployment: "prod",
		Claims: map[string]string{
			"repository":       "octo-org/octo-repo",
			"ref":              "refs/heads/main",
			"job_workflow_ref": "octo-org/octo-repo/.github/workflows/deploy.yml@refs/heads/main",
		},
	}
}

// TestCISenderNamedBySignalPolicy checks that a signal policy can name a CI job
// as the sender, both by its issuer-qualified subject and by a carried claim.
func TestCISenderNamedBySignalPolicy(t *testing.T) {
	identity := ciIdentity()

	bySubject := &v1.SignalPolicy{Allow: []*v1.SignalPolicyRule{{
		Subject: v1.QualifiedSubject(
			"https://token.actions.githubusercontent.com",
			"repo:octo-org/octo-repo:ref:refs/heads/main",
		),
	}}}
	require.True(t, v1.SignalPolicyAllows(bySubject, identity),
		"the qualified CI subject names the sender")

	// The same rule against a job on another branch of the same repository. The
	// branch is part of the subject the platform mints, so an exact match is
	// already branch-specific with nothing extra written.
	otherBranch := ciIdentity()
	otherBranch.Subject = "repo:octo-org/octo-repo:ref:refs/heads/topic"
	require.False(t, v1.SignalPolicyAllows(bySubject, otherBranch),
		"another branch of the same repository is a different subject")

	// And a rule keyed on a carried claim instead, which is how a policy names
	// a repository without pinning the branch.
	byClaim := &v1.SignalPolicy{Allow: []*v1.SignalPolicyRule{{
		Namespace: "platform",
		Claims:    map[string]string{"repository": "octo-org/octo-repo"},
	}}}
	require.True(t, v1.SignalPolicyAllows(byClaim, identity))
	require.True(t, v1.SignalPolicyAllows(byClaim, otherBranch))

	// A claim the deployment did not carry cannot be matched, however the token
	// was signed: the rule is checked against the attested identity, and an
	// operator who did not name the claim has nothing to check.
	notCarried := &v1.SignalPolicy{Allow: []*v1.SignalPolicyRule{{
		Claims: map[string]string{"runner_environment": "github-hosted"},
	}}}
	require.False(t, v1.SignalPolicyAllows(notCarried, identity),
		"a claim not carried into the identity cannot authorize anyone")
}

// TestCIIdentityInTaskPolicyRules checks that a task-shape policy's CEL rules
// can read a CI-derived identity: its subject, its namespace, and the claims an
// operator chose to carry.
func TestCIIdentityInTaskPolicyRules(t *testing.T) {
	policy, err := v1.TaskPolicyConfig{
		Allow: []string{
			`identity.namespace == "platform" && ` +
				`"repository" in identity.claims && ` +
				`identity.claims["repository"] == "octo-org/octo-repo"`,
		},
		Deny: []string{
			`task == "command.run" && identity.subject.startsWith("repo:")`,
		},
	}.Policy()
	require.NoError(t, err)

	identity := ciIdentity()

	require.NoError(t, policy.Check(context.Background(), "http.request", identity),
		"the allow rule reads the carried claim")

	require.Error(t, policy.Check(context.Background(), "command.run", identity),
		"the deny rule reads the CI subject and wins")

	// A run from another repository satisfies no allow rule.
	other := ciIdentity()
	other.Claims = map[string]string{"repository": "octo-org/other-repo"}
	require.Error(t, policy.Check(context.Background(), "http.request", other))
}
