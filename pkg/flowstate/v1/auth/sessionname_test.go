package auth

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// A role session name is what CloudTrail shows for everything a federated
// credential goes on to do, so it is the only thing connecting an AWS audit trail
// back to the workload that caused the call. [awsSessionName]'s own doc states the
// property that makes it worth anything — *two different subjects must not collapse
// into one session name* — and this file is the check, because it did.
//
// [WorkloadIdentity.SubjectFor] takes the same care one layer up and says why: a
// component may not contain `/` or `:`, "otherwise one component could spell out
// several, and a subject could be made to look like a different workload's". The
// session name is where that guarantee was being given back, in two ways at once.

// TestTwoSubjectsCannotShareASessionName is the ambiguous-encoding bug, and it is
// the one CLAUDE.md already names for the secret providers: *no separator fixes it,
// because every character legal in a prefix is legal in a name.*
//
// AWS accepts `-`, so the rewrite mapped both separators onto a character that is
// also legal *inside* a component. `.../c/d-e` and `.../c-d/e` are two different
// workloads with two valid subjects, and they produced one session name.
func TestTwoSubjectsCannotShareASessionName(t *testing.T) {
	t.Parallel()

	first, err := WorkloadIdentity{Namespace: "a", Deployment: "b"}.
		SubjectFor(StepRef{Workflow: "c", Step: "d-e"})
	require.NoError(t, err)

	second, err := WorkloadIdentity{Namespace: "a", Deployment: "b"}.
		SubjectFor(StepRef{Workflow: "c-d", Step: "e"})
	require.NoError(t, err)

	require.NotEqual(t, first, second, "the two subjects are the same, so this proves nothing")

	assert.NotEqual(t, awsSessionName(first), awsSessionName(second),
		"two workloads share one session name, so an AWS audit trail cannot say which of "+
			"them made a call")
}

// TestATenantCannotSpellAnotherTenantsSessionName is the direction that matters,
// and the one an isolation test written the other way round cannot see.
//
// The separator collision above needs two subjects that happen to collide. This is
// a tenant *choosing* to. Truncation kept the tail on the reasoning that "the
// workload and step are more distinguishing than the namespace prefix they share
// with every other workflow" — which holds inside one tenant and is exactly
// backwards across tenants, because the namespace is the only component a tenant
// does not choose. Workflow and step both come out of the attacker's own Flowfile,
// so dropping the head hands them the whole 64 characters.
func TestATenantCannotSpellAnotherTenantsSessionName(t *testing.T) {
	t.Parallel()

	victim, err := WorkloadIdentity{
		Namespace:  "acme-production-platform-engineering",
		Deployment: "prod",
	}.SubjectFor(StepRef{Workflow: "billing-export", Step: "assume-payments-role"})
	require.NoError(t, err)

	target := awsSessionName(victim)
	require.Len(t, target, awsMaxSessionNameLength,
		"the victim's name is not at the length limit, so truncation is not being exercised")

	// Everything the attacker controls, and nothing it does not: its own namespace
	// is its own, and the two remaining components come from a file it writes.
	attacker, err := WorkloadIdentity{Namespace: "evil", Deployment: "d"}.
		SubjectFor(StepRef{Workflow: "w", Step: target})
	require.NoError(t, err)

	assert.NotEqual(t, target, awsSessionName(attacker),
		"a tenant in another namespace produced the victim's session name, so every call it "+
			"makes is attributed to the victim")
}

// TestASessionNameIsSomethingAWSAccepts keeps the fix inside the constraint that
// made the original lossy.
//
// A name AWS refuses is an exchange that fails, which is worse than an ambiguous
// one: the alphabet and the 64-character limit are the reason this function is not
// simply the subject.
func TestASessionNameIsSomethingAWSAccepts(t *testing.T) {
	t.Parallel()

	for _, subject := range []string{
		"flowstate:a/b/c/d",
		"flowstate:" + strings.Repeat("long-namespace", 40) + "/b/c/d",
		"flowstate:ünïcode/déployment/wörkflow/stëp",
		"flowstate:a/b/c/" + strings.Repeat("x", 200),
		"",
	} {
		name := awsSessionName(subject)

		assert.GreaterOrEqual(t, len(name), 2, "AWS refuses a session name shorter than two characters")
		assert.LessOrEqual(t, len(name), awsMaxSessionNameLength, "AWS refuses a session name over 64 characters")

		for _, r := range name {
			legal := r >= 'a' && r <= 'z' || r >= 'A' && r <= 'Z' || r >= '0' && r <= '9' ||
				strings.ContainsRune("+=,.@-_", r)
			assert.True(t, legal, "%q contains %q, which is outside the alphabet AWS accepts", name, r)
		}
	}
}

// TestASessionNameStillNamesTheWorkload is the half the digest must not cost.
//
// A name that is only a hash is unambiguous and useless: somebody reading CloudTrail
// at three in the morning needs to recognise the workload without going back to
// Flowstate to resolve it. The step is the most specific component and the one an
// operator is usually looking for.
func TestASessionNameStillNamesTheWorkload(t *testing.T) {
	t.Parallel()

	subject, err := WorkloadIdentity{Namespace: "acme", Deployment: "prod"}.
		SubjectFor(StepRef{Workflow: "deploy-service", Step: "assume-deploy-role"})
	require.NoError(t, err)

	assert.Contains(t, awsSessionName(subject), "assume-deploy-role",
		"the session name does not name the step, so an audit trail cannot be read without "+
			"resolving it somewhere else")
}

// TestASessionNameIsTheSameEveryTime is what makes it an identity rather than a
// label.
//
// An operator writes a session name into a dashboard filter or an alert; a name that
// varied per exchange would break both, and would make correlating two calls from
// one workload impossible.
func TestASessionNameIsTheSameEveryTime(t *testing.T) {
	t.Parallel()

	subject := "flowstate:acme/prod/deploy-service/assume-deploy-role"

	assert.Equal(t, awsSessionName(subject), awsSessionName(subject))
}
