package server_test

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/picatz/flowstate/pkg/flowstate/v1/auth"
	"github.com/picatz/flowstate/pkg/flowstate/v1/server"
)

// TestNewRefusesANamespaceOutsideTheGrammar is the negative direction of #823.
//
// Every id this package derives from a tenant is unambiguous only for a
// namespace [auth.ValidateNamespace] admits, and until this refusal existed the
// deployment's own fallback tenant — [server.WithNamespace] — was the one way in
// that never passed through it. The positive direction (two grammatical tenants
// cannot collide) is TestTwoTenantsSchedulesOfOneNameCannotCollide; a test that
// only says that is a functionality test wearing a security test's clothes,
// because it never asks what happens to a value the grammar forbids.
func TestNewRefusesANamespaceOutsideTheGrammar(t *testing.T) {
	t.Parallel()

	for _, namespace := range []string{
		// The issue's own example, and the one that matters most: an
		// underscore is the schedule id's separator, so this is the character
		// whose admission makes the encoding ambiguous. See the collision
		// spelled out below.
		"team_a",
		// A space, which is what a human writing a deployment's tenant by hand
		// actually types.
		"Prod Team",
		// Uppercase, which reads as the same tenant as "teama" to everyone
		// except the byte comparison every authorization decision makes.
		"TeamA",
		// A leading dash, which the grammar forbids so a namespace can never be
		// read as a flag or as one of the underscore-prefixed sentinels.
		"-team",
		// Path traversal, refused for the secret provider's sake: a namespace
		// reaches a file path.
		"..",
		// Longer than auth.MaxNamespaceLen. A bound that is checked is worth
		// having a case for, since the check is what a fairness key and a
		// workflow id length budget both rest on.
		strings.Repeat("a", auth.MaxNamespaceLen+1),
	} {
		s, err := server.New(nil, server.WithNamespace(namespace))

		require.Error(t, err, "a deployment configured with namespace %q must not construct", namespace)
		require.Nil(t, s, "a refused option must not also yield a usable server")

		// The message has to be actionable by whoever configured the
		// deployment: what is wrong, and where the rule that decides it lives.
		// Diagnostics are a feature — an operator reading this at start-up has
		// no other pointer to the grammar.
		require.ErrorContains(t, err, "WithNamespace")
		require.ErrorContains(t, err, "auth.ValidateNamespace")
		require.ErrorContains(t, err, "pkg/flowstate/v1/auth/namespace.go")
	}
}

// TestAnUngrammaticalNamespaceWouldHaveCollided is why the refusal above is not
// merely tidiness.
//
// It derives the two schedule ids the issue names, through the server's own
// derivation, and shows they are the same string: tenant `team_a` with a
// schedule named `x`, and tenant `team` with a schedule named `a_x`. A schedule
// name may contain an underscore and a namespace may not, which is the whole of
// what makes the first underscore after the prefix the separator — so admitting
// one into a namespace is admitting a second tenant to the same id.
//
// The derivation is deliberately called with a namespace [server.New] now
// refuses, which is the point: the function cannot state the grammar itself, so
// the guarantee has to be made where the value is chosen.
func TestAnUngrammaticalNamespaceWouldHaveCollided(t *testing.T) {
	t.Parallel()

	require.Error(t, auth.ValidateNamespace("team_a"),
		"this test rests on team_a being outside the grammar; if it is admitted, the collision below is live")

	require.Equal(t,
		server.ScheduleIDForTest("team", "a_x"),
		server.ScheduleIDForTest("team_a", "x"),
		"an underscore in a namespace makes two tenants derive one schedule id")
}

// TestNewAcceptsEveryGrammaticalNamespace is the other half, including the two
// shapes a refusal would be most tempting to over-apply to.
//
// The empty namespace is legal and is the single-tenant default: a deployment
// that names no tenant — including one started with --insecure-no-auth — must
// keep constructing exactly as it did. Refusing it would break the
// zero-configuration path [server.WithNamespace] exists to serve, and it is not
// a value anyone can forge into another tenant: the ids derived from it carry a
// leading separator no non-empty namespace can produce, and the `_default`
// segment the task queues and secret providers substitute is unforgeable
// because the grammar forbids the underscore it begins with.
func TestNewAcceptsEveryGrammaticalNamespace(t *testing.T) {
	t.Parallel()

	for _, namespace := range []string{
		"",
		"team-a",
		"default",
		"a",
		"tenant0",
		strings.Repeat("a", auth.MaxNamespaceLen),
	} {
		s, err := server.New(nil, server.WithNamespace(namespace))

		require.NoError(t, err, "namespace %q satisfies the grammar and must construct", namespace)
		require.NotNil(t, s)
	}

	// And the untenanted deployment's ids stay distinguishable from every
	// tenant's, which is the property that makes admitting "" safe rather than
	// merely convenient.
	require.True(t, strings.HasSuffix(server.ScheduleIDForTest("", "nightly"), "_nightly"))
	require.NotEqual(t,
		server.ScheduleIDForTest("", "nightly"),
		server.ScheduleIDForTest("default", "nightly"),
		"the unnamed tenant and a tenant literally named \"default\" must not share an id")
}
