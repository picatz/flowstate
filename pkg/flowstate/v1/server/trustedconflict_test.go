package server_test

import (
	"testing"

	"connectrpc.com/connect"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	v1 "github.com/picatz/flowstate/pkg/flowstate/v1"
	"github.com/picatz/flowstate/pkg/flowstate/v1/server"
)

// The negative direction of the trusted-workflow boundary, per CLAUDE.md: a
// test that a registration is honoured is a functionality test, and every test
// beside this one is that shape. What has to be false is that a *second*
// registration under the same (namespace, name) can displace the first — the
// weaker copy winning by arriving later is the whole bypass, and it is invisible
// to any test that registers a policy once and checks it binds.

// openWorkflow is `break-glass` with no manual restriction at all: the weaker
// half of every pair below, and what an attacker would want a trusted name to
// resolve to.
func openWorkflow() *v1.Workflow {
	open := narrowedWorkflow()
	open.Triggers.Manual = nil
	return open
}

// TestALaterTrustedRegistrationCannotWeakenAnEarlierOne fixes the P1 on #709:
// a second WithTrustedWorkflows naming a registered workflow used to overwrite
// it, so option order decided policy.
func TestALaterTrustedRegistrationCannotWeakenAnEarlierOne(t *testing.T) {
	t.Parallel()

	temporal, _ := newTemporalNamespace(t)
	flowstate := mustNew(t, temporal,
		server.WithTrustedWorkflows("", narrowedWorkflow()),
		server.WithTrustedWorkflows("", openWorkflow()),
	)

	// Submitting the open copy is the attack the trusted set exists to stop.
	// Before the fix this succeeded, because the second option had replaced
	// the narrowed specification with this one.
	_, err := flowstate.Run(t.Context(), connect.NewRequest(&v1.RunRequest{
		Workflow: openWorkflow(),
		Reason:   "trying to bypass the deployment policy",
	}))

	require.Error(t, err, "a later trusted registration replaced a stricter earlier one")
	// Refused for the conflict rather than by the narrowed policy: with two
	// deployment-owned answers there is no single policy to enforce, and
	// picking either one is a guess. Failing closed means saying so.
	assert.Equal(t, connect.CodeFailedPrecondition, connect.CodeOf(err),
		"a conflicting registration was resolved rather than refused: %v", err)
	assert.Contains(t, err.Error(), "registered twice",
		"the refusal does not tell an operator what to fix: %v", err)
}

// TestAnIdenticalTrustedRegistrationIsNotAConflict keeps the fix from being a
// blanket refusal: one specification loaded twice says one thing, so it must
// still authorize. Without this, a deployment that both serves a Flowfile for
// webhooks and names it in WithTrustedWorkflows would be bricked by the fix.
func TestAnIdenticalTrustedRegistrationIsNotAConflict(t *testing.T) {
	t.Parallel()

	temporal, _ := newTemporalNamespace(t)
	flowstate := mustNew(t, temporal,
		server.WithTrustedWorkflows("", narrowedWorkflow()),
		server.WithTrustedWorkflows("", narrowedWorkflow()),
	)

	_, err := flowstate.Run(t.Context(), connect.NewRequest(&v1.RunRequest{
		Workflow: openWorkflow(),
		Reason:   "trying to bypass the deployment policy",
	}))

	require.Error(t, err, "the trusted narrowed policy stopped binding")
	assert.Equal(t, connect.CodePermissionDenied, connect.CodeOf(err),
		"an identical re-registration was treated as a conflict: %v", err)
	assert.Contains(t, err.Error(), "oncall@example.com")
}

// TestAConflictIsScopedToItsOwnTenant is the other negative direction. The key
// is (namespace, name), so one tenant registering a name twice must not refuse
// that name for a different tenant — a conflict that leaked across the boundary
// would let any tenant deny another's workflow by registering it twice.
func TestAConflictIsScopedToItsOwnTenant(t *testing.T) {
	t.Parallel()

	temporal, _ := newTemporalNamespace(t)
	flowstate := mustNew(t, temporal,
		server.WithNamespace("team-b"),
		server.WithTrustedWorkflows("team-a", narrowedWorkflow()),
		server.WithTrustedWorkflows("team-a", openWorkflow()),
		server.WithTrustedWorkflows("team-b", narrowedWorkflow()),
	)

	// This caller is team-b, whose single registration is unambiguous, so
	// team-a's conflict must not reach it: it is refused by the narrowed
	// policy it actually has.
	_, err := flowstate.Run(t.Context(), connect.NewRequest(&v1.RunRequest{
		Workflow: openWorkflow(),
		Reason:   "trying to bypass the deployment policy",
	}))

	require.Error(t, err)
	assert.Equal(t, connect.CodePermissionDenied, connect.CodeOf(err),
		"another tenant's conflicting registration refused this tenant's workflow: %v", err)
	assert.Contains(t, err.Error(), "oncall@example.com")
}

// TestAnInvalidTrustedRegistrationIsNotSubstituted is the second half of the
// same gap. Every RPC runs v1.Validate over the *request*, and it runs before
// the trusted lookup replaces the caller's copy; validateSubmission never asks
// again afterwards. So a specification the schema refuses, registered by an
// embedder, used to be substituted for a caller's valid one and carried to
// Temporal on the strength of being deployment-owned.
//
// Written as the negative direction again: what must be false is that
// registering a malformed copy gives it authority.
func TestAnInvalidTrustedRegistrationIsNotSubstituted(t *testing.T) {
	t.Parallel()

	// A valid name and no steps: past the field rules a Go embedder building a
	// Workflow by hand would notice, refused by the whole-message rules.
	malformed := &v1.Workflow{Name: "break-glass", Profile: v1.CurrentProfile}
	require.Error(t, v1.Validate(malformed), "the fixture is valid, so this test proves nothing")

	temporal, _ := newTemporalNamespace(t)
	flowstate := mustNew(t, temporal, server.WithTrustedWorkflows("", malformed))

	_, err := flowstate.Run(t.Context(), connect.NewRequest(&v1.RunRequest{
		Workflow: narrowedWorkflow(),
		Reason:   "an ordinary manual start",
	}))

	require.Error(t, err, "a specification the schema refuses was substituted and run")
	assert.Equal(t, connect.CodeFailedPrecondition, connect.CodeOf(err))
	assert.Contains(t, err.Error(), "refuses",
		"the refusal does not tell an operator their registration is the problem: %v", err)
}

// namelessWorkflow is otherwise exactly the narrowed specification — a real
// `manual:` policy an embedder meant to install — with the one field the trusted
// set is addressed by removed.
func namelessWorkflow() *v1.Workflow {
	nameless := narrowedWorkflow()
	nameless.Name = ""
	return nameless
}

// TestANamelessTrustedRegistrationRefusesConstruction is the third direction of
// the same boundary, and the one the conflict and validity arms above cannot
// cover.
//
// Both of those refuse a *key*: a name is registered twice, or registered with a
// specification the schema rejects, and every request for that (namespace, name)
// is then denied. A registration carrying no name has no key to refuse. Passing
// over it — which is what this used to do — leaves an embedder holding a server
// they believe enforces a deployment-owned `manual:` policy while the name they
// meant to protect still accepts whatever a caller submits, which is the open
// behaviour the trusted set exists to remove.
//
// The negative direction, per CLAUDE.md: what must be false is that such a
// configuration yields a serving deployment at all.
func TestANamelessTrustedRegistrationRefusesConstruction(t *testing.T) {
	t.Parallel()

	for _, test := range []struct {
		name     string
		workflow *v1.Workflow
	}{
		{name: "a nil entry", workflow: nil},
		{name: "a workflow with no name", workflow: namelessWorkflow()},
	} {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()

			// A nil Temporal client on purpose: options run before the client is
			// ever touched, so this asserts on the construction itself and needs
			// no dev server — the same shape
			// TestNewRefusesANamespaceOutsideTheGrammar uses.
			s, err := server.New(nil, server.WithTrustedWorkflows("team-a", test.workflow))

			require.Error(t, err, "a trusted registration naming nothing was accepted")
			require.Nil(t, s, "a refused option must not also yield a usable server")

			// Actionable at start-up, while the embedder can still fix it:
			// which tenant, and what is missing.
			assert.Contains(t, err.Error(), "team-a")
			assert.Contains(t, err.Error(), "`name:`")
		})
	}
}
