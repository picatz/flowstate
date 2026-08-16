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
	flowstate := server.New(temporal,
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
	flowstate := server.New(temporal,
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
	flowstate := server.New(temporal,
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
