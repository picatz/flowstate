package flowstatev1_test

import (
	"testing"

	v1 "github.com/picatz/flowstate/pkg/flowstate/v1"
	"github.com/stretchr/testify/require"
)

func TestEntityWorkflowIDComposesNamespaceAndKey(t *testing.T) {
	id, err := v1.EntityWorkflowID("acme", "order-123")
	require.NoError(t, err)
	require.Equal(t, "flowstate-entity-acme_order-123", id)
}

func TestEntityWorkflowIDAllowsEmptyNamespace(t *testing.T) {
	// The single-tenant default (no auth configured, or an unauthenticated
	// caller under --insecure-no-auth) composes just as any other namespace
	// does: the separator is still present, still the only underscore in the
	// string, and still unambiguous.
	id, err := v1.EntityWorkflowID("", "order-123")
	require.NoError(t, err)
	require.Equal(t, "flowstate-entity-_order-123", id)
}

// TestEntityWorkflowIDRefusesTheSeparatorInEitherHalf is the negative
// direction CLAUDE.md's "test that A cannot reach B" section asks for,
// applied to addressing rather than to a secret backend: a namespace or an
// entity key that could itself contain the separator character would make
// the join ambiguous, which is exactly the env-provider incident
// (`prefix + NAMESPACE + "_" + name`) reproduced in id space. Since
// [v1.EntityWorkflowID] validates both halves against a grammar that forbids
// "_" entirely, neither can ever reach this far carrying one — this test
// proves that refusal happens rather than assuming the grammar is enough.
func TestEntityWorkflowIDRefusesTheSeparatorInEitherHalf(t *testing.T) {
	_, err := v1.EntityWorkflowID("team_a", "secret")
	require.Error(t, err, "a namespace containing the separator character must be refused, not silently joined")

	_, err = v1.EntityWorkflowID("team", "a_secret")
	require.Error(t, err, "an entity key containing the separator character must be refused, not silently joined")
}

// TestEntityWorkflowIDJoinIsUnambiguousAcrossTheBoundary is the same negative
// direction stated as an injectivity property rather than as a single
// example: CLAUDE.md's env-provider incident was exactly this failure —
// namespace "team_a" + name "key" and namespace "team" + name "a_key" both
// resolved to $FLOWSTATE_SECRET_TEAM_A_KEY. Because neither half of an entity
// id may contain the separator, "team-a" + "secret" and "team" + "a-secret"
// (the id-space analogue, using the grammar's legal dash instead of the
// forbidden underscore) must never collide.
func TestEntityWorkflowIDJoinIsUnambiguousAcrossTheBoundary(t *testing.T) {
	a, err := v1.EntityWorkflowID("team-a", "secret")
	require.NoError(t, err)

	b, err := v1.EntityWorkflowID("team", "a-secret")
	require.NoError(t, err)

	require.NotEqual(t, a, b,
		"two different (namespace, entity_key) pairs must never compose to the same workflow id")
}

func TestEntityWorkflowIDRefusesUppercase(t *testing.T) {
	_, err := v1.EntityWorkflowID("Acme", "order")
	require.Error(t, err)

	_, err = v1.EntityWorkflowID("acme", "Order")
	require.Error(t, err)
}

func TestEntityWorkflowIDRefusesALeadingDash(t *testing.T) {
	_, err := v1.EntityWorkflowID("acme", "-order")
	require.Error(t, err)
}

func TestEntityWorkflowIDRefusesAnEmptyKey(t *testing.T) {
	_, err := v1.EntityWorkflowID("acme", "")
	require.Error(t, err)
}

func TestEntityWorkflowIDRefusesAnOverLongKey(t *testing.T) {
	long := make([]byte, v1.MaxEntityKeyLen+1)
	for i := range long {
		long[i] = 'a'
	}
	_, err := v1.EntityWorkflowID("acme", string(long))
	require.Error(t, err)
}

func TestValidateEntityKeyAcceptsTheGrammarProtovalidateAlsoEnforces(t *testing.T) {
	require.NoError(t, v1.ValidateEntityKey("order-123"))
	require.NoError(t, v1.ValidateEntityKey("a"))
	require.Error(t, v1.ValidateEntityKey(""))
	require.Error(t, v1.ValidateEntityKey("Order"))
	require.Error(t, v1.ValidateEntityKey("order_123"))
}
