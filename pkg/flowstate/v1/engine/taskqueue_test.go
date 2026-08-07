package engine_test

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/picatz/flowstate/pkg/flowstate/v1/auth"
	"github.com/picatz/flowstate/pkg/flowstate/v1/engine"
)

// TestDefaultTaskQueueIsByteIdenticalToTodaysConstant is the "nothing existing
// moves" assertion, made against bytes rather than by inspection.
//
// A deployment that configures no routing gets the one constant, for every
// namespace including the ones the namespace grammar would refuse — a run whose
// recorded identity predates that grammar starts today and has to keep starting.
// The unconfigured path therefore validates nothing and cannot fail, and that is
// the whole of what this asserts.
func TestDefaultTaskQueueIsByteIdenticalToTodaysConstant(t *testing.T) {
	namespaces := []string{
		"",
		"team-a",
		"acme",
		// Everything below is outside auth.ValidateNamespace's grammar and would
		// be refused on the routed path. On the unrouted path it is not looked
		// at, because looking at it would fail a run that starts today.
		"Prod Team",
		"..",
		"team_a",
		"UPPER",
		strings.Repeat("x", auth.MaxNamespaceLen+1),
		"\x00control",
	}

	var queues engine.TaskQueues
	require.False(t, queues.Enabled())
	require.NoError(t, queues.Validate())

	for _, namespace := range namespaces {
		queue, err := queues.For(namespace)
		require.NoError(t, err, "namespace %q", namespace)
		require.Equal(t, "flowstate-run-task-queue", queue,
			"the unconfigured path must be byte-identical to the constant, for namespace %q", namespace)
		require.Equal(t, engine.RunTaskQueueName, queue)
	}
}

// TestRunTaskQueueNameCannotBeComposed asserts the structural separation between
// the shared queue and every per-tenant one.
//
// Every composed name contains the separator and the shared queue's name does
// not, so no prefix an operator can spell — and no tenant they can admit — makes
// a routed deployment's queue collide with the unrouted one. Without this a
// deployment could route one tenant onto the queue every unrouted deployment's
// general fleet polls, which is the fallback this whole change exists to refuse.
func TestRunTaskQueueNameCannotBeComposed(t *testing.T) {
	require.NotContains(t, engine.RunTaskQueueName, "_",
		"the shared queue's name must contain no separator, or a composed name could equal it")

	for _, prefix := range validPrefixes() {
		for _, namespace := range validNamespaces() {
			queue, err := engine.TaskQueues{Prefix: prefix}.For(namespace)
			require.NoError(t, err)
			require.NotEqual(t, engine.RunTaskQueueName, queue,
				"prefix %q + namespace %q composed the shared queue's own name", prefix, namespace)
			require.Contains(t, queue, "_")
		}
	}
}

// TestTaskQueueNamesCannotBeForged is the negative direction the house rule
// asks for, probed in the direction the env secrets provider's bug was actually
// found in: not "does each tenant reach its own queue" — which is a
// functionality test — but "can two different (prefix, namespace) pairs be made
// to name one queue".
//
// The env provider composed prefix + NAMESPACE + "_" + name, and namespace
// "team" with name "A_API_KEY" collided with the default tenant and
// "TEAM_A_API_KEY" because every character legal in one component was legal in
// the other. The pairs below are that shape, transplanted: prefixes and
// namespaces chosen to straddle each other's boundaries, plus a tenant named
// "default" attempting to spell the default tenant's own component.
//
// The assertion is injectivity over the whole cross product, which is the
// property "cannot be forged" actually means. It holds structurally — see
// [engine.TaskQueues] — so a failure here is a real hole rather than a missing
// case.
func TestTaskQueueNamesCannotBeForged(t *testing.T) {
	type pair struct{ prefix, namespace string }

	seen := map[string]pair{}

	for _, prefix := range validPrefixes() {
		for _, namespace := range validNamespaces() {
			queue, err := engine.TaskQueues{Prefix: prefix}.For(namespace)
			require.NoError(t, err)

			this := pair{prefix, namespace}
			if other, collided := seen[queue]; collided {
				t.Fatalf("queue %q is named by two different tenants: %+v and %+v", queue, other, this)
			}
			seen[queue] = this
		}
	}

	// Named explicitly as well, because it is the one collision an operator
	// could plausibly reach for: the default tenant's component is spelled with
	// the character the namespace grammar refuses, so a tenant called "default"
	// is a different queue and not the same one.
	forged, err := engine.TaskQueues{Prefix: "flowstate-run"}.For("default")
	require.NoError(t, err)
	actual, err := engine.TaskQueues{Prefix: "flowstate-run"}.For("")
	require.NoError(t, err)
	require.NotEqual(t, actual, forged)
	require.Equal(t, "flowstate-run__default", actual)
	require.Equal(t, "flowstate-run_default", forged)
}

// TestTaskQueueRoutingFailsClosed checks the configured path refuses what it
// cannot place, rather than falling back to the shared queue — the fallback
// temporalclient.Pool.For already refuses to make, for the same reason.
func TestTaskQueueRoutingFailsClosed(t *testing.T) {
	queues := engine.TaskQueues{Prefix: "flowstate-run"}

	for _, namespace := range []string{"Prod Team", "team_a", "-leading", "UPPER", ".."} {
		queue, err := queues.For(namespace)
		require.Error(t, err, "namespace %q", namespace)
		require.Empty(t, queue)
		require.NotEqual(t, engine.RunTaskQueueName, queue)
	}

	// And a prefix that could not compose a trustworthy boundary is refused when
	// configuration loads, not when a run arrives.
	for _, prefix := range []string{"flowstate_run", "Flowstate", "-leading", strings.Repeat("x", 64)} {
		require.Error(t, engine.TaskQueues{Prefix: prefix}.Validate(), "prefix %q", prefix)
	}
	require.NoError(t, engine.TaskQueues{Prefix: strings.Repeat("x", auth.MaxNamespaceLen)}.Validate())
}

// validPrefixes and validNamespaces are chosen to straddle one another: every
// prefix is also a legal namespace and vice versa, and several are prefixes or
// suffixes of others, so a composition that leaned on anything weaker than the
// separator argument would collide somewhere in the cross product.
func validPrefixes() []string {
	return []string{"q", "qa", "flowstate-run", "flowstate", "run", "team", "team-a", "a", "default"}
}

func validNamespaces() []string {
	return []string{"", "a", "team", "team-a", "a-team", "default", "run", "flowstate", "q", "qa", "0"}
}
