package server

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/picatz/flowstate/pkg/flowstate/v1/secrets"
)

// TestScheduleIDsAreUnambiguous is the check the env secret provider did not have.
//
// That provider derived `prefix + NAMESPACE + "_" + name`, and because every
// character legal in a namespace was also legal in a name, three different tenants
// resolved to one variable and read each other's secrets. The encoding here has the
// same shape, so it gets the check that one lacked: no two distinct (tenant, name)
// pairs may produce the same id, and every id must read back as the pair that made
// it.
//
// The property holds because `secrets.ValidateNamespace` forbids an underscore in a
// namespace, so the first underscore after the prefix is always the separator
// whatever the name contains. That is a fact about another package, which is why it
// is asserted here rather than assumed.
func TestScheduleIDsAreUnambiguous(t *testing.T) {
	t.Parallel()

	namespaces := []string{"", "team-a", "team", "a", "team-a-b", strings.Repeat("t", 63)}
	names := []string{"nightly", "a_nightly", "_nightly", "nightly_report", "team-a_nightly", "a", "_"}

	seen := make(map[string][2]string)

	for _, namespace := range namespaces {
		require.NoError(t, secrets.ValidateNamespace(namespace),
			"a namespace this test claims is legal is not")

		for _, name := range names {
			id := scheduleIDFor(namespace, name)

			if previous, collided := seen[id]; collided {
				t.Fatalf("id %q is produced by both %q/%q and %q/%q", id,
					previous[0], previous[1], namespace, name)
			}
			seen[id] = [2]string{namespace, name}

			// And it reads back as exactly what went in, which is what makes a
			// listing able to report a tenant's own names.
			got, mine := scheduleNameFrom(id, namespace)
			require.True(t, mine, "id %q did not read back as %q's", id, namespace)
			assert.Equal(t, name, got)
		}
	}
}

// TestAScheduleIDIsNotReadableByAnotherTenant is the negative direction of the
// same encoding: the ids one tenant can produce must never be claimed by another.
//
// This is the direction the env provider's tests missed. Each of theirs asserted
// that a tenant reads its own secret, which a broken encoding also satisfies.
func TestAScheduleIDIsNotReadableByAnotherTenant(t *testing.T) {
	t.Parallel()

	for _, tt := range []struct {
		owner, name, reader string
	}{
		{owner: "team-a", name: "nightly", reader: "team"},
		{owner: "team", name: "a_nightly", reader: "team-a"},
		{owner: "team-a", name: "nightly", reader: ""},
		{owner: "", name: "team-a_nightly", reader: "team-a"},
		{owner: "team-a", name: "nightly", reader: "team-b"},
	} {
		id := scheduleIDFor(tt.owner, tt.name)

		_, mine := scheduleNameFrom(id, tt.reader)
		assert.False(t, mine, "%q read %q's schedule id %q as its own", tt.reader, tt.owner, id)
	}
}

// TestAnIDThatIsNotOursIsNotOurs covers what else may share a Temporal namespace.
//
// A schedule created by another application has no Flowstate prefix, and a listing
// that returned it would hand a caller a name that `flow schedule delete` would
// then act on.
func TestAnIDThatIsNotOursIsNotOurs(t *testing.T) {
	t.Parallel()

	for _, id := range []string{
		"some-other-apps-schedule",
		"flowstate-schedule-team-a",         // No separator: not a pair.
		"flowstate-schedule-team-a_",        // A separator and no name.
		"flowstate-workflow-team-a_nightly", // The runs' prefix, not the schedules'.
	} {
		_, mine := scheduleNameFrom(id, "team-a")
		assert.False(t, mine, "%q was read as team-a's schedule", id)
	}
}
