package server

import (
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/types/known/durationpb"
	"google.golang.org/protobuf/types/known/timestamppb"

	v1 "github.com/picatz/flowstate/pkg/flowstate/v1"
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

// TestAnUnwrittenBoundReachesTemporalUnset is the unit-level guard on the trap
// that took every schedule's future firings away.
//
// `(*timestamppb.Timestamp)(nil).AsTime()` is the Unix epoch, not the zero
// `time.Time` the Temporal SDK reads as "no bound". Copied through unconditionally,
// a schedule with no `end_at`, which is every schedule anybody had created,
// declared that it ended in 1970 and was created with nothing ahead of it. The
// cluster-level proof is TestBoundedRecoveryControlsReachTemporal; this is the same
// claim in a form that runs in milliseconds and names the field.
func TestAnUnwrittenBoundReachesTemporalUnset(t *testing.T) {
	t.Parallel()

	spec, err := scheduleSpecOf(&v1.ScheduleTrigger{Cron: []string{"0 * * * *"}})
	require.NoError(t, err)
	assert.True(t, spec.StartAt.IsZero(), "an unwritten start_at reached Temporal as %s", spec.StartAt)
	assert.True(t, spec.EndAt.IsZero(), "an unwritten end_at reached Temporal as %s", spec.EndAt)

	// And a bound that *was* written is carried, so the fix cannot be "never send
	// one", which would pass the assertions above and lose the feature.
	written := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	spec, err = scheduleSpecOf(&v1.ScheduleTrigger{
		Cron:    []string{"0 * * * *"},
		StartAt: timestamppb.New(written),
		EndAt:   timestamppb.New(written.AddDate(1, 0, 0)),
	})
	require.NoError(t, err)
	assert.Equal(t, written, spec.StartAt.UTC())
	assert.Equal(t, written.AddDate(1, 0, 0), spec.EndAt.UTC())
}

// TestAnUnwrittenCatchupWindowTakesTheBoundedDefault holds the substitution that
// makes the catch-up bound a bound at all.
//
// Temporal's default for an unset window is one year, so leaving the field absent
// is not "no catch-up": it is the largest catch-up in the system, applied to
// exactly the schedules whose authors never thought about it. The default is
// therefore applied here, and this is what says so.
func TestAnUnwrittenCatchupWindowTakesTheBoundedDefault(t *testing.T) {
	t.Parallel()

	assert.Equal(t, v1.DefaultScheduleCatchupWindow, scheduleCatchupWindowOf(&v1.ScheduleTrigger{}))
	assert.Equal(t, v1.DefaultScheduleCatchupWindow, scheduleCatchupWindowOf(nil))
	assert.Less(t, v1.DefaultScheduleCatchupWindow, v1.MaxScheduleCatchupWindow,
		"a default at the ceiling is not a default, it is the ceiling")

	// What was written wins, up to the ceiling the checker enforces.
	assert.Equal(t, 6*time.Hour, scheduleCatchupWindowOf(&v1.ScheduleTrigger{
		CatchupWindow: durationpb.New(6 * time.Hour),
	}))
}
