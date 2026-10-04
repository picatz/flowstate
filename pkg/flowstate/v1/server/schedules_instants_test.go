package server_test

import (
	"slices"
	"testing"
	"time"

	"connectrpc.com/connect"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/types/known/timestamppb"

	v1 "github.com/picatz/flowstate/pkg/flowstate/v1"
	"github.com/picatz/flowstate/pkg/flowstate/v1/flowfile"
)

// A backfilled firing reads the slot Temporal recovered it for, not the wall
// clock it ran at (#1904), and a manual fire is a firing now: its slot is the
// moment it was asked for, which keeps a window computed from the slot well
// defined where the Unix epoch would not.
//
// This runs against a real Temporal dev server, because what is being proved is
// that the `TemporalScheduledStartTime` attribute the engine reads is the one
// Temporal writes, with the type and the nominal slot the engine expects. A test
// that injects the attribute into the SDK's test environment would pass
// whatever Temporal actually did.
func TestAScheduledFiringReadsTheSlotItWasFor(t *testing.T) {
	t.Parallel()

	fixture := newTenantFixture(t)

	workflow, _, err := flowfile.Parse([]byte(`
edition: v2026.4
name: window
triggers:
  schedule:
    cron: 0 * * * *
steps:
  - id: noop
    value: 1
outputs:
  slot:
    value: ${string(trigger.scheduled_at)}
  started:
    value: ${string(run.started_at)}
`))
	require.NoError(t, err)

	// Two hourly slots that closed in the past: exclusive start, inclusive end.
	hour := time.Now().UTC().Truncate(time.Hour)
	slots := []time.Time{hour.Add(-2 * time.Hour), hour.Add(-time.Hour)}
	before := time.Now().UTC().Add(-time.Second)

	_, err = fixture.teamA.CreateSchedule(t.Context(), connect.NewRequest(&v1.CreateScheduleRequest{
		Workflow: workflow,
		Backfill: []*v1.ScheduleBackfill{{
			StartAt: timestamppb.New(slots[0].Add(-time.Minute)),
			EndAt:   timestamppb.New(slots[1]),
			// Allowed to overlap, so the second slot is not skipped while the
			// first is still running.
			Overlap: v1.ScheduleTrigger_OVERLAP_ALLOW_ALL,
		}},
		// Paused, so nothing fires but what the test asks for.
		Paused: true,
	}))
	require.NoError(t, err)

	// What each run reported, by the workflow id Temporal gave it.
	read := func(workflowID string) (slot, started time.Time) {
		t.Helper()

		var out v1.Workflow_StepOutputs
		require.NoError(t, fixture.temporal.GetWorkflow(t.Context(), workflowID, "").Get(t.Context(), &out))
		values := out.GetRunOutputs().GetValues()
		slot, err := time.Parse(time.RFC3339Nano, values["slot"].GetLiteral().GetStringValue())
		require.NoError(t, err)
		started, err = time.Parse(time.RFC3339Nano, values["started"].GetLiteral().GetStringValue())
		require.NoError(t, err)

		return slot, started
	}
	recent := func(n int) []string {
		var ids []string
		require.Eventually(t, func() bool {
			described, err := fixture.teamA.DescribeSchedule(t.Context(),
				connect.NewRequest(&v1.DescribeScheduleRequest{Name: "window"}))
			if err != nil {
				return false
			}
			ids = ids[:0]
			for _, run := range described.Msg.GetSchedule().GetRecentRuns() {
				ids = append(ids, run.GetWorkflowId())
			}

			return len(ids) >= n
		}, 60*time.Second, 100*time.Millisecond, "the schedule has not started %d runs", n)

		return ids
	}

	t.Run("a backfilled firing reads the recovered slot, and began now", func(t *testing.T) {
		var got []time.Time
		for _, id := range recent(2) {
			slot, started := read(id)
			got = append(got, slot)
			assert.True(t, started.After(before),
				"run.started_at %s is the wall clock the run began at, after the test began", started)
			assert.True(t, started.After(slot.Add(30*time.Minute)),
				"a recovered slot (%s) is well before the run that recovers it (%s)", slot, started)
		}
		assert.ElementsMatch(t, slots, got, "each backfilled run reads its own slot")
	})

	t.Run("a manual fire is a firing now, not the epoch", func(t *testing.T) {
		asked := time.Now().UTC().Add(-time.Second)
		_, err := fixture.teamA.TriggerSchedule(t.Context(),
			connect.NewRequest(&v1.TriggerScheduleRequest{Name: "window"}))
		require.NoError(t, err)

		var manual string
		for _, id := range recent(3) {
			slot, _ := read(id)
			if !slices.ContainsFunc(slots, slot.Equal) {
				manual = id
			}
		}
		require.NotEmpty(t, manual, "the manual fire started no run")

		slot, started := read(manual)
		assert.False(t, slot.Before(asked), "the slot %s is the moment of the fire, after %s", slot, asked)
		assert.False(t, slot.After(started.Add(time.Second)), "and not after the run began: %s vs %s", slot, started)
	})
}
