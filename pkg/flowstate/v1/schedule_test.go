package flowstatev1_test

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/types/known/durationpb"
	"google.golang.org/protobuf/types/known/timestamppb"

	v1 "github.com/picatz/flowstate/pkg/flowstate/v1"
)

func TestScheduleBoundsAndBackfillAreBounded(t *testing.T) {
	now := time.Now().UTC()
	require.ErrorContains(t, v1.CheckScheduleTrigger(&v1.ScheduleTrigger{
		Every: durationpb.New(time.Hour), StartAt: timestamppb.New(now), EndAt: timestamppb.New(now.Add(-time.Hour)),
	}), "start_at must be before end_at")

	require.NoError(t, v1.CheckScheduleBackfill([]*v1.ScheduleBackfill{{
		StartAt: timestamppb.New(now.Add(-time.Hour)), EndAt: timestamppb.New(now),
	}}))
	require.ErrorContains(t, v1.CheckScheduleBackfill([]*v1.ScheduleBackfill{{
		StartAt: timestamppb.New(now.Add(-32 * 24 * time.Hour)), EndAt: timestamppb.New(now),
	}}), "maximum total span")
}

// TestCronExpressionsThatCannotBeRight covers what the checker must refuse.
//
// Every case here is wrong on every cluster in the world, which is the line the
// checker is held to: a cron expression is a property of the file, and what a
// particular Temporal deployment additionally dislikes is its own to say.
func TestCronExpressionsThatCannotBeRight(t *testing.T) {
	t.Parallel()

	for _, tt := range []struct {
		name string
		cron string
		want string
	}{
		{name: "four fields", cron: "0 9 * *", want: "has 4 fields"},
		{name: "eight fields", cron: "0 0 9 * * MON 2026 extra", want: "has 8 fields"},
		{name: "empty", cron: "   ", want: "says nothing about when to fire"},
		{name: "only a comment", cron: "# nightly", want: "says nothing about when to fire"},
		{name: "hour out of range", cron: "0 25 * * *", want: "hours is 25, which is outside 0-23"},
		{name: "minute out of range", cron: "99 9 * * *", want: "minutes is 99"},
		{name: "day of month zero", cron: "0 9 0 * *", want: "day of month is 0"},
		{name: "month thirteen", cron: "0 9 * 13 *", want: "month is 13"},
		{name: "month misspelled", cron: "0 9 * JANUAR *", want: `month is "JANUAR"`},
		{name: "weekday misspelled", cron: "0 9 * * MONDAY", want: `day of week is "MONDAY"`},
		{name: "range end out of order is fine but out of range is not", cron: "0 9 * * 1-9", want: "day of week is 9"},
		{name: "unknown shorthand", cron: "@dialy", want: "not a shorthand this understands"},
		{name: "every with no interval", cron: "@every", want: "`@every` with no interval"},
		{name: "seconds out of range in the seven-field form", cron: "60 0 9 * * * *", want: "seconds is 60"},
		{name: "a word where a number belongs", cron: "abc 9 * * *", want: `minutes is "abc"`},
		{name: "a word where an hour belongs", cron: "0 noon * * *", want: `hours is "noon"`},
	} {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			err := v1.CheckCronExpression(tt.cron)
			require.Error(t, err, "expected %q to be refused", tt.cron)
			assert.Contains(t, err.Error(), tt.want)
		})
	}
}

// TestCronExpressionsThatMustBeAccepted is the other direction, and the one that
// matters more.
//
// A false diagnostic is worse than a missing one: an author told their correct file
// is wrong has nowhere to go. So everything Temporal documents as legal is asserted
// legal here, including the syntax the checker deliberately does not model.
func TestCronExpressionsThatMustBeAccepted(t *testing.T) {
	t.Parallel()

	for _, cron := range []string{
		"0 9 * * MON-FRI",
		"0 12 * * MON-WED,FRI",
		"*/15 * * * *",
		"0 0 9 * * * ",
		"0 9 1 1 *",
		"0 9 * * 1-5 2026",
		"0 0 9 * * MON 2026",
		"@daily",
		"@hourly",
		"@midnight",
		"@every 15m",
		"@every 1h/10m",
		"CRON_TZ=America/New_York 0 9 * * *",
		"TZ=UTC 0 9 * * *",
		"0 9 * * MON-FRI # the weekday report",
		// Syntax the checker does not model, and so must not refuse.
		"0 9 L * *",
		"0 9 * * 5#3",
		"0 9 ? * MON",
		"0 9 15W * *",
	} {
		t.Run(cron, func(t *testing.T) {
			t.Parallel()

			assert.NoError(t, v1.CheckCronExpression(cron), "%q must be accepted", cron)
		})
	}
}

// TestAScheduleNeedsACadence holds the refusal that stops a schedule Temporal
// creates happily and never fires — the silent success worse than any error.
func TestAScheduleNeedsACadence(t *testing.T) {
	t.Parallel()

	require.ErrorContains(t, v1.CheckScheduleTrigger(nil), "a schedule needs a cadence")

	require.ErrorContains(t,
		v1.CheckScheduleTrigger(&v1.ScheduleTrigger{TimeZone: "UTC", Jitter: durationpb.New(60)}),
		"a schedule needs a cadence")

	assert.NoError(t, v1.CheckScheduleTrigger(&v1.ScheduleTrigger{Cron: []string{"0 9 * * *"}}))
	assert.NoError(t, v1.CheckScheduleTrigger(&v1.ScheduleTrigger{Every: durationpb.New(900)}))
}

// TestOverlapNamesComeFromTheSchema asserts the spelling is derived rather than
// listed, so an arm added to the enum is writable without anybody editing a list.
func TestOverlapNamesComeFromTheSchema(t *testing.T) {
	t.Parallel()

	names := v1.OverlapNames()
	assert.Equal(t, []string{"skip", "buffer_one", "buffer_all", "cancel_other", "terminate_other", "allow_all"}, names)

	for _, name := range names {
		overlap, ok := v1.ParseOverlap(name)
		require.True(t, ok, "%q must parse", name)
		assert.Equal(t, name, v1.OverlapName(overlap))
	}

	// The unspecified arm is not a name an author writes, in either direction.
	_, ok := v1.ParseOverlap("unspecified")
	assert.False(t, ok)
	_, ok = v1.ParseOverlap("unset")
	assert.False(t, ok)
}

// TestScheduleNameDefaultsToTheWorkflow holds the one rule that must not be
// computed twice: what the CLI reports it is creating and what the server creates.
func TestScheduleNameDefaultsToTheWorkflow(t *testing.T) {
	t.Parallel()

	wf := &v1.Workflow{Name: "nightly-report"}

	assert.Equal(t, "nightly-report", v1.ScheduleNameFor("", wf))
	assert.Equal(t, "report-eu", v1.ScheduleNameFor("report-eu", wf))
	assert.Equal(t, "", v1.ScheduleNameFor("", &v1.Workflow{}))
}
