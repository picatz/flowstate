package flowstatev1_test

import (
	"math"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/types/known/durationpb"
	"google.golang.org/protobuf/types/known/timestamppb"

	v1 "github.com/picatz/flowstate/pkg/flowstate/v1"
)

// TestScheduleWindowBoundsHoldAtTheirEdges checks the bounds on a schedule's own
// window, at the values where they change their answer.
//
// Each bound is asserted *reached* as well as not exceeded, which is CLAUDE.md's
// rule for a bound: a checker that refuses everything satisfies "nothing over the
// limit gets through" and is useless, so the exact limit has to be accepted in the
// same test that refuses the value one nanosecond past it.
func TestScheduleWindowBoundsHoldAtTheirEdges(t *testing.T) {
	t.Parallel()

	now := time.Now().UTC()
	every := func(trigger *v1.ScheduleTrigger) *v1.ScheduleTrigger {
		trigger.Every = durationpb.New(time.Hour)

		return trigger
	}

	// A window whose start is after its end can never contain a firing, and one
	// whose start *equals* its end cannot either. The equal case is the edge, and
	// it is the one a checker written with `After` lets through.
	assert.ErrorContains(t, v1.CheckScheduleTrigger(every(&v1.ScheduleTrigger{
		StartAt: timestamppb.New(now), EndAt: timestamppb.New(now.Add(-time.Hour)),
	})), "is not before its `end_at`")
	assert.ErrorContains(t, v1.CheckScheduleTrigger(every(&v1.ScheduleTrigger{
		StartAt: timestamppb.New(now), EndAt: timestamppb.New(now),
	})), "is not before its `end_at`")
	assert.NoError(t, v1.CheckScheduleTrigger(every(&v1.ScheduleTrigger{
		StartAt: timestamppb.New(now), EndAt: timestamppb.New(now.Add(time.Nanosecond)),
	})))

	// Either bound alone is legal: a schedule that starts on Monday and runs
	// forever is a schedule, and so is one that has only an end.
	assert.NoError(t, v1.CheckScheduleTrigger(every(&v1.ScheduleTrigger{StartAt: timestamppb.New(now)})))
	assert.NoError(t, v1.CheckScheduleTrigger(every(&v1.ScheduleTrigger{EndAt: timestamppb.New(now)})))

	// The catch-up window, at both ends and at the values just outside them.
	assert.NoError(t, v1.CheckScheduleTrigger(every(&v1.ScheduleTrigger{
		CatchupWindow: durationpb.New(v1.MinScheduleCatchupWindow),
	})), "the shortest allowed catch-up window must be allowed")
	assert.ErrorContains(t, v1.CheckScheduleTrigger(every(&v1.ScheduleTrigger{
		CatchupWindow: durationpb.New(v1.MinScheduleCatchupWindow - time.Nanosecond),
	})), "shortest window")
	assert.NoError(t, v1.CheckScheduleTrigger(every(&v1.ScheduleTrigger{
		CatchupWindow: durationpb.New(v1.MaxScheduleCatchupWindow),
	})), "the longest allowed catch-up window must be allowed")
	assert.ErrorContains(t, v1.CheckScheduleTrigger(every(&v1.ScheduleTrigger{
		CatchupWindow: durationpb.New(v1.MaxScheduleCatchupWindow + time.Nanosecond),
	})), "longest window")

	// A calendar is a cadence in its own right, so a schedule carrying one and
	// nothing else is not the "says nothing about when to fire" case.
	assert.NoError(t, v1.CheckScheduleTrigger(&v1.ScheduleTrigger{
		Calendars: []*v1.ScheduleTrigger_Calendar{{Hour: []*v1.ScheduleTrigger_Calendar_Range{{Start: 7}}}},
	}))
}

// TestScheduleBackfillBoundsAreReachedAndNotExceeded holds both bounds on a
// create-time replay, including the one arithmetic can lose.
//
// The overflow case is the reason this test is written against durations rather
// than against a couple of plausible operator inputs. `time.Duration` is a signed
// nanosecond count that saturates at about 292 years, and a checker that *sums*
// spans before comparing the total wraps negative on two RFC3339 ranges a caller
// is free to send, making a request for several millennia of history compare as
// comfortably under 31 days.
func TestScheduleBackfillBoundsAreReachedAndNotExceeded(t *testing.T) {
	t.Parallel()

	now := time.Now().UTC()
	span := func(d time.Duration) *v1.ScheduleBackfill {
		return &v1.ScheduleBackfill{StartAt: timestamppb.New(now.Add(-d)), EndAt: timestamppb.New(now)}
	}

	assert.NoError(t, v1.CheckScheduleBackfill(nil), "no backfill at all is the ordinary case")

	// The count bound, reached and then exceeded by one. Each range is short
	// enough that the *span* bound cannot be what refuses the second listing.
	var many []*v1.ScheduleBackfill
	for range v1.MaxScheduleBackfills {
		many = append(many, span(time.Hour))
	}
	assert.NoError(t, v1.CheckScheduleBackfill(many), "exactly the maximum number of ranges must be accepted")
	assert.ErrorContains(t, v1.CheckScheduleBackfill(append(many, span(time.Hour))), "at most 10 are accepted")

	// The span bound, likewise: the whole budget in one range is fine, and one
	// nanosecond more is not.
	assert.NoError(t, v1.CheckScheduleBackfill([]*v1.ScheduleBackfill{span(v1.MaxScheduleBackfillSpan)}),
		"a backfill of exactly the maximum span must be accepted")
	assert.ErrorContains(t,
		v1.CheckScheduleBackfill([]*v1.ScheduleBackfill{span(v1.MaxScheduleBackfillSpan + time.Nanosecond)}),
		"more than 744h0m0s of history")

	// The budget is a *total*, so ranges that are each acceptable are refused
	// together: the count bound alone would let ten 30-day ranges through.
	assert.ErrorContains(t, v1.CheckScheduleBackfill([]*v1.ScheduleBackfill{
		span(v1.MaxScheduleBackfillSpan / 2), span(v1.MaxScheduleBackfillSpan/2 + time.Nanosecond),
	}), "more than 744h0m0s of history")

	// Two ranges wide enough that each saturates `Time.Sub`, which is where an
	// accumulating total wraps negative and reports millennia as under a month.
	wide := &v1.ScheduleBackfill{
		StartAt: timestamppb.New(time.Date(1, 1, 1, 0, 0, 0, 0, time.UTC)),
		EndAt:   timestamppb.New(time.Date(9999, 12, 31, 23, 59, 59, 0, time.UTC)),
	}
	assert.ErrorContains(t, v1.CheckScheduleBackfill([]*v1.ScheduleBackfill{wide, wide}),
		"more than 744h0m0s of history")

	// A range that says nothing, and one that runs backwards.
	assert.ErrorContains(t, v1.CheckScheduleBackfill([]*v1.ScheduleBackfill{{StartAt: timestamppb.New(now)}}),
		"needs both a start and an end")
	assert.ErrorContains(t, v1.CheckScheduleBackfill([]*v1.ScheduleBackfill{span(-time.Hour)}),
		"the start must come first")
	assert.ErrorContains(t, v1.CheckScheduleBackfill([]*v1.ScheduleBackfill{span(0)}),
		"the start must come first")
}

func TestScheduleBackfillIsBoundedByCadence(t *testing.T) {
	t.Parallel()
	now := time.Now().UTC()
	backfill := func(d time.Duration) []*v1.ScheduleBackfill {
		return []*v1.ScheduleBackfill{{StartAt: timestamppb.New(now.Add(-d)), EndAt: timestamppb.New(now)}}
	}
	oneSecond := &v1.ScheduleTrigger{Every: durationpb.New(time.Second)}

	assert.NoError(t, v1.CheckScheduleBackfillForTrigger(oneSecond,
		backfill(v1.MaxScheduleBackfillFirings*time.Second)))
	assert.ErrorContains(t, v1.CheckScheduleBackfillForTrigger(oneSecond,
		backfill((v1.MaxScheduleBackfillFirings+1)*time.Second)), "more than 100000 firings")
	assert.NoError(t, v1.CheckScheduleBackfillForTrigger(
		&v1.ScheduleTrigger{Cron: []string{"0 9 * * *"}}, backfill(v1.MaxScheduleBackfillSpan)),
		"ordinary minute-resolution cron keeps the existing 31-day recovery window")

	// Rounding down is not the maximum count a half-open (start, end] range can
	// hold: aligned, it holds one more. At a 3s cadence a 300001s range holds
	// 100,001 firings, which the floor calls 100,000 and lets past the ceiling.
	assert.ErrorContains(t, v1.CheckScheduleBackfillForTrigger(
		&v1.ScheduleTrigger{Every: durationpb.New(3 * time.Second)}, backfill(300001*time.Second)),
		"more than 100000 firings")

	// Every cron form CheckCronExpression accepts has to be classified the way
	// that function reads it, not by counting whitespace-separated fields in the
	// raw string. All of these fire far under the limit over the widest
	// permitted window, and an estimator that reads them as one-second cadences
	// refuses every one.
	for _, expression := range []string{
		"@daily",
		"@midnight",
		"@hourly",
		"@weekly",
		"@monthly",
		"@yearly",
		"@every 15m",
		"CRON_TZ=UTC 0 9 * * *",
		"TZ=UTC 0 9 * * *",
		"0 9 * * * 2030", // Six fields: the ordinary five, plus a year.
		"0 9 * * MON-FRI # weekday mornings",
	} {
		require.NoError(t, v1.CheckCronExpression(expression),
			"premise: %q is an expression this repository accepts", expression)
		assert.NoError(t, v1.CheckScheduleBackfillForTrigger(
			&v1.ScheduleTrigger{Cron: []string{expression}}, backfill(v1.MaxScheduleBackfillSpan)),
			"cron %q fires well under the limit over the widest permitted window", expression)
	}

	// The seven-field form puts seconds first and so is charged a second, as is
	// a calendar, which carries its own seconds field.
	assert.ErrorContains(t, v1.CheckScheduleBackfillForTrigger(
		&v1.ScheduleTrigger{Cron: []string{"* * * * * * 2030"}}, backfill(v1.MaxScheduleBackfillSpan)),
		"more than 100000 firings")
	assert.ErrorContains(t, v1.CheckScheduleBackfillForTrigger(
		&v1.ScheduleTrigger{Calendars: []*v1.ScheduleTrigger_Calendar{
			{Second: []*v1.ScheduleTrigger_Calendar_Range{{Start: 0, End: 59}}},
		}}, backfill(v1.MaxScheduleBackfillSpan)),
		"more than 100000 firings")

	// A calendar's unwritten second, minute and hour default to zero rather
	// than to "every", so the fields it does write decide how fast it can be.
	// Charging every calendar one firing a second refuses `hour: 9` — a daily
	// schedule — over any span past about a day.
	for _, c := range []*v1.ScheduleTrigger_Calendar{
		{}, // Nothing written: 00:00:00, once a day.
		{Hour: []*v1.ScheduleTrigger_Calendar_Range{{Start: 9}}},
		{Hour: []*v1.ScheduleTrigger_Calendar_Range{{Start: 9, End: 17}}},
		{Minute: []*v1.ScheduleTrigger_Calendar_Range{{Start: 0, End: 59}}},
	} {
		assert.NoError(t, v1.CheckScheduleBackfillForTrigger(
			&v1.ScheduleTrigger{Calendars: []*v1.ScheduleTrigger_Calendar{c}},
			backfill(v1.MaxScheduleBackfillSpan)),
			"a calendar writing no seconds fires far under the limit over the widest window")
	}

	// An `@every` period near the top of time.Duration must not wrap the
	// ceiling arithmetic negative and pay for another cadence's real firings.
	assert.ErrorContains(t, v1.CheckScheduleBackfillForTrigger(
		&v1.ScheduleTrigger{
			Every: durationpb.New(time.Duration(math.MaxInt64)),
			Cron:  []string{"* * * * * * 2030"},
		}, backfill((v1.MaxScheduleBackfillFirings+1)*time.Second)),
		"more than 100000 firings")

	// Each cadence is charged its own count. A daily cron beside a one-second
	// interval fires 86,401 times a day; charging both the faster of the two
	// would report 172,800 and refuse a backfill well inside the limit.
	assert.NoError(t, v1.CheckScheduleBackfillForTrigger(
		&v1.ScheduleTrigger{Every: durationpb.New(time.Second), Cron: []string{"@daily"}},
		backfill(24*time.Hour)),
		"a day of one-second firings plus one daily firing is inside the limit")

	// A trigger with no cadence at all cannot have its firing count bounded, and
	// is refused rather than passed through unbounded.
	assert.ErrorContains(t, v1.CheckScheduleBackfillForTrigger(&v1.ScheduleTrigger{}, backfill(time.Hour)),
		"needs a schedule cadence")
}

// TestScheduleCalendarsAreCheckedAgainstTemporalsOwnRanges covers the calendar
// values that cannot be right on any cluster.
//
// Everything refused here is refused by Temporal's own field ranges, which is what
// keeps this on the correct side of docs/DSL.md's line: a month of 13 is a property
// of the file, and the checker may say so. The empty calendar is the interesting
// one, because Temporal *accepts* it, as 00:00:00 every day, so it is the silent
// success this repository ranks worst rather than a value a cluster would refuse.
func TestScheduleCalendarsAreCheckedAgainstTemporalsOwnRanges(t *testing.T) {
	t.Parallel()

	calendar := func(c *v1.ScheduleTrigger_Calendar) *v1.ScheduleTrigger {
		return &v1.ScheduleTrigger{Calendars: []*v1.ScheduleTrigger_Calendar{c}}
	}
	at := func(v int32) []*v1.ScheduleTrigger_Calendar_Range {
		return []*v1.ScheduleTrigger_Calendar_Range{{Start: v}}
	}

	assert.ErrorContains(t, v1.CheckScheduleTrigger(calendar(&v1.ScheduleTrigger_Calendar{})),
		"says nothing about when to fire")
	assert.ErrorContains(t,
		v1.CheckScheduleTrigger(calendar(&v1.ScheduleTrigger_Calendar{Comment: "documented and empty"})),
		"says nothing about when to fire")

	for _, tt := range []struct {
		name     string
		calendar *v1.ScheduleTrigger_Calendar
		want     string
	}{
		{"month 13", &v1.ScheduleTrigger_Calendar{Month: at(13)}, "month starting at 13, which is outside 1-12"},
		{"month 0", &v1.ScheduleTrigger_Calendar{Month: at(0)}, "month starting at 0, which is outside 1-12"},
		{"hour 24", &v1.ScheduleTrigger_Calendar{Hour: at(24)}, "hour starting at 24, which is outside 0-23"},
		{"minute 60", &v1.ScheduleTrigger_Calendar{Minute: at(60)}, "minute starting at 60, which is outside 0-59"},
		{"second 60", &v1.ScheduleTrigger_Calendar{Second: at(60)}, "second starting at 60, which is outside 0-59"},
		{"day of month 0", &v1.ScheduleTrigger_Calendar{DayOfMonth: at(0)}, "day_of_month starting at 0, which is outside 1-31"},
		{"day of month 32", &v1.ScheduleTrigger_Calendar{DayOfMonth: at(32)}, "day_of_month starting at 32, which is outside 1-31"},

		// Seven is Sunday in cron and is not a day of the week in a Temporal
		// calendar, which is the sort of difference a shared checker has to keep.
		{"day of week 7", &v1.ScheduleTrigger_Calendar{DayOfWeek: at(7)}, "day_of_week starting at 7, which is outside 0-6"},
		{"year 1969", &v1.ScheduleTrigger_Calendar{Year: at(1969)}, "year starting at 1969, which is outside 1970-3000"},
		{
			"end outside the field",
			&v1.ScheduleTrigger_Calendar{Hour: []*v1.ScheduleTrigger_Calendar_Range{{Start: 9, End: 25}}},
			"hour ending at 25, which is outside 0-23",
		},
		{
			"descending range",
			&v1.ScheduleTrigger_Calendar{Hour: []*v1.ScheduleTrigger_Calendar_Range{{Start: 17, End: 9}}},
			"hour running from 17 down to 9",
		},
		{
			"negative step",
			&v1.ScheduleTrigger_Calendar{Hour: []*v1.ScheduleTrigger_Calendar_Range{{Start: 9, End: 17, Step: -2}}},
			"hour step of -2",
		},
		{"an empty range", &v1.ScheduleTrigger_Calendar{Hour: []*v1.ScheduleTrigger_Calendar_Range{nil}}, "empty hour range"},
	} {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			assert.ErrorContains(t, v1.CheckScheduleTrigger(calendar(tt.calendar)), tt.want)
		})
	}

	// What the checker must not refuse: every field at both edges of its range, a
	// stepped range, and an `end` of zero, which the schema cannot tell from an end
	// nobody wrote and Temporal reads as the start alone.
	assert.NoError(t, v1.CheckScheduleTrigger(calendar(&v1.ScheduleTrigger_Calendar{
		Second:     []*v1.ScheduleTrigger_Calendar_Range{{Start: 0, End: 59, Step: 15}},
		Minute:     []*v1.ScheduleTrigger_Calendar_Range{{Start: 0, End: 59}},
		Hour:       []*v1.ScheduleTrigger_Calendar_Range{{Start: 0, End: 23, Step: 2}},
		DayOfMonth: []*v1.ScheduleTrigger_Calendar_Range{{Start: 1, End: 31}},
		Month:      []*v1.ScheduleTrigger_Calendar_Range{{Start: 1, End: 12}},
		Year:       []*v1.ScheduleTrigger_Calendar_Range{{Start: 1970, End: 3000}},
		DayOfWeek:  []*v1.ScheduleTrigger_Calendar_Range{{Start: 0, End: 6}},
		Comment:    "every edge of every field",
	})))
	assert.NoError(t, v1.CheckScheduleTrigger(calendar(&v1.ScheduleTrigger_Calendar{
		Hour: []*v1.ScheduleTrigger_Calendar_Range{{Start: 9}, {Start: 17}},
	})), "a range with no end holds its start alone, and two of them are two hours")
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
