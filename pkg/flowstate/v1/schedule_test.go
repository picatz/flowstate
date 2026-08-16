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

	// `#` inside a day-of-week field is the nth-weekday operator, not a
	// comment. Cutting at the first one loses a field, which turns this
	// seconds-resolution expression into a minute-resolution estimate — the
	// under-count that lets a backfill past the ceiling.
	assert.ErrorContains(t, v1.CheckScheduleBackfillForTrigger(
		&v1.ScheduleTrigger{Cron: []string{"* * * * * 5#3 2030"}}, backfill(v1.MaxScheduleBackfillSpan)),
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

// TestScheduleBackfillCadenceSourcesAreUnionedNotSummed is the regression for
// #716's first Codex finding: two entries that are the identical cron
// expression, or the identical calendar, fire at exactly the same instants —
// their union is the size of either one alone, not their sum. Charging the
// duplicate a second time is a false refusal: it reports a fan-out twice the
// real one for a schedule that never asked for two cadences, only wrote one
// twice.
func TestScheduleBackfillCadenceSourcesAreUnionedNotSummed(t *testing.T) {
	t.Parallel()
	now := time.Now().UTC()
	backfill := func(d time.Duration) []*v1.ScheduleBackfill {
		return []*v1.ScheduleBackfill{{StartAt: timestamppb.New(now.Add(-d)), EndAt: timestamppb.New(now)}}
	}

	// One second cadence, doubled: the naive sum would charge it twice, which
	// crosses the ceiling at a window this cadence alone stays well under.
	window := (v1.MaxScheduleBackfillFirings * 3 / 4) * time.Second // 75,000s: under the limit alone, over it doubled.
	assert.NoError(t, v1.CheckScheduleBackfillForTrigger(
		&v1.ScheduleTrigger{Cron: []string{"* * * * * * *", "* * * * * * *"}}, backfill(window)),
		"the same seconds-resolution cron listed twice is one cadence, not two — "+
			"a naive sum would refuse a window this cadence alone accepts")

	// A trailing comment is the same expression underneath, so it dedupes with
	// the bare form the same way — [cronDedupeKey] normalizes both.
	assert.NoError(t, v1.CheckScheduleBackfillForTrigger(
		&v1.ScheduleTrigger{Cron: []string{"* * * * * * *", "* * * * * * * # every second"}}, backfill(window)),
		"a comment does not make an otherwise-identical cron expression a second cadence")

	// Two structurally distinct one-second cadences legitimately double the
	// count: the fix must not conflate "same period" with "same schedule".
	assert.ErrorContains(t, v1.CheckScheduleBackfillForTrigger(
		&v1.ScheduleTrigger{Every: durationpb.New(time.Second), Cron: []string{"* * * * * * *"}}, backfill(window)),
		"more than 100000 firings",
		"a genuinely distinct second cadence must still be charged its own count")

	// The identical calendar, listed twice, dedupes the same way a duplicate
	// cron does.
	secondly := &v1.ScheduleTrigger_Calendar{Second: []*v1.ScheduleTrigger_Calendar_Range{{Start: 0, End: 59}}}
	assert.NoError(t, v1.CheckScheduleBackfillForTrigger(
		&v1.ScheduleTrigger{Calendars: []*v1.ScheduleTrigger_Calendar{secondly, secondly}}, backfill(window)),
		"the identical calendar listed twice is one cadence, not two")

	// Two calendars that are not identical values are not proven to overlap,
	// so both are still charged — the safe direction this fix must not give up.
	assert.ErrorContains(t, v1.CheckScheduleBackfillForTrigger(
		&v1.ScheduleTrigger{Calendars: []*v1.ScheduleTrigger_Calendar{
			secondly,
			{Second: []*v1.ScheduleTrigger_Calendar_Range{{Start: 0, End: 58}}},
		}}, backfill(window)),
		"more than 100000 firings")
}

// TestScheduleBackfillIsChargedOnlyInsideTheTriggersOwnWindow is the
// regression for #716's second Codex finding: the firing estimate charged the
// full requested backfill range even when the trigger's own start_at/end_at
// left only a sliver of it reachable. Temporal will not fire a schedule
// outside its own window, so a requested range wider than that window cannot
// produce a single firing beyond its edge — charging the unreachable part
// over-estimates exactly the schedules a narrow window was written to keep
// small, and refuses them for a fan-out that can never happen.
func TestScheduleBackfillIsChargedOnlyInsideTheTriggersOwnWindow(t *testing.T) {
	t.Parallel()
	now := time.Now().UTC()

	// A one-second cadence over the full 31-day window would be refused on its
	// own (2,678,400 firings, over the 100,000 ceiling) — but the trigger's own
	// end_at closed the schedule's window a day into the requested 31-day
	// range, so only that first day's firings can ever have happened.
	requested := []*v1.ScheduleBackfill{{
		StartAt: timestamppb.New(now.Add(-v1.MaxScheduleBackfillSpan)),
		EndAt:   timestamppb.New(now),
	}}
	trigger := &v1.ScheduleTrigger{
		Every: durationpb.New(time.Second),
		EndAt: timestamppb.New(now.Add(-v1.MaxScheduleBackfillSpan + 24*time.Hour)),
	}
	assert.NoError(t, v1.CheckScheduleBackfillForTrigger(trigger, requested),
		"the trigger's own end_at closes the schedule's window a day into the requested 31-day range; "+
			"charging the whole 31 days over-estimates a schedule that cannot fire past its own window")

	// start_at intersects from the other side the same way: only the last hour
	// of the requested range falls after it.
	trigger = &v1.ScheduleTrigger{
		Every:   durationpb.New(time.Second),
		StartAt: timestamppb.New(now.Add(-time.Hour)),
	}
	assert.NoError(t, v1.CheckScheduleBackfillForTrigger(trigger, requested),
		"the trigger's own start_at closes off all but the last hour of the requested range")

	// A window that does not overlap the requested range at all charges
	// nothing — not a negative span, not a wrapped one.
	trigger = &v1.ScheduleTrigger{
		Every: durationpb.New(time.Second),
		EndAt: timestamppb.New(now.Add(-v1.MaxScheduleBackfillSpan - time.Hour)),
	}
	assert.NoError(t, v1.CheckScheduleBackfillForTrigger(trigger, requested),
		"a trigger window entirely before the requested range can never fire inside it")

	// Without a start_at/end_at, the full requested range is still charged —
	// unchanged behavior for the common case of an unbounded schedule.
	trigger = &v1.ScheduleTrigger{Every: durationpb.New(time.Second)}
	assert.ErrorContains(t, v1.CheckScheduleBackfillForTrigger(trigger, requested),
		"more than 100000 firings",
		"a trigger with no start_at/end_at is charged the full requested range, as before")
}

// TestScheduleBackfillWallClockDayCadenceIsUnboundedOutsideUTC is the
// regression for two rounds of the DST finding on the wall-clock-day arms —
// a calendar with no field finer than a day, and the day-or-longer cron
// shorthands (@daily, @weekly, @monthly, @yearly).
//
// Round one charged those cadences their exact nominal period (24 hours for
// a day, multiples of it for the others). A `time_zone` observing a
// spring-forward transition can put two local-midnight firings closer
// together than that, so the nominal period undercounts — the dangerous
// direction, unlike the two false-refusal bugs fixed above this test, since
// combined with a fast cadence near the ceiling it can let more than
// [v1.MaxScheduleBackfillFirings] real firings past the check.
//
// A fixed one-hour margin (round one's fix) closed the *standard*
// spring-forward case but not the general one: this package never resolves
// `time_zone` against real tzdata, so it cannot rule out a larger offset
// change for whichever zone a schedule names — IANA's database records
// changes far larger than one hour. So the actual fix charges a
// wall-clock-day-or-longer cadence its exact nominal period only in UTC,
// where no offset change can ever happen, and falls back to the fastest
// cadence there is — one second, the same fail-closed default an
// unclassifiable cron form gets — for any other named zone.
func TestScheduleBackfillWallClockDayCadenceIsUnboundedOutsideUTC(t *testing.T) {
	t.Parallel()
	now := time.Now().UTC()

	// The two-day span this file's own doc comments use as the running
	// example: a calendar or @daily backfilled over it fires twice.
	backfill := []*v1.ScheduleBackfill{{
		StartAt: timestamppb.New(now.Add(-48 * time.Hour)),
		EndAt:   timestamppb.New(now),
	}}
	// day_of_month is the field that satisfies "at least one field
	// populated" without moving calendarMinimumPeriod off its default
	// (day-resolution) case — only second/minute/hour do that.
	dailyCalendar := &v1.ScheduleTrigger_Calendar{
		DayOfMonth: []*v1.ScheduleTrigger_Calendar_Range{{Start: 1, End: 31}},
	}

	t.Run("UTC (unset time_zone) keeps the exact nominal period", func(t *testing.T) {
		assert.NoError(t, v1.CheckScheduleBackfillForTrigger(
			&v1.ScheduleTrigger{Calendars: []*v1.ScheduleTrigger_Calendar{dailyCalendar}}, backfill),
			"a two-day backfill of a daily calendar in UTC fires twice — no offset change can ever "+
				"shorten that gap, so the exact 24-hour period is a safe bound, not just a convenient one")
		assert.NoError(t, v1.CheckScheduleBackfillForTrigger(
			&v1.ScheduleTrigger{Cron: []string{"@daily"}}, backfill),
			"@daily in UTC is the identical case")
	})

	t.Run("a named time_zone falls back to the fastest cadence", func(t *testing.T) {
		trigger := &v1.ScheduleTrigger{
			TimeZone:  "America/New_York",
			Calendars: []*v1.ScheduleTrigger_Calendar{dailyCalendar},
		}
		assert.ErrorContains(t, v1.CheckScheduleBackfillForTrigger(trigger, backfill), "more than 100000 firings",
			"a daily calendar in a named zone must be charged as though it could fire once a second — "+
				"172,800 times over two days — not twice, because this package cannot rule out an offset "+
				"change of any particular size for a zone it never resolves against real tzdata")

		trigger.Calendars = nil
		trigger.Cron = []string{"@daily"}
		assert.ErrorContains(t, v1.CheckScheduleBackfillForTrigger(trigger, backfill), "more than 100000 firings",
			"@daily shares the same fallback as a calendar with no sub-day field")
	})

	t.Run("a cron expression's own CRON_TZ overrides the trigger's time_zone", func(t *testing.T) {
		// The trigger names a zone; the expression's own prefix names UTC,
		// which must win for that one entry and keep the exact nominal period.
		trigger := &v1.ScheduleTrigger{
			TimeZone: "America/New_York",
			Cron:     []string{"CRON_TZ=UTC @daily"},
		}
		assert.NoError(t, v1.CheckScheduleBackfillForTrigger(trigger, backfill),
			"the expression's own CRON_TZ=UTC must override the trigger's named zone for this entry")

		// And the reverse: the trigger is UTC (unset), but this entry's own
		// prefix names a zone, which must fall back to the fastest cadence.
		trigger = &v1.ScheduleTrigger{Cron: []string{"TZ=America/New_York @daily"}}
		assert.ErrorContains(t, v1.CheckScheduleBackfillForTrigger(trigger, backfill), "more than 100000 firings",
			"an expression's own TZ= prefix naming a zone must fall back, even with no trigger-level time_zone set")
	})

	t.Run("only second resolution is unaffected by zone", func(t *testing.T) {
		// An hour-resolution calendar is subject to the identical fallback as
		// the day-resolution default: a named zone's offset change (e.g.
		// America/Caracas's 2016 thirty-minute shift) can undercut the
		// assumed 60-minute floor exactly the way a day-scale shift
		// undercuts a daily one.
		trigger := &v1.ScheduleTrigger{
			TimeZone:  "America/New_York",
			Calendars: []*v1.ScheduleTrigger_Calendar{{Hour: []*v1.ScheduleTrigger_Calendar_Range{{Start: 9}}}},
		}
		assert.ErrorContains(t, v1.CheckScheduleBackfillForTrigger(trigger, backfill), "more than 100000 firings",
			"an hour-resolution calendar in a named zone must fall back the same way a day-resolution one does")

		// Minute resolution, the identical reasoning one step finer.
		trigger.Calendars = []*v1.ScheduleTrigger_Calendar{{Minute: []*v1.ScheduleTrigger_Calendar_Range{{Start: 0, End: 59}}}}
		assert.ErrorContains(t, v1.CheckScheduleBackfillForTrigger(trigger, backfill), "more than 100000 firings",
			"a minute-resolution calendar in a named zone must fall back too")

		// Second resolution is the one genuinely unaffected: it is already
		// this package's finest assumed granularity, so no offset change can
		// undercut it further — a small window that fits at one second in
		// UTC must still fit in a named zone.
		trigger.Calendars = []*v1.ScheduleTrigger_Calendar{{Second: []*v1.ScheduleTrigger_Calendar_Range{{Start: 0, End: 59}}}}
		small := []*v1.ScheduleBackfill{{StartAt: timestamppb.New(now.Add(-time.Minute)), EndAt: timestamppb.New(now)}}
		assert.NoError(t, v1.CheckScheduleBackfillForTrigger(trigger, small),
			"a second-resolution calendar is charged at one second regardless of zone, so a one-minute "+
				"window fits the same way it would in UTC")
	})
}

// TestScheduleBackfillIntersectedStartIsInclusive is the regression for the
// Codex finding on [intersectedBackfillSpan]'s own fix: the trigger's own
// start_at bounds its window inclusively (its doc comment says so
// explicitly), but when it becomes the effective left edge of a clipped
// range, the firing-count arithmetic still treated it as the exclusive edge
// of a (start, end] interval — the convention that arithmetic already uses
// correctly for the *requested* range's own start, and for both ranges'
// inclusive end. A firing landing exactly on that boundary went uncounted.
func TestScheduleBackfillIntersectedStartIsInclusive(t *testing.T) {
	t.Parallel()
	start := time.Now().UTC()

	// An epoch-aligned one-second cadence: exactly 100,000 seconds from
	// start_at to end_at inclusive holds 100,001 possible firing instants
	// (0 through 100000), one more than the span alone (100,000 seconds)
	// accounts for.
	trigger := &v1.ScheduleTrigger{
		Every:   durationpb.New(time.Second),
		StartAt: timestamppb.New(start),
	}
	// The requested range starts well before the trigger's own window, so
	// start_at — not the request's own start — is the effective left edge
	// intersectedBackfillSpan clips to.
	requested := []*v1.ScheduleBackfill{{
		StartAt: timestamppb.New(start.Add(-time.Hour)),
		EndAt:   timestamppb.New(start.Add(100_000 * time.Second)),
	}}

	assert.ErrorContains(t, v1.CheckScheduleBackfillForTrigger(trigger, requested), "more than 100000 firings",
		"the trigger's own start_at is inclusive, so this range can hold 100,001 firings, one more than "+
			"the 100,000-second span alone accounts for — the ceiling must be reached, not merely approached")

	// The identical span, entirely within the requested range's own edges —
	// no trigger start_at to clip to — is accepted at exactly the ceiling:
	// this proves the extra firing above is specifically about the
	// trigger-clipped boundary, not a general off-by-one in the span math.
	trigger = &v1.ScheduleTrigger{Every: durationpb.New(time.Second)}
	requested = []*v1.ScheduleBackfill{{
		StartAt: timestamppb.New(start),
		EndAt:   timestamppb.New(start.Add(100_000 * time.Second)),
	}}
	assert.NoError(t, v1.CheckScheduleBackfillForTrigger(trigger, requested),
		"without a trigger start_at to clip to, the requested range's own start is the ordinary "+
			"exclusive edge of (start, end], and 100,000 seconds at a one-second cadence is exactly "+
			"the ceiling, not one over it")
}

// TestScheduleBackfillIntersectedStartExactAtRequestedEnd is the regression
// for the finding on [TestScheduleBackfillIntersectedStartIsInclusive]'s own
// fix: when a requested range's end lands exactly on the trigger's start_at,
// the two clip to a zero-length span and the old code read that as "no
// overlap" — but both boundaries are inclusive at that single instant, so it
// can still be one real firing, and the old code charged it as zero.
func TestScheduleBackfillIntersectedStartExactAtRequestedEnd(t *testing.T) {
	t.Parallel()
	start := time.Now().UTC()

	trigger := &v1.ScheduleTrigger{
		Every:   durationpb.New(time.Second),
		StartAt: timestamppb.New(start),
	}
	// The second range's own start is exclusive (per this file's (start,
	// end] convention for a request the trigger doesn't also clip), so on
	// its own it holds exactly 100,000 firings — start+1s through
	// start+100000s — never claiming the instant at start itself. The first
	// range's end lands exactly on the trigger's start_at, so the clip
	// collapses it to zero length — but that single instant is still a real
	// firing the second range never claimed, so together they must be
	// refused at 100,001.
	backfills := []*v1.ScheduleBackfill{
		{StartAt: timestamppb.New(start.Add(-time.Hour)), EndAt: timestamppb.New(start)},
		{StartAt: timestamppb.New(start), EndAt: timestamppb.New(start.Add(100_000 * time.Second))},
	}
	assert.ErrorContains(t, v1.CheckScheduleBackfillForTrigger(trigger, backfills), "more than 100000 firings",
		"the zero-length range coincident with the trigger's own inclusive start_at is still one real "+
			"firing, which the second range's own exclusive start never claims, reaching 100,001 together")

	// The same pair, with the first range's end one nanosecond short of
	// start_at, has no coincident instant at all and must be accepted at
	// exactly the ceiling: 100,000 from the second range, zero from the first.
	backfills[0].EndAt = timestamppb.New(start.Add(-time.Nanosecond))
	assert.NoError(t, v1.CheckScheduleBackfillForTrigger(trigger, backfills),
		"without the coincident instant, the two ranges hold exactly 100,000 firings between the first "+
			"(none, entirely before start_at) and the second, exactly the ceiling and not one over it")
}

// TestScheduleBackfillInclusiveStartDoesNotDoubleCountTheRemainder is the
// regression for the second finding on the same fix: the closed-interval
// bound (floor(span/period)+1) and the half-open remainder bump
// (span%period!=0 -> +1) both add one firing for a boundary instant, but
// they are the *same* boundary instant when the trigger's start_at is the
// effective left edge — applying both, as the first version of the fix did,
// over-counts by one and refuses a backfill that fits exactly.
func TestScheduleBackfillInclusiveStartDoesNotDoubleCountTheRemainder(t *testing.T) {
	t.Parallel()
	start := time.Now().UTC()

	trigger := &v1.ScheduleTrigger{
		Every:   durationpb.New(time.Second),
		StartAt: timestamppb.New(start),
	}
	// A span of 99999s and 1ns holds exactly 100,000 firing instants in a
	// closed [start, end] one-second grid (0 through 99999 inclusive) — the
	// 1ns is what makes the span not an exact multiple of the period, which
	// is precisely the case the old code's extra unconditional bump got
	// wrong by charging 100,001.
	requested := []*v1.ScheduleBackfill{{
		StartAt: timestamppb.New(start.Add(-time.Hour)),
		EndAt:   timestamppb.New(start.Add(99_999*time.Second + time.Nanosecond)),
	}}
	assert.NoError(t, v1.CheckScheduleBackfillForTrigger(trigger, requested),
		"a closed one-second grid over 99999s1ns holds exactly 100,000 instants, at the ceiling, "+
			"not one over it")

	// One nanosecond more crosses into a real 100,001st instant and must be refused.
	requested[0].EndAt = timestamppb.New(start.Add(100_000 * time.Second))
	assert.ErrorContains(t, v1.CheckScheduleBackfillForTrigger(trigger, requested), "more than 100000 firings",
		"an exact 100,000-second span closed at both ends holds 100,001 instants, one over the ceiling")
}

// TestScheduleBackfillCalendarDedupeIgnoresComment is the regression for the
// finding that calendarsEqual compared every field, including comment — pure
// documentation Temporal never reads when matching a calendar, so two
// calendars whose ranges are identical still fire at exactly the same
// instants regardless of what their comments say, and failing to dedupe them
// double-charges one set of firings as if it were two.
func TestScheduleBackfillCalendarDedupeIgnoresComment(t *testing.T) {
	t.Parallel()
	now := time.Now().UTC()

	secondly := func(comment string) *v1.ScheduleTrigger_Calendar {
		return &v1.ScheduleTrigger_Calendar{
			Second:  []*v1.ScheduleTrigger_Calendar_Range{{Start: 0, End: 59}},
			Comment: comment,
		}
	}
	window := (v1.MaxScheduleBackfillFirings * 3 / 4) * time.Second // 75,000s: under the limit alone, over it doubled.
	backfill := []*v1.ScheduleBackfill{{
		StartAt: timestamppb.New(now.Add(-window)),
		EndAt:   timestamppb.New(now),
	}}

	assert.NoError(t, v1.CheckScheduleBackfillForTrigger(
		&v1.ScheduleTrigger{Calendars: []*v1.ScheduleTrigger_Calendar{secondly("primary"), secondly("backup")}},
		backfill),
		"two calendars identical but for their comment must dedupe the same way two byte-identical "+
			"calendars do — a naive sum would refuse a window this cadence alone accepts")
}

// TestScheduleBackfillCronDedupeIgnoresInternalWhitespace is the regression
// for the finding that cronDedupeKey trimmed only leading and trailing
// whitespace, so two spellings of the identical seven fields that differ in
// how many spaces separate two of them — "* * * * * * *" and its double-space
// cousin — produced different dedup keys and were charged as two cadences.
// Both are the same seven fields to the cluster, and cronMinimumPeriod's own
// classification already reads through strings.Fields, so the dedup key has
// to agree with it.
func TestScheduleBackfillCronDedupeIgnoresInternalWhitespace(t *testing.T) {
	t.Parallel()
	now := time.Now().UTC()

	window := (v1.MaxScheduleBackfillFirings * 3 / 4) * time.Second // 75,000s: under the limit alone, over it doubled.
	backfill := []*v1.ScheduleBackfill{{
		StartAt: timestamppb.New(now.Add(-window)),
		EndAt:   timestamppb.New(now),
	}}

	assert.NoError(t, v1.CheckScheduleBackfillForTrigger(
		&v1.ScheduleTrigger{Cron: []string{"* * * * * * *", "*  * * * * * *"}}, backfill),
		"a stray extra space between two fields must not stop the second entry from deduping "+
			"against the first — both are the identical seven fields to the cluster")
}

// TestScheduleBackfillJitterExpandsTheWindow is the regression for the
// finding that the firing estimate ignored `jitter`: each real firing is
// delayed by a random amount up to jitter (capped by the gap to the next
// firing, which only ever shrinks the real delay, never grows it), so a
// nominal firing up to `jitter` before the requested window's start can be
// delayed into it — a firing this estimate would otherwise miss entirely,
// since it never happens at its own nominal instant. This is the dangerous
// direction (an undercount), unlike most of the false-refusal bugs fixed
// above: a schedule with jitter set could pass a check that a real cluster
// then exceeds.
func TestScheduleBackfillJitterExpandsTheWindow(t *testing.T) {
	t.Parallel()
	now := time.Now().UTC()

	// Exactly 100,000 seconds at a one-second cadence is exactly the
	// ceiling with no jitter — TestScheduleBackfillIsBoundedByCadence
	// already covers that. One second of jitter means a nominal firing one
	// second before the window could be delayed into it, adding a real
	// 100,001st firing this estimate has to count.
	trigger := &v1.ScheduleTrigger{
		Every:  durationpb.New(time.Second),
		Jitter: durationpb.New(time.Second),
	}
	backfill := []*v1.ScheduleBackfill{{
		StartAt: timestamppb.New(now.Add(-100_000 * time.Second)),
		EndAt:   timestamppb.New(now),
	}}
	assert.ErrorContains(t, v1.CheckScheduleBackfillForTrigger(trigger, backfill), "more than 100000 firings",
		"one second of jitter on a one-second cadence can delay a firing from just before the window "+
			"into it, reaching a real 100,001 where the nominal schedule alone would only reach 100,000")

	// Without jitter, the identical range is exactly at the ceiling and must
	// be accepted — proving the refusal above is specifically about jitter,
	// not a general regression in the span math.
	trigger.Jitter = nil
	assert.NoError(t, v1.CheckScheduleBackfillForTrigger(trigger, backfill),
		"without jitter, 100,000 seconds at a one-second cadence is exactly the ceiling, not one over it")
}

// TestScheduleBackfillJitterIsCappedPerCadence is the regression for the
// finding on the jitter fix above: a real delay can never exceed the gap to
// the *next* firing (the schema's own doc comment says so), so padding a
// fast cadence's span by the full, uncapped jitter — as the first version of
// the fix did — refuses backfills a jitter that large could never actually
// affect. The maximum allowed jitter is 24 hours; an every-second schedule's
// own firings are never more than a second apart, so no delay on it can ever
// exceed a second regardless of how large jitter is configured.
func TestScheduleBackfillJitterIsCappedPerCadence(t *testing.T) {
	t.Parallel()
	now := time.Now().UTC()

	trigger := &v1.ScheduleTrigger{
		Every:  durationpb.New(time.Second),
		Jitter: durationpb.New(24 * time.Hour), // the field's own maximum
	}
	// 20,000 one-second firings, comfortably under the ceiling — and it must
	// stay that way: the uncapped fix padded this span by a full day
	// (86,400s), reaching 106,400 and refusing a window that can only ever
	// hold about 20,001 real firings (20,000 nominal, plus at most one more
	// pulled in from just before the window by jitter capped at one second).
	backfill := []*v1.ScheduleBackfill{{
		StartAt: timestamppb.New(now.Add(-20_000 * time.Second)),
		EndAt:   timestamppb.New(now),
	}}
	assert.NoError(t, v1.CheckScheduleBackfillForTrigger(trigger, backfill),
		"jitter must be capped at each cadence's own period before padding its span — a 24-hour jitter "+
			"on a one-second cadence can only ever shift a firing by at most one second")
}

// TestScheduleBackfillNamedZoneAffectsEveryWallClockResolution is the
// regression for the finding that only the day-or-longer default case went
// through the named-zone fallback, leaving minute and hour resolutions
// charged a flat nominal period regardless of zone — undercountable the
// identical way a day-scale cadence is, just at a finer unit
// (America/Caracas shifted by thirty minutes in 2016, undercutting an
// hour-resolution calendar's assumed 60-minute floor).
func TestScheduleBackfillNamedZoneAffectsEveryWallClockResolution(t *testing.T) {
	t.Parallel()
	now := time.Now().UTC()

	// A two-day window: an hour-resolution calendar in UTC fires 48 times,
	// comfortably accepted; in a named zone it must fall back to one-second
	// resolution, refused at 172,800.
	backfill := []*v1.ScheduleBackfill{{
		StartAt: timestamppb.New(now.Add(-48 * time.Hour)),
		EndAt:   timestamppb.New(now),
	}}
	hourly := &v1.ScheduleTrigger_Calendar{Hour: []*v1.ScheduleTrigger_Calendar_Range{{Start: 9}}}

	assert.NoError(t, v1.CheckScheduleBackfillForTrigger(
		&v1.ScheduleTrigger{Calendars: []*v1.ScheduleTrigger_Calendar{hourly}}, backfill),
		"an hour-resolution calendar in UTC fires 48 times over two days, far under the ceiling")

	assert.ErrorContains(t, v1.CheckScheduleBackfillForTrigger(
		&v1.ScheduleTrigger{TimeZone: "America/Caracas", Calendars: []*v1.ScheduleTrigger_Calendar{hourly}},
		backfill), "more than 100000 firings",
		"the identical calendar in a named zone must fall back to one-second resolution, not stay "+
			"charged in hours, since this package cannot rule out an offset change smaller than an hour")

	// Minute resolution, one step finer, same reasoning.
	minutely := &v1.ScheduleTrigger_Calendar{Minute: []*v1.ScheduleTrigger_Calendar_Range{{Start: 0, End: 59}}}
	assert.ErrorContains(t, v1.CheckScheduleBackfillForTrigger(
		&v1.ScheduleTrigger{TimeZone: "America/Caracas", Calendars: []*v1.ScheduleTrigger_Calendar{minutely}},
		backfill), "more than 100000 firings",
		"a minute-resolution calendar in a named zone must fall back too")

	// @hourly shares the fix.
	assert.NoError(t, v1.CheckScheduleBackfillForTrigger(
		&v1.ScheduleTrigger{Cron: []string{"@hourly"}}, backfill),
		"@hourly in UTC is the identical case as the calendar above")
	assert.ErrorContains(t, v1.CheckScheduleBackfillForTrigger(
		&v1.ScheduleTrigger{TimeZone: "America/Caracas", Cron: []string{"@hourly"}}, backfill),
		"more than 100000 firings",
		"@hourly in a named zone must fall back the same way the calendar does")
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
