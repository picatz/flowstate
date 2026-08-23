package flowstatev1

import (
	"fmt"
	"strconv"
	"strings"
	"time"

	"google.golang.org/protobuf/proto"
)

// What a schedule can be wrong about before a cluster ever sees it.
//
// A trigger is read by three surfaces that must agree: `flow validate` in an
// author's editor, `flow schedule create` on the way to a server, and the server
// itself. Two of those have a line number to point at and one does not, so the
// checks live here, once, and each caller decides how to present what comes back —
// the same shape [CheckInputDefault] and [BindRunInputs] have, for the same reason
// the retry-attempt default had to move here: a rule with two implementations
// eventually has two meanings.
//
// # The line these checks stop at
//
// docs/DSL.md's rule is that a validator reports what is a property of the file and
// stays silent about what a deployment decides. A cron expression is a property of
// the file: `0 9 * *` has four fields and is wrong on every cluster in the world,
// and reporting it in an editor costs nothing and no I/O. Whether the cluster's zone
// database has `America/Argentina/ComodRivadavia` is not, so what is checked about a
// time zone is only that it is shaped like a zone name at all.
//
// The bias throughout is towards accepting. A false diagnostic is worse than a
// missing one — an author told their correct file is wrong has no way forward — so
// where Temporal's own grammar is generous, so is this. Every check below refuses
// only what cannot be right, and leaves the cluster the authority on the rest.

// CheckScheduleTrigger reports whether a schedule's cadence is one that could fire.
//
// The errors name what is wrong and what to write instead, because they are read by
// an author in an editor as often as by a caller of the API.
func CheckScheduleTrigger(trigger *ScheduleTrigger) error {
	if trigger == nil {
		return fmt.Errorf("a schedule needs a cadence: write `cron:` with a cron expression, `every:` with an " +
			"interval such as 15m, or `calendars:` with the times to match")
	}

	for _, expression := range trigger.GetCron() {
		if err := CheckCronExpression(expression); err != nil {
			return err
		}
	}

	// A jitter or a time zone with nothing to apply it to is not merely useless: it
	// is a line that reads as if the schedule fires, written by somebody who
	// believed it did. A schedule with no cadence at all is created successfully by
	// Temporal and never fires, which is the silent failure worth refusing.
	if len(trigger.GetCron()) == 0 && trigger.GetEvery() == nil && len(trigger.GetCalendars()) == 0 {
		return fmt.Errorf("a schedule needs a cadence: write `cron:` with a cron expression, `every:` with an " +
			"interval such as 15m, or `calendars:` with the times to match")
	}
	if start, end := trigger.GetStartAt(), trigger.GetEndAt(); start != nil && end != nil && !start.AsTime().Before(end.AsTime()) {
		return fmt.Errorf("the schedule's `start_at` (%s) is not before its `end_at` (%s), so no firing time can "+
			"fall between them and the schedule would never fire; write the earlier instant first",
			start.AsTime().UTC().Format(time.RFC3339), end.AsTime().UTC().Format(time.RFC3339))
	}

	if window := trigger.GetCatchupWindow(); window != nil {
		switch d := window.AsDuration(); {
		case d < MinScheduleCatchupWindow:
			return fmt.Errorf("`catchup_window:` is %s; the shortest window that can recover anything is %s, "+
				"and leaving the key out takes %s", d, MinScheduleCatchupWindow, DefaultScheduleCatchupWindow)
		case d > MaxScheduleCatchupWindow:
			return fmt.Errorf("`catchup_window:` is %s; the longest window is %s, because a window longer than "+
				"that turns a forgotten schedule into an unbounded burst of runs the moment a cluster returns",
				d, MaxScheduleCatchupWindow)
		}
	}

	for i, calendar := range trigger.GetCalendars() {
		if err := checkScheduleCalendar(i, calendar); err != nil {
			return err
		}
	}

	if zone := trigger.GetTimeZone(); zone != "" {
		if err := checkTimeZoneName(zone); err != nil {
			return err
		}
	}

	return nil
}

// The catch-up window's bounds, and what an unset one takes.
//
// Temporal leaves an unset window to the *server's* default, which is one year,
// so a schedule that says nothing about catch-up is a schedule that will, after a
// long enough outage, take a year of missed firings at once. That is the exact
// unbounded burst this surface exists to refuse, and "the field is optional" is not
// a bound. [DefaultScheduleCatchupWindow] is therefore applied by the server when
// the field is absent, which makes the maximum below a real ceiling rather than a
// ceiling on the values somebody bothered to write.
//
// An hour is chosen because it is the window in which recovering a missed firing is
// still recovery rather than archaeology: a firing an hour late is usually still
// wanted, and a schedule that wants more says so in one line and gets up to thirty
// days of it.
const (
	MinScheduleCatchupWindow     = time.Minute
	MaxScheduleCatchupWindow     = 30 * 24 * time.Hour
	DefaultScheduleCatchupWindow = time.Hour
)

// The bounds on an operator-requested historical replay.
//
// Two bounds rather than one, because they bound different resources and an
// attacker (or a tired operator) picks whichever the other one leaves open. The
// count bounds how many separate evaluations the cluster is asked for; the total
// span bounds how much *time* those evaluations cover. One 40-day range and forty
// 1-day ranges are the same window, and only checking both refuses both. The
// cadence-aware check below separately bounds how many executions that window can
// produce.
const (
	MaxScheduleBackfills       = 10
	MaxScheduleBackfillSpan    = 31 * 24 * time.Hour
	MaxScheduleBackfillFirings = 100000
)

// CheckScheduleBackfillForTrigger also bounds the work produced by the ranges.
// A span alone is not a work bound: at the minimum one-second cadence the span
// above contains millions of firing times. The estimate is deliberately an upper
// bound. Five- and six-field cron cannot fire more than once a minute; calendars
// and the seven-field form carrying seconds may fire once a second.
//
// Every cadence the trigger carries is charged its *own* firing count rather
// than the whole trigger being charged the fastest cadence once per entry.
// Those are not the same number and only one of them is the real ceiling: a
// trigger with `every: 1s` alongside a `@daily` cron fires 86,401 times a day,
// not twice the faster of the two. Charging the fastest one to all of them
// refuses schedules whose actual fan-out is nowhere near the limit, which is
// the other way a bound can be wrong.
//
// That sum is a safe upper bound in general — two cadences with independent
// phases can, worst case, share no firing instant at all, so nothing shy of
// evaluating the real fire times can prove less than their sum without risking
// an undercount, which is the direction this check must never be wrong in.
// The one case it *can* prove less than the sum without evaluating anything is
// an exact duplicate: two entries that are the identical cron expression, or
// the identical calendar, fire at exactly the same instants by construction,
// so charging the duplicate a second time double-counts one set of firings as
// if it were two. cronDedupeKey and calendarsEqual below charge each distinct
// cadence once; anything short of a literal duplicate is still summed, which
// stays the safe direction (#716).
func CheckScheduleBackfillForTrigger(trigger *ScheduleTrigger, backfills []*ScheduleBackfill) error {
	if err := CheckScheduleBackfill(backfills); err != nil {
		return err
	}
	if len(backfills) == 0 {
		return nil
	}

	var cadences []time.Duration
	if every := trigger.GetEvery(); every != nil {
		if period := every.AsDuration(); period > 0 {
			cadences = append(cadences, period)
		}
	}

	seenCron := make(map[string]bool, len(trigger.GetCron()))
	for _, expression := range trigger.GetCron() {
		key := cronDedupeKey(expression)
		if seenCron[key] {
			continue
		}
		seenCron[key] = true
		cadences = append(cadences, cronMinimumPeriod(expression, trigger.GetTimeZone()))
	}

	calendars := trigger.GetCalendars()
	for i, calendar := range calendars {
		duplicate := false
		for _, earlier := range calendars[:i] {
			if calendarsEqual(calendar, earlier) {
				duplicate = true
				break
			}
		}
		if duplicate {
			continue
		}
		cadences = append(cadences, calendarMinimumPeriod(calendar, trigger.GetTimeZone()))
	}

	if len(cadences) == 0 {
		return fmt.Errorf("a backfill needs a schedule cadence whose firing count can be bounded")
	}

	remaining := int64(MaxScheduleBackfillFirings)
	for i, b := range backfills {
		// The estimate bounds what the *cadence* can produce inside a window,
		// so the window has to be the one the cadence is actually allowed to
		// fire in. A requested range wider than the trigger's own start_at/
		// end_at cannot produce a single firing outside that window — Temporal
		// will not schedule one — so charging the full requested range here
		// counts firings that can never happen, over-estimating exactly the
		// schedules a narrow start_at/end_at was written to keep small.
		//
		// startInclusive is true when the trigger's own start_at, not the
		// requested range's own start, ends up as the effective left edge.
		// trigger.start_at bounds its window inclusively (its own doc
		// comment says so), unlike the backfill range's own start, which the
		// remainder handling below already treats as the exclusive edge of
		// (start, end] — so a firing landing exactly on that boundary needs
		// charging separately rather than falling out of the same arithmetic
		// that already covers the end being inclusive.
		span, startInclusive := intersectedBackfillSpan(trigger, b)

		// Jitter delays each firing by a random amount up to its own value,
		// capped by the gap to the *next* firing — the schema's own doc
		// comment says so. That cap is what keeps the padding below
		// per-cadence rather than a flat addition to the shared span: a
		// firing can only be delayed as far as the next one it would
		// otherwise collide with, so a cadence firing every second can
		// never be shifted more than a second regardless of how large
		// `jitter` itself is. Padding every cadence by the *uncapped*
		// jitter — as an earlier version of this fix did — refuses a fast
		// cadence over a jitter the schedule can never actually apply to
		// it: a one-day jitter on an every-second schedule cannot shift any
		// firing by more than the one second to its neighbor, so charging a
		// whole day of padding there is a false refusal wide enough to be
		// its own bug, not just imprecision.
		jitter := trigger.GetJitter().AsDuration()

		var firings int64
		for _, period := range cadences {
			// A nominal firing up to jitter before the window's start can be
			// delayed into it — a firing this estimate would otherwise miss
			// entirely, since it never happens at its own nominal instant.
			// One within jitter of the end can be delayed past it, but that
			// only removes a firing this window would have charged, never
			// adds one, so only the left edge needs padding. Capped at this
			// cadence's own period for the reason above.
			padded := span
			if cap := min(jitter, period); cap > 0 {
				padded += cap
			}

			quotient := int64(padded / period)
			switch {
			case startInclusive:
				// [start, end] is closed at both ends now, and the tight upper
				// bound on evenly spaced points in a closed interval of length
				// L, at an unknown phase, is floor(L/period)+1 — always,
				// regardless of whether L divides evenly: place one point
				// exactly at the closed start, then one every period until the
				// next would exceed L. That is exactly one more than the
				// half-open bound below when L is an exact multiple of period,
				// and identical to it otherwise — so this must never also
				// apply the remainder bump below it, or an inexact L (span
				// 99999s1ns at a 1s period, say) is charged 100,001 for a
				// range that can hold at most 100,000, an over-count that
				// refuses a legitimate backfill.
				firings += quotient + 1
			case padded%period != 0:
				// Temporal's interval is (start, end], which holds one more
				// evenly spaced firing than the floor of its length when the
				// range is aligned that way: an aligned 300001s range at a 3s
				// cadence holds 100,001 firings, and rounding down would have
				// called it 100,000 and let it past the ceiling. Round up, so
				// what is compared against the limit is a number the range
				// can never exceed.
				//
				// By quotient and remainder rather than by
				// (span+period-1)/period, which is the same arithmetic only
				// while the sum fits: `@every` is an accepted spelling for a
				// period near the top of time.Duration, and that sum
				// overflows to a negative firing count, subtracting from a
				// total another cadence was contributing honestly.
				firings += quotient + 1
			default:
				firings += quotient
			}
			if firings > remaining {
				return fmt.Errorf("the backfill can produce more than %d firings at this cadence; "+
					"range %d would exceed the bounded recovery limit", MaxScheduleBackfillFirings, i+1)
			}
		}
		remaining -= firings
	}
	return nil
}

// intersectedBackfillSpan reports how much of a requested backfill range falls
// inside the trigger's own start_at/end_at window, saturating rather than
// wrapping for the same reason [CheckScheduleBackfill] does: `Time.Sub` clamps
// at the maximum duration rather than overflowing, so a clamped span is
// refused elsewhere instead of being carried into arithmetic that could wrap.
//
// A trigger without a start_at or an end_at is unbounded on that side, so the
// requested range's own edge stands; both unset reduces to the requested
// range untouched, which is the behavior before this bound existed.
//
// The second return reports whether the trigger's own start_at, rather than
// the requested range's own start, ended up as the effective left edge — see
// this function's caller for why that boundary needs different treatment.
func intersectedBackfillSpan(trigger *ScheduleTrigger, b *ScheduleBackfill) (time.Duration, bool) {
	start, end := b.GetStartAt().AsTime(), b.GetEndAt().AsTime()
	startFromTrigger := false

	if ts := trigger.GetStartAt(); ts != nil {
		if t := ts.AsTime(); t.After(start) {
			start = t
			startFromTrigger = true
		}
	}
	if ts := trigger.GetEndAt(); ts != nil {
		if t := ts.AsTime(); t.Before(end) {
			end = t
		}
	}

	switch {
	case start.After(end):
		// The trigger's window and the requested range do not overlap at
		// all — nothing in this range can ever fire, so it contributes
		// nothing to charge against the limit rather than a negative span.
		return 0, false
	case start.Equal(end):
		// Not the same as "no overlap": if start came from the trigger's own
		// inclusive start_at, that single instant is both the trigger's
		// first possible firing and inside the requested range's inclusive
		// end, so it can itself be a real firing — one this function's
		// caller charges through startInclusive rather than through span,
		// since a zero-length span carries no duration for span/period to
		// divide. If start did not come from the trigger, this coincidence
		// is the requested range's own exclusive start meeting its own
		// inclusive end after end_at clipped down to it — genuinely empty,
		// since the request's own start was never a firing this recovery
		// asked for in the first place.
		return 0, startFromTrigger
	default:
		return end.Sub(start), startFromTrigger
	}
}

// cronDedupeKey normalizes a cron expression to the same form
// [cronMinimumPeriod] classifies it by, so two spellings that are the
// identical schedule — differing only in a trailing comment, in surrounding
// whitespace, or in how many spaces separate two fields — are recognized as
// the one duplicate they are, and anything short of that stays distinct.
//
// strings.Fields splits on any run of whitespace and drops empty results, so
// rejoining with single spaces collapses "*  * * * * * *" (a stray extra
// space) to the same key as "* * * * * * *" — both are the identical seven
// fields to the cluster, which the dedup key has to agree with or a naive
// byte comparison charges the same cadence twice.
func cronDedupeKey(expression string) string {
	return strings.Join(strings.Fields(stripCronComment(expression)), " ")
}

// calendarsEqual reports whether two calendar specifications fire at exactly
// the same instants — every field that decides *when* a calendar matches, not
// necessarily byte-for-byte identical messages. Comment is excluded on
// purpose: Temporal reads it as documentation, never as part of what a
// calendar matches, so two calendars whose ranges agree fire identically
// regardless of what their comments say, and comparing it would refuse a
// dedup two otherwise-identical calendars are entitled to. Cloned and cleared
// rather than compared field by field, so a schema field added to this
// message later is picked up here automatically instead of silently being
// left out of the comparison.
func calendarsEqual(a, b *ScheduleTrigger_Calendar) bool {
	ac, _ := proto.Clone(a).(*ScheduleTrigger_Calendar)
	bc, _ := proto.Clone(b).(*ScheduleTrigger_Calendar)
	ac.Comment, bc.Comment = "", ""
	return proto.Equal(ac, bc)
}

// stripCronComment removes a trailing comment, and only a trailing one.
//
// `#` is two things in a cron expression. Starting a whitespace-separated word
// it begins a comment; inside a day-of-week field it is the nth-weekday-of-month
// operator, which this repository accepts and tests (`0 9 * * 5#3`, the third
// Friday). Cutting at the first `#` wherever it appears conflates them, and the
// conflation loses fields: `* * * * * 5#3 2030` becomes six fields rather than
// seven, so a seconds-resolution expression is charged one firing a *minute* —
// an under-estimate, which is the direction that lets a backfill past the
// ceiling. A 29-day range over two such Fridays estimates about 41,760 firings
// against a possible 172,800.
func stripCronComment(expression string) string {
	for i := range len(expression) {
		if expression[i] != '#' {
			continue
		}
		if i == 0 || expression[i-1] == ' ' || expression[i-1] == '\t' {
			return expression[:i]
		}
	}
	return expression
}

// isUTC reports whether zone names no offset at all: the empty string
// (unset, which [checkTimeZoneName] documents as meaning UTC) or a spelling
// of UTC itself. Every other IANA name is a zone [wallClockCadencePeriod]
// cannot inspect and therefore cannot bound.
func isUTC(zone string) bool {
	return zone == "" || strings.EqualFold(zone, "UTC") || strings.EqualFold(zone, "Etc/UTC")
}

// wallClockCadencePeriod is the safe minimum period for a cadence that
// nominally fires once every `nominal` wall-clock duration, in the
// schedule's own `time_zone`. Applies to every resolution coarser than a
// second — minute, hour, and whole days or multiples of them — since only
// second resolution is already the finest granularity this package assumes
// and cannot be undercut by any offset change.
//
// UTC never observes an offset change — there is no daylight-saving or
// political rule to apply to it — so a UTC cadence really is bound below by
// its nominal length: two once-an-hour firings cannot be less than 60 real
// minutes apart, ever, and the same holds at every coarser resolution. Any
// other named zone is a different problem entirely. This package never
// resolves `time_zone` against real tzdata (see [checkTimeZoneName]'s doc,
// and the package doc's rule about keeping I/O off an author's keystroke
// path — the same reasoning that keeps this whole file estimating rather
// than evaluating), so nothing here can rule out an offset change of any
// particular size for whichever zone a schedule names, at any resolution
// coarser than a second. The familiar spring-forward case shifts by an hour;
// `America/Caracas` shifted by thirty minutes in 2016, which would undercut
// an *hourly* calendar's assumed 60-minute floor exactly the way a day-scale
// shift undercuts a daily one; and IANA's database records changes as large
// as a whole day (Samoa's 2011 shift). A named zone is therefore charged the
// fastest cadence there is, at every resolution above a second — the
// identical fail-closed default an unrecognized cron form gets in
// [cronMinimumPeriod]. That gives up the ergonomic win this estimate exists
// for on exactly the schedules that ask for local wall-clock behavior — a
// real cost, stated here rather than left implicit — but the alternative is
// a bound this package cannot prove, and an unproven bound is the exact
// shape of undercount #716 exists to close.
func wallClockCadencePeriod(zone string, nominal time.Duration) time.Duration {
	if isUTC(zone) {
		return nominal
	}
	return time.Second
}

// calendarMinimumPeriod reports the shortest interval a calendar can fire at,
// read from which of its sub-day fields are populated.
//
// Temporal defaults an unwritten second, minute or hour to *zero* rather than to
// "every" — which is the whole reason a calendar of `hour: 9` means 09:00:00
// daily and not 3,600 firings between nine and ten. Charging every calendar one
// firing a second ignores that: it estimates a two-day backfill of that calendar
// at 172,800 firings and refuses it, for a schedule that produces two.
//
// So the ladder walks down from the finest field written. Anything at or below
// the finest one is defaulted to zero and cannot multiply the count; anything
// above it is free to be "every", which is already accounted for by the period
// being no longer than that field's own unit. Every resolution coarser than a
// second goes through [wallClockCadencePeriod], because every one of them
// assumes a wall-clock gap bounded below by something — an assumption a named
// zone's offset change can undercut regardless of which unit it is.
func calendarMinimumPeriod(calendar *ScheduleTrigger_Calendar, timeZone string) time.Duration {
	switch {
	case len(calendar.GetSecond()) > 0:
		return time.Second
	case len(calendar.GetMinute()) > 0:
		return wallClockCadencePeriod(timeZone, time.Minute)
	case len(calendar.GetHour()) > 0:
		return wallClockCadencePeriod(timeZone, time.Hour)
	default:
		// Second, minute and hour all default to zero, so the calendar fires at
		// most once on any day it matches at all.
		return wallClockCadencePeriod(timeZone, 24*time.Hour)
	}
}

// cronMinimumPeriod reports the shortest interval an expression accepted by
// [CheckCronExpression] can fire at, for use as the denominator of a firing
// count that must never come out too low.
//
// It reads the grammar that function reads, in the same order — comment, zone
// prefix, shorthand, field count — because classifying the raw string instead
// gets every accepted form but the bare five-field one wrong. `@daily` and
// `CRON_TZ=UTC 0 9 * * *` are not five whitespace-separated fields, and calling
// them one-second cadences estimates a two-day backfill of `@daily` at 172,800
// firings and refuses it, when it produces two.
//
// triggerTimeZone is the schedule's own `time_zone`, read when the expression
// carries no `CRON_TZ=`/`TZ=` prefix of its own — an expression's own prefix
// overrides the schedule's zone for that one entry, the same precedence
// Temporal itself gives it, so a day-or-longer shorthand's DST exposure is
// decided by whichever zone actually governs it.
//
// Anything it cannot classify is charged the fastest cadence there is. That is
// the fail-closed direction here: an over-estimate refuses a backfill somebody
// then writes more narrowly, where an under-estimate is the fan-out this whole
// check exists to stop.
func cronMinimumPeriod(expression string, triggerTimeZone string) time.Duration {
	expression = strings.TrimSpace(stripCronComment(expression))

	zoneName := triggerTimeZone
	if zone, rest, found := strings.Cut(expression, " "); found {
		if after, ok := strings.CutPrefix(zone, "CRON_TZ="); ok {
			zoneName = after
			expression = strings.TrimSpace(rest)
		} else if after, ok := strings.CutPrefix(zone, "TZ="); ok {
			zoneName = after
			expression = strings.TrimSpace(rest)
		}
	}

	if strings.HasPrefix(expression, "@") {
		head, rest, _ := strings.Cut(expression, " ")
		if strings.EqualFold(head, "@every") {
			// `@every` carries its own interval, and the same reading
			// CheckCronExpression leaves to the cluster is the one charged
			// here; an interval it cannot read is charged a second.
			if period, err := time.ParseDuration(strings.TrimSpace(rest)); err == nil && period > 0 {
				return period
			}
			return time.Second
		}
		// The fixed shorthands, each charged the shortest period it can mean:
		// a month is charged 28 days and a year 365, because a lower bound on
		// the period is an upper bound on the firings. Every shorthand here
		// goes through [wallClockCadencePeriod] for the same reason
		// [calendarMinimumPeriod]'s every-resolution-but-second case does:
		// each fires on a wall-clock boundary, whose minimum gap this
		// package can bound only in UTC, regardless of whether the boundary
		// is an hour or a year.
		switch strings.ToLower(head) {
		case "@hourly":
			return wallClockCadencePeriod(zoneName, time.Hour)
		case "@daily", "@midnight":
			return wallClockCadencePeriod(zoneName, 24*time.Hour)
		case "@weekly":
			return wallClockCadencePeriod(zoneName, 7*24*time.Hour)
		case "@monthly":
			return wallClockCadencePeriod(zoneName, 28*24*time.Hour)
		case "@yearly", "@annually":
			return wallClockCadencePeriod(zoneName, 365*24*time.Hour)
		default:
			return time.Second
		}
	}

	switch len(strings.Fields(expression)) {
	case 5, 6:
		// Minute-resolution: the five ordinary fields, and the same five with a
		// year appended. Routed through [wallClockCadencePeriod] for the same
		// reason the shorthands above are: an ordinary cron entry fires on a
		// wall-clock minute boundary, whose minimum gap this package can
		// bound only in UTC. A named zone can shift by more than a minute at
		// an offset change, so a sub-minute rollback can repeat a matching
		// local minute in under sixty seconds.
		return wallClockCadencePeriod(zoneName, time.Minute)
	default:
		// Seven fields put seconds first. Anything else is not an expression
		// CheckCronExpression accepts, and is charged the fastest cadence.
		return time.Second
	}
}

// CheckScheduleBackfill bounds an operator-requested historical replay before
// it can turn a short outage into an unbounded burst of executions.
//
// The budget is spent down rather than accumulated, and that is not a style
// preference. `time.Duration` is a signed 64-bit nanosecond count, and RFC3339
// admits years far enough apart that `Time.Sub` saturates at its maximum. Two
// such ranges summed wrap *negative*, and a total below zero is a total below 31
// days. Subtracting each span from what is left instead can never wrap, because a
// span is only ever subtracted after it has been shown to fit.
func CheckScheduleBackfill(backfills []*ScheduleBackfill) error {
	if len(backfills) > MaxScheduleBackfills {
		return fmt.Errorf("the backfill asks for %d ranges; at most %d are accepted, because a backfill is a "+
			"bounded recovery of a window somebody can name rather than a replay of history",
			len(backfills), MaxScheduleBackfills)
	}

	remaining := MaxScheduleBackfillSpan
	for i, b := range backfills {
		if b.GetStartAt() == nil || b.GetEndAt() == nil {
			return fmt.Errorf("backfill range %d needs both a start and an end, written as START..END with "+
				"RFC3339 timestamps", i+1)
		}

		start, end := b.GetStartAt().AsTime(), b.GetEndAt().AsTime()
		if !start.Before(end) {
			return fmt.Errorf("backfill range %d starts at %s and ends at %s; the start must come first",
				i+1, start.UTC().Format(time.RFC3339), end.UTC().Format(time.RFC3339))
		}

		// Saturating rather than wrapping: `Sub` clamps at the maximum duration, and
		// a clamped span is astronomically larger than what remains, so it is refused
		// here instead of being carried into an accumulator that could wrap.
		span := end.Sub(start)
		if span > remaining {
			return fmt.Errorf("the backfill covers more than %s of history in total; range %d alone would take "+
				"it past that. Recover the window that was missed, and leave the rest",
				MaxScheduleBackfillSpan, i+1)
		}
		remaining -= span
	}

	return nil
}

// scheduleCalendarField names one field of a calendar and the values Temporal
// allows in it.
//
// Written once here because three surfaces need the same numbers: the Flowfile
// parser, which reports an out-of-range value against the line it is on; this
// checker, which is what the server and `flow schedule create` ask; and the prose
// in docs/DSL.md. The bounds are Temporal's own (see client.ScheduleCalendarSpec):
// a value outside one is refused by every cluster in the world, which is exactly
// the line docs/DSL.md draws for what a validator may report.
type scheduleCalendarField struct {
	name string
	min  int32
	max  int32
}

// scheduleCalendarFields are the calendar's fields, in the order one is written.
var scheduleCalendarFields = []scheduleCalendarField{
	{name: "second", min: 0, max: 59},
	{name: "minute", min: 0, max: 59},
	{name: "hour", min: 0, max: 23},
	{name: "day_of_month", min: 1, max: 31},
	{name: "month", min: 1, max: 12},
	{name: "year", min: int32(cronYear.min), max: int32(cronYear.max)},
	{name: "day_of_week", min: 0, max: 6},
}

// ScheduleCalendarFieldNames are the keys a calendar may hold, in the order a
// calendar is written, for a parser that needs the same list.
func ScheduleCalendarFieldNames() []string {
	names := make([]string, 0, len(scheduleCalendarFields))
	for _, field := range scheduleCalendarFields {
		names = append(names, field.name)
	}

	return names
}

// ScheduleCalendarFieldBounds returns the values a named calendar field allows,
// so the Flowfile parser bounds each field with the numbers this checker uses
// rather than a second copy of them.
func ScheduleCalendarFieldBounds(name string) (low, high int32, known bool) {
	for _, field := range scheduleCalendarFields {
		if field.name == name {
			return field.min, field.max, true
		}
	}

	return 0, 0, false
}

// scheduleCalendarRanges returns a calendar's ranges in the field order above,
// so a walk over a calendar cannot forget a field the schema has.
func scheduleCalendarRanges(calendar *ScheduleTrigger_Calendar) [][]*ScheduleTrigger_Calendar_Range {
	return [][]*ScheduleTrigger_Calendar_Range{
		calendar.GetSecond(), calendar.GetMinute(), calendar.GetHour(), calendar.GetDayOfMonth(),
		calendar.GetMonth(), calendar.GetYear(), calendar.GetDayOfWeek(),
	}
}

// checkScheduleCalendar refuses a calendar that cannot mean what it says.
//
// An empty calendar is refused rather than defaulted, and it is the case worth
// naming: Temporal reads a calendar with no fields as 00:00:00 every day, so
// `calendars: [{}]` is a schedule that fires daily at midnight, written by
// somebody who meant something else and told nothing. It is the same silent
// success [CheckScheduleTrigger] refuses for a schedule with no cadence at all.
func checkScheduleCalendar(index int, calendar *ScheduleTrigger_Calendar) error {
	ranges := scheduleCalendarRanges(calendar)

	empty := true
	for _, field := range ranges {
		if len(field) > 0 {
			empty = false
			break
		}
	}
	if empty {
		return fmt.Errorf("calendar %d says nothing about when to fire; a calendar with no fields means "+
			"00:00:00 every day, which is a cadence nobody wrote. Name at least one of %s",
			index+1, strings.Join(ScheduleCalendarFieldNames(), ", "))
	}

	for i, field := range scheduleCalendarFields {
		for _, r := range ranges[i] {
			if err := checkScheduleCalendarRange(index, field, r); err != nil {
				return err
			}
		}
	}

	return nil
}

// checkScheduleCalendarRange checks one range of one calendar field.
//
// An `end` of zero is the schema's "no end written", which Temporal reads as the
// start alone, so it is accepted whatever the start is, and every other value has
// to be inside the field's range and at or after the start. A descending range is
// refused rather than passed on, because Temporal silently narrows one to its start
// and the author of `hour: {start: 17, end: 9}` did not ask for 17:00 only.
func checkScheduleCalendarRange(index int, field scheduleCalendarField, r *ScheduleTrigger_Calendar_Range) error {
	if r == nil {
		return fmt.Errorf("calendar %d has an empty %s range; write a whole number, or a mapping of "+
			"`start:`, `end:` and `step:`", index+1, field.name)
	}

	if r.GetStart() < field.min || r.GetStart() > field.max {
		return fmt.Errorf("calendar %d has %s starting at %d, which is outside %d-%d",
			index+1, field.name, r.GetStart(), field.min, field.max)
	}

	if end := r.GetEnd(); end != 0 {
		if end < field.min || end > field.max {
			return fmt.Errorf("calendar %d has %s ending at %d, which is outside %d-%d",
				index+1, field.name, end, field.min, field.max)
		}
		if end < r.GetStart() {
			return fmt.Errorf("calendar %d has %s running from %d down to %d; a range is written low to high, "+
				"and a descending one matches its start alone", index+1, field.name, r.GetStart(), end)
		}
	}

	if step := r.GetStep(); step < 0 {
		return fmt.Errorf("calendar %d has a %s step of %d; a step counts forward, and leaving it out "+
			"takes every value in the range", index+1, field.name, step)
	}

	return nil
}

// CheckCronExpression reports whether a cron expression is one Temporal could read.
//
// Structural only, and generous by design. Temporal accepts five, six or seven
// fields, the `@daily` family of shorthands, `@every <interval>`, an optional
// `CRON_TZ=`/`TZ=` prefix and a trailing `#` comment — so this refuses a wrong
// *number* of fields, a shorthand that does not exist, and a value outside the range
// its position allows, and accepts everything else. The cluster remains the
// authority; this exists so that the mistakes made by hand are reported where the
// hand is.
func CheckCronExpression(expression string) error {
	original := expression

	// A comment is Temporal's, not YAML's: the `#` is inside the quoted string, so
	// nothing has stripped it by the time it reaches here.
	if comment := strings.Index(expression, "#"); comment >= 0 {
		expression = expression[:comment]
	}

	expression = strings.TrimSpace(expression)
	if expression == "" {
		return fmt.Errorf("cron expression %q says nothing about when to fire; write five fields, "+
			"as in `0 9 * * MON-FRI` for 09:00 on weekdays", original)
	}

	// The zone prefix, which is a whole field of its own and would otherwise be
	// counted as a minute.
	if zone, rest, found := strings.Cut(expression, " "); found {
		if name, isZone := strings.CutPrefix(zone, "CRON_TZ="); isZone {
			if err := checkTimeZoneName(name); err != nil {
				return fmt.Errorf("cron expression %q: %w", original, err)
			}
			expression = strings.TrimSpace(rest)
		} else if name, isZone := strings.CutPrefix(zone, "TZ="); isZone {
			if err := checkTimeZoneName(name); err != nil {
				return fmt.Errorf("cron expression %q: %w", original, err)
			}
			expression = strings.TrimSpace(rest)
		}
	}

	if strings.HasPrefix(expression, "@") {
		return checkCronShorthand(original, expression)
	}

	fields := strings.Fields(expression)
	switch len(fields) {
	case 5:
		// Minute, hour, day of month, month, day of week.
		return checkCronFields(original, fields, cronFieldsFrom(1))
	case 6:
		// The five above, plus a year.
		return checkCronFields(original, fields, append(cronFieldsFrom(1), cronYear))
	case 7:
		// Seconds first, then the six above.
		return checkCronFields(original, fields, append(cronFieldsFrom(0), cronYear))
	default:
		return fmt.Errorf("cron expression %q has %d fields; a cron expression has 5 "+
			"(minute hour day-of-month month day-of-week), 6 with a year, or 7 with seconds first — "+
			"or one of the shorthands @hourly, @daily, @weekly, @monthly, @yearly and @every",
			original, len(fields))
	}
}

// cronField is one position in a cron expression: what it is called and what
// numbers and names belong in it.
type cronField struct {
	name  string
	min   int
	max   int
	names []string

	// symbols says this position has cron syntax beyond numbers and names — `L`,
	// `W`, `15#3`, `?` — which this checker deliberately does not model.
	//
	// Only the two day fields have any, and the distinction earns its place: in a
	// field that has none, a token of letters cannot be right, so `abc 9 * * *` is
	// refused. Without it that expression passes, because "not a number and not a
	// name" was being read as "syntax I do not model" everywhere rather than only
	// where such syntax exists.
	symbols bool
}

var (
	cronSecond     = cronField{name: "seconds", min: 0, max: 59}
	cronMinute     = cronField{name: "minutes", min: 0, max: 59}
	cronHour       = cronField{name: "hours", min: 0, max: 23}
	cronDayOfMonth = cronField{name: "day of month", min: 1, max: 31, symbols: true}
	cronMonth      = cronField{name: "month", min: 1, max: 12, names: []string{
		"JAN", "FEB", "MAR", "APR", "MAY", "JUN", "JUL", "AUG", "SEP", "OCT", "NOV", "DEC",
	}}
	cronDayOfWeek = cronField{name: "day of week", min: 0, max: 7, symbols: true, names: []string{
		"SUN", "MON", "TUE", "WED", "THU", "FRI", "SAT",
	}}

	// A year is bounded loosely on purpose: what a cluster will accept here is its
	// business, and the only mistake worth catching is a value that is plainly not
	// a year.
	cronYear = cronField{name: "year", min: 1970, max: 3000}
)

// cronFieldsFrom returns the standard field order, starting at seconds (0) or at
// minutes (1).
//
// Derived rather than written out twice, because the five-field and seven-field
// forms are the same list with a different beginning — and the bug this shape
// prevents is the one where a seven-field expression is checked against the
// five-field meanings, reporting a correct hour as an impossible minute.
func cronFieldsFrom(start int) []cronField {
	all := []cronField{cronSecond, cronMinute, cronHour, cronDayOfMonth, cronMonth, cronDayOfWeek}

	return append([]cronField(nil), all[start:]...)
}

// cronShorthands are the `@` forms, mapped to nothing: what matters is only that a
// shorthand is one that exists, since a misspelled `@dialy` is otherwise accepted by
// this function and rejected by the cluster at create.
var cronShorthands = []string{
	"@yearly", "@annually", "@monthly", "@weekly", "@daily", "@midnight", "@hourly",
}

// checkCronShorthand reads the `@` forms, including `@every`.
func checkCronShorthand(original, expression string) error {
	head, rest, hasRest := strings.Cut(expression, " ")

	if strings.EqualFold(head, "@every") {
		if !hasRest || strings.TrimSpace(rest) == "" {
			return fmt.Errorf("cron expression %q is `@every` with no interval; write one, "+
				"as in `@every 15m` — or use the schedule's own `every:` key, which says the same thing "+
				"in the DSL's own notation", original)
		}

		return nil
	}

	for _, shorthand := range cronShorthands {
		if strings.EqualFold(head, shorthand) && !hasRest {
			return nil
		}
	}

	return fmt.Errorf("cron expression %q is not a shorthand this understands; the shorthands are "+
		"%s, and `@every <interval>`", original, strings.Join(cronShorthands, ", "))
}

// checkCronFields checks each field against the range its position allows.
func checkCronFields(original string, fields []string, positions []cronField) error {
	for i, field := range fields {
		if err := checkCronFieldValue(field, positions[i]); err != nil {
			return fmt.Errorf("cron expression %q: %w", original, err)
		}
	}

	return nil
}

// checkCronFieldValue checks one field of one expression.
//
// It walks the comma-separated list, and within each element handles the `/` step
// and the `-` range, refusing a number outside the field's range and a name the
// field does not have. Anything it does not recognize — `L`, `W`, `#`, a `?` — is
// left alone, because those are real cron syntax somewhere and refusing one would be
// this function inventing a restriction the cluster does not have.
func checkCronFieldValue(field string, position cronField) error {
	if field == "" {
		return fmt.Errorf("has an empty %s field", position.name)
	}

	for _, element := range strings.Split(field, ",") {
		// A step applies to whatever precedes it; the step size itself is a count
		// rather than a value in the field's range, so only the left half is checked
		// against the range.
		value, _, _ := strings.Cut(element, "/")

		for _, bound := range strings.Split(value, "-") {
			if err := checkCronAtom(bound, position); err != nil {
				return err
			}
		}
	}

	return nil
}

// checkCronAtom checks one number or name.
func checkCronAtom(atom string, position cronField) error {
	atom = strings.TrimSpace(atom)

	// `*` and the empty half of `*/5` say "every", and `?` says "no opinion" in the
	// day fields. Neither is a value.
	if atom == "" || atom == "*" || atom == "?" {
		return nil
	}

	if number, err := strconv.Atoi(atom); err == nil {
		if number < position.min || number > position.max {
			return fmt.Errorf("%s is %d, which is outside %d-%d", position.name, number, position.min, position.max)
		}

		return nil
	}

	upper := strings.ToUpper(atom)
	for _, name := range position.names {
		if upper == name {
			return nil
		}
	}

	// Only a purely alphabetic token is judged. Anything carrying a character this
	// does not understand passes through untouched, because refusing it would be a
	// diagnostic about a restriction Flowstate does not have.
	for _, r := range upper {
		if r < 'A' || r > 'Z' {
			return nil
		}
	}

	// A field with symbols may legitimately hold letters this does not model — `L`
	// for the last day of the month, `W` for the nearest weekday — so a word there
	// is left alone. A field with neither names nor symbols holds numbers and
	// nothing else, and a word in one cannot be right anywhere.
	if position.symbols && len(position.names) == 0 {
		return nil
	}
	if len(position.names) == 0 {
		return fmt.Errorf("%s is %q, and %s is written as a number", position.name, atom, position.name)
	}

	return fmt.Errorf("%s is %q, which is not one of %s", position.name, atom, strings.Join(position.names, ", "))
}

// checkTimeZoneName reports whether a string is shaped like an IANA zone name.
//
// Shape only. Which zones exist is the cluster's answer — it loads them from the
// environment it runs in — and a validator running in an editor that refused a zone
// its own machine had never heard of would be reporting a fact about the wrong
// computer. What is caught here is the mistake that is wrong everywhere: a space, a
// quote, an empty segment.
func checkTimeZoneName(zone string) error {
	if zone == "" {
		return fmt.Errorf("time zone is empty; leave the key out for UTC, or name a zone such as America/New_York")
	}

	for _, r := range zone {
		switch {
		case r >= 'A' && r <= 'Z', r >= 'a' && r <= 'z', r >= '0' && r <= '9':
		case r == '/', r == '_', r == '-', r == '+':
		default:
			return fmt.Errorf("time zone %q contains %q; a zone is named the way the IANA database names one, "+
				"such as UTC or America/New_York", zone, string(r))
		}
	}

	return nil
}

// OverlapName is how an overlap policy is written in a Flowfile, which is how a
// message about one should name it.
//
// Derived from the enum by stripping the prefix and lowering, the same way
// [DeclaredTypeName] is, so an arm added to the schema is spelled here without
// anybody editing a list — and refusing to invent a name for the unspecified value,
// which is not something an author writes.
func OverlapName(overlap ScheduleTrigger_Overlap) string {
	if overlap == ScheduleTrigger_OVERLAP_UNSPECIFIED {
		return "unset"
	}

	return strings.ToLower(strings.TrimPrefix(overlap.String(), "OVERLAP_"))
}

// OverlapNames returns every overlap policy a schedule may name, in the order the
// schema declares them, for a diagnostic offering the alternatives.
func OverlapNames() []string {
	values := ScheduleTrigger_Overlap(0).Descriptor().Values()

	names := make([]string, 0, values.Len())
	for i := range values.Len() {
		overlap := ScheduleTrigger_Overlap(values.Get(i).Number())
		if overlap == ScheduleTrigger_OVERLAP_UNSPECIFIED {
			continue
		}
		names = append(names, OverlapName(overlap))
	}

	return names
}

// ParseOverlap reads an overlap policy as a Flowfile spells it.
func ParseOverlap(name string) (ScheduleTrigger_Overlap, bool) {
	value, ok := ScheduleTrigger_Overlap_value["OVERLAP_"+strings.ToUpper(name)]
	if !ok || ScheduleTrigger_Overlap(value) == ScheduleTrigger_OVERLAP_UNSPECIFIED {
		return ScheduleTrigger_OVERLAP_UNSPECIFIED, false
	}

	return ScheduleTrigger_Overlap(value), true
}

// ScheduleNameFor returns what a schedule is called when the caller named it, or the
// workflow's own name when they did not.
//
// One function because the default is a rule and not a convenience: the CLI reports
// the name it is about to create, the server creates it, and a name computed twice
// is a name that can differ between what somebody was told and what exists.
func ScheduleNameFor(requested string, wf *Workflow) string {
	if requested != "" {
		return requested
	}

	return wf.GetName()
}
