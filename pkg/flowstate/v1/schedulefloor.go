package flowstatev1

import (
	"fmt"
	"slices"
	"time"
)

// The cadence floor, and how far this package can see towards it.
//
// Nothing in Flowstate computes a firing time. A cron expression, an interval
// and a calendar are handed to Temporal as written, and the cluster is the
// only party that evaluates them — the projection rule ScheduleTrigger's schema
// comment states, and one this file keeps. So the floor cannot be checked by
// listing the next few firings and measuring the gaps; it is checked by reading
// the one part of a cadence that decides whether it can fire twice inside a
// minute, which is its *seconds*. A five-field expression fires on minute
// boundaries and cannot; a seven-field one fires at the seconds its first field
// names, and two of those are as close as the nearest pair; an interval fires
// every so many nanoseconds and says so; a calendar's `second:` is the same set
// a seven-field expression writes as its first field. A cron expression is read
// by [readCron], the one walk the backfill estimate reads it by too, so the two
// cannot classify one expression two ways.
//
// That covers each cadence alone. A trigger's cadences are *unioned* — a firing
// happens whenever any of them matches — so two cadences that are each slower
// than the floor can, together, be faster: `0 9 * * *` beside `30 0 9 * * * *`
// fires at 09:00:00 and again at 09:00:30. Whether two cadences ever land in
// the same minute is exactly the question this package cannot answer without
// evaluating them, so it is not asked. What is asked is whether they *could*:
// two cadences firing on different seconds of the minute are charged the gap
// between those seconds, and two firing on the same second are known to be a
// whole number of minutes apart. That refuses `every: 90s` beside a daily
// expression — the interval fires on :00 and :30 alternately, and the
// expression on :00, so a 30-second gap is possible — where a real evaluation
// would find the two never meet. The bias here is the opposite of the file
// header's, deliberately: an author refused for a union that was in fact slow
// enough is told which two cadences to align and is one edit from done, where
// a tenant admitted with a union that fires every thirty seconds has arranged
// a burst nobody is present to notice.

// The classifier reads the seconds of a cadence and nothing coarser: a
// five-field expression is known to fire at most once a minute and is never
// asked which minutes. That answers a floor of a minute exactly and answers a
// longer one wrongly — `0 9 * * *` would be charged a minute and refused by a
// five-minute floor — so the floor is pinned at or under a minute here rather
// than left to a constant somebody raises without reading this.
const _ = uint(time.Minute - MinScheduleInterval)

// cadenceSource is one of a trigger's cadences, reduced to what decides its
// shortest gap: how close two of its own firings can be, and which seconds of
// the minute it can fire on at all.
type cadenceSource struct {
	// key is the schedule key the cadence was written under, for the refusal.
	key string

	// label names the cadence the way a diagnostic should.
	label string

	// gap is a lower bound on the time between two firings of this cadence
	// alone. A cadence that fires on minute boundaries is charged a minute,
	// which is as far as this file looks.
	gap time.Duration

	// offsets are the seconds of the minute this cadence fires on, sorted and
	// distinct, when they are a set this file can enumerate.
	offsets []time.Duration

	// lattice, when offsets is empty, says the cadence fires on every point
	// shift + k×lattice of the minute: an interval that does not divide into
	// minutes reaches every one of them eventually, and an `@every` phase
	// moves them all by the same amount.
	lattice time.Duration
	shift   time.Duration
}

// checkScheduleCadenceFloor refuses a trigger that can fire more often than
// [MinScheduleInterval] allows: each cadence on its own, then every pair.
//
// Every refusal is a [ScheduleCadenceError] naming the key it is about, so a
// block carrying both `every:` and `cron:` is refused for its interval on the
// interval's line. A pair is refused on the later of the two keys.
func checkScheduleCadenceFloor(trigger *ScheduleTrigger) error {
	var sources []cadenceSource

	if every := trigger.GetEvery(); every != nil {
		d := every.AsDuration()
		if d < MinScheduleInterval {
			return &ScheduleCadenceError{Key: "every", Err: fmt.Errorf(
				"`every:` is %s; the shortest cadence a schedule may fire at is %s, so write `every: %s` or longer",
				d, MinScheduleInterval, MinScheduleInterval)}
		}
		sources = append(sources, intervalCadence("every", "`every:`", d, 0))
	}

	for _, expression := range trigger.GetCron() {
		source, err := cronCadenceSource(expression)
		if err != nil {
			return &ScheduleCadenceError{Key: "cron", Err: err}
		}
		if source.gap < MinScheduleInterval {
			return &ScheduleCadenceError{Key: "cron", Err: fmt.Errorf(
				"%s can fire every %s; the shortest cadence a schedule may fire at is %s",
				source.label, source.gap, MinScheduleInterval)}
		}
		sources = append(sources, source)
	}

	for i, calendar := range trigger.GetCalendars() {
		source := calendarCadence(i, calendar)
		if source.gap < MinScheduleInterval {
			return &ScheduleCadenceError{Key: "calendars", Err: fmt.Errorf(
				"%s can fire every %s; the shortest cadence a schedule may fire at is %s",
				source.label, source.gap, MinScheduleInterval)}
		}
		sources = append(sources, source)
	}

	for i := 1; i < len(sources); i++ {
		for j := range i {
			if gap := crossGap(sources[j], sources[i]); gap < MinScheduleInterval {
				return &ScheduleCadenceError{Key: sources[i].key, Err: fmt.Errorf(
					"%s and %s together can fire %s apart; the shortest cadence a schedule may fire at is %s. "+
						"Fire both on the same second of the minute, or write the union as one cadence",
					sources[j].label, sources[i].label, gap, MinScheduleInterval)}
			}
		}
	}

	return nil
}

// intervalCadence classifies a cadence that fires every d, measured from the
// epoch and shifted by phase: on one second of the minute when d is a whole
// number of minutes, and otherwise on every multiple of whatever d and a
// minute have in common, shifted by the phase.
func intervalCadence(key, label string, d, phase time.Duration) cadenceSource {
	source := cadenceSource{key: key, label: label, gap: d}
	if g := gcdDuration(d, time.Minute); g == time.Minute {
		source.offsets = []time.Duration{phase % time.Minute}
	} else {
		source.lattice = g
		source.shift = phase % g
	}

	return source
}

// minuteAligned is a cadence that fires on minute boundaries and no faster —
// every cron form without a seconds field, and a calendar that writes none.
func minuteAligned(key, label string) cadenceSource {
	return cadenceSource{key: key, label: label, gap: time.Minute, offsets: []time.Duration{0}}
}

// secondsCadence classifies a cadence by the seconds of the minute it names:
// one second is once a minute at most, and several are as close as the nearest
// pair, the wrap from the last to the first included.
func secondsCadence(key, label string, seconds []int) cadenceSource {
	offsets := make([]time.Duration, 0, len(seconds))
	for _, s := range seconds {
		offsets = append(offsets, time.Duration(s)*time.Second)
	}
	slices.Sort(offsets)
	offsets = slices.Compact(offsets)

	return cadenceSource{key: key, label: label, gap: secondsGap(seconds), offsets: offsets}
}

// cronCadenceSource classifies one expression [CheckCronExpression] has
// accepted, through the same [readCron] the backfill estimate reads it by.
//
// The errors are what that reading cannot answer: an `@every` interval or
// phase it cannot read, and a seconds field written in syntax it does not
// expand. The backfill estimate charges those a second and moves on, because a
// backfill over-refused is rewritten more narrowly; a floor that charged them a
// second would refuse with a sentence about a gap nobody wrote, so it says
// what it could not read instead.
func cronCadenceSource(expression string) (cadenceSource, error) {
	label := fmt.Sprintf("cron expression %q", expression)

	reading, err := readCron(expression, "")
	switch {
	case err != nil:
		return cadenceSource{}, fmt.Errorf("%w, so it cannot be held to the shortest cadence of %s",
			err, MinScheduleInterval)
	case reading.interval > 0:
		return intervalCadence("cron", label, reading.interval, reading.phase), nil
	case reading.secondsUnmodelled:
		return cadenceSource{}, fmt.Errorf("%s has a seconds field this cannot read, so it cannot be held to the "+
			"shortest cadence of %s; write the seconds as a number, a list, a range or a step", label, MinScheduleInterval)
	case reading.seconds != nil:
		return secondsCadence("cron", label, reading.seconds), nil
	default:
		return minuteAligned("cron", label), nil
	}
}

// calendarCadence classifies a calendar by its `second:` field, which defaults
// to zero when unwritten — see [calendarMinimumPeriod] for why that default is
// the whole reason a calendar of `hour: 9` fires once and not 3,600 times.
func calendarCadence(index int, calendar *ScheduleTrigger_Calendar) cadenceSource {
	label := fmt.Sprintf("calendar %d", index+1)

	ranges := calendar.GetSecond()
	if len(ranges) == 0 {
		return minuteAligned("calendars", label)
	}

	var seconds []int
	for _, r := range ranges {
		start, end, step := int(r.GetStart()), int(r.GetEnd()), int(r.GetStep())
		if end == 0 {
			end = start
		}
		if step <= 0 {
			step = 1
		}
		// [checkScheduleCalendar] has already refused a value outside the
		// field's range and a range that runs backwards; the clamp is so this
		// loop is bounded by the field rather than by that ordering.
		for v := max(start, 0); v <= min(end, 59); v += step {
			seconds = append(seconds, v)
		}
	}

	return secondsCadence("calendars", label, seconds)
}

// crossGap is a lower bound on how close a firing of one cadence can come to
// a firing of another, read from the seconds of the minute each fires on.
//
// Two cadences on the same second of the minute are a whole number of minutes
// apart or coincident, and a coincident firing is one firing. Two on different
// seconds are charged the distance between those seconds, because nothing
// here can rule out their landing in the same minute.
func crossGap(a, b cadenceSource) time.Duration {
	switch {
	case a.lattice > 0 && b.lattice > 0:
		// Points shift_a + k×g_a against shift_b + m×g_b: their differences
		// are shift_a − shift_b plus every multiple of gcd(g_a, g_b).
		return residueGap(gcdDuration(a.lattice, b.lattice), a.shift-b.shift)
	case a.lattice > 0:
		return latticeGap(a, b.offsets)
	case b.lattice > 0:
		return latticeGap(b, a.offsets)
	}

	gap := time.Minute
	for _, x := range a.offsets {
		for _, y := range b.offsets {
			d := (x - y).Abs()
			d = min(d, time.Minute-d)
			if d > 0 {
				gap = min(gap, d)
			}
		}
	}

	return gap
}

// latticeGap is [crossGap] for an interval against a set of seconds: a second
// on the lattice is a spacing away from the lattice's next point, and one off
// it is as close as its distance to the nearest.
func latticeGap(lattice cadenceSource, offsets []time.Duration) time.Duration {
	gap := lattice.lattice
	for _, offset := range offsets {
		gap = min(gap, residueGap(lattice.lattice, offset-lattice.shift))
	}

	return gap
}

// residueGap is how far delta is from the nearest multiple of spacing, with a
// delta that is itself a multiple charged the spacing: the point coincides
// with one lattice point, and the next is a spacing away.
func residueGap(spacing, delta time.Duration) time.Duration {
	r := delta % spacing
	if r < 0 {
		r += spacing
	}
	if r == 0 {
		return spacing
	}

	return min(r, spacing-r)
}

// gcdDuration is the greatest duration dividing both, by Euclid.
func gcdDuration(a, b time.Duration) time.Duration {
	for b != 0 {
		a, b = b, a%b
	}

	return a.Abs()
}

// daysInMonth is the most days a month can have, February at its leap-year
// longest, because a day that exists in *some* year is a day a cron expression
// can fire on.
var daysInMonth = [13]int{0, 31, 29, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31}

// checkCronCanFire refuses an expression whose day-of-month and month fields
// can never agree on a date: `0 0 31 2 *` is the 31st of February, which no
// year has, and Temporal creates a schedule for it that fires nothing, ever.
//
// Judged only when the day of week is unrestricted. Cron dialects disagree on
// whether a restricted day of week is ANDed with the day of month or ORed with
// it, and under the OR reading such an expression fires on the weekday named
// — so an expression that could be right under either reading is left alone,
// which is this checker's standing bias. Left alone likewise when either field
// carries syntax [expandCronField] does not model.
func checkCronCanFire(original string, fields []string, positions []cronField) error {
	dayOfMonth, month, dayOfWeek := -1, -1, -1
	for i, position := range positions {
		switch position.name {
		case cronDayOfMonth.name:
			dayOfMonth = i
		case cronMonth.name:
			month = i
		case cronDayOfWeek.name:
			dayOfWeek = i
		}
	}
	if dayOfMonth < 0 || month < 0 || dayOfWeek < 0 {
		return nil
	}
	if weekday := fields[dayOfWeek]; weekday != "*" && weekday != "?" {
		return nil
	}

	days, modelled, err := expandCronField(fields[dayOfMonth], cronDayOfMonth)
	if err != nil || !modelled {
		return nil
	}
	months, modelled, err := expandCronField(fields[month], cronMonth)
	if err != nil || !modelled {
		return nil
	}

	longest := 0
	for _, m := range months {
		longest = max(longest, daysInMonth[m])
	}
	for _, d := range days {
		if d <= longest {
			return nil
		}
	}

	return fmt.Errorf("cron expression %q can never fire: no month it names has a day %s (the longest of them "+
		"has %d days), so the schedule would be created and start nothing; name a day every named month has",
		original, fields[dayOfMonth], longest)
}
