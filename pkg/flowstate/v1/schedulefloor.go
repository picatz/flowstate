package flowstatev1

import (
	"fmt"
	"slices"
	"strconv"
	"strings"
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
// a seven-field expression writes as its first field.
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
	// label names the cadence the way a diagnostic should.
	label string

	// gap is a lower bound on the time between two firings of this cadence
	// alone. A cadence that fires on minute boundaries is charged a minute,
	// which is as far as this file looks.
	gap time.Duration

	// offsets are the seconds of the minute this cadence fires on, sorted and
	// distinct, when they are a set this file can enumerate.
	offsets []time.Duration

	// lattice, when offsets is empty, says the cadence fires on every multiple
	// of this spacing within the minute: an interval that does not divide into
	// minutes reaches every one of them eventually.
	lattice time.Duration
}

// checkScheduleCadenceFloor refuses a trigger that can fire more often than
// [MinScheduleInterval] allows: each cadence on its own, then every pair.
func checkScheduleCadenceFloor(trigger *ScheduleTrigger) error {
	var sources []cadenceSource

	if every := trigger.GetEvery(); every != nil {
		d := every.AsDuration()
		if d < MinScheduleInterval {
			return fmt.Errorf("`every:` is %s; the shortest cadence a schedule may fire at is %s, "+
				"so write `every: %s` or longer", d, MinScheduleInterval, MinScheduleInterval)
		}
		sources = append(sources, intervalCadence("`every:`", d))
	}

	for _, expression := range trigger.GetCron() {
		source, err := cronCadence(expression)
		if err != nil {
			return err
		}
		if source.gap < MinScheduleInterval {
			return fmt.Errorf("%s can fire every %s; the shortest cadence a schedule may fire at is %s",
				source.label, source.gap, MinScheduleInterval)
		}
		sources = append(sources, source)
	}

	for i, calendar := range trigger.GetCalendars() {
		source := calendarCadence(i, calendar)
		if source.gap < MinScheduleInterval {
			return fmt.Errorf("%s can fire every %s; the shortest cadence a schedule may fire at is %s",
				source.label, source.gap, MinScheduleInterval)
		}
		sources = append(sources, source)
	}

	for i := 1; i < len(sources); i++ {
		for j := range i {
			if gap := crossGap(sources[j], sources[i]); gap < MinScheduleInterval {
				return fmt.Errorf("%s and %s together can fire %s apart; the shortest cadence a schedule may "+
					"fire at is %s. Fire both on the same second of the minute, or write the union as one cadence",
					sources[j].label, sources[i].label, gap, MinScheduleInterval)
			}
		}
	}

	return nil
}

// intervalCadence classifies a cadence that fires every d, measured from the
// epoch: on the minute when d is a whole number of minutes, and otherwise on
// every multiple of whatever d and a minute have in common.
func intervalCadence(label string, d time.Duration) cadenceSource {
	source := cadenceSource{label: label, gap: d}
	if g := gcdDuration(d, time.Minute); g == time.Minute {
		source.offsets = []time.Duration{0}
	} else {
		source.lattice = g
	}

	return source
}

// minuteAligned is a cadence that fires on minute boundaries and no faster —
// every cron form without a seconds field, and a calendar that writes none.
func minuteAligned(label string) cadenceSource {
	return cadenceSource{label: label, gap: time.Minute, offsets: []time.Duration{0}}
}

// secondsCadence classifies a cadence by the seconds of the minute it names:
// one second is once a minute at most, and several are as close as the nearest
// pair, the wrap from the last to the first included.
func secondsCadence(label string, seconds []int) cadenceSource {
	offsets := make([]time.Duration, 0, len(seconds))
	for _, s := range seconds {
		offsets = append(offsets, time.Duration(s)*time.Second)
	}
	slices.Sort(offsets)
	offsets = slices.Compact(offsets)

	gap := time.Minute
	if len(offsets) > 1 {
		for i := range offsets {
			d := time.Minute - offsets[i] + offsets[0]
			if i+1 < len(offsets) {
				d = offsets[i+1] - offsets[i]
			}
			gap = min(gap, d)
		}
	}

	return cadenceSource{label: label, gap: gap, offsets: offsets}
}

// cronCadence classifies one expression [CheckCronExpression] has accepted,
// reading the grammar in the order that function reads it.
//
// The error is for an `@every` whose interval this cannot read. The checker
// leaves that interval to the cluster, which is right for a grammar question;
// it is not right here, because an interval nobody can read is an interval
// nobody can hold to a floor.
func cronCadence(expression string) (cadenceSource, error) {
	label := fmt.Sprintf("cron expression %q", expression)

	body := strings.TrimSpace(stripCronComment(expression))
	if zone, rest, found := strings.Cut(body, " "); found &&
		(strings.HasPrefix(zone, "CRON_TZ=") || strings.HasPrefix(zone, "TZ=")) {
		body = strings.TrimSpace(rest)
	}

	if strings.HasPrefix(body, "@") {
		head, rest, _ := strings.Cut(body, " ")
		if !strings.EqualFold(head, "@every") {
			return minuteAligned(label), nil
		}

		// `@every <interval>[/<phase>]`: the interval is what sets the gap, and
		// a phase only shifts it.
		interval, _, _ := strings.Cut(strings.TrimSpace(rest), "/")
		d, err := ParseDuration(strings.TrimSpace(interval))
		if err != nil || d <= 0 {
			return cadenceSource{}, fmt.Errorf("%s has an `@every` interval this cannot read, so it cannot be held "+
				"to the shortest cadence of %s; write the interval as 15m, 1h or 7d, or use the schedule's "+
				"own `every:` key", label, MinScheduleInterval)
		}

		return intervalCadence(label, d), nil
	}

	fields := strings.Fields(body)
	if len(fields) != 7 {
		return minuteAligned(label), nil
	}

	seconds, ok := cronFieldValues(fields[0], cronSecond)
	if !ok {
		// A seconds field written in a form this does not expand is charged
		// the fastest cadence a seconds field can carry, which refuses it: the
		// alternative is accepting a cadence this never measured.
		return cadenceSource{label: label, gap: time.Second, lattice: time.Second}, nil
	}

	return secondsCadence(label, seconds), nil
}

// calendarCadence classifies a calendar by its `second:` field, which defaults
// to zero when unwritten — see [calendarMinimumPeriod] for why that default is
// the whole reason a calendar of `hour: 9` fires once and not 3,600 times.
func calendarCadence(index int, calendar *ScheduleTrigger_Calendar) cadenceSource {
	label := fmt.Sprintf("calendar %d", index+1)

	ranges := calendar.GetSecond()
	if len(ranges) == 0 {
		return minuteAligned(label)
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

	return secondsCadence(label, seconds)
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
		return gcdDuration(a.lattice, b.lattice)
	case a.lattice > 0:
		return latticeGap(a.lattice, b.offsets)
	case b.lattice > 0:
		return latticeGap(b.lattice, a.offsets)
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
func latticeGap(spacing time.Duration, offsets []time.Duration) time.Duration {
	gap := spacing
	for _, offset := range offsets {
		if r := offset % spacing; r > 0 {
			gap = min(gap, r, spacing-r)
		}
	}

	return gap
}

// gcdDuration is the greatest duration dividing both, by Euclid.
func gcdDuration(a, b time.Duration) time.Duration {
	for b != 0 {
		a, b = b, a%b
	}

	return a.Abs()
}

// cronFieldValues expands one field into the values it names, in the range
// its position allows.
//
// Only the numeric grammar: `*`, a number or name, a range, a list, and a step
// over any of them. Anything else — `L`, `W`, `15#3`, a range that runs
// backwards — answers false, which every caller reads as "not something this
// judges" rather than as a set.
func cronFieldValues(field string, position cronField) ([]int, bool) {
	var values []int

	for _, element := range strings.Split(field, ",") {
		value, stepText, hasStep := strings.Cut(element, "/")

		step := 1
		if hasStep {
			n, err := strconv.Atoi(strings.TrimSpace(stepText))
			if err != nil || n <= 0 {
				return nil, false
			}
			step = n
		}

		low, high := position.min, position.max
		if value = strings.TrimSpace(value); value != "*" && value != "?" && value != "" {
			from, to, isRange := strings.Cut(value, "-")
			a, ok := cronAtomValue(from, position)
			if !ok {
				return nil, false
			}
			low = a
			switch {
			case isRange:
				b, ok := cronAtomValue(to, position)
				if !ok {
					return nil, false
				}
				high = b
			case hasStep:
				// `5/10` is "from 5, every 10", up to the field's end.
			default:
				high = a
			}
		}

		if low < position.min || high > position.max || low > high {
			return nil, false
		}
		for v := low; v <= high; v += step {
			values = append(values, v)
		}
	}

	slices.Sort(values)
	values = slices.Compact(values)

	return values, len(values) > 0
}

// cronAtomValue reads one number, or one of the names the position has.
func cronAtomValue(atom string, position cronField) (int, bool) {
	atom = strings.TrimSpace(atom)
	if n, err := strconv.Atoi(atom); err == nil {
		return n, true
	}

	upper := strings.ToUpper(atom)
	for i, name := range position.names {
		if upper == name {
			return position.min + i, true
		}
	}

	return 0, false
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
// which is this checker's standing bias.
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

	days, ok := cronFieldValues(fields[dayOfMonth], cronDayOfMonth)
	if !ok {
		return nil
	}
	months, ok := cronFieldValues(fields[month], cronMonth)
	if !ok {
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
