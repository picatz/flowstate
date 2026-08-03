package flowstatev1

import (
	"fmt"
	"strconv"
	"strings"
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
		return fmt.Errorf("a schedule needs a cadence: write `cron:` with a cron expression, or `every:` with an interval such as 15m")
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
	if len(trigger.GetCron()) == 0 && trigger.GetEvery() == nil {
		return fmt.Errorf("a schedule needs a cadence: write `cron:` with a cron expression, or `every:` with an interval such as 15m")
	}

	if zone := trigger.GetTimeZone(); zone != "" {
		if err := checkTimeZoneName(zone); err != nil {
			return err
		}
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
