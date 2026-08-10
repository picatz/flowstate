package flowfile

import (
	"fmt"
	"strings"
	"time"

	yaml "github.com/goccy/go-yaml"
	"github.com/goccy/go-yaml/ast"

	v1 "github.com/picatz/flowstate/pkg/flowstate/v1"
	"google.golang.org/protobuf/types/known/timestamppb"
)

// `triggers:` — how a file says it is meant to start without anybody asking.
//
// Parsing, writing and validating the block are together here rather than spread
// across parse.go, marshal.go and validate.go, because the three have to agree
// exactly: [Marshal] is the inverse of [Parse], and a key one of them knows about
// and the other does not is a `flow fmt` that silently deletes an author's
// schedule. Keeping them adjacent is the cheapest guard against that, and it is
// the same reason `flow fix`'s scope rules live beside the rewriter they bind.
//
// Nothing here starts anything. A `triggers:` block compiles into the
// specification and both drivers ignore it, so `flow run local` on a scheduled
// workflow runs it once, now — which is what makes a scheduled example runnable in
// CI, and is also the right behavior: the block declares an intent, and
// `flow schedule create` is the act.

// triggerKeys are the kinds of trigger a file may declare.
//
// One today. It is a mapping rather than a bare `schedule:` at the top level so
// that the second kind — a webhook, an event — is a key here rather than another
// top-level word competing with `steps:` for a reader's attention.
var triggerKeys = []string{"schedule"}

// scheduleKeys are what a schedule says about when it fires.
//
// In the order [scheduleTriggerToYAML] writes them, which is the order a reader
// wants them in: the cadence first, then how it is read, then the window it is
// bounded by, then what happens when a firing is late or fails.
var scheduleKeys = []string{
	"cron", "every", "calendars", "time_zone", "jitter", "overlap",
	"start_at", "end_at", "catchup_window", "pause_on_failure",
}

// calendarKeys are the fields of one calendar.
//
// The seven Temporal matches a time against come from
// [v1.ScheduleCalendarFieldNames] rather than being written out again, so the
// parser cannot know a different set of fields from the checker that bounds them,
// and `comment`, which says why the calendar is there and matches nothing.
var calendarKeys = append(v1.ScheduleCalendarFieldNames(), "comment")

// calendarRangeKeys are the long spelling of one range within a calendar field.
var calendarRangeKeys = []string{"start", "end", "step"}

// triggers compiles the top-level `triggers:` block.
//
// It takes the key node as well as the value, which nothing else here needs: the
// one diagnostic this reports is about a block with *nothing* in it, and an empty
// flow mapping has no span to point at. Falling back to the key means `triggers: {}`
// is reported on the line it is written on rather than with no position at all.
func (c *compiler) triggers(key, n ast.Node, path string, r ref) *v1.Triggers {
	c.pos.record(path, spanOfNode(c.resolveQuiet(n)))

	fields, ok := c.fields(n, path, r, triggerKeys)
	if !ok {
		c.report(spanOfNode(n), r,
			"is a mapping of the ways this workflow starts on its own; the one kind today is `schedule:`")

		return nil
	}

	triggers := &v1.Triggers{}

	f, found := fields.get("schedule")
	if !found {
		// Reported rather than dropped. `triggers: {}` is a block that reads as if
		// this workflow starts on its own, and silently compiling it to nothing gives
		// the author no reason to doubt the file — which is the outcome this
		// repository ranks worst.
		c.report(spanOrKey(n, key), r,
			"declares no trigger; the one kind today is `schedule:`, or remove the block — "+
				"an empty `triggers:` reads as if this workflow starts on its own, and it does not")

		return nil
	}

	schedulePath := fieldPath(path, "schedule")
	triggers.Schedule = c.scheduleTrigger(f.value, schedulePath, ref{path: schedulePath, label: "schedule"})

	if triggers.GetSchedule() == nil {
		// Nil rather than an empty message, so `triggers:` written with nothing under
		// it is indistinguishable from `triggers:` absent — the rule `inputs:` and
		// `vars:` follow, and what keeps Marshal an exact inverse.
		return nil
	}

	return triggers
}

// spanOrKey is a node's span, or its key's when the node has none.
func spanOrKey(n, key ast.Node) Span {
	if span := spanOfNode(n); span.IsValid() {
		return span
	}

	return spanOfNode(key)
}

// scheduleTrigger compiles `triggers.schedule`.
func (c *compiler) scheduleTrigger(n ast.Node, path string, r ref) *v1.ScheduleTrigger {
	c.pos.record(path, spanOfNode(c.resolveQuiet(n)))

	fields, ok := c.fields(n, path, r, scheduleKeys)
	if !ok {
		c.report(spanOfNode(n), r,
			"says when the workflow runs: `cron:` with a cron expression, `every:` with an interval such as "+
				"15m, or `calendars:` with explicit times, and optionally `time_zone:`, `jitter:`, `overlap:`, "+
				"`start_at:`, `end_at:`, `catchup_window:` and `pause_on_failure:`")

		return nil
	}

	schedule := &v1.ScheduleTrigger{}

	if f, found := fields.get("cron"); found {
		cronPath := fieldPath(path, "cron")
		schedule.Cron = c.cronExpressions(f.value, cronPath, ref{path: cronPath, label: "schedule cron"})
	}

	if f, found := fields.get("every"); found {
		everyPath := fieldPath(path, "every")
		if every, ok := c.duration(f.value, everyPath, ref{path: everyPath, label: "schedule every"}); ok {
			schedule.Every = every
		}
	}

	if f, found := fields.get("time_zone"); found {
		zonePath := fieldPath(path, "time_zone")
		if zone, ok := c.text(f.value, zonePath, ref{path: zonePath, label: "schedule time_zone"}); ok {
			schedule.TimeZone = zone
		}
	}

	if f, found := fields.get("jitter"); found {
		jitterPath := fieldPath(path, "jitter")
		if jitter, ok := c.duration(f.value, jitterPath, ref{path: jitterPath, label: "schedule jitter"}); ok {
			schedule.Jitter = jitter
		}
	}

	if f, found := fields.get("overlap"); found {
		overlapPath := fieldPath(path, "overlap")
		overlapRef := ref{path: overlapPath, label: "schedule overlap"}
		if text, ok := c.text(f.value, overlapPath, overlapRef); ok {
			overlap, known := v1.ParseOverlap(text)
			if !known {
				c.report(spanOfNode(f.value), overlapRef,
					"is %q, which is not a policy for a firing that overlaps the last one; the policies are %s",
					text, strings.Join(v1.OverlapNames(), ", "))
			}
			schedule.Overlap = overlap
		}
	}

	for _, key := range []string{"start_at", "end_at"} {
		if f, found := fields.get(key); found {
			p := fieldPath(path, key)
			rr := ref{path: p, label: "schedule " + key}
			if text, ok := c.text(f.value, p, rr); ok {
				at, err := time.Parse(time.RFC3339, text)
				if err != nil {
					c.report(spanOfNode(f.value), rr, "must be an RFC3339 timestamp such as 2026-08-09T09:00:00Z")
				} else if key == "start_at" {
					schedule.StartAt = timestamppb.New(at)
				} else {
					schedule.EndAt = timestamppb.New(at)
				}
			}
		}
	}
	if f, found := fields.get("catchup_window"); found {
		p := fieldPath(path, "catchup_window")
		if d, ok := c.duration(f.value, p, ref{path: p, label: "schedule catchup_window"}); ok {
			schedule.CatchupWindow = d
		}
	}
	if f, found := fields.get("pause_on_failure"); found {
		p := fieldPath(path, "pause_on_failure")
		if b, ok := c.boolean(f.value, p, ref{path: p, label: "schedule pause_on_failure"}); ok {
			schedule.PauseOnFailure = b
		}
	}
	if f, found := fields.get("calendars"); found {
		p := fieldPath(path, "calendars")
		schedule.Calendars = c.calendarSpecs(f.value, p)
	}

	return schedule
}

// calendarSpecs reads `calendars:`, written as one calendar or as a list of them.
//
// One or many, the spelling [cronExpressions] already uses for the same schema
// shape and for the same reason: a file naming a single calendar should not have to
// write a one-element list to say so.
func (c *compiler) calendarSpecs(n ast.Node, path string) []*v1.ScheduleTrigger_Calendar {
	resolved := c.resolve(n, path, ref{path: path, label: "schedule calendars"})
	if resolved == nil {
		return nil
	}

	// Recorded for the block as a whole, because the diagnostics
	// [v1.CheckScheduleTrigger] returns are about a calendar rather than about one
	// number in it (an empty calendar, or a range that runs backwards), and a
	// diagnostic with no position is one the author has to go looking for.
	c.pos.record(path, spanOfNode(resolved))

	nodes := []ast.Node{resolved}
	if seq, ok := resolved.(*ast.SequenceNode); ok {
		nodes = seq.Values
	}

	out := make([]*v1.ScheduleTrigger_Calendar, 0, len(nodes))
	for i, node := range nodes {
		p := indexPath(path, i)
		r := ref{path: p, label: "schedule calendar"}

		fields, ok := c.fields(node, p, r, calendarKeys)
		if !ok {
			c.report(spanOfNode(node), r,
				"is a mapping naming the times to match: %s, each written as a whole number, a list of them, "+
					"or `{start: 9, end: 17, step: 2}`", strings.Join(v1.ScheduleCalendarFieldNames(), ", "))

			continue
		}

		calendar := &v1.ScheduleTrigger_Calendar{}
		for _, key := range v1.ScheduleCalendarFieldNames() {
			f, found := fields.get(key)
			if !found {
				continue
			}

			ranges := c.calendarRanges(f.value, fieldPath(p, key), key)
			switch key {
			case "second":
				calendar.Second = ranges
			case "minute":
				calendar.Minute = ranges
			case "hour":
				calendar.Hour = ranges
			case "day_of_month":
				calendar.DayOfMonth = ranges
			case "month":
				calendar.Month = ranges
			case "year":
				calendar.Year = ranges
			case "day_of_week":
				calendar.DayOfWeek = ranges
			}
		}

		if f, found := fields.get("comment"); found {
			commentPath := fieldPath(p, "comment")
			if s, ok := c.text(f.value, commentPath, ref{path: commentPath, label: "calendar comment"}); ok {
				calendar.Comment = s
			}
		}

		out = append(out, calendar)
	}

	return out
}

// calendarRanges reads one calendar field: `hour: 9`, `hour: [9, 17]`, or the long
// form `hour: {start: 9, end: 17, step: 2}`.
//
// The long form exists because the schema has three numbers in a range and a
// grammar that can only spell one of them makes the other two unreachable. That is
// house rule that a capability is not done until a Flowfile can express it. The
// short form stays because `hour: 9` is what almost every calendar says.
//
// Each number is bounded by [v1.ScheduleCalendarFieldBounds] rather than by a
// range wide enough for every field, so `month: 13` is refused where it is written
// instead of being carried to a cluster that refuses it at 03:00. The bounds come
// from the same table [v1.CheckScheduleTrigger] uses: one spelling, so a field
// widened there cannot stay narrow here.
func (c *compiler) calendarRanges(n ast.Node, path, field string) []*v1.ScheduleTrigger_Calendar_Range {
	resolved := c.resolve(n, path, ref{path: path, label: "schedule calendar " + field})
	if resolved == nil {
		return nil
	}

	nodes, listed := []ast.Node{resolved}, false
	if seq, ok := resolved.(*ast.SequenceNode); ok {
		nodes, listed = seq.Values, true
	}

	out := make([]*v1.ScheduleTrigger_Calendar_Range, 0, len(nodes))
	for i, node := range nodes {
		p := path
		if listed {
			p = indexPath(path, i)
		}

		if r := c.calendarRange(node, p, field); r != nil {
			out = append(out, r)
		}
	}

	return out
}

// calendarRange reads one range of one calendar field, in either spelling.
func (c *compiler) calendarRange(n ast.Node, path, field string) *v1.ScheduleTrigger_Calendar_Range {
	low, high, known := v1.ScheduleCalendarFieldBounds(field)
	if !known {
		// Unreachable while calendarKeys is derived from the same table, and cheap
		// insurance against the day it is not: a field nobody can bound is a field
		// nobody should be compiling.
		return nil
	}

	r := ref{path: path, label: "schedule calendar " + field}

	resolved := c.resolve(n, path, r)
	if resolved == nil {
		return nil
	}

	// The short form is anything that is not a mapping, so that a scalar which is
	// not a number is reported by [compiler.integer] as the number it should have
	// been rather than as a mapping it was never trying to be.
	switch resolved.(type) {
	case *ast.MappingNode, *ast.MappingValueNode:
	default:
		value, ok := c.integer(resolved, path, r, int64(low), int64(high))
		if !ok {
			return nil
		}

		return &v1.ScheduleTrigger_Calendar_Range{Start: value}
	}

	fields, ok := c.fields(resolved, path, r, calendarRangeKeys)
	if !ok {
		return nil
	}

	// `start:` is required in the long form: the schema's zero is a real value in
	// four of the seven fields, so a range with no start written would compile to
	// second zero rather than to the absence somebody meant.
	f, found := fields.get("start")
	if !found {
		c.report(spanOfNode(resolved), r, "needs a `start:`; a range with no start does not say where it begins")

		return nil
	}

	startPath := fieldPath(path, "start")
	start, ok := c.integer(f.value, startPath, ref{path: startPath, label: "schedule calendar " + field + " start"}, int64(low), int64(high))
	if !ok {
		return nil
	}

	out := &v1.ScheduleTrigger_Calendar_Range{Start: start}

	// The end is bounded from zero rather than from the field's own minimum,
	// because zero is the schema's "no end written". [v1.CheckScheduleTrigger]
	// holds the rest of the relationship, including that an end may not precede
	// its start.
	if f, found := fields.get("end"); found {
		endPath := fieldPath(path, "end")
		if end, ok := c.integer(f.value, endPath, ref{path: endPath, label: "schedule calendar " + field + " end"}, 0, int64(high)); ok {
			out.End = end
		}
	}

	if f, found := fields.get("step"); found {
		stepPath := fieldPath(path, "step")
		if step, ok := c.integer(f.value, stepPath, ref{path: stepPath, label: "schedule calendar " + field + " step"}, 1, int64(high)); ok {
			out.Step = step
		}
	}

	return out
}

// cronExpressions reads `cron:`, written either as one expression or as a list.
//
// Both spellings, because the schema is repeated and the common case is one. A file
// wanting a single weekday cadence should not have to write a one-element list to
// say so, and a file wanting two must not have to choose which one is real.
func (c *compiler) cronExpressions(n ast.Node, path string, r ref) []string {
	resolved := c.resolve(n, path, r)
	if resolved == nil {
		return nil
	}
	c.pos.record(path, spanOfNode(resolved))

	sequence, isList := resolved.(*ast.SequenceNode)
	if !isList {
		expression, ok := c.text(resolved, path, r)
		if !ok {
			return nil
		}

		return []string{expression}
	}

	if len(sequence.Values) == 0 {
		c.report(spanOfNode(resolved), r,
			"is an empty list, so the schedule never fires; write a cron expression, or remove the key")

		return nil
	}

	expressions := make([]string, 0, len(sequence.Values))
	for i, value := range sequence.Values {
		// Each element carries its own path so a diagnostic about the second
		// expression underlines the second expression.
		elementPath := indexPath(path, i)
		expression, ok := c.text(value, elementPath, ref{path: elementPath, label: r.label})
		if !ok {
			continue
		}
		expressions = append(expressions, expression)
	}

	return expressions
}

// triggersToYAML writes the `triggers:` block back out.
func triggersToYAML(triggers *v1.Triggers) (yaml.MapSlice, error) {
	doc := yaml.MapSlice{}

	if schedule := triggers.GetSchedule(); schedule != nil {
		written, err := scheduleTriggerToYAML(schedule)
		if err != nil {
			return nil, err
		}
		doc = append(doc, yaml.MapItem{Key: "schedule", Value: written})
	}

	return doc, nil
}

// scheduleTriggerToYAML writes one schedule.
//
// The key order is the order the block is read in — when it fires, in whose clock,
// spread by how much, and what happens on an overlap — rather than the schema's
// field order, which is the same rule every other block here follows.
func scheduleTriggerToYAML(schedule *v1.ScheduleTrigger) (yaml.MapSlice, error) {
	doc := yaml.MapSlice{}

	switch expressions := schedule.GetCron(); len(expressions) {
	case 0:
	case 1:
		// One expression is written as a scalar, because that is how it was written
		// and how anybody would write it. A list of one would be a formatter changing
		// a file for no reason a reader can see.
		doc = append(doc, yaml.MapItem{Key: "cron", Value: expressions[0]})
	default:
		doc = append(doc, yaml.MapItem{Key: "cron", Value: expressions})
	}

	if every := schedule.GetEvery(); every != nil {
		doc = append(doc, yaml.MapItem{Key: "every", Value: durationToYAML(every)})
	}

	if calendars := schedule.GetCalendars(); len(calendars) > 0 {
		doc = append(doc, yaml.MapItem{Key: "calendars", Value: calendarsToYAML(calendars)})
	}

	if zone := schedule.GetTimeZone(); zone != "" {
		doc = append(doc, yaml.MapItem{Key: "time_zone", Value: zone})
	}

	if jitter := schedule.GetJitter(); jitter != nil {
		doc = append(doc, yaml.MapItem{Key: "jitter", Value: durationToYAML(jitter)})
	}

	if overlap := schedule.GetOverlap(); overlap != v1.ScheduleTrigger_OVERLAP_UNSPECIFIED {
		doc = append(doc, yaml.MapItem{Key: "overlap", Value: v1.OverlapName(overlap)})
	}

	if at := schedule.GetStartAt(); at != nil {
		doc = append(doc, yaml.MapItem{Key: "start_at", Value: at.AsTime().UTC().Format(time.RFC3339)})
	}

	if at := schedule.GetEndAt(); at != nil {
		doc = append(doc, yaml.MapItem{Key: "end_at", Value: at.AsTime().UTC().Format(time.RFC3339)})
	}

	if window := schedule.GetCatchupWindow(); window != nil {
		doc = append(doc, yaml.MapItem{Key: "catchup_window", Value: durationToYAML(window)})
	}

	if schedule.GetPauseOnFailure() {
		doc = append(doc, yaml.MapItem{Key: "pause_on_failure", Value: true})
	}

	if len(doc) == 0 {
		// A schedule with nothing in it would be written as an empty mapping, which
		// reads back as a schedule that never fires — [validateTriggers] refuses that,
		// and refusing to write it keeps `flow fmt` from producing a file its own
		// validator rejects.
		return nil, fmt.Errorf("triggers schedule: has no cadence, so there is nothing to write; " +
			"give it a `cron:` or an `every:`")
	}

	return doc, nil
}

// calendarsToYAML writes `calendars:` back out.
//
// Every field of every range is written, and that is the whole point of this
// function rather than a nicety: [Marshal] is the inverse of [Parse], and a writer
// that knew only a range's start turned `hour: {start: 9, end: 17, step: 2}` into
// `hour: 9`, a `flow fmt` that silently deletes eight of an author's nine firing
// times, which is the exact class of corruption CLAUDE.md names as the worst thing
// this repository can do.
func calendarsToYAML(calendars []*v1.ScheduleTrigger_Calendar) []yaml.MapSlice {
	out := make([]yaml.MapSlice, 0, len(calendars))
	for _, calendar := range calendars {
		written := yaml.MapSlice{}

		for i, name := range v1.ScheduleCalendarFieldNames() {
			ranges := scheduleCalendarRanges(calendar)[i]
			if len(ranges) == 0 {
				continue
			}

			values := make([]any, 0, len(ranges))
			for _, r := range ranges {
				values = append(values, calendarRangeToYAML(r))
			}

			// A single range is written bare rather than as a one-element list, the
			// spelling `cron:` uses and the one [calendarRanges] reads back.
			var value any = values
			if len(values) == 1 {
				value = values[0]
			}

			written = append(written, yaml.MapItem{Key: name, Value: value})
		}

		if comment := calendar.GetComment(); comment != "" {
			written = append(written, yaml.MapItem{Key: "comment", Value: comment})
		}

		out = append(out, written)
	}

	return out
}

// calendarRangeToYAML writes a range in the shortest spelling that loses nothing.
func calendarRangeToYAML(r *v1.ScheduleTrigger_Calendar_Range) any {
	if r.GetEnd() == 0 && r.GetStep() == 0 {
		return r.GetStart()
	}

	written := yaml.MapSlice{{Key: "start", Value: r.GetStart()}}
	if r.GetEnd() != 0 {
		written = append(written, yaml.MapItem{Key: "end", Value: r.GetEnd()})
	}
	if r.GetStep() != 0 {
		written = append(written, yaml.MapItem{Key: "step", Value: r.GetStep()})
	}

	return written
}

// scheduleCalendarRanges returns a calendar's ranges in the field order
// [v1.ScheduleCalendarFieldNames] gives, so a walk over one cannot fall out of
// step with the names it is writing.
func scheduleCalendarRanges(calendar *v1.ScheduleTrigger_Calendar) [][]*v1.ScheduleTrigger_Calendar_Range {
	return [][]*v1.ScheduleTrigger_Calendar_Range{
		calendar.GetSecond(), calendar.GetMinute(), calendar.GetHour(), calendar.GetDayOfMonth(),
		calendar.GetMonth(), calendar.GetYear(), calendar.GetDayOfWeek(),
	}
}

// validateTriggers reports what is wrong with a declared trigger.
//
// The cadence checks are [v1.CheckScheduleTrigger]'s, asked here where there is a
// line to point at — the shape [validateInputDefault] uses for a mistyped default,
// and for the same reason: one rule, two moments, and the author gets the one with
// a position.
func validateTriggers(wf *v1.Workflow) Diagnostics {
	triggers := wf.GetTriggers()
	if triggers == nil {
		return nil
	}

	schedule := triggers.GetSchedule()
	if schedule == nil {
		return Diagnostics{{
			Field: "triggers",
			Message: "declares no trigger; the one kind today is `schedule:`, or remove the block — " +
				"an empty `triggers:` reads as if this workflow starts on its own, and it does not",
		}}
	}

	var ds Diagnostics

	// Reported against the cadence key that is present, so the squiggle lands on the
	// expression rather than on the whole block — and against the block itself when
	// neither key is there, which is exactly when there is nothing more specific to
	// point at.
	field := "triggers.schedule"
	switch {
	case len(schedule.GetCron()) > 0:
		field = "triggers.schedule.cron"
	case schedule.GetEvery() != nil:
		field = "triggers.schedule.every"
	case len(schedule.GetCalendars()) > 0:
		field = "triggers.schedule.calendars"
	}

	if err := v1.CheckScheduleTrigger(schedule); err != nil {
		ds = append(ds, Diagnostic{Field: field, Message: err.Error()})
	}

	if zone := schedule.GetTimeZone(); zone != "" && len(schedule.GetCron()) == 0 && len(schedule.GetCalendars()) == 0 {
		// An interval is measured from the epoch and has no calendar in it, so a zone
		// beside one does nothing. Reported because a line that does nothing was
		// written by somebody who believed it did — the rule this file's diagnostics
		// are held to.
		ds = append(ds, Diagnostic{
			Field: "triggers.schedule.time_zone",
			Message: "has no effect beside `every:`: an interval is measured from a fixed instant rather " +
				"than against a calendar, so there is no local clock for a zone to shift. Write the cadence " +
				"as a `cron:` expression if it should follow local time",
		})
	}

	return ds
}
