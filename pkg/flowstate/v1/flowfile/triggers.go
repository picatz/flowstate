package flowfile

import (
	"fmt"
	"strings"

	yaml "github.com/goccy/go-yaml"
	"github.com/goccy/go-yaml/ast"

	v1 "github.com/picatz/flowstate/pkg/flowstate/v1"
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
var scheduleKeys = []string{"cron", "every", "time_zone", "jitter", "overlap"}

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
			"says when the workflow runs: `cron:` with a cron expression, or `every:` with an interval "+
				"such as 15m, and optionally `time_zone:`, `jitter:` and `overlap:`")

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

	return schedule
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

	if zone := schedule.GetTimeZone(); zone != "" {
		doc = append(doc, yaml.MapItem{Key: "time_zone", Value: zone})
	}

	if jitter := schedule.GetJitter(); jitter != nil {
		doc = append(doc, yaml.MapItem{Key: "jitter", Value: durationToYAML(jitter)})
	}

	if overlap := schedule.GetOverlap(); overlap != v1.ScheduleTrigger_OVERLAP_UNSPECIFIED {
		doc = append(doc, yaml.MapItem{Key: "overlap", Value: v1.OverlapName(overlap)})
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
	if len(schedule.GetCron()) > 0 {
		field = "triggers.schedule.cron"
	} else if schedule.GetEvery() != nil {
		field = "triggers.schedule.every"
	}

	if err := v1.CheckScheduleTrigger(schedule); err != nil {
		ds = append(ds, Diagnostic{Field: field, Message: err.Error()})
	}

	if zone := schedule.GetTimeZone(); zone != "" && len(schedule.GetCron()) == 0 {
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
