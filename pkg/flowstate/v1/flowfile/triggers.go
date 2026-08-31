package flowfile

import (
	"fmt"
	"maps"
	"slices"
	"strconv"
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

// triggerKeys are the kinds of trigger a file may declare in the mapping
// spelling.
//
// Two, and both are kinds a workflow has at most one of. `triggers:` written as a
// mapping is the spelling every scheduled file in this repository already uses, and
// it keeps meaning exactly what it meant; `manual:` joins it because a workflow says
// one thing about being started by a person, so the mapping's one-of-each shape is
// the right shape for it. A *webhook* arrives only through the list spelling below,
// because it is a call site — it carries arguments — and a mapping keyed by kind has
// nowhere to put two of the same kind.
var triggerKeys = []string{"schedule", "manual"}

// triggerKindKeys are the words that make one entry of the list spelling a
// trigger of a particular kind.
//
// The list is read the way `steps:` is: each entry names its kind first, and the
// rest of the entry belongs to that kind. `webhook:` names a source; `schedule:`
// carries the same cadence mapping the top-level spelling does, so a file that
// wants a nightly sweep *and* a webhook can write both without the mapping
// spelling's one-of-each limit. `manual:` is the one that narrows rather than
// declares a source — see [manualKeys].
var triggerKindKeys = []string{"webhook", "schedule", "manual"}

// manualKeys are what a `manual:` block says when it is written as a mapping.
//
// Deliberately short, and deliberately without a `denied:` key. Refusal has one
// spelling — `manual: denied`, a scalar on one line — because the whole argument for
// declaring it at all is that a lock nobody can grep for is a lock nobody knows
// about. Two spellings of one refusal would mean a search for the greppable one
// misses half the files that have it.
var manualKeys = []string{"require_reason", "allowed_principals"}

// manualDenied is the scalar that refuses manual starts.
//
// A word rather than `denied: true`, for the reason [webhookKeys] gives its own
// order: the line is read by whoever is looking for it, and `manual: denied` is a
// sentence where `manual: {denied: true}` is a configuration.
const manualDenied = "denied"

// webhookKeys are what one webhook entry says.
//
// In the order [webhookTriggerToYAML] writes them, which is the order the entry
// reads in: which source this is, how a delivery from it is proved genuine, what
// names one delivery, and what it binds.
var webhookKeys = []string{"webhook", "verify", "idempotency_key", "with"}

// scheduleItemKeys are what one `- schedule:` entry of the list spelling says: the
// cadence mapping, under its own key, and nothing else.
//
// A schedule has no `with:` here, deliberately. Arguments for a scheduled run are
// bound when the schedule is *created* (see [v1.ScheduleTrigger]'s doc on why), and
// giving the list spelling a second place to write them would make one value
// expressible twice with a precedence rule nobody benefits from learning.
var scheduleItemKeys = []string{"schedule"}

// manualItemKeys are what one `- manual:` entry of the list spelling says: what a
// person may do, under its own key, and nothing else.
//
// The narrowing itself is written *under* `manual:` rather than beside it, so the
// list spelling and the mapping spelling hold the identical block and there is one
// grammar for it rather than two.
var manualItemKeys = []string{"manual"}

// notInTriggerHelp is why a `${secret(...)}` cannot appear in a webhook's `with:`.
//
// The same shape of refusal as [notAcrossCallHelp] and for the same reason read one
// step earlier: a trigger's argument is bound into `inputs:`, which is ordinary run
// data — evaluated by the workflow and written to durable history — and a reference
// names something that does not exist until a worker resolves it. `verify:` is the
// one place under a webhook where a reference belongs, because that material is
// consumed by the receiver checking a signature and never by the run.
const notInTriggerHelp = "a secret reference cannot be bound to an input; a trigger's `with:` is " +
	"resolved into `inputs:`, which the workflow evaluates and writes to durable history. Write " +
	"${secret('...')} on the task input that consumes the secret, or — for a signing key — under " +
	"this webhook's `verify:`"

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

	// Two spellings, one block. A sequence is the call-site spelling, where each
	// entry names its kind — which is what a webhook needs, since several sources
	// starting one workload is ordinary and a mapping keyed by kind can hold only
	// one of each. A mapping is the older spelling and still means what it meant.
	if sequence, listed := c.resolveQuiet(n).(*ast.SequenceNode); listed {
		return c.triggerList(sequence, path, r)
	}

	fields, ok := c.fields(n, path, r, triggerKeys)
	if !ok {
		c.report(spanOfNode(n), r,
			"is a mapping of what this workflow says about starting: `schedule:` for a cadence, "+
				"`manual:` for what a person may do")

		return nil
	}

	triggers := &v1.Triggers{}

	if f, found := fields.get("schedule"); found {
		schedulePath := fieldPath(path, "schedule")
		triggers.Schedule = c.scheduleTrigger(f.value, schedulePath, ref{path: schedulePath, label: "schedule"})
	}

	if f, found := fields.get("manual"); found {
		manualPath := fieldPath(path, "manual")
		triggers.Manual = c.manualTrigger(f.key, f.value, manualPath, ref{path: manualPath, label: "manual"})
	}

	if triggers.GetSchedule() == nil && triggers.GetManual() == nil {
		// Reported rather than dropped. `triggers: {}` is a block that reads as if
		// this workflow starts on its own, and silently compiling it to nothing gives
		// the author no reason to doubt the file — which is the outcome this
		// repository ranks worst.
		//
		// Reported only when *neither* key produced anything, so a `schedule:` whose
		// own cadence was refused says one thing rather than two: the cadence
		// diagnostic already has the position and the remedy.
		if _, hasSchedule := fields.get("schedule"); !hasSchedule {
			if _, hasManual := fields.get("manual"); !hasManual {
				c.report(spanOrKey(n, key), r,
					"declares no trigger; the kinds are `schedule:`, `webhook:` (in the list spelling) and "+
						"`manual:`, or remove the block. An empty `triggers:` reads as if this workflow "+
						"starts on its own, and it does not")
			}
		}

		// Nil rather than an empty message, so `triggers:` written with nothing under
		// it is indistinguishable from `triggers:` absent — the rule `inputs:` and
		// `vars:` follow, and what keeps Marshal an exact inverse.
		return nil
	}

	return triggers
}

// manualTrigger compiles a `manual:` block, in either of its two spellings: the
// scalar `denied`, and a mapping that narrows.
//
// The asymmetry between them is the design and not an accident of parsing. A
// refusal is one word on one line, so that `grep -r 'manual: denied'` finds every
// workflow a person may not start; a narrowing is a mapping, because it has parts.
// Anything else under `manual:` is reported rather than guessed at, including the
// `denied: true` somebody will reach for — see [manualKeys].
// key is the `manual:` key itself, used only where the value has no span of its
// own: `manual: {}` is an empty flow mapping, and a diagnostic about a block with
// nothing in it would otherwise have no line at all. The same fallback
// [compiler.triggers] makes for `triggers: {}`, and for the same reason.
func (c *compiler) manualTrigger(key, n ast.Node, path string, r ref) *v1.ManualTrigger {
	resolved := c.resolve(n, path, r)
	if resolved == nil {
		return nil
	}
	c.pos.record(path, spanOfNode(resolved))

	// The scalar spelling first, so `manual: denied` is never read as a mapping
	// that failed to parse.
	switch resolved.(type) {
	case *ast.MappingNode, *ast.MappingValueNode:
	default:
		word, ok := c.text(resolved, path, r)
		if !ok {
			return nil
		}
		if word != manualDenied {
			c.report(spanOfNode(resolved), r,
				"is %q, which is not something a `manual:` says. Write `manual: %s` to refuse manual "+
					"starts outright, or a mapping with `require_reason:` and `allowed_principals:` to "+
					"narrow them. Declaring nothing at all leaves manual starts as they are, which is "+
					"what every workflow without this block does",
				word, manualDenied)

			return nil
		}

		return &v1.ManualTrigger{Denied: true}
	}

	fields, ok := c.fields(resolved, path, r, manualKeys)
	if !ok {
		return nil
	}

	manual := &v1.ManualTrigger{}

	if f, found := fields.get("require_reason"); found {
		p := fieldPath(path, "require_reason")
		if b, ok := c.boolean(f.value, p, ref{path: p, label: "manual require_reason"}); ok {
			manual.RequireReason = b
		}
	}

	if f, found := fields.get("allowed_principals"); found {
		p := fieldPath(path, "allowed_principals")
		manual.AllowedPrincipals = c.manualPrincipals(f.value, p)
	}

	if !manual.GetRequireReason() && len(manual.GetAllowedPrincipals()) == 0 {
		// A block that narrows nothing reads as if it did, which is the one thing a
		// diagnostic here must not allow: `manual:` written with nothing under it is
		// how somebody believes they have restricted a workflow they have not.
		c.report(spanOrKey(resolved, key), r,
			"narrows nothing, so it says exactly what writing no `manual:` at all says. Write "+
				"`manual: %s` to refuse manual starts, `require_reason: true` to require a reason for "+
				"one, or `allowed_principals:` to say who may make one",
			manualDenied)

		return nil
	}

	return manual
}

// manualPrincipals reads `allowed_principals:`, written as one subject or as a
// list of them.
//
// One or many, the spelling `cron:` and `calendars:` already use for the same
// schema shape and for the same reason: a file naming a single principal should not
// have to write a one-element list to say so.
//
// Every entry is a subject a caller authenticates as, so an empty one is refused
// where it is written. It would otherwise become a set member matching the empty
// subject a deployment with no identity provider attests — a policy admitting
// nobody in particular, which is a policy admitting everyone. [v1.CheckManualStart]
// refuses that at the boundary too; this is the same rule with a line to point at.
func (c *compiler) manualPrincipals(n ast.Node, path string) []string {
	r := ref{path: path, label: "manual allowed_principals"}

	resolved := c.resolve(n, path, r)
	if resolved == nil {
		return nil
	}
	c.pos.record(path, spanOfNode(resolved))

	nodes, listed := []ast.Node{resolved}, false
	if seq, ok := resolved.(*ast.SequenceNode); ok {
		nodes, listed = seq.Values, true
	}

	if listed && len(nodes) == 0 {
		c.report(spanOfNode(resolved), r,
			"is an empty list, which allows nobody at all rather than everybody; remove the key to "+
				"leave manual starts open, or write `manual: %s` to refuse them", manualDenied)

		return nil
	}
	if len(nodes) > 64 {
		c.report(spanOfNode(resolved), r,
			"names %d principals, exceeding the limit of 64; narrow the allowlist", len(nodes))

		return nil
	}

	out := make([]string, 0, len(nodes))
	seen := make(map[string]struct{}, len(nodes))
	for i, node := range nodes {
		p := path
		if listed {
			p = indexPath(path, i)
		}

		subject, ok := c.text(node, p, ref{path: p, label: r.label})
		if !ok {
			continue
		}
		if strings.TrimSpace(subject) == "" {
			c.report(spanOfNode(node), ref{path: p, label: r.label},
				"is empty, which names nobody; write the subject a caller authenticates as, or remove "+
					"the entry — an empty principal would match a caller a deployment with no identity "+
					"provider attests, which is every caller")

			continue
		}
		if !v1.LooksLikeQualifiedSubject(subject) {
			c.report(spanOfNode(node), ref{path: p, label: r.label},
				"%q is not \"<issuer>#<subject>\"; a bare or malformed subject is refused because a "+
					"subject is only unique within its issuer", subject)

			continue
		}
		if _, duplicate := seen[subject]; duplicate {
			c.report(spanOfNode(node), ref{path: p, label: r.label},
				"lists %q twice; a principal is either allowed or not, so the second entry does nothing",
				subject)

			continue
		}
		seen[subject] = struct{}{}

		out = append(out, subject)
	}

	if len(out) == 0 {
		return nil
	}

	return out
}

// triggerList compiles the call-site spelling: `triggers:` written as a list, one
// entry per source, each naming its kind.
//
// Compiled into the same [v1.Triggers] the mapping spelling produces, because there
// is one message and a file's spelling is not a fact about the workload. A second
// `- schedule:` is refused rather than merged: the schema holds one cadence (a
// schedule is a union of cron expressions, intervals and calendars, so a second
// entry would be a second way to write what one entry already unions), and silently
// keeping the last would drop a cadence somebody wrote.
func (c *compiler) triggerList(sequence *ast.SequenceNode, path string, r ref) *v1.Triggers {
	if len(sequence.Values) == 0 {
		c.report(spanOfNode(sequence), r,
			"is an empty list, so this workflow starts on its own in no way at all; write an entry such as "+
				"`- webhook: <name>`, or remove the block")

		return nil
	}

	triggers := &v1.Triggers{}

	for i, node := range sequence.Values {
		p := indexPath(path, i)
		entryRef := ref{path: p, label: "trigger"}
		c.pos.record(p, spanOfNode(c.resolveQuiet(node)))

		entries, ok := c.entries(node, p, entryRef)
		if !ok {
			continue
		}

		// The kind is read from the entry's own keys rather than assumed from its
		// position, and the rest of the entry is then checked against *that* kind's
		// keys — so `verify:` under a schedule is reported as a key that does not
		// belong there rather than quietly accepted by a union of every kind's keys.
		kind, found := triggerKindOf(entries)
		if !found {
			c.report(spanOfNode(node), entryRef,
				"names no kind of trigger; each entry says what it is first — `- webhook: <name>` for a "+
					"delivery, or `- schedule:` with a cadence under it")

			continue
		}

		switch kind {
		case "webhook":
			fields := c.check(entries, entryRef, webhookKeys)
			if webhook := c.webhookTrigger(fields, p, entryRef); webhook != nil {
				// [v1.Triggers.Webhooks] holds only webhook entries, so its own
				// index is not this entry's position in the `triggers:` list the
				// author wrote whenever a `- schedule:` sits before or between
				// webhooks. p is that original position, still known here;
				// recorded by name so a later walk over Webhooks (which has lost
				// the list index) can recover it — see [Positions.TriggerPath].
				c.pos.recordTrigger(webhook.GetName(), p)
				triggers.Webhooks = append(triggers.Webhooks, webhook)
			}

		case "schedule":
			fields := c.check(entries, entryRef, scheduleItemKeys)
			f, _ := fields.get("schedule")
			if triggers.GetSchedule() != nil {
				c.report(spanOfNode(f.key), entryRef,
					"is a second `- schedule:`, and a workflow has one cadence: cron expressions, intervals "+
						"and calendars are unioned within a single schedule, so write the extra times in the "+
						"first entry")

				continue
			}

			// Recorded under the canonical `triggers.schedule` path rather than under
			// this entry's index, because a workflow has one schedule and
			// [validateTriggers] addresses its cadence diagnostics there — a position
			// keyed by which list entry the author happened to write it in would be a
			// span nothing looks up, and a cron expression reported with no line at
			// all.
			schedulePath := fieldPath("triggers", "schedule")
			triggers.Schedule = c.scheduleTrigger(f.value, schedulePath, ref{path: schedulePath, label: "schedule"})

		case "manual":
			fields := c.check(entries, entryRef, manualItemKeys)
			f, _ := fields.get("manual")
			if triggers.GetManual() != nil {
				c.report(spanOfNode(f.key), entryRef,
					"is a second `- manual:`, and a workflow says one thing about being started by a "+
						"person: write the whole rule in the first entry")

				continue
			}

			// Recorded under the canonical `triggers.manual` path rather than under
			// this entry's index, for the reason a schedule is: [validateTriggers]
			// addresses the contradiction diagnostics there, and a position keyed by
			// which list entry the author happened to write it in would be a span
			// nothing looks up.
			manualPath := fieldPath("triggers", "manual")
			triggers.Manual = c.manualTrigger(f.key, f.value, manualPath, ref{path: manualPath, label: "manual"})
		}
	}

	if triggers.GetSchedule() == nil && len(triggers.GetWebhooks()) == 0 && triggers.GetManual() == nil {
		// Nil rather than an empty message, exactly as the mapping spelling returns
		// nil for a block with nothing usable in it: what keeps [Marshal] an exact
		// inverse is that `triggers:` absent and `triggers:` empty compile alike.
		return nil
	}

	return triggers
}

// triggerKindOf returns the kind one list entry declares, reading the entry's keys
// in the order they were written.
//
// First key wins, which matters only for an entry that names two kinds — a mistake
// the kind's own key check then reports, since the other kind's word is not one of
// its keys.
func triggerKindOf(entries []entry) (string, bool) {
	for _, e := range entries {
		if slices.Contains(triggerKindKeys, e.name) {
			return e.name, true
		}
	}

	return "", false
}

// webhookTrigger compiles one `- webhook:` entry.
//
// Nothing here decides whether a delivery is genuine, and nothing here reads a
// secret: `verify:` compiles to a reference, checked for shape and left for
// whatever resolves it. That division is the standing rule about what a validator
// may report — whether this deployment has the signing key is the deployment's
// answer — and it is what keeps `flow validate` free of I/O.
func (c *compiler) webhookTrigger(fields *fieldSet, path string, r ref) *v1.WebhookTrigger {
	nameField, found := fields.get("webhook")
	if !found {
		// Unreachable while [triggerKindOf] found the key, and cheap insurance
		// against a caller that did not.
		return nil
	}

	namePath := fieldPath(path, "webhook")
	name, ok := c.text(nameField.value, namePath, ref{path: namePath, label: "webhook"})
	if !ok {
		return nil
	}

	webhook := &v1.WebhookTrigger{Name: name}
	webhookRef := ref{path: path, label: fmt.Sprintf("webhook %q", name)}

	if f, found := fields.get("verify"); found {
		verifyPath := fieldPath(path, "verify")
		webhook.Verify = c.webhookVerify(f.value, verifyPath, ref{path: verifyPath, label: fmt.Sprintf("webhook %q verify", name)})
	} else {
		// Reported here as well as by the validator, because the two answer for
		// different inputs: this one has the entry's own span, and a specification
		// built by hand reaches [v1.CheckWebhookTrigger] instead. Fail closed either
		// way — a webhook with no scheme can never accept a delivery.
		c.report(spanOfNode(nameField.key), webhookRef, "%s", v1.CheckWebhookVerify(name, nil).Error())
	}

	if f, found := fields.get("idempotency_key"); found {
		keyPath := fieldPath(path, "idempotency_key")
		webhook.IdempotencyKey = c.exprValue(f.value, keyPath,
			ref{path: keyPath, label: fmt.Sprintf("webhook %q idempotency_key", name)})
	} else {
		c.report(spanOfNode(nameField.key), webhookRef, "%s", v1.CheckWebhookIdempotencyKey(name, nil).Error())
	}

	if f, found := fields.get("with"); found {
		withPath := fieldPath(path, "with")
		c.pos.record(withPath, spanOfNode(c.resolveQuiet(f.value)))
		webhook.Arguments = c.triggerArguments(f.value, withPath, ref{path: withPath, label: fmt.Sprintf("webhook %q with", name)})
	}

	return webhook
}

// webhookVerify compiles a `verify:` block: one signing scheme per entry, each
// bound to the material a delivery is checked against.
//
// The value is compiled as a task input would be, which is what makes
// `${secret('env:STRIPE_WEBHOOK_SECRET')}` a reference rather than an expression
// somebody has to evaluate. That a scheme's value *must* be a reference is
// [v1.CheckWebhookVerifyScheme]'s rule, reported against this position by the
// validator, so the two moments agree.
func (c *compiler) webhookVerify(n ast.Node, path string, r ref) map[string]*v1.Value {
	entries, ok := c.entries(n, path, r)
	if !ok {
		return nil
	}

	verify := make(map[string]*v1.Value, len(entries))
	for _, e := range entries {
		valuePath := fieldPath(path, e.name)
		if value := c.inputValue(e.value, valuePath, ref{path: valuePath, label: "verify." + e.name}); value != nil {
			verify[e.name] = value
		}
	}

	if len(verify) == 0 {
		return nil
	}

	return verify
}

// triggerArguments compiles a trigger's `with:` mapping — the call site's
// arguments, one value per input it binds.
//
// The same construct a `call:` step's `with:` is, and compiled the same way down to
// the refusal of a secret reference: an argument is bound into `inputs:` and read
// by the workflow, so a reference here would be resolved into durable history. Only
// the sentence differs, because the boundary it is about differs. See
// [notInTriggerHelp].
func (c *compiler) triggerArguments(n ast.Node, path string, r ref) map[string]*v1.Value {
	entries, ok := c.entries(n, path, r)
	if !ok {
		return nil
	}

	arguments := make(map[string]*v1.Value, len(entries))
	for _, e := range entries {
		valuePath := fieldPath(path, e.name)
		valueRef := ref{path: valuePath, label: "with." + e.name}

		if resolved := c.resolveQuiet(e.value); resolved != nil && c.holdsSecretMarker(resolved) {
			c.report(c.secretMarkerSpan(resolved), valueRef, "%s", notInTriggerHelp)

			continue
		}

		if value := c.inputValue(e.value, valuePath, valueRef); value != nil {
			arguments[e.name] = value
		}
	}

	if len(arguments) == 0 {
		return nil
	}

	return arguments
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
// the house rule that a capability is not done until a Flowfile can express it. The
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

// triggersToYAML writes the `triggers:` block back out, in whichever of the two
// spellings can carry what the message holds.
//
// A block with only a schedule is written as a mapping, which is the spelling every
// scheduled file already uses: writing it as a one-entry list would be `flow fmt`
// rewriting a file for no reason its author can see. Anything with a webhook in it
// is written as a list, because that is the only spelling that can hold one.
//
// The `any` return is what that choice costs: [Marshal]'s other blocks are all
// mappings and this one is a mapping or a sequence. Worth it — the alternative is
// migrating every existing file to the list spelling the first time somebody runs
// `flow fmt`, which is a diff nobody asked for.
func triggersToYAML(triggers *v1.Triggers) (any, error) {
	if len(triggers.GetWebhooks()) > 0 {
		return triggerListToYAML(triggers)
	}

	doc := yaml.MapSlice{}

	if schedule := triggers.GetSchedule(); schedule != nil {
		written, err := scheduleTriggerToYAML(schedule)
		if err != nil {
			return nil, err
		}
		doc = append(doc, yaml.MapItem{Key: "schedule", Value: written})
	}

	if manual := triggers.GetManual(); manual != nil {
		written, err := manualTriggerToYAML(manual)
		if err != nil {
			return nil, err
		}
		doc = append(doc, yaml.MapItem{Key: "manual", Value: written})
	}

	return doc, nil
}

// manualTriggerToYAML writes a `manual:` block in the spelling that can carry what
// it holds: the scalar for a refusal, a mapping for a narrowing.
//
// The two are not interchangeable and choosing between them here is the same choice
// [manualTrigger] reads. Writing a refusal as `{denied: true}` would be `flow fmt`
// turning the one greppable spelling into one nobody searches for.
func manualTriggerToYAML(manual *v1.ManualTrigger) (any, error) {
	if manual.GetDenied() {
		if manual.GetRequireReason() || len(manual.GetAllowedPrincipals()) > 0 {
			// Refused rather than written, for the reason [scheduleTriggerToYAML]
			// refuses a cadence-less schedule: the contradiction is what
			// [v1.CheckManualTrigger] reports, so writing it would produce a file
			// this package's own validator rejects.
			return nil, fmt.Errorf("triggers manual: refuses manual starts and also narrows them, which " +
				"cannot both hold; write `manual: denied`, or the narrowing without it")
		}

		return manualDenied, nil
	}

	doc := yaml.MapSlice{}

	if manual.GetRequireReason() {
		doc = append(doc, yaml.MapItem{Key: "require_reason", Value: true})
	}

	if principals := manual.GetAllowedPrincipals(); len(principals) > 0 {
		// A single principal is written bare rather than as a one-element list, the
		// spelling `cron:` uses and the one [manualPrincipals] reads back.
		var value any = principals
		if len(principals) == 1 {
			value = principals[0]
		}
		doc = append(doc, yaml.MapItem{Key: "allowed_principals", Value: value})
	}

	if len(doc) == 0 {
		return nil, fmt.Errorf("triggers manual: narrows nothing, so there is nothing to write; give it " +
			"`require_reason: true`, an `allowed_principals:`, or write `manual: denied`")
	}

	return doc, nil
}

// triggerListToYAML writes the call-site spelling.
//
// Webhooks first, in the order they were declared, and the schedule last if there
// is one: a reader meets the sources that carry arguments together, and the cadence
// — which carries none — does not sit between two of them.
func triggerListToYAML(triggers *v1.Triggers) ([]any, error) {
	out := make([]any, 0, len(triggers.GetWebhooks())+1)

	for _, webhook := range triggers.GetWebhooks() {
		written, err := webhookTriggerToYAML(webhook)
		if err != nil {
			return nil, err
		}
		out = append(out, written)
	}

	if schedule := triggers.GetSchedule(); schedule != nil {
		written, err := scheduleTriggerToYAML(schedule)
		if err != nil {
			return nil, err
		}
		out = append(out, yaml.MapSlice{{Key: "schedule", Value: written}})
	}

	// Last of all, because it is the entry that says what the others do *not*
	// cover: a reader meets the sources first and then what a person may do
	// alongside them. `- manual: denied` under a list of webhooks is the shape the
	// design is written around, and it reads as the sentence it is.
	if manual := triggers.GetManual(); manual != nil {
		written, err := manualTriggerToYAML(manual)
		if err != nil {
			return nil, err
		}
		out = append(out, yaml.MapSlice{{Key: "manual", Value: written}})
	}

	return out, nil
}

// webhookTriggerToYAML writes one webhook entry.
//
// The key order is the order the entry reads in — which source, how a delivery is
// proved genuine, what names one delivery, what it binds — rather than the schema's
// field order, which is the rule every other block here follows.
//
// Every field is written, including a `verify:` scheme's reference and every
// argument, for the reason [calendarsToYAML] states at length: [Marshal] is the
// inverse of [Parse], and a writer that knew about fewer keys than the parser reads
// is a `flow fmt` that silently deletes what an author wrote.
func webhookTriggerToYAML(webhook *v1.WebhookTrigger) (yaml.MapSlice, error) {
	if webhook.GetName() == "" {
		// Refused rather than written: an entry with no name reads back as a list
		// entry naming no kind, which the parser reports — so writing it would
		// produce a file this package's own validator rejects.
		return nil, fmt.Errorf("triggers webhook: has no name, so there is nothing to write; " +
			"give it a `webhook: <name>`")
	}

	doc := yaml.MapSlice{{Key: "webhook", Value: webhook.GetName()}}

	if verify := webhook.GetVerify(); len(verify) > 0 {
		written := yaml.MapSlice{}
		for _, scheme := range slices.Sorted(maps.Keys(verify)) {
			value, err := inputValueToYAML(verify[scheme])
			if err != nil {
				return nil, fmt.Errorf("triggers webhook %q verify %q: %w", webhook.GetName(), scheme, err)
			}
			written = append(written, yaml.MapItem{Key: scheme, Value: value})
		}
		doc = append(doc, yaml.MapItem{Key: "verify", Value: written})
	}

	if key := webhook.GetIdempotencyKey(); key != nil {
		written, err := exprValueToYAML(key)
		if err != nil {
			return nil, fmt.Errorf("triggers webhook %q idempotency_key: %w", webhook.GetName(), err)
		}
		doc = append(doc, yaml.MapItem{Key: "idempotency_key", Value: written})
	}

	if arguments := webhook.GetArguments(); len(arguments) > 0 {
		written := yaml.MapSlice{}
		for _, name := range slices.Sorted(maps.Keys(arguments)) {
			value, err := inputValueToYAML(arguments[name])
			if err != nil {
				return nil, fmt.Errorf("triggers webhook %q with %q: %w", webhook.GetName(), name, err)
			}
			written = append(written, yaml.MapItem{Key: name, Value: value})
		}
		doc = append(doc, yaml.MapItem{Key: "with", Value: written})
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
		doc = append(doc, yaml.MapItem{Key: "time_zone", Value: textToYAML(zone)})
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
			written = append(written, yaml.MapItem{Key: "comment", Value: textToYAML(comment)})
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

	ds := append(validateWebhookTriggers(wf), validateManualTrigger(wf)...)

	schedule := triggers.GetSchedule()
	if schedule == nil {
		if len(triggers.GetWebhooks()) > 0 || triggers.GetManual() != nil {
			// A block holding webhooks, or saying what a person may do, is complete
			// as it stands.
			return ds
		}

		return append(ds, Diagnostic{
			Field: "triggers",
			Message: "declares no trigger; the kinds are `schedule:`, `webhook:` and `manual:`, or remove " +
				"the block. An empty `triggers:` reads as if this workflow starts on its own, and it does not",
		})
	}

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

// validateManualTrigger reports what is wrong with a `manual:` block.
//
// Two things, and they are different in kind. [v1.CheckManualTrigger] is the
// contradiction — a refusal that also narrows — asked here where there is a line to
// point at, the shape [validateInputDefault] uses and for the same reason: one
// rule, two moments, and the author gets the one with a position.
//
// The second is a fact about the *set*: refusing manual starts in a file that
// declares no other way in leaves a workflow nothing can start. That is not a
// contradiction inside the block, so [v1.CheckManualTrigger] cannot see it — it is
// only visible from the whole `triggers:` block, which is why it is reported here.
// It is a property of the file rather than of a deployment, so it is fair game for
// a validator: whether a webhook's secret exists is a deployment's answer, but
// whether a webhook is *declared* is written on the page.
func validateManualTrigger(wf *v1.Workflow) Diagnostics {
	manual := wf.GetTriggers().GetManual()
	if manual == nil {
		return nil
	}

	if err := v1.CheckManualTrigger(manual); err != nil {
		return Diagnostics{{Field: "triggers.manual", Message: err.Error()}}
	}

	if manual.GetDenied() && len(wf.GetTriggers().GetWebhooks()) == 0 && wf.GetTriggers().GetSchedule() == nil {
		return Diagnostics{{
			Field: "triggers.manual",
			Message: "refuses manual starts, and this workflow declares no other trigger, so nothing " +
				"can start it at all. Declare the source that is meant to — a `- webhook:` or a " +
				"`schedule:` — or remove the refusal. `flow run local` is unaffected either way, because " +
				"a workflow that cannot be developed locally is not one anybody will maintain",
		}}
	}

	return nil
}

// validateWebhookTriggers reports what is wrong with a file's declared webhooks.
//
// This is the diagnostic the whole call-site design was chosen for. `inputs:` is a
// signature and a trigger is a caller, so `with:` can be checked against that
// signature *statically*, in both directions, exactly as a `call:` step's `with:`
// already is (see validateCallAtDepth, whose two loops these two mirror
// deliberately — one construct, one rule, and the sentences differ only in naming
// the caller). A per-trigger mapping block could never buy this: it would state
// what an input is a second time, with nothing forcing the two statements to agree.
//
// Everything reported here is a property of the file. Whether the secret behind a
// `verify:` scheme exists, whether this deployment has that scheme configured, and
// whether the sender can reach us are deployment answers, and nothing here resolves
// a reference or performs any I/O.
func validateWebhookTriggers(wf *v1.Workflow) Diagnostics {
	webhooks := wf.GetTriggers().GetWebhooks()
	if len(webhooks) == 0 {
		return nil
	}

	var ds Diagnostics

	declared := make(map[string]*v1.InputDeclaration, len(wf.GetDeclaredInputs()))
	for _, d := range wf.GetDeclaredInputs() {
		declared[d.GetName()] = d
	}

	seen := make(map[string]int, len(webhooks))
	for i, webhook := range webhooks {
		at := indexPath("triggers", i)
		name := webhook.GetName()

		if err := v1.CheckWebhookName(name); err != nil {
			ds = append(ds, Diagnostic{Field: at, Message: err.Error()})

			// Everything below says `webhook "…"`, which reads as nonsense for a
			// webhook with no name — and the fix is the same edit either way.
			continue
		}

		if first, duplicate := seen[name]; duplicate {
			ds = append(ds, Diagnostic{
				Field: fieldPath(at, "webhook"),
				Message: fmt.Sprintf(
					"webhook %q is already declared by entry %d; each source needs its own name, because a "+
						"diagnostic and a `flow test` case address a webhook by name and would otherwise "+
						"reach whichever came first",
					name, first+1),
			})
		} else {
			seen[name] = i
		}

		if err := v1.CheckWebhookVerify(name, webhook.GetVerify()); err != nil {
			ds = append(ds, Diagnostic{Field: at, Message: err.Error()})
		}
		for _, scheme := range slices.Sorted(maps.Keys(webhook.GetVerify())) {
			if err := v1.CheckWebhookVerifyScheme(name, scheme, webhook.GetVerify()[scheme]); err != nil {
				ds = append(ds, Diagnostic{Field: fieldPath(fieldPath(at, "verify"), scheme), Message: err.Error()})
			}
		}

		if err := v1.CheckWebhookIdempotencyKey(name, webhook.GetIdempotencyKey()); err != nil {
			ds = append(ds, Diagnostic{Field: at, Message: err.Error()})
		} else {
			ds = append(ds, validateTriggerExpr(
				fieldPath(at, "idempotency_key"), name, "idempotency_key", webhook.GetIdempotencyKey())...)
		}

		// An argument for a name the workflow does not declare, which is almost
		// always a rename in one place and not the other. Read as "extra data
		// nobody minds" it would go unnoticed until whoever reads the workflow
		// wonders why its own `${inputs.foo}` is never bound.
		for _, argument := range slices.Sorted(maps.Keys(webhook.GetArguments())) {
			field := fieldPath(fieldPath(at, "with"), argument)

			if _, ok := declared[argument]; !ok {
				ds = append(ds, Diagnostic{
					Field: field,
					Message: fmt.Sprintf(
						"webhook %q binds %q, which this workflow declares no input named; the inputs it "+
							"takes are %s",
						name, argument, declaredInputNameList(wf)),
				})
			}

			ds = append(ds, validateTriggerExpr(field, name, "with."+argument, webhook.GetArguments()[argument])...)
		}

		// The other direction, and the sentence #491 names: a required input with
		// no default that this call site never binds. A run started by this source
		// would be refused by [v1.BindRunInputs] at submit, which is a refusal
		// nobody is present for — a delivery arrives at three in the morning — so
		// it is worth an author's editor saying it now.
		for _, d := range wf.GetDeclaredInputs() {
			if !d.GetRequired() || d.GetDefault() != nil {
				continue
			}
			if _, bound := webhook.GetArguments()[d.GetName()]; bound {
				continue
			}

			field := at
			if len(webhook.GetArguments()) > 0 {
				field = fieldPath(at, "with")
			}

			ds = append(ds, Diagnostic{
				Field: field,
				Message: fmt.Sprintf(
					"webhook %q does not supply required input %q; bind it under this webhook's `with:`, "+
						"mapping it out of `%s` — or give the input a `default:`",
					name, d.GetName(), v1.EventRoot),
			})
		}
	}

	return ds
}

// validateTriggerExpr reports a reference a trigger's expression cannot resolve.
//
// A trigger is evaluated before there is a run: no step has produced anything, no
// `vars:` block has been evaluated, and `inputs:` is the thing being *computed*
// here rather than something to read. So exactly one name is in scope, `event`, and
// every other reference is reported — with the scope named, because an author
// reaching for `${inputs.order_id}` in a trigger has a coherent mental model that
// is one step out of order rather than a typo.
//
// The reverse rule — `event` outside a trigger — is reported by [validateInputRefs],
// where every other out-of-place binding is, so that the two halves of one name's
// scope are as close together in the tool's answers as they are in the language.
func validateTriggerExpr(field, webhook, what string, value *v1.Value) Diagnostics {
	parsed := value.GetExpr()
	if parsed == nil {
		// A literal argument — `amount: 0` — references nothing. A `verify:` entry's
		// secret reference does not reach here at all: that is not an argument.
		return nil
	}

	rooted, vars, inputs, run, trigger, bare := referencedIdentifiers(parsed)

	unresolvable := make([]string, 0, len(rooted)+len(vars)+len(inputs)+len(run)+len(trigger)+len(bare))
	for _, ref := range rooted {
		unresolvable = append(unresolvable, v1.StepsRoot+"."+ref.ID)
	}
	for _, ref := range vars {
		unresolvable = append(unresolvable, v1.VarsRoot+"."+ref)
	}
	for _, ref := range inputs {
		unresolvable = append(unresolvable, v1.InputsRoot+"."+ref)
	}
	for _, ref := range run {
		unresolvable = append(unresolvable, v1.RunRoot+"."+ref.Field)
	}
	// `trigger` is unresolvable here for a reason worth being precise about: it is
	// not that a trigger has no context, it is that this expression *is* the
	// trigger. The context is fixed at the moment this mapping produces a run, so
	// reading it while computing that run's arguments would be a value asking about
	// itself — and every field of it is already known to whoever wrote this entry,
	// because they are looking at the trigger it describes.
	for _, ref := range trigger {
		unresolvable = append(unresolvable, v1.TriggerRoot+"."+ref)
	}
	for _, ref := range bare {
		if ref == v1.EventRoot || functionNamespaces[ref] {
			// The one name a trigger binds, and the qualifier of a namespaced
			// function from the profile — `regex.replace(...)` — which cel-go parses
			// as an identifier and which is not a reference to anything.
			continue
		}
		unresolvable = append(unresolvable, ref)
	}

	if len(unresolvable) == 0 {
		return nil
	}

	slices.Sort(unresolvable)

	return Diagnostics{{
		Field: field,
		Value: unresolvable[0],
		Message: fmt.Sprintf(
			"webhook %q reads %s in `%s`, which a trigger cannot: a trigger is evaluated before the run "+
				"exists, so the only name in scope is `%s` — the delivery, as `%s.%s` and `%s.%s`. "+
				"Bind what the run needs under `with:` and read it as `%s.<name>` in the steps",
			webhook, quotedNameList(unresolvable), what,
			v1.EventRoot, v1.EventRoot, v1.EventHeadersField, v1.EventRoot, v1.EventBodyField, v1.InputsRoot),
		Code: v1.DiagnosticCodeUnresolvedReference,
	}}
}

// quotedNameList renders the names one diagnostic is about, quoted, so a reference
// holding a space or a dot is legible in the sentence it appears in.
func quotedNameList(names []string) string {
	quoted := make([]string, 0, len(names))
	for _, name := range names {
		quoted = append(quoted, strconv.Quote(name))
	}

	return strings.Join(quoted, ", ")
}
