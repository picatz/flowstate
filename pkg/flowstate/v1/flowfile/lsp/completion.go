package lsp

import (
	"fmt"
	"slices"
	"strings"

	v1 "github.com/picatz/flowstate/pkg/flowstate/v1"
	"github.com/picatz/flowstate/pkg/flowstate/v1/celcomplete"
	"github.com/picatz/flowstate/pkg/flowstate/v1/flowfile"
	"github.com/sourcegraph/go-lsp"
)

// Completion is where an editor either feels like it understands the language or
// like it is guessing. Every candidate here comes from the task registry, the
// Protobuf descriptors, or the evaluator's library set, so a suggestion the author
// accepts is always something the engine accepts too.
//
// The scoping that matters most is inside ${...}: only steps declared earlier in
// the document may be referenced, because a step's outputs do not exist until it
// has run. Offering a later step would be offering a workflow that fails.
//
// Inside ${...} there are two namespaces rather than one ordered list. A step is
// reached through the root — `steps.<id>.<output>` — and a name bound where the
// cursor stands is written bare: a loop's iterator, `now` inside a wait.
// Which one a position is in is decided by what has been typed, so a menu never
// mixes them: offering a step id bare, or a loop's item under the root, is
// offering a reference the engine cannot resolve.

// A dslKey is one key of the Flowfile document shape.
type dslKey struct {
	name   string
	detail string
	// docs is the hover documentation, which is also shown as completion detail.
	docs string
}

// oneStepKind names every kind of work a step can be, derived from the flowfile
// package rather than written out here.
//
// Written out, this sentence said "One of `task`, `for_each`, and `parallel`" for
// as long as waiting had existed — a closed enumeration, stated with confidence, of
// half the language. An author who typed `sleep:` got no completion, no hover, and
// this sentence telling them the key was not one of the choices, while the parser,
// the engine, both drivers, and two examples in CI all accepted it.
//
// Deriving it means the next kind added to the DSL cannot leave this describing the
// language as it used to be. It is the same reasoning `stepKindList` was written
// with on the diagnostics side; this package simply had no way to reach it.
var oneStepKind = "A step does exactly one of " + flowfile.StepKindList() + "."

// editionList names the grammar versions this build compiles, derived from the
// flowfile package for the same reason oneStepKind is: a version number copied
// into a sentence here is a version number that will eventually describe a
// grammar this build does not have.
//
// Rendered through [flowfile.KnownEditionsList] rather than joining
// [flowfile.KnownEditions] directly: the compiler's own diagnostics quote each
// edition, and a hover that joined the raw names would spell the same set a
// second way — one bare, one the compiler's error text quotes — for an author
// who compares the two (#385).
var editionList = flowfile.KnownEditionsList()

// dslKeys are the keys the Flowfile document shape defines, as opposed to those a
// task's schema defines.
//
// This is the only list in the package not derived from a central definition,
// because the document shape lives in unexported structs in the flowfile package.
// It is also the only list that has already drifted: the DSL gained `if`,
// `timeout`, `retry`, and `continue_on_error` after this package was first written,
// and nothing failed to tell us. TestDSLKeysMatchTheDSL closes that gap by deriving
// the real key set from flowfile.Marshal, and the report accompanying this package
// proposes exporting the shape so the table can go away entirely.
//
// # The prose stays hand-written, and that is a decision rather than an omission
//
// The obvious next step is to derive these strings from the schema, which does
// document every field it has. The reason not to is narrower than it first looks,
// and worth stating exactly, because the wide version of the argument does not
// survive checking: the schema's prose is *good*, and mostly says what the hover
// says. It gives `iterator`'s default and its collision rule, both of which an
// earlier draft of this comment claimed were unique to the hover. They are not.
//
// What does not survive derivation is the name. The schema documents `iterator`,
// and an author writes `as:`. A hover built from the field would call the key by a
// name the parser rejects, and would do it on the surface whose whole job is to
// tell somebody what to type. The same holds for the reference spelling: the schema
// has no reason to mention `${name}`, because a reader of the schema is not writing
// a reference.
//
// So: a key whose DSL spelling matches its field name could take the schema's text
// today and lose nothing. Deriving them all could not, and the machinery to tell
// those two cases apart is larger than the table.
//
// What is worth deriving is the key *set* rather than the sentences, and
// TestDSLKeysMatchTheDSL does that — with one bound worth knowing. It compares
// against a document rendered by flowfile.Marshal from a hand-built workflow, so it
// sees a key only if the fixture populates the field behind it. A field added
// without touching that fixture is invisible to both directions of the comparison;
// the test says so itself, having once been green while the three keys that spell a
// wait were missing. TestHoverDocumentsEveryDSLKey covers the other way, an entry
// nothing shows.
var dslKeys = map[string][]dslKey{
	"": {
		{name: "edition", detail: "version", docs: "Required. Names the grammar this file is written in, as a v-prefixed date: `" + flowfile.CurrentEdition + "`.\n\n" +
			"It exists so that a build can *refuse* a file rather than silently reinterpret it: surface syntax here gets no deprecation window, " +
			"and a file that says which grammar it was written in is a file `flow fix` can rewrite across that boundary. " +
			"It was optional once, which was the one thing it could not afford to be: leaving it out did not mean \"any grammar\", it meant being read as whichever grammar came next.\n\n" +
			"`flow fix` writes it, below any header comment, so no one has to type it.\n\n" +
			"It is not a compatibility switch. A build compiles one grammar (this one knows " + editionList + "), and declaring anything else is refused, not translated."},
		{name: "name", detail: "string", docs: "What this workflow is called."},
		{name: "description", detail: "string", docs: "Optional prose about the workflow."},
		{name: "vars", detail: "map", docs: "Names values once, for the whole file. Every step reads them as `${" + v1.VarsRoot + ".<name>}`.\n\n" +
			"Rooted rather than bare because a var is *ambient*: it is in scope everywhere rather than bound where you read it, the same distinction that makes a step's outputs `" + v1.StepsRoot + ".<id>.<output>` and a loop's binding bare.\n\n" +
			"Evaluated once, before the first step runs. A var may therefore use literals, operators and the profile's functions, and may not read a step, another var, or anything else that does not exist yet. " +
			"The `${...}` fence is still required for an expression: without it the value is the text as written, which is what lets a var hold the literal string `steps.greet.result`.\n\n" +
			"A `${secret(...)}` reference may not be stored here: a var is evaluated by the workflow and its value is written to durable history. Write the reference on the task input that consumes the secret instead."},
		{name: "steps", detail: "list", docs: "The steps to run, in order. Each step may reference the outputs of the steps before it."},
	},
	"steps": {
		{name: "id", detail: "string", docs: "How later steps reference this one, as `${" + v1.StepsRoot + ".<id>.<output>}`. Must be a valid CEL identifier and unique in the workflow."},
		{name: "description", detail: "string", docs: "Optional prose about this step: why it is here, which the mechanics under it cannot say.\n\n" +
			"A property of the step rather than of the work it does, so every kind of step can carry one, a `for_each` or a `sleep` as readily as a task. " +
			"It belongs here, directly under `id`, and not under the task's own key: the keys there are that task's inputs, so a `description` written among them asks for an input by that name."},
		{name: "for_each", detail: "map", docs: "Repeat a body of steps once per item of a list. " + oneStepKind},
		{name: "loop", detail: "map", docs: "Repeat a body of steps, carrying a value from one iteration to the next, until a condition holds or `max_iterations:` is reached. " + oneStepKind + "\n\n" +
			"The body runs, then `until:` is checked, so `until:` reads what the body produced, which is what lets a loop page a cursor until the last page reports it is done. " +
			"`as:` names the carried value, `init:` is what it holds first, `update:` computes the next from the body's outputs. " +
			"Reaching `max_iterations:` without `until:` holding is a distinct failure, not a silent stop. Reports `results` (one entry per iteration) and, when it carries state, `state` (the final value)."},
		{name: "parallel", detail: "list", docs: "Run branches of steps concurrently. " + oneStepKind},
		{name: "sleep", detail: "duration", docs: "Wait for a duration on a durable timer, written as `30s`, `5m`, `1h`, or `7d`. " +
			"The run holds nothing while it waits, so a week is as cheap as a second. " + oneStepKind},
		{name: "wait_until", detail: "expression", docs: "Wait until a moment, written as `${...}` producing an RFC 3339 time. " +
			"Inside it, `now` is the moment the wait is evaluated and `seconds`/`minutes`/`hours`/`days`/`weeks` build durations, " +
			"so a deadline reads as `${now + days(3)}`. " + oneStepKind},
		{name: "wait_for_signal", detail: "string or map", docs: "Wait for a named signal, which is how a human approval reaches a workload. " +
			"Write `wait_for_signal: deploy-approved`, or a mapping with `name:` and `timeout:`. " +
			"What the sender sent becomes this step's outputs. " + oneStepKind},
		{name: "call", detail: "string", docs: "Run another Flowfile as a step, resolved relative to *this file's* own directory at compile time. " + oneStepKind + "\n\n" +
			"The callee runs isolated: its steps see only its bound arguments (`with:`) and the profile, not this file's steps or `vars:`. " +
			"What it declares in its own `outputs:` comes back under this step's id, the way a task's would."},
		{name: "value", detail: "expression", docs: "Names a computed value, so a fact the file states once can be read everywhere it matters. " + oneStepKind + "\n\n" +
			"```yaml\n- id: cleared_to_move\n  value: ${" + v1.InputsRoot + ".amount_cents < " + v1.InputsRoot + ".approval_threshold_cents || " +
			v1.StepsRoot + ".approval.outcome == \"approved\"}\n```\n\n" +
			"Read as `${" + v1.StepsRoot + ".<id>." + v1.ValueOutput + "}`: an ordinary named output called `" + v1.ValueOutput + "`, not a special whole-step form, so every tool that reads outputs reads this one the same way. " +
			"Negation is `${!" + v1.StepsRoot + ".cleared_to_move." + v1.ValueOutput + "}`: one spelling of the fact and one `!`, instead of a hand-expanded complement that can drift from the thing it negates.\n\n" +
			"Evaluated in the workflow, in written order, against exactly what a task's inputs written in the same place would see: `" + v1.VarsRoot + ".<name>`, `" + v1.InputsRoot + ".<name>`, the outputs of steps already run, and any enclosing binding. " +
			"The result may be anything an output can hold, a list or a mapping as readily as a boolean.\n\n" +
			"It is not a task and schedules nothing, so `retry:`, `timeout:` and `undo:` are refused on it: a pure expression has nothing to attempt again, nothing to bound beyond the cost limit every expression shares, and no effect to take back. " +
			"An `if:` composes as it does anywhere; a value that is skipped produces no outputs, and a later reference to it does not resolve.\n\n" +
			"A `${secret(...)}` reference may not be written here, for the reason it may not go in `vars:`: the workflow evaluates this, and what the workflow evaluates is written to durable history."},
		{name: "switch", detail: "map", docs: "Dispatch on one value: literal cases tried in written order, first match wins, no fallthrough. " + oneStepKind + "\n\n" +
			"```yaml\n- id: route\n  switch:\n    value: ${" + v1.StepsRoot + ".approval.outcome}\n    cases:\n      - case: deployed\n        steps: [...]\n      - case: [rejected, withdrawn]\n        steps: [...]\n    default:\n      steps: [...]\n```\n\n" +
			"Cases are literals; a computed comparison is what `if:` is for. `default:` runs when no case matches and means \"a value arrived I didn't enumerate\", never \"the value couldn't be computed\" — an unresolvable `value:` fails the step. " +
			"The step records what it observed: `" + v1.SwitchValueOutput + "` (the evaluated discriminant) and `" + v1.SwitchCaseOutput + "` (the case literal that matched, `null` when none did), so an unmatched value with no `default:` runs nothing and says so. " +
			"An empty body (`steps: []`) is written-down ignoring. Exactly one body runs; its step outputs merge into the enclosing scope like a parallel branch's."},
		{name: "if", detail: "expression", docs: "A condition deciding whether the step runs, written as `${...}`. A step that is skipped produces no outputs."},
		{name: "vars", detail: "map", docs: "Names values for this step, read *bare*: `${modified}`.\n\n" +
			"Bare rather than rooted because these are author-chosen and lexically local (the same standing as the name a loop binds), where the workflow's `vars:` are ambient and so are rooted. " +
			"They are private to the step: on a `for_each` or `parallel:` they reach the whole body, and nowhere else. Pass a value to a *later* step through its outputs instead.\n\n" +
			"A name already bound by an enclosing loop or step is refused rather than shadowed, and a var may not read its siblings: `vars:` is a mapping, so there is no order that would make one available to another. " +
			"Everything else in scope is fair: `" + v1.VarsRoot + ".<name>`, the outputs of steps already run, and any enclosing binding.\n\n" +
			"A `${secret(...)}` reference may not be stored here either, for the same reason it may not go in the workflow's own `vars:`. Write it on the task input that consumes the secret."},
		{name: "timeout", detail: "duration", docs: "Bounds one attempt at the step, written as `30s`, `5m`, or `1h`.\n\n" +
			"Only a task step schedules the one activity this bounds. `for_each:`, `parallel:`, `call:`, `loop:`, `switch:`, `sleep:`, " +
			"`wait_until:`, `wait_for_signal:` and `value:` are refused: each either schedules zero or more of something else, or nothing " +
			"at all, so put `timeout:` on the steps inside the body that need it instead."},
		{name: "total_timeout", detail: "duration", docs: "Bounds the step across *every* attempt and every wait between them, written as `30s`, `5m`, or `1h`: the wall-clock budget for the whole retried step, where `timeout:` bounds one attempt inside it.\n\n" +
			"Write it when the deadline is what matters — an SLA is ten minutes however many tries that is, not an attempt count computed backwards from a backoff curve. A step declaring none takes the engine's default overall budget, so a step is bounded across its attempts either way; this key is how an author says by how much.\n\n" +
			"A declared value is honoured exactly. The engine widens its default overall budget to fit `timeout:` multiplied by the attempts allowed, and writing this key suppresses that widening — a budget the engine silently extends is not a budget. It may not be shorter than `timeout:`, since one that expires inside the first attempt allows none.\n\n" +
			"Refused on the same step kinds `timeout:` is refused on, for the same reason: they schedule no single activity for either clock to bound."},
		{name: "retry", detail: "map", docs: "How a failed attempt is retried. Omit it to use the engine's defaults.\n\n" +
			"Only a task step schedules the one activity this re-runs. `for_each:`, `parallel:`, `call:`, `loop:`, `switch:`, `sleep:`, " +
			"`wait_until:`, `wait_for_signal:` and `value:` are refused for the same reason `timeout:` is: put `retry:` on the steps inside " +
			"the body that need it instead."},
		{name: "continue_on_error", detail: "bool", docs: "Let the run proceed when this step fails. A cancellation is not a failure, so this does not tolerate one."},
		{name: "undo", detail: "map", docs: "How this step is taken back when a *later* step fails and the run cannot continue: the saga compensation for what it did.\n\n" +
			"Written as the task that undoes it, with its inputs beneath: the same shape as the step's own work, because it is the same kind of thing. " +
			"A compensation is one task: it cannot loop, branch, wait, or carry an `undo:` of its own.\n\n" +
			"Registered when this step *succeeds*, and never otherwise: a step skipped by its `if:` did nothing, and a step that failed may have applied part of its effect, which the engine will not guess at. " +
			"Compensations then run in reverse order, because steps depend forwards.\n\n" +
			"Its inputs are resolved the moment the step succeeds, in that step's scope with its own outputs added, so `${" + v1.StepsRoot + ".<this step>.<output>}` is the reference to use, and it is the one place a step may name itself. " +
			"A run that failed and compensated still reports FAILED; what it undid is in the failure.\n\n" +
			"Top-level task steps only in this version. Inside a `for_each` body or a `parallel:` branch it is refused, because the order work registers in there is not the same under `flow run local` as it is durably."},
		{name: "with", detail: "map", docs: "Arguments binding the callee's declared `inputs:`, resolved in *this* file's scope, the same scope a task's inputs are resolved in. " +
			"Only meaningful beside `call:`. Checked against what the callee declares when this file is compiled: a missing required input or an argument it does not declare is refused here, not at run time.\n\n" +
			"A secret reference may not be bound through `with:`. Pass it to the task that needs it inside the callee instead."},
	},
	"wait_for_signal": {
		{name: "name", detail: "string", docs: "The signal this step waits for, and what a sender addresses with `flow signal <workflow-id> <name>`."},
		{name: "timeout", detail: "duration", docs: "Bounds the wait. A gate that lapses is not a failure: the step produces `timed_out: true` and the run carries on, " +
			"so an author branches on it with `if: ${!" + v1.StepsRoot + ".approval.timed_out}`. Omit it to wait indefinitely."},
		{name: "prompt", detail: "string or expression", docs: "What this gate is asking for, carried to whoever is being asked.\n\n" +
			"A signal name is a routing key: `deploy-approved` says what to send and nothing about what agreeing means. " +
			"This is the sentence, and it rides along on every surface that reports a parked gate - so an operator holding a run id " +
			"learns the question rather than only the name.\n\n" +
			"```yaml\nprompt: ${\"deploy %s to %s?\".format([" + v1.InputsRoot + ".version, " + v1.InputsRoot + ".environment])}\n```\n\n" +
			"Evaluated once, the moment the wait *parks*, with `" + v1.NowIdentifier + "` bound over the ordinary scope - the same names `timeout:` sees. " +
			"Not `" + v1.PayloadOutput + "`, `" + v1.SenderOutput + "` or `" + v1.TimedOutOutput + "`: those are the wait's result, and the result does not exist yet when the question is asked. " +
			"A gate released by a signal that arrived early never parks, so it asks nothing.\n\n" +
			"It may not reach an input declared `sensitive:`, by any spelling, and may not hold a `${secret(...)}` reference - " +
			"a prompt is rendered to somebody who was handed a run id rather than this file, so the rule is reaching rather than surfacing. " +
			"The evaluated text is bounded, and a prompt that was cut says so rather than looking complete."},
		{name: "outputs", detail: "map", docs: "Shapes what this step produces, the way the http task's own `outputs:` shapes a response.\n\n" +
			"Each value is evaluated once, the moment the wait resolves, with the wait's result bound bare (`" +
			v1.PayloadOutput + "`, `" + v1.SenderOutput + "`, `" + v1.TimedOutOutput + "`, and `" + v1.NowIdentifier +
			"`) over the ordinary scope, so `" + v1.StepsRoot + ".*` and `" + v1.InputsRoot + ".*` read here as they do in an `if:`:\n\n" +
			"```yaml\noutputs:\n  approved: ${has(" + v1.PayloadOutput + ".approved) && " + v1.PayloadOutput + ".approved}\n  " +
			v1.TimedOutOutput + ": ${" + v1.TimedOutOutput + "}\n```\n\n" +
			"It **replaces** the wait's own outputs rather than adding to them, exactly as the http task's does, so `" +
			v1.PayloadOutput + "` and `" + v1.SenderOutput + "` are gone unless a line re-exposes them, and reading one that was dropped is a `flow validate` error rather than an empty value.\n\n" +
			"The point is to state a gate once: later `if:`s and the workflow's `outputs:` read `${" + v1.StepsRoot +
			".approval.approved}` instead of each re-deriving the condition, so the report cannot disagree with the branch that ran."},
	},
	"for_each": {
		{name: "items", detail: "expression", docs: "An expression producing the list to iterate, written as `${...}`."},
		{name: "as", detail: "string", docs: "Names the variable bound to the current item, read bare inside the body: `${name}`. Defaults to `item`.\n\n" +
			"Reads as the sentence it is (*for each item as name*) and names the binding rather than the mechanism. It was `iterator:`; `flow fix` rewrites that."},
		{name: "max_parallel", detail: "int", docs: "How many iterations may run at once. Omitted, `0` or `1` runs them one at a time.\n\n" +
			"Zero is accepted rather than refused because it is the field's own zero value: a spec built " +
			"without it and one that sets it to nothing mean the same thing, and the schema says so with `gte: 0`."},
		{name: "steps", detail: "list", docs: "The body run once per item."},
	},
	"loop": {
		{name: "as", detail: "string", docs: "Names the value carried between iterations, read bare inside the body, `until:` and `update:`: `${cursor}`.\n\n" +
			"The same standing as a `for_each` binding: author-chosen and lexically local. Optional: a loop that carries nothing omits `as:`, `init:` and `update:` together. A name that collides with an enclosing binding, `now`, or a root is refused."},
		{name: "init", detail: "expression", docs: "The value the carried state holds on the first iteration, written as `${...}`. Evaluated once before the loop, so it cannot read the state it defines. Required when `as:` is set."},
		{name: "update", detail: "expression", docs: "Computes the next iteration's carried state from the current one and the body's outputs, written as `${...}`: `${" + v1.StepsRoot + ".page.next_cursor}` or `${acc + n}`. Evaluated after the body. Required when `as:` is set."},
		{name: "until", detail: "expression", docs: "The stop condition, written as `${...}` producing a boolean. Evaluated after the body each iteration, so it reads the body's own outputs. When it holds, the loop stops."},
		{name: "max_iterations", detail: "int", docs: "The hard ceiling on how many times the body runs. Omitted or `0` uses the engine's default. Reaching it without `until:` holding fails the run distinctly rather than stopping silently: a loop that could run forever is one the engine must be able to stop."},
		{name: "steps", detail: "list", docs: "The body run each iteration."},
	},
	"parallel": {
		{name: "steps", detail: "list", docs: "One branch's steps. Each `- steps:` entry is a branch that runs concurrently with the others."},
	},
	"switch": {
		{name: "value", detail: "expression", docs: "The discriminant: evaluated exactly once, then matched against the cases in written order. " +
			"A value that cannot be evaluated — a skipped step's output, say — fails the step rather than flowing into `default:`."},
		{name: "cases", detail: "list", docs: "The dispatch arms. Each entry is `case:` (a literal, or a list of literals sharing one body — `case: [rejected, withdrawn]`) and `steps:`. " +
			"First match wins, no fallthrough. Cases are literals; a computed comparison is what `if:` is for."},
		{name: "default", detail: "map", docs: "The body that runs when no case matches: `steps:` alone. Optional — without it, an unmatched value runs nothing and the step's outputs record that none matched. " +
			"Where the value's domain is checkable, no `default:` claims the cases are exhaustive and `flow validate` verifies the claim; `default: {steps: []}` is how deliberately handling nothing else is written down."},
	},
	"cases": {
		{name: "case", detail: "literal or list", docs: "The literal (or list of literals) this body handles, matched by equality. Static values only: cases are literals; a computed comparison is what `if:` is for."},
		{name: "steps", detail: "list", docs: "The body to run when this case matches. `steps: []` is legal: ignoring a value, written down where a reviewer can see it."},
	},
	"default": {
		{name: "steps", detail: "list", docs: "The body to run when no case matches. `steps: []` is legal: deliberately handling nothing else, written down."},
	},
	"retry": {
		{name: "attempts", detail: "int", docs: "Total attempts including the first, so `1` disables retrying."},
		{name: "interval", detail: "duration", docs: "The delay before the second attempt."},
		{name: "backoff", detail: "double", docs: "Multiplies the delay after each attempt."},
		{name: "max_interval", detail: "duration", docs: "Caps the delay between attempts."},
	},
}

// lookupDSLKey returns the documentation for a document-shape key at one level of
// nesting.
func lookupDSLKey(level, name string) (dslKey, bool) {
	for _, k := range dslKeys[level] {
		if k.name == name {
			return k, true
		}
	}
	return dslKey{}, false
}

// completeAt returns the completion candidates for a position.
//
// It reads the document by line rather than from the parsed model, because a
// document is usually mid-edit and therefore invalid at exactly the moment
// completion is requested.
func completeAt(doc *document, pos lsp.Position) *lsp.CompletionList {
	if !doc.speaksFlowfile() {
		return &lsp.CompletionList{Items: []lsp.CompletionItem{}} // see [document.speaksFlowfile]
	}
	// See [clampPosition]: a coordinate the protocol cannot express is brought
	// to the origin here, so that every range this answer carries is one the
	// client can apply.
	pos = clampPosition(pos)

	empty := &lsp.CompletionList{IsIncomplete: false, Items: []lsp.CompletionItem{}}
	if doc.tooLarge {
		return empty
	}

	line := doc.index.line(pos.Line)
	col := doc.index.byteOfUTF16(pos.Line, pos.Character)
	before := line[:min(col, len(line))]

	steps := scanOutline(doc.index, doc.tasks)
	current, earlier := stepScope(steps, pos.Line)
	path := keyPath(doc.index, pos.Line)
	key, valuePos := keyAndPosition(line, col)

	// Inside ${...} nothing else applies: the cursor is in an expression, not in
	// YAML structure.
	if inner, ok := openExpression(before); ok {
		scope := referenceScope(doc, pos, bindsClock(key, path), current, earlier)
		scope.locals = append(waitResultCandidates(path), scope.locals...)
		return completeInExpression(pos, inner, scope)
	}

	word, replace := wordBefore(pos, before)

	if valuePos {
		// Nothing is offered in a value position any more. `libs:` was the only key
		// whose values came from a closed set this package knew; every other value in
		// a Flowfile is a URL, a duration, a message, or an expression, and guessing
		// at those is how an editor starts getting in the way.
		return empty
	}

	// The cursor is where a key goes.
	switch {
	case endsWith(path, "steps", "with"):
		// A call's arguments, which are the callee's declared inputs and live in
		// another file. Both keys of the path are checked rather than the last
		// one alone: `with:` is a step key, so the level is what separates it
		// from a task input a plugin happens to have called the same thing.
		return list(callArgumentCandidates(doc, current, word, replace))
	case insideATask(path, doc.tasks):
		// The keys under a task's own name are its inputs, which come from its
		// schema rather than from this package's table.
		return list(inputCandidates(word, replace, current, doc.tasks))
	case endsWith(path, "retry"):
		return list(dslCandidates("retry", word, replace))
	case endsWith(path, "for_each"):
		return list(dslCandidates("for_each", word, replace))
	case endsWith(path, "loop"):
		return list(dslCandidates("loop", word, replace))
	case endsWith(path, "parallel"):
		return list(dslCandidates("parallel", word, replace))
	case endsWith(path, "switch", "cases"):
		return list(dslCandidates("cases", word, replace))
	case endsWith(path, "switch", "default"):
		return list(dslCandidates("default", word, replace))
	case endsWith(path, "switch"):
		return list(dslCandidates("switch", word, replace))
	case endsWith(path, "wait_for_signal"):
		return list(dslCandidates("wait_for_signal", word, replace))
	case endsWith(path, "steps"):
		// A step key is a property or a task name, and from where the cursor is
		// they are the same kind of thing: both are ways to finish this line. The
		// registry supplies one half and the table the other, which is why a task
		// added to the registry becomes completable with no change here.
		//
		// stepOwningKeyAt rather than current: current is stepScope's innermost
		// containing step, which on a sibling line after a for_each/loop/parallel/
		// switch body is the last nested step inside that body — not the
		// composite this key actually belongs to. See that function's doc.
		owner := stepOwningKeyAt(doc.index, steps, pos.Line)
		return list(append(stepKeyCandidates(owner, word, replace), taskCandidates(word, replace, doc.tasks)...))
	case len(path) == 0:
		return list(dslCandidates("", word, replace))
	}
	return empty
}

// insideATask reports whether a key path ends inside a task's inputs.
//
// The task's own name is the innermost path element, so this asks the registry
// rather than matching a literal.
//
// Deliberately a narrower question than flowfile.StepTaskKeys, which the model
// and the compiler ask: they must recognise an *unregistered* name so that
// "unknown task" has a token to land on, whereas there is nothing to complete
// under a task with no schema. Suggesting the enclosing level's keys there would
// offer `id:` as an input.
func insideATask(path []string, tasks *v1.Registry) bool {
	if len(path) == 0 {
		return false
	}
	_, known := tasks.Lookup(path[len(path)-1])
	return known
}

// stepScope returns the step containing a line and the steps declared before it.
func stepScope(steps []*outlineStep, line0 int) (current *outlineStep, earlier []*outlineStep) {
	for _, s := range steps {
		if s.containsLine(line0) {
			current = s
			break
		}
	}
	if current == nil {
		// Not inside a step: every step declared above the cursor is in scope.
		for _, s := range steps {
			if s.endLine < line0 {
				earlier = append(earlier, s)
			}
		}
		return nil, earlier
	}
	return current, steps[:current.index]
}

// openExpression returns the expression source between the last unclosed `${` and
// the cursor.
//
// The rule is [flowfile.OpenFence], the compiler's own scanner, and not a search
// for the last `${` — which was wrong twice over. It saw a fence in `$${`, the
// escape for a literal `${`, so completing `write $${inputs.ver` offered inputs
// and steps inside text the author means a reader to see. And it decided a fence
// was closed by looking for any `}`, which CEL puts inside maps, string literals
// and comments, so `${ {'k': 1}` read as closed and offered nothing.
func openExpression(before string) (string, bool) {
	return flowfile.OpenFence(before)
}

// A refScope is what an expression at the cursor may name, in the two namespaces
// the grammar keeps apart.
//
// It mirrors the refScope flowfile's validator carries, deliberately: the editor
// offering a name the compiler would refuse is the one failure this package must
// not have, and a scope shaped like the compiler's cannot drift into one that
// merges them again.
//
// The three lists become a [celcomplete.Scope] on the way out ([refScope.shared]),
// which is where the rules about them live: a candidate is that package's type
// already, so this is a builder for one scope rather than a second model of a
// name. What stays here is the *document's* half — which names a parsed Flowfile
// puts in each list, and which roots an editor offers — because those are answers
// about a file being typed rather than about the language.
type refScope struct {
	// steps are the steps whose outputs exist at this point, offered after
	// `steps.` and never bare.
	steps []celcomplete.Candidate

	// locals are the names bound bare where the cursor is: a loop's iterator, a
	// step's own `vars:` keys, and `now` where a wait binds it. Offered bare and
	// never after the root.
	locals []celcomplete.Candidate

	// vars are the workflow's declared variables, offered after `vars.` and never
	// bare — the crossing [v1.Scope.Activation] describes, met from the editor's
	// side: the rooted block is `vars.<name>` and the bare one is a step's.
	//
	// They are the file's rather than a step's, so they are the same list wherever
	// the cursor is. Held separately from steps for the same reason the engine holds
	// two namespaces: a step called `region` and a var called `region` are different
	// things and completing one as the other is the mistake this package exists to
	// avoid.
	vars []celcomplete.Candidate
}

// referenceScope returns the names an expression at pos may reference.
//
// It prefers the parsed model, which knows the engine's scoping rules: a loop
// body's outputs do not escape the loop, a parallel branch cannot see a sibling's,
// and a loop binds an iterator inside its body. Those rules are the whole reason
// this is not simply "every step above the cursor" — offering a name that cannot
// resolve is worse than offering nothing.
//
// It falls back to document order from the line scan when the document does not
// parse, which is rarer than it sounds: `message: ${` is valid YAML, so the model
// is usually available at exactly the moment completion is asked for.
//
// clock reports whether the expression at the cursor is one the engine binds the
// moment to, which is the only thing that puts `now` in scope.
func referenceScope(doc *document, pos lsp.Position, clock bool, current *outlineStep, earlier []*outlineStep) refScope {
	currentIndent := 0
	if current != nil {
		currentIndent = current.indent
	}

	var scope refScope
	if from := stepAtIfParsed(doc, pos); from != nil {
		scope = scopeFromModel(doc, from, from.loopScopeAt(pos))
	} else {
		scope = scopeFromOutline(earlier, currentIndent, doc.tasks)
	}

	// The workflow's own vars, which belong to the file rather than to a step and
	// so are the same wherever the cursor is — including the outline fallback,
	// where the model is unavailable but this block usually still parsed.
	if doc.parsed != nil {
		scope.vars = varsCandidates(doc.parsed.varsEntry, "a variable declared by the workflow")
	}

	if clock {
		// Bound by the engine for a wait's expressions and nowhere else, which is
		// why it is added here rather than living in the scope every expression gets. A task
		// input has no clock that survives a retry, and the validator says so with
		// a diagnostic; offering the name there would be walking an author into it.
		scope.locals = append(scope.locals, celcomplete.Candidate{
			Name:   v1.NowIdentifier,
			Kind:   celcomplete.KindValue,
			Detail: "timestamp",
			// The same text hover shows, for the same reason a CEL library's is
			// shared: accepting a candidate and then hovering what was accepted
			// must not produce two accounts of one name.
			Docs: nowDoc(),
		})
	}
	return scope
}

// waitUntilKey, sleepKey and signalTimeoutKey are the three keys whose
// expressions the engine evaluates as a wait's, and so binds the clock into.
//
// Written once, next to the things that read them, [bindsClock] and nowDoc,
// because each is the join of two facts that live apart: v1 owns the name `now`
// and the DSL owns the keys. The keys themselves are checked against the grammar
// by TestDSLKeysMatchTheDSL, which is what keeps a renamed key from leaving
// these pointing at nothing.
const (
	waitUntilKey     = "wait_until"
	sleepKey         = "sleep"
	signalTimeoutKey = "timeout"
)

// bindsClock reports whether an expression written after a key is a wait's, and
// so has `now` bound in it.
//
// Both halves are needed. The key says which kind of value is being written, and
// the path says at which level — a `wait_until:` directly under a step is the
// wait the grammar defines, while the same word among a task's inputs would be
// that task's input and is resolved somewhere with no clock in it. Asking only the
// key would offer `now` there, and a candidate an author accepts in a place the
// validator refuses is the failure this package exists to avoid. The level is
// also all that separates a signal's `timeout:` from the step's own: the step's
// bounds one attempt and has no clock in it, and `retry:`'s durations sit under
// their own key and are excluded the same way.
//
// The set is the validator's (flowfile's validateWait, which takes it from where
// the engine evaluates waits): all three of a wait's own expressions, plus a
// signal's `outputs:` shaping, whose scope validateWait builds from the waiting
// one, so the clock is bound there alongside the wait's result names.
//
// It is the line scan's answer to the question [parsedStep.bindsNow] answers from
// the model, and the two are separate on purpose: completion is asked for while a
// document does not parse, which is exactly when there is no model to ask. The
// rule they implement is one rule, so a change to either belongs in both.
func bindsClock(key string, path []string) bool {
	switch {
	case endsWith(path, "steps"):
		return key == waitUntilKey || key == sleepKey
	case endsWith(path, "wait_for_signal"):
		return key == signalTimeoutKey
	case withinWaitOutputsShaping(path):
		return true
	}
	return false
}

// withinWaitOutputsShaping reports whether path descends from a signal's
// `outputs:` shaping, at any depth beneath it.
//
// A shaping value can itself be a mapping (`outputs: { audit: { observed_at:
// ${...} } }`), and every level below `outputs:` is still evaluated in the
// wait's scope: validateWait builds the shaping scope once, from the waiting
// one, and walks every entry the shaping mapping holds regardless of how deep
// it nests. An exact-suffix check on `[wait_for_signal, outputs]` saw only the
// entries directly under `outputs:` and stopped there, which is the DSL saying
// more than the editor did about the same file.
//
// The pair must be adjacent (`wait_for_signal` immediately followed by
// `outputs`), and `wait_for_signal` must sit directly under a step, the same
// level [bindsClock]'s own `wait_for_signal` case reads a step's `timeout:`
// at. That excludes a step-level `timeout:` (which never has `outputs` under
// it) and a task's own shaping `outputs:` (whose ancestor is a task name, not
// `wait_for_signal`), so a task input named `outputs` with a nested map is
// left alone.
func withinWaitOutputsShaping(path []string) bool {
	for i := 0; i+1 < len(path); i++ {
		if path[i] != "wait_for_signal" || path[i+1] != taskShapingKey {
			continue
		}
		if i == 0 || path[i-1] != "steps" {
			continue
		}
		return true
	}
	return false
}

// waitResultCandidates are the names bound bare inside a `wait_for_signal:`'s
// `outputs:` shaping, and nowhere else in the language.
//
// Offered ahead of the rest of the scope because they are the whole reason an
// author is typing here: a shaping expression exists to read the wait's own
// result. Decided from the key path rather than from the model, for the reason
// [bindsClock] is: completion is asked for while the document does not parse.
//
// `outputs:` alone is not enough to decide it — the workflow's own declared
// outputs and the http task's shaping use the same word, and neither binds these
// — so the path must show the wait above it.
func waitResultCandidates(path []string) []celcomplete.Candidate {
	if !endsWith(path, "wait_for_signal", "outputs") {
		return nil
	}

	// The documentation is [waitResultDoc]'s, not a shorter retelling of it. The
	// three candidates used to carry their own wording, which made two accounts of
	// one name in the two places an author meets it in the same keystroke: the
	// menu, and the hover over what the menu inserted. Now the schema's sentence
	// about `payload` and `sender` reaches both, and there is one place left to
	// edit when any of it changes.
	return []celcomplete.Candidate{
		{
			Name:   v1.PayloadOutput,
			Kind:   celcomplete.KindValue,
			Detail: "map",
			Docs:   waitResultDoc(v1.PayloadOutput),
		},
		{
			Name:   v1.SenderOutput,
			Kind:   celcomplete.KindValue,
			Detail: "map",
			Docs:   waitResultDoc(v1.SenderOutput),
		},
		{
			Name:   v1.TimedOutOutput,
			Kind:   celcomplete.KindValue,
			Detail: "bool",
			Docs:   waitResultDoc(v1.TimedOutOutput),
		},
	}
}

// stepAtIfParsed returns the model's step at a position, or nil when the document
// does not parse.
func stepAtIfParsed(doc *document, pos lsp.Position) *parsedStep {
	if doc.parsed == nil {
		return nil
	}
	return doc.parsed.stepAt(pos)
}

// scopeFromModel builds the candidate lists using the engine's scoping rules.
//
// ls names which of a loop's own scopes the cursor's expression is evaluated
// in — [loopScopeAfterBody] inside `until:`/`update:`, where the carried state
// and the body's top-level steps are readable — and loopScopeNone everywhere
// else, including `init:`, whose scope is the step's own.
func scopeFromModel(doc *document, from *parsedStep, ls loopScope) refScope {
	var scope refScope

	// The step's own `vars:`, first because they are the nearest binding there is —
	// nearer than an enclosing loop's item, which is why they win at evaluation too
	// (`StepsOutputActivation` reads Locals before anything else).
	//
	// They were offered nowhere. `expressionEntries` has read this block since it
	// landed, so hover and diagnostics knew about it and completion did not — the
	// author declaring `vars:` two lines above got a menu that did not contain what
	// they had just written.
	scope.locals = append(scope.locals, varsCandidates(from.varsEntry, "a variable this step declares")...)

	// The carried state a loop's own `until:`/`update:` read, bound under its
	// `as:` name. Only the after-body flavor offers it: in `init:` the value
	// does not exist yet — that expression is defining it — and offering it
	// there would teach a reference the validator refuses. A loop without `as:`
	// binds nothing, which iteratorName answering "" already says.
	if ls == loopScopeAfterBody && from.loopEntry != nil {
		if name := from.iteratorName(); name != "" {
			scope.locals = append(scope.locals, celcomplete.Candidate{
				Name:   name,
				Kind:   celcomplete.KindValue,
				Detail: "carried value",
				Docs: fmt.Sprintf(
					"The value the %s loop carries between iterations: init sets it, the body reads it, update computes the next one.",
					from.id),
			})
		}
	}

	// Innermost loop first: standing in nested bodies, the nearest binding is the
	// one most likely to be wanted.
	for _, loop := range from.iteratorsInScope() {
		name := loop.iteratorName()
		if name == "" {
			continue
		}
		// The same split hover makes, for the same reason: a `loop:` binds the
		// value it carries, not an item of a pre-existing list, and a menu that
		// says "loop item" about carried state teaches the wrong model.
		detail, docs := "loop item", fmt.Sprintf(
			"The current item of the %s loop. Its type is whatever the loop's items expression yields an element of.",
			loop.id)
		if loop.loopEntry != nil {
			detail, docs = "carried value", fmt.Sprintf(
				"The value the %s loop carries between iterations: init sets it, the body reads it, update computes the next one.",
				loop.id)
		}
		scope.locals = append(scope.locals, celcomplete.Candidate{
			Name:   name,
			Kind:   celcomplete.KindValue,
			Detail: detail,
			Docs:   docs,
		})
	}

	// Steps in reverse document order, so the list reads nearest-first: the step
	// just above you is the one most often referenced.
	//
	// Nearest-first is load-bearing rather than cosmetic, because one id can name
	// two steps in a document — ids are unique within a visibility domain, not
	// within a file. Every candidate visible from one position sits in a prefix of
	// that position's scope frames, so of two carrying the same id the deeper one
	// is always the later in document order; taking the first of the reversed walk
	// therefore takes the binding the engine reads, which is what
	// `outputCandidates` then reads the outputs of. The rest are dropped rather
	// than offered: a menu with the same id twice cannot say which is which, and
	// the second entry's outputs are a step this expression cannot reach.
	seen := map[string]bool{}
	for i := len(doc.parsed.steps) - 1; i >= 0; i-- {
		s := doc.parsed.steps[i]
		if s.id == "" || seen[s.id] || !visibleFromEntry(s, from, ls) {
			continue
		}
		seen[s.id] = true
		scope.steps = append(scope.steps, stepCandidate(s, doc.tasks))
	}

	// A `vars:` on an enclosing block binds for that block's whole body, so a step
	// inside a loop sees the loop's. Outermost first, so a name declared nearer the
	// cursor is offered nearer the top — and after the step's own, which is nearer
	// still.
	for _, frame := range from.scope {
		if frame.block == nil {
			continue
		}
		scope.locals = append(scope.locals,
			varsCandidates(frame.block.varsEntry, "a variable the enclosing "+frame.block.kind()+" declares")...)
	}

	return scope
}

// varsCandidates offers the keys of a `vars:` block.
//
// One function for both positions, because the block is one grammar rule written at
// several sites — the same reason the compiler compiles it in one place. What
// differs is only what to call it, which is what detail says.
func varsCandidates(vars *entry, detail string) []celcomplete.Candidate {
	if vars == nil || vars.value == nil {
		return nil
	}

	var out []celcomplete.Candidate
	for _, e := range vars.value.entries {
		if e.key == "" {
			continue
		}
		out = append(out, celcomplete.Candidate{
			Name:   e.key,
			Kind:   celcomplete.KindValue,
			Detail: detail,
			Docs:   varDoc(e),
		})
	}

	return out
}

// varDoc describes one declared variable, showing what it is bound to.
//
// The value is the whole of what there is to say: a var has no declared type, so
// what an author wants to be reminded of is the expression or literal they wrote.
func varDoc(e *entry) string {
	value := e.valueText()
	if value == "" {
		return "A variable bound where it is declared."
	}

	return "Bound to " + value + "."
}

// scopeFromOutline builds the candidate lists from the line scan, for a document
// that does not parse.
//
// The scan cannot see the scoping rules, so it approximates them with indentation
// and errs towards offering too little: a step nested deeper than the cursor's is
// inside a block the cursor is not in, and one of the cursor's own enclosing blocks
// has not finished. Both are excluded. Omitting a name that would have worked is a
// small cost; offering one that cannot resolve is the thing to avoid.
//
// It finds no locals at all, which is the same judgement: an iterator is declared
// by a key inside the block above, and a scan that guessed at one would offer a
// bare name in the one namespace where a wrong candidate cannot be told from a
// right one.
func scopeFromOutline(earlier []*outlineStep, currentIndent int, tasks *v1.Registry) refScope {
	// The enclosing blocks are the nearest preceding step at each shallower
	// indentation.
	ancestors := map[*outlineStep]bool{}
	depth := currentIndent
	for i := len(earlier) - 1; i >= 0; i-- {
		if earlier[i].indent < depth {
			ancestors[earlier[i]] = true
			depth = earlier[i].indent
		}
	}

	scope := refScope{steps: make([]celcomplete.Candidate, 0, len(earlier))}
	for i := len(earlier) - 1; i >= 0; i-- {
		s := earlier[i]
		if s.id == "" || s.indent > currentIndent || ancestors[s] {
			continue
		}
		c := celcomplete.Candidate{Name: s.id, Kind: celcomplete.KindValue, Detail: "step"}
		if def, ok := tasks.Lookup(s.taskName); ok {
			c.Detail = def.Name
			c.Docs = fmt.Sprintf("Runs the %s task.", def.Name)
			// The line scan sees input keys but not their values, so a step
			// that shapes — the same rule the validator and the model use, now
			// a declared capability rather than a name — gets no output
			// candidates here rather than the declared names its shaping
			// removed. Offering too little is the scan's stated posture;
			// offering a name nothing produces is the failure.
			if !s.shapes(tasks) {
				c.Members = taskOutputs(def)
			}
		}
		scope.steps = append(scope.steps, c)
	}
	return scope
}

// stepCandidate describes one step as a reference candidate.
func stepCandidate(s *parsedStep, tasks *v1.Registry) celcomplete.Candidate {
	c := celcomplete.Candidate{Name: s.id, Kind: celcomplete.KindValue, Detail: s.kind()}

	switch {
	case s.forEachEntry != nil:
		// A loop reports every iteration through one output; its body's outputs
		// are not reachable from outside it.
		c.Detail = "for_each"
		c.Docs = fmt.Sprintf(
			"A loop. Reports one entry per iteration in %s, each a map of body step id to that step's outputs. Body outputs do not escape the loop.",
			rootedRef(s.id, loopResultsOutput))
		c.Members = []celcomplete.Candidate{{
			Name:   loopResultsOutput,
			Kind:   celcomplete.KindField,
			Detail: "list",
			Docs:   "One entry per iteration, each a map of body step id to that step's named outputs.",
		}}

	case s.parallelEntry != nil:
		c.Detail = "parallel"
		c.Docs = "A parallel block. Its branches' step outputs merge into this scope once it joins, so name those steps under the root, not this one."

	case s.loopEntry != nil, s.waitUntilEntry != nil, s.sleepEntry != nil,
		s.waitForSignalEntry != nil, s.hasKey(waitUntilKey), s.hasKey(sleepKey), s.hasKey("wait_for_signal"):
		// A `loop:` or a wait runs no task either, and stepCandidate had no
		// branch for either — so completion fell into the default arm below,
		// looked up an empty task name, and offered nothing after
		// `${steps.paginate.}` or `${steps.gate.}` (#322, Codex's #320
		// findings). [v1.OutputNames] is the same answer [hoverConstructOutput]
		// reads, so accepting a candidate here and then hovering it cannot
		// describe two different things.
		if node := constructOutputNode(s); node != nil {
			c.Detail = s.kind()
			if c.Detail == "" {
				c.Detail = "wait"
			}
			names, _ := v1.OutputNames(node, tasks)
			for _, n := range names {
				if n.Name == "" {
					continue
				}
				c.Members = append(c.Members, celcomplete.Candidate{
					Name: n.Name,
					Kind: celcomplete.KindField,
					Docs: n.Description,
				})
			}
		}

	case s.valueEntry != nil:
		// The only candidate here whose output set is fixed by the *grammar*
		// rather than read from a descriptor, a shaping expression, or a sender.
		// So it is the one menu that can be complete with nothing consulted, and
		// the one that has exactly one honest answer after the dot, which is the
		// ergonomic half of the argument the `.value` spelling was chosen on.
		c.Detail = "value"
		c.Docs = fmt.Sprintf(
			"A computed value, evaluated in the workflow where it is written. Read the whole of it as %s.",
			rootedRef(s.id, v1.ValueOutput))
		c.Members = []celcomplete.Candidate{{
			Name:   v1.ValueOutput,
			Kind:   celcomplete.KindField,
			Detail: "the computed value",
			Docs: "What the step's expression evaluated to. A `value:` step produces exactly this one output, " +
				"so this is the whole of what it contributes.",
		}}

	case s.switchEntry != nil:
		// Like a `value:`, the output set is the grammar's own: the observed
		// discriminant and the case that took it. Body step outputs are read
		// under their own ids, not this one.
		c.Detail = "switch"
		c.Docs = fmt.Sprintf(
			"A dispatch on one value. Records what it observed as %s and which case took it as %s; the taken body's step outputs merge into this scope under their own ids.",
			rootedRef(s.id, v1.SwitchValueOutput), rootedRef(s.id, v1.SwitchCaseOutput))
		c.Members = []celcomplete.Candidate{{
			Name:   v1.SwitchValueOutput,
			Kind:   celcomplete.KindField,
			Detail: "the observed value",
			Docs:   "What the switch's `value:` evaluated to, recorded whether or not any case matched.",
		}, {
			Name:   v1.SwitchCaseOutput,
			Kind:   celcomplete.KindField,
			Detail: "the matching case literal, or null",
			Docs:   "The case literal that took the value, and `null` when none did — whether the `default:` body ran or nothing did.",
		}}

	default:
		if def, ok := tasks.Lookup(s.taskName); ok {
			c.Detail = def.Name
			c.Docs = fmt.Sprintf("Runs the %s task.", def.Name)
			if shaping := s.shapingEntry(tasks); shaping != nil {
				// The declared outputs are exactly what shaping removed, so they
				// are not candidates: accepting one would write a reference
				// nothing produces. The shaping's own names are offered when
				// they are statically knowable, and nothing is offered when they
				// are not — a fabricated name is worse than an empty menu.
				c.Docs = fmt.Sprintf("Runs the %s task. Its %s: replaces the task's declared outputs with names of its own.", def.Name, taskShapingKey)
				c.Members = shapedOutputCandidates(s, def, tasks)
			} else {
				c.Members = taskOutputs(def)
			}
		}
	}
	return c
}

// shapedOutputCandidates renders a shaped step's own output names, or nothing
// when the shaping expression keeps them to itself.
func shapedOutputCandidates(s *parsedStep, def v1.TaskDef, tasks *v1.Registry) []celcomplete.Candidate {
	names, ok := shapedOutputNames(s.shapingEntry(tasks))
	if !ok {
		return nil
	}
	out := make([]celcomplete.Candidate, 0, len(names))
	for _, name := range names {
		out = append(out, celcomplete.Candidate{
			Name: name,
			Kind: celcomplete.KindField,
			// The type is whatever the shaping expression yields for this key,
			// which is not statically known, so none is claimed.
			Detail: "shaped output",
			Docs: fmt.Sprintf("Shaped output of step %s: its %s: names this output itself, replacing what the %s task declares.",
				s.id, taskShapingKey, def.Name),
		})
	}
	return out
}

// taskOutputs renders a task's declared outputs as reference candidates.
func taskOutputs(def v1.TaskDef) []celcomplete.Candidate {
	if def.Outputs == nil {
		return nil
	}
	fields := def.Outputs.Fields()
	out := make([]celcomplete.Candidate, 0, fields.Len())
	for i := range fields.Len() {
		fd := fields.Get(i)
		out = append(out, celcomplete.Candidate{
			Name:   string(fd.Name()),
			Kind:   celcomplete.KindField,
			Detail: typeName(fd),
			Docs: fmt.Sprintf("%s output of the %s task, of type %s.",
				fd.Name(), def.Name, typeName(fd)),
		})
	}
	return out
}

// shared renders this scope as the one [celcomplete] answers over.
//
// The two judgements an editor makes about roots are made here, and nowhere
// else, because they are answers about a document rather than about the
// language. The steps root is offered unconditionally, because the first step of
// a file is written before there is anything to reference and the name is still
// what an author needs to learn. The vars root is offered only where the file
// declares one, because a root that resolves to an empty map is a name nobody
// should be taught. There is no inputs root, and that is a gap rather than a
// decision: a Flowfile's `inputs:` are declared in the file and an editor could
// offer them — see the note on [refScope.vars].
func (s refScope) shared() celcomplete.Scope {
	shared := celcomplete.Scope{
		// The editor completes against the vocabulary this build compiles
		// with, which is the one a file being typed will be compiled by.
		Profile: v1.CurrentProfile,
		Locals:  s.locals,
		Roots:   []celcomplete.Candidate{celcomplete.StepsRoot(s.steps)},
	}
	if len(s.vars) > 0 {
		shared.Roots = append(shared.Roots, celcomplete.VarsRoot(s.vars))
	}

	return shared
}

// completeInExpression offers what an expression may name at the cursor.
//
// The rules are [celcomplete.Complete]'s — which names a scope holds where, in
// what order, and what a dot reaches — and what is left here is the protocol:
// turning each candidate into an item with the range it replaces.
func completeInExpression(pos lsp.Position, inner string, scope refScope) *lsp.CompletionList {
	result := celcomplete.Complete(inner, scope.shared())
	replace := rangeBack(pos, result.Prefix)

	items := make([]lsp.CompletionItem, 0, len(result.Candidates))
	for i, c := range result.Candidates {
		items = append(items, lsp.CompletionItem{
			Label:         c.Name,
			Kind:          itemKind(c.Kind),
			Detail:        c.Detail,
			Documentation: plainText(c.Docs),
			// The list is already nearest-first, and the nearest name is usually
			// the one being referenced.
			SortText: fmt.Sprintf("%04d%s", i, c.Name),
			TextEdit: &lsp.TextEdit{Range: replace, NewText: c.Text()},
		})
	}

	// An answer that reached [celcomplete.MaxCandidates] is a prefix of what
	// matched, and `isIncomplete` is the protocol's word for exactly that: the
	// client re-asks as more is typed rather than filtering a list it was told
	// is whole.
	return &lsp.CompletionList{IsIncomplete: result.Truncated, Items: items}
}

// itemKind maps the completer's small vocabulary onto the protocol's icons.
func itemKind(kind celcomplete.Kind) lsp.CompletionItemKind {
	switch kind {
	case celcomplete.KindRoot:
		// A value with named members, which is what a root is.
		return lsp.CIKStruct
	case celcomplete.KindField:
		return lsp.CIKField
	case celcomplete.KindFunction:
		return lsp.CIKFunction
	case celcomplete.KindNamespace:
		return lsp.CIKModule
	default:
		return lsp.CIKVariable
	}
}

// A completion list is ordered by the order an author writes a step in, not
// alphabetically: `id` first, then the prose saying why the step is there, then
// the work it does, then how that work runs. That order is the one dslKeys is
// written in, so a key's position in that list is its position in the menu.
//
// Positions are spaced so that a group assembled by a different function can be
// placed *between* two of them without renumbering either. Tasks are the only
// such group today: they are a kind of work, so they belong beside `for_each` and
// friends rather than after `continue_on_error`, and ahead of them because a step
// that runs a task is the common case.
//
// The spacing buys nothing when a key is inserted *before* the group, which is
// what `description` did: every slot after it moves by one place, so taskSlot has
// to move with them. It is written as a slot rather than derived because what it
// says is a judgement about the menu — tasks come first among the kinds of work —
// and there is no list it could be read off.
const (
	slotSpacing = 10
	taskSlot    = 15 // between `description` at 10 and `for_each` at 20.
)

// sortAt renders a menu position as the string an editor sorts by.
//
// Zero-padded so that comparison is numeric where it looks numeric: unpadded, a
// slot of 10 would sort before one of 5. The name is appended so that candidates
// sharing a slot — every task does — stay in the registry's own order.
func sortAt(slot int, name string) string {
	return fmt.Sprintf("%04d%s", slot, name)
}

// taskCandidates offers every registered task, with its summary as the detail.
//
// A task's name is a key of the step like `id:` or `retry:`, so it is offered the
// way every other key is: with its colon, and with a sort position. Both were
// wrong when a task name was a *value* under `task:`, and neither reads as wrong
// on its own — a missing SortText is an absent field, not a visible mistake, and
// it sorted the whole registry above `id`.
func taskCandidates(prefix string, replace lsp.Range, tasks *v1.Registry) []lsp.CompletionItem {
	var items []lsp.CompletionItem
	for _, def := range tasks.All() {
		if !strings.HasPrefix(def.Name, prefix) {
			continue
		}
		items = append(items, lsp.CompletionItem{
			Label:         def.Name,
			Kind:          lsp.CIKFunction,
			Detail:        def.Summary,
			Documentation: plainText(taskDoc(def)),
			SortText:      sortAt(taskSlot, def.Name),
			// The colon is included for the same reason an input key includes it:
			// the key is never written without one.
			TextEdit: &lsp.TextEdit{Range: replace, NewText: def.Name + ": "},
		})
	}
	return items
}

// inputCandidates offers the inputs the enclosing step's task declares, required
// ones first, omitting those already written.
func inputCandidates(prefix string, replace lsp.Range, step *outlineStep, tasks *v1.Registry) []lsp.CompletionItem {
	if step == nil {
		return nil
	}
	def, ok := tasks.Lookup(step.taskName)
	if !ok || def.Inputs == nil {
		return nil
	}

	written := make(map[string]bool, len(step.inputKeys))
	for _, k := range step.inputKeys {
		written[k] = true
	}

	fields := def.Inputs.Fields()
	var items []lsp.CompletionItem
	for i := range fields.Len() {
		fd := fields.Get(i)
		name := string(fd.Name())
		if !strings.HasPrefix(name, prefix) || (written[name] && name != prefix) {
			continue
		}
		detail := typeName(fd)
		order := "1"
		if required(fd) {
			detail += " (required)"
			order = "0"
		}
		items = append(items, lsp.CompletionItem{
			Label:         name,
			Kind:          lsp.CIKProperty,
			Detail:        detail,
			Documentation: plainText(inputDoc(def, name, fd)),
			SortText:      order + fmt.Sprintf("%04d", i) + name,
			// The colon is included because an input key is never written
			// without one, and typing it again is friction.
			TextEdit: &lsp.TextEdit{Range: replace, NewText: name + ": "},
		})
	}
	return items
}

// stepKeyCandidates is dslCandidates("steps", ...), minus timeout,
// total_timeout and retry once the step at the cursor has already committed to
// a kind that refuses all three.
//
// checkPolicyPlacement (flowfile/parse_wait.go) refuses
// timeout:/total_timeout:/retry: on
// every step kind but a task, because only a task's arm ever reads a
// StepPolicy — so once current.kindKey names one of those kinds,
// recommending any of them is completeAt actively suggesting syntax its own
// diagnostic then refuses. current is nil (no kind chosen yet, or not inside
// a step at all) and current.kindKey == "" (a task, or a kind not decided
// yet) both still offer the full menu: both are exactly the cases where
// all three are legal or might become so.
func stepKeyCandidates(current *outlineStep, prefix string, replace lsp.Range) []lsp.CompletionItem {
	if current == nil || current.kindKey == "" {
		return dslCandidates("steps", prefix, replace)
	}

	all := dslCandidates("steps", prefix, replace)
	items := make([]lsp.CompletionItem, 0, len(all))
	for _, item := range all {
		if item.Label == "timeout" || item.Label == "total_timeout" || item.Label == "retry" {
			continue
		}
		items = append(items, item)
	}
	return items
}

// dslCandidates offers the document's own keys at one level of nesting.
func dslCandidates(level, prefix string, replace lsp.Range) []lsp.CompletionItem {
	var items []lsp.CompletionItem
	for i, k := range dslKeys[level] {
		if !strings.HasPrefix(k.name, prefix) {
			continue
		}
		items = append(items, lsp.CompletionItem{
			Label:         k.name,
			Kind:          lsp.CIKKeyword,
			Detail:        k.detail,
			Documentation: plainText(k.docs),
			SortText:      sortAt(i*slotSpacing, k.name),
			TextEdit:      &lsp.TextEdit{Range: replace, NewText: k.name + ": "},
		})
	}
	return items
}

// keyAndPosition returns the key a line declares and whether a byte column falls
// after its colon, which is what distinguishes completing a key from completing
// its value.
func keyAndPosition(line string, col int) (key string, inValue bool) {
	m := keyLine.FindStringSubmatch(line)
	if m == nil {
		return "", false
	}
	after := len(m[1]) + len(m[2]) + len(m[3])
	offset := strings.Index(line[after:], ":")
	if offset < 0 {
		// Unreachable given the pattern matched, but a negative index here would
		// silently classify every position as a value.
		return m[3], false
	}
	return m[3], col > after+offset
}

// wordBefore returns the partial word the cursor is typing and the range it should
// replace.
func wordBefore(pos lsp.Position, before string) (string, lsp.Range) {
	word := trailingWord(before, func(c byte) bool {
		return c == '_' || c == '-' ||
			(c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z') || (c >= '0' && c <= '9')
	})
	return word, rangeBack(pos, word)
}

// trailingWord returns the run of accepted bytes at the end of s.
func trailingWord(s string, accept func(byte) bool) string {
	i := len(s)
	for i > 0 && accept(s[i-1]) {
		i--
	}
	return s[i:]
}

// rangeBack returns the range covering word, which is the text ending at pos.
//
// The width is measured in UTF-16 code units rather than bytes, so a candidate
// replacing a partial word that contains non-ASCII still replaces exactly that
// word instead of eating the characters before it.
func rangeBack(pos lsp.Position, word string) lsp.Range {
	start := pos
	start.Character = max(pos.Character-utf16Len(word), 0)
	return lsp.Range{Start: start, End: pos}
}

// plainText strips the Markdown a hover popup renders but a completion popup does
// not.
//
// The protocol's string form of a completion item's documentation is plain text by
// definition — only MarkupContent may be Markdown, and the LSP types in use here
// cannot express it. Left alone, hover copy shows its own backticks and code fences
// to the reader as literal characters.
func plainText(md string) string {
	var kept []string
	for _, line := range strings.Split(md, "\n") {
		if strings.HasPrefix(strings.TrimSpace(line), "```") {
			continue
		}
		kept = append(kept, line)
	}
	out := strings.Join(kept, "\n")
	out = strings.ReplaceAll(out, "**", "")
	out = strings.ReplaceAll(out, "`", "")
	return strings.TrimSpace(out)
}

// list wraps candidates in a completion list, never returning null items.
//
// The items are returned already in the order their sort text asks for. Ordering is
// nominally the client's job, but a client that ignores sortText — several do —
// should still see required inputs before optional ones and the nearest step before
// a distant one.
func list(items []lsp.CompletionItem) *lsp.CompletionList {
	if items == nil {
		items = []lsp.CompletionItem{}
	}
	slices.SortStableFunc(items, func(a, b lsp.CompletionItem) int {
		if c := strings.Compare(a.SortText, b.SortText); c != 0 {
			return c
		}
		return strings.Compare(a.Label, b.Label)
	})
	return &lsp.CompletionList{IsIncomplete: false, Items: items}
}
