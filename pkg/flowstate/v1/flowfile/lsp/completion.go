package lsp

import (
	"fmt"
	"slices"
	"strings"

	v1 "github.com/picatz/flowstate/pkg/flowstate/v1"
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
// cursor stands is written bare: a loop's iterator, `now` inside a `wait_until:`.
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
var editionList = strings.Join(flowfile.KnownEditions(), ", ")

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
			"It was optional once, which was the one thing it could not afford to be — leaving it out did not mean \"any grammar\", it meant being read as whichever grammar came next.\n\n" +
			"`flow fix` writes it, below any header comment, so no one has to type it.\n\n" +
			"It is not a compatibility switch. A build compiles one grammar — this one knows " + editionList + " — and declaring anything else is refused, not translated."},
		{name: "name", detail: "string", docs: "What this workflow is called."},
		{name: "description", detail: "string", docs: "Optional prose about the workflow."},
		{name: "vars", detail: "map", docs: "Names values once, for the whole file. Every step reads them as `${" + v1.VarsRoot + ".<name>}`.\n\n" +
			"Rooted rather than bare because a var is *ambient*: it is in scope everywhere rather than bound where you read it, the same distinction that makes a step's outputs `" + v1.StepsRoot + ".<id>.<output>` and a loop's binding bare.\n\n" +
			"Evaluated once, before the first step runs — so a var may use literals, operators and the profile's functions, and may not read a step, another var, or anything else that does not exist yet. " +
			"The `${...}` fence is still required for an expression: without it the value is the text as written, which is what lets a var hold the literal string `steps.greet.result`.\n\n" +
			"A `${secret(...)}` reference may not be stored here — a var is evaluated by the workflow and its value is written to durable history. Write the reference on the task input that consumes the secret instead."},
		{name: "steps", detail: "list", docs: "The steps to run, in order. Each step may reference the outputs of the steps before it."},
	},
	"steps": {
		{name: "id", detail: "string", docs: "How later steps reference this one, as `${" + v1.StepsRoot + ".<id>.<output>}`. Must be a valid CEL identifier and unique in the workflow."},
		{name: "description", detail: "string", docs: "Optional prose about this step: why it is here, which the mechanics under it cannot say.\n\n" +
			"A property of the step rather than of the work it does, so every kind of step can carry one — a `for_each` or a `sleep` as readily as a task. " +
			"It belongs here, directly under `id`, and not under the task's own key: the keys there are that task's inputs, so a `description` written among them asks for an input by that name."},
		{name: "for_each", detail: "map", docs: "Repeat a body of steps once per item of a list. " + oneStepKind},
		{name: "loop", detail: "map", docs: "Repeat a body of steps, carrying a value from one iteration to the next, until a condition holds or `max_iterations:` is reached. " + oneStepKind + "\n\n" +
			"The body runs, then `until:` is checked — so `until:` reads what the body produced, which is what lets a loop page a cursor until the last page reports it is done. " +
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
			"The callee runs isolated: its steps see only its bound arguments (`with:`) and the profile — not this file's steps or `vars:`. " +
			"What it declares in its own `outputs:` comes back under this step's id, the way a task's would."},
		{name: "if", detail: "expression", docs: "A condition deciding whether the step runs, written as `${...}`. A step that is skipped produces no outputs."},
		{name: "vars", detail: "map", docs: "Names values for this step, read *bare*: `${modified}`.\n\n" +
			"Bare rather than rooted because these are author-chosen and lexically local — the same standing as the name a loop binds — where the workflow's `vars:` are ambient and so are rooted. " +
			"They are private to the step: on a `for_each` or `parallel:` they reach the whole body, and nowhere else. Pass a value to a *later* step through its outputs instead.\n\n" +
			"A name already bound by an enclosing loop or step is refused rather than shadowed, and a var may not read its siblings — `vars:` is a mapping, so there is no order that would make one available to another. " +
			"Everything else in scope is fair: `" + v1.VarsRoot + ".<name>`, the outputs of steps already run, and any enclosing binding.\n\n" +
			"A `${secret(...)}` reference may not be stored here either, for the same reason it may not go in the workflow's own `vars:`. Write it on the task input that consumes the secret."},
		{name: "timeout", detail: "duration", docs: "Bounds one attempt at the step, written as `30s`, `5m`, or `1h`."},
		{name: "retry", detail: "map", docs: "How a failed attempt is retried. Omit it to use the engine's defaults."},
		{name: "continue_on_error", detail: "bool", docs: "Let the run proceed when this step fails. A cancellation is not a failure, so this does not tolerate one."},
		{name: "undo", detail: "map", docs: "How this step is taken back when a *later* step fails and the run cannot continue — the saga compensation for what it did.\n\n" +
			"Written as the task that undoes it, with its inputs beneath: the same shape as the step's own work, because it is the same kind of thing. " +
			"A compensation is one task — it cannot loop, branch, wait, or carry an `undo:` of its own.\n\n" +
			"Registered when this step *succeeds*, and never otherwise: a step skipped by its `if:` did nothing, and a step that failed may have applied part of its effect, which the engine will not guess at. " +
			"Compensations then run in reverse order, because steps depend forwards.\n\n" +
			"Its inputs are resolved the moment the step succeeds, in that step's scope with its own outputs added — so `${" + v1.StepsRoot + ".<this step>.<output>}` is the reference to use, and it is the one place a step may name itself. " +
			"A run that failed and compensated still reports FAILED; what it undid is in the failure.\n\n" +
			"Top-level task steps only in this version. Inside a `for_each` body or a `parallel:` branch it is refused, because the order work registers in there is not the same under `flow run local` as it is durably."},
		{name: "with", detail: "map", docs: "Arguments binding the callee's declared `inputs:`, resolved in *this* file's scope — the same scope a task's inputs are resolved in. " +
			"Only meaningful beside `call:`. Checked against what the callee declares when this file is compiled: a missing required input or an argument it does not declare is refused here, not at run time.\n\n" +
			"A secret reference may not be bound through `with:` — pass it to the task that needs it inside the callee instead."},
	},
	"wait_for_signal": {
		{name: "name", detail: "string", docs: "The signal this step waits for, and what a sender addresses with `flow signal <workflow-id> <name>`."},
		{name: "timeout", detail: "duration", docs: "Bounds the wait. A gate that lapses is not a failure: the step produces `timed_out: true` and the run carries on, " +
			"so an author branches on it with `if: ${!" + v1.StepsRoot + ".approval.timed_out}`. Omit it to wait indefinitely."},
	},
	"for_each": {
		{name: "items", detail: "expression", docs: "An expression producing the list to iterate, written as `${...}`."},
		{name: "as", detail: "string", docs: "Names the variable bound to the current item, read bare inside the body: `${name}`. Defaults to `item`.\n\n" +
			"Reads as the sentence it is — *for each item as name* — and names the binding rather than the mechanism. It was `iterator:`; `flow fix` rewrites that."},
		{name: "max_parallel", detail: "int", docs: "How many iterations may run at once. Omitted, `0` or `1` runs them one at a time.\n\n" +
			"Zero is accepted rather than refused because it is the field's own zero value: a spec built " +
			"without it and one that sets it to nothing mean the same thing, and the schema says so with `gte: 0`."},
		{name: "steps", detail: "list", docs: "The body run once per item."},
	},
	"loop": {
		{name: "as", detail: "string", docs: "Names the value carried between iterations, read bare inside the body, `until:` and `update:`: `${cursor}`.\n\n" +
			"The same standing as a `for_each` binding — author-chosen and lexically local. Optional: a loop that carries nothing omits `as:`, `init:` and `update:` together. A name that collides with an enclosing binding, `now`, or a root is refused."},
		{name: "init", detail: "expression", docs: "The value the carried state holds on the first iteration, written as `${...}`. Evaluated once before the loop, so it cannot read the state it defines. Required when `as:` is set."},
		{name: "update", detail: "expression", docs: "Computes the next iteration's carried state from the current one and the body's outputs, written as `${...}` — `${" + v1.StepsRoot + ".page.next_cursor}` or `${acc + n}`. Evaluated after the body. Required when `as:` is set."},
		{name: "until", detail: "expression", docs: "The stop condition, written as `${...}` producing a boolean. Evaluated after the body each iteration, so it reads the body's own outputs. When it holds, the loop stops."},
		{name: "max_iterations", detail: "int", docs: "The hard ceiling on how many times the body runs. Omitted or `0` uses the engine's default. Reaching it without `until:` holding fails the run distinctly rather than stopping silently — a loop that could run forever is one the engine must be able to stop."},
		{name: "steps", detail: "list", docs: "The body run each iteration."},
	},
	"parallel": {
		{name: "steps", detail: "list", docs: "One branch's steps. Each `- steps:` entry is a branch that runs concurrently with the others."},
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
		return completeInExpression(pos, inner, referenceScope(doc, pos, bindsClock(key, path), current, earlier))
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
	case endsWith(path, "wait_for_signal"):
		return list(dslCandidates("wait_for_signal", word, replace))
	case endsWith(path, "steps"):
		// A step key is a property or a task name, and from where the cursor is
		// they are the same kind of thing: both are ways to finish this line. The
		// registry supplies one half and the table the other, which is why a task
		// added to the registry becomes completable with no change here.
		return list(append(dslCandidates("steps", word, replace), taskCandidates(word, replace, doc.tasks)...))
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
func openExpression(before string) (string, bool) {
	open := strings.LastIndex(before, "${")
	if open < 0 {
		return "", false
	}
	if strings.Contains(before[open:], "}") {
		return "", false
	}
	return before[open+len("${"):], true
}

// A refCandidate is one name an expression at the cursor may reference, together
// with what it exposes after a dot.
type refCandidate struct {
	name string

	// detail and docs describe the candidate in the popup.
	detail string
	docs   string

	// outputs are the names reachable after a dot, with their rendered types.
	// A candidate with none — a loop iterator, whose element type is not known
	// statically — offers nothing after the dot rather than guessing.
	outputs []refOutput

	// insert is the text an editor writes when the candidate is accepted, when
	// that differs from the name. The root is the only one that does: it is never
	// the whole of a reference, so the dot that continues it comes with it.
	insert string

	// kind distinguishes a step from a bound variable, for the popup's icon.
	kind lsp.CompletionItemKind
}

// A refOutput is one name reachable after a dot.
type refOutput struct {
	name   string
	detail string
	docs   string
}

// A refScope is what an expression at the cursor may name, in the two namespaces
// the grammar keeps apart.
//
// It mirrors the refScope flowfile's validator carries, deliberately: the editor
// offering a name the compiler would refuse is the one failure this package must
// not have, and a scope shaped like the compiler's cannot drift into one that
// merges them again.
type refScope struct {
	// steps are the steps whose outputs exist at this point, offered after
	// `steps.` and never bare.
	steps []refCandidate

	// locals are the names bound bare where the cursor is: a loop's iterator, a
	// step's own `vars:` keys, and `now` where a wait binds it. Offered bare and
	// never after the root.
	locals []refCandidate

	// vars are the workflow's declared variables, offered after `vars.` and never
	// bare — the crossing [v1.Scope.Activation] describes, met from the editor's
	// side: the rooted block is `vars.<name>` and the bare one is a step's.
	//
	// They are the file's rather than a step's, so they are the same list wherever
	// the cursor is. Held separately from steps for the same reason the engine holds
	// two namespaces: a step called `region` and a var called `region` are different
	// things and completing one as the other is the mistake this package exists to
	// avoid.
	vars []refCandidate
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
		scope = scopeFromModel(doc, from)
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
		// Bound by the engine for one key and nowhere else — which is why it is
		// added here rather than living in the scope every expression gets. A task
		// input has no clock that survives a retry, and the validator says so with
		// a diagnostic; offering the name there would be walking an author into it.
		scope.locals = append(scope.locals, refCandidate{
			name:   v1.NowIdentifier,
			kind:   lsp.CIKVariable,
			detail: "timestamp",
			// The same text hover shows, for the same reason a CEL library's is
			// shared: accepting a candidate and then hovering what was accepted
			// must not produce two accounts of one name.
			docs: nowDoc(),
		})
	}
	return scope
}

// waitUntilKey is the step key whose expression binds the clock.
//
// Written once, next to the only thing that reads it, because it is the join of
// two facts that live apart: v1 owns the name `now` and the DSL owns the key. The
// key itself is checked against the grammar by TestDSLKeysMatchTheDSL, which is
// what keeps a renamed key from leaving this pointing at nothing.
const waitUntilKey = "wait_until"

// bindsClock reports whether an expression written after a key is a wait's, and
// so has `now` bound in it.
//
// Both halves are needed. The key says which kind of value is being written, and
// the path says at which level — a `wait_until:` directly under a step is the
// wait the grammar defines, while the same word among a task's inputs would be
// that task's input and is resolved somewhere with no clock in it. Asking only the
// key would offer `now` there, and a candidate an author accepts in a place the
// validator refuses is the failure this package exists to avoid.
//
// It is the line scan's answer to the question [parsedStep.bindsNow] answers from
// the model, and the two are separate on purpose: completion is asked for while a
// document does not parse, which is exactly when there is no model to ask. The
// rule they implement is one rule, so a change to either belongs in both.
func bindsClock(key string, path []string) bool {
	return key == waitUntilKey && endsWith(path, "steps")
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
func scopeFromModel(doc *document, from *parsedStep) refScope {
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

	// Innermost loop first: standing in nested bodies, the nearest binding is the
	// one most likely to be wanted.
	for _, loop := range from.iteratorsInScope() {
		name := loop.iteratorName()
		if name == "" {
			continue
		}
		scope.locals = append(scope.locals, refCandidate{
			name:   name,
			kind:   lsp.CIKVariable,
			detail: "loop item",
			docs: fmt.Sprintf(
				"The current item of the %s loop. Its type is whatever the loop's items expression yields an element of.",
				loop.id),
		})
	}

	// Steps in reverse document order, so the list reads nearest-first: the step
	// just above you is the one most often referenced.
	for i := len(doc.parsed.steps) - 1; i >= 0; i-- {
		s := doc.parsed.steps[i]
		if s.id == "" || !visibleFrom(s, from) {
			continue
		}
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
func varsCandidates(vars *entry, detail string) []refCandidate {
	if vars == nil || vars.value == nil {
		return nil
	}

	var out []refCandidate
	for _, e := range vars.value.entries {
		if e.key == "" {
			continue
		}
		out = append(out, refCandidate{
			name:   e.key,
			kind:   lsp.CIKVariable,
			detail: detail,
			docs:   varDoc(e),
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

	scope := refScope{steps: make([]refCandidate, 0, len(earlier))}
	for i := len(earlier) - 1; i >= 0; i-- {
		s := earlier[i]
		if s.id == "" || s.indent > currentIndent || ancestors[s] {
			continue
		}
		c := refCandidate{name: s.id, kind: lsp.CIKVariable, detail: "step"}
		if def, ok := tasks.Lookup(s.taskName); ok {
			c.detail = def.Name
			c.docs = fmt.Sprintf("Runs the %s task.", def.Name)
			c.outputs = taskOutputs(def)
		}
		scope.steps = append(scope.steps, c)
	}
	return scope
}

// stepCandidate describes one step as a reference candidate.
func stepCandidate(s *parsedStep, tasks *v1.Registry) refCandidate {
	c := refCandidate{name: s.id, kind: lsp.CIKVariable, detail: s.kind()}

	switch {
	case s.forEachEntry != nil:
		// A loop reports every iteration through one output; its body's outputs
		// are not reachable from outside it.
		c.detail = "for_each"
		c.docs = fmt.Sprintf(
			"A loop. Reports one entry per iteration in %s, each a map of body step id to that step's outputs. Body outputs do not escape the loop.",
			rootedRef(s.id, loopResultsOutput))
		c.outputs = []refOutput{{
			name:   loopResultsOutput,
			detail: "list",
			docs:   "One entry per iteration, each a map of body step id to that step's named outputs.",
		}}

	case s.parallelEntry != nil:
		c.detail = "parallel"
		c.docs = "A parallel block. Its branches' step outputs merge into this scope once it joins, so name those steps under the root, not this one."

	default:
		if def, ok := tasks.Lookup(s.taskName); ok {
			c.detail = def.Name
			c.docs = fmt.Sprintf("Runs the %s task.", def.Name)
			c.outputs = taskOutputs(def)
		}
	}
	return c
}

// taskOutputs renders a task's declared outputs as reference candidates.
func taskOutputs(def v1.TaskDef) []refOutput {
	if def.Outputs == nil {
		return nil
	}
	fields := def.Outputs.Fields()
	out := make([]refOutput, 0, fields.Len())
	for i := range fields.Len() {
		fd := fields.Get(i)
		out = append(out, refOutput{
			name:   string(fd.Name()),
			detail: typeName(fd),
			docs: fmt.Sprintf("%s output of the %s task, of type %s.",
				fd.Name(), def.Name, typeName(fd)),
		})
	}
	return out
}

// completeInExpression offers what an expression may name at the cursor.
//
// Three positions, because the root has depth: bare at the start of an
// expression, step ids after `steps.`, and one step's outputs after
// `steps.<id>.`. Splitting on the *last* dot is what makes the middle one
// reachable — the qualifier there is two segments, `steps.<id>`, where before
// rooting every qualifier was one.
func completeInExpression(pos lsp.Position, inner string, scope refScope) *lsp.CompletionList {
	word := trailingWord(inner, func(c byte) bool {
		return c == '_' || c == '.' ||
			(c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z') || (c >= '0' && c <= '9')
	})

	dot := strings.LastIndex(word, ".")
	if dot < 0 {
		return list(bareCandidates(word, rangeBack(pos, word), scope))
	}

	qualifier, member := word[:dot], word[dot+1:]
	replace := rangeBack(pos, member)
	switch {
	case qualifier == v1.StepsRoot:
		return list(offer(scope.steps, member, replace))
	case qualifier == v1.VarsRoot:
		// The other root, which was reachable by typing it and by nothing else:
		// `vars` was not offered bare and `vars.` fell through to the arm below,
		// which treats an unknown qualifier as a binding and offers nothing. One
		// root answered and one silent, for two names the grammar treats alike.
		return list(offer(scope.vars, member, replace))
	case functionsAfter(qualifier) != nil:
		// A namespace the profile declares — `math.`, `regex.`, `json.`. Checked
		// after the root and before the bare-qualifier fallthrough below, which
		// treats an unknown qualifier as a binding and offers nothing.
		return list(offer(functionsAfter(qualifier), member, replace))
	case strings.HasPrefix(qualifier, v1.StepsRoot+"."):
		id := strings.TrimPrefix(qualifier, v1.StepsRoot+".")
		if strings.Contains(id, ".") {
			// Past the output: selecting into a value whose shape the schema does
			// not describe. There is nothing to offer that would not be a guess.
			return list(nil)
		}
		return list(outputCandidates(id, member, replace, scope.steps))
	}
	// A bare qualifier. Either a binding, whose element type is not known
	// statically, or the retired spelling of a step reference — and offering that
	// one's outputs would keep an author writing a form `flow validate` refuses.
	return list(nil)
}

// bareCandidates offers what may be written bare at the start of an expression:
// the names bound where the cursor is, the root every step hangs from, and then the
// profile's functions.
//
// Bindings come first because they are the nearer thing — bound by the block the
// cursor stands in, where the root spans the whole document — and because inside a
// loop body the item is usually what is wanted.
//
// Functions come last, and there are a lot of them. That ordering is the whole of
// the design decision: an author who knows the name they want types it and the
// prefix filter does the work, while one who does not gets the names in scope first
// rather than having to scroll past sixty functions to find the loop variable they
// bound two lines up.
func bareCandidates(prefix string, replace lsp.Range, scope refScope) []lsp.CompletionItem {
	candidates := slices.Clone(scope.locals)
	candidates = append(candidates, stepsRootCandidate(scope))

	// Both roots, and `vars` only where the file has one. The steps root is offered
	// unconditionally because the first step is written before there is anything to
	// reference and the name is still what an author needs to learn; a `vars:` block
	// that does not exist is a different case — offering the root would be teaching
	// a name that resolves to an empty map.
	if len(scope.vars) > 0 {
		candidates = append(candidates, varsRootCandidate(scope))
	}

	return offer(append(candidates, functionCandidates()...), prefix, replace)
}

// varsRootCandidate describes the workflow's variables root.
func varsRootCandidate(scope refScope) refCandidate {
	return refCandidate{
		name: v1.VarsRoot,
		// A value with named members, the same shape as the steps root.
		kind:   lsp.CIKStruct,
		detail: "workflow variables",
		docs: "The workflow's declared variables, keyed by name: write " + v1.VarsRoot +
			".<name>. They are evaluated once before the first step runs, so every step " +
			"sees the same values. A step's own `vars:` are written bare instead.",
		// The dot comes with it, as the steps root's does.
		insert: v1.VarsRoot + ".",
	}
}

// stepsRootCandidate describes the root itself.
//
// It is offered even when no step is in scope yet, because the first step of a
// file is written before there is anything to reference and the name is still the
// one an author needs to learn.
func stepsRootCandidate(scope refScope) refCandidate {
	docs := "Every step's outputs, keyed by step id: write " + v1.StepsRoot + ".<id>.<output>. " +
		"Only steps that have already run are in scope here."
	if len(scope.steps) == 0 {
		docs = "Every step's outputs, keyed by step id. No step has run at this point, so there " +
			"is nothing to select yet."
	}
	return refCandidate{
		name: v1.StepsRoot,
		// A value with named members, which is what the root is: a map from step
		// id to that step's outputs.
		kind:   lsp.CIKStruct,
		detail: "step outputs",
		docs:   docs,
		// The root is never the whole of a reference, so the dot that continues it
		// is inserted too — the same reason a key is offered with its colon.
		insert: v1.StepsRoot + ".",
	}
}

// offer renders candidates as completion items, keeping the order they arrive in.
func offer(candidates []refCandidate, prefix string, replace lsp.Range) []lsp.CompletionItem {
	var items []lsp.CompletionItem
	for i, c := range candidates {
		if !strings.HasPrefix(c.name, prefix) {
			continue
		}
		text := c.insert
		if text == "" {
			text = c.name
		}
		items = append(items, lsp.CompletionItem{
			Label:         c.name,
			Kind:          c.kind,
			Detail:        c.detail,
			Documentation: plainText(c.docs),
			// The list is already nearest-first, and the nearest name is usually
			// the one being referenced.
			SortText: fmt.Sprintf("%04d", i),
			TextEdit: &lsp.TextEdit{Range: replace, NewText: text},
		})
	}
	return items
}

// outputCandidates offers what one step exposes after `steps.<id>.`.
func outputCandidates(id, prefix string, replace lsp.Range, steps []refCandidate) []lsp.CompletionItem {
	var target *refCandidate
	for i := range steps {
		if steps[i].name == id {
			target = &steps[i]
			break
		}
	}
	if target == nil {
		// Not a step whose outputs exist here. Offering them would suggest a
		// reference the engine rejects.
		return nil
	}

	var items []lsp.CompletionItem
	for i, o := range target.outputs {
		if !strings.HasPrefix(o.name, prefix) {
			continue
		}
		items = append(items, lsp.CompletionItem{
			Label:         o.name,
			Kind:          lsp.CIKField,
			Detail:        o.detail,
			Documentation: plainText(o.docs),
			SortText:      fmt.Sprintf("%04d%s", i, o.name),
			TextEdit:      &lsp.TextEdit{Range: replace, NewText: o.name},
		})
	}
	return items
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
