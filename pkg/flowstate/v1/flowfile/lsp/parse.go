package lsp

import (
	"slices"
	"strings"

	"github.com/goccy/go-yaml/ast"
	"github.com/goccy/go-yaml/parser"
	"github.com/goccy/go-yaml/token"
	v1 "github.com/picatz/flowstate/pkg/flowstate/v1"
	"github.com/picatz/flowstate/pkg/flowstate/v1/flowfile"
	"github.com/sourcegraph/go-lsp"
)

// This file turns a Flowfile's YAML syntax tree into a positional model: the
// same structure the compiler sees, plus the source range of every part of it.
//
// The model is what makes a diagnostic land under the token at fault instead of
// on the whole line. It is built from the parser's own tree rather than by
// searching the text, so a step id that also appears inside a string value
// cannot be mistaken for its declaration.

// Which scalars are expressions is decided by [flowfile.SplitFence], the compiler's
// own rule, rather than by a pattern kept here. The compiler treats a value as an
// expression only when the whole scalar is one, so a looser rule would report CEL
// errors inside strings the engine keeps as literal text.

// valueKind distinguishes the YAML shapes a Flowfile value can take.
type valueKind int

const (
	// kindScalar is a string, number, or boolean.
	kindScalar valueKind = iota
	// kindSequence is a list, in either flow or block style.
	kindSequence
	// kindMapping is a nested map, such as an http task's headers.
	kindMapping
	// kindOther is a shape the DSL does not interpret — an anchor, alias, or
	// tag. It is recorded positionally and otherwise left alone, so that an
	// unusual document produces no diagnostics rather than wrong ones.
	kindOther
)

// A value is one YAML value in a Flowfile together with its source range.
type value struct {
	kind valueKind

	// text is the value as YAML decodes it: quotes removed, escapes resolved.
	// It is what the compiler sees.
	text string

	// textOffset is the byte offset in the document where text begins, which is
	// past the opening quote of a quoted scalar. An input whose whole value is
	// expression source — an input the task evaluates itself, such as `http:`'s
	// `expect:` — is reported against this, so the
	// squiggle lands inside the quotes rather than on them.
	textOffset int

	// rng covers the value exactly as written, including any quotes.
	rng lsp.Range

	items   []*value // sequence elements
	entries []*entry // mapping entries

	// expr holds the source between the fences of a ${...} scalar, with exprRange
	// covering the whole ${...} and exprOffset giving the byte offset where the
	// source itself begins.
	expr       string
	exprRange  lsp.Range
	exprOffset int

	// fenced reports that the scalar is written as one ${...}, whether or not what
	// is inside it parses.
	//
	// Validity is deliberately not recorded alongside it. Whether the fenced text
	// is a valid expression is decided by running the CEL parser, which the
	// diagnostics do anyway — and a broken expression still needs a position, so
	// recognizing the fence has to work without the answer.
	fenced bool

	// inline reports that the decoded text — and so the expr cut out of it —
	// occupies a contiguous run of the document's own bytes, so an offset within
	// it maps to a document offset by adding textOffset or exprOffset. Every
	// consumer doing that arithmetic must ask this first.
	//
	// It is false wherever the parser hands back something it rewrote rather than
	// sliced: a block scalar, a plain scalar written across lines — in both the
	// line breaks have already become spaces before the model sees the text — and
	// a quoted scalar carrying an escape. In each, an offset into the text names a
	// place the document does not have. The honest answer there is the whole of
	// the value's range, or nothing at all. A position computed anyway would land
	// in the middle of some other line, which is the `flow fix` corruption class
	// one surface over: a wrong position is worse than no position, because a
	// wrong one is believed.
	inline bool

	// lineMap places each line of the decoded text back in the document, for a
	// value whose text is not contiguous but whose *lines* still are.
	//
	// A literal block scalar (`|`, `|-`, `|+`) is the case: the decoder strips
	// each content line's indentation and joins the lines with the newlines that
	// were already there, so every byte of the decoded text is still a byte of
	// the document — just not at a fixed distance from the start. One addition
	// per line recovers the position exactly.
	//
	// It is deliberately not `inline`. That flag's contract is that *one*
	// addition works everywhere in the value, and three consumers do that
	// arithmetic on the strength of it; a value with a line map does not satisfy
	// it and must never be handed to code that assumes it does. Ask through
	// [value.exprSpan], [value.textSpan] and [value.exprCursor], which know both
	// shapes, rather than reading either field.
	//
	// Folding is what a line map cannot survive: `>-` turns the breaks into
	// spaces before the model sees the text, so a byte of the decoded text is not
	// a byte of any document line. Those values have neither flag, and get whole
	// ranges or nothing — see #306.
	lineMap []lineSpan
}

// A lineSpan places one line of a value's decoded text in the document.
//
// The three numbers are one line's worth of the same fact: the line's bytes
// begin at textStart in the decoded text, at docStart in the document, and run
// for length bytes with no rewriting in between. A terminator is not covered by
// either offset, because a literal scalar's line break is one byte on both
// sides and lands between spans.
type lineSpan struct {
	textStart int
	docStart  int
	length    int
}

// An entry is one key/value pair of a YAML mapping.
type entry struct {
	key      string
	keyRange lsp.Range

	// value is nil when the key was written without a value, which happens
	// constantly while a document is being typed.
	value *value
}

// valueText returns an entry's decoded scalar text, or the empty string when the
// entry has no scalar value.
func (e *entry) valueText() string {
	if e == nil || e.value == nil {
		return ""
	}
	return e.value.text
}

// valueRange returns the range to underline for a problem with an entry's value,
// falling back to the key when no value was written.
func (e *entry) valueRange() lsp.Range {
	if e == nil {
		return documentStart
	}
	if e.value == nil {
		return e.keyRange
	}
	return e.value.rng
}

// A scopeFrame is one block a step sits inside.
//
// Two rules make the frames necessary rather than decorative, and both are the
// engine's, mirrored from flowfile's validator:
//
//   - A loop body's outputs do not escape the loop. Every iteration would
//     overwrite the last, so the loop reports them through its own `results`
//     output instead, and a step after the loop cannot name a body step.
//   - A parallel block's branch outputs do merge into the enclosing scope once the
//     block joins, so a later step can name them — but one branch cannot name
//     another's, because branches are unordered.
type scopeFrame struct {
	// block is the for_each or parallel step this frame belongs to.
	block *parsedStep

	// branch is the index of the parallel branch, or -1 for a loop body.
	branch int
}

// loopBody reports whether the frame is a for_each body rather than a parallel
// branch.
func (f scopeFrame) loopBody() bool { return f.branch < 0 }

// A parsedStep is one step of a Flowfile with the source range of each part.
type parsedStep struct {
	index int
	id    string

	idEntry *entry

	// descriptionEntry is the author's prose about the step. Nothing executes it,
	// so the model holds it for one reason: it is the best answer there is to "what
	// is this step?", which hover is asked whenever a cursor rests on the id.
	descriptionEntry *entry

	// taskEntry is the step's task, whose *key* is the task's name and whose
	// *value* is its inputs. There is no separate name or inputs entry, because
	// there is no longer anything separate to point at: `http:` with the request
	// under it is one key, and the name and the inputs are its two halves.
	//
	// A diagnostic about the task name therefore ranges over the key, and one
	// about an input over an entry of the value — which is what the positional
	// model existed to provide and is now simply where the source already is.
	taskEntry *entry

	// conditionEntry is the step's `if`, whose value is an expression deciding
	// whether the step runs at all.
	conditionEntry *entry

	// forEachEntry and parallelEntry are two of the other kinds of work a step can
	// be. Exactly one of the kinds [flowfile.StepKinds] names is present in a valid
	// step.
	forEachEntry  *entry
	parallelEntry *entry

	// loopEntry is a `loop:` block, whose `as`, `init`, `update`, `until`,
	// `max_iterations` and `steps` keys are documented at their own level — the same
	// standing forEachEntry gives a `for_each` block.
	loopEntry *entry

	// loopInitEntry, loopUntilEntry and loopUpdateEntry are the loop's own three
	// expressions, resolved once here so that every consumer agrees which entry
	// each key names — the same reason waitUntilEntry is a field rather than a
	// search. They are held apart from the step's other expression entries
	// because each is evaluated in a scope no other position has, which is what
	// [parsedStep.loopScopeOf] answers.
	loopInitEntry   *entry
	loopUntilEntry  *entry
	loopUpdateEntry *entry

	// callEntry is a `call:` and its value: the path of another Flowfile, relative
	// to this file's own directory.
	//
	// Held on the model rather than looked up among the step's other keys because
	// it is the one value in a Flowfile that names a *file*, and go-to-definition
	// follows it — the only definition this language has that lives outside the
	// document. Recording it here is what keeps that answer from being derived by
	// searching for a key called `call`, which is the same mistake `timeout:`
	// taught: a word can be spelled in more than one position, and the model is
	// where the question of which one this is has already been settled.
	callEntry *entry

	// withEntry is a `call:` step's `with:` block, whose *keys* are the names of
	// the callee's declared inputs and whose *values* are the arguments bound to
	// them.
	//
	// Held beside callEntry because the two are one construct: a `with:` key
	// means nothing without the target that says what may be written there, and
	// hover on such a key is answered out of the callee's own declarations. It
	// is not among the step's expression entries, because the values are checked
	// against the callee's declared types by the compiler and are described by
	// nothing this model holds.
	withEntry *entry

	// waitUntilEntry is a wait whose value is an expression naming the moment to
	// wait for.
	//
	// It is here because it carries an expression, which is the only thing this
	// model has ever recorded an entry for. Its absence read as a decision — the
	// comment above said the wait kinds carried no expressions of their own — and
	// it was simply wrong: `wait_until:` is one, and leaving it out cost the
	// feature everywhere expressions are handled. Hover and go-to-definition
	// stopped at the fence, and a CEL error in it was reported by the validator
	// against the position it could work out rather than at the character at
	// fault.
	//
	// Rooting is what made that expensive rather than merely untidy. A wait now
	// commonly holds `${steps.<id>.<output>}` — a moment that arrived as data —
	// so the one kind of step whose expression the editor could not read is the
	// one whose expression most often names another step.
	waitUntilEntry *entry

	// sleepEntry is a `sleep:` whose value is written as an expression, the
	// computed-duration form the compiler recognizes by the fence
	// (flowfile's computedDuration: `sleep: 30s` is a literal and
	// `sleep: ${inputs.grace}` is an expression, with nothing between them
	// ambiguous). Only the expression form is recorded, because a literal
	// duration holds no expression for hover, diagnostics, or the clock binding
	// to attach to; the key itself is documented from the DSL table either way.
	//
	// It is here for the reason waitUntilEntry is: the engine binds the clock
	// into every one of a wait's expressions, not only `wait_until:` (see
	// [parsedStep.bindsNow]), and an entry this model does not hold is an
	// expression no editor surface can see.
	sleepEntry *entry

	// waitTimeoutEntry is a `wait_for_signal:`'s own `timeout:`, again only in
	// its expression form and by the same fence rule as sleepEntry. Kept apart
	// from the step's timeoutEntry for the reason waitForSignalEntry is: the two
	// are spelled the same and mean different things: the step's bounds one
	// attempt and is no wait expression, while this one is evaluated by the
	// engine with the clock bound in it.
	waitTimeoutEntry *entry

	// varsEntry is the step's own `vars:` block, whose *values* are expressions and
	// whose *keys* are the bare names those values are bound to.
	//
	// It is the step's, whatever kind of step it is: a `vars:` on a `for_each` binds
	// names for the loop's `items:` and its whole body, and one on a task step binds
	// them for that step's inputs. Same key, same level, one entry.
	varsEntry *entry

	// itemsEntry is a for_each block's `items`, whose value is an expression
	// producing the list to iterate.
	itemsEntry *entry

	// parent is the step whose for_each or parallel block contains this one, or
	// nil for a step at the top level.
	parent *parsedStep

	// scope is the chain of blocks this step sits inside, outermost first. It is
	// what decides which other steps' outputs it may reference.
	scope []scopeFrame

	// timeoutEntry and retryEntry are the step's execution policy. The durations
	// inside them are checked, but nothing else interprets them.
	timeoutEntry *entry
	retryEntry   *entry

	// waitForSignalEntry is a gate written in its mapping form, whose own `name`
	// and `timeout` keys are documented at their own level.
	//
	// Kept apart from the step's `timeout` deliberately: the two are spelled the
	// same and mean different things — the step's bounds one attempt at it, and a
	// gate's bounds how long it waits before reporting `timed_out`. Without this,
	// hovering the one inside a gate answered with the documentation for the other,
	// which is the kind of wrong answer that is worse than no answer.
	waitForSignalEntry *entry

	// waitShapingEntries are the values of a `wait_for_signal:`'s `outputs:` block,
	// which are expressions evaluated in a scope no other position has: the wait's
	// own result, bound bare. Held apart from every other `outputs:` in the
	// language — the workflow's declared outputs and the http task's own shaping
	// are spelled the same and bind none of these — because what a bare `payload`
	// means is decided by which of the three this is.
	waitShapingEntries []*entry

	// taskName is the task the step invokes, empty when not written. It is the
	// task entry's key.
	taskName string

	// inputs are the entries of the step's inputs mapping, in source order.
	inputs []*entry

	// entries are the step's own keys, in source order, including any this
	// package does not interpret. Hover reads them so that a key the DSL gains
	// tomorrow is at worst undocumented rather than unrecognized.
	entries []*entry

	// rng covers the whole step, for the document outline.
	rng lsp.Range
}

// expressionEntries returns every entry of a step whose value may contain a
// ${...} expression, so that hover, go-to-definition, and expression diagnostics
// all cover the same places and cannot fall out of step with each other.
func (s *parsedStep) expressionEntries() []*entry {
	entries := make([]*entry, 0, len(s.inputs)+3)
	if s.conditionEntry != nil {
		entries = append(entries, s.conditionEntry)
	}
	if s.itemsEntry != nil {
		entries = append(entries, s.itemsEntry)
	}
	if s.waitUntilEntry != nil {
		entries = append(entries, s.waitUntilEntry)
	}
	if s.sleepEntry != nil {
		entries = append(entries, s.sleepEntry)
	}
	if s.waitTimeoutEntry != nil {
		entries = append(entries, s.waitTimeoutEntry)
	}
	entries = append(entries, s.waitShapingEntries...)

	// A loop's own `init:`, `until:` and `update:`, which are expressions
	// evaluated in scopes no other position has: `init:` against the enclosing
	// scope alone, before the state it defines exists, and the other two after
	// the body each iteration, with the carried state bound bare and the body's
	// top-level steps readable. They were deliberately absent while every
	// consumer of this list assumed the *step's* scope — walked that way, a
	// carried name colliding with a profile function would have been described
	// as that function. Each consumer now asks [parsedStep.loopScopeOf] which
	// scope an entry's expression is evaluated in, which is what made adding
	// them honest — see #306.
	for _, e := range []*entry{s.loopInitEntry, s.loopUntilEntry, s.loopUpdateEntry} {
		if e != nil {
			entries = append(entries, e)
		}
	}

	// A step's `vars:` bindings, which are expressions like any other value here and
	// were missing from this list until the retirement edition made them the place
	// most expressions are written. An author who moved a value out of a `cel:` step
	// — which this list did cover, through the task inputs — lost hover, go to
	// definition, and the syntax squiggle in the same edit.
	entries = append(entries, nestedEntries(s.varsEntry)...)

	return append(entries, s.inputs...)
}

// bindsNow reports whether an entry is one the engine binds the clock into.
//
// The set is the validator's, which takes it from where the engine evaluates
// waits (flowfile's validateWait): all three of a wait's own expressions
// (`wait_until:`, an expression-valued `sleep:`, and a signal's `timeout:`) plus
// a signal's `outputs:` shaping, whose scope validateWait builds from the
// waiting one, so `now` is bound there alongside the wait's result. The clock is
// the node kind's, not one field's, and answering for fewer entries than the
// validator accepts is the drift #319 pinned.
//
// It is asked of the entry rather than of its key, because a key is a word and
// two different things can be spelled the same: `wait_until:` under a step is the
// wait the grammar defines, and the model has already resolved which entry that
// is. Comparing entries is what keeps this from answering yes for a task input
// that happens to share the name — the same class of mistake waitForSignalEntry
// exists to prevent for `timeout:`.
//
// Completion answers the same question from the line scan instead, in bindsClock,
// because it is asked for while a document does not parse and there is no model
// then to ask. One rule, two readers: a change to either belongs in both.
func (s *parsedStep) bindsNow(e *entry) bool {
	if e == nil {
		return false
	}
	return e == s.waitUntilEntry || e == s.sleepEntry || e == s.waitTimeoutEntry ||
		s.bindsWaitResult(e)
}

// bindsWaitResult reports whether an entry is one of a `wait_for_signal:`'s
// `outputs:` values, which is the only position the wait's own result —
// `payload`, `sender`, `timed_out` — is bound bare.
//
// Asked of the entry rather than of the key for the reason [parsedStep.bindsNow]
// is: `outputs:` is a word three different blocks use, and only this one binds
// these names. Answering yes for the workflow's declared outputs would document a
// name there as though it resolved, which it does not.
func (s *parsedStep) bindsWaitResult(e *entry) bool {
	return e != nil && slices.Contains(s.waitShapingEntries, e)
}

// A loopScope names which of a loop's own scopes an expression is evaluated in.
// It is the tag that made the loop's three keys admissible to
// [parsedStep.expressionEntries]: the engine evaluates each against a different
// scope, and a consumer that assembled names without asking would describe a
// binding that does not exist at that position.
//
// The rules are the engine's, mirrored from flowfile's validator (which mirrors
// pkg/flowstate/v1/loop.go): `init:` runs once before the loop, against the
// enclosing scope — it is *defining* the carried state, so the name is not
// bound in it, and no body step has run. `until:` and `update:` run after the
// body each iteration, so they read the enclosing scope, the carried state
// under its bare `as:` name (a loop without `as:` carries nothing and binds
// nothing), and the body's own top-level steps.
type loopScope int

const (
	// loopScopeNone is every entry that is not one of a loop's three keys: the
	// step's own scope applies unchanged.
	loopScopeNone loopScope = iota
	// loopScopeOuter is `init:` — the enclosing scope, with neither the carried
	// state nor any body step in it.
	loopScopeOuter
	// loopScopeAfterBody is `until:` and `update:` — the enclosing scope plus
	// the carried state bound bare plus the body's top-level steps.
	loopScopeAfterBody
)

// loopScopeOf reports which scope an entry's expression is evaluated in.
//
// Asked of the entry rather than of its key, the rule [parsedStep.bindsNow] is
// written to: `until:` under a `loop:` is the loop's stop condition, and a task
// input spelled the same is resolved in a scope with no carried state in it.
func (s *parsedStep) loopScopeOf(e *entry) loopScope {
	switch {
	case e == nil:
		return loopScopeNone
	case e == s.loopInitEntry:
		return loopScopeOuter
	case e == s.loopUntilEntry || e == s.loopUpdateEntry:
		return loopScopeAfterBody
	}
	return loopScopeNone
}

// loopScopeAt reports which of a loop's own scopes a position's expression is
// evaluated in, for the one consumer — completion — that starts from a position
// rather than from a walk over the entries.
func (s *parsedStep) loopScopeAt(pos lsp.Position) loopScope {
	for _, e := range []*entry{s.loopInitEntry, s.loopUntilEntry, s.loopUpdateEntry} {
		if e != nil && e.value != nil && contains(e.value.rng, pos) {
			return s.loopScopeOf(e)
		}
	}
	return loopScopeNone
}

// kind names the kind of work a step does, for the outline and for diagnostics
// that need to say what a step is.
// kind names the kind of work a step does, in the grammar's own spelling.
//
// Two surfaces read it, and they are why the answer has to cover every kind the
// model records rather than the three it grew up with. The outline's second
// column is this string, so a `call:` or a gate with a blank there reads as a
// step the tool does not understand. And hover and completion write it into a
// sentence — "a variable the enclosing X declares" — where a missing word is
// not a blank cell but a malformed sentence: `loop` is the main enclosing block
// that declares `vars:`, and hovering a loop-scoped variable rendered
// "the enclosing  declares" for as long as this switch stopped at three.
//
// The empty default is the honest answer for the two shapes this switch does
// not name: `sleep:`, whose expression form the model now records in
// [parsedStep.sleepEntry] for the expression it holds, but whose literal form
// is one duration with nothing under it, and a gate written in its scalar
// form (`wait_for_signal: name`), which [parsedStep.waitForSignalEntry]
// deliberately does not hold. Neither can enclose anything or declare a var,
// so neither can reach the sentence above; the blank is confined to the
// outline column, where it costs a label rather than the grammar of a
// sentence.
func (s *parsedStep) kind() string {
	switch {
	case s.taskEntry != nil:
		return "task"
	case s.forEachEntry != nil:
		return "for_each"
	case s.loopEntry != nil:
		return "loop"
	case s.parallelEntry != nil:
		return "parallel"
	case s.callEntry != nil:
		return "call"
	case s.waitUntilEntry != nil:
		return "wait_until"
	case s.waitForSignalEntry != nil:
		return "wait_for_signal"
	default:
		return ""
	}
}

// input returns the step's input entry with the given name.
func (s *parsedStep) input(name string) *entry {
	for _, in := range s.inputs {
		if in.key == name {
			return in
		}
	}
	return nil
}

// entryForField returns the entry a validator diagnostic's field names.
//
// A field is either a task input or one of the step's own properties — `if`,
// `timeout`, a key inside `retry` or `for_each`. flowfile distinguishes the two in
// its message but not in the field name, so both are searched here, inputs first:
// a task input shadowing a property name is the case where the author is looking at
// the input.
func (s *parsedStep) entryForField(field string) *entry {
	if in := s.input(field); in != nil {
		return in
	}
	// A `wait_for_signal:`'s shaping entries, which the validator names as
	// `outputs.<name>` — a dotted field, because the entry's own key alone would
	// collide with the http task's `outputs:` input. The prefixed lookup is what
	// lets a bad reference inside one land on that expression instead of on the
	// step (#318); the entries are only ever populated on a gate written in its
	// mapping form, so a task input spelled `outputs.x` cannot reach them.
	if name, isShaping := strings.CutPrefix(field, "outputs."); isShaping {
		for _, e := range s.waitShapingEntries {
			if e.key == name {
				return e
			}
		}
	}

	// The gate's own `timeout:`, named with its full path by the validator so
	// it can never be confused with the step-level key spelled the same. The
	// qualified lookup resolves inside the gate's mapping only.
	if name, isWait := strings.CutPrefix(field, "wait_for_signal."); isWait {
		for _, e := range nestedEntries(s.waitForSignalEntry) {
			if e.key == name {
				return e
			}
		}
	}

	// The gate's own keys come last so that a step-level key spelled the same —
	// the step's `timeout:`, which bounds an attempt rather than the wait —
	// resolves first, exactly as hover keeps the two apart.
	for _, group := range [][]*entry{s.entries, nestedEntries(s.forEachEntry), nestedEntries(s.loopEntry), nestedEntries(s.retryEntry), nestedEntries(s.waitForSignalEntry)} {
		for _, e := range group {
			if e.key == field {
				return e
			}
		}
	}
	return nil
}

// nestedEntries returns the entries of an entry's mapping value, or nil.
func nestedEntries(e *entry) []*entry {
	if e == nil || e.value == nil {
		return nil
	}
	return e.value.entries
}

// A parsedFile is the positional model of a whole Flowfile.
type parsedFile struct {
	nameEntry  *entry
	stepsEntry *entry
	steps      []*parsedStep

	// varsEntry is the document's own `vars:` block. Its values are expressions
	// evaluated before the first step runs, so they belong to the file rather than
	// to any step — which is why they are held here and not reachable through
	// [parsedFile.stepAt].
	varsEntry *entry

	// entries are the document's own keys, in source order, including any this
	// package does not interpret — the same reason parsedStep keeps its own: a key
	// the DSL gains tomorrow is then undocumented rather than unreachable. It is
	// what hover resolves above `steps:`, where there is no step to ask.
	entries []*entry
}

// expressionEntries returns the document's own entries whose values may contain a
// ${...} expression.
//
// One entry today, and a method rather than a field read for the same reason the
// step has one: everything that walks expressions asks the model where they are,
// so a position the DSL gains tomorrow reaches hover, definition and diagnostics
// together or not at all.
func (p *parsedFile) expressionEntries() []*entry {
	if p == nil {
		return nil
	}

	return nestedEntries(p.varsEntry)
}

// step returns the step with the given id, preferring the first declaration so
// that a duplicate id resolves the same way the engine's first write does.
func (p *parsedFile) step(id string) *parsedStep {
	if p == nil || id == "" {
		return nil
	}
	for _, s := range p.steps {
		if s.id == id {
			return s
		}
	}
	return nil
}

// stepAt returns the step whose source range contains pos.
func (p *parsedFile) stepAt(pos lsp.Position) *parsedStep {
	if p == nil {
		return nil
	}
	for _, s := range p.steps {
		if contains(s.rng, pos) {
			return s
		}
	}
	return nil
}

// parseFlowfile builds the positional model of a Flowfile.
//
// A syntax error is returned rather than partially modeled: the parser reports
// one error with a token, which is a better diagnostic than anything that could
// be reconstructed from a half-built tree.
func parseFlowfile(text string, ix *lineIndex) (*parsedFile, error) {
	file, err := parser.ParseBytes([]byte(text), 0)
	if err != nil {
		return nil, err
	}

	p := &parsedFile{}
	for _, doc := range file.Docs {
		if doc.Body == nil {
			continue
		}
		for _, mv := range mappingValues(doc.Body) {
			e := buildEntry(mv, ix)
			if e == nil {
				continue
			}
			p.entries = append(p.entries, e)
			switch e.key {
			case "name":
				if p.nameEntry == nil {
					p.nameEntry = e
				}
			case "vars":
				if p.varsEntry == nil && e.value != nil && e.value.kind == kindMapping {
					p.varsEntry = e
				}
			case "steps":
				if p.stepsEntry == nil {
					p.stepsEntry = e
					p.steps = buildSteps(e, ix)
				}
			}
		}
		// Only the first document of a multi-document file is a Flowfile; the
		// compiler unmarshals into a single struct.
		break
	}
	return p, nil
}

// buildSteps converts a steps sequence into the step model.
//
// Steps nest: a for_each block repeats a body of steps and a parallel block runs
// branches of them. They are flattened into one list in document order rather than
// kept as a tree, because every feature here asks the same two questions — which
// step is at this position, and which steps come before it — and both are answered
// by document order at any depth.
func buildSteps(steps *entry, ix *lineIndex) []*parsedStep {
	var out []*parsedStep
	collectSteps(steps, nil, nil, ix, &out)
	assignStepRanges(ix, out, steps.keyRange.Start.Character)
	return out
}

// collectSteps appends the steps of one sequence, and the steps nested inside them,
// to out.
func collectSteps(steps *entry, parent *parsedStep, scope []scopeFrame, ix *lineIndex, out *[]*parsedStep) {
	if steps == nil || steps.value == nil || steps.value.kind != kindSequence {
		return
	}
	for _, item := range steps.value.items {
		if item.kind != kindMapping {
			continue
		}
		s := &parsedStep{
			index:   len(*out),
			parent:  parent,
			scope:   scope,
			rng:     item.rng,
			entries: item.entries,
		}
		fillParsedStep(s, item.entries)
		*out = append(*out, s)

		// A nested body's steps follow their parent in document order, which is
		// also the order they run in. Each carries a frame naming the block it is
		// inside, which is what the visibility rules are expressed against.
		//
		// [slices.Clip] before each append, and it is load-bearing rather than
		// tidy. `scope` is retained by every step built below — `parsedStep.scope`
		// — so `append(scope, frame)` on a slice with spare capacity hands two
		// siblings the same backing array, and the second one's frame overwrites
		// the first one's *in a slice somebody already kept*. Sibling A ends up
		// recorded as being inside sibling B.
		//
		// It is not a deep-nesting curiosity, though it looks like one: it happens
		// exactly when the incoming scope has room to spare, which follows append's
		// growth, so it comes and goes with depth — clean at 2, 3, 5 and 9, broken
		// at 4, 6, 7, 8 and everything from 10 up. `TestReferenceScoping` covered
		// depth 1, which is one of the depths that happens to be safe.
		//
		// What it costs is the rule this whole file exists to mirror. At depth 4,
		// one parallel branch was offered its sibling's step ids and one loop body
		// was offered a different loop body's — names the validator on the same
		// file rejects with `references unknown step`.
		if s.forEachEntry != nil && s.forEachEntry.value != nil {
			for _, fe := range s.forEachEntry.value.entries {
				if fe.key == "steps" {
					collectSteps(fe, s, append(slices.Clip(scope), scopeFrame{block: s, branch: -1}), ix, out)
				}
			}
		}
		if s.parallelEntry != nil && s.parallelEntry.value != nil {
			for i, branch := range s.parallelEntry.value.items {
				for _, be := range branch.entries {
					if be.key == "steps" {
						collectSteps(be, s, append(slices.Clip(scope), scopeFrame{block: s, branch: i}), ix, out)
					}
				}
			}
		}
		// A `loop:` body, under the same frame shape a `for_each` body gets,
		// because the visibility rules are the same rule: body outputs do not
		// escape, and the body reads a binding the block declares bare. This
		// recursion was missing for as long as `loop:` existed, which made the
		// whole body invisible to the model — its steps absent from the outline,
		// its expressions attributed to nothing, and hover, completion and
		// definition all silent inside it while the validator resolved every one
		// of those names happily. Not a wrong answer, which would at least be
		// noticed, but no answer, which reads as the tool having nothing to say.
		if s.loopEntry != nil && s.loopEntry.value != nil {
			for _, le := range s.loopEntry.value.entries {
				if le.key == "steps" {
					collectSteps(le, s, append(slices.Clip(scope), scopeFrame{block: s, branch: -1}), ix, out)
				}
			}
		}
	}
}

// visibleFrom reports whether the outputs of target can be referenced by from.
//
// This mirrors flowfile's validator rather than inventing a rule: offering or
// resolving a name the engine will reject is the one thing this package must never
// do, and a rule maintained twice is a rule that will eventually differ.
func visibleFrom(target, from *parsedStep) bool {
	if target == nil || from == nil || target == from {
		return false
	}
	// A step can only reference something that has already run.
	if target.index >= from.index {
		return false
	}

	// An enclosing block is still running while a step inside it runs, so its
	// outputs do not exist yet — a loop body cannot read the loop's own results,
	// and a branch cannot read the parallel block it belongs to. Document order
	// alone would allow both, since the block opens above its contents.
	for a := from.parent; a != nil; a = a.parent {
		if a == target {
			return false
		}
	}

	for _, tf := range target.scope {
		if tf.loopBody() {
			// Inside a loop body: visible only to steps in that same body.
			if !containsFrame(from.scope, tf) {
				return false
			}
			continue
		}
		// Inside a parallel branch: visible after the block joins, but not to a
		// sibling branch.
		for _, ff := range from.scope {
			if ff.block == tf.block && ff.branch != tf.branch {
				return false
			}
		}
	}
	return true
}

// visibleFromEntry reports whether the outputs of target can be referenced by
// an expression of from that is evaluated in scope ls — [visibleFrom] with the
// one addition a loop's after-body scope has.
//
// `until:` and `update:` see the same steps a hypothetical last step of the
// loop body would: everything the enclosing scope sees, plus the body's own
// top-level steps — the ones whose parent is the loop itself. A step nested
// deeper inside the body belongs to an inner block whose outputs do not escape
// it, which is why the parent link is the whole test: the validator collects
// only the body's top-level ids, and one collection cannot disagree with
// itself when both read the same tree the same way.
func visibleFromEntry(target, from *parsedStep, ls loopScope) bool {
	if ls == loopScopeAfterBody && target != nil && from != nil && target.parent == from {
		return true
	}
	return visibleFrom(target, from)
}

// containsFrame reports whether scope includes the given frame.
func containsFrame(scope []scopeFrame, want scopeFrame) bool {
	for _, f := range scope {
		if f.block == want.block && f.branch == want.branch {
			return true
		}
	}
	return false
}

// iteratorsInScope returns the loop iterator variables a step may reference, from
// the innermost loop outwards.
//
// Inside a loop body the current item is bound under the loop's iterator name, so
// it is a name that resolves and belongs in completion — but only in the body.
func (s *parsedStep) iteratorsInScope() []*parsedStep {
	var loops []*parsedStep
	for i := len(s.scope) - 1; i >= 0; i-- {
		if s.scope[i].loopBody() {
			loops = append(loops, s.scope[i].block)
		}
	}
	return loops
}

// iteratorName returns the variable a block binds bare for its body — the item a
// `for_each` yields, or the value a `loop:` carries.
//
// The defaults differ, and the difference is the engine's, checked against
// `flow validate` rather than assumed: a `for_each` that writes no `as:` binds
// [v1.DefaultIterator], but a `loop:` that writes no `as:` binds *nothing* —
// stateless or stateful alike, `${item}` in its body is an unknown name.
// Sharing the fallback read as symmetry and was an invention: it would have
// offered, resolved, and documented a binding the validator rejects, which is
// the one thing this package must never do.
func (s *parsedStep) iteratorName() string {
	if s.forEachEntry != nil && s.forEachEntry.value != nil {
		if name := blockAs(s.forEachEntry); name != "" {
			return name
		}
		return v1.DefaultIterator
	}
	if s.loopEntry != nil && s.loopEntry.value != nil {
		return blockAs(s.loopEntry)
	}
	return ""
}

// blockAs reads a block's `as:` name, or "" when it writes none.
func blockAs(block *entry) string {
	for _, e := range block.value.entries {
		if e.key == "as" {
			if name := e.valueText(); name != "" {
				return name
			}
		}
	}
	return ""
}

// loopResultsOutput is the single output a for_each step reports.
//
// A loop is not a task, so it has no TaskDef and no descriptor to read this from;
// the name is written in v1.LoopOutputs. It is the one output name in this package
// that the schema cannot supply — see the accompanying report about exporting it.
const loopResultsOutput = "results"

// fillParsedStep reads one step's keys into the model.
func fillParsedStep(s *parsedStep, entries []*entry) {
	// Asked once for the whole step, because whether an unregistered key names a
	// task depends on what else the step says — see flowfile.StepTaskKeys.
	keys := make([]string, len(entries))
	for i, e := range entries {
		keys[i] = e.key
	}
	taskKeys := flowfile.StepTaskKeys(keys)

	for _, e := range entries {
		switch e.key {
		case "id":
			if s.idEntry == nil {
				s.idEntry = e
				s.id = e.valueText()
			}
		case "description":
			if s.descriptionEntry == nil {
				s.descriptionEntry = e
			}
		case "if":
			if s.conditionEntry == nil {
				s.conditionEntry = e
			}
		case "vars":
			// Only the mapping form has anything under it. `vars: something` is a
			// mistake the validator reports, and a model holding it as a block would
			// have nothing to put in the block.
			if s.varsEntry == nil && e.value != nil && e.value.kind == kindMapping {
				s.varsEntry = e
			}
		case "timeout":
			if s.timeoutEntry == nil {
				s.timeoutEntry = e
			}
		case "retry":
			if s.retryEntry == nil {
				s.retryEntry = e
			}
		case "wait_for_signal":
			// Only the mapping form has keys of its own; `wait_for_signal: name`
			// is a scalar and is documented at the step level like any other key.
			if s.waitForSignalEntry == nil && e.value != nil && e.value.kind == kindMapping {
				s.waitForSignalEntry = e
				for _, we := range e.value.entries {
					switch we.key {
					case "outputs":
						s.waitShapingEntries = nestedEntries(we)
					case "timeout":
						// Only the expression form, by the same fence rule
						// `sleep:` follows below: a literal duration carries no
						// expression, and the key's own documentation comes from
						// the DSL table at this level either way.
						if s.waitTimeoutEntry == nil && we.value != nil && we.value.fenced {
							s.waitTimeoutEntry = we
						}
					}
				}
			}
		case "sleep":
			// Only the expression form has anything for this model to hold. The
			// fence decides, which is the compiler's own rule (computedDuration):
			// `sleep: 30s` is a literal duration with no expression in it, and
			// recording it would put a non-expression in expressionEntries.
			if s.sleepEntry == nil && e.value != nil && e.value.fenced {
				s.sleepEntry = e
			}
		case "wait_until":
			// A case of its own rather than falling through to the task branch
			// below, which is where it used to land — harmlessly, because
			// flowfile.StepTaskKeys refuses to promote a word the step grammar
			// speaks for, but with the expression it holds going unrecorded.
			//
			// It opens no level of its own: the value is one expression, not a
			// mapping with keys under it, so nothing here has a `wait_until` level
			// to document and `dslKeyAt` is deliberately left alone.
			if s.waitUntilEntry == nil {
				s.waitUntilEntry = e
			}
		case "parallel":
			if s.parallelEntry == nil {
				s.parallelEntry = e
			}
		case "for_each":
			if s.forEachEntry != nil {
				continue
			}
			s.forEachEntry = e
			if e.value == nil || e.value.kind != kindMapping {
				continue
			}
			for _, fe := range e.value.entries {
				if fe.key == "items" && s.itemsEntry == nil {
					s.itemsEntry = fe
				}
			}
		case "loop":
			if s.loopEntry != nil {
				continue
			}
			s.loopEntry = e
			if e.value == nil || e.value.kind != kindMapping {
				continue
			}
			for _, le := range e.value.entries {
				switch le.key {
				case "init":
					if s.loopInitEntry == nil {
						s.loopInitEntry = le
					}
				case "until":
					if s.loopUntilEntry == nil {
						s.loopUntilEntry = le
					}
				case "update":
					if s.loopUpdateEntry == nil {
						s.loopUpdateEntry = le
					}
				}
			}
		case "call":
			// Only the scalar form names a file. `call:` written as a mapping is a
			// mistake the validator reports, and there would be no path in it to
			// resolve.
			if s.callEntry == nil && e.value != nil && e.value.kind == kindScalar {
				s.callEntry = e
			}
		case "with":
			// Only the mapping form binds arguments. `with:` written as a scalar
			// is a mistake the validator reports, and there would be no keys in
			// it to describe.
			if s.withEntry == nil && e.value != nil && e.value.kind == kindMapping {
				s.withEntry = e
			}
		default:
			// A task — decided by flowfile.StepTaskKeys, the same call the compiler
			// makes, so the editor underlines the token `flow validate` names.
			//
			// Asking the registry instead would be almost right and wrong exactly
			// where it matters: an *unregistered* name is the case the unknown-task
			// diagnostic exists for, and a model that does not hold it has nowhere
			// to put that diagnostic but the step. A step property can never reach
			// here — those have their own cases above — and v1.ReservedStepKeys
			// keeps a task from ever being named like one.
			if s.taskEntry != nil {
				continue
			}
			if !slices.Contains(taskKeys, e.key) {
				continue
			}
			s.taskEntry = e
			s.taskName = e.key
			if e.value != nil && e.value.kind == kindMapping {
				s.inputs = e.value.entries
			}
		}
	}
}

// assignStepRanges gives each step the lines it occupies.
//
// A step's range ends where the next one begins, in document order. The parser's
// node extents do not line up with the source for block mappings, and an outline
// that stops short of a step's last line makes an editor's breadcrumb flicker as
// the cursor moves, so the extent is taken from the neighbours instead.
//
// A step containing nested steps therefore ends where its first child begins. That
// is what makes the ranges disjoint, which is what lets a position inside a loop
// body resolve to the body's step rather than to the loop.
//
// The *last* step has no neighbour to end at, and ending it at the document instead
// gave it everything written below `steps:`. A Flowfile's keys are unordered, so
// `vars:` or `edition:` at the bottom is an ordinary file — and every one of its
// lines then belonged to the last step. The cost is not cosmetic: `stepAt` is what
// completion asks which step the cursor is in, so a trailing `vars:` block was
// offered the last step's scope and answered with step ids and a loop iterator,
// which the validator rejects on that exact line with "a var may not read a step".
// Hover went the other way and stopped answering at all, because `hoverAt` takes
// the step branch and never reaches the document keys — so `edition:` written last
// documented itself and written first did not.
//
// stepsIndent is the column of the `steps:` key. Everything belonging to the block
// is indented past it, so a line at or left of it ends the block — which is the
// same rule YAML itself used to decide the line was not part of it.
func assignStepRanges(ix *lineIndex, steps []*parsedStep, stepsIndent int) {
	for i, s := range steps {
		var endLine int
		if i+1 < len(steps) {
			endLine = steps[i+1].rng.Start.Line - 1
		} else {
			// Forward from the step, not back from the document. Walking back finds
			// the *inside* of a trailing block first — `greeting:` under a trailing
			// `vars:` is indented exactly like a step's own lines — and stops there,
			// having skipped only the top-level key. Going forward stops at that key,
			// which is where the steps block actually ends.
			endLine = s.rng.Start.Line
			for next := endLine + 1; next < ix.lineCount() && withinBlock(ix.line(next), stepsIndent); next++ {
				endLine = next
			}
		}
		for endLine > s.rng.Start.Line && strings.TrimSpace(ix.line(endLine)) == "" {
			endLine--
		}
		s.rng = lsp.Range{
			Start: lsp.Position{Line: s.rng.Start.Line, Character: 0},
			End:   lsp.Position{Line: endLine, Character: utf16Len(ix.line(endLine))},
		}
	}
}

// withinBlock reports whether a line belongs to a block whose key sits at indent.
//
// A blank line is inside it: blank lines appear between steps constantly and a step
// that stopped at one would have a hole in the middle of it. The caller trims the
// trailing blanks afterwards, so a run of them at the end of the file is not kept.
func withinBlock(line string, indent int) bool {
	trimmed := strings.TrimLeft(line, " \t")
	if trimmed == "" {
		return true
	}

	return len(line)-len(trimmed) > indent
}

// buildEntry converts one mapping entry, returning nil when its key is not a
// plain scalar — a complex key cannot name a Flowfile field.
func buildEntry(mv *ast.MappingValueNode, ix *lineIndex) *entry {
	if mv == nil || mv.Key == nil {
		return nil
	}
	keyTok := mv.Key.GetToken()
	if keyTok == nil || keyTok.Position == nil {
		return nil
	}
	start := ix.offsetOfYAML(keyTok.Position.Line, keyTok.Position.Column)
	raw := mv.Key.String()
	return &entry{
		key:      keyTok.Value,
		keyRange: ix.rangeOfOffsets(start, start+len(raw)),
		value:    buildValue(mv.Value, ix),
	}
}

// buildValue converts a value node, recording its range and recursing into
// sequences and mappings.
func buildValue(n ast.Node, ix *lineIndex) *value {
	if n == nil {
		return nil
	}
	tok := n.GetToken()
	if tok == nil || tok.Position == nil {
		return nil
	}

	switch t := n.(type) {
	case *ast.MappingNode, *ast.MappingValueNode:
		entries := mappingValues(n)
		v := &value{kind: kindMapping, entries: make([]*entry, 0, len(entries))}
		for _, mv := range entries {
			if e := buildEntry(mv, ix); e != nil {
				v.entries = append(v.entries, e)
			}
		}
		v.rng = spanOf(v.entries, nil, ix, tok)
		return v

	case *ast.SequenceNode:
		v := &value{kind: kindSequence, items: make([]*value, 0, len(t.Values))}
		for _, item := range t.Values {
			if iv := buildValue(item, ix); iv != nil {
				v.items = append(v.items, iv)
			}
		}
		v.rng = spanOf(nil, v.items, ix, tok)
		return v

	case *ast.NullNode:
		// An unwritten value: `name:` with nothing after it. Treated as absent
		// so that features keyed on "has a value" behave while typing.
		return nil

	case *ast.AnchorNode, *ast.AliasNode, *ast.TagNode:
		start := ix.offsetOfYAML(tok.Position.Line, tok.Position.Column)
		return &value{kind: kindOther, rng: ix.rangeOfOffsets(start, start+len(tok.Value))}

	case *ast.LiteralNode:
		// A block scalar. Its content spans lines, and the parser's reconstruction
		// of it is not the exact source slice, so its length cannot be used to
		// measure a range and no *single* offset maps it back to the document.
		// That is what `inline` being false records.
		//
		// What it does not lack is being an expression. The compiler reads a
		// block scalar's decoded text exactly as it reads any other scalar's, so
		// `message: |-` with `${greeting}` under it evaluates and a broken one
		// fails `flow validate` at the header's position. Recognizing the fence
		// is therefore the same question here as anywhere, asked with the same
		// rule.
		//
		// How much of a position is available depends on which block scalar it
		// is, and the difference is the folding. A literal one (`|`) never joins
		// two lines: the decoder strips each line's indentation and keeps the
		// breaks, so every byte of the decoded text is still a byte of the
		// document and one addition *per line* finds it — that is the lineMap. A
		// folded one (`>`) turns the breaks into spaces before the model sees the
		// text, and no arithmetic recovers what was joined; it keeps the whole
		// range and nothing finer.
		v := &value{kind: kindScalar}
		if t.Value != nil {
			v.text = t.Value.Value
		}
		start := ix.offsetOfYAML(tok.Position.Line, tok.Position.Column)
		v.textOffset = start
		v.rng = blockScalarRange(ix, start, t)
		if t.Value != nil && strings.HasPrefix(tok.Value, literalIndicator) {
			v.lineMap = literalLineMap(ix, max(tok.Position.Line-1, 0), t.Value.GetToken().Origin, v.text)
		}
		if source, ok := flowfile.SplitFence(v.text); ok && source != "" {
			v.fenced = true
			v.expr = source
			// The range stays the whole of the value's lines. It is what a
			// diagnostic falls back to when a position cannot be mapped, and it
			// is the gate a consumer asks before looking for a cursor — both want
			// the generous answer, since the exact one is [value.exprCursor]'s
			// job and it declines a position it cannot place.
			v.exprRange = v.rng
			// exprOffset is deliberately left zero: no single offset would be
			// correct for either kind of block scalar, and a plausible-looking
			// one — the header's start — is exactly what a consumer that reached
			// past the mapping methods would use.
		}
		return v
	}

	// Everything else is a scalar. The parser's String form is the source text
	// exactly as written, including quotes, so its length gives the range; the
	// token's value is the decoded text the compiler will see.
	raw := n.String()
	start := ix.offsetOfYAML(tok.Position.Line, tok.Position.Column)
	content := start
	inline := true
	if len(raw) >= 2 && (raw[0] == '"' || raw[0] == '\'') && raw[len(raw)-1] == raw[0] {
		content++
		// The quotes are allowed to be the difference between the source and the
		// decoded text, and nothing else is: an escape — `\n` in a double-quoted
		// scalar, `''` in a single-quoted one — rewrites the bytes, and after
		// that an offset into the decoded text is a different place from the same
		// offset into the source. Comparing the lengths asks exactly that, which
		// no pattern over the raw text would.
		inline = len(tok.Value) == len(raw)-2
	}
	v := &value{
		kind:       kindScalar,
		text:       tok.Value,
		textOffset: content,
		rng:        ix.rangeOfOffsets(start, start+len(raw)),
		inline:     inline,
	}

	// A value written across lines is not measurable by length, for the same
	// reason a block scalar is not, so it gets its first line and no inner
	// positions.
	//
	// It used to get no fence scan at all, on the stated ground that "the
	// compiler does not treat a multi-line scalar as an expression either". That
	// was not true and is easy to check: a plain scalar broken over two lines
	// decodes to one folded line, [flowfile.SplitFence] matches it, and the
	// engine evaluates it. So the scan runs here too, and only the arithmetic is
	// withheld.
	if strings.Contains(raw, "\n") {
		v.rng = restOfLine(ix, start)
		v.inline = false
		if source, ok := flowfile.SplitFence(v.text); ok && source != "" {
			v.fenced = true
			v.expr = source
			v.exprRange = v.rng
		}
		return v
	}

	// The fence rule is the compiler's, not a copy of it. Validity is deliberately
	// a separate question, decided by running the CEL parser, so that a fenced
	// scalar whose contents are broken still has a position to report against —
	// which is exactly the case the author most needs pointed at.
	if source, ok := flowfile.SplitFence(v.text); ok && source != "" {
		if inner := strings.Index(raw, exprOpen); inner >= 0 {
			v.fenced = true
			v.expr = source
			v.exprOffset = start + inner + len(exprOpen)
			v.exprRange = ix.rangeOfOffsets(start+inner, start+inner+len(exprOpen)+len(source)+len(exprClose))
		}
	}
	return v
}

// The fence delimiters, needed to locate the fence in the source text. The rule
// for what counts as fenced is [flowfile.SplitFence], not these.
const (
	exprOpen  = "${"
	exprClose = "}"
)

// literalIndicator opens a block scalar the decoder will not fold. The header
// may carry an indentation digit and a chomping sign after it — `|2-` is one
// header — so it is a prefix test and not an equality.
const literalIndicator = "|"

// A spanMapper turns a byte span of some source text into the document range
// that text occupies, and reports false where the source has no such place —
// which is the whole of what a caller needs to know about how the value was
// written.
type spanMapper func(start, end int) (lsp.Range, bool)

// exprMapper and textMapper hand out that view of a value's expression source
// and of its decoded text.
func (v *value) exprMapper(ix *lineIndex) spanMapper {
	return func(start, end int) (lsp.Range, bool) { return v.exprSpan(ix, start, end) }
}

func (v *value) textMapper(ix *lineIndex) spanMapper {
	return func(start, end int) (lsp.Range, bool) { return v.textSpan(ix, start, end) }
}

// textSpan returns the document range covering [start,end) of the value's
// decoded text, and reports false when the value has no position mapping to
// answer with.
//
// This and its two neighbours are the only readers of `inline` and `lineMap`,
// on purpose. Whether a value's positions come from one addition or one per
// line is a property of how the parser handed the text over, and a consumer
// asking about a cursor has no business knowing which — the last time three
// consumers each did this arithmetic themselves, each had to be taught
// separately that a block scalar cannot support it.
func (v *value) textSpan(ix *lineIndex, start, end int) (lsp.Range, bool) {
	from, ok := v.docOffsetOfText(start)
	if !ok {
		return lsp.Range{}, false
	}
	to, ok := v.docOffsetOfText(end)
	if !ok {
		return lsp.Range{}, false
	}
	return ix.rangeOfOffsets(from, to), true
}

// exprSpan returns the document range covering [start,end) of the value's
// expression source.
//
// The expression sits inside the fence, so its offsets are the text's shifted by
// the opening `${`. That shift is the whole of the "first line is different"
// problem: an expression's line 1 begins two bytes into the text's line 1 and
// every later line begins where the text's does. Adding it before the line
// lookup, rather than to a column afterwards, is what keeps that from becoming a
// per-line special case to get wrong.
func (v *value) exprSpan(ix *lineIndex, start, end int) (lsp.Range, bool) {
	if !v.fenced {
		return lsp.Range{}, false
	}
	return v.textSpan(ix, start+len(exprOpen), end+len(exprOpen))
}

// exprSpanOrWhole is [value.exprSpan] for a caller with nowhere to decline to:
// hover has already decided there is something to say, and a hover with no range
// underlines nothing at all. The whole value's range is the same coarse-and-true
// answer a diagnostic falls back to.
func (v *value) exprSpanOrWhole(ix *lineIndex, start, end int) lsp.Range {
	if rng, ok := v.exprSpan(ix, start, end); ok {
		return rng
	}
	return v.exprRange
}

// exprCursor returns the offset within the value's expression source that a
// document position names, and reports false when the position is not inside
// that source — the header line of a block scalar, the indentation stripped off
// a content line, or anywhere at all in a value with no mapping.
func (v *value) exprCursor(ix *lineIndex, pos lsp.Position) (int, bool) {
	if !v.fenced {
		return 0, false
	}
	off, ok := v.textOffsetOfDoc(ix.offsetOfPosition(pos))
	if !ok {
		return 0, false
	}
	cursor := off - len(exprOpen)
	if cursor < 0 || cursor > len(v.expr) {
		return 0, false
	}
	return cursor, true
}

// docOffsetOfText converts an offset within the decoded text to a document byte
// offset.
func (v *value) docOffsetOfText(off int) (int, bool) {
	if off < 0 || off > len(v.text) {
		return 0, false
	}
	if v.lineMap == nil {
		if !v.inline {
			return 0, false
		}
		return v.textOffset + off, true
	}
	for _, s := range v.lineMap {
		// The end of a line is included, so a range ending at the last byte of a
		// line resolves rather than falling through to the next one.
		if off >= s.textStart && off <= s.textStart+s.length {
			return s.docStart + (off - s.textStart), true
		}
	}
	return 0, false
}

// textOffsetOfDoc converts a document byte offset to an offset within the
// decoded text.
func (v *value) textOffsetOfDoc(off int) (int, bool) {
	if v.lineMap == nil {
		if !v.inline {
			return 0, false
		}
		rel := off - v.textOffset
		if rel < 0 || rel > len(v.text) {
			return 0, false
		}
		return rel, true
	}
	for _, s := range v.lineMap {
		if off >= s.docStart && off <= s.docStart+s.length {
			return s.textStart + (off - s.docStart), true
		}
	}
	return 0, false
}

// literalLineMap builds the mapping from a literal block scalar's decoded text
// back into the document, one content line at a time.
//
// origin is the content token's raw slice — the source the parser cut before
// decoding it — which begins at column zero of the line after the header and is
// byte-identical to the document from there. headerLine0 is the 0-based line the
// `|` was written on.
//
// The indentation each line lost is not recomputed from the header's indicator,
// because that is a second implementation of the decoder and would have to agree
// with it about every case the indicator decides: an explicit indentation digit,
// a line indented deeper than the block (whose extra spaces are *content* in a
// literal scalar and are kept), a blank line with no indentation to strip. It is
// measured instead — the decoded line is what is left of the source line, so the
// difference in their lengths is what was removed. That answer cannot disagree
// with the decoder, because it is read off the decoder's own output.
//
// It reports false rather than guessing whenever the two do not line up: a
// source line that is not the document's, a decoded line that is not a suffix of
// it, or anything but blanks in front of the content. A value with no map falls
// back to the coarse whole-value range, which is the same answer a folded scalar
// gets, and the same reason: a wrong position is worse than none.
func literalLineMap(ix *lineIndex, headerLine0 int, origin, text string) []lineSpan {
	if origin == "" || text == "" {
		return nil
	}
	src := strings.Split(origin, "\n")
	decoded := strings.Split(text, "\n")

	// Text ending in a newline splits with a trailing empty element that is the
	// position after the last line rather than a line of its own — `|` and `|+`
	// both produce one. Dropping exactly that element keeps the two lists in step
	// without swallowing a real blank line.
	if n := len(decoded); n > 1 && decoded[n-1] == "" {
		decoded = decoded[:n-1]
	}

	spans := make([]lineSpan, 0, len(decoded))
	textStart := 0
	for i, d := range decoded {
		line0 := headerLine0 + 1 + i
		if i >= len(src) || line0 >= ix.lineCount() {
			return nil
		}
		// The document's own line, not the origin's copy of it, decides — and
		// they must agree. That equality is what proves the origin still aligns
		// with the file, which everything below reads positions out of.
		if src[i] != ix.line(line0) {
			return nil
		}
		// A CRLF document keeps its carriage return in the source line and loses
		// it in the decoded one: it is part of the break, not of the content, so
		// it affects only where a line ends and never where one begins.
		content := strings.TrimSuffix(src[i], "\r")
		if !strings.HasSuffix(content, d) {
			return nil
		}
		indent := content[:len(content)-len(d)]
		if strings.TrimLeft(indent, " \t") != "" {
			return nil
		}
		spans = append(spans, lineSpan{
			textStart: textStart,
			docStart:  ix.lineStart(line0) + len(indent),
			length:    len(d),
		})
		textStart += len(d) + 1
	}
	return spans
}

// blockScalarRange returns the range a block scalar occupies: its header line
// through the last line holding content.
//
// The extent comes from the content token's Origin, which is the raw source the
// parser cut before folding it — the one thing about a block scalar that has not
// been rewritten. Only whole lines are taken from it. Columns inside a folded
// scalar correspond to nothing in the document, so a range claiming to know one
// would be the wrong-position class this file is careful about; whole lines are
// the finest honest grain.
//
// The header line alone is the fallback whenever the origin is missing or says
// something smaller, because a range that is too short still points at the value
// while one that is too long swallows the keys after it.
func blockScalarRange(ix *lineIndex, headerStart int, t *ast.LiteralNode) lsp.Range {
	header := restOfLine(ix, headerStart)
	if t.Value == nil {
		return header
	}
	origin := strings.TrimRight(strings.TrimLeft(t.Value.GetToken().Origin, "\r\n"), " \t\r\n")
	if origin == "" {
		return header
	}

	// The origin begins on the line after the header, so its last line is the
	// header's line plus one plus however many line breaks it holds.
	end := min(header.Start.Line+1+strings.Count(origin, "\n"), ix.lineCount()-1)
	if end <= header.End.Line {
		return header
	}
	return lsp.Range{
		Start: header.Start,
		End:   lsp.Position{Line: end, Character: utf16Len(ix.line(end))},
	}
}

// restOfLine returns the range from a byte offset to the end of its line.
func restOfLine(ix *lineIndex, start int) lsp.Range {
	from := ix.positionOfOffset(start)
	return lsp.Range{
		Start: from,
		End:   lsp.Position{Line: from.Line, Character: utf16Len(ix.line(from.Line))},
	}
}

// spanOf returns the range covering a collection's contents.
func spanOf(entries []*entry, items []*value, ix *lineIndex, tok *token.Token) lsp.Range {
	fallbackStart := ix.offsetOfYAML(tok.Position.Line, tok.Position.Column)
	span := ix.rangeOfOffsets(fallbackStart, fallbackStart)

	first := true
	extend := func(r lsp.Range) {
		if first {
			span = r
			first = false
			return
		}
		if r.Start.Line < span.Start.Line ||
			(r.Start.Line == span.Start.Line && r.Start.Character < span.Start.Character) {
			span.Start = r.Start
		}
		if r.End.Line > span.End.Line ||
			(r.End.Line == span.End.Line && r.End.Character > span.End.Character) {
			span.End = r.End
		}
	}
	for _, e := range entries {
		extend(e.keyRange)
		if e.value != nil {
			extend(e.value.rng)
		}
	}
	for _, i := range items {
		extend(i.rng)
	}
	return span
}

// mappingValues returns a mapping's entries, accepting both shapes the parser
// produces: a MappingNode for two or more keys, and a bare MappingValueNode for
// exactly one.
func mappingValues(n ast.Node) []*ast.MappingValueNode {
	switch t := n.(type) {
	case *ast.MappingNode:
		return t.Values
	case *ast.MappingValueNode:
		return []*ast.MappingValueNode{t}
	default:
		return nil
	}
}

// walkValues calls fn for v and every value nested inside it.
func walkValues(v *value, fn func(*value)) {
	if v == nil {
		return
	}
	fn(v)
	for _, item := range v.items {
		walkValues(item, fn)
	}
	for _, e := range v.entries {
		walkValues(e.value, fn)
	}
}
