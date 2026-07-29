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
	// expression source — a cel step's expr — is reported against this, so the
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
	// step; the three wait kinds carry no nested expressions of their own beyond
	// wait_until's, so they need no entry of their own here.
	forEachEntry  *entry
	parallelEntry *entry

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
	entries := make([]*entry, 0, len(s.inputs)+2)
	if s.conditionEntry != nil {
		entries = append(entries, s.conditionEntry)
	}
	if s.itemsEntry != nil {
		entries = append(entries, s.itemsEntry)
	}
	return append(entries, s.inputs...)
}

// kind names the kind of work a step does, for the outline and for diagnostics
// that need to say what a step is.
func (s *parsedStep) kind() string {
	switch {
	case s.taskEntry != nil:
		return "task"
	case s.forEachEntry != nil:
		return "for_each"
	case s.parallelEntry != nil:
		return "parallel"
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
	for _, group := range [][]*entry{s.entries, nestedEntries(s.forEachEntry), nestedEntries(s.retryEntry)} {
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

	// entries are the document's own keys, in source order, including any this
	// package does not interpret — the same reason parsedStep keeps its own: a key
	// the DSL gains tomorrow is then undocumented rather than unreachable. It is
	// what hover resolves above `steps:`, where there is no step to ask.
	entries []*entry
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
	assignStepRanges(ix, out)
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
		if s.forEachEntry != nil && s.forEachEntry.value != nil {
			for _, fe := range s.forEachEntry.value.entries {
				if fe.key == "steps" {
					collectSteps(fe, s, append(scope, scopeFrame{block: s, branch: -1}), ix, out)
				}
			}
		}
		if s.parallelEntry != nil && s.parallelEntry.value != nil {
			for i, branch := range s.parallelEntry.value.items {
				for _, be := range branch.entries {
					if be.key == "steps" {
						collectSteps(be, s, append(scope, scopeFrame{block: s, branch: i}), ix, out)
					}
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

// iteratorName returns the variable a for_each step binds each item to, falling
// back to the engine's own default rather than a copy of it.
func (s *parsedStep) iteratorName() string {
	if s.forEachEntry == nil || s.forEachEntry.value == nil {
		return ""
	}
	for _, e := range s.forEachEntry.value.entries {
		if e.key == "iterator" {
			if name := e.valueText(); name != "" {
				return name
			}
		}
	}
	return v1.DefaultIterator
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
func assignStepRanges(ix *lineIndex, steps []*parsedStep) {
	for i, s := range steps {
		endLine := ix.lineCount() - 1
		if i+1 < len(steps) {
			endLine = steps[i+1].rng.Start.Line - 1
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
		// of it is folded text rather than the exact source slice, so its length
		// cannot be used to measure a range. The range is the header line — enough
		// to point at, and never pointing at the wrong thing.
		v := &value{kind: kindScalar}
		if t.Value != nil {
			v.text = t.Value.Value
		}
		start := ix.offsetOfYAML(tok.Position.Line, tok.Position.Column)
		v.textOffset = start
		v.rng = restOfLine(ix, start)
		return v
	}

	// Everything else is a scalar. The parser's String form is the source text
	// exactly as written, including quotes, so its length gives the range; the
	// token's value is the decoded text the compiler will see.
	raw := n.String()
	start := ix.offsetOfYAML(tok.Position.Line, tok.Position.Column)
	content := start
	if len(raw) >= 2 && (raw[0] == '"' || raw[0] == '\'') && raw[len(raw)-1] == raw[0] {
		content++
	}
	v := &value{
		kind:       kindScalar,
		text:       tok.Value,
		textOffset: content,
		rng:        ix.rangeOfOffsets(start, start+len(raw)),
	}

	// A value written across lines is not measurable by length, for the same
	// reason a block scalar is not, so it gets its header line and no expression
	// scan. The compiler does not treat a multi-line scalar as an expression
	// either, so nothing is lost.
	if strings.Contains(raw, "\n") {
		v.rng = restOfLine(ix, start)
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
