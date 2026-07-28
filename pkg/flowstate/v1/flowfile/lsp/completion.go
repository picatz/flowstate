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
var dslKeys = map[string][]dslKey{
	"": {
		{name: "name", detail: "string", docs: "What this workflow is called."},
		{name: "description", detail: "string", docs: "Optional prose about the workflow."},
		{name: "steps", detail: "list", docs: "The steps to run, in order. Each step may reference the outputs of the steps before it."},
	},
	"steps": {
		{name: "id", detail: "string", docs: "How later steps reference this one, as `${id.output}`. Must be a valid CEL identifier and unique in the workflow."},
		{name: "task", detail: "map", docs: "Run a task. " + oneStepKind},
		{name: "for_each", detail: "map", docs: "Repeat a body of steps once per item of a list. " + oneStepKind},
		{name: "parallel", detail: "list", docs: "Run branches of steps concurrently. " + oneStepKind},
		{name: "sleep", detail: "duration", docs: "Wait for a duration on a durable timer, written as `30s`, `5m`, `1h`, or `7d`. " +
			"The run holds nothing while it waits, so a week is as cheap as a second. " + oneStepKind},
		{name: "wait_until", detail: "expression", docs: "Wait until a moment, written as `${...}` producing an RFC 3339 time. " +
			"Inside it, `now` is the moment the wait is evaluated and `seconds`/`minutes`/`hours`/`days`/`weeks` build durations, " +
			"so a deadline reads as `${now + days(3)}`. " + oneStepKind},
		{name: "wait_for_signal", detail: "string or map", docs: "Wait for a named signal, which is how a human approval reaches a workload. " +
			"Write `wait_for_signal: deploy-approved`, or a mapping with `name:` and `timeout:`. " +
			"What the sender sent becomes this step's outputs. " + oneStepKind},
		{name: "if", detail: "expression", docs: "A condition deciding whether the step runs, written as `${...}`. A step that is skipped produces no outputs."},
		{name: "timeout", detail: "duration", docs: "Bounds one attempt at the step, written as `30s`, `5m`, or `1h`."},
		{name: "retry", detail: "map", docs: "How a failed attempt is retried. Omit it to use the engine's defaults."},
		{name: "continue_on_error", detail: "bool", docs: "Let the run proceed when this step fails. A cancellation is not a failure, so this does not tolerate one."},
	},
	"wait_for_signal": {
		{name: "name", detail: "string", docs: "The signal this step waits for, and what a sender addresses with `flow signal <workflow-id> <name>`."},
		{name: "timeout", detail: "duration", docs: "Bounds the wait. A gate that lapses is not a failure: the step produces `timed_out: true` and the run carries on, " +
			"so an author branches on it with `if: ${!approval.timed_out}`. Omit it to wait indefinitely."},
	},
	"for_each": {
		{name: "items", detail: "expression", docs: "An expression producing the list to iterate, written as `${...}`."},
		{name: "iterator", detail: "string", docs: "Names the variable bound to the current item. Defaults to `item`."},
		{name: "max_parallel", detail: "int", docs: "How many iterations may run at once. Omitted or `1` runs them one at a time."},
		{name: "steps", detail: "list", docs: "The body run once per item."},
	},
	"parallel": {
		{name: "steps", detail: "list", docs: "One branch's steps. Each `- steps:` entry is a branch that runs concurrently with the others."},
	},
	"task": {
		{name: "name", detail: "string", docs: "The registered task to run."},
		{name: "description", detail: "string", docs: "Optional prose about this task."},
		{name: "inputs", detail: "map", docs: "The task's inputs. Which ones are accepted comes from the task's schema."},
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

	steps := scanOutline(doc.index)
	current, earlier := stepScope(steps, pos.Line)

	// Inside ${...} nothing else applies: the cursor is in an expression, not in
	// YAML structure.
	if inner, ok := openExpression(before); ok {
		return completeInExpression(pos, inner, referenceScope(doc, pos, current, earlier))
	}

	path := keyPath(doc.index, pos.Line)
	key, valuePos := keyAndPosition(line, col)
	word, replace := wordBefore(pos, before)

	if valuePos {
		switch {
		case key == "name" && endsWith(path, "task"):
			return list(taskCandidates(word, replace))
		case key == "libs" || endsWith(path, "libs"):
			return list(libraryCandidates(word, replace, current))
		}
		return empty
	}

	// The cursor is where a key goes.
	switch {
	case endsWith(path, "libs"):
		return list(libraryCandidates(word, replace, current))
	case endsWith(path, "task", "inputs"):
		return list(inputCandidates(word, replace, current))
	case endsWith(path, "task"):
		return list(dslCandidates("task", word, replace))
	case endsWith(path, "retry"):
		return list(dslCandidates("retry", word, replace))
	case endsWith(path, "for_each"):
		return list(dslCandidates("for_each", word, replace))
	case endsWith(path, "parallel"):
		return list(dslCandidates("parallel", word, replace))
	case endsWith(path, "wait_for_signal"):
		return list(dslCandidates("wait_for_signal", word, replace))
	case endsWith(path, "steps"):
		return list(dslCandidates("steps", word, replace))
	case len(path) == 0:
		return list(dslCandidates("", word, replace))
	}
	return empty
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

	// kind distinguishes a step from a bound variable, for the popup's icon.
	kind lsp.CompletionItemKind
}

// A refOutput is one name reachable after a dot.
type refOutput struct {
	name   string
	detail string
	docs   string
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
func referenceScope(doc *document, pos lsp.Position, current *outlineStep, earlier []*outlineStep) []refCandidate {
	currentIndent := 0
	if current != nil {
		currentIndent = current.indent
	}
	if doc.parsed != nil {
		if from := doc.parsed.stepAt(pos); from != nil {
			return scopeFromModel(doc, from)
		}
	}
	return scopeFromOutline(earlier, currentIndent)
}

// scopeFromModel builds the candidate list using the engine's scoping rules.
func scopeFromModel(doc *document, from *parsedStep) []refCandidate {
	var out []refCandidate

	// Iterators first: inside a loop body the current item is the name most
	// likely to be wanted, and the innermost loop's is the nearest.
	for _, loop := range from.iteratorsInScope() {
		name := loop.iteratorName()
		if name == "" {
			continue
		}
		out = append(out, refCandidate{
			name:   name,
			kind:   lsp.CIKVariable,
			detail: "loop item",
			docs: fmt.Sprintf(
				"The current item of the %s loop. Its type is whatever the loop's items expression yields an element of.",
				loop.id),
		})
	}

	// Steps in reverse document order, so the whole list reads nearest-first: the
	// iterator of the loop you are standing in, then the step just above you.
	for i := len(doc.parsed.steps) - 1; i >= 0; i-- {
		s := doc.parsed.steps[i]
		if s.id == "" || !visibleFrom(s, from) {
			continue
		}
		out = append(out, stepCandidate(s))
	}
	return out
}

// scopeFromOutline builds the candidate list from the line scan, for a document
// that does not parse.
//
// The scan cannot see the scoping rules, so it approximates them with indentation
// and errs towards offering too little: a step nested deeper than the cursor's is
// inside a block the cursor is not in, and one of the cursor's own enclosing blocks
// has not finished. Both are excluded. Omitting a name that would have worked is a
// small cost; offering one that cannot resolve is the thing to avoid.
func scopeFromOutline(earlier []*outlineStep, currentIndent int) []refCandidate {
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

	out := make([]refCandidate, 0, len(earlier))
	for i := len(earlier) - 1; i >= 0; i-- {
		s := earlier[i]
		if s.id == "" || s.indent > currentIndent || ancestors[s] {
			continue
		}
		c := refCandidate{name: s.id, kind: lsp.CIKVariable, detail: "step"}
		if def, ok := v1.LookupTask(s.taskName); ok {
			c.detail = def.Name
			c.docs = fmt.Sprintf("Runs the %s task.", def.Name)
			c.outputs = taskOutputs(def)
		}
		out = append(out, c)
	}
	return out
}

// stepCandidate describes one step as a reference candidate.
func stepCandidate(s *parsedStep) refCandidate {
	c := refCandidate{name: s.id, kind: lsp.CIKVariable, detail: s.kind()}

	switch {
	case s.forEachEntry != nil:
		// A loop reports every iteration through one output; its body's outputs
		// are not reachable from outside it.
		c.detail = "for_each"
		c.docs = fmt.Sprintf(
			"A loop. Reports one entry per iteration in %s, each a map of body step id to that step's outputs. Body outputs do not escape the loop.",
			loopResultsOutput)
		c.outputs = []refOutput{{
			name:   loopResultsOutput,
			detail: "list",
			docs:   "One entry per iteration, each a map of body step id to that step's named outputs.",
		}}

	case s.parallelEntry != nil:
		c.detail = "parallel"
		c.docs = "A parallel block. Its branches' step outputs merge into this scope once it joins, so reference those step ids directly."

	default:
		if def, ok := v1.LookupTask(s.taskName); ok {
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

// completeInExpression offers references in scope, and after a dot, what the named
// one exposes.
func completeInExpression(pos lsp.Position, inner string, scope []refCandidate) *lsp.CompletionList {
	word := trailingWord(inner, func(c byte) bool {
		return c == '_' || c == '.' ||
			(c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z') || (c >= '0' && c <= '9')
	})

	if dot := strings.LastIndex(word, "."); dot >= 0 {
		qualifier, member := word[:dot], word[dot+1:]
		return list(outputCandidates(qualifier, member, rangeBack(pos, member), scope))
	}
	return list(stepCandidates(word, rangeBack(pos, word), scope))
}

// stepCandidates offers the names in scope, nearest first.
func stepCandidates(prefix string, replace lsp.Range, scope []refCandidate) []lsp.CompletionItem {
	var items []lsp.CompletionItem
	for i, c := range scope {
		if !strings.HasPrefix(c.name, prefix) {
			continue
		}
		items = append(items, lsp.CompletionItem{
			Label:         c.name,
			Kind:          c.kind,
			Detail:        c.detail,
			Documentation: plainText(c.docs),
			// The scope is already nearest-first, and the nearest name is usually
			// the one being referenced.
			SortText: fmt.Sprintf("%04d", i),
			TextEdit: &lsp.TextEdit{Range: replace, NewText: c.name},
		})
	}
	return items
}

// outputCandidates offers what a named reference exposes after a dot.
func outputCandidates(qualifier, prefix string, replace lsp.Range, scope []refCandidate) []lsp.CompletionItem {
	var target *refCandidate
	for i := range scope {
		if scope[i].name == qualifier {
			target = &scope[i]
			break
		}
	}
	if target == nil {
		// Not in scope. Offering its outputs would suggest a reference the engine
		// rejects.
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

// taskCandidates offers every registered task, with its summary as the detail.
func taskCandidates(prefix string, replace lsp.Range) []lsp.CompletionItem {
	var items []lsp.CompletionItem
	for _, def := range v1.DefaultRegistry().All() {
		if !strings.HasPrefix(def.Name, prefix) {
			continue
		}
		items = append(items, lsp.CompletionItem{
			Label:         def.Name,
			Kind:          lsp.CIKFunction,
			Detail:        def.Summary,
			Documentation: plainText(taskDoc(def)),
			TextEdit:      &lsp.TextEdit{Range: replace, NewText: def.Name},
		})
	}
	return items
}

// inputCandidates offers the inputs the enclosing step's task declares, required
// ones first, omitting those already written.
func inputCandidates(prefix string, replace lsp.Range, step *outlineStep) []lsp.CompletionItem {
	if step == nil {
		return nil
	}
	def, ok := v1.LookupTask(step.taskName)
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

// libraryCandidates offers the CEL extension libraries a step may enable.
func libraryCandidates(prefix string, replace lsp.Range, step *outlineStep) []lsp.CompletionItem {
	enabled := map[string]bool{}
	if step != nil {
		for _, l := range step.libs {
			enabled[l] = true
		}
	}

	var items []lsp.CompletionItem
	for _, name := range v1.ExtensionLibraries() {
		if !strings.HasPrefix(name, prefix) || (enabled[name] && name != prefix) {
			continue
		}
		lib, _ := lookupCELLibrary(name)
		items = append(items, lsp.CompletionItem{
			Label:         name,
			Kind:          lsp.CIKModule,
			Detail:        lib.Summary,
			Documentation: plainText(lib.hover()),
			TextEdit:      &lsp.TextEdit{Range: replace, NewText: name},
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
			SortText:      fmt.Sprintf("%04d%s", i, k.name),
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
