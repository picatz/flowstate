package lsp

import (
	"fmt"
	"slices"
	"strings"

	v1 "github.com/picatz/flowstate/pkg/flowstate/v1"
	"github.com/sourcegraph/go-lsp"
	"google.golang.org/protobuf/reflect/protoreflect"
)

// Hover answers "what is this?" for the four things a Flowfile author points at:
// a task name, an input key, a reference to another step's output, and a CEL
// library. Every answer is read out of the schema or the registry at the moment
// it is asked, so hover cannot describe a task the engine would not run.

// hoverAt returns the documentation for whatever is at pos, or nil when there is
// nothing to say. Returning nil is important: a hover popup containing a guess is
// worse than no popup.
func hoverAt(doc *document, pos lsp.Position) *lsp.Hover {
	if doc.parsed == nil {
		return nil
	}

	step := doc.parsed.stepAt(pos)
	if step == nil {
		return nil
	}
	def, taskKnown := v1.LookupTask(step.taskName)

	// An expression reference is checked first: it is the innermost thing at a
	// position, nested inside an input's value or the step's condition.
	for _, in := range step.expressionEntries() {
		var found *lsp.Hover
		walkValues(in.value, func(v *value) {
			if found != nil || !v.fenced || !contains(v.exprRange, pos) {
				return
			}
			found = hoverReference(doc, step, v, pos)
		})
		if found != nil {
			return found
		}
	}

	// A CEL library name in a libs list.
	if in := step.input("libs"); in != nil && in.value != nil {
		for _, el := range append([]*value{in.value}, in.value.items...) {
			if el.kind != kindScalar || !contains(el.rng, pos) {
				continue
			}
			if lib, ok := lookupCELLibrary(el.text); ok {
				return markdownHover(lib.hover(), el.rng)
			}
		}
	}

	// An input key.
	for _, in := range step.inputs {
		if !contains(in.keyRange, pos) {
			continue
		}
		if !taskKnown {
			return nil
		}
		fd := findField(def.Inputs, in.key)
		if fd == nil {
			if acceptsAnyInput(def) {
				return markdownHover(fmt.Sprintf(
					"`%s` — a variable bound for the `%s` task's expression to reference.",
					in.key, def.Name), in.keyRange)
			}
			return nil
		}
		return markdownHover(inputDoc(def, in.key, fd), in.keyRange)
	}

	// The task name.
	if step.nameEntry != nil && step.nameEntry.value != nil && contains(step.nameEntry.value.rng, pos) {
		if !taskKnown {
			return nil
		}
		return markdownHover(taskDoc(def), step.nameEntry.value.rng)
	}

	// The step's own id: the least interesting of the four, but the author is
	// pointing at something, and naming the task it runs is a useful answer.
	if step.idEntry != nil && step.idEntry.value != nil && contains(step.idEntry.value.rng, pos) {
		return markdownHover(stepDoc(step, def, taskKnown), step.idEntry.value.rng)
	}

	// A key of the document's own shape: id, if, timeout, retry, and so on.
	if k, rng, ok := dslKeyAt(step, pos); ok {
		return markdownHover(fmt.Sprintf("**`%s`** · `%s`\n\n%s", k.name, k.detail, k.docs), rng)
	}
	return nil
}

// dslKeyAt returns the document-shape key whose name is at a position.
//
// Only keys this package documents produce hover. A key the DSL gains later shows
// nothing rather than something invented, which is the failure mode to prefer.
func dslKeyAt(step *parsedStep, pos lsp.Position) (dslKey, lsp.Range, bool) {
	type level struct {
		name    string
		entries []*entry
	}
	levels := []level{
		{"steps", step.entries},
		{"task", step.taskEntries},
	}
	// The keys of a nested block are documented at their own level.
	for _, block := range []struct {
		name  string
		entry *entry
	}{
		{"retry", step.retryEntry},
		{"for_each", step.forEachEntry},
	} {
		if block.entry != nil && block.entry.value != nil {
			levels = append(levels, level{block.name, block.entry.value.entries})
		}
	}

	for _, l := range levels {
		for _, e := range l.entries {
			if !contains(e.keyRange, pos) {
				continue
			}
			if k, ok := lookupDSLKey(l.name, e.key); ok {
				return k, e.keyRange, true
			}
		}
	}
	return dslKey{}, lsp.Range{}, false
}

// taskDoc renders a task's summary and its full typed signature.
func taskDoc(def v1.TaskDef) string {
	var b strings.Builder
	fmt.Fprintf(&b, "**task `%s`**", def.Name)
	if def.Summary != "" {
		fmt.Fprintf(&b, "\n\n%s", def.Summary)
	}
	fmt.Fprintf(&b, "\n\n%s", signature(def))

	if len(def.DeferredInputs) > 0 {
		fmt.Fprintf(&b, "\n\nThe task evaluates `%s` itself, so those inputs may reference values that exist only while it runs.",
			strings.Join(def.DeferredInputs, "`, `"))
	}
	if def.Outputs != nil && def.Outputs.Fields().Len() > 0 {
		names := fieldNames(def.Outputs)
		fmt.Fprintf(&b, "\n\nLater steps reference its outputs as `${%s}`.",
			strings.Join(prefixEach("step.", names), "}`, `${"))
	}
	return b.String()
}

// inputDoc renders one input's type, whether it is required, and the constraints
// the schema places on it.
func inputDoc(def v1.TaskDef, name string, fd protoreflect.FieldDescriptor) string {
	var b strings.Builder
	fmt.Fprintf(&b, "**`%s`** · `%s`", name, typeName(fd))
	if required(fd) {
		b.WriteString(" · required")
	} else {
		b.WriteString(" · optional")
	}
	fmt.Fprintf(&b, "\n\nInput of the `%s` task.", def.Name)
	if slices.Contains(def.DeferredInputs, name) {
		// Worth saying: an input the task evaluates itself has a different scope
		// from every other input, which is otherwise surprising.
		b.WriteString(" The task evaluates this input itself, so it may reference values that exist only while the step runs.")
	}
	if cs := constraints(fd); len(cs) > 0 {
		fmt.Fprintf(&b, "\n\nMust be: %s.", strings.Join(cs, "; "))
	}
	return b.String()
}

// stepDoc describes a step by the task it runs.
func stepDoc(step *parsedStep, def v1.TaskDef, taskKnown bool) string {
	var b strings.Builder
	fmt.Fprintf(&b, "**step `%s`** · step %d", step.id, step.index+1)
	if !taskKnown {
		if step.taskName == "" {
			b.WriteString("\n\nNo task named yet.")
		} else {
			fmt.Fprintf(&b, "\n\nRuns `%s`, which is not a registered task.", step.taskName)
		}
		return b.String()
	}
	fmt.Fprintf(&b, "\n\nRuns the `%s` task", def.Name)
	if def.Summary != "" {
		fmt.Fprintf(&b, ": %s", strings.ToLower(def.Summary[:1])+def.Summary[1:])
	} else {
		b.WriteString(".")
	}
	if names := fieldNames(def.Outputs); len(names) > 0 {
		fmt.Fprintf(&b, "\n\nProduces `${%s}`.", strings.Join(prefixEach(step.id+".", names), "}`, `${"))
	}
	return b.String()
}

// hoverReference describes a ${...} reference: which step produces the value, the
// task that produces it, and the output's declared type.
func hoverReference(doc *document, from *parsedStep, v *value, pos lsp.Position) *lsp.Hover {
	// The character offset of the cursor within the expression source.
	cursor := doc.index.offsetOfPosition(pos) - v.exprOffset
	ident, field, span := referenceAt(v.expr, cursor)
	if ident == "" {
		return nil
	}

	// A loop iterator is a name that resolves, but to an item rather than to a
	// step, so it is described before the step lookup that would not find it.
	for _, loop := range from.iteratorsInScope() {
		if loop.iteratorName() != ident {
			continue
		}
		rng := doc.index.rangeOfOffsets(v.exprOffset+span[0], v.exprOffset+span[1])
		return markdownHover(fmt.Sprintf(
			"**`%s`** — the current item of the `%s` loop.\n\n"+
				"Its type is whatever the loop's `items` expression yields an element of. "+
				"The loop reports every iteration through `${%s.results}`; body outputs do "+
				"not escape it.", ident, loop.id, loop.id), rng)
	}

	target := doc.parsed.step(ident)
	if !visibleFrom(target, from) {
		// Not a step reference, or one whose outputs this step cannot see. The
		// diagnostics say so; hover stays quiet rather than repeating it.
		return nil
	}

	rng := doc.index.rangeOfOffsets(v.exprOffset+span[0], v.exprOffset+span[1])
	def, known := v1.LookupTask(target.taskName)

	var b strings.Builder
	if field == "" {
		fmt.Fprintf(&b, "**`%s`** — step %d", target.id, target.index+1)
		if known {
			fmt.Fprintf(&b, ", running the `%s` task", def.Name)
			if names := fieldNames(def.Outputs); len(names) > 0 {
				fmt.Fprintf(&b, "\n\nOutputs: `%s`", strings.Join(names, "`, `"))
			}
		}
		return markdownHover(b.String(), rng)
	}

	fmt.Fprintf(&b, "**`%s.%s`**", target.id, field)
	if !known {
		fmt.Fprintf(&b, "\n\nOutput of step `%s`, whose task `%s` is not registered.", target.id, target.taskName)
		return markdownHover(b.String(), rng)
	}
	fd := findField(def.Outputs, field)
	if fd == nil {
		fmt.Fprintf(&b, "\n\nThe `%s` task does not declare an output named `%s`", def.Name, field)
		if names := fieldNames(def.Outputs); len(names) > 0 {
			fmt.Fprintf(&b, "; it produces `%s`", strings.Join(names, "`, `"))
		}
		b.WriteString(".")
		return markdownHover(b.String(), rng)
	}
	fmt.Fprintf(&b, " · `%s`\n\nOutput of step `%s` on line %d, produced by the `%s` task.",
		typeName(fd), target.id, target.rng.Start.Line+1, def.Name)
	if cs := constraints(fd); len(cs) > 0 {
		fmt.Fprintf(&b, "\n\nThe task guarantees: %s.", strings.Join(cs, "; "))
	}
	return markdownHover(b.String(), rng)
}

// referenceAt returns the identifier and optional field selected at a cursor
// offset within an expression, along with the byte span of the whole reference.
//
// It works on the expression text rather than a CEL syntax tree because an
// expression being edited frequently does not parse, and hover should still work
// while it does not.
func referenceAt(src string, cursor int) (ident, field string, span [2]int) {
	if cursor < 0 || cursor > len(src) {
		return "", "", span
	}
	isWord := func(c byte) bool {
		return c == '_' || c == '.' ||
			(c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z') || (c >= '0' && c <= '9')
	}

	start := min(cursor, len(src))
	for start > 0 && isWord(src[start-1]) {
		start--
	}
	end := min(cursor, len(src))
	for end < len(src) && isWord(src[end]) {
		end++
	}
	word := src[start:end]
	if word == "" {
		return "", "", span
	}

	// A reference is `ident` or `ident.field`; anything deeper is indexing into a
	// value whose shape the schema does not describe.
	parts := strings.SplitN(word, ".", 3)
	ident = parts[0]
	if len(parts) > 1 {
		field = parts[1]
		end = start + len(ident) + 1 + len(field)
	}
	return ident, field, [2]int{start, end}
}

// markdownHover wraps content as a hover response.
//
// The protocol's plain-string form of MarkedString is interpreted as Markdown by
// every client, and is the form that renders identically in editors that predate
// MarkupContent.
func markdownHover(content string, rng lsp.Range) *lsp.Hover {
	r := rng
	return &lsp.Hover{
		Contents: []lsp.MarkedString{lsp.RawMarkedString(content)},
		Range:    &r,
	}
}

// prefixEach returns names with a prefix applied to each, for rendering the
// ${step.output} forms a reader can copy.
func prefixEach(prefix string, names []string) []string {
	out := make([]string, 0, len(names))
	for _, n := range names {
		out = append(out, prefix+n)
	}
	return out
}
