package lsp

import (
	"errors"
	"fmt"
	"slices"
	"strings"

	v1 "github.com/picatz/flowstate/pkg/flowstate/v1"
	"github.com/picatz/flowstate/pkg/flowstate/v1/flowfile"
	"github.com/picatz/flowstate/pkg/flowstate/v1/secrets"
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
		// Outside every step — the keys above `steps:` in a conventionally ordered
		// file — the only thing at a position is one of the document's own keys.
		// They are described from the same table as a step's, because from an
		// author's position `edition:` and `timeout:` are the same kind of thing: a
		// word the grammar defines, whose meaning is not readable off the value
		// written beside it.
		if k, rng, ok := documentKeyAt(doc.parsed, pos); ok {
			return dslKeyHover(k, rng)
		}
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

	// The task name, which is the step key naming it.
	if step.taskEntry != nil && contains(step.taskEntry.keyRange, pos) {
		if !taskKnown {
			return nil
		}
		return markdownHover(taskDoc(def), step.taskEntry.keyRange)
	}

	// The step's own id: the least interesting of the four, but the author is
	// pointing at something, and naming the task it runs is a useful answer.
	if step.idEntry != nil && step.idEntry.value != nil && contains(step.idEntry.value.rng, pos) {
		return markdownHover(stepDoc(step, def, taskKnown), step.idEntry.value.rng)
	}

	// A key of the document's own shape: id, description, if, timeout, retry, and
	// so on.
	if k, rng, ok := dslKeyAt(step, pos); ok {
		return dslKeyHover(k, rng)
	}
	return nil
}

// dslKeyHover renders one document-shape key, so that a key documented at the
// document level and one documented inside a step read identically.
func dslKeyHover(k dslKey, rng lsp.Range) *lsp.Hover {
	return markdownHover(fmt.Sprintf("**`%s`** · `%s`\n\n%s", k.name, k.detail, k.docs), rng)
}

// documentKeyAt returns the top-level key whose name is at a position.
//
// The document's keys were unreachable until `edition:` arrived, and the gap was
// invisible while every one of them said something an author could read off the
// value beside it — `name: deploy` needs no popup. An edition does: the value is a
// date, and nothing in the file says what declaring it means or that leaving it
// out is normal.
func documentKeyAt(file *parsedFile, pos lsp.Position) (dslKey, lsp.Range, bool) {
	if file == nil {
		return dslKey{}, lsp.Range{}, false
	}
	for _, e := range file.entries {
		if !contains(e.keyRange, pos) {
			continue
		}
		if k, ok := lookupDSLKey("", e.key); ok {
			return k, e.keyRange, true
		}
	}
	return dslKey{}, lsp.Range{}, false
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
	// One level fewer than there used to be. A task's keys were documented under
	// a `task` level of their own; they are now the step's inputs, and an input is
	// documented from the task's schema rather than from this package's table.
	levels := []level{
		{"steps", step.entries},
	}
	// The keys of a nested block are documented at their own level.
	for _, block := range []struct {
		name  string
		entry *entry
	}{
		{"retry", step.retryEntry},
		{"for_each", step.forEachEntry},
		{"wait_for_signal", step.waitForSignalEntry},
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

	if n := len(def.DeferredInputs); n > 0 {
		// Agreeing in number matters here because the sentence has two referents —
		// the inputs and the task — and "it ... it" makes a reader work out which
		// is which. Naming the task in the second clause settles it either way.
		subject := "those inputs"
		if n == 1 {
			subject = "it"
		}
		fmt.Fprintf(&b, "\n\nThe task evaluates %s itself, so %s may reference values that exist only while `%s` runs.",
			joinNames(def.DeferredInputs), subject, def.Name)
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
//
// The author's own prose comes first when there is any. Everything else here is
// derived — the task, its summary, the outputs — and derived text can only say
// what a step *does*; a description is the one sentence in the file that says why
// it is there. It is also the only place the outline cannot show it, so hover on
// the id is where it earns its keep.
func stepDoc(step *parsedStep, def v1.TaskDef, taskKnown bool) string {
	var b strings.Builder
	fmt.Fprintf(&b, "**step `%s`** · step %d", step.id, step.index+1)
	if prose := step.descriptionEntry.valueText(); prose != "" {
		fmt.Fprintf(&b, "\n\n%s", prose)
	}
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

	// A secret reference resolves to neither a step nor an iterator, so it is
	// described before either lookup.
	if ref, span, err := secretRefAt(v.expr, cursor); err == nil {
		rng := doc.index.rangeOfOffsets(v.exprOffset+span[0], v.exprOffset+span[1])
		return markdownHover(secretDoc(ref), rng)
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

// secretRefAt returns the reference text of a secret marker containing the cursor,
// along with the byte span of the whole `secret('...')` call.
//
// The marker's name comes from flowfile rather than a literal here, so an editor
// cannot describe a spelling the compiler does not recognize. The argument must be
// a quoted literal — a reference is resolved at compile time, so there is nothing
// for a computed one to resolve against — which is what makes scanning for it
// reliable rather than a guess at CEL syntax.
func secretRefAt(src string, cursor int) (string, [2]int, error) {
	var span [2]int
	call := flowfile.SecretMarker + "("
	for at := 0; ; {
		i := strings.Index(src[at:], call)
		if i < 0 {
			return "", span, errNoSecretRef
		}
		start := at + i

		rest := src[start+len(call):]
		quote := strings.IndexAny(rest, `'"`)
		if quote < 0 {
			return "", span, errNoSecretRef
		}
		end := strings.IndexByte(rest[quote+1:], rest[quote])
		if end < 0 {
			return "", span, errNoSecretRef
		}

		ref := rest[quote+1 : quote+1+end]
		closing := strings.IndexByte(rest[quote+1+end:], ')')
		if closing < 0 {
			return "", span, errNoSecretRef
		}
		span = [2]int{start, start + len(call) + quote + 1 + end + closing + 1}

		if cursor >= span[0] && cursor <= span[1] {
			return ref, span, nil
		}
		at = span[1]
	}
}

// errNoSecretRef reports that a cursor is not inside a secret reference. It is a
// sentinel rather than a bool so the caller reads as a lookup rather than a test.
var errNoSecretRef = errors.New("no secret reference at this position")

// secretDoc renders what a secret reference names.
//
// The reference is parsed with the secrets package's own parser, so the editor
// cannot accept a form a worker would refuse. What it deliberately does not say is
// which backend a scheme resolves to: that is a deployment's choice, registered
// worker-side, and naming a concrete provider here would be a guess about someone
// else's configuration.
func secretDoc(ref string) string {
	var b strings.Builder
	parsed, err := secrets.ParseRef(ref)
	if err != nil {
		fmt.Fprintf(&b, "**secret reference** — not usable as written\n\n%s", err)
		return b.String()
	}

	fmt.Fprintf(&b, "**secret** · `%s`\n\n", secrets.RefString(parsed))
	fmt.Fprintf(&b, "Scheme `%s`, resolved by whichever provider this deployment registers for it. "+
		"The name `%s` is passed to that provider unchanged.\n\n",
		parsed.GetScheme(), parsed.GetName())
	b.WriteString("The value is resolved on the worker running the step, at the moment it runs. " +
		"It never enters workflow history, which is why a reference has to be the whole value of a " +
		"task input and cannot be combined with anything else.")
	return b.String()
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

// joinNames renders a list of names as prose: "a", "a and b", or "a, b, and c".
//
// Hover text is read, not parsed, and a comma-separated list of two reads as a
// mistake. This came up when a task gained a second input it evaluates itself.
func joinNames(names []string) string {
	quoted := make([]string, 0, len(names))
	for _, n := range names {
		quoted = append(quoted, "`"+n+"`")
	}
	switch len(quoted) {
	case 0:
		return ""
	case 1:
		return quoted[0]
	case 2:
		return quoted[0] + " and " + quoted[1]
	default:
		return strings.Join(quoted[:len(quoted)-1], ", ") + ", and " + quoted[len(quoted)-1]
	}
}
