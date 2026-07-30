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
		// An expression written above `steps:` — a workflow `vars:` value — before
		// the keys, because it is the innermost thing at a position for the same
		// reason a step's expressions are checked before the step's keys.
		if h := hoverDocumentExpression(doc, pos); h != nil {
			return h
		}

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
		clock := step.bindsNow(in)
		walkValues(in.value, func(v *value) {
			if found != nil || !v.fenced || !contains(v.exprRange, pos) {
				return
			}
			found = hoverReference(doc, step, v, clock, pos)
		})
		if found != nil {
			return found
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
			// An input the task does not declare. There is nothing true to say about
			// it, and the validator is already reporting it as unknown.
			//
			// One task used to take names beyond its schema — the compiler emptied its
			// `vars:` mapping into the input map, so every name under it was a legal
			// input — and this described one. That task retired at edition v2026.2 and
			// the hoist went with it, so an undeclared input is a mistake again; a
			// hover explaining it as a binding would contradict the diagnostic sitting
			// on the same key.
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

	// Said separately from the deferred sentence, because the two are separate
	// facts and the earlier version of this change proved they do not travel
	// together: `outputs` is evaluated by the task *and* takes a literal, while
	// `expect` is evaluated by the task and does not. Folding this into the
	// sentence above would restate that wrong symmetry in the editor, where an
	// author would meet it before the validator got a chance to correct them.
	if n := len(def.ExpressionInputs); n > 0 {
		verb := "have"
		if n == 1 {
			verb = "has"
		}
		fmt.Fprintf(&b, "\n\n%s %s to be written as an expression: `${...}` around the whole value.",
			joinNames(def.ExpressionInputs), verb)
	}
	if def.Outputs != nil && def.Outputs.Fields().Len() > 0 {
		names := fieldNames(def.Outputs)
		// A task is described without a step to hang the outputs off, so the id is
		// a placeholder — but the root is not. Writing the bare form here would
		// hand the reader something `flow validate` refuses, which is the one thing
		// documentation generated from the schema must never do.
		fmt.Fprintf(&b, "\n\nLater steps reference its outputs as `${%s}`.",
			strings.Join(prefixEach(v1.StepsRoot+".<id>.", names), "}`, `${"))
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
		fmt.Fprintf(&b, "\n\nProduces `${%s}`.",
			strings.Join(prefixEach(v1.StepsRoot+"."+step.id+".", names), "}`, `${"))
	}
	return b.String()
}

// hoverReference describes a ${...} reference: which step produces the value, the
// task that produces it, and the output's declared type.
func hoverReference(doc *document, from *parsedStep, v *value, clock bool, pos lsp.Position) *lsp.Hover {
	// The character offset of the cursor within the expression source.
	cursor := doc.index.offsetOfPosition(pos) - v.exprOffset
	ref := referenceAt(v.expr, cursor)
	if ref.empty() {
		// No reference here, which does not mean nothing is here. `referenceAt`
		// treats a dot as part of a word, so the name in `[3,1,2].sortBy(v, v)`
		// comes back as `.sortBy` — a first segment that is empty, and no
		// reference. A function is still the thing under the cursor.
		return hoverFunction(doc, v, cursor)
	}

	// A secret reference resolves to neither a step nor a binding, so it is
	// described before either lookup.
	if name, span, err := secretRefAt(v.expr, cursor); err == nil {
		rng := doc.index.rangeOfOffsets(v.exprOffset+span[0], v.exprOffset+span[1])
		return markdownHover(secretDoc(name), rng)
	}

	rng := doc.index.rangeOfOffsets(v.exprOffset+ref.span[0], v.exprOffset+ref.span[1])
	if ref.step != "" {
		// A rooted reference. Nothing inside one is a call, so a function is not a
		// reading to fall back on here — `${steps.web.value}` names an output, and
		// `value` is also a function in the optional library.
		return hoverStepOutput(doc, from, ref, rng)
	}

	if h := hoverBareName(from, ref.local, clock, rng); h != nil {
		return h
	}

	// Nothing bound that name, so the cursor may be on a function. Second,
	// deliberately: a binding is what the author wrote and a function of the same
	// spelling is a coincidence.
	return hoverFunction(doc, v, cursor)
}

// hoverDocumentExpression describes a reference inside one of the document's own
// expressions, which today means a workflow `vars:` value.
//
// The answers here are refusals, and that is the useful part. This block is
// evaluated before the first step runs and it is a mapping with no order, so a
// value in it can reference neither a step nor another var — the two things an
// author most naturally reaches for. Saying which, at the moment they point at it,
// is worth more than the silence hover gives a name it cannot describe: the
// validator's diagnostic says the same thing, and an author who has not run it yet
// has no other way to find out.
//
// It cannot go through [hoverReference]: that describes a reference *from a step*,
// and there is no step here — `visibleFrom` would answer no and say nothing, which
// reads as "this is fine" rather than "this cannot be written here".
func hoverDocumentExpression(doc *document, pos lsp.Position) *lsp.Hover {
	for _, in := range doc.parsed.expressionEntries() {
		var found *lsp.Hover
		walkValues(in.value, func(v *value) {
			if found != nil || !v.fenced || !contains(v.exprRange, pos) {
				return
			}

			cursor := doc.index.offsetOfPosition(pos) - v.exprOffset

			// A secret is the one reference that resolves here, because it is
			// resolved by the worker rather than out of the run's state.
			if name, span, err := secretRefAt(v.expr, cursor); err == nil {
				rng := doc.index.rangeOfOffsets(v.exprOffset+span[0], v.exprOffset+span[1])
				found = markdownHover(secretDoc(name), rng)

				return
			}

			ref := referenceAt(v.expr, cursor)
			if ref.empty() {
				return
			}
			rng := doc.index.rangeOfOffsets(v.exprOffset+ref.span[0], v.exprOffset+ref.span[1])

			switch {
			case ref.step != "" || ref.local == v1.StepsRoot:
				found = markdownHover(fmt.Sprintf(
					"**`%s`** — not readable here.\n\n"+
						"A `%s:` block at the top of a file is evaluated *before the first step "+
						"runs*, so no step has produced anything for it to read. Write the "+
						"expression where the value is wanted, or under `%s:` on the step that "+
						"uses it — a step's own block is evaluated just before that step, and "+
						"can read whatever has happened by then.",
					v1.StepsRoot, varsKeyword, varsKeyword), rng)

			case ref.local == v1.VarsRoot:
				found = markdownHover(fmt.Sprintf(
					"**`%s`** — not readable here.\n\n"+
						"One var cannot read another. `%s:` is a mapping, and a mapping has no "+
						"order, so \"the one above\" is not something this file can mean. Write "+
						"the shared part out in both, or move the value to the step that needs "+
						"the combination.",
					v1.VarsRoot, varsKeyword), rng)
			}
		})
		if found != nil {
			return found
		}
	}

	return nil
}

// varsKeyword is the `vars:` key as an author writes it, for prose that names it.
const varsKeyword = "vars"

// hoverBareName describes a name written without the root.
//
// The two namespaces are what decides the answer rather than a lookup order. A
// bare name is a *binding*, so the only things that can be said about one are what
// bound it; a step is not a candidate reading at all, which is why a bare
// `${web.body}` in an unmigrated file gets silence here. The diagnostic on it says
// what to run, and hover repeating that would be a second voice saying the same
// thing in a smaller box.
//
// clock reports that the expression is a wait's, which is the only thing that puts
// `now` in scope. It is passed in rather than derived from the name, because the
// answer to `${now}` written in a task input is not this documentation — it is the
// validator's diagnostic saying the name is not bound there, and describing it as
// though it were would contradict a squiggle the author is looking at.
func hoverBareName(from *parsedStep, name string, clock bool, rng lsp.Range) *lsp.Hover {
	if clock && name == v1.NowIdentifier {
		return markdownHover(nowDoc(), rng)
	}
	for _, loop := range from.iteratorsInScope() {
		if loop.iteratorName() != name {
			continue
		}
		return markdownHover(fmt.Sprintf(
			"**`%s`** — the current item of the `%s` loop.\n\n"+
				"Its type is whatever the loop's `items` expression yields an element of. "+
				"The loop reports every iteration through `${%s.%s.%s}`; body outputs do "+
				"not escape it.", name, loop.id, v1.StepsRoot, loop.id, loopResultsOutput), rng)
	}

	if name == v1.StepsRoot {
		// The root itself, which is a value an expression may legitimately name:
		// `size(steps)` counts what has run. Saying so is also the answer to the
		// question an author asks by hovering the word — what this thing in front
		// of every reference is.
		return markdownHover(fmt.Sprintf(
			"**`%s`** — every step's outputs, keyed by step id.\n\n"+
				"One step's output is `${%s.<id>.<output>}`. The root is what keeps a step id "+
				"and a name bound here — a loop's item, `%s` inside a `wait_until:` — in "+
				"separate namespaces, so neither can hide the other.",
			v1.StepsRoot, v1.StepsRoot, v1.NowIdentifier), rng)
	}
	return nil
}

// hoverStepOutput describes a reference rooted under `steps.`.
func hoverStepOutput(doc *document, from *parsedStep, ref reference, rng lsp.Range) *lsp.Hover {
	target := doc.parsed.step(ref.step)
	if !visibleFrom(target, from) {
		// Not a step, or one whose outputs this step cannot see. The diagnostics
		// say so; hover stays quiet rather than repeating it.
		return nil
	}
	def, known := v1.LookupTask(target.taskName)

	var b strings.Builder
	if ref.output == "" {
		fmt.Fprintf(&b, "**`%s`** — step %d", rootedRef(target.id, ""), target.index+1)
		if known {
			fmt.Fprintf(&b, ", running the `%s` task", def.Name)
			if names := fieldNames(def.Outputs); len(names) > 0 {
				fmt.Fprintf(&b, "\n\nOutputs: `%s`", strings.Join(names, "`, `"))
			}
		}
		return markdownHover(b.String(), rng)
	}

	fmt.Fprintf(&b, "**`%s`**", rootedRef(target.id, ref.output))
	if !known {
		fmt.Fprintf(&b, "\n\nOutput of step `%s`, whose task `%s` is not registered.", target.id, target.taskName)
		return markdownHover(b.String(), rng)
	}
	fd := findField(def.Outputs, ref.output)
	if fd == nil {
		fmt.Fprintf(&b, "\n\nThe `%s` task does not declare an output named `%s`", def.Name, ref.output)
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

// nowDoc describes the one identifier whose availability depends on where it is
// written.
//
// Written once and shown by both hover and completion, the way a CEL library's
// documentation is: an author who accepts the candidate and then hovers what they
// accepted should not be told two different things.
//
// What it says is the validator's reasoning, not a second account of it. `flowfile`
// refuses `now` in a task input with the same three claims — the moment the wait is
// evaluated, resolved inside an activity, no clock that survives a retry — and
// TestNowIsExplainedTheSameWayTheValidatorRefusesIt asserts both texts still make
// them, because the string itself is unexported and cannot be shared. That is the
// weaker guarantee of the two, and it is why the phrasing here follows the
// diagnostic rather than improving on it.
func nowDoc() string {
	return fmt.Sprintf(
		"**`%s`** · `timestamp` — the moment the wait is evaluated.\n\n"+
			"Bound inside `%s:` and nowhere else, from the clock the driver controls, so a "+
			"deadline computed from it survives replay and a worker restart. A task input is "+
			"resolved inside an activity, which has no clock that survives a retry: a `%s` there "+
			"would read differently on every attempt, so the name is not bound in one. Compute "+
			"the moment here, or pass a time in as an input.\n\n"+
			"Durations build from `%s`, so a deadline reads as `${%s + days(3)}`.",
		v1.NowIdentifier, waitUntilKey, v1.NowIdentifier,
		strings.Join(v1.DurationUnits(), "`, `"), v1.NowIdentifier)
}

// rootedRef spells a reference the way an author writes it, from the root the
// grammar defines rather than from a literal here.
func rootedRef(step, output string) string {
	ref := v1.StepsRoot + "." + step
	if output != "" {
		ref += "." + output
	}
	return ref
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

// A reference is what an expression names at one position.
//
// Two namespaces, held in two fields rather than in one name plus a flag. A step's
// outputs hang off the root — `steps.web.body` — and everything bound where the
// cursor stands is written bare: a loop's iterator, `now` inside a `wait_until:`,
// the response variables an http task resolves against its own scope. Before
// rooting the two were the same shape, `a.b`, and only a lookup could say which
// was meant; keeping them apart here is what stops a caller having to remember
// which reading applies.
type reference struct {
	// step is the id a rooted reference names, and output the step's output when
	// one is written. Both are empty for a bare name.
	step   string
	output string

	// local is the bare name, empty for a rooted reference.
	local string

	// span is the byte span of the reference within the expression source.
	span [2]int
}

// empty reports that nothing at the cursor is a reference.
func (r reference) empty() bool { return r.step == "" && r.local == "" }

// referenceAt returns the reference selected at a cursor offset within an
// expression.
//
// It works on the expression text rather than a CEL syntax tree because an
// expression being edited frequently does not parse, and hover should still work
// while it does not.
//
// The whole word is read and then split, rather than being split at a fixed
// depth: a rooted reference is three segments where the retired spelling was two,
// and `steps.a.result.code` is four. What the extra segments mean is the same
// either way — indexing into a value whose shape the schema does not describe —
// so the reference ends at the output and the rest is left alone.
func referenceAt(src string, cursor int) reference {
	var ref reference
	if cursor < 0 || cursor > len(src) {
		return ref
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
		return ref
	}

	segments := strings.Split(word, ".")
	if segments[0] != v1.StepsRoot || len(segments) == 1 {
		// Bare. Only the first segment is named: what follows a bare name selects
		// into a value whose type is not statically known — a loop item's fields,
		// a signal's payload — so the reference is the name and nothing after it.
		if segments[0] == "" {
			return reference{}
		}
		return reference{local: segments[0], span: [2]int{start, start + len(segments[0])}}
	}
	if segments[1] == "" {
		// `steps.` with nothing after it yet, which is what an author has typed the
		// instant completion is asked for. It names no step.
		return reference{}
	}

	ref.step = segments[1]
	ref.span = [2]int{start, start + len(v1.StepsRoot) + 1 + len(ref.step)}
	if len(segments) > 2 && segments[2] != "" {
		ref.output = segments[2]
		ref.span[1] += 1 + len(ref.output)
	}
	return ref
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
// ${steps.<id>.<output>} forms a reader can copy.
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
