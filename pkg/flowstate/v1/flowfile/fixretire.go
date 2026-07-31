package flowfile

import (
	"fmt"
	"regexp"
	"strconv"
	"strings"

	"github.com/goccy/go-yaml/ast"
	v1 "github.com/picatz/flowstate/pkg/flowstate/v1"
)

// Retiring a task is the migration this rewriter exists for.
//
// `cel:`, `echo:` and `printf:` are gone, and the no-deprecation-window decision is
// only affordable because the migration is a program someone runs in a second. A
// retirement without a rewrite is a breaking change with a note attached.
//
// # There are exactly two cases, and only one is mechanical
//
// A retired step whose result *is read* by something later was a way of naming a value.
// That is `vars:` now, and the rewrite is total: the value moves to the top of the file
// under the step's own id, the step goes away, and every `${steps.<id>.result}` becomes
// `${vars.<id>}`. Nothing is guessed — the name is the id the author already chose, and
// every reference already spells it.
//
// A retired step whose result *nothing reads* is intent this cannot see. It might have
// meant "show a human this line", which is `log:`; it might have meant nothing at all
// and want deleting. Choosing would be the rewriter inventing a step the author did not
// write, so it refuses and says which two things it cannot tell apart. That is already
// its contract for flow style and for a task behind an alias.

// retiredTasks are the task keys this rewriter migrates, and how to read the value each
// one produced.
//
// Keyed by the step key an author wrote. The function returns the CEL source of the
// value the step's `result` used to hold, or a refusal explaining what stopped it.
var retiredTasks = map[string]func(*fixer, *ast.MappingValueNode) (string, bool){
	"echo":   (*fixer).retiredEcho,
	"printf": (*fixer).retiredPrintf,
	"cel":    (*fixer).retiredCEL,
}

// stepReference matches a rooted reference to one step's `result`.
//
// Textual rather than parsed, and that is a considered trade. Both ways of being wrong
// are safe here: reading a reference that is not one produces an unused `vars:` entry,
// and missing one produces a refusal. Neither writes something that means something
// else, which is the only outcome a rewriter may not have.
var stepReference = regexp.MustCompile(`\bsteps\.([A-Za-z_][A-Za-z0-9_]*)\b`)

// varReference matches a rooted reference to a declared var, for the same reason and
// with the same tolerances.
var varReference = regexp.MustCompile(`\bvars\.([A-Za-z_][A-Za-z0-9_]*)\b`)

// expressionSpans returns the byte range of the source inside each `${...}` on a
// line.
//
// A reference is a reference only inside a fence. Outside one, `steps.greet.result`
// is text — a sentence in a `message:`, a URL path, a description — and reading it
// as a reference is not the harmless over-read [stepReference]'s comment claims:
//
//	message: "to read the greeting write steps.greet.result in your expression"
//
// was rewritten to say `vars.greet`, in a file that then validated and ran,
// printing prose the author did not write. The over-read also decides whether a
// retired step is *migrated or deleted*, so a sentence in the margin was enough to
// remove a step somebody meant a person to see.
//
// Comments are stripped by the callers and quotes are deliberately not, because the
// ordinary spelling of an expression is a quoted YAML scalar —
// `message: "${steps.a.result}"` — so a scanner that skipped quoted regions would
// skip everything worth reading.
//
// Braces are counted rather than searched for, since an expression may contain
// them: `${ {'a': steps.x.result} }` closes at the last one and not the first. And
// the count skips CEL string literals, because a brace inside one is text —
// `${a + '}' + b}` also closes at the last brace, and stopping at the quoted one
// rewrites the first reference and leaves the second naming a step the same pass
// has just deleted.
//
// An unterminated fence yields the rest of the line, which keeps a half-written
// file's references visible while somebody is still editing it.
func expressionSpans(line string) [][2]int {
	var out [][2]int

	for i := 0; i+1 < len(line); i++ {
		if line[i] != '$' || line[i+1] != '{' {
			continue
		}

		depth, start := 1, i+2

		// Braces inside a CEL string are text, not structure. `${a + '}' + b}`
		// closes at the last brace, and a counter that stopped at the quoted one
		// would rewrite the first reference and leave the second naming a step the
		// same pass had just deleted — a current-edition file the validator then
		// rejects.
		var quote byte
		j := start
		for ; j < len(line) && depth > 0; j++ {
			c := line[j]
			switch {
			case quote != 0 && c == '\\':
				// An escape consumes the next character, so `\'` does not close the
				// literal it is inside.
				j++
			case quote != 0 && c == quote:
				quote = 0
			case quote != 0:
			case c == '\'' || c == '"':
				quote = c
			case c == '{':
				depth++
			case c == '}':
				depth--
			}
		}
		if depth > 0 {
			out = append(out, [2]int{start, len(line)})

			break
		}

		out = append(out, [2]int{start, j - 1})
		i = j - 1
	}

	return out
}

// rewriteExpressions applies a rewrite to each expression on a line, leaving
// everything around them untouched.
//
// Right to left, so an earlier span's offsets are still good after a later one has
// changed length.
func rewriteExpressions(line string, rewrite func(string) (string, bool)) (string, bool) {
	spans := expressionSpans(line)

	out, changed := line, false
	for i := len(spans) - 1; i >= 0; i-- {
		span := spans[i]

		replaced, did := rewrite(out[span[0]:span[1]])
		if !did {
			continue
		}

		out = out[:span[0]] + replaced + out[span[1]:]
		changed = true
	}

	return out, changed
}

// retiredStep rewrites one step running a retired task, or refuses.
//
// Returns whether it acted, so the caller knows not to look for anything else on a step
// that no longer exists.
func (f *fixer) retiredStep(step *ast.MappingNode, entry *ast.MappingValueNode, key string, scope stepScope) bool {
	read, ok := retiredTasks[key]
	if !ok {
		return false
	}

	id, hasID := stepID(step)
	if !hasID {
		f.refuse(entry.Key,
			"`%s:` is retired, and this step has no `id:` written as a plain value — so there is no name to move its value to; rewrite this step by hand",
			key)

		return true
	}

	if !f.referenced[id] {
		f.refuse(entry.Key,
			"`%s:` is retired and nothing reads `%s.%s.result`, so this cannot tell what the step was for: "+
				"a line for a person to see is `log:`, and a step that produced a value nobody uses can simply go. "+
				"Only you know which, so this leaves it alone",
			key, v1.StepsRoot, id)

		return true
	}

	// A guarded step does not always run, and a workflow var always evaluates.
	//
	// Lifting the value out drops the guard: an expression that never ran when the
	// condition was false now runs every time, before the first step. A step guarded
	// by `${vars.enabled}` computing something that fails when the feature is off is
	// the ordinary shape of that, and it turns a workflow that succeeded into one that
	// cannot start.
	//
	// Refused rather than carried, because there is nothing to carry it to: `vars:`
	// has no `if:`, and inventing one would be a grammar this build does not have.
	if condition, guarded := stepCondition(step); guarded {
		f.refuse(entry.Key,
			"`%s:` is retired and this step's value could move to `%s:` — but the step is guarded by "+
				"`if: %s`, and a `%s:` block has no condition: it is evaluated before the first step "+
				"runs, every time. Moving the value would run an expression that used to be skipped. "+
				"Write it under `%s:` on the step that uses it, which keeps the guard",
			key, varsKey, condition, varsKey, varsKey)

		return true
	}

	if scope.alone {
		f.refuse(entry.Key,
			"`%s:` is retired and this step's value could move to `%s:` — but it is the only step in its "+
				"list, and removing it would leave a loop body or a branch with nothing in it. Move the "+
				"value by hand, or give the block another step first",
			key, varsKey)

		return true
	}

	source, built := read(f, entry)
	if !built {
		return true
	}

	if f.declaredVars[id] {
		f.refuse(entry.Key,
			"`%s:` is retired and this step's value belongs under `%s:` as %q — but a var of that name is "+
				"already declared, and overwriting it would change what every reference to it means; rename one and run this again",
			key, varsKey, id)

		return true
	}

	// A workflow var is evaluated before the first step runs, so it can hold a value
	// that reads nothing — and a retired step reading an *earlier* step is a value
	// with nowhere to go. That is a limit of the destination rather than of this
	// rewriter, and the two ways out both need a judgement it does not have: inline
	// the expression at each place that uses it, or move it to the `vars:` of the one
	// step that does.
	//
	// Checked against what is left after the other moves are folded in, below, since a
	// reference to a step that is itself moving is not a reference to a step.
	folded := f.foldMovedInto(source)

	// A bare name bound where the step is written — a loop's iterator, a name an
	// enclosing block declared — exists nowhere at the top of the file.
	//
	// Nothing about the source gives this away: `${person}` is a word, and the two
	// checks below look for roots. So the walk carries the scope down (see stepScope),
	// and a value mentioning any of it stays where the name is.
	//
	// Matched textually, which can only be wrong in the safe direction: the name
	// appearing inside a string literal produces a refusal rather than a move.
	if name, mentioned := mentionsAny(folded, scope.bound); mentioned {
		f.refuse(entry.Key,
			"`%s:` is retired and this step's value reads `%s`, which is bound where the step is "+
				"written — by an enclosing loop or block — and means nothing at the top of the file. "+
				"Write the expression where it is used, or under `%s:` on the step that uses it",
			key, name, varsKey)

		return true
	}

	if reads := stepReference.FindStringSubmatch(folded); reads != nil {
		f.refuse(entry.Key,
			"`%s:` is retired and this step's value reads `%s.%s`, which a workflow var cannot — "+
				"`%s:` at the top of a file is evaluated before the first step runs. Write the expression "+
				"where it is used, or under `%s:` on the step that uses it",
			key, v1.StepsRoot, reads[1], varsKey, varsKey)

		return true
	}

	// The same limit from the other side: a var may not read a *var* either, because
	// `vars:` is a mapping and there is no order that would make one available to
	// another. A value already inlined by foldMovedInto is fine — that is what the
	// fold is for — but one reading a var the author declared has nowhere to go.
	if reads := varReference.FindStringSubmatch(folded); reads != nil {
		f.refuse(entry.Key,
			"`%s:` is retired and this step's value reads `%s.%s`, which another var cannot — "+
				"`%s:` is a mapping, so there is no order that would make one available to the other. "+
				"Write the expression where it is used, or under `%s:` on the step that uses it",
			key, v1.VarsRoot, reads[1], varsKey, varsKey)

		return true
	}

	// Recorded rather than written here. The `vars:` block is one place in the
	// document and several steps may move into it, so the entries are collected and
	// emitted together once the walk is done — writing each as it is found would mean
	// several edits at one line, and the edit map keeps only the first.
	f.movedVars = append(f.movedVars, movedVar{name: id, source: folded})
	f.deleteStep(step)

	return true
}

// retiredEcho reads the value an `echo:` step produced: its message.
func (f *fixer) retiredEcho(entry *ast.MappingValueNode) (string, bool) {
	value, found := inputValueNode(entry.Value, "message")
	if !found {
		f.refuse(entry.Key, "`echo:` is retired and this step has no `message:` to move; rewrite it by hand")

		return "", false
	}

	return f.celSourceOf(value, "echo", "message")
}

// retiredCEL reads the value a `cel:` step produced: its expression, already CEL.
func (f *fixer) retiredCEL(entry *ast.MappingValueNode) (string, bool) {
	value, found := inputValueNode(entry.Value, "expr")
	if !found {
		f.refuse(entry.Key, "`cel:` is retired and this step has no `expr:` to move; rewrite it by hand")

		return "", false
	}

	text, ok := scalarText(value)
	if !ok {
		f.refuse(entry.Key, "`cel:` is retired and this step's `expr:` is not written as a plain value; rewrite it by hand")

		return "", false
	}

	// A fenced `expr:` is evaluated *twice*, and only one of those survives a move.
	//
	// The fence produced a string, and the task then parsed and evaluated that string
	// as CEL: `expr: ${'1 + 2'}` was the integer 3. A var holds one expression and
	// evaluates it once, so the same text there is the string "1 + 2" — a rewritten
	// file that validates, runs, and quietly answers something else. That is the one
	// outcome a migration may not have.
	//
	// Refused rather than approximated. Computing expression source at run time has no
	// spelling in this language now, deliberately: an expression whose *text* is chosen
	// while the workload runs is the nondeterminism the whole design is arranged to
	// make inexpressible.
	if _, fenced := SplitFence(text); fenced {
		f.refuse(entry.Key,
			"`cel:` is retired and this step's `expr:` is itself an expression, so it was evaluated "+
				"twice — once to produce the source, once to run it. A `%s:` binding evaluates once, so "+
				"there is no rewrite that keeps the meaning. Write out the expression it was computing",
			varsKey)

		return "", false
	}

	// `expr` is the expression itself rather than a value containing one, so it is
	// already the source — unlike every other input, which carries a fence.
	return text, true
}

// stepCondition returns a step's `if:` as written.
func stepCondition(step *ast.MappingNode) (string, bool) {
	for _, v := range step.Values {
		if name, ok := keyNameOf(v.Key); ok && name == "if" {
			text, _ := scalarText(v.Value)

			return text, true
		}
	}

	return "", false
}

// mentionsAny reports the first of names that source uses as a whole word.
func mentionsAny(source string, names []string) (string, bool) {
	for _, name := range names {
		if name == "" {
			continue
		}
		if regexp.MustCompile(`\b` + regexp.QuoteMeta(name) + `\b`).MatchString(source) {
			return name, true
		}
	}

	return "", false
}

// retiredPrintf reads the value a `printf:` step produced, as a `format()` call.
//
// The replacement is not a task at all: the profile ships CEL's strings extension, so
// the formatting a task was wrapping is an operator on a string. What that buys is a
// specification — `format` is defined at the CEL level, cross-language and pinned by the
// profile, which a task wrapping Go's `fmt` could never be.
func (f *fixer) retiredPrintf(entry *ast.MappingValueNode) (string, bool) {
	formatNode, hasFormat := inputValueNode(entry.Value, "format")
	if !hasFormat {
		f.refuse(entry.Key, "`printf:` is retired and this step has no `format:` to move; rewrite it by hand")

		return "", false
	}

	format, ok := scalarText(formatNode)
	if !ok {
		f.refuse(entry.Key, "`printf:` is retired and this step's `format:` is not written as a plain value; rewrite it by hand")

		return "", false
	}
	if inner, fenced := SplitFence(format); fenced {
		// A computed format string. `format` takes the receiver as an expression
		// just as happily, so this composes rather than refusing.
		format = "(" + inner + ")"
	} else {
		format = strconv.Quote(format)
	}

	args, hasArgs := inputValueNode(entry.Value, "args")
	if !hasArgs {
		return format + ".format([])", true
	}

	list, isList := args.(*ast.SequenceNode)
	if !isList {
		f.refuse(entry.Key, "`printf:` is retired and this step's `args:` is not written as a list; rewrite it by hand")

		return "", false
	}

	sources := make([]string, 0, len(list.Values))
	for _, arg := range list.Values {
		source, ok := f.celSourceOf(arg, "printf", "args")
		if !ok {
			return "", false
		}
		sources = append(sources, source)
	}

	return format + ".format([" + strings.Join(sources, ", ") + "])", true
}

// celSourceOf reads one input value as CEL source.
//
// A fenced value is the expression inside it; anything else is a literal, and a string
// literal has to be *quoted* rather than pasted — the value is moving from a position
// where text is text into one where text is code.
func (f *fixer) celSourceOf(value ast.Node, task, input string) (string, bool) {
	// Numbers and booleans first, because they arrive as their own node kinds and
	// [scalarText] answers only for text. Asking it first refused them — so
	// `printf:` with `args: [${name.result}, 0]`, which is the migration DSL.md
	// documents, could not be rewritten at all, and the branch written below for
	// exactly these three kinds could never run. `flow fix` still stamped the new
	// edition, leaving a file that was neither the old spelling nor a valid one.
	switch node := unwrapAnchor(value).(type) {
	case *ast.IntegerNode, *ast.FloatNode, *ast.BoolNode:
		// Their own CEL source already: `0` is `0` in both languages, and quoting
		// would turn a number into a string.
		return node.String(), true
	}

	text, ok := scalarText(value)
	if !ok {
		f.refuse(value,
			"`%s:` is retired and this step's `%s` is not written as a plain value, so it cannot be moved into an expression; rewrite it by hand",
			task, input)

		return "", false
	}

	if inner, fenced := SplitFence(text); fenced {
		return inner, true
	}

	// Only text is left, and it needs quoting: the value is moving from a position
	// where text is text into one where text is code.
	return strconv.Quote(text), true
}

// inputValueNode returns one input's value from a task's block.
func inputValueNode(block ast.Node, name string) (ast.Node, bool) {
	mapping, ok := unwrapAnchor(block).(*ast.MappingNode)
	if !ok {
		if single, isOne := unwrapAnchor(block).(*ast.MappingValueNode); isOne {
			mapping = &ast.MappingNode{Values: []*ast.MappingValueNode{single}}
		} else {
			return nil, false
		}
	}
	for _, v := range mapping.Values {
		if key, ok := keyNameOf(v.Key); ok && key == name {
			return v.Value, true
		}
	}

	return nil, false
}

// stepID reads a step's id, when it is written as a plain value.
func stepID(step *ast.MappingNode) (string, bool) {
	for _, v := range step.Values {
		if key, ok := keyNameOf(v.Key); ok && key == "id" {
			return scalarText(v.Value)
		}
	}

	return "", false
}

// deleteStep removes a whole step from the sequence it sits in.
//
// The range is read from indentation rather than from the node's span, for the reason
// [fixer.taskBlock] gives: a span reaches a node's values and not the comments among
// them, so a span-derived end drops any comment written inside the step. This is a
// migration, and dropping an author's comment is losing their work — but here the whole
// step is going, so the comments inside it go with it, which is the one case where that
// is what was meant.
func (f *fixer) deleteStep(step *ast.MappingNode) {
	if len(step.Values) == 0 {
		return
	}
	span := spanOfNode(step.Values[0].Key)
	if !span.IsValid() {
		return
	}

	// Bounded by the *dash*, not by the first key.
	//
	// A step's keys all sit at one indent, so a range measured from the first key ends
	// at the second — `- id: greet` would go and `echo:` would stay, leaving an orphan
	// block where a step used to be. The dash is one level further out, and the lines
	// belonging to the step are everything indented past it, up to the next dash.
	// And the dash is not always on the same line as the first key. YAML allows it
	// on its own, and then a range starting at the key leaves the dash behind: the
	// sequence keeps an entry with nothing under it, `flow validate` refuses the
	// file with "must be a mapping of keys to values, but nothing was written
	// here", and `flow fix` had already exited zero. `flow fix . && git commit`
	// succeeded on a file the validator refuses, which is the one thing
	// cmd/flow/fix.go says must not happen.
	first := span.Start.Line
	if dash := f.dashLineAbove(first, span.Start.Column-2); dash > 0 {
		first = dash
	}

	through := f.blockEnd(span.Start.Line, span.Start.Column-2)

	f.record(first, through, nil, "step moved into `"+varsKey+":` and removed")
}

// dashLineAbove returns the line holding the sequence dash a step's keys begin
// below, or zero when the dash shares a line with the first key.
//
// column is where the dash would be: one indent level outside the keys, which is
// how [fixer.deleteStep] and [fixer.blockEnd] already locate it.
//
// Blank lines and comments between the dash and the first key are walked over
// rather than treated as the end of the search, because both are legal there and
// both belong to the step being removed. Anything else means the dash is not above
// this key after all, and nothing is claimed.
func (f *fixer) dashLineAbove(first, column int) int {
	for line := first - 1; line >= 1; line-- {
		text := f.line(line)

		trimmed := strings.TrimSpace(text)
		if trimmed == "" || strings.HasPrefix(trimmed, "#") {
			continue
		}

		// The dash alone. `- id: greet` never reaches here, because then the key is
		// on the dash's own line and this loop starts above both.
		if trimmed == "-" && indentWidth(text) == column-1 {
			return line
		}

		return 0
	}

	return 0
}

// A movedVar is one retired step's value, on its way to the `vars:` block.
type movedVar struct {
	// name is the step's id, which becomes the var's name. Not a choice: it is what
	// every reference already spells, so reusing it is what makes the reference
	// rewrite a rooting rather than a rename.
	name string

	// source is the CEL the value is written as, without a fence.
	source string
}

// writeMovedVars puts the collected values into the document's `vars:` block, creating
// one if there is none.
//
// Emitted together at the end rather than one at a time, because they all land in one
// place: several edits recorded at the same line would keep only the first, and the
// steps they came from are scattered through the file.
func (f *fixer) writeMovedVars(mapping *ast.MappingNode) {
	if len(f.movedVars) == 0 {
		return
	}

	lines := make([]string, 0, len(f.movedVars))
	for _, moved := range f.movedVars {
		lines = append(lines, "  "+moved.name+": "+yamlScalar(fenceOpen+moved.source+fenceClose))
	}

	// Appended to an existing block, so an author's own vars keep their order and
	// their comments.
	for _, v := range mapping.Values {
		key, ok := keyNameOf(v.Key)
		if !ok || key != varsKey {
			continue
		}
		keySpan := spanOfNode(v.Key)
		if !keySpan.IsValid() {
			return
		}
		last := f.blockEnd(keySpan.Start.Line, keySpan.Start.Column-1)
		f.record(last, last, append([]string{f.line(last)}, lines...),
			fmt.Sprintf("%d step(s) moved into `%s:`", len(f.movedVars), varsKey))

		return
	}

	// No block yet, so one is written above `steps:` — where an author writes it, and
	// where a reader looks for a value the steps below reach.
	for _, v := range mapping.Values {
		key, ok := keyNameOf(v.Key)
		if !ok || key != "steps" {
			continue
		}
		keySpan := spanOfNode(v.Key)
		if !keySpan.IsValid() {
			return
		}
		block := append([]string{varsKey + ":"}, lines...)
		f.record(keySpan.Start.Line, keySpan.Start.Line,
			append(block, f.line(keySpan.Start.Line)),
			fmt.Sprintf("%d step(s) moved into a new `%s:` block", len(f.movedVars), varsKey))

		return
	}
}

// movedReferences rewrites `steps.<id>.result` to `vars.<id>` for every step that moved.
//
// The reference does not change what it names — the id is the same word — so this is a
// re-rooting rather than a rename, which is what makes it safe to do without asking.
func (f *fixer) movedReferences(text string) (string, bool) {
	if len(f.movedVars) == 0 {
		return text, false
	}

	moved := make(map[string]bool, len(f.movedVars))
	for _, m := range f.movedVars {
		moved[m.name] = true
	}

	out := text
	for name := range moved {
		out = strings.ReplaceAll(out,
			v1.StepsRoot+"."+name+".result", v1.VarsRoot+"."+name)
	}

	return out, out != text
}

// collectRetirementContext answers the two questions the retirement rewrite depends on,
// before the walk that needs them.
//
// Both are about the *rest* of the document, which is why they cannot be answered while
// walking one step: whether anything reads a step's result decides which of the two
// cases it is, and which names `vars:` already holds decides whether it has somewhere to
// land.
func (f *fixer) collectRetirementContext(n ast.Node) {
	f.referenced = map[string]bool{}
	f.declaredVars = map[string]bool{}

	// Code only. A comment mentioning `steps.greet.result` is prose *about* a
	// reference and not one — and reading it as one flips the decision that matters
	// most here: a step nothing reads is the case this must refuse, so a sentence in
	// the margin was enough to delete an `echo` an author meant a person to see.
	//
	// The other direction was already harmless, which is why the tolerance looked
	// symmetric and is not: over-reading produces an unused `vars:` entry, and
	// under-reading produces a refusal. Only this one loses work.
	for _, line := range f.lines {
		if at := commentStart(line); at >= 0 {
			line = line[:at]
		}
		for _, span := range expressionSpans(line) {
			for _, match := range stepReference.FindAllStringSubmatch(line[span[0]:span[1]], -1) {
				f.referenced[match[1]] = true
			}
		}
	}

	mapping, ok := unwrapAnchor(n).(*ast.MappingNode)
	if !ok {
		return
	}
	for _, v := range mapping.Values {
		name, ok := keyNameOf(v.Key)
		if !ok || name != varsKey {
			continue
		}
		declared, isMapping := unwrapAnchor(v.Value).(*ast.MappingNode)
		if !isMapping {
			continue
		}
		for _, entry := range declared.Values {
			if varName, ok := keyNameOf(entry.Key); ok {
				f.declaredVars[varName] = true
			}
		}
	}
}

// rewriteMovedReferences re-roots every reference to a step that moved into `vars:`.
//
// Done in place over the lines rather than through the edit map, which is what lets it
// run *after* the rooting pass and read what that pass wrote: `${a.result}` becomes
// `${steps.a.result}` there and `${vars.a}` here, in that order, on the same line. The
// edit map is overlaid on these lines at the end, so the step deletions this pairs with
// compose rather than compete.
//
// Comments are left alone, for the reason nothing else here rewrites them: a comment is
// prose about the file rather than part of it, and an author's sentence rewritten to
// stay syntactically current is a sentence that now says something they did not write.
// They get a note instead, from the machinery that already exists for it.
func (f *fixer) rewriteMovedReferences() {
	if len(f.movedVars) == 0 {
		return
	}

	for i, line := range f.lines {
		code, comment := line, ""
		if at := commentStart(line); at >= 0 {
			code, comment = line[:at], line[at:]
		}

		// Inside the fences only. Rewriting the whole line is what edited an
		// author's prose, and the same over-read had already decided the step was
		// unread and deleted it.
		//
		// Except on a deferred input's value, which is expression source whole —
		// see [fixer.deferredValueLines]. There the whole of it is code, and
		// leaving it out let a rooted reference outlive the step it named.
		rewrite := func(source string) (string, bool) {
			return rewriteExpressions(source, f.movedReferences)
		}
		if f.deferredValueLines[i+1] {
			rewrite = f.movedReferences
		}

		rewritten, changed := rewrite(code)
		if !changed {
			continue
		}

		f.lines[i] = rewritten + comment
		f.substituted = true
	}
}

// foldMovedInto replaces references to already-moved steps with their values.
//
// Inlined rather than referenced, because a workflow var may not read another var —
// `vars:` is a mapping, so there is no order that would make one available to the other,
// and the validator says so. A `printf` formatting an `echo`'s result is the ordinary
// case, and two vars would be a file this build refuses.
//
// Parenthesised, because the value is moving into a larger expression and CEL's
// precedence is not the author's line breaks: `a + b` folded into `x * ...` has to stay
// the sum it was.
//
// Terminates because a step may only read steps before it, so the moves form a chain
// rather than a cycle — and each fold consumes one link of it.
func (f *fixer) foldMovedInto(source string) string {
	out := source
	for _, moved := range f.movedVars {
		out = strings.ReplaceAll(out,
			v1.StepsRoot+"."+moved.name+".result", "("+moved.source+")")
	}

	return out
}

// yamlScalar quotes a value when writing it plainly would make YAML read it as
// something else.
//
// A CEL map literal is the case that found this: `${{'outer': 'inner'}}` is a fenced
// expression to this language and a flow mapping to YAML, so an unquoted one is not a
// document. The value being moved is *code* now — it came from a position where YAML
// held it as a scalar and it is going into one where YAML has opinions about braces.
//
// Quoted only when needed, because a migration is a diff people read: quoting every
// moved value would put quotation marks on lines that never needed them and make the
// change look bigger than it is.
func yamlScalar(text string) string {
	if text == "" {
		return `""`
	}

	// The indicators YAML gives meaning to at the start of a scalar, plus the two
	// sequences it gives meaning to anywhere in one.
	if strings.ContainsAny(text[:1], "{}[]&*!|>'\"%@`#,?:-") ||
		strings.Contains(text, ": ") || strings.Contains(text, " #") {
		return strconv.Quote(text)
	}

	return text
}
