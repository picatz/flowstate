package flowfile

import (
	"bytes"
	"fmt"
	"slices"
	"strconv"
	"strings"

	"github.com/goccy/go-yaml/ast"
	"github.com/goccy/go-yaml/parser"
	"github.com/goccy/go-yaml/token"
)

// Fix rewrites a Flowfile written in an older edition into the current one.
//
// This is the other half of a decision recorded in docs/DSL.md: surface syntax
// gets no deprecation window, because carrying two spellings costs the parser,
// the validator, the language server, the marshaller, and every test matrix that
// crosses them, for as long as the window lasts — and windows do not close on
// schedule. What makes that decision affordable rather than merely strict is
// that the migration is a program someone runs in a second.
//
// # Why this is not the marshaller
//
// The obvious implementation is parse-then-marshal, and it is wrong twice over.
// The old grammar no longer parses, which is the entire point; and a formatter
// rewrites a whole document, so an author fixing one retired key would get every
// comment moved and every string requoted in the same diff. A migration is a
// thing people read in review, and a diff that touches every line is a diff
// nobody reads.
//
// So this works on the document's own token stream, edits the ranges it must,
// and copies the rest through byte for byte. A file with nothing to fix comes
// back identical, which is what makes running it on a whole directory safe.
//
// # Refusing
//
// It refuses rather than guesses. Flow style — `task: {name: echo, inputs: {…}}`
// — has no line structure to rewrite, and a rewriter that reflows it is a
// rewriter that reformats an author's file in ways they did not ask for. An
// alias standing in for a task block cannot be rewritten at all without knowing
// what it will contain. In both cases the file is left alone and the position is
// reported, which is worth more than a mangled file that looks fixed.

// A FixResult reports what [Fix] did to one document.
type FixResult struct {
	// Source is the rewritten document, or the original when nothing changed.
	Source []byte

	// Changes describes each edit made, in source order, for a caller that wants
	// to tell an author what happened rather than only that something did.
	Changes []FixChange

	// Refusals are the places the rewriter could not act on safely. A document
	// with refusals is still rewritten everywhere else — stopping entirely would
	// mean one unrewritable step blocks the other nine — but it is not finished,
	// and a caller must say so.
	Refusals []Diagnostic
}

// Changed reports whether the rewrite altered the document.
func (r FixResult) Changed() bool { return len(r.Changes) > 0 }

// A FixChange is one edit the rewriter made.
type FixChange struct {
	// Line is the 1-based line the change was made at.
	Line int

	// Message says what changed, in the terms an author wrote the file in.
	Message string
}

// Fix rewrites data into the current edition.
//
// A document that already compiles is returned unchanged with no changes
// recorded, so this is safe to run over a directory that is mostly current.
//
// An error is returned only when the document is not YAML at all. Everything
// else — a shape that cannot be rewritten, a key that means nothing — is
// reported through [FixResult] so that a caller can rewrite what it can and say
// what it could not.
func Fix(data []byte) (FixResult, error) {
	if len(data) > maxBytes {
		return FixResult{}, Diagnostics{{
			Line:   1,
			Column: 1,
			Message: fmt.Sprintf(
				"file is %d bytes, larger than the %d byte limit a Flowfile is read up to; nothing was rewritten",
				len(data), maxBytes),
		}}
	}

	file, err := parser.ParseBytes(data, parser.ParseComments)
	if err != nil {
		return FixResult{}, err
	}

	f := &fixer{
		lines:           splitLines(data),
		trailingNewline: bytes.HasSuffix(data, []byte("\n")),
		terminator:      lineTerminator(data),
	}
	for _, doc := range file.Docs {
		f.workflow(doc.Body)
	}

	if len(f.edits) == 0 {
		return FixResult{Source: data, Refusals: f.refusals}, nil
	}
	return FixResult{
		Source:   f.apply(),
		Changes:  f.changes,
		Refusals: f.refusals,
	}, nil
}

// A fixer accumulates line edits over one document.
//
// Edits are recorded against line numbers and applied at the end, so that
// rewriting one step cannot move the lines another step was located at. A
// rewriter that edits as it walks has to keep an offset, and an offset is a
// thing to get wrong.
type fixer struct {
	lines []string

	// trailingNewline records whether the source ended with one, so that a file
	// that did not gets one back the same way. It is a byte nobody asked to have
	// changed, and a migration that quietly adds it puts a line in the diff of
	// every file it touches that has nothing to do with the migration.
	trailingNewline bool

	// terminator is how this document ends its lines. A rewritten line has to end
	// the same way the copied ones do, or a CRLF file comes back with LF on the
	// lines that changed — mixed endings in a file the tool promised to leave
	// alone except where it had to act.
	terminator string

	edits    map[int]lineEdit
	changes  []FixChange
	refusals []Diagnostic
}

// A lineEdit replaces a run of source lines with new text.
type lineEdit struct {
	// through is the last line the edit consumes, 1-based and inclusive.
	through int

	// replacement is the lines written in their place, already indented. Empty
	// deletes the run.
	replacement []string
}

// record adds an edit, keeping the first when two overlap.
func (f *fixer) record(line, through int, replacement []string, message string) {
	if f.edits == nil {
		f.edits = make(map[int]lineEdit)
	}
	if _, taken := f.edits[line]; taken {
		return
	}
	f.edits[line] = lineEdit{through: through, replacement: replacement}
	f.changes = append(f.changes, FixChange{Line: line, Message: message})
}

// refuse records a place the rewriter would have had to guess.
func (f *fixer) refuse(n ast.Node, format string, args ...any) {
	span := spanOfNode(n)
	f.refusals = append(f.refusals, Diagnostic{
		Line:    span.Start.Line,
		Column:  span.Start.Column,
		Message: fmt.Sprintf(format, args...),
	})
}

// unwrapAnchor returns the node an anchor names, or the node itself.
//
// An anchor is written *on* a value — `- &first` above a step's keys — so every
// walker below has to look through one or it sees a shape it does not recognise
// and returns. It did, and the result was the worst outcome this command can
// produce: `flow fix` reported "already current" and exited zero on a file that
// `flow validate` refuses, which is precisely the "`flow fix . && git commit`
// must not succeed" property it exists to hold.
//
// An *alias* is deliberately not followed. It is a reference to a value written
// elsewhere, and that value is rewritten where it was declared; following it
// would send the rewriter at lines belonging to another step.
func unwrapAnchor(n ast.Node) ast.Node {
	for {
		anchor, ok := n.(*ast.AnchorNode)
		if !ok {
			return n
		}
		n = anchor.Value
	}
}

// workflow walks a document body, rewriting its steps and its edition marker.
func (f *fixer) workflow(n ast.Node) {
	n = unwrapAnchor(n)
	mapping, ok := n.(*ast.MappingNode)
	if !ok {
		if single, isOne := n.(*ast.MappingValueNode); isOne {
			mapping = &ast.MappingNode{Values: []*ast.MappingValueNode{single}}
		} else {
			return
		}
	}
	for _, v := range mapping.Values {
		name, ok := keyNameOf(v.Key)
		if !ok {
			continue
		}
		switch name {
		case "steps":
			f.steps(v.Value)
		case "edition":
			f.edition(v)
		}
	}
}

// edition brings a declared edition marker up to the current one.
//
// Without this the two halves of the design contradict each other: an older
// edition is refused with "run `flow fix` to rewrite the file", and `flow fix`
// would answer "already current" while leaving the marker that caused the
// refusal. A migration tool that does not migrate the thing whose diagnostic
// names it is a migration tool nobody will trust twice.
//
// Only a marker that is written is updated. A file with no `edition:` is a file
// that has not asked to be pinned, and stamping one in would be the rewriter
// adding an opinion the author did not have.
func (f *fixer) edition(entry *ast.MappingValueNode) {
	declared, ok := editionText(entry.Value)
	if !ok || declared == CurrentEdition {
		return
	}

	// Only editions this build knows how to bring forward. A marker from the
	// future is a file a newer `flow` wrote, and rewriting it to an older edition
	// would be this build claiming to understand a grammar it does not have.
	if !slices.Contains(knownEditions, declared) {
		f.refuse(entry.Value,
			"edition %q is not one this build knows, so there is nothing to rewrite it to; a newer flow wrote this file",
			declared)
		return
	}

	keySpan := spanOfNode(entry.Key)
	if !keySpan.IsValid() {
		return
	}
	indent := strings.Repeat(" ", keySpan.Start.Column-1)
	// The key's own line and no more. An edition is a scalar written beside its
	// key, so taking the block under it would consume anything indented on the next
	// line — a comment, most likely — and delete it while claiming to have updated
	// a version number.
	f.record(keySpan.Start.Line, keySpan.Start.Line,
		[]string{indent + "edition: " + strconv.Quote(CurrentEdition)},
		fmt.Sprintf("edition %q updated to %q", declared, CurrentEdition))
}

// steps walks a sequence of steps, at any nesting depth.
func (f *fixer) steps(n ast.Node) {
	seq, ok := unwrapAnchor(n).(*ast.SequenceNode)
	if !ok {
		return
	}
	for _, step := range seq.Values {
		f.step(step)
	}
}

// step rewrites one step, and descends into any steps nested inside it.
func (f *fixer) step(n ast.Node) {
	var values []*ast.MappingValueNode
	switch node := unwrapAnchor(n).(type) {
	case *ast.MappingNode:
		values = node.Values
	case *ast.MappingValueNode:
		values = []*ast.MappingValueNode{node}
	default:
		return
	}

	for _, v := range values {
		name, ok := keyNameOf(v.Key)
		if !ok {
			continue
		}
		switch name {
		case "task":
			f.taskBlock(v)
		case "for_each":
			f.nested(v.Value, "steps")
		case "parallel":
			f.branches(v.Value)
		}
	}
}

// nested descends into a named key holding a step sequence.
func (f *fixer) nested(n ast.Node, key string) {
	n = unwrapAnchor(n)
	mapping, ok := n.(*ast.MappingNode)
	if !ok {
		if single, isOne := n.(*ast.MappingValueNode); isOne {
			mapping = &ast.MappingNode{Values: []*ast.MappingValueNode{single}}
		} else {
			return
		}
	}
	for _, v := range mapping.Values {
		if name, ok := keyNameOf(v.Key); ok && name == key {
			f.steps(v.Value)
		}
	}
}

// branches descends into a parallel's list of branches.
func (f *fixer) branches(n ast.Node) {
	seq, ok := unwrapAnchor(n).(*ast.SequenceNode)
	if !ok {
		return
	}
	for _, branch := range seq.Values {
		f.nested(branch, "steps")
	}
}

// taskBlock rewrites `task:` / `name:` / `inputs:` into the task's own key.
//
// The transformation is a deletion and a rename, which is why it can be done on
// lines at all: `task:` and `name:` go away, `inputs:` becomes the task's name,
// and everything under `inputs:` dedents by one level. A description moves up to
// the step, which is where prose about a step lives now.
func (f *fixer) taskBlock(entry *ast.MappingValueNode) {
	keySpan := spanOfNode(entry.Key)
	if !keySpan.IsValid() {
		return
	}

	block, ok := entry.Value.(*ast.MappingNode)
	if !ok {
		if single, isOne := entry.Value.(*ast.MappingValueNode); isOne {
			block = &ast.MappingNode{Values: []*ast.MappingValueNode{single}}
		} else {
			f.refuse(entry.Value,
				"`task:` here is %s rather than a mapping of `name:` and `inputs:`, so there is no task name to rewrite it to; fix this step by hand",
				describeNode(entry.Value))
			return
		}
	}
	if block.IsFlowStyle {
		f.refuse(entry.Value,
			"`task:` is written in flow style, which has no line structure to rewrite; write it across lines and run this again")
		return
	}

	var (
		nameNode        ast.Node
		inputsKey       ast.Node
		inputsNode      ast.Node
		descriptionNode ast.Node
	)
	for _, v := range block.Values {
		key, ok := keyNameOf(v.Key)
		if !ok {
			continue
		}
		switch key {
		case "name":
			nameNode = v.Value
		case "inputs":
			inputsKey, inputsNode = v.Key, v.Value
		case "description":
			descriptionNode = v.Value
		}
	}

	taskName, ok := scalarText(nameNode)
	if !ok {
		f.refuse(entry.Value,
			"`task:` has no `name:` written as a plain value, so the key to rewrite it to is not known here; fix this step by hand")
		return
	}

	// The whole `task:` block is replaced, so the run of lines it covers has to be
	// exact — and it is read from indentation rather than from the tokens beneath
	// it. A node's span reaches its values and not the comments among them, so a
	// span-derived end drops any comment written inside the block, and this is a
	// migration: dropping an author's comment is losing their work.
	taskIndent := keySpan.Start.Column - 1
	through := f.blockEnd(keySpan.Start.Line, taskIndent)
	indent := strings.Repeat(" ", taskIndent)

	var replacement []string

	// Comments written among the keys that are going away — above `name:`, after
	// the inputs, anywhere in the block that is not among the inputs themselves —
	// described the task, and the task is still here. Carried up to sit above its
	// key rather than deleted with the lines they were on.
	replacement = append(replacement, f.commentsOutsideInputs(keySpan.Start.Line, through, inputsKey, indent)...)

	if descriptionNode != nil {
		text, ok := scalarText(descriptionNode)
		if !ok {
			f.refuse(descriptionNode,
				"the task's `description:` is not a plain value, so it cannot be moved to the step; write it as one and run this again")
			return
		}
		replacement = append(replacement, indent+"description: "+quoteScalar(text))
	}

	inputLines, ok := f.inputLines(inputsKey, inputsNode, taskIndent)
	if !ok {
		return
	}
	if len(inputLines) == 0 {
		// A task with no inputs is written `echo: {}` on one line. The empty
		// mapping is deliberate — `echo:` alone reads as an unfinished line — but
		// putting it on a line of its own reads as one too.
		replacement = append(replacement, indent+taskName+": {}")
	} else {
		replacement = append(replacement, indent+taskName+":")
		replacement = append(replacement, inputLines...)
	}

	f.record(keySpan.Start.Line, through, replacement,
		fmt.Sprintf("`task:` naming %q rewritten to `%s:`", taskName, taskName))
}

// inputLines renders a task's inputs dedented by one level under the task's key.
//
// The source text is copied rather than re-rendered, so a comment, a block
// scalar, or a hand-chosen quoting style survives. Only the indentation changes,
// and only by the fixed amount `inputs:` used to add.
func (f *fixer) inputLines(inputsKey, inputs ast.Node, taskIndent int) ([]string, bool) {
	// No inputs at all, and `inputs:` written with nothing under it, are the same
	// task. The caller writes both as `echo: {}`.
	if inputs == nil {
		return nil, true
	}
	if _, empty := inputs.(*ast.NullNode); empty {
		return nil, true
	}

	mapping, ok := inputs.(*ast.MappingNode)
	if !ok {
		if single, isOne := inputs.(*ast.MappingValueNode); isOne {
			mapping = &ast.MappingNode{Values: []*ast.MappingValueNode{single}, IsFlowStyle: false}
		} else {
			f.refuse(inputs,
				"`inputs:` is %s rather than a mapping, so it cannot be moved under the task's name; fix this step by hand",
				describeNode(inputs))
			return nil, false
		}
	}
	if mapping.IsFlowStyle {
		f.refuse(inputs,
			"`inputs:` is written in flow style, which has no line structure to rewrite; write it across lines and run this again")
		return nil, false
	}

	keySpan := spanOfNode(inputsKey)
	if !keySpan.IsValid() {
		f.refuse(inputs, "`inputs:` has no source position, so it cannot be moved; fix this step by hand")
		return nil, false
	}

	// The block is the lines under `inputs:`, read by indentation rather than from
	// the nodes beneath it — so a comment written among the inputs is carried along
	// with them instead of falling into a gap between two token spans.
	inputsIndent := keySpan.Start.Column - 1
	first := keySpan.Start.Line + 1
	last := f.blockEnd(keySpan.Start.Line, inputsIndent)
	if last < first {
		return nil, true
	}

	// How far every line moves left. The values sat two levels in from `task:` —
	// once for `inputs:` and once for themselves — and end up one level in from the
	// task's own key, which is where a task's inputs go.
	//
	// Measured from the first line with something on it. A blank line straight
	// under `inputs:` is legal and common, and its indent is zero, so measuring
	// from whatever line came first refused a perfectly good file — telling an
	// author their indentation was wrong when it was not, which is the kind of
	// diagnostic that teaches people to stop reading them.
	shift := indentWidth(f.line(f.firstContentLine(first, last))) - (taskIndent + 2)
	if shift < 0 {
		f.refuse(inputs,
			"the values under `inputs:` are indented less than the key they belong to, so dedenting them would change what they nest under; fix this step by hand")
		return nil, false
	}

	out := make([]string, 0, last-first+1)
	for n := first; n <= last; n++ {
		line := f.line(n)
		if strings.TrimSpace(line) == "" {
			out = append(out, "")
			continue
		}
		if indentWidth(line) < shift {
			f.refuse(inputs,
				"line %d is indented less than the values under `inputs:` it sits among, so this block cannot be dedented as a whole; fix this step by hand", n)
			return nil, false
		}
		out = append(out, line[shift:])
	}
	return out, true
}

// commentsOutsideInputs returns the comment-only lines of a `task:` block that do
// not belong to its inputs, re-indented to the given level.
//
// They are the comments about the task itself: the ones explaining `name:`, and
// any written after the inputs at the level of the keys being removed. A comment
// among the *inputs* travels with them and is not collected here — which is why
// this is a whole-block scan with a hole in it rather than a scan that stops at
// the inputs. Stopping there dropped every comment written below them.
func (f *fixer) commentsOutsideInputs(taskLine, through int, inputsKey ast.Node, indent string) []string {
	inputsFirst, inputsLast := 0, -1
	if span := spanOfNode(inputsKey); span.IsValid() {
		inputsFirst = span.Start.Line
		inputsLast = f.blockEnd(span.Start.Line, span.Start.Column-1)
	}

	var out []string
	for n := taskLine; n <= through; n++ {
		if n > taskLine && n >= inputsFirst && n <= inputsLast {
			continue
		}
		text := strings.TrimSpace(f.line(n))
		if strings.HasPrefix(text, "#") {
			out = append(out, indent+text)
			continue
		}
		// A comment written at the end of a key that is going away goes with the
		// rest of that key's line otherwise. `name: echo # the greeting one` says
		// something about the task, and the task is still here.
		if comment := trailingComment(text); comment != "" {
			out = append(out, indent+comment)
		}
	}
	if inputsFirst > 0 {
		if comment := trailingComment(strings.TrimSpace(f.line(inputsFirst))); comment != "" {
			out = append(out, indent+comment)
		}
	}
	return out
}

// trailingComment returns the comment at the end of a line, or the empty string
// when there is none.
//
// Only called on the structural lines a task block is made of — `task:`,
// `name: <task>`, `inputs:` — which is what makes a simple rule safe. Two of them
// have no value at all, and a task name is `[A-Za-z][A-Za-z0-9_-]*`, so a `#`
// after the colon on any of them is a comment and cannot be part of a value.
//
// A line carrying a quote is left alone regardless. Deciding whether a `#` inside
// a string is a comment means lexing YAML, and a rewriter that guesses wrong there
// truncates an author's value — which is worse than dropping the comment it was
// trying to save.
func trailingComment(line string) string {
	if strings.ContainsAny(line, `"'`) {
		return ""
	}
	i := strings.Index(line, " #")
	if i < 0 {
		return ""
	}
	return strings.TrimSpace(line[i+1:])
}

// firstContentLine returns the first line in a range with something other than
// whitespace on it, or the range's start when there is none.
func (f *fixer) firstContentLine(first, last int) int {
	for n := first; n <= last; n++ {
		if strings.TrimSpace(f.line(n)) != "" {
			return n
		}
	}
	return first
}

// blockEnd returns the last line belonging to the block a key opens.
//
// A block is its key's line plus every following line indented further. Unlike a
// node's token span it takes in the comments written among the values, which a
// rewriter must carry rather than drop — a comment is the part of a file a tool
// can least afford to lose.
//
// A comment's own indentation says nothing about YAML's structure — people dedent
// one to the margin all the time — so a comment never *ends* the block. Only a
// content line at or left of the key does that. A comment indented past the key is
// still part of the block and extends it, which is what keeps a note written under
// the last input travelling with the inputs.
//
// Treating a dedented comment as the end was a real defect and the worst-shaped
// kind: the replacement consumed only the lines above it, the `name:` and
// `inputs:` below it were left where they were, and the rewriter reported success
// on a document it had just mangled.
func (f *fixer) blockEnd(keyLine, indent int) int {
	last := keyLine
	for n := keyLine + 1; n <= len(f.lines); n++ {
		line := f.line(n)
		trimmed := strings.TrimSpace(line)
		if trimmed == "" {
			continue
		}
		comment := strings.HasPrefix(trimmed, "#")
		if indentWidth(line) <= indent {
			if comment {
				// Neither in the block nor the end of it. A comment at the key's own
				// level after the block belongs to whatever comes next.
				continue
			}
			break
		}
		last = n
	}
	return last
}

// line returns a 1-based source line, or empty past the end.
func (f *fixer) line(n int) string {
	if n < 1 || n > len(f.lines) {
		return ""
	}
	return f.lines[n-1]
}

// apply writes the edits into the source.
func (f *fixer) apply() []byte {
	var b strings.Builder
	for n := 1; n <= len(f.lines); n++ {
		edit, edited := f.edits[n]
		if !edited {
			b.WriteString(f.lines[n-1])
			b.WriteString(f.terminator)
			continue
		}
		for _, line := range edit.replacement {
			b.WriteString(line)
			b.WriteString(f.terminator)
		}
		n = edit.through
	}

	out := b.String()
	if !f.trailingNewline {
		out = strings.TrimSuffix(out, f.terminator)
	}
	return []byte(out)
}

// splitLines splits source into lines without their terminators.
//
// A carriage return is stripped with the newline rather than left on the end of
// the line, so that every line this package measures, compares, or re-indents is
// the line's text and nothing else. [fixer.terminator] puts it back.
func splitLines(data []byte) []string {
	text := strings.TrimSuffix(string(data), "\n")
	text = strings.TrimSuffix(text, "\r")
	if text == "" {
		return nil
	}
	lines := strings.Split(text, "\n")
	for i, line := range lines {
		lines[i] = strings.TrimSuffix(line, "\r")
	}
	return lines
}

// lineTerminator reports how a document ends its lines.
//
// The first one decides, because a document with both is already inconsistent and
// this is not the tool to normalise it: a rewriter that changed every line ending
// in a file it was asked to fix one step of would be doing something nobody asked
// for.
func lineTerminator(data []byte) string {
	if i := bytes.IndexByte(data, '\n'); i > 0 && data[i-1] == '\r' {
		return "\r\n"
	}
	return "\n"
}

// indentWidth returns how many leading spaces a line has.
func indentWidth(line string) int {
	for i, r := range line {
		if r != ' ' {
			return i
		}
	}
	return len(line)
}

// scalarText returns the text of a plain or quoted scalar.
func scalarText(n ast.Node) (string, bool) {
	switch node := n.(type) {
	case *ast.StringNode:
		return node.Value, true
	case *ast.LiteralNode:
		return blockText(node), true
	default:
		return "", false
	}
}

// quoteScalar writes a string as YAML, quoting only when it would otherwise read
// as something else.
//
// A rewriter that quotes everything produces a diff full of changes nobody asked
// for, and one that quotes nothing eventually writes a description of `yes` and
// turns it into a boolean.
func quoteScalar(s string) string {
	if s == "" {
		return `""`
	}
	if strings.ContainsAny(s, ":#\n\"'{}[]&*!|>%@`") || s != strings.TrimSpace(s) {
		return strconv.Quote(s)
	}
	if slices.Contains(yamlReservedScalars, strings.ToLower(s)) {
		return strconv.Quote(s)
	}
	if _, isNumber := parseYAMLNumber(s); isNumber {
		return strconv.Quote(s)
	}
	return s
}

// yamlReservedScalars are the unquoted words YAML reads as something other than
// a string.
var yamlReservedScalars = []string{
	"y", "n", "yes", "no", "true", "false", "on", "off", "null", "~",
}

// parseYAMLNumber reports whether a scalar would be read as a number.
func parseYAMLNumber(s string) (string, bool) {
	tok := token.New(s, s, nil)
	switch tok.Type {
	case token.IntegerType, token.FloatType, token.InfinityType, token.NanType:
		return s, true
	default:
		return "", false
	}
}
