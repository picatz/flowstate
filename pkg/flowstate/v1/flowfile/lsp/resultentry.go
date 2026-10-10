package lsp

import (
	"fmt"
	"strings"

	"github.com/sourcegraph/go-lsp"

	v1 "github.com/picatz/flowstate/pkg/flowstate/v1"
	"github.com/picatz/flowstate/pkg/flowstate/v1/celcomplete"
	"github.com/picatz/flowstate/pkg/flowstate/v1/flowfile"
)

// The variable of a macro over a loop's `results`.
//
// `steps.fan.results.filter(r, r.|)` binds `r` to one entry of the loop's results:
// a map of the loop body's step ids to their outputs. The compiler knows the keys
// ([flowfile.ResultEntryOf]) and refuses a misspelling of one; this file is how a
// hover and a completion say the same keys before it does. The expression is read
// as text, because the one being typed does not parse: which macros are open at the
// cursor, what each ranges over, and what the variable of the one that ranges over
// a `results` list can be followed by.

// An openMacro is a comprehension macro whose argument list is open at the cursor.
type openMacro struct {
	variable string

	// receiver is the expression the macro is called on, as written.
	receiver string
}

// maxMacroFrames bounds how deep the scan tracks brackets, so a source made of
// nothing but openers cannot make it grow without limit.
const maxMacroFrames = 256

// macrosAround returns the comprehension macros whose arguments are open at offset
// at of src, outermost first.
func macrosAround(src string, at int) []openMacro {
	at = min(at, len(src))
	type frame struct{ macro *openMacro }
	var stack []frame

	for i := 0; i < at; {
		switch c := src[i]; c {
		case '"', '\'':
			i = stringEnd(src, i, false)
			if i > at {
				return nil // the cursor is inside a string
			}
			continue

		case '(', '[', '{':
			if len(stack) == maxMacroFrames {
				return nil
			}
			f := frame{}
			if c == '(' {
				f.macro = macroAt(src, i)
			}
			stack = append(stack, f)

		case ')', ']', '}':
			if len(stack) > 0 {
				stack = stack[:len(stack)-1]
			}
		}
		i++
	}

	var open []openMacro
	for _, f := range stack {
		if f.macro != nil {
			open = append(open, *f.macro)
		}
	}

	return open
}

// macroAt reads the parenthesis at open as the start of a comprehension macro's
// arguments: `<receiver>.<macro>(<variable>,`.
func macroAt(src string, open int) *openMacro {
	end := open
	start := end
	for start > 0 && isIdentPart(src[start-1]) {
		start--
	}
	if start == end || start == 0 || src[start-1] != '.' || !bindsFirstArgument[src[start:end]] {
		return nil
	}
	variable, ok := firstArgument(src, open)
	if !ok {
		return nil
	}

	return &openMacro{variable: variable, receiver: receiverBefore(src, start-1)}
}

// receiverBefore is the chain of names, calls and indexes that ends just before the
// dot at src[dot]: `steps.fan.results.filter(r, true)` before the `.map` that
// follows it. It stops at anything an operand cannot contain, so `a in xs.map` is
// `xs`.
func receiverBefore(src string, dot int) string {
	j := dot
	for j > 0 {
		c := src[j-1]
		switch {
		case isIdentPart(c) || c == '.' || c == '?':
			j--

		case c == ')' || c == ']':
			opener := matchingOpener(src, j-1)
			if opener < 0 {
				return strings.TrimSpace(src[j:dot])
			}
			j = opener

		default:
			return strings.TrimSpace(src[j:dot])
		}
	}

	return strings.TrimSpace(src[:dot])
}

// matchingOpener is the offset of the bracket that closes at src[closer], or -1.
// A string in between is skipped by quote, which is the approximation of a scan
// that runs backwards.
func matchingOpener(src string, closer int) int {
	depth := 0
	for i := closer; i >= 0; i-- {
		switch src[i] {
		case ')', ']', '}':
			depth++
		case '(', '[', '{':
			depth--
			if depth == 0 {
				return i
			}
		}
	}

	return -1
}

// resultEntryFor is the entry the macro variable name is bound to at the cursor,
// from the step it is written in. The innermost macro that binds the name decides:
// one that binds it to anything but a `results` list hides an outer entry.
func resultEntryFor(wf *v1.Workflow, step string, macros []openMacro, name string) (flowfile.ResultEntry, bool) {
	for i := len(macros) - 1; i >= 0; i-- {
		if macros[i].variable != name {
			continue
		}

		return flowfile.ResultEntryOf(wf, step, macros[i].receiver)
	}

	return flowfile.ResultEntry{}, false
}

// resultEntryLocals are the candidates for the macro variables open in the
// expression typed so far that are bound to an entry, so `r.` offers the body's
// step ids and `r.check.` the outputs of that step.
func resultEntryLocals(doc *document, pos lsp.Position, step, inner string) []celcomplete.Candidate {
	macros := macrosAround(inner, len(inner))
	if len(macros) == 0 || step == "" {
		return nil
	}

	wf := compiledWorkflow(doc)
	if wf == nil {
		// The expression being typed rarely parses, and a document that does not
		// compile declares no loop: compile it with this expression stood in for.
		wf = compiledText(doc, withoutFenceAt(doc, pos))
	}
	if wf == nil {
		return nil
	}

	var locals []celcomplete.Candidate
	seen := map[string]bool{}
	for i := len(macros) - 1; i >= 0; i-- {
		name := macros[i].variable
		if seen[name] {
			continue
		}
		seen[name] = true

		entry, ok := resultEntryFor(wf, step, macros, name)
		if !ok {
			continue
		}
		locals = append(locals, entryCandidate(name, entry))
	}

	return locals
}

// entryCandidate is the candidate for a macro variable bound to an entry.
func entryCandidate(name string, entry flowfile.ResultEntry) celcomplete.Candidate {
	members := make([]celcomplete.Candidate, 0, len(entry.Steps))
	for _, step := range entry.Steps {
		member := celcomplete.Candidate{
			Name:   step.ID,
			Kind:   celcomplete.KindField,
			Detail: "body step",
			Docs:   fmt.Sprintf("The outputs of the step %q in this iteration of the loop %q.", step.ID, entry.Loop),
		}
		for _, output := range step.Outputs {
			member.Members = append(member.Members, celcomplete.Candidate{
				Name:   output.Name,
				Kind:   celcomplete.KindField,
				Detail: output.Type,
				Docs:   output.Description,
			})
		}
		members = append(members, member)
	}

	return celcomplete.Candidate{
		Name:    name,
		Kind:    celcomplete.KindValue,
		Detail:  fmt.Sprintf("an entry of %s.results", entry.Loop),
		Docs:    entryDoc(entry),
		Members: members,
	}
}

// entryDoc describes an entry: what it is, and the keys it has.
func entryDoc(entry flowfile.ResultEntry) string {
	var b strings.Builder
	fmt.Fprintf(&b, "One iteration of the loop `%s`: a map of the body's step ids to that step's outputs.", entry.Loop)
	for _, step := range entry.Steps {
		fmt.Fprintf(&b, "\n\n- `%s`", step.ID)
		if len(step.Outputs) > 0 {
			names := make([]string, 0, len(step.Outputs))
			for _, output := range step.Outputs {
				names = append(names, "`"+output.Name+"`")
			}
			fmt.Fprintf(&b, ": %s", strings.Join(names, ", "))
		}
	}

	return b.String()
}

// hoverResultEntry describes the macro variable under the cursor when it is bound
// to an entry of a loop's `results`, and the step id or output after it.
//
// Nil when the cursor is on anything else.
func hoverResultEntry(doc *document, from *parsedStep, v *value, f fence, cursor int) *lsp.Hover {
	start, word, ok := wordAt(f.source, cursor)
	if !ok || from == nil || from.id == "" {
		return nil
	}

	macros := macrosAround(f.source, cursor)
	if len(macros) == 0 {
		return nil
	}

	segments := strings.Split(word, ".")
	segments = segments[:min(len(segments), 3)]
	upto, end := -1, start
	for i, s := range segments {
		end += len(s)
		if cursor <= end {
			upto = i
			break
		}
		end++ // the dot
	}
	if upto < 0 || segments[upto] == "" {
		return nil
	}
	end = start + len(strings.Join(segments[:upto+1], "."))

	wf := compiledWorkflow(doc)
	if wf == nil {
		return nil
	}
	entry, ok := resultEntryFor(wf, from.id, macros, segments[0])
	if !ok {
		return nil
	}

	rng := v.fenceSpanOrWhole(doc.index, f, start, end)
	if upto == 0 {
		return markdownHover("**`"+segments[0]+"`**: "+entryDoc(entry), rng)
	}

	for _, step := range entry.Steps {
		if step.ID != segments[1] {
			continue
		}
		if upto == 1 {
			var b strings.Builder
			fmt.Fprintf(&b, "**`%s.%s`**: the outputs of the body step `%s` in one iteration of the loop `%s`.", segments[0], step.ID, step.ID, entry.Loop)
			for _, output := range step.Outputs {
				fmt.Fprintf(&b, "\n\n- `%s`", output.Name)
				if output.Type != "" {
					fmt.Fprintf(&b, ": `%s`", output.Type)
				}
			}

			return markdownHover(b.String(), rng)
		}
		for _, output := range step.Outputs {
			if output.Name != segments[2] {
				continue
			}
			text := fmt.Sprintf("**`%s.%s.%s`**", segments[0], step.ID, output.Name)
			if output.Type != "" {
				text += fmt.Sprintf(": `%s`", output.Type)
			}
			if output.Description != "" {
				text += "\n\n" + output.Description
			}

			return markdownHover(text, rng)
		}
	}

	return nil
}
