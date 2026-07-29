package lsp

import (
	v1 "github.com/picatz/flowstate/pkg/flowstate/v1"
	"regexp"
	"slices"
	"strings"
)

// Completion cannot use the YAML parser. A document being typed is usually not
// valid YAML — a task's key followed by a half-written input is a syntax error —
// and that is exactly the moment the author wants a suggestion. So this file
// reads the document by line, which degrades instead of failing.
//
// That trade is right for completion and wrong for diagnostics: a line scan can
// misjudge an unusual document, and a wrong diagnostic is worse than none.
// Diagnostics therefore use the parsed model in parse.go, and the two never share
// a code path.

// keyLine matches a `key: rest` line, allowing a leading sequence dash.
//
// The key must look like an identifier so that a wrapped value containing a colon
// — a URL, say — is not mistaken for a new key.
var keyLine = regexp.MustCompile(`^(\s*)(-\s+)?([A-Za-z_][A-Za-z0-9_.-]*)\s*:(.*)$`)

// dashLine matches a block sequence entry.
var dashLine = regexp.MustCompile(`^(\s*)-(\s|$)`)

// An outlineStep is one step of a Flowfile as the line scan sees it.
type outlineStep struct {
	index int

	id        string
	taskName  string
	inputKeys []string

	// startLine is the line of the dash that opens the step and endLine the last
	// line belonging to it, both 0-based.
	startLine int
	endLine   int

	// idLine is the line the step's id is declared on, or -1.
	idLine int

	// indent is the column of the dash that opens the step, which is deeper for a
	// step inside a for_each body or a parallel branch.
	indent int
}

// contains reports whether a line belongs to the step.
func (s *outlineStep) containsLine(line0 int) bool {
	return line0 >= s.startLine && line0 <= s.endLine
}

// scanOutline reads the steps of a Flowfile, tolerating text that does not parse.
//
// Steps nest: a for_each body and each parallel branch declare their own `steps:`.
// Every one of them is treated the same way, so completion works at any depth — a
// step inside a loop body is where an author is just as likely to want a suggestion
// as one at the top level.
func scanOutline(ix *lineIndex) []*outlineStep {
	// A step entry is a dash at the indentation a `steps:` key opens. Any other
	// dash belongs to some other list nested under an input.
	entryIndents := map[int]bool{}
	for l := range ix.lineCount() {
		m := keyLine.FindStringSubmatch(ix.line(l))
		if m == nil || m[3] != "steps" {
			continue
		}
		stepsIndent := len(m[1]) + len(m[2])
		// The first dash deeper than the key sets the indentation for that block.
		for next := l + 1; next < ix.lineCount(); next++ {
			line := ix.line(next)
			if strings.TrimSpace(line) == "" || strings.HasPrefix(strings.TrimSpace(line), "#") {
				continue
			}
			if d := dashLine.FindStringSubmatch(line); d != nil && len(d[1]) > stepsIndent {
				entryIndents[len(d[1])] = true
			}
			break
		}
	}
	if len(entryIndents) == 0 {
		return nil
	}

	var steps []*outlineStep
	for l := range ix.lineCount() {
		m := dashLine.FindStringSubmatch(ix.line(l))
		if m == nil || !entryIndents[len(m[1])] {
			continue
		}
		steps = append(steps, &outlineStep{
			index:     len(steps),
			startLine: l,
			idLine:    -1,
			indent:    len(m[1]),
		})
	}

	// A step ends where the next one begins, at any depth, which keeps the ranges
	// disjoint so that a line inside a loop body belongs to the body's step rather
	// than to the loop.
	for i, s := range steps {
		s.endLine = ix.lineCount() - 1
		if i+1 < len(steps) {
			s.endLine = steps[i+1].startLine - 1
		}
		fillStep(ix, s, s.indent)
	}
	return steps
}

// fillStep reads one step's id, task name, and input keys.
func fillStep(ix *lineIndex, s *outlineStep, entryIndent int) {
	// A step's own keys sit at the column after the dash, wherever the writer put
	// it: `- id: a` and `-\n  id: a` are both legal.
	contentIndent := -1
	inTask := false
	inInputs, inputsIndent := false, -1

	// The column the input mapping's own keys sit at, learned from the first one.
	inputKeyIndent := -1

	for l := s.startLine; l <= s.endLine; l++ {
		line := ix.line(l)
		if strings.TrimSpace(line) == "" || strings.HasPrefix(strings.TrimSpace(line), "#") {
			continue
		}

		// A nested list item, such as a lib on its own line.
		if m := dashLine.FindStringSubmatch(line); m != nil && len(m[1]) > entryIndent {
			continue
		}

		m := keyLine.FindStringSubmatch(line)
		if m == nil {
			continue
		}
		indent := len(m[1])
		if m[2] != "" {
			// `- id: a`: the key's own column is past the dash.
			indent += len(m[2])
		}
		key, rest := m[3], strings.TrimSpace(m[4])

		if contentIndent < 0 {
			contentIndent = indent
		}
		if indent <= contentIndent {
			inTask, inInputs = false, false
		}
		if inInputs && indent <= inputsIndent {
			inInputs = false
		}

		switch {
		case indent == contentIndent && key == "id":
			if s.id == "" {
				s.id, s.idLine = unquote(rest), l
			}
		case indent == contentIndent && isRegisteredTask(key):
			// The key is the task, and everything under it is an input.
			//
			// A registered name only — see isRegisteredTask for why this asks a
			// narrower question than the parsed model does, and what it costs.
			if s.taskName == "" {
				s.taskName = key
			}
			inTask = true
			inInputs, inputsIndent = true, indent
		case inTask && inInputs && indent > inputsIndent:
			// Only the input mapping's own keys are inputs. A key at a deeper
			// column belongs to a nested value — an http task's headers — and is
			// not an input name.
			if inputKeyIndent < 0 {
				inputKeyIndent = indent
			}
			if indent == inputKeyIndent {
				s.inputKeys = append(s.inputKeys, key)
			}
		}
	}
}

// flowListItems returns the elements of a flow-style list, `[a, b]`, ignoring an
// unterminated one so a list being typed still yields what has been written.
func flowListItems(s string) []string {
	s = strings.TrimSpace(s)
	if !strings.HasPrefix(s, "[") {
		return nil
	}
	s = strings.TrimSuffix(strings.TrimPrefix(s, "["), "]")
	var out []string
	for _, part := range strings.Split(s, ",") {
		if v := unquote(strings.TrimSpace(part)); v != "" {
			out = append(out, v)
		}
	}
	return out
}

// unquote removes the surrounding quotes of a YAML scalar, if any.
func unquote(s string) string {
	s = strings.TrimSpace(s)
	if len(s) >= 2 && (s[0] == '"' || s[0] == '\'') && s[len(s)-1] == s[0] {
		return s[1 : len(s)-1]
	}
	// Strip a trailing comment from a plain scalar.
	if i := strings.Index(s, " #"); i >= 0 {
		s = strings.TrimSpace(s[:i])
	}
	return s
}

// isRegisteredTask reports whether a key names a task the registry has.
//
// Deliberately a narrower question than flowfile.StepTaskKeys, which the parsed
// model and the compiler ask — and the difference is not a shortcut. Promoting an
// *unregistered* key to a task requires knowing the step has no other kind, and
// this scanner exists precisely for the document where that is unknowable: it
// reads one line at a time, forwards, over a step the author is still typing. A
// key three lines below the cursor has not been written yet.
//
// What that costs is bounded, because every consumer of the scanner's taskName
// needs a registered TaskDef to say anything — completion offers a task's inputs
// from its descriptors, and there are none for a task nobody registered. So the
// scanner declining to name it produces the same empty answer by a shorter route.
// A consumer that ever wants the broader rule needs the parsed model instead, not
// a wider guess here.
func isRegisteredTask(key string) bool {
	_, known := v1.LookupTask(key)
	return known
}

// keyPath returns the chain of YAML keys enclosing a line, outermost first.
//
// It walks backwards over lines with strictly smaller indentation, which is how
// an editor's own YAML support locates context and needs no valid parse.
func keyPath(ix *lineIndex, line0 int) []string {
	depth := indentOf(ix.line(line0))
	var reversed []string
	for l := line0 - 1; l >= 0; l-- {
		line := ix.line(l)
		trimmed := strings.TrimSpace(line)
		if trimmed == "" || strings.HasPrefix(trimmed, "#") {
			continue
		}
		indent := indentOf(line)
		if indent >= depth {
			continue
		}
		if m := keyLine.FindStringSubmatch(line); m != nil {
			reversed = append(reversed, m[3])
		}
		depth = indent
		if depth == 0 {
			break
		}
	}
	path := make([]string, 0, len(reversed))
	for i := len(reversed) - 1; i >= 0; i-- {
		path = append(path, reversed[i])
	}
	return path
}

// indentOf returns the column where a line's content begins.
//
// A leading sequence dash counts as indentation, so that a step's `- id:` and the
// `echo:` beneath it are seen at the same depth — which is what makes the
// backwards walk in keyPath skip the dash line instead of treating `id` as a
// parent key.
func indentOf(line string) int {
	i := 0
	for i < len(line) && (line[i] == ' ' || line[i] == '\t') {
		i++
	}
	if i < len(line) && line[i] == '-' {
		j := i + 1
		for j < len(line) && (line[j] == ' ' || line[j] == '\t') {
			j++
		}
		if j > i+1 || j == len(line) {
			return j
		}
	}
	return i
}

// endsWith reports whether path ends with the given keys, which is how each
// completion context is recognized.
func endsWith(path []string, keys ...string) bool {
	if len(path) < len(keys) {
		return false
	}
	return slices.Equal(path[len(path)-len(keys):], keys)
}
