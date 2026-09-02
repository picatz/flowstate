package lsp

import (
	"regexp"
	"slices"
	"strings"

	"github.com/goccy/go-yaml/ast"
	"github.com/goccy/go-yaml/parser"

	v1 "github.com/picatz/flowstate/pkg/flowstate/v1"
)

// Completion cannot parse the document as YAML. A document being typed is usually
// not valid YAML — a task's key followed by a half-written input is a syntax error
// — and that is exactly the moment the author wants a suggestion. So this file
// reads the document by line, which degrades instead of failing. A complete quoted
// key captured from one line can still use YAML's scalar decoder without making
// the rest of the document a prerequisite.
//
// That trade is right for completion and wrong for diagnostics: a line scan can
// misjudge an unusual document, and a wrong diagnostic is worse than none.
// Diagnostics therefore use the parsed model in parse.go, and the two never share
// a code path.

// scannedKeyLine is the one line-scan representation of a `key: rest` entry.
// key is the semantic name the YAML decoder sees; keyStart, keyEnd, and colon
// remain byte offsets in the source spelling so positions do not shrink when an
// escape does (`"st\u0065ps"` is eleven source bytes but the key is "steps").
type scannedKeyLine struct {
	keyStart int
	keyEnd   int
	colon    int
	key      string
	rest     string
}

// scanKeyLine recognizes the deliberately small YAML mapping-key grammar the
// tolerant line-scan features understand: the existing identifier-shaped bare
// keys, and complete single- or double-quoted scalar keys on one physical line.
// Quoted keys are located with a small scanner instead of a regex so doubled
// single quotes, double-quote backslash escapes, `:` and `#` inside quotes, and
// comments after the value cannot move the key's closing boundary. The captured
// scalar is then decoded through goccy's YAML AST, the same parser and
// *ast.StringNode.Value that the Flowfile loader uses in keyNameOf; invalid YAML
// escapes therefore fail closed rather than becoming almost-the-loader strings.
//
// Explicit (`? key`), tagged, anchored, aliased, multiline, and flow-mapping keys
// remain unsupported here. They were outside the bare-key scanner too, and need
// a real document parse rather than another tolerant guess. Diagnostics and the
// parsed workflow outline continue to use that real parse.
func scanKeyLine(line string) (scannedKeyLine, bool) {
	var out scannedKeyLine
	i := 0
	for i < len(line) && keySpace(line[i]) {
		i++
	}

	if i < len(line) && line[i] == '-' {
		i++
		spaceStart := i
		for i < len(line) && keySpace(line[i]) {
			i++
		}
		if i == spaceStart {
			return scannedKeyLine{}, false
		}
	}

	out.keyStart = i
	if i >= len(line) {
		return scannedKeyLine{}, false
	}

	switch line[i] {
	case '\'', '"':
		quote := line[i]
		i++
		closed := false
		for i < len(line) {
			switch {
			case quote == '\'' && line[i] == '\'' && i+1 < len(line) && line[i+1] == '\'':
				i += 2
			case quote == '"' && line[i] == '\\':
				// Whether the escaped byte is legal YAML is the decoder's
				// decision below; this step only prevents an escaped quote from
				// being mistaken for the closing quote.
				if i+1 >= len(line) {
					return scannedKeyLine{}, false
				}
				i += 2
			case line[i] == quote:
				i++
				closed = true
			default:
				i++
			}
			if closed {
				break
			}
		}
		if !closed {
			return scannedKeyLine{}, false
		}
		out.keyEnd = i
		key, ok := yamlStringScalar(line[out.keyStart:out.keyEnd])
		if !ok {
			return scannedKeyLine{}, false
		}
		out.key = key
	default:
		if !bareKeyStart(line[i]) {
			return scannedKeyLine{}, false
		}
		i++
		for i < len(line) && bareKeyContinue(line[i]) {
			i++
		}
		out.keyEnd = i
		out.key = line[out.keyStart:out.keyEnd]
	}

	for i < len(line) && keySpace(line[i]) {
		i++
	}
	if i >= len(line) || line[i] != ':' {
		return scannedKeyLine{}, false
	}
	out.colon = i
	out.rest = line[i+1:]
	return out, true
}

func keySpace(b byte) bool {
	return b == ' ' || b == '\t' || b == '\r' || b == '\f'
}

func bareKeyStart(b byte) bool {
	return b == '_' || b >= 'A' && b <= 'Z' || b >= 'a' && b <= 'z'
}

func bareKeyContinue(b byte) bool {
	return bareKeyStart(b) || b >= '0' && b <= '9' || b == '.' || b == '-'
}

// yamlStringScalar decodes one complete scalar through the same AST value the
// Flowfile loader reads for mapping keys. The bool distinguishes legal `""`
// from a parse failure.
func yamlStringScalar(src string) (string, bool) {
	f, err := parser.ParseBytes([]byte(src), 0)
	if err != nil || len(f.Docs) != 1 || f.Docs[0].Body == nil {
		return "", false
	}
	scalar, ok := f.Docs[0].Body.(*ast.StringNode)
	if !ok {
		return "", false
	}
	return scalar.Value, true
}

// dashLine matches a block sequence entry.
var dashLine = regexp.MustCompile(`^(\s*)-(\s|$)`)

// nonTaskKindKeys are the step keys that decide a non-task step's kind —
// flowfile/parse.go's nodeKindKeys, minus "call", which the scanner already
// tracks separately for callTarget. Kept in sync with that list by hand: both
// name the same exclusive set of keys a step's `kind` oneof compiles from, so
// a kind added there needs an entry here before completion stops offering
// `timeout:`/`retry:` on it too.
var nonTaskKindKeys = map[string]bool{
	"for_each":         true,
	"loop":             true,
	"parallel":         true,
	"sleep":            true,
	"wait_until":       true,
	"wait_for_signal":  true,
	"wait_for_signals": true,
	"value":            true,
	"switch":           true,
}

// An outlineStep is one step of a Flowfile as the line scan sees it.
type outlineStep struct {
	index int

	id        string
	taskName  string
	inputKeys []string

	// callTarget is the path written beside `call:`, empty for a step that is
	// not a call.
	//
	// Recorded here for the reason taskName is: the keys a step's block takes
	// come from somewhere outside this file, and the scanner is what completion
	// asks while the document is mid-edit. A call's `with:` keys are the
	// callee's declared inputs, so the target is the one thing needed to go and
	// read them.
	callTarget string

	// withKeys are the argument names this step's `with:` block already binds,
	// in source order. Completion leaves them out of the menu the way inputKeys
	// are left out of a task's.
	withKeys []string

	// kindKey is the step's own kind-deciding key — one of nodeKindKeys
	// (flowfile/parse.go), e.g. "for_each" or "sleep" — once the scanner has
	// seen it, empty otherwise. A step's kind is exclusive (the grammar's own
	// invariant, not this scanner's), so the first one seen at the step's own
	// content indent is the whole answer.
	//
	// Recorded for the same reason taskName and callTarget are: once a step
	// has committed to a kind, `timeout:`/`retry:` stop applying to it (see
	// checkPolicyPlacement), and completion offering them anyway is
	// completeAt recommending syntax the diagnostic it just wrote will
	// immediately refuse.
	kindKey string

	// startLine is the line of the dash that opens the step and endLine the last
	// line belonging to it, both 0-based.
	startLine int
	endLine   int

	// idLine is the line the step's id is declared on, or -1.
	idLine int

	// indent is the column of the dash that opens the step, which is deeper for a
	// step inside a for_each body or a parallel branch.
	indent int

	// concurrent is true when the step is inside a for_each body or parallel
	// branch. Those are the two placements that already multiply work and where
	// the shared execution model therefore refuses async task steps.
	concurrent bool
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
func scanOutline(ix *lineIndex, tasks *v1.Registry) []*outlineStep {
	// A step entry is a dash at the indentation a `steps:` key opens. Any other
	// dash belongs to some other list nested under an input.
	entryIndents := map[int]bool{}
	for l := range ix.lineCount() {
		m, ok := scanKeyLine(ix.line(l))
		if !ok || m.key != "steps" {
			continue
		}
		stepsIndent := m.keyStart
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
		dash := dashLine.FindStringSubmatch(ix.line(l))
		if dash == nil || !entryIndents[len(dash[1])] {
			continue
		}
		steps = append(steps, &outlineStep{
			index:     len(steps),
			startLine: l,
			idLine:    -1,
			indent:    len(dash[1]),
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
		fillStep(ix, s, s.indent, tasks)
	}

	// Reconstruct the step ancestry from the entry indentation after fillStep has
	// identified each ancestor's actual kind. This avoids mistaking an ordinary
	// nested mapping key named `parallel` for the parallel step kind.
	var ancestors []*outlineStep
	for _, s := range steps {
		for len(ancestors) > 0 && ancestors[len(ancestors)-1].indent >= s.indent {
			ancestors = ancestors[:len(ancestors)-1]
		}
		for _, ancestor := range ancestors {
			if ancestor.kindKey == "for_each" || ancestor.kindKey == "parallel" {
				s.concurrent = true
				break
			}
		}
		ancestors = append(ancestors, s)
	}
	return steps
}

// fillStep reads one step's id, task name, and input keys.
func fillStep(ix *lineIndex, s *outlineStep, entryIndent int, tasks *v1.Registry) {
	// A step's own keys sit at the column after the dash, wherever the writer put
	// it: `- id: a` and `-\n  id: a` are both legal.
	contentIndent := -1
	inTask := false
	inInputs, inputsIndent := false, -1

	// The column the input mapping's own keys sit at, learned from the first one.
	inputKeyIndent := -1

	// The same two facts for a call's `with:` block, kept apart from the task's
	// because a step is one or the other and reusing the pair would let a
	// half-typed step carry one block's state into the other.
	inWith, withIndent := false, -1
	withKeyIndent := -1

	for l := s.startLine; l <= s.endLine; l++ {
		line := ix.line(l)
		if strings.TrimSpace(line) == "" || strings.HasPrefix(strings.TrimSpace(line), "#") {
			continue
		}

		// A nested list item, such as a lib on its own line.
		if m := dashLine.FindStringSubmatch(line); m != nil && len(m[1]) > entryIndent {
			continue
		}

		m, ok := scanKeyLine(line)
		if !ok {
			continue
		}
		indent := m.keyStart
		key, rest := m.key, strings.TrimSpace(m.rest)

		if contentIndent < 0 {
			contentIndent = indent
		}
		if indent <= contentIndent {
			inTask, inInputs, inWith = false, false, false
		}
		if inInputs && indent <= inputsIndent {
			inInputs = false
		}
		if inWith && indent <= withIndent {
			inWith = false
		}

		switch {
		case indent == contentIndent && key == "id":
			if s.id == "" {
				s.id, s.idLine = unquote(rest), l
			}
		case indent == contentIndent && key == "call":
			if s.callTarget == "" {
				s.callTarget = scalarText(rest)
			}
			if s.kindKey == "" {
				s.kindKey = key
			}
		case indent == contentIndent && nonTaskKindKeys[key]:
			if s.kindKey == "" {
				s.kindKey = key
			}
		case indent == contentIndent && key == "with":
			inWith, withIndent = true, indent
		case inWith && indent > withIndent:
			// Only the argument mapping's own keys are arguments. A key at a
			// deeper column belongs to a value one of them is bound to, and is
			// not an argument name, which is the rule an input key follows below.
			if withKeyIndent < 0 {
				withKeyIndent = indent
			}
			if indent == withKeyIndent {
				s.withKeys = append(s.withKeys, key)
			}
		case indent == contentIndent && isRegisteredTask(tasks, key):
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

// unquote removes the surrounding quotes of a YAML scalar, if any.
// scalarText decodes rest, the text a line scanner found after a key, the way
// YAML reads it rather than the way substring surgery does: quoting unwraps
// with its escapes resolved, an anchor unwraps to the value it names, and a
// trailing comment drops even after a quoted scalar. The distinction matters
// where the answer becomes a filename: [unquote]'s trimming hands
// ResolveCallTarget a path with an anchor or a quote still in it, which then
// names a file that does not exist.
//
// Anything the line does not fully carry answers empty, and empty means "no
// target" downstream, which is the honest reading: a block scalar's content
// lives on the lines below the header, and a flow collection is not a path.
func scalarText(rest string) string {
	if strings.TrimSpace(rest) == "" {
		return ""
	}
	f, err := parser.ParseBytes([]byte(rest), 0)
	if err != nil || len(f.Docs) != 1 || f.Docs[0].Body == nil {
		return ""
	}
	node := f.Docs[0].Body
	if anchor, ok := node.(*ast.AnchorNode); ok {
		node = anchor.Value
	}
	if _, ok := node.(*ast.StringNode); !ok {
		return ""
	}
	tok := node.GetToken()
	if tok == nil {
		return ""
	}
	return tok.Value
}

func unquote(s string) string {
	s = strings.TrimSpace(s)
	// Through [scalarText] — the real decoder, one screen up — rather than any
	// substring surgery of this function's own. Three review rounds each found
	// one more escape class this used to hand-decode (a trailing comment, an
	// escaped quote, then \u-style sequences), and each fix was a step toward
	// re-implementing the YAML scalar decoder beside the function that already
	// calls it. Deriving from the one decoder is what ends the class: every
	// escape the loader resolves, this resolves, because it is the same
	// resolution (Codex, #1173, three rounds).
	if v := scalarText(s); v != "" {
		return v
	}
	// The parser refused the fragment — a mid-edit line, an unclosed quote —
	// so degrade the way the old scan did: comment off a plain scalar, flanking
	// quotes off the rest. Best-effort is honest here; the parseable case never
	// reaches it.
	if i := strings.Index(s, " #"); i >= 0 {
		s = strings.TrimSpace(s[:i])
	}
	return strings.Trim(s, `"'`)
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
func isRegisteredTask(tasks *v1.Registry, key string) bool {
	_, known := tasks.Lookup(key)
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
		if m, ok := scanKeyLine(line); ok {
			reversed = append(reversed, m.key)
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
// `log:` beneath it are seen at the same depth — which is what makes the
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

// stepOwningKeyAt returns the step whose own key list a line at the "steps"
// level of nesting belongs to — the step whose dash opens at exactly the
// same column the cursor's own key would sit at — or nil if none does.
//
// This is deliberately not [stepScope]: that function's `current` is the
// *innermost* step whose line range contains the cursor, which is right for
// a task's own input keys but wrong here. scanOutline ends a step's range
// where the next step begins at any depth (see its own doc), so on a sibling
// line after a for_each/loop/parallel/switch body, stepScope's current is
// the last nested step inside that body, not the composite the cursor is
// actually a sibling of. keyPath's indentation walk gets the *level* right
// (it says "steps", correctly, regardless of nesting) but discards which
// step owns that level, since it skips dash lines entirely.
//
// This function is the missing piece: it matches contentIndent, computed the
// same way [indentOf] treats a dash line, against every step's own opening
// line, and returns the nearest one at or before the cursor. That is exactly
// the step a new key at this column would be written onto.
func stepOwningKeyAt(ix *lineIndex, steps []*outlineStep, line0 int) *outlineStep {
	depth := indentOf(ix.line(line0))

	var owner *outlineStep
	for _, s := range steps {
		if s.startLine > line0 {
			break
		}
		if indentOf(ix.line(s.startLine)) == depth {
			owner = s
		}
	}
	return owner
}

// endsWith reports whether path ends with the given keys, which is how each
// completion context is recognized.
func endsWith(path []string, keys ...string) bool {
	if len(path) < len(keys) {
		return false
	}
	return slices.Equal(path[len(path)-len(keys):], keys)
}
