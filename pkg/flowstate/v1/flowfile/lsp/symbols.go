package lsp

import (
	"net/url"
	"os"
	"path/filepath"
	"slices"
	"strings"

	"github.com/sourcegraph/go-lsp"

	v1 "github.com/picatz/flowstate/pkg/flowstate/v1"
	"github.com/picatz/flowstate/pkg/flowstate/v1/flowfile"
)

// A Flowfile is a list of steps, so its outline is the list of steps, and
// go-to-definition means following a ${steps.<id>.<output>} reference back to the
// step that produces it. Both fall out of the positional model for free, and both
// are what makes an editor feel like it understands a language rather than merely
// checking it.

// A step's `description:` is deliberately not in the outline, which is worth
// writing down because it is the obvious thing to put there.
//
// A SymbolInformation has two fields a reader sees — Name and ContainerName — and
// no third one to grow into: the protocol's detail field belongs to
// DocumentSymbol, a response shape this server does not implement and the LSP
// types in use here cannot express. So surfacing prose means spending one of the
// two, and both are already spent on facts with nowhere else to appear. Name is
// the step's id, which is what a symbol picker filters on and what a reference in
// another step spells. ContainerName carries what kind of work the step does and,
// for a nested step, which block it is inside — the only place a flat outline can
// say a step is inside a loop body.
//
// Prose is also unlike anything else in a row here: unbounded text the author
// writes, in a column an editor truncates. A sentence would push "log in loop"
// out of view in order to show a fragment of itself. Hover on the step's id shows
// it whole instead (see stepDoc), which is where a reader asks what a step is for
// and where there is room to answer.

// documentSymbols returns the outline of a Flowfile: one symbol per step, named by
// its id and attributed to the task it runs.
func documentSymbols(doc *document) []lsp.SymbolInformation {
	if doc.kind == docTestFile {
		// The test language's own outline — see testsymbols.go. A
		// testdefaults.yaml falls through to the empty answer below: it
		// declares no `tests:` of its own to name a case from.
		return testDocumentSymbols(doc)
	}
	if !doc.speaksFlowfile() {
		return []lsp.SymbolInformation{} // see [document.speaksFlowfile]
	}
	out := []lsp.SymbolInformation{}
	if doc.parsed == nil {
		return out
	}
	// What the file declares comes before what it does, as it is written. A module
	// has nothing else to show, so without these its outline would be empty.
	out = append(out, declarationSymbols(doc)...)
	for _, s := range doc.parsed.steps {
		name := s.id
		if name == "" {
			// A step with no id still belongs in the outline; without a name an
			// editor would show a blank row.
			name = "(step with no id)"
		}
		// The container reads as the outline's second column: what the step does.
		container := s.kind()
		if s.taskEntry != nil {
			container = s.taskName
			if _, known := doc.tasks.Lookup(s.taskName); !known && s.taskName != "" {
				container = s.taskName + " (unknown task)"
			}
		}
		if s.parent != nil && s.parent.id != "" {
			// Nesting is otherwise invisible in a flat outline, and a step inside
			// a loop body behaves differently from one at the top level.
			container += " in " + s.parent.id
		}
		out = append(out, lsp.SymbolInformation{
			Name:          name,
			Kind:          lsp.SKFunction,
			ContainerName: container,
			Location:      lsp.Location{URI: doc.uri, Range: s.rng},
		})
	}
	return out
}

// declarationBlocks are the top-level keys whose entries are named
// declarations, with the symbol kind and container each is listed under.
var declarationBlocks = []struct {
	key       string
	kind      lsp.SymbolKind
	container string
}{
	{"types", lsp.SKStruct, "type"},
	{"errors", lsp.SKEvent, "error"},
	{"functions", lsp.SKFunction, "function"},
}

// declarationSymbols lists the types, errors and functions a file declares, one
// symbol per name, in the order the file writes them.
func declarationSymbols(doc *document) []lsp.SymbolInformation {
	var out []lsp.SymbolInformation
	for _, e := range doc.parsed.entries {
		for _, block := range declarationBlocks {
			if e.key != block.key {
				continue
			}
			for _, d := range nestedEntries(e) {
				out = append(out, lsp.SymbolInformation{
					Name:          d.key,
					Kind:          block.kind,
					ContainerName: block.container,
					Location:      lsp.Location{URI: doc.uri, Range: d.keyRange},
				})
			}
		}
	}
	return out
}

// definitionAt resolves a ${steps.<id>.<output>} reference to the step's id
// declaration, and a `call:` target to the file it names.
//
// Only a reference to an earlier step resolves. A forward reference is a mistake
// the diagnostics already report, and jumping to it would suggest it works.
func definitionAt(doc *document, pos lsp.Position) []lsp.Location {
	if !doc.speaksFlowfile() {
		// A test document's one definition is a `workflow:` naming a sibling
		// Flowfile, answered by [testDefinition] through the same resolver
		// [callDefinition] uses; nothing else in that language has a target.
		return testDefinition(doc, pos)
	}
	pos = clampPosition(pos) // see [clampPosition]

	if doc.parsed == nil {
		return nil
	}
	if locations := useDefinition(doc, pos); locations != nil {
		return locations
	}
	from := doc.parsed.stepAt(pos)
	if from == nil {
		// Outside every step, the one place that reads a step is the workflow's
		// own `outputs:`, which sees the steps that merge into the top-level
		// namespace.
		if target := stepReferencedAt(doc, pos); target != nil && target.idEntry != nil {
			return []lsp.Location{{URI: doc.uri, Range: target.idEntry.valueRange()}}
		}
		return nil
	}

	// A call's target is not an expression, so it is answered before the walk
	// below rather than inside it. It is also the only definition in this
	// language that is in another file — every other one is a position in the
	// document the cursor is already in.
	if locations := callDefinition(doc, from, pos); locations != nil {
		return locations
	}

	var locations []lsp.Location
	for _, in := range from.expressionEntries() {
		// Which of a loop's own scopes the entry's expression is evaluated in,
		// loopScopeNone for every other entry — it decides both lookups below.
		ls := from.loopScopeOf(in)
		walkValues(in.value, func(v *value) {
			// Only a fence that can place the cursor inside its own source has
			// somewhere to jump from. A jump computed from a folded offset would
			// land on whatever name happens to sit at that byte, which is a worse
			// answer than none: go-to-definition is trusted to be right when it
			// moves at all.
			if locations != nil {
				return
			}
			f, cursor, ok := v.fenceAt(doc.index, pos)
			if !ok {
				return
			}
			ref := referenceAt(f.source, cursor)

			if ref.step == "" {
				// A bare name is a binding, and the only binding with a
				// declaration to jump to is a loop's iterator: the loop that
				// binds it. `now` has no declaration in the file, and a bare name
				// that used to be a step reference is not one now — the
				// diagnostic on it names the migration, and jumping to the step
				// anyway would say the spelling still works.
				//
				// A loop's own `until:`/`update:` read the carried state, whose
				// binding loop is the step the cursor is already in — and only
				// there: in `init:` the state does not exist yet, so the name
				// declines here and stays a non-answer.
				if ls == loopScopeAfterBody && from.loopEntry != nil && ref.local != "" &&
					ref.local == from.iteratorName() && from.idEntry != nil {
					locations = []lsp.Location{{URI: doc.uri, Range: from.idEntry.valueRange()}}
					return
				}
				for _, loop := range from.iteratorsInScope() {
					if loop.iteratorName() == ref.local && loop.idEntry != nil {
						locations = []lsp.Location{{URI: doc.uri, Range: loop.idEntry.valueRange()}}
						return
					}
				}
				if !freeNameAt(f.source, ref.span[0]) {
					return
				}
				if at := declaredNameAt(doc, from, ref, ownVars(from, in)); at != nil {
					locations = []lsp.Location{{URI: doc.uri, Range: at.keyRange}}
				}
				return
			}

			// Among the steps in scope at this position, not the first id match in
			// the document: sibling blocks may each declare a body step of the
			// same id, and jumping to the wrong block's — or, once visibility
			// rejected it, nowhere at all — is what #323 was.
			target := doc.parsed.stepVisibleFrom(ref.step, from, ls)
			if target == nil || target.idEntry == nil {
				return
			}
			locations = []lsp.Location{{URI: doc.uri, Range: target.idEntry.valueRange()}}
		})
		if locations != nil {
			return locations
		}
	}
	return nil
}

// declaredNameAt is the `vars:` or `inputs:` key a bare reference names, with the
// step's own `vars:` counted only when ownVars says they are bound at the cursor.
//
// Three spellings reach a declaration that is not a step or an iterator:
//
//   - `vars.<name>` is the workflow's own `vars:` block, and `inputs.<name>` is
//     its `inputs:` block. Both roots resolve whether or not anything is declared,
//     so a name with no key is answered with nothing rather than guessed at.
//   - A bare `<name>` is a var the step itself declares, or a block around it
//     declares: the order the engine resolves them in, and the order hover reads
//     them in, so the two surfaces name the same declaration.
//
// The key is the answer, not the value: it is the word the author renames and the
// one every other reference spells.
func declaredNameAt(doc *document, from *parsedStep, ref reference, ownVars bool) *entry {
	switch ref.local {
	case v1.VarsRoot:
		return varEntry(doc.parsed.varsEntry, ref.member)
	case v1.InputsRoot:
		for _, e := range doc.parsed.entries {
			if e.key == "inputs" {
				return varEntry(e, ref.member)
			}
		}
		return nil
	}
	blocks := blocksAround(from)
	if ownVars {
		blocks = append([]*parsedStep{from}, blocks...)
	}
	for _, block := range blocks {
		if e := varEntry(block.varsEntry, ref.local); e != nil {
			return e
		}
	}
	return nil
}

// ownVars reports whether the step's own `vars:` are bound where the expression
// in entry is evaluated. They are bound for the step's run, not for its `if:`
// (decided before the vars exist) or for the vars' own values (evaluated against
// the outer scope, so one var cannot read another), and the validator refuses a
// reference written there; pointing at the var would claim it works.
func ownVars(from *parsedStep, in *entry) bool {
	if in == from.conditionEntry {
		return false
	}
	return !slices.Contains(nestedEntries(from.varsEntry), in)
}

// callDefinition resolves a `call:` step's target — when the cursor is on it —
// to the Flowfile it names.
//
// A call is the one place a Flowfile names another file, so it is the one place
// this language has a definition that is not a position in the document already
// open. Three things decide whether there is an answer, and each of them can only
// take one away:
//
//   - Where the path is resolved from. A call is relative to the calling *file's*
//     own directory — not the editor's working directory, not a workspace root —
//     and the rule is asked of [flowfile.ResolveCallTarget], the same function the
//     compiler asks. A second path rule here is how an editor comes to navigate
//     to a file the run does not compile.
//   - Whether the caller has a location at all. An untitled buffer has no
//     directory for a relative path to mean anything against, which is the same
//     answer [document.filesystemPath] gives the diagnostics.
//   - Whether the file is there. A [lsp.Location] naming a path that does not
//     exist opens an editor on nothing, or worse, on an empty buffer it offers to
//     create — a wrong answer where silence is a correct one. Whether a missing
//     callee is *reported* belongs to the validator and is not touched here.
//
// The stat, and the bounded read below it, are I/O on an explicit
// go-to-definition request rather than on the keystroke path — which is the
// distinction that keeps this on the right side of the rule that keeps DNS out of
// a validator.
func callDefinition(doc *document, from *parsedStep, pos lsp.Position) []lsp.Location {
	if from.callEntry == nil || !contains(from.callEntry.valueRange(), pos) {
		return nil
	}

	target, err := flowfile.LiteralText(from.callEntry.valueText())
	if target == "" || err != nil {
		// The compiler reads a call target as literal text and refuses any
		// expression or interpolation. Navigating to a literal filename matching
		// rejected source would claim that an invalid call works.
		return nil
	}

	return siblingFlowfile(doc, target)
}

// useDefinition resolves the `path:` of a `use:` entry, when the cursor is on it, to
// the module it names. A `use:` is the second place this language names another
// file, and it is resolved by the same [flowfile.ResolveCallTarget] a call is, so
// the editor and the compiler cannot disagree about which file a path means.
// Navigating to a declaration inside the module is a later slice; this lands on the
// file.
func useDefinition(doc *document, pos lsp.Position) []lsp.Location {
	for _, e := range doc.parsed.entries {
		if e.key != "use" {
			continue
		}
		for _, alias := range nestedEntries(e) {
			for _, field := range nestedEntries(alias) {
				if field.key != "path" || !contains(field.valueRange(), pos) {
					continue
				}
				target, err := flowfile.LiteralText(field.valueText())
				if target == "" || err != nil {
					return nil
				}

				return siblingFlowfile(doc, target)
			}
		}
	}

	return nil
}

// siblingFlowfile resolves a `call:` target the way the compiler does — the
// caller's location, [flowfile.ResolveCallTarget] — and hands the path to
// [flowfileLocation]. It returns nil, never a wrong location, when any step says
// no.
func siblingFlowfile(doc *document, target string) []lsp.Location {
	callerPath, ok := doc.filesystemPath()
	if !ok {
		return nil
	}

	located := flowfile.ResolveCallTarget(callerPath, target)
	if located.Refusal != flowfile.CallTargetResolved {
		// A path the compiler refuses to read — absolute, or climbing out of the
		// calling file's directory. The diagnostic already says so; navigating
		// there anyway would say the call works.
		return nil
	}
	return flowfileLocation(located.Path)
}

// flowfileLocation is where go-to-definition arrives for a Flowfile at path: its
// `name:`, provided the path is a regular file. Anything else is nil, because a
// [lsp.Location] naming a path that is not there opens an editor on nothing.
func flowfileLocation(path string) []lsp.Location {
	info, err := os.Stat(path)
	if err != nil || !info.Mode().IsRegular() {
		return nil
	}
	return []lsp.Location{{URI: fileURI(path), Range: calleeRange(path)}}
}

// testDefinition resolves the `workflow:` value of a test case, or of a
// `defaults:` stanza (in a suite or a testdefaults.yaml), to the Flowfile it
// names, when the cursor is on the value. The cursor on the key, on a comment,
// on any other key, or on a `workflow:` that is not a literal path yields nil.
//
// The path means what it means to `flow test`: absolute as written, otherwise
// joined onto the test file's directory, with no containment rule — the runner
// accepts an absolute or parent-relative workflow, so refusing one here would
// leave a suite that runs without a jump to the file it runs. That is the one
// way this differs from `call:`, whose containment is the compiler's.
func testDefinition(doc *document, pos lsp.Position) []lsp.Location {
	if doc.tooLarge {
		return nil
	}
	pos = clampPosition(pos)

	line := doc.index.line(pos.Line)
	key, _, rng, ok := keyValueOnLine(line, pos.Line)
	if !ok || key != "workflow" {
		return nil
	}
	// keyValueOnLine's range runs to the end of the line, comment and all; the
	// value is only what the YAML scalar decoder accepts, up to its own end.
	start := doc.index.byteOfUTF16(pos.Line, rng.Start.Character)
	span := scalarSpan(line[start:])
	if span == 0 {
		return nil
	}
	end := lsp.Position{Line: pos.Line, Character: utf16Len(line[:start+span])}
	if !contains(lsp.Range{Start: rng.Start, End: end}, pos) {
		return nil
	}
	literal, err := flowfile.LiteralText(scalarText(line[start : start+span]))
	if literal == "" || err != nil {
		// Same reading of the path callDefinition gives a `call:` target.
		return nil
	}

	level, structural := testDocLevelAt(doc.kind, keyPath(doc.index, pos.Line))
	if !structural || (level != testLevelCase && level != testLevelDefaults) {
		return nil
	}
	testPath, ok := doc.filesystemPath()
	if !ok {
		return nil
	}
	if !filepath.IsAbs(literal) {
		literal = filepath.Join(filepath.Dir(testPath), literal)
	}
	return flowfileLocation(literal)
}

// scalarSpan is how many bytes of rest are the YAML scalar it begins with: a
// quoted scalar through its closing quote, a plain one up to a trailing comment.
// Zero when rest is not a scalar the decoder accepts, which includes an unclosed
// quote — go-to-definition answers on a valid value or not at all.
func scalarSpan(rest string) int {
	if scalarText(rest) == "" {
		return 0
	}
	switch rest[0] {
	case '"':
		for i := 1; i < len(rest); i++ {
			switch rest[i] {
			case '\\':
				i++
			case '"':
				return i + 1
			}
		}
		return 0
	case '\'':
		for i := 1; i < len(rest); i++ {
			if rest[i] != '\'' {
				continue
			}
			if i+1 < len(rest) && rest[i+1] == '\'' {
				i++
				continue
			}
			return i + 1
		}
		return 0
	}
	if i := strings.Index(rest, " #"); i >= 0 {
		rest = rest[:i]
	}
	return len(strings.TrimRight(rest, " \t"))
}

// calleeRange is where in the called file to put the cursor: its `name:`, or the
// start of the file when there is not one to find.
//
// A callee's name is what the author is going to the file to see, and landing on
// it rather than on line one is the difference between arriving in a file and
// arriving at the thing that was named. It is best effort by construction — a
// callee that does not parse, or is too large to be worth reading, still has a
// first line, and arriving there is a better answer than not arriving.
//
// The read is [readCalleeSource], bounded by [maxDocumentBytes], the same bound
// an open document gets. Nothing about being on the other end of a `call:` makes
// a file smaller, and a definition request must not turn into an unbounded read
// of whatever the path happens to name.
func calleeRange(path string) lsp.Range {
	data, ok := readCalleeSource(path)
	if !ok {
		return documentStart
	}

	text := string(data)
	parsed, err := parseFlowfile(text, newLineIndex(text))
	if err != nil || parsed == nil || parsed.nameEntry == nil {
		return documentStart
	}
	return parsed.nameEntry.valueRange()
}

// fileURI renders a filesystem path as the `file://` URI an editor is handed
// back.
//
// Built through [url.URL] rather than by concatenation, for the reason
// [document.filesystemPath] parses one rather than trimming a prefix: a path
// holding a space, a `#`, or anything non-ASCII has to arrive percent-encoded or
// the client resolves a different path than the one meant — and this is the
// direction that produces the encoding the other direction is careful to undo.
func fileURI(path string) lsp.DocumentURI {
	slashed := filepath.ToSlash(path)

	// A Windows path begins with its drive letter, and a URI path must begin
	// with a separator; `C:/x` becomes `/C:/x`, which is the spelling
	// filesystemPath reads back.
	if !strings.HasPrefix(slashed, "/") {
		slashed = "/" + slashed
	}

	u := url.URL{Scheme: "file", Path: slashed}
	return lsp.DocumentURI(u.String())
}
