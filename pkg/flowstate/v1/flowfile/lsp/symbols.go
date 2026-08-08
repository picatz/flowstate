package lsp

import (
	"io"
	"net/url"
	"os"
	"path/filepath"
	"strings"

	"github.com/sourcegraph/go-lsp"

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
	out := []lsp.SymbolInformation{}
	if doc.parsed == nil {
		return out
	}
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

// definitionAt resolves a ${steps.<id>.<output>} reference to the step's id
// declaration, and a `call:` target to the file it names.
//
// Only a reference to an earlier step resolves. A forward reference is a mistake
// the diagnostics already report, and jumping to it would suggest it works.
func definitionAt(doc *document, pos lsp.Position) []lsp.Location {
	if doc.parsed == nil {
		return nil
	}
	from := doc.parsed.stepAt(pos)
	if from == nil {
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
		walkValues(in.value, func(v *value) {
			if locations != nil || !v.fenced || !contains(v.exprRange, pos) {
				return
			}
			cursor := doc.index.offsetOfPosition(pos) - v.exprOffset
			ref := referenceAt(v.expr, cursor)

			if ref.step == "" {
				// A bare name is a binding, and the only binding with a
				// declaration to jump to is a loop's iterator: the loop that
				// binds it. `now` has no declaration in the file, and a bare name
				// that used to be a step reference is not one now — the
				// diagnostic on it names the migration, and jumping to the step
				// anyway would say the spelling still works.
				for _, loop := range from.iteratorsInScope() {
					if loop.iteratorName() == ref.local && loop.idEntry != nil {
						locations = []lsp.Location{{URI: doc.uri, Range: loop.idEntry.valueRange()}}
						return
					}
				}
				return
			}

			target := doc.parsed.step(ref.step)
			if !visibleFrom(target, from) || target.idEntry == nil {
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

	target := from.callEntry.valueText()
	if target == "" {
		return nil
	}

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

	info, err := os.Stat(located.Path)
	if err != nil || !info.Mode().IsRegular() {
		return nil
	}

	return []lsp.Location{{URI: fileURI(located.Path), Range: calleeRange(located.Path)}}
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
// The read is bounded by [maxDocumentBytes], the same bound an open document
// gets. Nothing about being on the other end of a `call:` makes a file smaller,
// and a definition request must not turn into an unbounded read of whatever the
// path happens to name.
func calleeRange(path string) lsp.Range {
	f, err := os.Open(path)
	if err != nil {
		return documentStart
	}
	defer f.Close()

	// One byte past the bound, so that a file at exactly the limit is read whole
	// and one above it is recognizable as over rather than silently truncated
	// into a document that parses as something its author did not write.
	data, err := io.ReadAll(io.LimitReader(f, maxDocumentBytes+1))
	if err != nil || len(data) > maxDocumentBytes {
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
