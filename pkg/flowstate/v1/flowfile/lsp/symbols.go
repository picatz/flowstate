package lsp

import (
	v1 "github.com/picatz/flowstate/pkg/flowstate/v1"
	"github.com/sourcegraph/go-lsp"
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
// writes, in a column an editor truncates. A sentence would push "echo in loop"
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
			if _, known := v1.LookupTask(s.taskName); !known && s.taskName != "" {
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
// declaration.
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
