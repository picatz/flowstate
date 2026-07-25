package lsp

import (
	v1 "github.com/picatz/flowstate/pkg/flowstate/v1"
	"github.com/sourcegraph/go-lsp"
)

// A Flowfile is a list of steps, so its outline is the list of steps, and
// go-to-definition means following a ${step.output} reference back to the step
// that produces it. Both fall out of the positional model for free, and both are
// what makes an editor feel like it understands a language rather than merely
// checking it.

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

// definitionAt resolves a ${step.output} reference to the step's id declaration.
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
			ident, _, _ := referenceAt(v.expr, cursor)

			// A loop iterator resolves to the loop that binds it, which is the
			// only declaration there is to jump to.
			for _, loop := range from.iteratorsInScope() {
				if loop.iteratorName() == ident && loop.idEntry != nil {
					locations = []lsp.Location{{URI: doc.uri, Range: loop.idEntry.valueRange()}}
					return
				}
			}

			target := doc.parsed.step(ident)
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
