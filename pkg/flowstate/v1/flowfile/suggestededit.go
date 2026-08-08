package flowfile

import (
	v1 "github.com/picatz/flowstate/pkg/flowstate/v1"
)

// A diagnostic says what is wrong and where. A suggested edit says what to write
// instead, in a form a program can apply without reading the sentence, and this
// is the one place a span and a replacement become one.
//
// One place on purpose. The hazard here is the one `flow fix` has twice paid
// for: a rewriter that knows less about the source than the grammar does writes
// bytes an author never asked for. Every edit this package offers is therefore a
// replacement of a region a checker had in its hand at the moment it reported the
// problem, converted here and nowhere else, so the conversion from Span to
// [v1.SourceRange] cannot be got right in one checker and wrong in the next.
//
// Nothing in this file decides *whether* an edit is safe. That judgement belongs
// to the checker, which is the only thing that knows what the region it is
// holding means. See [renameKeyEdit] for the shape a caller's guards take.

// replaceSpan renders a replacement of the source a span covers as the schema's
// suggested edit, or nil when the span does not name a region to replace.
//
// Nil rather than a zero-valued edit, because an edit whose range is unknown
// would be applied at the top of the file by any consumer that trusted it, and
// this field's whole contract is that a consumer may trust it.
func replaceSpan(title string, span Span, newText string) *v1.SuggestedEdit {
	if !span.IsValid() || !span.End.IsValid() {
		return nil
	}

	return &v1.SuggestedEdit{
		Title: title,
		Changes: []*v1.TextChange{{
			Range: &v1.SourceRange{
				StartLine:   uint32(max(span.Start.Line, 0)),
				StartColumn: uint32(max(span.Start.Column, 0)),
				EndLine:     uint32(max(span.End.Line, 0)),
				EndColumn:   uint32(max(span.End.Column, 0)),
			},
			NewText: newText,
		}},
	}
}
