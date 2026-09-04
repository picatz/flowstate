package flowtest

import (
	"strconv"
	"strings"

	v1 "github.com/picatz/flowstate/pkg/flowstate/v1"
)

// caseAnchor places a case's run-time failures in the file that claimed them.
//
// A failure found while running says which key it contradicts — `expect.outputs`,
// `expect.ran` — and often which entry of it, in [v1.Diagnostic.Value]. The
// loader already computed where every key of this file is, so nothing here
// re-derives a position: it walks that same document ([document.positionOf]),
// which is what keeps a failure and a load-time problem agreeing about where
// `expect.outputs` starts rather than disagreeing by a line.
//
// Zero for a [File] that was built in Go rather than parsed, and for a key an
// author never wrote — an `expect.failed` a case left unset is still reported
// when the run fails, and there is no `expect.failed:` line to underline. That
// is [document.positionOf]'s exact-or-nothing rule, kept deliberately: this
// package would rather hand an editor no position than send an author to
// correct a line that is already right.
type caseAnchor struct {
	doc *document

	// at is the case's own path, `tests[i]`, so a failure's field extends it
	// rather than restating it.
	at loc
}

// anchorFor is the anchor for one case of a parsed file, or the zero anchor when
// there is no document to place anything in.
func anchorFor(file *File, index int) caseAnchor {
	if file == nil || file.doc == nil {
		return caseAnchor{}
	}

	return caseAnchor{doc: file.doc, at: at("tests").item(index)}
}

// place fills in the position and code of every failure a case produced.
//
// Called once where a case's failures are complete rather than at each of the
// fifteen places one is built: what a failure is about is already written on it,
// so placing them here keeps the rule in one place and means a new failure class
// is located by saying what it is about rather than by remembering to look a
// position up.
func (a caseAnchor) place(failures []*v1.Diagnostic) {
	for _, failure := range failures {
		if failure == nil {
			continue
		}

		if failure.GetCode() == "" {
			failure.Code = string(codeFor(failure.GetField()))
		}

		if a.doc == nil || failure.GetLine() != 0 {
			continue
		}
		if position, known := a.locate(failure); known {
			failure.Line, failure.Column = uint32(position.line), uint32(position.column)
		}
	}
}

// locate finds the most specific key this failure is about that the file
// actually wrote.
//
// Most specific first, and each step is exact: an output the case named is found
// at `expect.outputs.<name>` and underlines that entry, while an output the run
// produced that the case did *not* name has no entry of its own and falls back to
// the `expect.outputs:` key it should have been added to. The document decides
// which of those a failure is, so the two classes are told apart by what the
// author wrote rather than by this package parsing its own messages.
func (a caseAnchor) locate(failure *v1.Diagnostic) (position, bool) {
	field := failure.GetField()
	if field == "" {
		return position{}, false
	}

	path := a.at
	for _, step := range strings.Split(field, ".") {
		path = extend(path, step)
	}

	// The entry, when the failure names one and the author wrote it.
	if value := failure.GetValue(); value != "" {
		if position, known := a.doc.positionOf(path.field(value)); known {
			return position, true
		}
	}

	// Otherwise the key the claim is written under. positionOfKey rather than
	// positionOf: the subject is the name the author wrote, and underlining the
	// whole mapping that follows it would cover a screen of correct lines.
	if position, known := a.doc.positionOfKey(path); known {
		return position, true
	}

	// A claim that is not written under a key of its own has only itself to
	// point at — one entry of `expect.check:` is a bare expression in a
	// sequence, so there is no `check[0]:` to underline and the expression is
	// the thing the author would fix.
	if position, known := a.doc.positionOf(path); known {
		return position, true
	}

	return position{}, false
}

// extend adds one step of a failure's field to a path, reading the `name[i]`
// form some of them are written in.
//
// A claim about one entry of a list names it that way — `expect.check[0]`,
// `stubs[1]` — which is the spelling [loc.String] renders and this package's
// prose already uses. Treating it as a key would look for a mapping entry
// literally called "check[0]", find nothing, and quietly place the failure at
// the enclosing key instead of the claim the author wrote.
//
// A malformed index is taken as a plain name rather than an error: this is
// placing a diagnostic, and failing to place one is the documented outcome
// already.
func extend(path loc, step string) loc {
	open := strings.IndexByte(step, '[')
	if open <= 0 || !strings.HasSuffix(step, "]") {
		return path.field(step)
	}

	index, err := strconv.Atoi(step[open+1 : len(step)-1])
	if err != nil || index < 0 {
		return path.field(step)
	}

	return path.field(step[:open]).item(index)
}

// codeFor is the code a failure of this class carries.
//
// The field a failure names is its class — a consumer grouping "an output the
// case did not name" apart from "an output whose value differs" is grouping by
// what the claim was about — so the mapping is from field to code rather than
// from a message this would have to parse.
//
// A field with no entry here is [v1.DiagnosticCodeExpectationUnmet], which is
// what every one of these is: a claim the file made that the run contradicted.
// The named codes exist where a consumer has a reason to treat one differently,
// and adding a class is adding a row rather than a mechanism.
func codeFor(field string) v1.DiagnosticCode {
	switch field {
	case "expect.outputs":
		return v1.DiagnosticCodeOutputMismatch
	case "stubs":
		return v1.DiagnosticCodeStubUnmatched
	default:
		return v1.DiagnosticCodeExpectationUnmet
	}
}
