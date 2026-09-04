package flowtest

import (
	"strconv"
	"strings"

	v1 "github.com/picatz/flowstate/pkg/flowstate/v1"
)

// caseAnchor places a case's run-time findings in the file that claimed them.
//
// A finding says which key it contradicts — `expect.outputs`, `expect.ran` —
// and often which entry of it, in [v1.Diagnostic.Value]. The loader already
// computed where every key of this file is, so nothing here re-derives a
// position: it walks that same document ([document.positionOf]), which is what
// keeps a failure and a load-time problem agreeing about where `expect.outputs`
// starts rather than disagreeing by a line.
//
// Nothing is placed for a [File] built in Go rather than parsed, and nothing for
// a key an author never wrote — an `expect.failed` a case left unset is still
// reported when the run fails, and there is no `expect.failed:` line to
// underline. That is [document.positionOf]'s exact-or-nothing rule, kept
// deliberately: this package would rather hand an editor no position than send
// an author to correct a line that is already right.
type caseAnchor struct {
	doc *document

	// source is where this case was written, and how much of it it wrote for
	// itself. Both matter: the path is not `tests[i]` for a table's row, and an
	// index into a merged list is not an index into the document's.
	source caseSource

	// mergedStubs and mergedChecks are the lengths the case actually ran with,
	// which is what a finding's index counts against.
	mergedStubs  int
	mergedChecks int
}

// anchorFor is the anchor for one case of a parsed file, or the zero anchor when
// there is nothing to place anything in.
func anchorFor(file *File, index int) caseAnchor {
	if file == nil || file.doc == nil || index >= len(file.sources) {
		return caseAnchor{}
	}

	test := &file.Tests[index]

	return caseAnchor{
		doc:          file.doc,
		source:       file.sources[index],
		mergedStubs:  len(test.Stubs),
		mergedChecks: len(test.Expect.Check),
	}
}

// HasPositions reports whether this file kept the parsed YAML that places a
// run-time finding in it.
//
// True for a file loaded from bytes, false for one built in Go, whose findings
// carry no line because there is no text they came from. Exported for the fuzz
// target, which checks that the same bytes answer this the same way twice rather
// than deep-comparing the index itself.
func (f *File) HasPositions() bool { return f != nil && f.doc != nil }

// place fills in the position and code of every finding a case produced.
//
// Called where a case's findings are complete rather than at each of the fifteen
// places one is built: what a finding is about is already written on it, so
// placing them here keeps the rule in one place and means a new class is located
// by saying what it is about rather than by remembering to look a position up.
//
// Warnings are placed by the same call as failures. A case that invoked a task
// no stub answered is something an editor should underline at the `stubs:` block
// exactly as it underlines a false claim at `expect:`; that one is a warning
// rather than a failure says how loudly to report it, not whether it has a
// place in the file.
func (a caseAnchor) place(findings []*v1.Diagnostic) {
	for _, finding := range findings {
		if finding == nil {
			continue
		}

		if finding.GetCode() == "" {
			finding.Code = string(codeFor(finding.GetField()))
		}

		if a.doc == nil || finding.GetLine() != 0 {
			continue
		}
		if position, known := a.locate(finding); known {
			finding.Line, finding.Column = uint32(position.line), uint32(position.column)
		}
	}
}

// locate finds the most specific key this finding is about that the case itself
// wrote.
//
// Most specific first, and every step is exact: an output the case named is
// found at `expect.outputs.<name>` and underlines that entry, while an output
// the run produced that the case did *not* name has no entry of its own and
// falls back to the `expect.outputs:` key it should have been added to. The
// document decides which of those a finding is, so the two classes are told
// apart by what the author wrote rather than by this package parsing its own
// messages.
func (a caseAnchor) locate(finding *v1.Diagnostic) (position, bool) {
	field := finding.GetField()
	if field == "" {
		return position{}, false
	}

	path, ok := a.pathOf(field)
	if !ok {
		return position{}, false
	}

	// The entry, when the finding names one and the author wrote it.
	if value := finding.GetValue(); value != "" {
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

// pathOf turns the field a finding names into a path in this document, or
// reports that the case did not write the thing it addresses.
//
// The two indexed families need translating rather than copying, and they
// translate in opposite directions, so they are named rather than folded into
// one rule that would be wrong for one of them: [mergeDefaults] appends a case's
// own stubs after the inherited ones it prepends for checks. An index outside
// the case's own run addresses something it inherited, which has no position
// here at all — a position on this case would underline a stub or a claim the
// case did not write, which is the false position [document.positionOf] refuses
// to produce by construction and this must not reintroduce by arithmetic.
func (a caseAnchor) pathOf(field string) (loc, bool) {
	switch name, index, indexed := splitIndex(field); {
	case indexed && name == "stubs":
		// A case's own stubs come first in the merged list ([caseSource.stubOrigin]).
		if index >= a.source.ownStubs {
			return nil, false
		}

		return a.source.path.field("stubs").item(index), true

	case indexed && name == "expect.check":
		// A case's own claims come last: [checkCheckClaims] subtracts the
		// inherited ones prepended ahead of them to reach the authored index.
		inherited := a.mergedChecks - a.source.ownChecks
		if index < inherited {
			return nil, false
		}

		return a.source.path.field("expect").field("check").item(index - inherited), true
	}

	path := a.source.path
	for _, step := range strings.Split(field, ".") {
		path = path.field(step)
	}

	return path, true
}

// splitIndex reads the `name[i]` form a finding about one entry of a list is
// written in — `expect.check[0]`, `stubs[1]` — which is the spelling
// [loc.String] renders and this package's prose already uses.
//
// A malformed index is not indexed rather than an error: this is placing a
// diagnostic, and failing to place one is the documented outcome already.
func splitIndex(field string) (name string, index int, indexed bool) {
	open := strings.IndexByte(field, '[')
	if open <= 0 || !strings.HasSuffix(field, "]") {
		return field, 0, false
	}

	index, err := strconv.Atoi(field[open+1 : len(field)-1])
	if err != nil || index < 0 {
		return field, 0, false
	}

	return field[:open], index, true
}

// codeFor is the code a finding of this class carries.
//
// The field a finding names is its class — a consumer grouping "an output the
// case did not name" apart from "an output whose value differs" is grouping by
// what the claim was about — so the mapping is from field to code rather than
// from a message this would have to parse.
//
// A field with no entry here is [v1.DiagnosticCodeExpectationUnmet], which is
// what every one of these is: a claim the file made that the run contradicted.
// The named codes exist where a consumer has a reason to treat one differently,
// and adding a class is adding a row rather than a mechanism.
func codeFor(field string) v1.DiagnosticCode {
	name, _, _ := splitIndex(field)
	switch name {
	case "expect.outputs":
		return v1.DiagnosticCodeOutputMismatch
	case "stubs":
		return v1.DiagnosticCodeStubUnmatched
	default:
		return v1.DiagnosticCodeExpectationUnmet
	}
}
