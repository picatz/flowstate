package flowfile

import (
	"errors"
	"fmt"
	"strings"
	"unicode"

	v1 "github.com/picatz/flowstate/pkg/flowstate/v1"
)

// ErrSourceMapMismatch is returned by [Diagnostics.Locate] when a source map was
// made for a different program than the one that was validated. Applying it
// would move a diagnostic onto a line that does not hold the step.
var ErrSourceMapMismatch = errors.New("the source map describes a different program than this specification")

// maxLocatedFileBytes bounds the file name a located diagnostic carries, the
// same limit [v1.RelatedLocation.File] holds.
const maxLocatedFileBytes = 512

// Locate places each diagnostic that names a step, and has no position of its
// own, where sourceMap says that step was written: the source document in File
// and, for a text document, the line and column the step starts at. It is how a
// specification with no Flowfile beside it (a compiled one, a builder's, a
// canvas's) reports `file:line:column` instead of only an address (#1568).
//
// sourceMap must describe workflow: its [v1.DebugSourceMap.IrDigest] has to be
// [IRDigest] of workflow, or Locate returns [ErrSourceMapMismatch] and changes
// nothing. A map that is not within the schema's bounds is refused the same
// way. It returns the number of diagnostics placed.
//
// A diagnostic is left as it was when it names no step, the map does not locate
// it, its location indexes no document, or changed reports that the document is
// no longer the bytes the map was made from (changed may be nil, which trusts
// the map). Nothing is guessed: such a diagnostic is rendered by its address.
// A document that is not text (a canvas) has no line, so only File is set, and
// the diagnostic's Path is what a consumer resolves against the map's node id.
//
// Locate reads no file and opens nothing the map names: a locator is data, not
// an instruction to fetch. A document's name is cleaned of control characters
// and capped before it reaches a diagnostic, because the map's author is not
// the reader of the report.
func (ds Diagnostics) Locate(workflow *v1.Workflow, sourceMap *v1.DebugSourceMap, changed func(*v1.DebugSourceDocument) bool) (int, error) {
	if err := v1.Validate(sourceMap); err != nil {
		return 0, fmt.Errorf("the source map is not valid: %w", err)
	}
	if sourceMap.GetIrDigest() != IRDigest(workflow) {
		return 0, ErrSourceMapMismatch
	}

	at := map[string]*v1.DebugSourceLocation{}
	for _, entry := range sourceMap.GetEntries() {
		site := entry.GetSite()
		if site.GetWorkflow() != workflow.GetName() || entry.GetLocation() == nil {
			continue
		}
		// First wins, as [Positions.StepPath] does.
		if key := v1.DebugSiteKey(site); at[key] == nil {
			at[key] = entry.GetLocation()
		}
	}

	var placed int
	stale := map[int32]bool{}
	for i := range ds {
		d := &ds[i]
		if len(d.Path) == 0 || d.Line != 0 || d.File != "" {
			continue
		}
		loc := at[v1.DebugSiteKey(&v1.DebugSite{Workflow: workflow.GetName(), Path: d.Path})]
		if loc == nil || loc.GetDocument() < 0 || int(loc.GetDocument()) >= len(sourceMap.GetDocuments()) {
			continue
		}
		document := sourceMap.GetDocuments()[loc.GetDocument()]
		if changed != nil {
			gone, known := stale[loc.GetDocument()]
			if !known {
				gone = changed(document)
				stale[loc.GetDocument()] = gone
			}
			if gone {
				continue
			}
		}
		file := printableName(document.GetUri())
		if file == "" {
			continue
		}
		d.File = file
		if r := loc.GetRange(); r != nil {
			d.Line, d.Column = int(r.GetStartLine()), int(r.GetStartColumn())
		}
		placed++
	}

	return placed, nil
}

// printableName is name with every character that is not printable replaced by
// `?` and cut to [maxLocatedFileBytes], on a rune boundary.
func printableName(name string) string {
	var b strings.Builder
	for _, r := range name {
		if b.Len()+len(string(r)) > maxLocatedFileBytes {
			break
		}
		if !unicode.IsPrint(r) {
			r = '?'
		}
		b.WriteRune(r)
	}

	return b.String()
}
