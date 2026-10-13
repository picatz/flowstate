package lsp

import (
	"cmp"
	"context"
	"fmt"
	"io"
	"maps"
	"path/filepath"
	"slices"
	"strings"

	"github.com/sourcegraph/go-lsp"
	"github.com/sourcegraph/jsonrpc2"

	v1 "github.com/picatz/flowstate/pkg/flowstate/v1"
	"github.com/picatz/flowstate/pkg/flowstate/v1/flowfile"
)

// One open document can be the reason for a problem in another file: a test
// suite that includes testdefaults.yaml, a workflow that calls a file with a
// problem of its own. The document is the *source* of the diagnostic and the
// file it lands in is the *target*, and one target can have several sources.
//
// A target's published list is the union of what its sources say about it. That
// is what keeps one source from clearing a problem another still reports, and
// what lets a source that closes, or stops mentioning the target, retract
// exactly its own part: the target is told its new aggregate, which is the empty
// list when nobody is left to complain.

// maxCalleeTargets bounds how many files one workflow may publish diagnostics
// on. Callees are another party's to write and a call tree fans out, so the
// count is held down where the work is spent rather than where it is reported.
const maxCalleeTargets = 64

// maxRelatedPerDiagnostic bounds the related locations one published diagnostic
// carries once several sources' views of the same problem are merged. It matches
// the limit [v1.Diagnostic] holds its own related list to.
const maxRelatedPerDiagnostic = 16

// wrapDiagnostics lifts plain diagnostics into the stored form.
func wrapDiagnostics(ds []lsp.Diagnostic) []publishedDiagnostic {
	out := make([]publishedDiagnostic, 0, len(ds))
	for _, d := range ds {
		out = append(out, publishedDiagnostic{Diagnostic: d})
	}

	return out
}

// replaceContributionsLocked makes next everything source says about any file,
// and returns the files whose aggregate may have changed: source itself, every
// file it used to mention, and every file it mentions now. The caller holds
// diagnosticsMu.
func (s *FlowfileServer) replaceContributionsLocked(source lsp.DocumentURI, next map[lsp.DocumentURI][]publishedDiagnostic) map[lsp.DocumentURI]bool {
	touched := s.dropContributionsLocked(source)
	if s.diagnosticsBySource == nil {
		s.diagnosticsBySource = make(map[lsp.DocumentURI]map[lsp.DocumentURI][]publishedDiagnostic)
	}
	if s.sourcesByTarget == nil {
		s.sourcesByTarget = make(map[lsp.DocumentURI]map[lsp.DocumentURI]bool)
	}
	for uri := range next {
		touched[uri] = true
		if s.sourcesByTarget[uri] == nil {
			s.sourcesByTarget[uri] = make(map[lsp.DocumentURI]bool)
		}
		s.sourcesByTarget[uri][source] = true
	}
	s.diagnosticsBySource[source] = next

	return touched
}

// dropContributionsLocked forgets everything source said and returns the files
// that said it reached, source among them. The caller holds diagnosticsMu.
func (s *FlowfileServer) dropContributionsLocked(source lsp.DocumentURI) map[lsp.DocumentURI]bool {
	touched := map[lsp.DocumentURI]bool{source: true}
	for uri := range s.diagnosticsBySource[source] {
		touched[uri] = true
		delete(s.sourcesByTarget[uri], source)
		if len(s.sourcesByTarget[uri]) == 0 {
			delete(s.sourcesByTarget, uri)
		}
	}
	delete(s.diagnosticsBySource, source)

	return touched
}

// aggregateDiagnostics is what each touched file should now show, in URI order.
//
// A file that is open as a document of its own is not published a workflow's
// view of it: the buffer's analysis is the live one, where the caller read the
// file from disk, and the two disagree exactly while the author is editing. The
// withheld contributions stay stored, so closing the document brings them back.
func (s *FlowfileServer) aggregateDiagnostics(touched map[lsp.DocumentURI]bool) []publishDiagnosticsParams {
	uris := make([]lsp.DocumentURI, 0, len(touched))
	for uri := range touched {
		uris = append(uris, uri)
	}
	slices.Sort(uris)
	out := make([]publishDiagnosticsParams, 0, len(uris))
	for _, uri := range uris {
		var diagnostics []publishedDiagnostic
		seen := map[string]int{}
		ordered := make([]lsp.DocumentURI, 0, len(s.sourcesByTarget[uri]))
		for source := range s.sourcesByTarget[uri] {
			ordered = append(ordered, source)
		}
		slices.Sort(ordered)
		// A directly open document owns the code for an otherwise identical
		// diagnostic an including suite also reports on its URI.
		if own := slices.Index(ordered, uri); own >= 0 {
			ordered[0], ordered[own] = ordered[own], ordered[0]
		}
		open := len(ordered) > 0 && s.isOpenFile(uri)
		for _, source := range ordered {
			if open && source != uri && s.isWorkflowSource(source) {
				continue
			}
			byURI := s.diagnosticsBySource[source]
			for _, d := range byURI[uri] {
				key := fmt.Sprintf("%d:%d:%d:%d:%s", d.Range.Start.Line, d.Range.Start.Character,
					d.Range.End.Line, d.Range.End.Character, d.Message)
				if i, dup := seen[key]; dup {
					// The same problem reached through another source: keep every
					// source's reason for it. Merged into a copy, so the stored
					// contribution is never written to.
					diagnostics[i].RelatedInformation = mergeRelated(diagnostics[i].RelatedInformation, d.RelatedInformation)
					continue
				}
				seen[key] = len(diagnostics)
				diagnostics = append(diagnostics, d)
			}
		}
		if diagnostics == nil {
			diagnostics = []publishedDiagnostic{}
		}
		slices.SortStableFunc(diagnostics, func(a, b publishedDiagnostic) int {
			return cmp.Or(
				cmp.Compare(a.Range.Start.Line, b.Range.Start.Line),
				cmp.Compare(a.Range.Start.Character, b.Range.Start.Character),
				strings.Compare(a.Message, b.Message))
		})
		out = append(out, publishDiagnosticsParams{URI: uri, Diagnostics: diagnostics})
	}

	return out
}

// mergeRelated is the union of two related-location lists as a new slice:
// deduplicated, in a deterministic order, and no longer than
// [maxRelatedPerDiagnostic].
func mergeRelated(a, b []relatedInformation) []relatedInformation {
	out := slices.Concat(a, b)
	slices.SortStableFunc(out, func(x, y relatedInformation) int {
		return cmp.Or(
			strings.Compare(string(x.Location.URI), string(y.Location.URI)),
			cmp.Compare(x.Location.Range.Start.Line, y.Location.Range.Start.Line),
			cmp.Compare(x.Location.Range.Start.Character, y.Location.Range.Start.Character),
			cmp.Compare(x.Location.Range.End.Line, y.Location.Range.End.Line),
			cmp.Compare(x.Location.Range.End.Character, y.Location.Range.End.Character),
			strings.Compare(x.Message, y.Message))
	})
	out = slices.CompactFunc(out, func(x, y relatedInformation) bool {
		return x.Location == y.Location && x.Message == y.Message
	})

	return out[:min(len(out), maxRelatedPerDiagnostic)]
}

// isWorkflowSource reports whether source is an open workflow document.
func (s *FlowfileServer) isWorkflowSource(source lsp.DocumentURI) bool {
	doc, ok := s.docs.get(source)

	return ok && doc.kind == docWorkflow
}

// isOpenFile reports whether uri names a file some open document holds, however
// the client spelled its URI.
func (s *FlowfileServer) isOpenFile(uri lsp.DocumentURI) bool {
	if _, ok := s.docs.get(uri); ok {
		return true
	}
	path, ok := (&document{uri: uri}).filesystemPath()
	if !ok {
		return false
	}
	_, ok = s.docs.getByFilesystemPath(path)

	return ok
}

// sourceFirst moves source's own publication to the front, so an editor learns
// about the document it is looking at before the files behind it.
func sourceFirst(source lsp.DocumentURI, publications []publishDiagnosticsParams) []publishDiagnosticsParams {
	for i := range publications {
		if publications[i].URI == source {
			publications[0], publications[i] = publications[i], publications[0]
			break
		}
	}

	return publications
}

// canonicalURI is the URI other documents' contributions name doc by: its path
// rendered the way [fileURI] does, whatever authority the client opened it with.
func canonicalURI(doc *document) lsp.DocumentURI {
	if path, ok := doc.filesystemPath(); ok {
		return fileURI(path)
	}

	return doc.uri
}

// touchCanonicalLocked adds the canonical spelling of a document's URI to touched
// when other documents report on the file under it: the document may have been
// opened under another spelling, and what it publishes (or stops publishing)
// changes what those others' contributions should show. Nothing is added when no
// one contributes, so a document is not published a second, empty list under a
// spelling its client never used.
func (s *FlowfileServer) touchCanonicalLocked(touched map[lsp.DocumentURI]bool, canonical lsp.DocumentURI) {
	if len(s.sourcesByTarget[canonical]) > 0 {
		touched[canonical] = true
	}
}

// publishWorkflowDiagnostics analyzes a workflow document and publishes what it
// found: its own problems on its own URI, and each called file's on that file's.
func (s *FlowfileServer) publishWorkflowDiagnostics(ctx context.Context, conn *jsonrpc2.Conn, doc *document) {
	own, callees, reads := diagnoseAll(doc)
	loc := newLocator(doc)
	files := s.canonicalReads(doc.uri, loc, reads)

	next := map[lsp.DocumentURI][]publishedDiagnostic{doc.uri: publishable(loc, own)}
	for _, report := range callees {
		uri := loc.uri(report.GetFile())
		if uri == doc.uri {
			continue
		}
		if _, known := next[uri]; !known && len(next)-1 >= maxCalleeTargets {
			s.logger().Debug("callee diagnostics beyond the target limit not published",
				"uri", doc.uri, "limit", maxCalleeTargets)
			continue
		}
		for _, d := range report.GetDiagnostics() {
			next[uri] = append(next[uri], loc.calleeDiagnostic(report.GetFile(), d))
		}
	}

	s.diagnosticsMu.Lock()
	defer s.diagnosticsMu.Unlock()
	// An analysis of a buffer that has since changed or closed must not replace
	// the newer one's contribution.
	if current, ok := s.docs.get(doc.uri); !ok || current != doc {
		return
	}
	touched := s.replaceContributionsLocked(doc.uri, next)
	s.replaceCalleeReadsLocked(doc.uri, files)
	s.touchCanonicalLocked(touched, canonicalURI(doc))
	s.logger().Debug("published diagnostics",
		"uri", doc.uri, "version", doc.version, "count", len(next[doc.uri]), "targets", len(next)-1)
	for _, publication := range sourceFirst(doc.uri, s.aggregateDiagnostics(touched)) {
		s.notify(ctx, conn, publication)
	}
}

// clearWorkflowDiagnostics retracts a closed workflow's contributions: its own
// problems, and every file it reported on, which may be one the editor never
// opened. A file another open document still reports on keeps that part.
func (s *FlowfileServer) clearWorkflowDiagnostics(ctx context.Context, conn *jsonrpc2.Conn, uri lsp.DocumentURI) {
	s.diagnosticsMu.Lock()
	defer s.diagnosticsMu.Unlock()
	// A later didOpen may already have published a new generation for this URI.
	if _, reopened := s.docs.get(uri); reopened {
		return
	}
	touched := s.dropContributionsLocked(uri)
	delete(s.calleeReadsBySource, uri)
	s.touchCanonicalLocked(touched, canonicalURI(&document{uri: uri}))
	for _, publication := range sourceFirst(uri, s.aggregateDiagnostics(touched)) {
		s.notify(ctx, conn, publication)
	}
}

// maxCalleeReads bounds the callee files remembered for one workflow. The compile
// bounds the tree it reads only by its expansion budget (100,000 compiled
// steps), which is far more files than an editor needs to match a save against,
// so this bounds what is kept for each open document. A workflow past it is
// marked overflowed and republished by any callee save instead.
const maxCalleeReads = 1024

// calleeReads is what a save of a callee is matched against for one workflow:
// the canonical path of each file its compile read for `call:` steps, and
// whether more were read than are kept.
type calleeReads struct {
	files      map[string]bool
	overflowed bool
}

// canonicalReads is the canonical path of each of reads, the files a workflow's
// compile read for its `call:` steps as the compiler displayed them, up to the
// limit. It resolves symlinks, so the caller must not hold diagnosticsMu.
func (s *FlowfileServer) canonicalReads(source lsp.DocumentURI, loc *locator, reads []string) calleeReads {
	limit := cmp.Or(s.calleeReadLimit, maxCalleeReads)
	var out calleeReads
	if len(reads) > limit {
		s.logger().Warn("workflow reads more callee files than are remembered for saves; any callee save republishes it",
			"uri", source, "limit", limit)
		reads = reads[:limit]
		out.overflowed = true
	}
	out.files = make(map[string]bool, len(reads))
	for _, read := range reads {
		out.files[canonicalPath(loc.path(read))] = true
	}

	return out
}

// replaceCalleeReadsLocked makes reads the whole of what a save is matched
// against for source, replacing the previous publication's. The caller holds
// diagnosticsMu.
func (s *FlowfileServer) replaceCalleeReadsLocked(source lsp.DocumentURI, reads calleeReads) {
	if len(reads.files) == 0 && !reads.overflowed {
		delete(s.calleeReadsBySource, source)
		return
	}
	if s.calleeReadsBySource == nil {
		s.calleeReadsBySource = make(map[lsp.DocumentURI]calleeReads)
	}
	s.calleeReadsBySource[source] = reads
}

// calleeSourcesFor lists the open workflows a save of the file doc holds should
// bring up to date: those that last reported problems in it, and those whose last
// compile read it, which includes a callee that was clean then, one that could not
// be compiled, and one reached only through another callee. The second set is recorded when a workflow is
// published and replaced by its next publication or dropped when it closes, so a
// workflow edited to stop calling the file has no stale entry. A workflow that
// read more files than are remembered is named by every save. At most
// [v1.MaxModules] workflows are named.
func (s *FlowfileServer) calleeSourcesFor(doc *document) []lsp.DocumentURI {
	set := map[lsp.DocumentURI]bool{}
	var saved string
	path, hasPath := doc.filesystemPath()
	if hasPath {
		saved = canonicalPath(path)
	}
	s.diagnosticsMu.Lock()
	for source := range s.sourcesByTarget[canonicalURI(doc)] {
		if source != doc.uri && s.isWorkflowSource(source) {
			set[source] = true
		}
	}
	if hasPath {
		for source, reads := range s.calleeReadsBySource {
			if source != doc.uri && (reads.overflowed || reads.files[saved]) && s.isWorkflowSource(source) {
				set[source] = true
			}
		}
	}
	s.diagnosticsMu.Unlock()
	out := slices.Sorted(maps.Keys(set))
	if len(out) > v1.MaxModules {
		s.logger().Warn("callee save changes more open files than are rechecked", "limit", v1.MaxModules)
		out = out[:v1.MaxModules]
	}

	return out
}

// A locator places the files a workflow's diagnostics name: the URI an editor
// knows each by, and the range a line and column are in it.
//
// Diagnostics name a callee by the path the compiler displayed it under, which
// for a document with a filesystem path is that path joined onto the document's
// own directory, so it converts to the same URI [fileURI] would give the file.
type locator struct {
	doc   *document
	self  string
	views map[string]*document
}

func newLocator(doc *document) *locator {
	l := &locator{doc: doc, views: map[string]*document{}}
	if path, ok := doc.filesystemPath(); ok {
		l.self = filepath.Clean(path)
	}

	return l
}

// path is the cleaned absolute spelling of a file a diagnostic names.
func (l *locator) path(file string) string {
	if filepath.IsAbs(file) || l.self == "" {
		return filepath.Clean(file)
	}

	return filepath.Join(filepath.Dir(l.self), file)
}

// uri is the URI of file; the locator's own document keeps the URI the client
// opened it with.
func (l *locator) uri(file string) lsp.DocumentURI {
	if file == "" {
		return l.doc.uri
	}
	if path := l.path(file); path != l.self {
		return fileURI(path)
	}

	return l.doc.uri
}

// view is the document whose text a position in file is measured in: the
// workflow itself, or the called file as the disk holds it, read no further than
// the limit any document is analyzed to. Nil when it cannot be read.
func (l *locator) view(file string) *document {
	path := l.path(file)
	if file == "" || path == l.self {
		return l.doc
	}
	if view, done := l.views[path]; done {
		return view
	}
	var view *document
	// OpenRegular so a FIFO or a /proc path is refused rather than waited on or read.
	if f, err := flowfile.OpenRegular(path); err == nil {
		data, err := io.ReadAll(io.LimitReader(f, maxDocumentBytes))
		_ = f.Close()
		if err == nil {
			view = &document{text: string(data), index: newLineIndex(string(data))}
		}
	}
	l.views[path] = view

	return view
}

// rng is the range of a one-based line and column in file: the rest of the token
// there, as for any position the validator reports without an extent.
func (l *locator) rng(file string, line, column int) lsp.Range {
	view := l.view(file)
	if view == nil {
		at := lsp.Position{Line: max(line-1, 0)}
		return lsp.Range{Start: at, End: at}
	}

	return reportedRange(view, flowfile.Diagnostic{Line: line, Column: column})
}

// related converts the locations a diagnostic carries.
func (l *locator) related(locations []*v1.RelatedLocation) []relatedInformation {
	out := make([]relatedInformation, 0, len(locations))
	for _, r := range locations {
		out = append(out, relatedInformation{
			Location: lsp.Location{URI: l.uri(r.GetFile()), Range: l.rng(r.GetFile(), int(r.GetLine()), int(r.GetColumn()))},
			Message:  r.GetMessage(),
		})
	}

	return out
}

// calleeDiagnostic is a diagnostic of the called file `file`, as published on it.
func (l *locator) calleeDiagnostic(file string, d *v1.Diagnostic) publishedDiagnostic {
	return publishedDiagnostic{
		Diagnostic: lsp.Diagnostic{
			Range:    l.rng(file, int(d.GetLine()), int(d.GetColumn())),
			Severity: lsp.Error,
			Source:   diagnosticSource,
			Code:     d.GetCode(),
			Message:  d.GetMessage(),
		},
		RelatedInformation: l.related(d.GetRelated()),
	}
}
