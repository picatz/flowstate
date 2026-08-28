package flowtest

import (
	"errors"
	"fmt"
	"slices"
	"strings"

	"github.com/goccy/go-yaml"
)

// What a refused `*.test.yaml` reports (#923 step 1): every problem the file
// has, each positioned where the file wrote it.
//
// Two things changed here at once, and they are the same change. A load used to
// stop at its first refusal and return one `fmt.Errorf` string, so an author
// fixing a suite met its mistakes one run at a time — while `flowfile` has
// reported every diagnostic in a workflow at once since it existed, "so a
// caller can report all of them at once" ([flowfile.Parse]). And those strings
// named the case they were about and nothing else, which left the schema's own
// promise false: [v1.TestCase] says failures are "positioned to the test file",
// and `line: 0, column: 0` is not a position.
//
// The prose is unchanged, deliberately. A diagnostic still names the case —
// `test "the rollback" stub 2 …` — because that is the identity a reader
// matches back to the file, and it now names the line and column as well. The
// standard is `flowfile/validate.go`'s, restated by CLAUDE.md: the position,
// what is wrong, and what to do instead.

// MaxLoadProblems bounds how many problems one load reports at once.
//
// The resource is the refusal and the ratio is the file's: a suite at
// [MaxTestsPerFile] cases whose every case is malformed is five hundred
// diagnostics, which is a message nobody reads and a slice sized by whoever
// wrote the file. Twenty is what fits on a screen — [flowdebug.MaxScriptProblems]
// bounds a recorded script for the identical reason and lands on the identical
// number — and the count of what was found travels beside them
// ([Diagnostics.Total]), so a bounded report never reads as a short one.
const MaxLoadProblems = 20

// MaxLoadProblemBytes bounds the message text one refusal carries, after the
// first problem.
//
// Counting problems does not bound them, because the document chooses how large
// one is: a diagnostic quotes what it refused — `holds the expression %q`, a
// signature, a case's name — and a value in a file of [MaxTestFileBytes] can be
// most of a megabyte. One such problem was reachable before anything was
// collected, and is still reachable now; what collecting adds is the
// *multiplier*, since an anchored value aliased into twenty positions earns
// twenty diagnostics quoting it. This is that multiplier, removed: the first
// problem is kept whatever its size — a refusal with no explanation in it would
// be the worse failure — and the rest are kept while they fit. What was not
// kept is counted, so [Diagnostics.Total] answers for a report shortened by
// either bound.
const MaxLoadProblemBytes = 64 << 10

// A Diagnostic is one problem a `*.test.yaml` has, positioned in it.
//
// Shaped like [flowfile.Diagnostic] and for the same reasons, but this
// package's own: that type carries a step, a task input and a step kind, none
// of which a test file has, and this one carries a case name, which a workflow
// does not.
type Diagnostic struct {
	// File is the path the document was loaded from, when the door that loaded
	// it had one. Empty for bytes with no path ([LoadSource]), and empty for a
	// document built in Go.
	//
	// Carried per problem rather than once per report because a problem is what
	// travels: an editor publishing diagnostics needs the URI on each one, and
	// a loader that grows a refusal originating in a sibling `testdefaults.yaml`
	// needs to be able to say which file it is about.
	File string

	// Line is the 1-based source line, or zero when the position is not known.
	//
	// Zero is not an omission. A problem with the document as a whole has no
	// line, and neither has one about a value this document inherited rather
	// than wrote — see [document.positionOf], where that rule lives.
	Line int

	// Column is the 1-based column within Line, or zero when only the line is
	// known. It counts characters rather than bytes, as
	// [flowfile.Diagnostic.Column] does.
	Column int

	// Test is the `name:` of the case the problem is in, when it is in one.
	// Empty for a problem with the file itself — its `vars:`, its `defaults:`,
	// its `coverage:` stanza, or the document as a whole.
	//
	// Structured beside the message rather than only inside it, because reading
	// a case name back out of prose is what the editor does today
	// (`anchorAtNamedTest`, #1110 slice 1) and rewording a diagnostic silently
	// moves a squiggle when a consumer does that.
	Test string

	// Field addresses the value in the source — `tests[0].stubs[1].returns` —
	// the way the loader's own prose names one.
	//
	// A key holding a dot or a bracket makes this ambiguous to read back, which
	// is why nothing positions anything by parsing it: [loc] is the addressing,
	// and this is its rendering.
	Field string

	// Message states the problem and, where possible, what to do instead.
	Message string
}

// Error renders the diagnostic as `file:line:column: message`, dropping each
// part it does not have.
//
// A diagnostic with a file and no position keeps `file: message`, which is the
// shape every refusal from this loader has always had — and the honest end of
// the rule rather than an exception to it, since a problem with the whole
// document has no line to name. [flowfile.Diagnostic.Error] renders the same
// two cases the same way, and `cmd/flow`'s positionLine joins them identically.
func (d Diagnostic) Error() string {
	var b strings.Builder
	if d.File != "" {
		b.WriteString(d.File)
		b.WriteString(":")
	}
	switch {
	case d.Line > 0 && d.Column > 0:
		fmt.Fprintf(&b, "%d:%d: ", d.Line, d.Column)
	case d.Line > 0:
		fmt.Fprintf(&b, "%d: ", d.Line)
	case d.File != "":
		// Nothing to position, so the file name and the first word of the
		// message would otherwise run together.
		b.WriteString(" ")
	}
	b.WriteString(d.Message)

	return b.String()
}

// Diagnostics is everything wrong with one test document, together.
//
// It is what every door of this loader returns for a document it read and
// refused, so a caller that wants the problems rather than their rendering asks
// for it:
//
//	if problems, refused := errors.AsType[*flowtest.Diagnostics](err); refused {
//		for _, p := range problems.Problems { … }
//	}
//
// Two refusals are deliberately not this type, because neither is about
// anything in the document. A file larger than [MaxTestFileBytes] is refused
// before it is a document at all — nothing was parsed, so there is no line to
// name — and a refusal originating in the directory's [DirDefaultsName] stays a
// [*DirDefaultsError] naming that file, which is the type the editor already
// asks for by name rather than by reading prose (#1109).
//
// A caller that only prints keeps working unchanged, because the rendering of a
// single unpositioned problem is byte for byte what this loader returned before
// it collected anything.
type Diagnostics struct {
	// Problems are the problems found, in source order, at most
	// [MaxLoadProblems] of them.
	Problems []Diagnostic

	// Total is how many were found, which is larger than len(Problems) exactly
	// when the bound was reached. A bounded report that did not say so would
	// read as a complete one.
	Total int

	// cause is the underlying error a problem was translated from, kept so that
	// `errors.As` still finds it: a YAML syntax or strict-key refusal is
	// goccy's, and the language server reads its token to underline the exact
	// characters at fault.
	cause error
}

// Error renders every problem on its own line, each naming its own file.
//
// One error whose every line names its file, because a line that travels on its
// own — into a CI log, into a `refused:` field, into an editor — has to say
// which file it is about. `cmd/flow`'s scriptProblemsError renders a bounded
// report the same way, down to the tail line.
func (ds *Diagnostics) Error() string {
	lines := make([]string, 0, len(ds.Problems)+1)
	for _, d := range ds.Problems {
		lines = append(lines, d.Error())
	}
	if ds.Total > len(ds.Problems) {
		tail := fmt.Sprintf("%d more problems were found and %d are shown",
			ds.Total-len(ds.Problems), len(ds.Problems))
		// Named only when there is one file to name. What was dropped is not
		// necessarily about the file the first kept problem happens to be
		// about: a suite and the directory's `testdefaults.yaml` are refused
		// together, and prefixing that line with either one asserts a
		// provenance for problems nobody can see (Codex, #1185). The count is
		// the honest part, and it is the whole line when the report spans two
		// documents.
		if file := ds.oneFile(); file != "" {
			tail = file + ": " + tail
		}
		lines = append(lines, tail)
	}

	return strings.Join(lines, "\n")
}

// Unwrap returns the error a problem was translated from, when there was one.
func (ds *Diagnostics) Unwrap() error { return ds.cause }

// oneFile is the document every problem shown is about, or empty when they are
// about more than one — or when none of them names a file, which is what bytes
// with no path answer.
//
// A function taking its input from the problems rather than a comparison written
// where they are built, because a report that spans two files is the only input
// that tells the two answers apart, and a real report usually does not
// (CLAUDE.md, "assert where the answers differ").
func (ds *Diagnostics) oneFile() string {
	file := ""
	for _, d := range ds.Problems {
		switch {
		case d.File == "":
			continue
		case file == "":
			file = d.File
		case d.File != file:
			return ""
		}
	}

	return file
}

// inFile stamps path on every problem that does not already name a file, for
// the doors that have one.
//
// Done here rather than at every report site because the path is a property of
// the door, not of the check: [parseSourceWith] is reached with bytes from a
// file, from an editor's buffer, and from an MCP request, and only two of those
// know a path. A problem that already names one was written in a document this
// door did not open — the directory's [DirDefaultsName] — and keeps it.
func (ds *Diagnostics) inFile(path string) *Diagnostics {
	if ds == nil || path == "" {
		return ds
	}
	for i := range ds.Problems {
		if ds.Problems[i].File == "" {
			ds.Problems[i].File = path
		}
	}

	return ds
}

// yamlProblem translates a refusal from the YAML decoder — a syntax error, a
// duplicate key, an unknown field the strict decode will not accept — into the
// [Diagnostic] grammar every other refusal here speaks.
//
// One problem, not a collection, and that is not a shortcut: the decode
// produces the value every check below reads, so a document it refused has
// nothing for them to judge. `flowfile` answers a YAML-level failure the same
// way and for the same reason (#654) — an author meets one error language
// whichever layer rejected the file.
//
// goccy's errors carry the token they failed on, which is where the position
// comes from, and its bare message, which is the sentence without the rendered
// source excerpt the parser would otherwise print into the middle of a
// diagnostic. The original travels as the cause, so a caller that wants the
// token — the language server underlines exactly the characters at fault —
// still reaches it through `errors.As`.
func yamlProblem(err error) *Diagnostics {
	d := Diagnostic{Message: err.Error()}

	var yamlErr yaml.Error
	if errors.As(err, &yamlErr) {
		if message := yamlErr.GetMessage(); message != "" {
			d.Message = message
		}
		if tok := yamlErr.GetToken(); tok != nil && tok.Position != nil {
			d.Line, d.Column = tok.Position.Line, tok.Position.Column
		}
	}

	return &Diagnostics{Problems: []Diagnostic{d}, Total: 1, cause: err}
}

// A site names what a diagnostic is about: the case it is in, and where in the
// source the offending value was written.
//
// The same shape [flowfile]'s compiler passes to its own report — a diagnostic
// is about something, and saying so at the call site is what keeps the message
// and the position from being decided in two different places.
type site struct {
	// test is the case's `name:`, when the problem is inside a case.
	test string

	// at addresses the value in the source. Nil where this document did not
	// write the value — an inherited stub, a default folded in from a sibling
	// file — which is exactly when no position may be claimed.
	at loc

	// file names the document that wrote the value, for a check that knows it
	// directly rather than by the path.
	//
	// Two mechanisms answer one question, and the split is the point. A path
	// answers it for a value the fold moves *unchanged*: `defaults.sender`
	// means the same thing in both documents, so [problems.writtenElsewhere]
	// can look it up. It cannot answer it where the fold **renumbers** — the
	// directory's claims are prepended, so `defaults.check[0]` names the
	// sibling's first claim and the suite's first claim on the same string,
	// and a lookup keyed on that string attributed a suite-written claim to
	// the sibling and threw away its real position (Codex, #1185). Where a
	// path is that ambiguous, the check that knows the answer says it here
	// instead of encoding it in an identity another site can forge.
	file string
}

// in returns the site with its path replaced, for a check that descends into a
// value while staying in the same case and the same document.
func (r site) in(path loc) site { return site{test: r.test, at: path, file: r.file} }

// writtenIn returns the site with the document that wrote the value named, and
// no path: a path into another document is not one this parse can resolve.
func (r site) writtenIn(file string) site { return site{test: r.test, file: file} }

// problems collects every refusal one document earns.
//
// The collection is the point: a loader that stopped at the first refusal made
// an author fix a suite one run at a time, and the checks below are
// independent of each other in almost every case — a malformed stub in case
// three says nothing about whether case seven's trigger is coherent.
//
// Where they are not independent, the check that establishes what a value *is*
// stops descending into it rather than reporting a cascade about a shape
// nobody wrote. That is the same rule flowfile's compiler follows when its
// entries pass fails: report once, and do not judge the inside of a value whose
// kind is already wrong.
type problems struct {
	doc   *document
	found []Diagnostic
	total int

	// bytes is the message text kept so far, bounded by [MaxLoadProblemBytes].
	bytes int

	// elsewhere are the paths whose values were written in another file —
	// [dirDefaults.combineInto]'s answer — and elsewhereFile is that file. A
	// problem at or under one of them is attributed there and left
	// unpositioned, because the path addresses that document and looking it up
	// in this one could only match by accident.
	elsewhere     []loc
	elsewhereFile string
}

// newProblems collects against a parsed document, or against none — the Go
// door builds a [File] rather than parsing one, so its refusals are the same
// refusals with no position to give them.
func newProblems(doc *document) *problems {
	return &problems{doc: doc}
}

// wrote records that path's value came from file rather than from the document
// being parsed.
func (p *problems) wrote(file string, paths []loc) {
	if file == "" || len(paths) == 0 {
		return
	}
	p.elsewhereFile = file
	p.elsewhere = append(p.elsewhere, paths...)
}

// writtenElsewhere reports whether path addresses a value another file wrote.
//
// At or under: a directory that contributed `defaults.sender` also contributed
// every claim inside it, and the check that refuses one reports the field it is
// on rather than the stanza.
func (p *problems) writtenElsewhere(path loc) bool {
	for _, contributed := range p.elsewhere {
		if len(path) < len(contributed) {
			continue
		}
		if slices.Equal(contributed, path[:len(contributed)]) {
			return true
		}
	}

	return false
}

// report records one problem, positioned at the value r names.
func (p *problems) report(r site, format string, args ...any) {
	p.record(r, false, fmt.Sprintf(format, args...))
}

// reportKey is [problems.report] positioned at the key rather than the value,
// for a problem whose subject is the name an author wrote.
func (p *problems) reportKey(r site, format string, args ...any) {
	p.record(r, true, fmt.Sprintf(format, args...))
}

// record appends one problem, up to both bounds, counting every one.
func (p *problems) record(r site, atKey bool, message string) {
	p.total++
	if len(p.found) >= MaxLoadProblems {
		return
	}
	// Both bounds are on what is kept rather than on what is found, and the
	// byte one spares the first problem: see [MaxLoadProblemBytes].
	if len(p.found) > 0 && p.bytes+len(message) > MaxLoadProblemBytes {
		return
	}
	p.bytes += len(message)

	d := Diagnostic{Test: r.test, Field: r.at.String(), Message: message}
	switch {
	case r.file != "":
		// The check named the document itself, which it does where a path
		// could not tell the two apart — see [site.file].
		d.File = r.file
	case p.writtenElsewhere(r.at):
		// Another document wrote this value, so it is named after that
		// document and positioned in neither: the path addresses the file that
		// holds the text, and this parse has no tree for it.
		d.File = p.elsewhereFile
	default:
		locate := p.doc.positionOf
		if atKey {
			locate = p.doc.positionOfKey
		}
		if position, known := locate(r.at); known {
			d.Line, d.Column = position.line, position.column
		}
	}
	p.found = append(p.found, d)
}

// err returns the collected problems, or nil when there are none.
//
// Typed rather than returned as an error, so that a caller writing
// `if refused := p.err(); refused != nil` cannot hand back a nil pointer
// wearing an error interface — the return every Go program eventually gets
// wrong once.
//
// Sorted by position, so the same file always reports the same way and a reader
// meets the problems in the order the file states them. Problems with no
// position sort first, as [flowfile.Diagnostics] sorts its own: they are about
// the document rather than about a line in it.
func (p *problems) err() *Diagnostics {
	if p.total == 0 {
		return nil
	}

	slices.SortStableFunc(p.found, func(a, b Diagnostic) int {
		if a.Line != b.Line {
			return a.Line - b.Line
		}
		if a.Column != b.Column {
			return a.Column - b.Column
		}

		return strings.Compare(a.Message, b.Message)
	})

	return &Diagnostics{Problems: p.found, Total: p.total}
}
