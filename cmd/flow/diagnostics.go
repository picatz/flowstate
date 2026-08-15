package main

import (
	"errors"
	"fmt"
	"io"
	"strings"

	"github.com/picatz/flowstate/cmd/flow/internal/ui"
	v1 "github.com/picatz/flowstate/pkg/flowstate/v1"
	"github.com/picatz/flowstate/pkg/flowstate/v1/flowfile"
)

// A diagnostic's position had three spellings, and only one of them was the one
// the rest of the world links on (#384):
//
//	bad2.yaml: 1:10: edition: ...      a space after the filename
//	bad9.yaml:5:5: step "fetch": ...   the correct form
//	6:5: step "greet": ...             a continuation line, no filename at all
//
// The space defeats every consumer that matches `file:line:col`. Terminal
// emulators, editor terminals, `errorformat` and friends make `bad9.yaml:5:5`
// clickable and leave `bad2.yaml: 1:10` inert. The bare continuation line is
// worse: it is compact for someone reading one file's report top to bottom, and
// unusable everywhere a line travels on its own, which is most places a
// diagnostic actually ends up. A CI log line reading `6:5: unknown key "withh"`
// answers "which file?" with "scroll up", and grep, CI annotations and agent
// transcripts all consume single lines out of context.
//
// The three spellings arose because three code paths each rendered a position
// themselves: a slice of diagnostics was written a line at a time with the path
// in front of each, while an *error* carrying the same diagnostics was handed to
// `%v`, and [flowfile.Diagnostics.Error] joins its members with newlines and
// knows no filename. So the shape depended on which of two return values a
// failure came back through, which is not a distinction an author can see.
//
// One formatter, used by every text surface, is the whole fix. The machine
// surfaces are unaffected: `--output json` has carried file, line and column as
// fields per diagnostic all along, and this is only the text form agreeing with
// the convention.

// positionLine is the one join every rendered diagnostic line goes through:
// path, a colon, and whatever comes after — no space, because a space between
// the filename and the position is exactly the first of the three spellings
// #384 found. Both path and tail arrive already themed where a caller has a
// theme, so the styling wraps either half without touching the colon a
// consumer matches on.
func positionLine(path, tail string) string {
	return path + ":" + tail
}

// diagnosticLine renders one diagnostic as `file:line:column: message`, or
// `file:line: message` when no column is known.
//
// path is passed already themed where a caller has a theme, because the styling
// wraps the filename and leaves the punctuation a consumer matches on intact.
//
// A diagnostic with no position at all keeps `file: message`, and that is not an
// exception to the rule so much as the honest end of it: it describes the whole
// document rather than a line in it, so there is no line to name. See
// [flowfile.Diagnostic.Error], which supplies the leading separator.
func diagnosticLine(path string, d flowfile.Diagnostic) string {
	return positionLine(path, d.Error())
}

// writeDiagnostics writes every diagnostic in ds, one standalone line each.
func writeDiagnostics(w io.Writer, path string, ds flowfile.Diagnostics) {
	for _, d := range ds {
		fmt.Fprintln(w, diagnosticLine(path, d))
	}
}

// diagnosticsError joins ds into one error whose every line names its own file,
// for a caller that returns a failure rather than printing one.
//
// Written out rather than returning ds itself, because ds renders through
// [flowfile.Diagnostics.Error], which cannot name a file: the package that finds
// a problem is not the one that knows what the bytes were read from.
func diagnosticsError(path string, ds flowfile.Diagnostics) error {
	lines := make([]string, 0, len(ds))
	for _, d := range ds {
		lines = append(lines, diagnosticLine(path, d))
	}
	return errors.New(strings.Join(lines, "\n"))
}

// errDiagnosticsOf widens any error into the diagnostics it carries: a parse or
// compile failure already returns [flowfile.Diagnostics] with a real line and
// column, and anything else — a bare I/O or YAML-syntax error — becomes one
// unpositioned diagnostic naming the whole document, [flowfile.Diagnostic]'s own
// honest answer for a fact about the file as a whole. The `errors.As` pattern is
// shared with [diagnosticsError]'s callers rather than repeated per command, so
// a third command cannot widen an error its own way and spell the fallback
// differently.
func errDiagnosticsOf(err error) flowfile.Diagnostics {
	var diagnostics flowfile.Diagnostics
	if errors.As(err, &diagnostics) {
		return diagnostics
	}
	return flowfile.Diagnostics{{Message: err.Error()}}
}

// writeErrDiagnostics reports err as one diagnostic line per position it
// carries, each naming path and each danger-toned the way a refusal reads
// elsewhere in `flow fix` and `flow fmt`.
//
// This is the call site both commands were missing: err handed whole to
// theme.Danger.Render before this fix meant a multi-diagnostic compile failure
// rendered its first line with a space after the filename and every line after
// the first with no filename at all — the same two spellings #384 found in
// `flow validate`, reintroduced here because [flowfile.Diagnostics.Error]
// joins its members with newlines and knows no filename to put in front of
// them.
func writeErrDiagnostics(w io.Writer, theme ui.Theme, path string, err error) {
	themedPath := theme.Muted.Render(path)
	for _, d := range errDiagnosticsOf(err) {
		fmt.Fprintln(w, positionLine(themedPath, theme.Danger.Render(d.Error())))
	}
}

// errDiagnosticsProto widens err into the schema diagnostics a machine report
// carries — [flowfile.Diagnostic.Proto] applied over [errDiagnosticsOf]'s
// widening, shared by `flow fix` and `flow fmt` so a machine format and a
// human-readable one describe the same failure from the same source rather
// than each widening err its own way.
func errDiagnosticsProto(err error) []*v1.Diagnostic {
	diagnostics := errDiagnosticsOf(err)
	out := make([]*v1.Diagnostic, 0, len(diagnostics))
	for _, d := range diagnostics {
		out = append(out, d.Proto())
	}
	return out
}
