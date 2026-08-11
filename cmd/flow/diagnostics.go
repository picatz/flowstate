package main

import (
	"errors"
	"fmt"
	"io"
	"strings"

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
	return path + ":" + d.Error()
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
