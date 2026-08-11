package main

import (
	"regexp"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// The shape is the contract, and three spellings arose precisely because nothing
// pinned it (#384). These two patterns are the two ways it was broken.

// barePosition matches a line that opens with a position and names no file: the
// second and later diagnostics of one file used to arrive this way, which is
// compact for someone reading a report top to bottom and unusable for grep, CI
// annotations, editor terminals and every other consumer that meets a log line
// on its own.
var barePosition = regexp.MustCompile(`^\d+:\d+:`)

// spacedPosition matches a filename separated from its position by a space,
// which defeats the `file:line:col` convention terminals link on.
var spacedPosition = regexp.MustCompile(`^[^\s:]+: \d+:\d+:`)

// linkablePosition is the form every positioned diagnostic line must take.
var linkablePosition = regexp.MustCompile(`^[^\s:]+:\d+(:\d+)?: `)

// assertLinkableDiagnostics checks every non-empty line of a report.
//
// A line with no position at all is allowed through: it describes the whole
// document rather than a line in it, so there is no line to name. What is never
// allowed is a line that *has* a position and spells it in either broken way.
func assertLinkableDiagnostics(t *testing.T, output string) {
	t.Helper()

	for _, line := range strings.Split(strings.TrimSpace(output), "\n") {
		if line == "" {
			continue
		}
		assert.NotRegexp(t, barePosition, line,
			"every diagnostic line must name its own file, because log lines travel alone")
		assert.NotRegexp(t, spacedPosition, line,
			"a space between the filename and the position defeats every consumer that links on file:line:col")
	}
}

// TestValidateDiagnosticsAllShareOnePositionSpelling is the regression for #384.
//
// The reported case is a file whose diagnostics come back as a parse *error*
// rather than as a slice, which is the branch that rendered the whole error
// through `%v`: the filename landed once, followed by a space, and every
// diagnostic after the first had no filename at all.
func TestValidateDiagnosticsAllShareOnePositionSpelling(t *testing.T) {
	// Two problems in one file, so the second diagnostic is the one that used to
	// arrive with no filename in front of it. Both are refused by the compiler,
	// which is what makes them a parse error rather than a validation report.
	const twoParseProblems = `edition: v2026.2
name: broken
steps:
  - id: greet
    log:
      message: ${vars.a +}
    withh: nope
`

	path := writeWorkflow(t, "two-problems.yaml", twoParseProblems)

	out, err := validateOutput(t, path)
	require.Error(t, err)

	lines := diagnosticLinesOf(out)
	require.GreaterOrEqual(t, len(lines), 2,
		"the fixture must produce more than one diagnostic or it cannot see the continuation-line bug")

	assertLinkableDiagnostics(t, out)

	for _, line := range lines {
		assert.True(t, strings.HasPrefix(line, path+":"),
			"every diagnostic line must stand alone and name its file, got %q", line)
		assert.Regexp(t, linkablePosition, line)
	}
}

// TestValidateDiagnosticsFromValidationShareTheSameSpelling covers the other
// branch, where diagnostics come back as a slice. It was already correct, and it
// is pinned here so the two branches cannot drift apart again: which of two
// return values a failure travels through is not a distinction an author can
// see, and it decided the shape of the report for as long as each branch spelled
// a position itself.
func TestValidateDiagnosticsFromValidationShareTheSameSpelling(t *testing.T) {
	path := writeWorkflow(t, "broken.yaml", brokenWorkflow)

	out, err := validateOutput(t, path)
	require.Error(t, err)

	assertLinkableDiagnostics(t, out)
	for _, line := range diagnosticLinesOf(out) {
		assert.True(t, strings.HasPrefix(line, path+":"), "got %q", line)
	}
}

// TestLoadWorkflowDiagnosticsNameTheirFile covers the surface `flow run`,
// `flow run local` and `flow test` share. It printed the path, a colon, a
// newline, and then bare positions, so no line of it was linkable at all.
func TestLoadWorkflowDiagnosticsNameTheirFile(t *testing.T) {
	const twoParseProblems = `edition: v2026.2
name: broken
steps:
  - id: greet
    log:
      message: ${vars.a +}
    withh: nope
`

	path := writeWorkflow(t, "two-problems.yaml", twoParseProblems)

	_, err := loadWorkflow(path)
	require.Error(t, err)

	lines := strings.Split(err.Error(), "\n")
	require.GreaterOrEqual(t, len(lines), 2)

	for _, line := range lines {
		assert.True(t, strings.HasPrefix(line, path+":"),
			"every line of the error must name its file, got %q", line)
		assert.Regexp(t, linkablePosition, line)
	}
}

// diagnosticLinesOf keeps the lines of a report that carry a position, dropping
// the summary words `validate` writes beside them.
func diagnosticLinesOf(output string) []string {
	var lines []string
	for _, line := range strings.Split(strings.TrimSpace(output), "\n") {
		if line == "" || strings.HasSuffix(line, ": ok") {
			continue
		}
		lines = append(lines, line)
	}
	return lines
}
