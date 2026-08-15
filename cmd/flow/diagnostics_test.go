package main

import (
	"os"
	"path/filepath"
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
	const twoParseProblems = `edition: v2026.3
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
	const twoParseProblems = `edition: v2026.3
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

// diagnosticCorpus is a directory's worth of deliberately-broken Flowfiles, one
// per mistake, chosen to walk different code paths through the compiler and the
// validator rather than different phrasings of the same one: a parse-time
// rejection (unknown key, retired key, unknown task, bad edition, CEL syntax
// error) returns diagnostics as an *error*, and a compile-time rejection
// (duplicate step id, unresolved reference, bad retry, bad switch case, a bad
// wait_for_signal field, a bad for_each field) returns them as a slice — the two
// branches #384 found spelling positions differently.
var diagnosticCorpus = map[string]string{
	"bad-edition.yaml": `edition: 2026.1
name: too-old
steps:
  - id: s
    log:
      message: hi
`,
	"unknown-task.yaml": `edition: v2026.3
name: broken
steps:
  - id: fetch
    htttp:
      url: https://example.com
`,
	"retired-key.yaml": `edition: v2026.3
name: broken
steps:
  - id: greet
    task: log
    log:
      message: hi
`,
	"unknown-step-key.yaml": `edition: v2026.3
name: broken
steps:
  - id: greet
    log:
      message: hi
    withh: nope
`,
	"cel-syntax-error.yaml": `edition: v2026.3
name: broken
steps:
  - id: greet
    log:
      message: ${vars.a +}
`,
	"duplicate-step-id.yaml": `edition: v2026.3
name: broken
steps:
  - id: dup
    log:
      message: one
  - id: dup
    log:
      message: two
`,
	"unresolved-reference.yaml": `edition: v2026.3
name: broken
steps:
  - id: greet
    log:
      message: ${steps.nope.result}
`,
	"bad-retry.yaml": `edition: v2026.3
name: broken
steps:
  - id: greet
    log:
      message: hi
    retry:
      attempts: -1
`,
	"bad-switch-case.yaml": `edition: v2026.3
name: broken
steps:
  - id: pick
    switch:
      value: ${vars.a}
      cases:
        - case: ${vars.a +}
          steps:
            - id: inner
              log:
                message: hi
`,
	"bad-wait-timeout.yaml": `edition: v2026.3
name: broken
steps:
  - id: hold
    wait_for_signal:
      name: approved
      timeout: ${vars.a +}
`,
	"bad-for-each-items.yaml": `edition: v2026.3
name: broken
steps:
  - id: loop
    for_each:
      items: ${vars.a +}
      steps:
        - id: inner
          log:
            message: hi
`,
	"bad-http-method.yaml": brokenWorkflow,
}

// TestValidateCorpusAllDiagnosticsShareOneSpelling is the sweep #384 asked for:
// a whole directory of deliberately-broken files, run through the same
// `flow validate` a person or CI would run, with every rendered line checked
// against the one canonical position spelling. A corpus beats one fixture per
// branch because a *new* diagnostic call site, added anywhere in the validator
// or the parser, lands in this walk automatically and fails here rather than
// shipping a third spelling nobody wrote a test for.
func TestValidateCorpusAllDiagnosticsShareOneSpelling(t *testing.T) {
	dir := t.TempDir()
	for name, body := range diagnosticCorpus {
		require.NoError(t, os.WriteFile(filepath.Join(dir, name), []byte(body), 0o600))
	}

	out, err := validateOutput(t, dir)
	require.Error(t, err, "every file in the corpus is broken; validate must fail")

	assertLinkableDiagnostics(t, out)

	lines := diagnosticLinesOf(out)

	// Anti-vacuity: a sweep that silently collected nothing would pass this test
	// for the same reason an empty corpus would, so require enough lines that a
	// walk which stopped finding files, or a validator that stopped finding
	// problems, cannot pass by accident.
	require.GreaterOrEqual(t, len(lines), len(diagnosticCorpus),
		"expected at least one diagnostic per broken file in the corpus, got:\n%s", out)

	for _, line := range lines {
		assert.Regexp(t, linkablePosition, line,
			"every positioned diagnostic line must read file:line[:col]: message, got %q", line)
	}
}
