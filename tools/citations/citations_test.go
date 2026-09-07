package main

import (
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

// The checks, exercised against a fixture tree written to say one thing each,
// and then the repository itself, which is the claim this tool exists to make.

// fixtureTree writes a small repository: one Go file and one document citing
// it, and returns the root. The Go file has a function at a known line so a
// citation can point at it, past it, or beside it.
func fixtureTree(t *testing.T, doc string) string {
	t.Helper()

	root := t.TempDir()
	require.NoError(t, os.MkdirAll(filepath.Join(root, "pkg"), 0o755))
	require.NoError(t, os.MkdirAll(filepath.Join(root, "docs"), 0o755))
	source := strings.Join([]string{
		"package pkg",
		"",
		"// Greet says hello.",
		"func Greet() string {", // line 4
		"\treturn \"hello\"",
		"}",
		"",
		"func farewell() {}", // line 8
	}, "\n") + "\n"
	require.NoError(t, os.WriteFile(filepath.Join(root, "pkg", "a.go"), []byte(source), 0o600))
	require.NoError(t, os.WriteFile(filepath.Join(root, "docs", "guide.md"), []byte(doc), 0o600))
	return root
}

func checkFixture(t *testing.T, doc string) []Finding {
	t.Helper()

	root := fixtureTree(t, doc)
	docs, err := Documents(root)
	require.NoError(t, err)
	require.Equal(t, []string{"docs/guide.md"}, docs)
	findings, total, err := Check(root, docs)
	require.NoError(t, err)
	require.Positive(t, total, "the fixture document cites nothing; the extraction is broken")
	return findings
}

func TestACitationThatHoldsIsSilent(t *testing.T) {
	t.Parallel()

	require.Empty(t, checkFixture(t, "`Greet` is the greeting (`pkg/a.go:4-6`).\n"))
	require.Empty(t, checkFixture(t, "One line with no symbol near it (`pkg/a.go:8`).\n"))
	require.Empty(t, checkFixture(t, "`Greet` is defined here (`pkg/a.go:3-4`), and `farewell` elsewhere.\n"),
		"one of the identifiers the sentence names is in the cited lines")
}

func TestAMissingFileIsNamedWithTheRootRelativeSpelling(t *testing.T) {
	t.Parallel()

	findings := checkFixture(t, "See `a.go:4`.\n")
	require.Len(t, findings, 1)
	require.Contains(t, findings[0].Problem, "no such file from the repository root")
	require.Contains(t, findings[0].Problem, "did you mean pkg/a.go")

	findings = checkFixture(t, "See `pkg/gone.go:4`.\n")
	require.Len(t, findings, 1)
	require.Contains(t, findings[0].Problem, "no such file from the repository root")
	require.NotContains(t, findings[0].Problem, "did you mean")
}

func TestALinePastTheEndIsRefused(t *testing.T) {
	t.Parallel()

	findings := checkFixture(t, "See `pkg/a.go:40`.\n")
	require.Len(t, findings, 1)
	require.Equal(t, "the file has 8 lines", findings[0].Problem)

	findings = checkFixture(t, "See `pkg/a.go:4-40`.\n")
	require.Len(t, findings, 1)
	require.Equal(t, "the file has 8 lines", findings[0].Problem)
}

// TestAMovedSymbolIsNamedWithItsNewLine is the check that catches the common
// drift: the file survives, the line exists, and the function is elsewhere.
func TestAMovedSymbolIsNamedWithItsNewLine(t *testing.T) {
	t.Parallel()

	findings := checkFixture(t, "`Greet` is the greeting (`pkg/a.go:8`).\n")
	require.Len(t, findings, 1)
	require.Equal(t, "docs/guide.md", findings[0].Citation.Doc)
	require.Equal(t, 1, findings[0].Citation.Line)
	require.Equal(t, "`pkg/a.go:8`", findings[0].Citation.Text)
	require.Equal(t, "the cited lines hold none of Greet, which the sentence names; Greet is at line 4", findings[0].Problem)
	require.Equal(t, "docs/guide.md:1: `pkg/a.go:8`: "+findings[0].Problem, findings[0].String())

	findings = checkFixture(t, "`pkg.Nowhere` is the greeting (`pkg/a.go:4`).\n")
	require.Len(t, findings, 1)
	require.Contains(t, findings[0].Problem, "nothing in the file does")
}

func TestSymbolsAreReadFromTheSentenceNotTheParagraph(t *testing.T) {
	t.Parallel()

	far := "`Greet` " + strings.Repeat("x", symbolReach+1) + " (`pkg/a.go:8`).\n"
	require.Empty(t, checkFixture(t, far), "an identifier further than a sentence away belongs to another claim")

	require.Empty(t, checkFixture(t, "`greet` is lower-case prose (`pkg/a.go:8`).\n"),
		"a plain lowercase word is as likely a flag as an identifier")

	require.Empty(t, checkFixture(t, "`flow validate` and `--against` are not identifiers (`pkg/a.go:8`).\n"))

	row := "| `Greet` | " + strings.Repeat("what the row says ", 8) + "| `pkg/a.go:8` |\n"
	findings := checkFixture(t, row)
	require.Len(t, findings, 1, "a table row's first cell is its subject, whatever the distance")
	require.Contains(t, findings[0].Problem, "Greet is at line 4")
}

func TestADependencyCitationIsSpelledWithItsModuleAndCheckedNowhere(t *testing.T) {
	t.Parallel()

	require.Empty(t, checkFixture(t, "`Interceptor` lives at `connectrpc.com/otelconnect/instruments.go:44-49`.\n"))
	require.True(t, isModulePath("connectrpc.com/otelconnect/instruments.go"))
	require.False(t, isModulePath("cmd/flow/main.go"))
	require.False(t, isModulePath("main.go"), "a bare file name has a dot and no module")
}

func TestDocumentsAreTheHandWrittenSet(t *testing.T) {
	t.Parallel()

	root := t.TempDir()
	for _, path := range []string{
		"AGENTS.md", "README.md", "THREAT_MODEL.md",
		"docs/DSL.md", "docs/guides/x.md", "docs/reference/cli.md", "docs/plans/2026-01-plan.md",
	} {
		require.NoError(t, os.MkdirAll(filepath.Dir(filepath.Join(root, path)), 0o755))
		require.NoError(t, os.WriteFile(filepath.Join(root, path), []byte("x\n"), 0o600))
	}
	docs, err := Documents(root)
	require.NoError(t, err)
	require.Equal(t, []string{"AGENTS.md", "README.md", "THREAT_MODEL.md", "docs/DSL.md", "docs/guides/x.md"}, docs,
		"docs/reference is generated and docs/plans is pinned to the revision it was written against")
}

// TestTheRepositoryCitationsHold is the claim: every citation in the
// hand-written documentation resolves against this tree. The root is named
// through the literals the gate's plan reads ("../../docs", "../../README.md",
// "../../AGENTS.md"), so a change under those roots seeds this package.
func TestTheRepositoryCitationsHold(t *testing.T) {
	t.Parallel()

	root := "../.."
	for _, must := range []string{"../../docs", "../../README.md", "../../AGENTS.md"} {
		_, err := os.Stat(must)
		require.NoError(t, err, "this test runs from tools/citations")
	}

	docs, err := Documents(root)
	require.NoError(t, err)
	findings, total, err := Check(root, docs)
	require.NoError(t, err)
	require.Positive(t, total, "no citations were found; the extraction is broken, not the documents")

	var lines []string
	for _, f := range findings {
		lines = append(lines, f.String())
	}
	require.Empty(t, findings, "citations that do not hold:\n%s", strings.Join(lines, "\n"))
}
