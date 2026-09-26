package main

import (
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

// The checks, exercised against a fixture module written to say one thing
// per comment, and then the repository itself, which is the claim this tool
// exists to make.

// fixtureModule writes a module with two packages: lib, which exports Greet
// and Greeter.Hello, and app, whose single file carries source. It returns
// the module root.
func fixtureModule(t *testing.T, source string) string {
	t.Helper()

	root := t.TempDir()
	write := func(rel, body string) {
		path := filepath.Join(root, rel)
		require.NoError(t, os.MkdirAll(filepath.Dir(path), 0o755))
		require.NoError(t, os.WriteFile(path, []byte(body), 0o600))
	}
	write("go.mod", "module example.com/m\n\ngo 1.27\n")
	write("lib/lib.go", strings.Join([]string{
		"// Package lib greets.",
		"package lib",
		"",
		"// Greet says hello.",
		"func Greet() string { return \"hello\" }",
		"",
		"// Greeter greets.",
		"type Greeter struct{ Name string }",
		"",
		"// Hello says hello.",
		"func (Greeter) Hello() string { return Greet() }",
		"",
		"func farewell() {}",
	}, "\n")+"\n")
	write("app/app.go", source)
	return root
}

func check(t *testing.T, source string) []Finding {
	t.Helper()

	findings, links, err := Check(fixtureModule(t, source))
	require.NoError(t, err)
	require.Positive(t, links, "the fixture holds a link and none was examined; the walk is broken")
	return findings
}

// links renders findings as their bracketed text, the part a test asserts on.
func links(findings []Finding) []string {
	var out []string
	for _, f := range findings {
		out = append(out, f.Link)
	}
	return out
}

func TestALinkThatResolvesIsSilent(t *testing.T) {
	t.Parallel()

	findings := check(t, `// Package app links everywhere it may.
//
// [Run], [Config.Validate], [lib.Greet], [lib.Greeter.Hello], [lib],
// [net/http.Client], [example.com/m/lib.Greet], and [golang.org/x/mod].
package app

import "example.com/m/lib"

// Config configures.
type Config struct{}

// Validate validates.
func (Config) Validate() error { return nil }

// Run runs, see [Config].
func Run() string { return lib.Greet() }
`)
	require.Empty(t, findings)
}

func TestAnUnresolvedLinkIsNamedWithItsLine(t *testing.T) {
	t.Parallel()

	findings := check(t, `// Package app is fine.
package app

// Run runs.
//
// It is tested by [TestRun] and calls [flowfile.Parse], unlike [lib].
func Run() {}
`)
	require.Equal(t, []string{"[TestRun]", "[flowfile.Parse]", "[lib]"}, links(findings))
	for _, f := range findings {
		require.Equal(t, "app/app.go", f.File)
		require.Equal(t, 6, f.Line, "the line of the comment that holds the link, not of the declaration")
		require.Contains(t, f.Reason, "plain text")
	}
}

func TestASlashFragmentIsAnImportPathThatDoesNotExist(t *testing.T) {
	t.Parallel()

	findings := check(t, `// Package app wraps [pkg/flowstate/v1/engine].
package app

// Run runs.
func Run() {}
`)
	require.Equal(t, []string{"[pkg/flowstate/v1/engine]"}, links(findings))
	require.Contains(t, findings[0].Reason, "neither in the standard library nor in this repository")
}

func TestALinkIntoTheRepositoryMustNameAnExportedSymbol(t *testing.T) {
	t.Parallel()

	findings := check(t, `// Package app is fine.
package app

import "example.com/m/lib"

// Run calls [lib.Greet] and [lib.Greeter.Name], then [lib.Missing] and
// [example.com/m/lib.Greeter.Goodbye].
func Run() string { return lib.Greet() }
`)
	require.Equal(t, []string{"[lib.Missing]", "[example.com/m/lib.Greeter.Goodbye]"}, links(findings))
}

func TestALinkGluedToASuffixIsNotALink(t *testing.T) {
	t.Parallel()

	findings := check(t, `// Package app is fine.
package app

// Config configures.
type Config struct{}

// Run needs a [Config]ured value, where [Config] alone is a link.
func Run(Config) {}
`)
	require.Equal(t, []string{"[Config]"}, links(findings))
	require.Contains(t, findings[0].Reason, "glued")
}

func TestALinkInAHeadingIsNamedAsSuch(t *testing.T) {
	t.Parallel()

	findings := check(t, `// Package app is fine.
package app

// Config configures.
type Config struct{}

// Run runs a [Config].
//
// # Why not [Config]
//
// Because.
func Run(Config) {}
`)
	require.Equal(t, []string{"[Config]"}, links(findings))
	require.Equal(t, 9, findings[0].Line)
	require.Contains(t, findings[0].Reason, "heading")
}

func TestProseAndCodeAreNotLinks(t *testing.T) {
	t.Parallel()

	findings := check(t, `// Package app is fine.
package app

// Run reads items[i] and a Set[T], then [key] from the map, per [RFC 9728].
// Only [Run] is a link.
//
//	cfg := [Unresolved]{} // code is verbatim
func Run() {}

// run is unexported, so nothing renders [Unresolved] for it.
func run() {}
`)
	require.Empty(t, findings)
}

// TestTheRepositoryDocLinksResolve is the claim: every doc link in the
// package comment and exported declarations of every package in this
// repository resolves to what it names. The root is the literal "../.." so
// this runs from tools/doclinks.
func TestTheRepositoryDocLinksResolve(t *testing.T) {
	t.Parallel()

	findings, links, err := Check("../..")
	require.NoError(t, err)
	require.Greater(t, links, 1000,
		"only %d links were examined, too few to have reached the tree; the walk is broken, not the comments", links)

	var lines []string
	for _, f := range findings {
		lines = append(lines, f.String())
	}
	require.Empty(t, findings,
		"doc links that render as literal brackets or point nowhere. Import the package, spell the full "+
			"import path, or drop the brackets from a name that is not a link:\n%s", strings.Join(lines, "\n"))
}
