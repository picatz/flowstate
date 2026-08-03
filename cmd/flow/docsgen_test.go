package main

import (
	"go/ast"
	"go/parser"
	"go/token"
	"io/fs"
	"os"
	"path/filepath"
	"regexp"
	"slices"
	"strconv"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// The generated reference is only worth having if it cannot quietly stop being
// true, so the tests here are all about the ways it could.
//
// Three of them, one per way. It has to be reproducible, or CI's `git diff
// --exit-code` pin fails for reasons that are about the machine rather than the
// change. It has to be committed, so the pin is checkable at all — and checking
// it here as well means an author finds out from `go test` rather than from a
// round trip through CI. And the one table that is written by hand, the
// environment variables, has to be held to the tree in both directions.

// referenceDir is the committed reference, from this package's directory.
const referenceDir = "../../docs/reference"

// TestGeneratedDocsAreStable is the property the CI pin rests on.
//
// A generator that ranges over a map produces a document that is correct and
// different every time, which makes `git diff --exit-code` a coin flip and
// teaches everyone to re-run it until it passes. Rendering twice and comparing
// bytes is the cheapest way to catch an unsorted iteration the moment it is
// introduced, and it fails deterministically enough to be believed — Go
// randomizes map order per range, so an unsorted listing of any size loses this
// almost immediately.
func TestGeneratedDocsAreStable(t *testing.T) {
	for _, doc := range referenceDocuments() {
		t.Run(doc.name, func(t *testing.T) {
			assert.Equal(t, doc.render(), doc.render(),
				"generating %s twice produced two different documents; something iterates unsorted", doc.name)
		})
	}
}

// TestGeneratedDocsAreCommitted is the same check CI makes, made here first.
//
// CI is a backstop rather than the feedback loop: a task added, a flag renamed or
// an RPC introduced changes these files, and finding that out from a red build is
// a round trip that bought nothing. The failure says what to run, because the fix
// is one command and the alternative is somebody hand-editing a generated file.
func TestGeneratedDocsAreCommitted(t *testing.T) {
	for _, doc := range referenceDocuments() {
		t.Run(doc.name, func(t *testing.T) {
			committed, err := os.ReadFile(filepath.Join(referenceDir, doc.name))
			require.NoError(t, err, "the generated reference moved and this test did not")

			assert.Equal(t, doc.render(), string(committed),
				"docs/reference/%s is out of date; run `flow docs generate` and commit the result", doc.name)
		})
	}
}

// TestGeneratingRestoresTheEnvironment keeps the generator from being a command
// with a side effect.
//
// It has to clear the environment to be reproducible — flag defaults are read
// from it when the command tree is built — and clearing it is exactly the kind of
// thing that gets left cleared. In-process that would be invisible here and
// catastrophic in a test binary, where the next test would run against an
// environment this one emptied.
func TestGeneratingRestoresTheEnvironment(t *testing.T) {
	t.Setenv("FLOWSTATE_ADDRESS", "example.test:9999")
	t.Setenv("FLOWSTATE_TOKEN", "not-a-real-token")

	rendered := renderCLIReference()

	assert.Equal(t, "example.test:9999", os.Getenv("FLOWSTATE_ADDRESS"),
		"generating the CLI reference left FLOWSTATE_ADDRESS cleared")
	assert.Equal(t, "not-a-real-token", os.Getenv("FLOWSTATE_TOKEN"),
		"generating the CLI reference left FLOWSTATE_TOKEN cleared")

	assert.NotContains(t, rendered, "example.test:9999",
		"the generated CLI reference recorded the environment it was generated in")
}

// TestEnvironmentMirrorsAreDerived pins the derivation the CLI reference's
// Environment column depends on.
//
// The mapping from a variable to the flag it feeds exists nowhere as data —
// pflag sees a default string and cannot know where it came from — so it is
// recovered by setting a sentinel and looking for it. That is clever enough to
// deserve a test that it still works: a change to how a default is composed
// (wrapping it in a `cmp.Or`, say, which is already how two of them are written)
// could silently empty the whole column, and an empty column reads as "no flag
// takes a variable" rather than as a broken derivation.
func TestEnvironmentMirrorsAreDerived(t *testing.T) {
	var mirrors map[string]string
	withCleanEnvironment(func() {
		mirrors = environmentMirrors()
	})

	assert.Equal(t, "FLOWSTATE_ADDRESS", mirrors["flow get address"],
		"--address no longer reads FLOWSTATE_ADDRESS, or the derivation stopped working")
	assert.Equal(t, "TEMPORAL_TASK_QUEUE", mirrors["flow worker task-queue"],
		"--task-queue no longer reads TEMPORAL_TASK_QUEUE, or the derivation stopped working")
}

// TestEveryMCPToolHasALocality holds the one hand-kept fact on the MCP reference
// to the tools that are actually registered.
//
// Where a tool answers — in this process or against a server — is carried by a Go
// func value in mcp.go's dispatch table, which nothing outside can inspect. So it
// is written down, and written down once; this is what stops it being written
// down wrongly. Both directions, because each fails differently: a method with no
// entry is documented as needing a server when it does not, and an entry for a
// method that is gone is a line about a tool nobody can call.
func TestEveryMCPToolHasALocality(t *testing.T) {
	t.Parallel()

	methods := map[string]bool{}
	for _, method := range workflowServiceMethods() {
		methods[method.name] = true
	}

	for name := range mcpLocalTools {
		assert.True(t, methods[name],
			"mcpLocalTools names %q, which is not a service method any more", name)
	}

	// The other direction is not "every method is local" — most are not — but that
	// every method was *considered*. A method absent from the map is documented as
	// remote, which is right today only because the map was written when those
	// methods were. So the assertion is that the tool set the reference renders
	// covers the registered set exactly.
	documented := map[string]bool{}
	for _, tool := range mcpTools() {
		documented[tool.name] = true
	}
	for _, method := range workflowServiceMethods() {
		assert.True(t, documented[mcpToolName(method.name)],
			"the MCP reference does not document %q", mcpToolName(method.name))
	}
	assert.True(t, documented[runLocalToolName],
		"the MCP reference does not document the one tool with no RPC behind it")
}

// envVarLiteral matches a string literal that is one of this project's
// environment variables.
//
// Deliberately anchored on the prefixes rather than on "looks shouty": a
// SCREAMING_SNAKE literal is also how a header name, a claim, and a Temporal
// search attribute are written, and a test that flagged those would be one people
// silence. What it costs is a variable named outside these prefixes, which the
// call-site half below catches instead.
var envVarLiteral = regexp.MustCompile(`^(FLOWSTATE|TEMPORAL|OTEL)_[A-Z0-9_]*$`)

// scannedSource is what one pass over the tree found.
type scannedSource struct {
	// names maps each environment variable mentioned to where it was mentioned.
	names map[string][]string

	// dynamic is every os.Getenv/os.LookupEnv whose argument could not be
	// resolved to a name, as file:line.
	dynamic []string
}

// exemptSourceDirs are trees the scan does not walk.
//
// One entry, and it is not `flow`: the example plugin is a separate binary that
// happens to live in this repository, with its own configuration
// (`EXAMPLE_UNHEALTHY`, `EXAMPLE_SECRET_*`) that a Flowstate operator never sets.
// Documenting it in Flowstate's own reference would be describing a different
// program.
var exemptSourceDirs = []string{
	"pkg/flowstate/v1/plugin/examples",
}

// exemptDynamicReads are the files permitted an unresolvable read, with the
// reason.
//
// pkg/flowstate/v1/secrets/env.go composes the variable from a prefix and the
// secret's name, which is the `FLOWSTATE_SECRET_<NAME>` family the reference
// documents as a family for exactly this reason: there is no literal to find
// because there is no single name.
//
// A file added here is a claim that a variable is read under a name no reader can
// grep for, so each one wants a sentence rather than a line.
// cmd/flow/docsgen.go reads whatever the table names, in order to clear and
// restore it — a read of every documented variable and, necessarily, of none
// that is not.
var exemptDynamicReads = map[string]bool{
	"pkg/flowstate/v1/secrets/env.go": true,
	"cmd/flow/docsgen.go":             true,
}

// TestEveryEnvironmentReadIsDocumented is the drift test, and the point of the
// whole exercise.
//
// The env-var table is the one VISION names as having already drifted — it
// shipped ten variables short — and it is the one document here that cannot be
// derived, because there is no registration point: a variable is read where it is
// needed, as a flag default in one place, a condition in another, a size ceiling
// in the server. So instead of deriving the table, the *set* is checked against
// the tree, which converts the drift from something nobody sees into a red test.
//
// Two halves, because a read can hide in two ways. A name written as a literal is
// found by matching the literal, which covers the constants and the inline
// `os.Getenv("…")` alike. A read whose argument is not a literal is found by
// looking at call sites, and has to be exempted deliberately — otherwise a
// variable composed at runtime would be a hole in exactly the shape of the thing
// this is defending.
func TestEveryEnvironmentReadIsDocumented(t *testing.T) {
	t.Parallel()

	found := scanEnvironmentReads(t)

	for name, where := range found.names {
		assert.True(t, environmentVariableIsDocumented(name),
			"%s is read in %s and docs/reference/envvars.md does not carry it; "+
				"add it to documentedEnvironmentVariables in cmd/flow/docsgen.go and regenerate",
			name, strings.Join(where, ", "))
	}

	for _, site := range found.dynamic {
		file, _, _ := strings.Cut(site, ":")
		assert.True(t, exemptDynamicReads[file],
			"%s reads an environment variable under a name this test cannot resolve, so nothing "+
				"can check that it is documented; either name it with a constant or add the file to "+
				"exemptDynamicReads with the reason", site)
	}
}

// TestEveryDocumentedEnvironmentVariableIsRead is the other direction, and the
// one that wastes a reader's time rather than hiding something from them.
//
// A documented variable nothing reads is a setting somebody will set and then
// wonder why nothing happened — the same defect `FLOWSTATE_VERBOSE_LOGGING` had
// for real when a flag default overwrote it. Family entries are skipped because
// there is deliberately no literal to find.
func TestEveryDocumentedEnvironmentVariableIsRead(t *testing.T) {
	t.Parallel()

	found := scanEnvironmentReads(t)

	for _, variable := range documentedEnvironmentVariables() {
		if variable.family {
			continue
		}

		assert.Contains(t, found.names, variable.name,
			"%s is documented and nothing under cmd/ or pkg/ reads it", variable.name)
	}
}

// environmentVariableIsDocumented reports whether the table covers a name,
// exactly or through a family.
func environmentVariableIsDocumented(name string) bool {
	for _, variable := range documentedEnvironmentVariables() {
		if variable.name == name {
			return true
		}
		if !variable.family {
			continue
		}

		// A family is written with the varying part spelled out —
		// `FLOWSTATE_SECRET_<NAME>`, `OTEL_EXPORTER_OTLP_*` — so what it covers is
		// everything sharing the fixed part.
		prefix := variable.name
		if cut := strings.IndexAny(prefix, "<*"); cut >= 0 {
			prefix = prefix[:cut]
		}
		if prefix != "" && strings.HasPrefix(name, prefix) {
			return true
		}
	}

	return false
}

// scanEnvironmentReads parses every non-test Go file under cmd/ and pkg/.
//
// go/parser rather than a grep, for one reason that matters: a grep finds
// `os.Getenv("VAULT_TOKEN")` written inside a doc comment, and a test that fails
// on prose is a test someone deletes. The AST carries no comments here, so what
// this sees is what the program does.
//
// Generated files are skipped. They are enormous, they contain no environment
// reads, and parsing them is most of the cost of this test.
func scanEnvironmentReads(t *testing.T) scannedSource {
	t.Helper()

	root, err := filepath.Abs("../..")
	require.NoError(t, err)

	found := scannedSource{names: map[string][]string{}}

	// Constant values, so a read spelled `os.Getenv(protocol.SocketEnv)` resolves
	// to the name it actually reads. Keyed by the identifier's own name, which is
	// enough here — these are unique across the tree — and avoids type-checking
	// the whole module for four lookups.
	constants := map[string]string{}

	var files []string
	for _, dir := range []string{"cmd", "pkg"} {
		err := filepath.WalkDir(filepath.Join(root, dir), func(path string, entry fs.DirEntry, err error) error {
			if err != nil {
				return err
			}

			relative, relErr := filepath.Rel(root, path)
			if relErr != nil {
				return relErr
			}
			relative = filepath.ToSlash(relative)

			if entry.IsDir() {
				if entry.Name() == "testdata" || slices.Contains(exemptSourceDirs, relative) {
					return fs.SkipDir
				}

				return nil
			}

			switch {
			case !strings.HasSuffix(path, ".go"),
				strings.HasSuffix(path, "_test.go"),
				strings.HasSuffix(path, ".pb.go"),
				strings.HasSuffix(path, ".connect.go"):
				return nil
			}

			files = append(files, relative)

			return nil
		})
		require.NoError(t, err)
	}
	require.NotEmpty(t, files, "found no Go files to scan; the tree moved and this test did not")

	fset := token.NewFileSet()
	parsed := make([]*ast.File, 0, len(files))
	for _, relative := range files {
		file, err := parser.ParseFile(fset, filepath.Join(root, relative), nil, parser.SkipObjectResolution)
		require.NoError(t, err, "parsing %s", relative)
		parsed = append(parsed, file)

		// Constants first, across every file, because a read in one package
		// resolves against a constant declared in another.
		for _, decl := range file.Decls {
			gen, ok := decl.(*ast.GenDecl)
			if !ok || gen.Tok != token.CONST {
				continue
			}
			for _, spec := range gen.Specs {
				value, ok := spec.(*ast.ValueSpec)
				if !ok {
					continue
				}
				for i, name := range value.Names {
					if i >= len(value.Values) {
						continue
					}
					if literal, ok := stringLiteral(value.Values[i]); ok {
						constants[name.Name] = literal
					}
				}
			}
		}
	}

	for i, file := range parsed {
		relative := files[i]

		ast.Inspect(file, func(node ast.Node) bool {
			switch n := node.(type) {
			case *ast.BasicLit:
				if value, ok := stringLiteral(n); ok && envVarLiteral.MatchString(value) {
					found.names[value] = appendOnce(found.names[value], relative)
				}

			case *ast.CallExpr:
				if !isEnvironmentRead(n.Fun) || len(n.Args) != 1 {
					return true
				}
				name, resolved := resolveEnvironmentName(n.Args[0], constants)
				if !resolved {
					found.dynamic = append(found.dynamic,
						relative+":"+strconv.Itoa(fset.Position(n.Pos()).Line))

					return true
				}
				found.names[name] = appendOnce(found.names[name], relative)
			}

			return true
		})
	}

	return found
}

// isEnvironmentRead reports whether an expression calls os.Getenv or
// os.LookupEnv.
func isEnvironmentRead(fun ast.Expr) bool {
	selector, ok := fun.(*ast.SelectorExpr)
	if !ok {
		return false
	}
	pkg, ok := selector.X.(*ast.Ident)
	if !ok || pkg.Name != "os" {
		return false
	}

	return selector.Sel.Name == "Getenv" || selector.Sel.Name == "LookupEnv"
}

// resolveEnvironmentName reads the name a call site asks for, through one level
// of constant.
func resolveEnvironmentName(arg ast.Expr, constants map[string]string) (string, bool) {
	switch a := arg.(type) {
	case *ast.BasicLit:
		return stringLiteral(a)
	case *ast.Ident:
		value, found := constants[a.Name]

		return value, found
	case *ast.SelectorExpr:
		value, found := constants[a.Sel.Name]

		return value, found
	}

	return "", false
}

// stringLiteral unquotes a string literal, reporting whether it was one.
func stringLiteral(expr ast.Expr) (string, bool) {
	literal, ok := expr.(*ast.BasicLit)
	if !ok || literal.Kind != token.STRING {
		return "", false
	}

	value, err := strconv.Unquote(literal.Value)
	if err != nil {
		return "", false
	}

	return value, true
}

// appendOnce keeps a file listed once however many times it mentions a name.
func appendOnce(files []string, file string) []string {
	if slices.Contains(files, file) {
		return files
	}

	return append(files, file)
}
