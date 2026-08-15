package docsgen

import (
	"go/ast"
	"go/parser"
	"go/token"
	"io/fs"
	"path/filepath"
	"regexp"
	"slices"
	"strconv"
	"strings"
	"testing"

	"github.com/spf13/cobra"
	"github.com/spf13/pflag"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// The env-var table is the one document here written by hand, so it is the one
// with a test that walks the tree rather than comparing bytes.
//
// It travels with the table rather than with the reference's other tests, which
// live in cmd/flow beside the live values they render: this pair needs no
// command tree and no MCP registration, only the table in envvars.go and the
// source files under cmd/ and pkg/.

// testGenerator is a generator over sources these two tests do not read.
//
// [Generator.documentedEnvironmentVariables] is a method because one of its
// rows takes the address default from the sources, and nothing else here
// touches them; the rest is whatever [New] will accept.
func testGenerator(t *testing.T) *Generator {
	t.Helper()

	generator, err := New(Sources{
		NewRoot:        func() *cobra.Command { return &cobra.Command{Use: "flow"} },
		UseLine:        func(c *cobra.Command) string { return c.CommandPath() },
		FlagName:       func(f *pflag.Flag) string { return "--" + f.Name },
		MCPTools:       []MCPTool{{Name: "flowstate_validate"}},
		DefaultAddress: "127.0.0.1:8080",
	})
	require.NoError(t, err)

	return generator
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
// cmd/flow/internal/docsgen/cli.go reads whatever the table names, in order to
// clear and restore it — a read of every documented variable and, necessarily,
// of none that is not.
// pkg/flowstate/v1/credentialsource/env.go reads whatever variable name its
// caller configured (credentialsource.NewEnvSource(variable), or
// credentialsource.Resolve's EnvVar). The default the CLI itself uses,
// FLOWSTATE_TOKEN, is a literal one call site up in cmd/flow/credentials.go
// and is documented there; a caller of the package with a different name
// documents its own choice.
var exemptDynamicReads = map[string]bool{
	"pkg/flowstate/v1/secrets/env.go":          true,
	"cmd/flow/internal/docsgen/cli.go":         true,
	"pkg/flowstate/v1/credentialsource/env.go": true,
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

	generator := testGenerator(t)
	found := scanEnvironmentReads(t)

	for name, where := range found.names {
		assert.True(t, generator.environmentVariableIsDocumented(name),
			"%s is read in %s and docs/reference/envvars.md does not carry it; "+
				"add it to documentedEnvironmentVariables in cmd/flow/internal/docsgen/envvars.go and regenerate",
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

	generator := testGenerator(t)
	found := scanEnvironmentReads(t)

	for _, variable := range generator.documentedEnvironmentVariables() {
		if variable.family {
			continue
		}

		assert.Contains(t, found.names, variable.name,
			"%s is documented and nothing under cmd/ or pkg/ reads it", variable.name)
	}
}

// environmentVariableIsDocumented reports whether the table covers a name,
// exactly or through a family.
func (g *Generator) environmentVariableIsDocumented(name string) bool {
	for _, variable := range g.documentedEnvironmentVariables() {
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

	root, err := filepath.Abs("../../../..")
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
