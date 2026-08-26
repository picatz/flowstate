package main

import (
	"go/ast"
	"go/parser"
	"go/token"
	"io/fs"
	"os"
	"path/filepath"
	"slices"
	"strings"
)

// Analyze walks the tree under root and reports every finding, sorted by
// position.
//
// It parses rather than type-checks, which is the decision that makes it
// reach further than the test suite does: `plugins/*` are separate Go modules
// outside this module's build graph, so `go test ./...` never sees them — and
// a plugin's tests are exactly where a vacuous containment check would hurt
// most. A syntax tree needs no build, so this reads all of it.
//
// The cost of that choice is stated in [analyzeFunc]: no types, so a helper is
// followed by name and a call that hands over the test handle is trusted to
// assert. Both err toward silence.
func Analyze(root string) ([]Finding, int, error) {
	fset := token.NewFileSet()

	var findings []Finding
	tests := 0

	err := filepath.WalkDir(root, func(path string, entry fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if !entry.IsDir() {
			return nil
		}

		switch entry.Name() {
		case ".git", ".coverage", "node_modules", "testdata":
			// testdata is Go's own convention for source that is not the
			// package's: a fixture there is input to a test rather than one.
			return fs.SkipDir
		}

		for _, files := range parseDir(fset, path) {
			found, counted := analyzePackage(fset, files)
			findings = append(findings, found...)
			tests += counted
		}

		return nil
	})
	if err != nil {
		return nil, 0, err
	}

	slices.SortFunc(findings, func(a, b Finding) int { return strings.Compare(a.Pos, b.Pos) })

	return findings, tests, nil
}

// parseDir reads one directory's Go files, grouped by the package each
// declares.
//
// Hand-rolled rather than `parser.ParseDir`, which is deprecated — and the
// deprecation's own reason is worth stating, because it is not a limitation
// here. `ParseDir` was retired for ignoring build tags when it decides which
// files belong to a package, and this grouping ignores them too: files are
// grouped by the `package` clause they carry, so two build-tagged halves of
// one package are analysed together. For asking whether a test asserts
// anything that is *more* complete rather than less — a helper behind a tag
// still asserts, whichever tag is set on the machine running this.
//
// A file that does not parse is skipped rather than reported. The build says
// so, louder and first, and a syntax error is not this tool's finding.
func parseDir(fset *token.FileSet, dir string) map[string][]source {
	entries, err := os.ReadDir(dir)
	if err != nil {
		return nil
	}

	packages := map[string][]source{}
	for _, entry := range entries {
		if entry.IsDir() || !strings.HasSuffix(entry.Name(), ".go") {
			continue
		}

		file, err := parser.ParseFile(fset, filepath.Join(dir, entry.Name()), nil, parser.ParseComments)
		if err != nil {
			continue
		}

		name := file.Name.Name
		packages[name] = append(packages[name], source{
			file: file,
			// Only a `_test.go` file holds tests the runner will call. An
			// ordinary file may perfectly well declare `func TestFixture(t
			// testing.TB)` as a helper for other packages to use, and judging
			// that as a test would fail a build over a function `go test`
			// never invokes (Codex, #1125).
			isTest:  strings.HasSuffix(entry.Name(), "_test.go"),
			testing: testingImport(file),
		})
	}

	return packages
}

// source is one parsed file and the two things about it the checks need.
type source struct {
	file *ast.File

	// isTest reports that this is a `_test.go` file.
	isTest bool

	// testing is the local name of the `testing` import — "testing" ordinarily,
	// an alias where the file wrote one, "." for a dot import, and "" where the
	// file does not import it at all.
	testing string
}

// testingImport is the name a file knows the standard `testing` package by.
//
// Resolved per file rather than assumed, because `import tst "testing"` is
// legal and `go test` runs `func TestAliased(t *tst.T)` exactly as it runs any
// other. An analysis that insists on the literal identifier misses such a test
// entirely — neither reported nor counted — and a count that silently omits
// what it could not recognise is the failure this whole tool is about
// (Codex, #1125).
func testingImport(file *ast.File) string {
	for _, imported := range file.Imports {
		if imported.Path == nil || imported.Path.Value != `"testing"` {
			continue
		}
		if imported.Name != nil {
			return imported.Name.Name
		}

		return "testing"
	}

	return ""
}

// analyzed is one function, and what the walk learned about it.
type analyzed struct {
	decl *ast.FuncDecl

	// from is the file it was declared in.
	from source

	// asserts reports that this function can fail a test.
	asserts bool

	// calls are the same-package functions it calls by bare name, for the
	// fixpoint below.
	calls map[string]bool

	// handles are the names this function knows a *testing.T by, its own
	// parameter and every closure's.
	handles map[string]bool
}

// analyzePackage reports the findings in one parsed package, and how many test
// functions it looked at.
func analyzePackage(fset *token.FileSet, files []source) ([]Finding, int) {
	functions := map[string]*analyzed{}
	for _, from := range files {
		for _, decl := range from.file.Decls {
			fn, ok := decl.(*ast.FuncDecl)
			if !ok || fn.Body == nil || fn.Recv != nil {
				continue
			}
			functions[fn.Name.Name] = analyzeFunc(fn, from)
		}
	}

	// A helper that asserts makes its callers assert, and a helper calling a
	// helper is ordinary here. Iterated to a fixpoint rather than recursed,
	// because mutual recursion between two test helpers is legal Go and a
	// naive walk of it does not come back.
	for range len(functions) + 1 {
		changed := false
		for _, fn := range functions {
			if fn.asserts {
				continue
			}
			for callee := range fn.calls {
				if called, known := functions[callee]; known && called.asserts {
					fn.asserts = true
					changed = true

					break
				}
			}
		}
		if !changed {
			break
		}
	}

	asserters := map[string]bool{}
	for name, fn := range functions {
		if fn.asserts {
			asserters[name] = true
		}
	}

	var findings []Finding
	tests := 0

	for name, fn := range functions {
		if !fn.isTest() {
			continue
		}
		tests++

		if !fn.asserts {
			if _, excused := suppressed(fn.decl.Doc, CheckUnasserted); excused {
				continue
			}

			findings = append(findings, Finding{
				Check: CheckUnasserted,
				Test:  name,
				Pos:   fset.Position(fn.decl.Pos()).String(),
			})

			continue
		}

		if _, excused := suppressed(fn.decl.Doc, CheckConditional); excused {
			continue
		}

		if loop, subject, found := conditionalClaim(fn, asserters); found {
			findings = append(findings, Finding{
				Check:  CheckConditional,
				Test:   name,
				Pos:    fset.Position(loop.Pos()).String(),
				Detail: subject,
			})
		}
	}

	return findings, tests
}

// analyzeFunc reads one function for what it asserts and whom it calls.
func analyzeFunc(fn *ast.FuncDecl, from source) *analyzed {
	out := &analyzed{decl: fn, from: from, calls: map[string]bool{}, handles: map[string]bool{}}

	namesHandles(fn.Type, from.testing, out.handles)
	ast.Inspect(fn.Body, func(node ast.Node) bool {
		if lit, ok := node.(*ast.FuncLit); ok {
			// A subtest's own `t`, and every other closure parameter that is
			// one. Collected across the whole body rather than per-scope: this
			// is a syntax walk, so the worst a shadowed name costs is that a
			// call is read as an assertion, which is the safe direction.
			namesHandles(lit.Type, from.testing, out.handles)
		}

		call, ok := node.(*ast.CallExpr)
		if !ok {
			return true
		}

		if assertion(call) {
			out.asserts = true
		}
		if name, ok := call.Fun.(*ast.Ident); ok {
			out.calls[name.Name] = true
		}

		return true
	})

	// A second pass, because the handles are only complete once the first has
	// finished: a closure declared after a call still names the same `t`.
	ast.Inspect(fn.Body, func(node ast.Node) bool {
		call, ok := node.(*ast.CallExpr)
		if !ok {
			return true
		}
		if handsOverTheHandle(call, out.handles) {
			out.asserts = true
		}

		return true
	})

	return out
}

// namesHandles adds every parameter of a testing-handle type to names.
func namesHandles(signature *ast.FuncType, testing string, names map[string]bool) {
	if signature.Params == nil {
		return
	}

	for _, param := range signature.Params.List {
		if !isTestingHandle(param.Type, testing) {
			continue
		}
		for _, name := range param.Names {
			names[name.Name] = true
		}
	}
}

// isTest reports whether this is a test function the `go test` runner would
// call: named `Test…`, in a `_test.go` file, taking exactly one `*testing.T`.
//
// All three clauses are load-bearing, and the last two were added because the
// first is not enough on its own (Copilot and Codex, #1125). A `Test…` function
// in an *ordinary* file is a helper other packages call — `func TestFixture(t
// testing.TB)` is a real shape — and the runner never invokes it, so judging it
// would fail a build over a function nothing runs. And a `Test…` taking a
// `*testing.B` or a `*testing.F` is likewise not a test.
//
// Benchmarks and fuzz targets are excluded by the name for their own reason: a
// benchmark's job is to run the code, and a fuzz target's assertions live in
// the corpus and the runtime rather than in the function — so both would be
// flagged for doing exactly what they are for.
func (a *analyzed) isTest() bool {
	if !a.from.isTest || !strings.HasPrefix(a.decl.Name.Name, "Test") || a.decl.Recv != nil {
		return false
	}
	if a.decl.Type.Params == nil || len(a.decl.Type.Params.List) != 1 {
		return false
	}

	// The pointer specifically: `func TestX(t testing.T)` does not compile, and
	// `func TestX(t testing.TB)` is a helper the runner will not call.
	star, ok := a.decl.Type.Params.List[0].Type.(*ast.StarExpr)
	if !ok {
		return false
	}

	return named(star.X, a.from.testing, "T")
}

// isTestingHandle reports whether a type is one of testing's handles, under the
// name the file knows that package by.
//
// Wider than [analyzed.isTest] deliberately: this decides which parameters are
// a *handle to fail the test with*, and a helper taking `testing.TB` or a
// benchmark's `*testing.B` can fail one just as well.
func isTestingHandle(expr ast.Expr, testing string) bool {
	if star, ok := expr.(*ast.StarExpr); ok {
		expr = star.X
	}

	return named(expr, testing, "T", "B", "F", "TB")
}

// named reports whether an expression is one of the given type names, qualified
// by the local name of the testing import.
//
// A dot import puts the names in the file's own scope, so there is no selector
// to match and a bare identifier is the whole reference.
func named(expr ast.Expr, testing string, names ...string) bool {
	if testing == "" {
		return false
	}

	switch expr := expr.(type) {
	case *ast.Ident:
		if testing != "." {
			return false
		}

		return slices.Contains(names, expr.Name)

	case *ast.SelectorExpr:
		pkg, ok := expr.X.(*ast.Ident)
		if !ok || pkg.Name != testing {
			return false
		}

		return slices.Contains(names, expr.Sel.Name)
	}

	return false
}

// assertion reports whether a call can fail a test on its own.
func assertion(call *ast.CallExpr) bool {
	selector, ok := call.Fun.(*ast.SelectorExpr)
	if !ok {
		return false
	}

	if pkg, ok := selector.X.(*ast.Ident); ok {
		switch pkg.Name {
		case "assert", "require":
			return true
		}
	}

	// `t.Fatalf`, `t.Errorf`, `t.Fail` — on whatever the handle is called, and
	// on an embedded one, which is why the receiver is not checked. Nothing
	// else in this tree has a method with these names, and reading one as an
	// assertion errs toward silence anyway.
	name := selector.Sel.Name

	return strings.HasPrefix(name, "Fatal") || strings.HasPrefix(name, "Error") ||
		strings.HasPrefix(name, "Fail")
}

// handsOverTheHandle reports whether a call passes a testing handle as an
// argument.
//
// That is a delegation of the power to fail, and this analysis has no types
// with which to follow it across a package. So it counts as an assertion: a
// test that gives its `t` to a conformance helper is asserting whatever that
// helper asserts, and calling it vacuous would be a confident wrong answer
// about the one shape this tree uses most.
func handsOverTheHandle(call *ast.CallExpr, handles map[string]bool) bool {
	for _, arg := range call.Args {
		name, ok := arg.(*ast.Ident)
		if ok && handles[name.Name] {
			return true
		}
	}

	return false
}
