package main

import (
	"go/ast"
	"go/parser"
	"go/token"
	"io/fs"
	"maps"
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

		case ".worktrees", ".claude":
			// Where an agent's isolated checkout materializes, both of them
			// ignored by `.gitignore:46-53` — and ignored here for the reason
			// that decides it there: each is a *copy of this tree* holding
			// somebody else's unfinished work. Walking into one lets a
			// half-written test in a checkout that is not this one, not
			// tracked, and not present in CI decide whether this checkout
			// passes: the shared-tree failure CLAUDE.md warns about, arriving
			// through a checker (Codex, #1125).
			//
			// `.claude` whole rather than `.claude/worktrees`, because the
			// directory is the agent's own — configuration and scratch — and
			// holds no source belonging to this module either way.
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
		testing := testingImport(file)
		packages[name] = append(packages[name], source{
			aliases: handleAliases(file, testing),
			file:    file,
			testify: testifyImports(file),
			// Only a `_test.go` file holds tests the runner will call. An
			// ordinary file may perfectly well declare `func TestFixture(t
			// testing.TB)` as a helper for other packages to use, and judging
			// that as a test would fail a build over a function `go test`
			// never invokes (Codex, #1125).
			isTest:  strings.HasSuffix(entry.Name(), "_test.go"),
			testing: testing,
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

	// testify are the local names this file knows testify's assert and require
	// packages by.
	testify map[string]bool

	// aliases maps a local name declared as an alias of a testing handle —
	// `type T = testing.T` — to which handle it names. The runner honours such
	// a declaration and a match on the qualified name alone does not.
	aliases map[string]string
}

// testifyImports are the names a file knows testify's assertion packages by.
//
// Resolved rather than assumed, because the bare identifiers `assert` and
// `require` are ordinary Go names that a test may perfectly well bind to
// something else — and a call on one of those was reading as an assertion, so a
// vacuous test holding an unrelated `require.Load()` walked past a check that
// fails builds (Codex, #1125). Matched on the import path, so an alias works
// and a local variable does not.
func testifyImports(file *ast.File) map[string]bool {
	names := map[string]bool{}

	for _, imported := range file.Imports {
		if imported.Path == nil {
			continue
		}

		path := strings.Trim(imported.Path.Value, `"`)
		if path != "github.com/stretchr/testify/assert" && path != "github.com/stretchr/testify/require" {
			continue
		}

		if imported.Name != nil {
			names[imported.Name.Name] = true

			continue
		}
		names[path[strings.LastIndexByte(path, '/')+1:]] = true
	}

	return names
}

// handleAliases are the local names a file declares as an alias of a testing
// handle.
//
// `type T = testing.T` is legal, and `go test` runs `func TestAliased(t *T)`
// exactly as it runs any other — so a signature match on the qualified name
// alone misses a real test entirely, neither reporting nor counting it
// (Codex, #1125). Aliases only: `type T testing.T` is a *new* type the runner
// will not accept, so it is correctly not one of these.
func handleAliases(file *ast.File, testing string) map[string]string {
	names := map[string]string{}

	ast.Inspect(file, func(node ast.Node) bool {
		spec, ok := node.(*ast.TypeSpec)
		if !ok || !spec.Assign.IsValid() {
			return true
		}

		// Which handle, not merely that it is one: the runner calls a `Test…`
		// taking `*testing.T` and nothing else, so an alias of `testing.B` must
		// not make one look like a test.
		for _, handle := range []string{"T", "B", "F", "TB"} {
			if named(spec.Type, testing, handle) {
				names[spec.Name.Name] = handle

				break
			}
		}

		return true
	})

	return names
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

	// testify are the names its file knows testify's packages by, less any this
	// function binds for itself.
	testify map[string]bool

	// shadowed are the names this function binds, so a builtin it re-declares
	// is not the builtin.
	shadowed map[string]bool
}

// analyzePackage reports the findings in one parsed package, and how many test
// functions it looked at.
func analyzePackage(fset *token.FileSet, files []source) ([]Finding, int) {
	// The names this package holds a testing handle under, beyond a
	// function's own parameter: a struct field of that type, which is how a
	// test client that wraps `t` reaches it (`c.t.Fatalf(…)`). Collected
	// package-wide because the wrapper and the test that uses it are ordinarily
	// in the same package, and a syntax walk has no types with which to follow
	// a field to its declaration.
	fields := map[string]bool{}
	for _, from := range files {
		collectHandleFields(from, fields)
	}

	// Type aliases are scoped to the *package*, not the file that declares
	// them: `type T = testing.T` in one file and `func TestCrossFile(t *T)` in
	// another is ordinary Go and `go test` runs it. Collected per file, because
	// resolving one needs that file's own name for the `testing` import, then
	// unioned — which is where they were being read from before, leaving a test
	// in the second file neither counted nor reported (Codex, #1126).
	aliases := map[string]string{}
	for _, from := range files {
		for name, handle := range from.aliases {
			aliases[name] = handle
		}
	}
	for i := range files {
		files[i].aliases = aliases
	}

	// Keyed by name and holding *every* declaration of it, because two
	// build-tagged files in one package may each declare `TestPlatform` — and
	// storing one per name kept whichever the directory listing reached last,
	// so the count was short and a vacuous variant could be hidden by an
	// asserting one, depending on filename order (Codex, #1125). This walk
	// groups by the `package` clause and ignores build tags on purpose, which
	// is what makes the collision reachable here where it is not in a build.
	functions := map[string][]*analyzed{}

	// Methods are analysed too, and kept apart from the plain functions.
	//
	// A test that delegates its checks to a method on a type holding the
	// handle — `s := suite{}; s.t = t; s.check(got)` — reaches its assertion
	// through a declaration that used to be dropped on the floor: not
	// propagated as a helper, because the call is a selector rather than a bare
	// name, and not a handoff either. The repository-wide check then rejected a
	// test that asserts perfectly well (Codex, #1125).
	//
	// Keyed by method name, with every receiver's collapsed together. Two types
	// may declare `check` and only one of them assert, and this will read a
	// call to either as asserting — which is the safe direction for a check
	// that fails a build, and the same trade [handsOverTheHandle] makes.
	methods := map[string][]*analyzed{}

	for _, from := range files {
		for _, decl := range from.file.Decls {
			fn, ok := decl.(*ast.FuncDecl)
			if !ok || fn.Body == nil {
				continue
			}

			analysed := analyzeFunc(fn, from, fields)
			if fn.Recv != nil {
				methods[fn.Name.Name] = append(methods[fn.Name.Name], analysed)

				continue
			}
			functions[fn.Name.Name] = append(functions[fn.Name.Name], analysed)
		}
	}

	// A helper that asserts makes its callers assert, and a helper calling a
	// helper is ordinary here. Iterated to a fixpoint rather than recursed,
	// because mutual recursion between two test helpers is legal Go and a
	// naive walk of it does not come back.
	var every []*analyzed
	for _, sameName := range functions {
		every = append(every, sameName...)
	}
	for _, sameName := range methods {
		every = append(every, sameName...)
	}

	// Any declaration of the name asserting makes a call to it assert, which is
	// the same safe direction the method collapsing takes: erring toward
	// silence for a check that fails a build.
	asserted := func(name string) bool {
		for _, called := range functions[name] {
			if called.asserts {
				return true
			}
		}
		for _, called := range methods[name] {
			if called.asserts {
				return true
			}
		}

		return false
	}

	for range len(every) + 1 {
		changed := false
		for _, fn := range every {
			if fn.asserts {
				continue
			}
			for callee := range fn.calls {
				if asserted(callee) {
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
	for name := range functions {
		if asserted(name) {
			asserters[name] = true
		}
	}
	for name := range methods {
		if asserted(name) {
			asserters[name] = true
		}
	}

	var findings []Finding
	tests := 0

	// Every declaration judged on its own, so a vacuous build-tagged variant is
	// reported even where its sibling asserts.
	for name, sameName := range functions {
		for _, fn := range sameName {
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

			if loop, subject, found := conditionalClaim(fset, fn, asserters); found {
				findings = append(findings, Finding{
					Check:  CheckConditional,
					Test:   name,
					Pos:    fset.Position(loop.Pos()).String(),
					Detail: subject,
				})
			}
		}
	}

	return findings, tests
}

// analyzeFunc reads one function for what it asserts and whom it calls.
func analyzeFunc(fn *ast.FuncDecl, from source, fields map[string]bool) *analyzed {
	out := &analyzed{
		decl:     fn,
		from:     from,
		calls:    map[string]bool{},
		handles:  map[string]bool{},
		shadowed: map[string]bool{},

		// Copied, because the shadowing below removes from it and the file's
		// own map is shared with every other function in it.
		testify: maps.Clone(from.testify),
	}
	if out.testify == nil {
		out.testify = map[string]bool{}
	}

	for name := range fields {
		out.handles[name] = true
	}
	// Before the walk below, which is the whole of it: these names decide what
	// that walk will read as an assertion, and the first version computed them
	// afterwards — so nothing was shadowed at the moment it mattered and the
	// test written to prove it failed. Caught by that test rather than by
	// reading, which is the argument for having written it.
	//
	// `import req "…/require"` followed by `req := loader{}` makes `req.Load()`
	// an ordinary method call, and `panic := func(any) {}` makes `panic("x")`
	// return normally. Both were being read as assertions, so a test with no
	// claim in it passed a check that fails builds (Codex, #1126).
	//
	// Collected for the whole function rather than per scope, because a
	// syntactic walk has no scopes: the cost is that a name shadowed in one
	// block stops counting in the others too, which errs toward *reporting* —
	// the direction that gets looked at rather than the one that hides.
	for name := range shadowedBy(fn) {
		delete(out.testify, name)
		out.shadowed[name] = true
	}

	namesHandles(fn.Type, from, out.handles)
	ast.Inspect(fn.Body, func(node ast.Node) bool {
		if lit, ok := node.(*ast.FuncLit); ok {
			// A subtest's own `t`, and every other closure parameter that is
			// one. Collected across the whole body rather than per-scope: this
			// is a syntax walk, so the worst a shadowed name costs is that a
			// call is read as an assertion, which is the safe direction.
			namesHandles(lit.Type, from, out.handles)
		}

		call, ok := node.(*ast.CallExpr)
		if !ok {
			return true
		}

		if assertion(call, out.handles, out.testify, out.shadowed) {
			out.asserts = true
		}
		switch callee := call.Fun.(type) {
		case *ast.Ident:
			out.calls[callee.Name] = true
		case *ast.SelectorExpr:
			// `s.check(got)`. Resolved against this package's own methods
			// below, so a selector into another package contributes a name
			// nothing matches rather than a false assertion.
			out.calls[callee.Sel.Name] = true
		}

		return true
	})

	// A second pass, because the handles are only complete once the first has
	// finished: a closure declared after a call still names the same `t`.
	ast.Inspect(fn.Body, func(node ast.Node) bool {
		switch node := node.(type) {
		case *ast.CallExpr:
			if handsOverTheHandle(node.Args, out.handles) {
				out.asserts = true
			}

		case *ast.AssignStmt:
			// `h.t = t` gives the handle away exactly as passing or storing it
			// does. Without this a test that assigns its handle into a helper
			// and then calls a method on it looks like it kept the handle to
			// itself and did nothing with it.
			if handsOverTheHandle(node.Rhs, out.handles) {
				out.asserts = true
			}

		case *ast.CompositeLit:
			// `&conn{t: t}` hands the handle over exactly as `f(t)` does — the
			// value now holds the power to fail the test, and this analysis has
			// no types with which to follow it into the methods that use it.
			// It is the shape a test client takes (`cmd/flow/dap_test.go`'s
			// `&dapConn{t: t, …}`), so leaving it out would call every test
			// driven through one vacuous.
			for _, element := range node.Elts {
				value := element
				if keyed, ok := element.(*ast.KeyValueExpr); ok {
					value = keyed.Value
				}
				if handsOverTheHandle([]ast.Expr{value}, out.handles) {
					out.asserts = true
				}
			}
		}

		return true
	})

	return out
}

// collectHandleFields adds every struct field of a testing-handle type to
// names.
func collectHandleFields(from source, names map[string]bool) {
	ast.Inspect(from.file, func(node ast.Node) bool {
		structure, ok := node.(*ast.StructType)
		if !ok || structure.Fields == nil {
			return true
		}

		for _, field := range structure.Fields.List {
			if !isTestingHandle(field.Type, from) {
				continue
			}
			for _, name := range field.Names {
				names[name.Name] = true
			}
		}

		return true
	})
}

// namesHandles adds every parameter of a testing-handle type to names.
func namesHandles(signature *ast.FuncType, from source, names map[string]bool) {
	if signature.Params == nil {
		return
	}

	for _, param := range signature.Params.List {
		if !isTestingHandle(param.Type, from) {
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
	// `testing.T` specifically. An alias of `testing.B` is a benchmark's
	// handle, and the runner does not call a `Test…` taking one — which is why
	// [handleAliases] records *which* handle each alias names rather than only
	// that it is one.
	if alias, ok := star.X.(*ast.Ident); ok && a.from.aliases[alias.Name] == "T" {
		return true
	}

	return named(star.X, a.from.testing, "T")
}

// isTestingHandle reports whether a type is one of testing's handles, under the
// name the file knows that package by.
//
// Wider than [analyzed.isTest] deliberately: this decides which parameters are
// a *handle to fail the test with*, and a helper taking `testing.TB` or a
// benchmark's `*testing.B` can fail one just as well.
func isTestingHandle(expr ast.Expr, from source) bool {
	if star, ok := expr.(*ast.StarExpr); ok {
		expr = star.X
	}
	if alias, ok := expr.(*ast.Ident); ok && from.aliases[alias.Name] != "" {
		return true
	}

	return named(expr, from.testing, "T", "B", "F", "TB")
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
//
// The receiver is checked, and the first version of this did not check it —
// which is a hole in the *fatal* gate rather than a tidiness. `t.Errorf` is an
// assertion; `err.Error()` and `fmt.Errorf(…)` are not, and a prefix match on
// the method name alone cannot tell them apart. Since `err.Error()` appears in
// almost every test in this tree, a test that made no claim at all would have
// been read as asserting and walked straight past the check that exists to
// catch it. The comment where that rule used to live said "nothing else in
// this tree has a method with these names", which was not true when it was
// written (Codex, #1125).
//
// So a handle it is: the identifier the function knows its `*testing.T` by, or
// a field of that type reached through one selector — `c.t.Fatalf(…)`, the
// shape a test client wrapping the handle uses.
func assertion(call *ast.CallExpr, handles, testify, shadowed map[string]bool) bool {
	// `if got != want { panic("mismatch") }` is a manual assertion, and the
	// testing runner turns the panic into a failure. Checked before the
	// selector below, because `panic` is a builtin called on nothing — so a
	// test written that way was being reported fatally unasserted while doing
	// the very thing the check looks for (Codex, #1125).
	if name, ok := call.Fun.(*ast.Ident); ok && name.Name == "panic" && !shadowed["panic"] {
		return true
	}

	selector, ok := call.Fun.(*ast.SelectorExpr)
	if !ok {
		return false
	}

	if pkg, ok := selector.X.(*ast.Ident); ok && testify[pkg.Name] {
		return true
	}

	name := selector.Sel.Name
	if !strings.HasPrefix(name, "Fatal") && !strings.HasPrefix(name, "Error") &&
		!strings.HasPrefix(name, "Fail") {
		return false
	}

	return isHandle(selector.X, handles)
}

// shadowedBy are the names a function binds: its receiver, its parameters, its
// named results, and everything its body declares.
//
// The signature half was missing at first, and the hole it left is worth
// recording because it is the same defect the body half was written to close,
// one step out. In a file importing testify as `req`, a helper
// `func helper(req loader) { req.Load() }` binds `req` in its *signature* — so
// the helper read as asserting, and the propagation then marked every test
// calling it as asserting too. One un-shadowed parameter hid an arbitrary
// number of vacuous tests (Codex, #1126).
//
// Deliberately generous about what counts as binding and deliberately blind to
// where the binding is in scope: this is a syntax walk. What it is for is
// deciding when a name can no longer be trusted to mean the package or builtin
// it usually means, and over-reporting there costs a finding somebody looks at
// rather than a claim nobody checks.
func shadowedBy(fn *ast.FuncDecl) map[string]bool {
	names := map[string]bool{}

	add := func(expr ast.Expr) {
		if name, ok := expr.(*ast.Ident); ok && name.Name != "_" {
			names[name.Name] = true
		}
	}

	fields := func(list *ast.FieldList) {
		if list == nil {
			return
		}
		for _, field := range list.List {
			for _, name := range field.Names {
				add(name)
			}
		}
	}

	fields(fn.Recv)
	fields(fn.Type.Params)
	fields(fn.Type.Results)

	if fn.Body == nil {
		return names
	}

	ast.Inspect(fn.Body, func(node ast.Node) bool {
		switch node := node.(type) {
		case *ast.AssignStmt:
			if node.Tok == token.DEFINE {
				for _, left := range node.Lhs {
					add(left)
				}
			}

		case *ast.ValueSpec:
			for _, name := range node.Names {
				add(name)
			}

		case *ast.TypeSpec:
			add(node.Name)

		case *ast.FuncLit:
			if node.Type.Params == nil {
				return true
			}
			for _, param := range node.Type.Params.List {
				for _, name := range param.Names {
					add(name)
				}
			}

		case *ast.RangeStmt:
			add(node.Key)
			add(node.Value)
		}

		return true
	})

	return names
}

// isHandle reports whether an expression is something this function can fail
// the test through.
func isHandle(expr ast.Expr, handles map[string]bool) bool {
	switch expr := expr.(type) {
	case *ast.Ident:
		return handles[expr.Name]

	case *ast.SelectorExpr:
		// `c.t.Fatalf(…)`: the field a wrapper holds its handle in. Matched by
		// the field's *name*, collected from every struct in the package that
		// declares a field of a testing-handle type — so it is the package's
		// own vocabulary rather than a guess at what people call it.
		return handles[expr.Sel.Name]
	}

	return false
}

// handsOverTheHandle reports whether any of these expressions is a testing
// handle being given away.
//
// That is a delegation of the power to fail, and this analysis has no types
// with which to follow it across a package. So it counts as an assertion: a
// test that gives its `t` to a conformance helper is asserting whatever that
// helper asserts, and calling it vacuous would be a confident wrong answer
// about the one shape this tree uses most.
func handsOverTheHandle(values []ast.Expr, handles map[string]bool) bool {
	for _, value := range values {
		name, ok := value.(*ast.Ident)
		if ok && handles[name.Name] {
			return true
		}
	}

	return false
}
