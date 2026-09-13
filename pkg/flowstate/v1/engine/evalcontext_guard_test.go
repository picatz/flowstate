package engine

import (
	"errors"
	"fmt"
	"go/ast"
	"go/importer"
	"go/parser"
	"go/token"
	"go/types"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"slices"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

// The evaluation-context guard for #1723.
//
// Workflow-side evaluation runs on [evalContext], the background context, and
// that is an invariant (see its doc) rather than twenty-one coincidences. Two
// things erode it one site at a time and nothing structural refuses either: a
// new call site written with a bare `context.Background()` that the next reader
// cannot tell from the smell it is everywhere else, and an evaluator that starts
// reading a value from its context — the local driver installs a clock, a
// debugger, a scheduler, the secret runtime and the run observer on the context
// it evaluates under, so the moment an evaluator consults one, the local driver
// sees a value the durable driver never will (invariant 3).
//
// The first check walks the workflow-side files and refuses a
// `context.Background()` or `context.TODO()` call outside [evalContext] itself.
// The second derives the evaluator entry points from those same files — every
// function the durable driver calls with `evalContext()` as an argument — and
// refuses, in the v1 package, a call to a `*FromContext` reader or to `.Value`
// on anything typed context.Context reached from any of them, following the
// calls to other functions and methods of that package that go/types resolves.
// Deriving the set from the call sites rather than a name prefix keeps
// activity-side functions such as `ResolveSecret`, which legitimately reads the
// task runtime off the activity's context, out of the rule.

// workflowSideFiles are the durable driver's files that evaluate
// specification-owned expressions in workflow code.
var workflowSideFiles = []string{"execute.go", "wait.go", "workflow.go", "evalcontext.go"}

func parseGoFile(t *testing.T, fset *token.FileSet, path string) *ast.File {
	t.Helper()
	f, err := parser.ParseFile(fset, path, nil, parser.SkipObjectResolution)
	require.NoError(t, err, "parse %s", path)
	return f
}

// isCall reports whether call is `pkg.name(...)`.
func isCall(call *ast.CallExpr, pkg, name string) bool {
	sel, ok := call.Fun.(*ast.SelectorExpr)
	if !ok {
		return false
	}
	ident, ok := sel.X.(*ast.Ident)
	return ok && ident.Name == pkg && sel.Sel.Name == name
}

// enclosingFunc is the name of the function declaration holding pos.
func enclosingFunc(file *ast.File, pos token.Pos) string {
	for _, decl := range file.Decls {
		fn, ok := decl.(*ast.FuncDecl)
		if ok && fn.Pos() <= pos && pos <= fn.End() {
			return fn.Name.Name
		}
	}
	return ""
}

func TestWorkflowSideEvaluationUsesEvalContext(t *testing.T) {
	t.Parallel()

	fset := token.NewFileSet()
	for _, name := range workflowSideFiles {
		file := parseGoFile(t, fset, name)
		ast.Inspect(file, func(n ast.Node) bool {
			call, ok := n.(*ast.CallExpr)
			if !ok || (!isCall(call, "context", "Background") && !isCall(call, "context", "TODO")) {
				return true
			}
			if enclosingFunc(file, call.Pos()) == "evalContext" {
				return true
			}
			t.Errorf("%s: a bare %s call; workflow-side evaluation runs on evalContext(), which states why",
				fset.Position(call.Pos()), backgroundCallName(call))
			return true
		})
	}
}

func backgroundCallName(call *ast.CallExpr) string {
	if isCall(call, "context", "TODO") {
		return "context.TODO()"
	}
	return "context.Background()"
}

// evaluatorEntryPoints are the v1 functions the durable driver calls with
// evalContext() as an argument, derived from the workflow-side files.
func evaluatorEntryPoints(t *testing.T) []string {
	t.Helper()

	fset := token.NewFileSet()
	var names []string
	for _, name := range workflowSideFiles {
		ast.Inspect(parseGoFile(t, fset, name), func(n ast.Node) bool {
			call, ok := n.(*ast.CallExpr)
			if !ok {
				return true
			}
			passesEvalContext := slices.ContainsFunc(call.Args, func(arg ast.Expr) bool {
				inner, ok := arg.(*ast.CallExpr)
				if !ok {
					return false
				}
				ident, ok := inner.Fun.(*ast.Ident)
				return ok && ident.Name == "evalContext"
			})
			if !passesEvalContext {
				return true
			}
			sel, ok := call.Fun.(*ast.SelectorExpr)
			if !ok {
				return true
			}
			if pkg, ok := sel.X.(*ast.Ident); ok && pkg.Name == "v1" && !slices.Contains(names, sel.Sel.Name) {
				names = append(names, sel.Sel.Name)
			}
			return true
		})
	}
	slices.Sort(names)
	return names
}

func TestEvaluatorsReadNothingFromTheirContext(t *testing.T) {
	t.Parallel()

	entryPoints := evaluatorEntryPoints(t)
	require.NotEmpty(t, entryPoints, "no v1 call passes evalContext(); the derivation is broken, not the tree")
	require.Contains(t, entryPoints, "EvalLoopUntilWithCost", "the derivation missed a known entry point")

	// The v1 package, type-checked, so a call resolves to the one function or
	// method it names rather than to every method sharing the name: without
	// types, `program.Eval(...)` on a CEL program would be followed into
	// `(*Evaluator).Eval` and from there into the local driver's whole
	// execution path, which is the durable driver's activity side and reads
	// the registry off its context on purpose.
	fset := token.NewFileSet()
	paths, err := filepath.Glob("../*.go")
	require.NoError(t, err)
	var files []*ast.File
	for _, path := range paths {
		if strings.HasSuffix(path, "_test.go") {
			continue
		}
		files = append(files, parseGoFile(t, fset, path))
	}
	info := &types.Info{
		Uses:       map[*ast.Ident]types.Object{},
		Selections: map[*ast.SelectorExpr]*types.Selection{},
		Types:      map[ast.Expr]types.TypeAndValue{},
	}
	conf := types.Config{Importer: importer.ForCompiler(fset, "gc", exportLookup(t))}
	pkg, err := conf.Check("github.com/picatz/flowstate/pkg/flowstate/v1", fset, files, info)
	require.NoError(t, err, "type-checking the v1 package")

	// Every function and method with a body, by its object, so a resolved
	// callee can be walked.
	decls := map[types.Object]*ast.FuncDecl{}
	byName := map[string]*ast.FuncDecl{}
	for _, file := range files {
		for _, decl := range file.Decls {
			fn, ok := decl.(*ast.FuncDecl)
			if !ok || fn.Body == nil {
				continue
			}
			if obj := info.Uses[fn.Name]; obj != nil {
				decls[obj] = fn
			}
			if obj := pkg.Scope().Lookup(fn.Name.Name); obj != nil && fn.Recv == nil {
				decls[obj] = fn
				byName[fn.Name.Name] = fn
			}
		}
	}
	// Methods are not in the package scope; find them through their receiver
	// type's method set.
	methods := map[string]bool{}
	for _, file := range files {
		for _, decl := range file.Decls {
			fn, ok := decl.(*ast.FuncDecl)
			if !ok || fn.Body == nil || fn.Recv == nil {
				continue
			}
			for _, obj := range methodObjects(pkg, fn) {
				decls[obj] = fn
				methods[fn.Name.Name] = true
			}
		}
	}

	// Asserted rather than assumed, because nothing below fails when decls
	// holds no method: the walk simply stops descending into one. A resolution
	// that came back empty would narrow this guard to v1's plain functions and
	// still pass green, which is the fail-open direction for a guard whose
	// whole subject is what a call chain reaches. `(*Evaluator).Eval` is named
	// for the same reason EvalLoopUntilWithCost is named above — a count alone
	// is satisfied by resolving the wrong things, and it is the value/pointer
	// receiver pair above that makes that one resolve at all.
	require.NotEmpty(t, methods, "no method of v1 resolved through its receiver's method set, so the walk "+
		"below would skip every method rather than refuse a context read inside one")
	require.Contains(t, methods, "Eval", "the method-set resolution missed (*Evaluator).Eval, "+
		"which is the method this walk most needs to follow")

	for _, name := range entryPoints {
		require.Containsf(t, byName, name, "entry point %s is not a function of v1", name)
	}

	// Walk from each entry point through every function and method it calls
	// in this package, refusing a context read anywhere on the way: a
	// `*FromContext` reader, or `.Value` on anything typed context.Context,
	// whatever it is named. A call through an interface or a function value
	// has no body here and is not followed.
	visited := map[*ast.FuncDecl]bool{}
	var walk func(path string, fn *ast.FuncDecl)
	walk = func(path string, fn *ast.FuncDecl) {
		if visited[fn] {
			return
		}
		visited[fn] = true
		if path == "" {
			path = fn.Name.Name
		} else {
			path += " -> " + fn.Name.Name
		}
		ast.Inspect(fn.Body, func(n ast.Node) bool {
			call, ok := n.(*ast.CallExpr)
			if !ok {
				return true
			}
			var callee types.Object
			switch fun := call.Fun.(type) {
			case *ast.Ident:
				callee = info.Uses[fun]
				if strings.HasSuffix(fun.Name, "FromContext") {
					t.Errorf("%s: %s reads %s, which the local driver's context carries and the durable driver's evalContext() never will (%s)",
						fset.Position(call.Pos()), fn.Name.Name, fun.Name, path)
				}
			case *ast.SelectorExpr:
				if sel, ok := info.Selections[fun]; ok {
					callee = sel.Obj()
					if fun.Sel.Name == "Value" && isContext(sel.Recv()) {
						t.Errorf("%s: %s reads a value off its context, which the durable driver's evalContext() never carries (%s)",
							fset.Position(call.Pos()), fn.Name.Name, path)
					}
				} else {
					callee = info.Uses[fun.Sel]
				}
			}
			if next, ok := decls[callee]; ok {
				walk(path, next)
			}
			return true
		})
	}
	for _, name := range entryPoints {
		walk("", byName[name])
	}
}

// exportLookup resolves an import path to its compiler export data through
// `go list -export`, which the gc importer needs for module dependencies: on
// its own it looks only under GOROOT and GOPATH. One `go list` over the v1
// package's dependency closure, from the build cache after the first run.
func exportLookup(t *testing.T) func(path string) (io.ReadCloser, error) {
	t.Helper()

	cmd := exec.Command("go", "list", "-export", "-deps", "-f", "{{.ImportPath}}={{.Export}}", ".")
	cmd.Dir = ".."
	out, err := cmd.Output()
	if err != nil {
		var exit *exec.ExitError
		if errors.As(err, &exit) {
			err = fmt.Errorf("%w: %s", err, exit.Stderr)
		}
		require.NoError(t, err, "go list -export over the v1 package")
	}
	exports := map[string]string{}
	for _, line := range strings.Split(strings.TrimSpace(string(out)), "\n") {
		path, export, ok := strings.Cut(line, "=")
		if ok && export != "" {
			exports[path] = export
		}
	}
	return func(path string) (io.ReadCloser, error) {
		export, ok := exports[path]
		if !ok {
			return nil, fmt.Errorf("no export data for %q", path)
		}
		return os.Open(export)
	}
}

// methodObjects is the method objects a declaration defines: the one on its
// receiver's named type, found through the type's method set so a pointer
// receiver and a value receiver both resolve.
func methodObjects(pkg *types.Package, fn *ast.FuncDecl) []types.Object {
	recv := fn.Recv.List[0].Type
	if star, ok := recv.(*ast.StarExpr); ok {
		recv = star.X
	}
	if index, ok := recv.(*ast.IndexExpr); ok {
		recv = index.X
	}
	if index, ok := recv.(*ast.IndexListExpr); ok {
		recv = index.X
	}
	ident, ok := recv.(*ast.Ident)
	if !ok {
		return nil
	}
	named, ok := pkg.Scope().Lookup(ident.Name).(*types.TypeName)
	if !ok {
		return nil
	}
	var objs []types.Object
	for _, typ := range []types.Type{named.Type(), types.NewPointer(named.Type())} {
		for selection := range types.NewMethodSet(typ).Methods() {
			if m := selection.Obj(); m.Name() == fn.Name.Name && m.Pos() == fn.Name.Pos() {
				objs = append(objs, m)
			}
		}
	}
	return objs
}

// isContext reports whether a type is context.Context.
func isContext(typ types.Type) bool {
	named, ok := typ.(*types.Named)
	if !ok {
		return false
	}
	obj := named.Obj()
	return obj.Pkg() != nil && obj.Pkg().Path() == "context" && obj.Name() == "Context"
}
