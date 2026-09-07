package engine

import (
	"go/ast"
	"go/parser"
	"go/token"
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
// on a context reached from any of them, following calls to other functions in
// that package. Deriving the set from the call sites rather than a name prefix
// keeps activity-side functions such as `ResolveSecret`, which legitimately
// reads the task runtime off the activity's context, out of the rule.

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
				fset.Position(call.Pos()), types(call))
			return true
		})
	}
}

func types(call *ast.CallExpr) string {
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
	require.Contains(t, entryPoints, "EvalLoopUntil", "the derivation missed a known entry point")

	// Every package-level function in the v1 package, by name, so calls between
	// them can be followed.
	fset := token.NewFileSet()
	paths, err := filepath.Glob("../*.go")
	require.NoError(t, err)
	funcs := map[string]*ast.FuncDecl{}
	for _, path := range paths {
		if strings.HasSuffix(path, "_test.go") || strings.HasSuffix(path, ".pb.go") {
			continue
		}
		for _, decl := range parseGoFile(t, fset, path).Decls {
			if fn, ok := decl.(*ast.FuncDecl); ok && fn.Recv == nil && fn.Body != nil {
				funcs[fn.Name.Name] = fn
			}
		}
	}
	for _, name := range entryPoints {
		require.Containsf(t, funcs, name, "entry point %s is not a package-level function of v1", name)
	}

	// Walk from each entry point through the functions it calls, refusing a
	// context read anywhere on the way. Methods are not followed: a call on a
	// value cannot be resolved without types, and the readers are functions.
	visited := map[string]bool{}
	var walk func(from, name string)
	walk = func(from, name string) {
		if visited[name] {
			return
		}
		visited[name] = true
		fn := funcs[name]
		ast.Inspect(fn.Body, func(n ast.Node) bool {
			call, ok := n.(*ast.CallExpr)
			if !ok {
				return true
			}
			switch fun := call.Fun.(type) {
			case *ast.Ident:
				if strings.HasSuffix(fun.Name, "FromContext") {
					t.Errorf("%s: %s reads %s, which the local driver's context carries and the durable driver's evalContext() never will (reached from %s)",
						fset.Position(call.Pos()), name, fun.Name, from)
				}
				if _, ok := funcs[fun.Name]; ok {
					walk(from, fun.Name)
				}
			case *ast.SelectorExpr:
				if fun.Sel.Name == "Value" && len(call.Args) == 1 {
					if x, ok := fun.X.(*ast.Ident); ok && x.Name == "ctx" {
						t.Errorf("%s: %s reads a value off its context, which the durable driver's evalContext() never carries (reached from %s)",
							fset.Position(call.Pos()), name, from)
					}
				}
			}
			return true
		})
	}
	for _, name := range entryPoints {
		walk(name, name)
	}
}
