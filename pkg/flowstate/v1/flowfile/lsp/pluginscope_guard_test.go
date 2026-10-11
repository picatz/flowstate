package lsp

import (
	"go/ast"
	"go/parser"
	"go/token"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// guardedSelectors are the calls that compile, parse or validate a Flowfile, or
// otherwise read the default task registry, keyed by package. In this package
// they are legal only inside the function literal handed to underDoc or
// underBase: that is what makes a locked document's tasks visible to them and
// invisible to everyone else (see underLock).
// (flowfile.StepTaskKeys is reached only through parseFlowfile, which is guarded.)
var guardedSelectors = map[string]map[string]bool{
	"flowfile": set("Parse", "ParseAt", "ParseFile", "Unmarshal", "ValidateSource", "ValidateSourceAt",
		"ValidateSourceFile", "ParseAndValidateSourceAt", "ParseAndValidateFile", "ParseAndValidateFileAt",
		"Fix", "Format", "Marshal", "RepinUses", "IteratorType", "IteratorTyper", "ResultEntryOf"),
	"flowtest": set("LoadSource", "LoadSourceAt", "LoadSourceAtWithDefaults", "LoadDirDefaultsSource"),
	"v1":       set("TaskShapesOutputs", "LookupTask", "TaskNames"),
}

// nilRegistrySelectors read the default registry only when given a nil one.
var nilRegistrySelectors = map[string]map[string]bool{
	"v1": set("OutputNames", "OutputEnumsOf"),
}

func set(names ...string) map[string]bool {
	m := map[string]bool{}
	for _, n := range names {
		m[n] = true
	}

	return m
}

// TestNoCompilerCallEscapesTheRegistryScope makes the invariant of
// pluginscope.go enforceable: a new call in this package that compiles a
// document, or reads the default registry, outside underDoc/underBase fails
// here rather than leaking one workspace's plugin tasks into another.
func TestNoCompilerCallEscapesTheRegistryScope(t *testing.T) {
	t.Parallel()

	// The file that implements the scope.
	exempt := set("pluginscope.go")

	files, err := filepath.Glob("*.go")
	require.NoError(t, err)
	checked := 0
	for _, file := range files {
		if strings.HasSuffix(file, "_test.go") || exempt[file] {
			continue
		}
		src, err := os.ReadFile(file)
		require.NoError(t, err)
		fset := token.NewFileSet()
		parsed, err := parser.ParseFile(fset, file, src, 0)
		require.NoError(t, err)
		checked++

		var stack []ast.Node
		ast.Inspect(parsed, func(n ast.Node) bool {
			if n == nil {
				stack = stack[:len(stack)-1]
				return true
			}
			stack = append(stack, n)

			what := ""
			switch n := n.(type) {
			case *ast.SelectorExpr:
				pkg, ok := n.X.(*ast.Ident)
				if !ok {
					break
				}
				switch {
				case guardedSelectors[pkg.Name][n.Sel.Name]:
					what = pkg.Name + "." + n.Sel.Name
				case nilRegistrySelectors[pkg.Name][n.Sel.Name] && nilSecondArg(stack):
					what = pkg.Name + "." + n.Sel.Name + "(…, nil)"
				case pkg.Name == "moduleCache":
					what = "moduleCache." + n.Sel.Name
				}
			case *ast.CallExpr:
				if id, ok := n.Fun.(*ast.Ident); ok && id.Name == "parseFlowfile" {
					what = "parseFlowfile"
				}
			}
			if what != "" && !insideScope(stack) {
				assert.Fail(t, "unscoped registry reader", "%s: %s is outside underDoc/underBase, so it reads "+
					"whatever the default registry holds, including another document's lock tasks",
					fset.Position(n.Pos()), what)
			}

			return true
		})
	}
	require.Positive(t, checked)
}

func nilSecondArg(stack []ast.Node) bool {
	if len(stack) < 2 {
		return false
	}
	call, ok := stack[len(stack)-2].(*ast.CallExpr)
	if !ok || len(call.Args) < 2 {
		return false
	}
	id, ok := call.Args[1].(*ast.Ident)

	return ok && id.Name == "nil"
}

// insideScope reports whether the innermost node sits in a function literal
// passed directly to underDoc or underBase.
func insideScope(stack []ast.Node) bool {
	for i := len(stack) - 1; i > 0; i-- {
		lit, ok := stack[i].(*ast.FuncLit)
		if !ok {
			continue
		}
		call, ok := stack[i-1].(*ast.CallExpr)
		if !ok {
			continue
		}
		fn := call.Fun
		if idx, ok := fn.(*ast.IndexExpr); ok {
			fn = idx.X
		}
		if id, ok := fn.(*ast.Ident); ok && (id.Name == "underDoc" || id.Name == "underBase") {
			for _, arg := range call.Args {
				if arg == lit {
					return true
				}
			}
		}
	}

	return false
}
