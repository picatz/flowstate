package lsp

import (
	"fmt"
	"go/ast"
	"go/parser"
	"go/token"
	"maps"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// Every document is compiled against its own [flowfile.Env] (see document.env),
// and nothing in this package adds a plugin lock's tasks to the process default
// registry. These tests make that enforceable: a call in a non-test file that
// reads the default registry, by naming a registry-reading package-level function
// of flowfile or v1, fails here rather than leaking one workspace's plugin tasks
// into another, or hiding a global read behind a handler that happens to be
// tested only without a lock.

// packageFuncs returns the names of the package-level functions, and of the
// methods of recv (empty for none), declared in the non-test files of dir.
func packageFuncs(t *testing.T, dir, recv string) (funcs, methods map[string]bool) {
	t.Helper()

	funcs, methods = map[string]bool{}, map[string]bool{}
	files, err := filepath.Glob(filepath.Join(dir, "*.go"))
	require.NoError(t, err)
	for _, file := range files {
		if strings.HasSuffix(file, "_test.go") {
			continue
		}
		parsed, err := parser.ParseFile(token.NewFileSet(), file, nil, 0)
		require.NoError(t, err)
		for _, decl := range parsed.Decls {
			fn, ok := decl.(*ast.FuncDecl)
			if !ok || !fn.Name.IsExported() {
				continue
			}
			if fn.Recv == nil {
				funcs[fn.Name.Name] = true

				continue
			}
			if id, ok := fn.Recv.List[0].Type.(*ast.Ident); ok && id.Name == recv {
				methods[fn.Name.Name] = true
			}
		}
	}

	return funcs, methods
}

// zeroEnvDelegators are the package-level functions of flowfile that are the
// zero [flowfile.Env]'s method of the same name: each reads the process default
// registry. They are derived from the package, so a delegator added later is
// covered, and the names below are the floor the derivation must reproduce.
func zeroEnvDelegators(t *testing.T) map[string]bool {
	t.Helper()

	funcs, methods := packageFuncs(t, "..", "Env")
	out := map[string]bool{}
	for name := range methods {
		if funcs[name] {
			out[name] = true
		}
	}
	for _, name := range []string{"Parse", "ParseAt", "ParseFile", "Unmarshal", "ValidateSource", "ValidateSourceAt",
		"ValidateSourceFile", "ParseAndValidateSourceAt", "ParseAndValidateFile", "ParseAndValidateFileAt",
		"Fix", "Format", "Marshal", "StepTaskKeys", "StepKinds", "StepKindList", "IteratorType",
		"IteratorTyper", "ResultEntryOf", "SourceMap"} {
		require.True(t, out[name], "flowfile.%s is no longer a zero-Env delegator; update this guard", name)
	}

	return out
}

// defaultReaders are the functions of v1 that read the default registry: every
// package-level F that has a registry-taking sibling FFrom, plus the registry
// accessors themselves.
func defaultReaders(t *testing.T) map[string]bool {
	t.Helper()

	funcs, _ := packageFuncs(t, "../..", "")
	out := map[string]bool{"DefaultRegistry": true, "LockDefaultRegistry": true}
	for name := range funcs {
		if funcs[name+"From"] {
			out[name] = true
		}
	}
	for _, name := range []string{"LookupTask", "TaskNames", "TaskShapesOutputs"} {
		require.True(t, out[name], "v1.%s has no registry-taking form any more; update this guard", name)
	}

	return out
}

// The calls below read no registry, or only the one handed to them, and are the
// whole of the exceptions. Each is checked to still occur, so this list cannot
// outlive its reason.
//
//   - flowfile.RepinUses hashes the module files a `use:` names; it reads no task
//     registry, which is why it has no Env form.
//   - flowtest.LoadSource, LoadSourceAt, LoadSourceAtWithDefaults and
//     LoadDirDefaultsSource strictly decode a `*.test.yaml` or `testdefaults.yaml`.
//     They resolve no task: the registry-reading code in flowtest (stub binding,
//     swapRegistry) is reached only from its Run entry points, which this package
//     never calls.
//   - flowfile.WithRegistry builds an Env. It is named only where an Env is made:
//     newDocument (the document's own registry), baseEnv (the server's), and
//     stepKindWords (an empty registry, so the shared completion table names no
//     task).
//   - v1.OutputNames and v1.OutputEnumsOf take a registry; nil means the default,
//     so only a nil argument is a read.
//   - v1.DefaultRegistry is called by defaultTasks in pluginscope.go and nowhere
//     else.
var (
	allowedUses = map[string]string{
		"flowfile.RepinUses":                "modules.go",
		"flowtest.LoadSource":               "testfile.go",
		"flowtest.LoadSourceAt":             "testfile.go",
		"flowtest.LoadSourceAtWithDefaults": "testfile.go",
		"flowtest.LoadDirDefaultsSource":    "testfile.go",
		"flowfile.WithRegistry":             "completion.go pluginscope.go store.go",
		"v1.DefaultRegistry":                "pluginscope.go",
	}
	registryTaking = map[string]bool{"OutputNames": true, "OutputEnumsOf": true}
)

// registryViolations are the places in one file that read a registry the
// document did not hand it, and the allowed names the file uses.
func registryViolations(t *testing.T, file string, src []byte, delegators, readers map[string]bool) (violations []string, used map[string]bool) {
	t.Helper()

	used = map[string]bool{}
	fset := token.NewFileSet()
	parsed, err := parser.ParseFile(fset, file, src, 0)
	require.NoError(t, err)
	fail := func(n ast.Node, format string, args ...any) {
		violations = append(violations, fset.Position(n.Pos()).String()+": "+fmt.Sprintf(format, args...))
	}

	var stack []ast.Node
	ast.Inspect(parsed, func(n ast.Node) bool {
		if n == nil {
			stack = stack[:len(stack)-1]

			return true
		}
		stack = append(stack, n)

		switch n := n.(type) {
		case *ast.SelectorExpr:
			pkg, ok := n.X.(*ast.Ident)
			if !ok {
				break
			}
			name := pkg.Name + "." + n.Sel.Name
			if allowedIn, allowed := allowedUses[name]; allowed {
				used[name] = true
				if !slices.Contains(strings.Fields(allowedIn), file) {
					fail(n, "%s may be used only in %q", name, allowedIn)
				}
				if name == "v1.DefaultRegistry" && !insideFunc(stack, "defaultTasks") {
					fail(n, "v1.DefaultRegistry outside defaultTasks")
				}

				break
			}
			switch {
			case pkg.Name == "flowfile" && delegators[n.Sel.Name]:
				fail(n, "%s reads the process default registry; call the document's Env (doc.env) or the server's baseEnv", name)
			case pkg.Name == "v1" && registryTaking[n.Sel.Name]:
				if nilSecondArg(stack) || missingSecondArg(stack) {
					fail(n, "%s with no registry reads the process default", name)
				}
			case pkg.Name == "v1" && readers[n.Sel.Name]:
				fail(n, "%s reads the process default registry; use its From form with the document's registry", name)
			case pkg.Name == "moduleCache" && slices.Contains([]string{"ValidateSourceAt", "ValidateSourceFile", "Interface"}, n.Sel.Name):
				fail(n, "%s compiles against the default registry; use Env.ValidateSourceAtCached, ValidateSourceFileCached or ModuleInterface", name)
			}
		case *ast.CompositeLit:
			if sel, ok := n.Type.(*ast.SelectorExpr); ok {
				if pkg, ok := sel.X.(*ast.Ident); ok && pkg.Name == "flowfile" && sel.Sel.Name == "Env" {
					fail(n, "flowfile.Env{} is the default registry; build an Env with flowfile.WithRegistry where a document or the server is made")
				}
			}
		}

		return true
	})

	return violations, used
}

func TestNoRegistryReaderEscapesTheDocumentEnv(t *testing.T) {
	t.Parallel()

	delegators := zeroEnvDelegators(t)
	readers := defaultReaders(t)

	files, err := filepath.Glob("*.go")
	require.NoError(t, err)
	checked := 0
	seen := map[string]bool{}
	for _, file := range files {
		if strings.HasSuffix(file, "_test.go") {
			continue
		}
		src, err := os.ReadFile(file)
		require.NoError(t, err)
		checked++
		violations, used := registryViolations(t, file, src, delegators, readers)
		for _, v := range violations {
			assert.Fail(t, "registry reader outside the document's Env", v)
		}
		maps.Copy(seen, used)
	}
	require.Positive(t, checked)

	for name := range allowedUses {
		assert.True(t, seen[name], "%s is allowed above but no longer used; remove it", name)
	}
}

// The guard only means something if it can fail: this hands it the calls it
// exists to refuse, and the spellings it must let through.
func TestTheRegistryGuardRefusesTheCallsItIsFor(t *testing.T) {
	t.Parallel()

	delegators := zeroEnvDelegators(t)
	readers := defaultReaders(t)
	scan := func(file, body string) []string {
		v, _ := registryViolations(t, file, []byte("package lsp\nfunc f() {\n"+body+"\n}\n"), delegators, readers)

		return v
	}

	for _, bad := range []string{
		`_, _, _ = flowfile.ParseAt(nil, "")`,
		`_, _ = flowfile.ValidateSourceAt(nil, "")`,
		`_, _ = flowfile.Format(nil, nil)`,
		`_, _ = flowfile.Fix(nil)`,
		`_ = flowfile.StepTaskKeys(nil)`,
		`_, _ = flowfile.IteratorType(nil, "", "")`,
		`_, _ = flowfile.ResultEntryOf(nil, "", "")`,
		`_ = flowfile.StepKindList()`,
		`_, _ = v1.LookupTask("x")`,
		`_ = v1.TaskNames()`,
		`_ = v1.TaskShapesOutputs("x")`,
		`_ = v1.DefaultRegistry()`,
		`_ = v1.LockDefaultRegistry()`,
		`_ = v1.OutputEnumsOf(nil, nil)`,
		`_, _ = v1.OutputNames(nil, nil)`,
		`_ = moduleCache.Interface("")`,
		`_, _ = moduleCache.ValidateSourceAt(nil, "")`,
		`_ = flowfile.Env{}`,
	} {
		assert.NotEmpty(t, scan("hover.go", bad), "the guard let this through: %s", bad)
	}
	for _, ok := range []string{
		`_, _, _ = doc.env.ParseAt(nil, "")`,
		`_ = v1.OutputEnumsOf(nil, doc.tasks)`,
		`_, _ = v1.OutputNames(nil, doc.tasks)`,
		`_, _ = doc.tasks.Lookup("x")`,
		`_, _ = doc.env.ValidateSourceAtCached(moduleCache, nil, "")`,
		`_ = flowfile.WithRegistry(nil)`,
	} {
		// WithRegistry is allowed only in the files that construct an Env.
		file := "hover.go"
		if strings.Contains(ok, "WithRegistry") {
			file = "store.go"
		}
		assert.Empty(t, scan(file, ok), "the guard refused this: %s", ok)
	}
	assert.NotEmpty(t, scan("hover.go", `_ = flowfile.WithRegistry(nil)`), "an Env built outside its construction points")
	assert.NotEmpty(t, scan("store.go", `_ = v1.DefaultRegistry()`), "the default registry named outside defaultTasks")
}

// callOf is the call whose function is the selector at the top of stack.
func callOf(stack []ast.Node) *ast.CallExpr {
	if len(stack) < 2 {
		return nil
	}
	call, ok := stack[len(stack)-2].(*ast.CallExpr)
	if !ok || call.Fun != stack[len(stack)-1] {
		return nil
	}

	return call
}

func nilSecondArg(stack []ast.Node) bool {
	call := callOf(stack)
	if call == nil || len(call.Args) < 2 {
		return false
	}
	id, ok := call.Args[1].(*ast.Ident)

	return ok && id.Name == "nil"
}

func missingSecondArg(stack []ast.Node) bool {
	call := callOf(stack)

	return call != nil && len(call.Args) < 2
}

// insideFunc reports whether the innermost node sits in the declaration of name.
func insideFunc(stack []ast.Node, name string) bool {
	for _, n := range stack {
		if fn, ok := n.(*ast.FuncDecl); ok && fn.Name.Name == name {
			return true
		}
	}

	return false
}
