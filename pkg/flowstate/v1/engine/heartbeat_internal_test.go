package engine

import (
	"go/ast"
	"go/parser"
	"go/token"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

// TestEveryTaskActivityHeartbeats is the structural test that the first version of
// this feature needed and did not have.
//
// `HeartbeatTimeout` is set on the options *every* activity runs under, so it is
// not opt-in: an entry point that does not heartbeat is one whose healthy
// long-running requests are failed at thirty seconds and retried. Wiring three of
// the five task entry points therefore did not deliver three-fifths of a feature —
// it broke the other two, and broke them in exactly the case the timeout exists to
// serve, since a task needing authority is a task talking to something that
// authenticates it.
//
// A count would not have caught it and neither would a test of any one path. What
// catches it is the relationship: the set of functions registered as task
// activities, and the set that install a heartbeat, have to be the same set. So
// both are read out of the source and compared.
//
// The registration list is read from versioning.go rather than written out here,
// because a hand-copied list is exactly the thing that would not be updated by the
// change this is guarding against.
func TestEveryTaskActivityHeartbeats(t *testing.T) {
	t.Parallel()

	dir, err := os.Getwd()
	require.NoError(t, err)

	registered := registeredActivities(t, filepath.Join(dir, "versioning.go"))

	// WorkflowVars is registered and deliberately absent from the expectation
	// below: it evaluates a workflow's `vars:` block, which is CEL over values
	// already in hand — no network, no plugin, nothing that can take thirty
	// seconds. Naming it here rather than filtering it silently, so that a future
	// activity which *can* be slow is a visible decision rather than an omission.
	require.Contains(t, registered, "WorkflowVars")
	delete(registered, "WorkflowVars")

	// CheckPlugins is absent for the same kind of reason: it compares a run's
	// pinned plugin tuples against the catalog this worker was registered with.
	// No network, no plugin process, nothing it waits on. Named here rather than
	// filtered silently, so an activity that can be slow cannot arrive quietly.
	require.Contains(t, registered, "CheckPlugins")
	delete(registered, "CheckPlugins")
	// CheckTaskCapabilities is the same shape: a bounded in-memory set comparison
	// against the names frozen when this worker was registered.
	require.Contains(t, registered, "CheckTaskCapabilities")
	delete(registered, "CheckTaskCapabilities")

	require.NotEmpty(t, registered,
		"no activities were found in versioning.go, so this asserted nothing")

	heartbeating := functionsCalling(t, dir, "withHeartbeat")

	for name := range registered {
		require.Contains(t, heartbeating, name,
			"activity %q is registered but never calls withHeartbeat; every activity "+
				"runs under a HeartbeatTimeout, so one that does not heartbeat has its "+
				"healthy long requests failed and retried", name)
	}
}

// registeredActivities returns the names passed to RegisterActivity and
// RegisterActivityWithOptions in a file.
func registeredActivities(t *testing.T, path string) map[string]bool {
	t.Helper()

	file, err := parser.ParseFile(token.NewFileSet(), path, nil, 0)
	require.NoError(t, err)

	names := map[string]bool{}
	ast.Inspect(file, func(node ast.Node) bool {
		call, ok := node.(*ast.CallExpr)
		if !ok || len(call.Args) == 0 {
			return true
		}

		selector, ok := call.Fun.(*ast.SelectorExpr)
		if !ok || !strings.HasPrefix(selector.Sel.Name, "RegisterActivity") {
			return true
		}

		// `Task` or `authorized.TaskAuthorized` — the last identifier is the name
		// either way, which is also how the SDK derives it when no explicit name is
		// given.
		switch arg := call.Args[0].(type) {
		case *ast.Ident:
			names[arg.Name] = true
		case *ast.SelectorExpr:
			names[arg.Sel.Name] = true
		}

		return true
	})

	return names
}

// functionsCalling returns the names of functions and methods in a package's
// non-test files whose body calls the named function.
func functionsCalling(t *testing.T, dir, callee string) map[string]bool {
	t.Helper()

	entries, err := os.ReadDir(dir)
	require.NoError(t, err)

	found := map[string]bool{}
	for _, entry := range entries {
		name := entry.Name()
		if entry.IsDir() || !strings.HasSuffix(name, ".go") || strings.HasSuffix(name, "_test.go") {
			continue
		}

		file, err := parser.ParseFile(token.NewFileSet(), filepath.Join(dir, name), nil, 0)
		require.NoError(t, err)

		for _, decl := range file.Decls {
			fn, ok := decl.(*ast.FuncDecl)
			if !ok {
				continue
			}

			ast.Inspect(fn, func(node ast.Node) bool {
				call, ok := node.(*ast.CallExpr)
				if !ok {
					return true
				}
				if ident, ok := call.Fun.(*ast.Ident); ok && ident.Name == callee {
					found[fn.Name.Name] = true
				}

				return true
			})
		}
	}

	return found
}
