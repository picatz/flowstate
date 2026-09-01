package main

import (
	"go/ast"
	"go/parser"
	"go/token"
	"testing"

	"github.com/stretchr/testify/require"
)

// The worker's end of the audit wiring (picatz/flowstate#1379).
//
// An end-to-end assertion of `flow worker`'s records needs a Temporal server
// and a task queue with work on it; what the records themselves say is proved
// at the seams instead (pkg/flowstate/v1/enforcementaudit_test.go, and
// engine/enforcementaudit_test.go for the durable driver). What is left for
// this file is the wiring those tests cannot see: that this command builds a
// recorder at all, installs it where the seams look for it, and does so before
// the worker polls.
//
// The analysis is syntactic, in the mould of server/auditseam_test.go: plain
// go/parser over this package's own source, no type checking. It cannot see
// ordering *within* the function beyond the call being present, which is why
// the ordering argument lives in a comment at the call site — but a deleted
// call is the failure that would otherwise ship silently, and this is what
// fails on it.

// TestTheWorkerBuildsAndInstallsTheAuditRecorder: without both calls, every
// worker-side policy decision goes unrecorded while `flow server`'s trail
// looks complete — the exact half-trail #1379 was filed about.
func TestTheWorkerBuildsAndInstallsTheAuditRecorder(t *testing.T) {
	t.Parallel()

	calls := callsIn(t, "runWorker")

	require.Contains(t, calls, "startAudit",
		"runWorker builds no audit recorder; `flow worker` would poll with nowhere to record")
	require.Contains(t, calls, "SetDefaultEnforcementAuditor",
		"runWorker builds a recorder and never installs it, so v1.EnforcementAuditorIn finds "+
			"none: an activity the Temporal SDK invokes carries none of this command's context")
	require.Contains(t, calls, "flushAudit",
		"a drained worker's last decisions must reach the sink before the process leaves")
}

// TestTheAuditPostureIsOnEveryDeploymentCommandAndNoRehearsal: one flag, one
// meaning. A deployment states its posture once; a rehearsal has no posture to
// state because it installs no recorder at all.
func TestTheAuditPostureIsOnEveryDeploymentCommandAndNoRehearsal(t *testing.T) {
	t.Parallel()

	for _, path := range []string{"worker", "server", "server dev", "mcp serve"} {
		require.NotNil(t, findCommand(t, path).Flags().Lookup(auditRequiredFlag),
			"`flow %s` runs a deployment and does not declare --%s", path, auditRequiredFlag)
	}

	for _, path := range []string{"run local", "test", "task run"} {
		require.Nil(t, findCommand(t, path).Flags().Lookup(auditRequiredFlag),
			"`flow %s` is a rehearsal and records nothing, so a posture flag there would promise "+
				"a trail that is never written — see pkg/flowstate/v1/audit's package doc", path)
	}
}

// callsIn returns the names of the functions and methods called in the named
// top-level function of this package.
func callsIn(t *testing.T, function string) map[string]bool {
	t.Helper()

	fset := token.NewFileSet()
	pkg, err := parser.ParseDir(fset, ".", nil, 0)
	require.NoError(t, err)

	calls := map[string]bool{}
	found := false

	for _, files := range pkg {
		for _, file := range files.Files {
			for _, decl := range file.Decls {
				fn, ok := decl.(*ast.FuncDecl)
				if !ok || fn.Name.Name != function || fn.Recv != nil {
					continue
				}

				found = true
				ast.Inspect(fn.Body, func(n ast.Node) bool {
					call, ok := n.(*ast.CallExpr)
					if !ok {
						return true
					}
					switch fun := call.Fun.(type) {
					case *ast.Ident:
						calls[fun.Name] = true
					case *ast.SelectorExpr:
						calls[fun.Sel.Name] = true
					}
					return true
				})
			}
		}
	}

	require.True(t, found, "this package declares no function named %s", function)

	return calls
}
