package main

import (
	"go/ast"
	"go/parser"
	"go/token"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"

	v1 "github.com/picatz/flowstate/pkg/flowstate/v1"
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

// TestServerDevInstallsTheAuditorForItsEmbeddedWorker: `flow server dev` runs a
// real worker under this command's own task, egress, secret and credential
// policies, so an auditor installed only in `runWorker` would leave its
// --audit-required flag gating half of what one process decides.
func TestServerDevInstallsTheAuditorForItsEmbeddedWorker(t *testing.T) {
	t.Parallel()

	calls := callsIn(t, "runServerDev")

	require.Contains(t, calls, "SetDefaultEnforcementAuditor",
		"runServerDev passes its recorder to server.WithAudit only, so its embedded worker's "+
			"dispatches, secret reads and dials reach no sink")
}

// TestServerDevStopsItsWorkerBeforeShuttingDownTheAuditSinks: a draining
// worker still records (Codex, picatz/flowstate#1394).
//
// worker.Stop waits for the activities already running, and one of those can
// reach a task-dispatch, secret, egress or credential seam on its way out.
// With flushAudit already run, that record meets a shut-down log processor: it
// vanishes under the best-effort posture, and under --audit-required it fails
// the activity with the sink's own error. The fix is an explicit stop before
// the flush, and this is what fails if it is deleted and the worker is left to
// its defer — which runs after this function, and therefore after the flush.
//
// What this exercises and what it cannot: the analysis is syntactic, so it
// sees the call order written in runServerDev and not the runtime order.
// Deferred calls are deliberately excluded, so `defer stopWorker()` alone does
// not satisfy it. That worker.Stop blocks until the drain finishes is
// Temporal's own contract, cited at the call site rather than asserted here.
func TestServerDevStopsItsWorkerBeforeShuttingDownTheAuditSinks(t *testing.T) {
	t.Parallel()

	calls := immediateCallOrderIn(t, "runServerDev")

	stop := indexOfCall(calls, "stopWorker")
	flush := indexOfCall(calls, "flushAudit")

	require.NotEqual(t, -1, stop,
		"runServerDev leaves its worker to a deferred stop, which runs after flushAudit: "+
			"an activity draining through an enforcement seam would record into a shut-down sink")
	require.NotEqual(t, -1, flush, "runServerDev no longer flushes the audit trail at all")
	require.Less(t, stop, flush,
		"the embedded worker must stop, and drain, while its audit sinks are still open")
}

// TestWorkerStopsBeforeShuttingDownTheAuditSinks holds the same ordering for
// the deployment worker. This path already stopped explicitly before flushing;
// pin it so a future cleanup cannot leave Stop to main's post-command flush.
func TestWorkerStopsBeforeShuttingDownTheAuditSinks(t *testing.T) {
	t.Parallel()

	calls := immediateCallOrderIn(t, "runWorker")

	stop := lastIndexOfCall(calls, "Stop")
	flush := indexOfCall(calls, "flushAudit")

	require.NotEqual(t, -1, stop, "runWorker never stops and drains its worker")
	require.NotEqual(t, -1, flush, "runWorker no longer flushes the audit trail")
	require.Less(t, stop, flush,
		"the worker must stop, and drain, while its audit sinks are still open")
}

// TestTheDevWorkerIsGivenTimeToDrain is the other half of the ordering above,
// and the half that makes it worth anything (Codex, picatz/flowstate#1394).
//
// Stopping the worker before the audit sinks close only helps if Stop waits.
// The SDK's zero WorkerStopTimeout does not mean "wait forever": the drain
// races a timer that has already expired, so Stop returns with activities
// still running — see [v1.DefaultWorkerStopTimeout], which says the same thing
// about `flow worker`. Left unset here, the stop would return immediately, the
// sinks would close, and a draining activity's records would meet a shut-down
// processor exactly as before.
//
// This asserts the option this command builds, not the SDK's behaviour under
// it: that Stop drains for up to the timeout is Temporal's contract, and
// proving it here would need a live worker and a Temporal server.
func TestTheDevWorkerIsGivenTimeToDrain(t *testing.T) {
	t.Parallel()

	options := devWorkerOptions()

	require.Equal(t, v1.DefaultWorkerStopTimeout, options.WorkerStopTimeout,
		"the dev stack's worker returns from Stop without draining, so stopping it before "+
			"flushAudit protects nothing")
	require.Equal(t, v1.WorkerDeadlockDetectionTimeout, options.DeadlockDetectionTimeout,
		"the budget this stack already had")
}

// immediateCallOrderIn returns the names of the functions called in the named
// top-level function of this package, in source order, excluding calls made
// through defer.
//
// Deferred calls are excluded because they are the thing being distinguished:
// `defer w.Stop()` and `stopWorker()` read identically to a walker that counts
// names, and only the second one happens before the rest of the function body.
func immediateCallOrderIn(t *testing.T, function string) []string {
	t.Helper()

	entries, err := os.ReadDir(".")
	require.NoError(t, err)

	fset := token.NewFileSet()
	var calls []string
	found := false

	for _, entry := range entries {
		name := entry.Name()
		if entry.IsDir() || !strings.HasSuffix(name, ".go") || strings.HasSuffix(name, "_test.go") {
			continue
		}

		file, err := parser.ParseFile(fset, filepath.Clean(name), nil, parser.SkipObjectResolution)
		require.NoError(t, err, "parsing %s", name)

		for _, decl := range file.Decls {
			fn, ok := decl.(*ast.FuncDecl)
			if !ok || fn.Name.Name != function || fn.Recv != nil || fn.Body == nil {
				continue
			}

			found = true
			ast.Inspect(fn.Body, func(n ast.Node) bool {
				switch node := n.(type) {
				case *ast.DeferStmt:
					// Not part of this function's own order: it runs after
					// everything below, which is the distinction under test.
					return false
				case *ast.CallExpr:
					switch fun := node.Fun.(type) {
					case *ast.Ident:
						calls = append(calls, fun.Name)
					case *ast.SelectorExpr:
						calls = append(calls, fun.Sel.Name)
					}
				}

				return true
			})
		}
	}

	require.True(t, found, "this package declares no function named %s", function)

	return calls
}

// indexOfCall returns where name is first called, or -1.
func indexOfCall(calls []string, name string) int {
	for i, call := range calls {
		if call == name {
			return i
		}
	}

	return -1
}

// lastIndexOfCall returns where name is last called, or -1. runWorker has an
// earlier Stop on a startup-error path; shutdown ordering is proved by its
// final Stop, immediately before the sinks flush.
func lastIndexOfCall(calls []string, name string) int {
	for i := len(calls) - 1; i >= 0; i-- {
		if calls[i] == name {
			return i
		}
	}

	return -1
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
//
// One file at a time over this package's own non-test sources, which is how
// [analyzeServerSource] next door reads the seam it guards and how
// docsgen's environment-variable walk reads its own: go/parser.ParseDir, the
// one call that would read a directory in a single step, is deprecated as of
// Go 1.25 and fails the repository's staticcheck leg (SA1019). The commands
// this file asks about are declared in main.go and serverdev.go, so the test
// files it skips hold none of them — and a function that moved into one would
// fail the `found` requirement below rather than pass quietly.
func callsIn(t *testing.T, function string) map[string]bool {
	t.Helper()

	entries, err := os.ReadDir(".")
	require.NoError(t, err)

	fset := token.NewFileSet()
	calls := map[string]bool{}
	found := false

	for _, entry := range entries {
		name := entry.Name()
		if entry.IsDir() || !strings.HasSuffix(name, ".go") || strings.HasSuffix(name, "_test.go") {
			continue
		}

		file, err := parser.ParseFile(fset, filepath.Clean(name), nil, parser.SkipObjectResolution)
		require.NoError(t, err, "parsing %s", name)

		for _, decl := range file.Decls {
			fn, ok := decl.(*ast.FuncDecl)
			if !ok || fn.Name.Name != function || fn.Recv != nil || fn.Body == nil {
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

	require.True(t, found, "this package declares no function named %s", function)

	return calls
}
