package server_test

import (
	"go/ast"
	"go/parser"
	"go/token"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"

	v1 "github.com/picatz/flowstate/pkg/flowstate/v1"
)

// The audited surface, checked against the surface itself rather than against
// a list of it.
//
// #1018's first question was "what is audited", and its answer was a
// derivation: the audited set is every authorization action the bindings
// attach to an RPC, and TestEveryRPCHasExactlyOneAuthorizationAction already
// fails when an RPC arrives that no action names. That closes the vocabulary
// end. This closes the other one — that each of those RPCs actually *reaches*
// an emitter — because an action bound to an RPC whose handler records nothing
// is a surface that is audited on paper.
//
// The analysis is syntactic, in the mould of
// internal/conformance/callers_test.go and tools/fuzztargets/targets_test.go:
// plain go/parser over this package's own source, no type checking. A handler
// reaches the seam if it calls auditAllow or auditDeny, or calls something
// else in this package that does. That propagation is what lets
// authorizeRun and authorizeSchedule hold the seam for the verbs that go
// through them, which is where it belongs — the decision they make is the
// decision being recorded.
//
// What it cannot see is whether the record is emitted before the mutation, or
// exactly once. Those are claims about behaviour, and audit_internal_test.go
// makes them by driving the handlers.

// auditSeamFunctions are the two functions that put a record in front of an
// emitter. Everything else reaches the seam only by calling one of them.
var auditSeamFunctions = map[string]bool{
	"auditAllow": true,
	"auditDeny":  true,
}

// TestEveryRPCReachesTheAuditSeam walks flowstate.v1.WorkflowService's
// descriptor for the RPC names, and this package's source for what each
// handler can reach.
func TestEveryRPCReachesTheAuditSeam(t *testing.T) {
	t.Parallel()

	calls, literals := analyzeServerSource(t)

	services := v1.File_flowstate_v1_service_proto.Services()
	require.Equal(t, 1, services.Len(), "the schema declares more than one service; this test names one")

	methods := services.Get(0).Methods()
	require.NotZero(t, methods.Len(), "the service declares no methods; the walk is broken")

	for i := range methods.Len() {
		rpc := string(methods.Get(i).Name())

		require.Contains(t, calls, rpc,
			"flowstate.v1.WorkflowService declares %s and this package declares no handler for it", rpc)

		require.True(t, reachesAuditSeam(rpc, calls, map[string]bool{}),
			"%s makes an authorization decision and records nothing: it reaches neither "+
				"auditAllow nor auditDeny, directly or through a helper. See server/audit.go — "+
				"an action bound to an RPC whose handler never emits is audited on paper only", rpc)

		require.Contains(t, literals[rpc], rpc,
			"%s reaches the audit seam without naming itself: the record's rpc field is what "+
				"keys it to an authorization action, so the handler has to pass its own schema "+
				"name (directly, or to the helper holding its seam)", rpc)
	}
}

// TestTheAuditSeamIsNotBypassed: the un-audited decision functions exist for
// the verbs that resolve a resource twice for one request, and nothing else
// may call them, or a decision would be made with no record at all.
//
// Named callers rather than a blanket ban, because the two-lookup shape is
// real — see [FlowstateServer.Signal] — and a rule with no exception would be
// one somebody deletes.
func TestTheAuditSeamIsNotBypassed(t *testing.T) {
	t.Parallel()

	allowed := map[string]map[string]string{
		"authorizeRunDecision": {
			"authorizeRun":    "the audited wrapper: this is where the record is written",
			"Signal":          "walks a Continue-As-New chain from its first run id to the current one, which is one decision reached in two lookups; it audits once itself",
			"SignalWithStart": "audits when the request is admitted, before anything is created; the already-running branch re-resolves that same decision",
		},
		"authorizeScheduleDecision": {
			"authorizeSchedule": "the audited wrapper: this is where the record is written",
		},
		"describeSchedule": {
			"CreateSchedule":   "describes what it has just created and been audited for",
			"DescribeSchedule": "the describe is this verb's decision, and it audits the outcome itself",
		},
	}

	calls, _ := analyzeServerSource(t)

	for callee, permitted := range allowed {
		for caller, callees := range calls {
			if !callees[callee] || permitted[caller] != "" {
				continue
			}

			t.Errorf("%s calls %s, which makes an authorization decision and records nothing. "+
				"Call the audited form instead, or add %s to this test's allowlist with the "+
				"reason it audits the decision itself", caller, callee, caller)
		}

		for caller, reason := range permitted {
			require.Contains(t, calls, caller,
				"the allowlist names %s, which this package no longer declares", caller)
			require.True(t, calls[caller][callee],
				"the allowlist says %s calls %s (%s) and it no longer does; a stale exemption is "+
					"a hole nobody is looking at", caller, callee, reason)
		}
	}
}

// reachesAuditSeam answers whether fn calls a seam function, directly or
// through anything else this package declares.
func reachesAuditSeam(fn string, calls map[string]map[string]bool, seen map[string]bool) bool {
	if seen[fn] {
		return false
	}
	seen[fn] = true

	for callee := range calls[fn] {
		if auditSeamFunctions[callee] {
			return true
		}
		if reachesAuditSeam(callee, calls, seen) {
			return true
		}
	}

	return false
}

// analyzeServerSource reads this package's non-test files and reports, per
// declared function or method, what it calls and which string literals it
// mentions.
//
// Names are unqualified: this package has one type with methods on it, so a
// method's name identifies it, and a selector's Sel is the callee's name. Two
// declarations sharing a name would merge, which would make this analysis
// permissive rather than wrong — and the package has no such pair.
func analyzeServerSource(t *testing.T) (calls map[string]map[string]bool, literals map[string]map[string]bool) {
	t.Helper()

	calls = map[string]map[string]bool{}
	literals = map[string]map[string]bool{}

	entries, err := os.ReadDir(".")
	require.NoError(t, err)

	fset := token.NewFileSet()
	files := 0

	for _, entry := range entries {
		name := entry.Name()
		if entry.IsDir() || !strings.HasSuffix(name, ".go") || strings.HasSuffix(name, "_test.go") {
			continue
		}

		file, err := parser.ParseFile(fset, filepath.Clean(name), nil, parser.ImportsOnly|parser.SkipObjectResolution)
		require.NoError(t, err, "parsing %s", name)
		files++

		// Import aliases, so that a package-qualified call is not mistaken for
		// a method of this package's own. Without this, `v1.Validate(...)` in
		// every handler reads as a call to this package's Validate handler —
		// which reaches the seam — and every handler in the package would
		// appear to reach it through a function it never calls. That is a
		// coverage test that passes by accident, which is the failure mode
		// CLAUDE.md names.
		imported := map[string]bool{}
		for _, spec := range file.Imports {
			alias := ""
			if spec.Name != nil {
				alias = spec.Name.Name
			} else if path, err := strconv.Unquote(spec.Path.Value); err == nil {
				alias = path[strings.LastIndex(path, "/")+1:]
			}
			if alias != "" {
				imported[alias] = true
			}
		}

		file, err = parser.ParseFile(fset, filepath.Clean(name), nil, parser.SkipObjectResolution)
		require.NoError(t, err, "parsing %s", name)

		for _, decl := range file.Decls {
			fn, ok := decl.(*ast.FuncDecl)
			if !ok || fn.Body == nil {
				continue
			}

			declared := fn.Name.Name
			if calls[declared] == nil {
				calls[declared] = map[string]bool{}
				literals[declared] = map[string]bool{}
			}

			ast.Inspect(fn.Body, func(node ast.Node) bool {
				switch n := node.(type) {
				case *ast.CallExpr:
					switch callee := n.Fun.(type) {
					case *ast.Ident:
						calls[declared][callee.Name] = true
					case *ast.SelectorExpr:
						if pkg, ok := callee.X.(*ast.Ident); ok && imported[pkg.Name] {
							// Another package's function, named here only by
							// coincidence of spelling.
							break
						}
						calls[declared][callee.Sel.Name] = true
					}
				case *ast.BasicLit:
					if n.Kind == token.STRING {
						if value, err := strconv.Unquote(n.Value); err == nil {
							literals[declared][value] = true
						}
					}
				}

				return true
			})
		}
	}

	require.NotZero(t, files, "no source files were read; the analysis proves nothing")

	return calls, literals
}
