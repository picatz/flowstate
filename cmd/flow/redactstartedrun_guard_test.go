package main

import (
	"go/ast"
	"go/parser"
	"go/token"
	"testing"

	"github.com/stretchr/testify/require"
)

// TestEveryRunThisProcessStartsIsRedactedAsOne holds every surface that starts
// a run to [redactStartedRun], so the next one cannot redact its declared
// outputs and forget the failure sentence. That happened twice: `flow run
// local` learned it in #974, and the MCP run_local tool and `flow task run`
// had not by #2173 (Codex), each with the arguments in hand.
//
// Two rules over every non-test Go file under cmd/flow:
//
//   - A function that calls v1.RunWithInputs calls redactStartedRun, unless it
//     is listed in startsRunsRedactedElsewhere with the reason.
//   - redactGetResponse is called with a literal nil specification, or from
//     redactStartedRun. A caller holding a specification started the run, or
//     is following one it started, and holds its arguments too.
func TestEveryRunThisProcessStartsIsRedactedAsOne(t *testing.T) {
	t.Parallel()

	// startsRunsRedactedElsewhere names a function that starts a run and
	// renders its failure through a redaction of its own, with that reason.
	startsRunsRedactedElsewhere := map[string]string{
		"launchDebuggedRun": "flowdap prints the failure through session.RedactText, against the debug session's own sensitive set",
	}

	paths := nonTestGoFiles(t)

	fset := token.NewFileSet()
	starters, redactions := 0, 0
	allowed := map[string]bool{}
	for _, path := range paths {
		file, err := parser.ParseFile(fset, path, nil, parser.SkipObjectResolution)
		require.NoError(t, err)
		for _, decl := range file.Decls {
			fn, ok := decl.(*ast.FuncDecl)
			if !ok || fn.Body == nil {
				continue
			}
			var startsRun, redactsStarted bool
			ast.Inspect(fn.Body, func(n ast.Node) bool {
				call, ok := n.(*ast.CallExpr)
				if !ok {
					return true
				}
				switch fun := call.Fun.(type) {
				case *ast.SelectorExpr:
					if pkg, ok := fun.X.(*ast.Ident); ok && pkg.Name == "v1" && fun.Sel.Name == "RunWithInputs" {
						startsRun = true
					}
				case *ast.Ident:
					switch fun.Name {
					case "redactStartedRun":
						redactsStarted = true
					case "redactGetResponse":
						redactions++
						if fn.Name.Name == "redactStartedRun" || len(call.Args) < 2 {
							return true
						}
						if spec, ok := call.Args[1].(*ast.Ident); !ok || spec.Name != "nil" {
							t.Errorf("%s: %s calls redactGetResponse holding a specification; "+
								"a caller with one holds the run's arguments too, so use redactStartedRun",
								fset.Position(call.Pos()), fn.Name.Name)
						}
					}
				}
				return true
			})
			if !startsRun {
				continue
			}
			starters++
			if _, ok := startsRunsRedactedElsewhere[fn.Name.Name]; ok {
				allowed[fn.Name.Name] = true
				continue
			}
			if !redactsStarted {
				t.Errorf("%s: %s starts a run with v1.RunWithInputs and does not redact it through redactStartedRun",
					fset.Position(fn.Pos()), fn.Name.Name)
			}
		}
	}

	// Not vacuous: the walk found the surfaces it is about, and every exception
	// still names a function that exists and starts a run.
	require.GreaterOrEqual(t, starters, 4, "too few run starters were found; the walk is wrong, not the package")
	require.Positive(t, redactions, "no redactGetResponse call was found; the walk is wrong, not the package")
	for name := range startsRunsRedactedElsewhere {
		require.True(t, allowed[name], "%s no longer starts a run; drop it from startsRunsRedactedElsewhere", name)
	}
}
