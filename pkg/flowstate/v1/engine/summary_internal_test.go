package engine

import (
	"go/ast"
	"go/parser"
	"go/token"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// The other half of the label guarantee, and the half that decays.
//
// TestEveryCommandInHistoryNamesItsStep proves that the commands one run
// actually writes are all labelled. It cannot prove anything about a dispatch
// site added next month, or about one this repository has but that run did not
// reach — a `call:` step's `vars:`, plugin admission — so it would go on passing
// while the blanket claim it makes stopped being true. This walks the source
// instead: every call in this package that writes an activity or a timer into
// history has to carry a label, or say in writing why its caller carries it.
//
// A step's own task activity is not checked here and needs no entry, because
// the compiler checks it: [activityOptionsFor] takes the summary as a
// parameter, so there is no way to schedule a step's activity without passing
// one. That is the stronger guarantee, and where a rule can be made
// unforgettable rather than merely tested, it should be.

// labelledByCaller are the functions that write a command into history under a
// context somebody else labelled, with the reason each does.
var labelledByCaller = map[string]string{
	"executor.dispatch": "it is the one path a step's task takes, forward or compensating, " +
		"and both callers build its context from activityOptionsFor, which cannot be called without a summary",
}

// unlabelledSpellings are the SDK calls that write a command with no summary
// this package can set. Each has a spelling that takes one, named here so a
// failure says what to reach for instead.
var unlabelledSpellings = map[string]string{
	"Sleep":    "workflow.NewTimerWithOptions, which is the same command (workflow.Sleep is that call with a fixed `Sleep` summary)",
	"NewTimer": "workflow.NewTimerWithOptions",
}

func TestEveryDispatchSiteLabelsItsCommand(t *testing.T) {
	t.Parallel()

	var (
		activities int
		timers     int
	)

	forEachWorkflowCall(t, func(t *testing.T, where string, call *ast.CallExpr, selector string) {
		if instead, unlabelled := unlabelledSpellings[selector]; unlabelled {
			t.Errorf(`%s calls workflow.%s, which writes a command carrying no summary.

A run holding one reads in Temporal Web and in `+"`temporal workflow show`"+` as a
row that says what kind of thing happened and not which part of the file it
came from — which is the whole of what summary.go exists to prevent.

Use %s.`, where, selector, instead)

			return
		}

		switch selector {
		case "NewTimerWithOptions":
			timers++
			if !optionsCarryASummary(call) {
				t.Errorf("%s starts a timer with no Summary in its workflow.TimerOptions", where)
			}

		case "ExecuteActivity":
			activities++
			if len(call.Args) == 0 || labelsItsOwnContext(call.Args[0]) {
				return
			}
			if reason, known := labelledByCaller[where]; known {
				if reason == "" {
					t.Errorf("labelledByCaller lists %s with no reason; the reason is the point of the entry", where)
				}

				return
			}
			t.Errorf(`%s schedules an activity on a context nothing here labelled.

Wrap it — workflow.ExecuteActivity(withSummary(ctx, …), …) — or, if the label
belongs to whoever calls this, add

    %q: "…why the caller is the one that knows the step…",

to labelledByCaller in summary_internal_test.go.`, where, where)
		}
	})

	// A walk that found nothing passes every assertion above, which would make
	// this test a guard that cannot fail. Both counts, because the two rules
	// are independent: a refactor that moved every activity behind a helper
	// would leave the timer rule still meaningful and this test still worth
	// running.
	if activities == 0 || timers == 0 {
		t.Fatalf("walked this package and found %d activity dispatch(es) and %d timer(s); "+
			"this test cannot fail for the reason it exists", activities, timers)
	}
}

// forEachWorkflowCall calls fn for every `workflow.X(...)` call in this
// package's non-test sources, with `where` naming the enclosing function.
//
// The source rather than the import graph, because what is being checked is a
// spelling: `workflow.Sleep` is not wrong to *reach*, it is wrong to *write*
// here, and no type it returns says so.
func forEachWorkflowCall(t *testing.T, fn func(t *testing.T, where string, call *ast.CallExpr, selector string)) {
	t.Helper()

	entries, err := os.ReadDir(".")
	if err != nil {
		t.Fatal(err)
	}

	fset := token.NewFileSet()
	for _, entry := range entries {
		name := entry.Name()
		if entry.IsDir() || filepath.Ext(name) != ".go" || strings.HasSuffix(name, "_test.go") {
			continue
		}

		file, err := parser.ParseFile(fset, name, nil, 0)
		if err != nil {
			t.Fatal(err)
		}

		for _, decl := range file.Decls {
			function, ok := decl.(*ast.FuncDecl)
			if !ok || function.Body == nil {
				continue
			}

			where := functionName(function)
			ast.Inspect(function.Body, func(n ast.Node) bool {
				call, ok := n.(*ast.CallExpr)
				if !ok {
					return true
				}
				selector, ok := call.Fun.(*ast.SelectorExpr)
				if !ok {
					return true
				}
				pkg, ok := selector.X.(*ast.Ident)
				if !ok || pkg.Name != "workflow" {
					return true
				}
				fn(t, where, call, selector.Sel.Name)

				return true
			})
		}
	}
}

// functionName is `Receiver.Name` for a method and `Name` for a function, which
// is how the entries in labelledByCaller are spelled.
func functionName(function *ast.FuncDecl) string {
	if function.Recv == nil || len(function.Recv.List) == 0 {
		return function.Name.Name
	}

	receiver := function.Recv.List[0].Type
	if star, isPointer := receiver.(*ast.StarExpr); isPointer {
		receiver = star.X
	}
	if ident, ok := receiver.(*ast.Ident); ok {
		return ident.Name + "." + function.Name.Name
	}

	return function.Name.Name
}

// labelsItsOwnContext reports whether the context handed to an activity was
// labelled right there, which is `withSummary(…)` and nothing else.
func labelsItsOwnContext(argument ast.Expr) bool {
	call, ok := argument.(*ast.CallExpr)
	if !ok {
		return false
	}
	ident, ok := call.Fun.(*ast.Ident)

	return ok && ident.Name == "withSummary"
}

// optionsCarryASummary reports whether a timer call passes a non-empty Summary
// in its options literal.
func optionsCarryASummary(call *ast.CallExpr) bool {
	for _, argument := range call.Args {
		literal, ok := argument.(*ast.CompositeLit)
		if !ok {
			continue
		}
		for _, element := range literal.Elts {
			pair, ok := element.(*ast.KeyValueExpr)
			if !ok {
				continue
			}
			key, ok := pair.Key.(*ast.Ident)
			if !ok || key.Name != "Summary" {
				continue
			}
			basic, isLiteral := pair.Value.(*ast.BasicLit)

			return !isLiteral || basic.Value != `""`
		}
	}

	return false
}
