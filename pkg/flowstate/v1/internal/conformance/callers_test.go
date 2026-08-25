package conformance

import (
	"go/ast"
	"go/parser"
	"go/token"
	"io/fs"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"testing"
)

// The two-callers rule, enforced by walking rather than by imitation (#934).
//
// CLAUDE.md states the rule — everything this package exports exists to be
// consumed by both drivers — and records what prose enforcement bought:
// ZeroValueCases sat for months at one caller, "proving half of what it was
// written for", and the policy-denial counter was asserted on one driver
// while the other's assertion simply never existed. A human walking fifty
// functions is the only check a prose rule has, so this file walks them
// instead, in the mold of tools/fuzztargets/targets_test.go — the checker
// that caught the deep tier running four of ten targets (#857).
//
// The analysis is deliberately syntactic: plain go/parser over the tree, no
// type checking. A reference is a selector through this package's import
// (alias-aware), and consumption propagates through this package's own
// exported functions — a set consumed via a two-sided assert helper counts
// through its consumer, which is one of the two bucketing subtleties #934's
// hand audit hit. The other is that the durable enforcement point for signal
// policy is server/, not engine/, so both are the durable bucket.

// confImportPath is how the tree names this package.
const confImportPath = "github.com/picatz/flowstate/pkg/flowstate/v1/internal/conformance"

// oneSidedByDesign names the exported functions whose claim is genuinely
// about one driver, with the reason — the coverage.allow_unreached shape: an
// entry with no reason would be the silent gap the record exists to refuse,
// and TestTheOneSidedAllowlistStaysHonest fails a stale entry the same way
// it fails a missing one.
var oneSidedByDesign = map[string]string{
	"AssertRehearsalSenderIsNeverAuthorizedDurably": "the claim is about durable admission only — " +
		"a rehearsal's stand-in sender must never authorize a durable delivery (signalrehearsal.go), " +
		"so a local caller would be asserting a property the local driver deliberately does not have",
}

// driverCallers is where each exported function is consumed from, after
// propagation.
type driverCallers struct {
	local   bool // pkg/flowstate/v1's own test packages
	durable bool // pkg/flowstate/v1/engine or pkg/flowstate/v1/server
}

// TestEverySharedCaseSetIsCalledByBothDrivers is the rule itself: every
// exported function in this package is reachable from a local-driver test
// and from a durable-driver test, directly or through another export, unless
// oneSidedByDesign says why not.
func TestEverySharedCaseSetIsCalledByBothDrivers(t *testing.T) {
	callers := analyzeCallers(t)

	names := make([]string, 0, len(callers))
	for name := range callers {
		names = append(names, name)
	}
	sort.Strings(names)

	for _, name := range names {
		if _, excused := oneSidedByDesign[name]; excused {
			continue
		}

		c := callers[name]
		switch {
		case !c.local && !c.durable:
			t.Errorf("%s has no caller on either driver — the ZeroValueCases shape: it proves "+
				"nothing at all; call it from both drivers, or delete it, or add it to "+
				"oneSidedByDesign with the reason", name)
		case !c.local:
			t.Errorf("%s is asserted only on the durable driver; add the local caller "+
				"(pkg/flowstate/v1's tests), or add it to oneSidedByDesign with the reason", name)
		case !c.durable:
			t.Errorf("%s is asserted only on the local driver; add the durable caller "+
				"(pkg/flowstate/v1/engine or /server tests), or add it to oneSidedByDesign "+
				"with the reason", name)
		}
	}
}

// TestTheOneSidedAllowlistStaysHonest: an entry must name a function that
// still exists and is still one-sided, and must carry a reason. A function
// that grew its second caller has outgrown its excuse; leaving the entry
// would quietly excuse the next regression.
func TestTheOneSidedAllowlistStaysHonest(t *testing.T) {
	callers := analyzeCallers(t)

	for name, reason := range oneSidedByDesign {
		if strings.TrimSpace(reason) == "" {
			t.Errorf("oneSidedByDesign[%q] has no reason; the reason is the record", name)
		}
		c, exists := callers[name]
		if !exists {
			t.Errorf("oneSidedByDesign lists %s, but this package exports no such function; "+
				"delete the entry", name)
			continue
		}
		if !c.local && !c.durable {
			// The entry excuses one-sidedness, not zero-sidedness: an
			// allowlisted export whose only caller is deleted would otherwise
			// stay green while its assertion never runs — the ZeroValueCases
			// shape hiding behind its own excuse (Codex, #1109).
			t.Errorf("%s is allowlisted as one-sided by design and now has no caller at all; "+
				"restore the caller its reason describes, or delete the function and the entry", name)
			continue
		}
		if c.local && c.durable {
			t.Errorf("%s now has callers on both drivers; delete its oneSidedByDesign entry — "+
				"a stale excuse is the next gap's cover", name)
		}
	}
}

// analyzeCallers parses this package for exported functions and the tree's
// test files for references to them, then propagates driver marks through
// intra-package consumption.
func analyzeCallers(t *testing.T) map[string]driverCallers {
	t.Helper()

	fset := token.NewFileSet()

	// Subjects and intra-package edges (consumer -> consumed), from this
	// package's non-test files. Bodies only: a name in a signature is a
	// type, and types are not subjects here.
	exported := map[string]bool{}
	var decls []*ast.FuncDecl
	entries, err := os.ReadDir(".")
	if err != nil {
		t.Fatal(err)
	}
	for _, e := range entries {
		if !strings.HasSuffix(e.Name(), ".go") || strings.HasSuffix(e.Name(), "_test.go") {
			continue
		}
		f, err := parser.ParseFile(fset, e.Name(), nil, 0)
		if err != nil {
			t.Fatal(err)
		}
		for _, d := range f.Decls {
			if fd, ok := d.(*ast.FuncDecl); ok && fd.Recv == nil && fd.Name.IsExported() {
				exported[fd.Name.Name] = true
				decls = append(decls, fd)
			}
		}
	}
	if len(exported) == 0 {
		t.Fatal("the walk found no exported functions in this package; the checker is broken, not the tree")
	}

	// A bare identifier in a body matching an exported sibling is an edge.
	// This can over-match a local variable that shares an export's name;
	// that errs toward marking, and every export here is named like a public
	// API rather than like a local, so the shapes do not in practice meet.
	edges := map[string]map[string]bool{}
	for _, fd := range decls {
		if fd.Body == nil {
			continue
		}
		ast.Inspect(fd.Body, func(n ast.Node) bool {
			if id, ok := n.(*ast.Ident); ok && exported[id.Name] && id.Name != fd.Name.Name {
				if edges[fd.Name.Name] == nil {
					edges[fd.Name.Name] = map[string]bool{}
				}
				edges[fd.Name.Name][id.Name] = true
			}
			return true
		})
	}

	// External references, bucketed by the directory of the test file.
	root := filepath.Join("..", "..")
	local := map[string]bool{}
	durable := map[string]bool{}
	referencingFiles := 0
	err = filepath.WalkDir(root, func(path string, d fs.DirEntry, err error) error {
		if err != nil || d.IsDir() || !strings.HasSuffix(path, "_test.go") {
			return err
		}
		rel, err := filepath.Rel(root, filepath.Dir(path))
		if err != nil {
			return err
		}
		rel = filepath.ToSlash(rel)
		if rel == "internal/conformance" {
			// This package's own tests prove nothing about either driver.
			return nil
		}

		var bucket *map[string]bool
		switch rel {
		case ".":
			bucket = &local
		case "engine", "server":
			bucket = &durable
		default:
			// dst and everything else consume these sets too, and prove
			// nothing about driver parity; walked and discarded.
			bucket = nil
		}

		f, err := parser.ParseFile(fset, path, nil, 0)
		if err != nil {
			return err
		}
		alias := ""
		for _, imp := range f.Imports {
			if strings.Trim(imp.Path.Value, `"`) == confImportPath {
				alias = "conformance"
				if imp.Name != nil {
					alias = imp.Name.Name
				}
			}
		}
		if alias == "" {
			return nil
		}
		referencingFiles++
		if bucket == nil {
			return nil
		}
		ast.Inspect(f, func(n ast.Node) bool {
			sel, ok := n.(*ast.SelectorExpr)
			if !ok {
				return true
			}
			if id, ok := sel.X.(*ast.Ident); ok && id.Name == alias && exported[sel.Sel.Name] {
				(*bucket)[sel.Sel.Name] = true
			}
			return true
		})
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	if referencingFiles == 0 {
		t.Fatal("no test file under pkg/flowstate/v1 imports this package; the walk is broken, not the tree")
	}

	// Consumption propagates the mark: a bucket that calls a helper is
	// asserting everything the helper runs, transitively.
	propagate := func(marks map[string]bool) {
		queue := make([]string, 0, len(marks))
		for n := range marks {
			queue = append(queue, n)
		}
		for len(queue) > 0 {
			n := queue[0]
			queue = queue[1:]
			for consumed := range edges[n] {
				if !marks[consumed] {
					marks[consumed] = true
					queue = append(queue, consumed)
				}
			}
		}
	}
	propagate(local)
	propagate(durable)

	callers := make(map[string]driverCallers, len(exported))
	for name := range exported {
		callers[name] = driverCallers{local: local[name], durable: durable[name]}
	}
	return callers
}
