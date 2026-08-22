package engine

import (
	"fmt"
	"go/ast"
	"go/parser"
	"go/token"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"
)

// repoRootFromEngine is this module's root: pkg/flowstate/v1/engine is four
// levels down.
const repoRootFromEngine = "../../../.."

// The scope-construction guard for #737, and it is deliberately the weak half of
// that issue rather than the fix.
//
// # The convention
//
// [tenantArg] reads a `*v1.Scope` among an activity's arguments as the answer to
// "whose work is this" — and reads a scope carrying no identity as the *default
// tenant*, because the default tenant's namespace is the empty string (see
// tenant.go, and TestTenantArgReadsAScopeWithoutIdentityAsTheDefaultTenant). That
// reading is only correct once every scope that *should* carry an identity does.
// So every scope assembled for dispatch carries the run's identity, and a scope
// that forgets it makes a `flow worker --tenant team-a` worker refuse team-a's own
// run — narrowly, only for tenants that declare `vars:` or use compensations, so a
// deployment can pass its own tests and break for one tenant.
//
// # Why a test and not a type
//
// #737 asks for one of two things. The version worth having is structural: a
// dispatch path that takes a type which can only be built from a run identity, so
// omitting it is a compile error. That is a design pass — it has to decide what the
// legitimately identity-less scope (the default-tenant read `tenantArg` relies on)
// becomes — and it is the owner's call, so this does not foreclose it. This is the
// issue's own fallback: "a test that enumerates the construction sites rather than
// naming them". Weaker, since it is a lint in test clothing, but it converts
// "somebody remembered" into "CI noticed", which is the property that was missing —
// reviewing #676 caught a third assembled-scope site the change had missed, and the
// convention was already one site short on its first outing.
//
// When the constructor lands, this file goes away with it.
//
// # What is walked
//
// Every non-generated Go file in this module, plus the test files of this package.
// Test files elsewhere are not walked, and that is a line rather than an oversight:
// a scope becomes an activity argument here, in the durable driver, and nowhere
// else. A scope a `waitprompt_test.go` builds is an argument to the function under
// test and reaches no worker, so requiring an identity of it would be a diagnostic
// about nothing — while a scope built in *this* package's tests is in exactly the
// shape the guard is about, which is why the one bare literal #737 names is inside
// the walked set and blessed below on its merits.
//
// Source-walking rather than reflective, matching tools/fuzztargets (#871): the
// question is about the text someone will write next, and a construction site that
// exists only in a branch nothing ran is precisely the one to catch.

// exemptScopeConstruction lists the construction sites that legitimately build a
// scope with no identity, each with the reason it cannot mis-route a tenant.
//
// Keyed by module-relative path and enclosing function, not by line: a line number
// is a key that changes when someone edits the paragraph above the site.
var exemptScopeConstruction = map[string]string{
	"pkg/flowstate/v1/engine/activities.go#WorkflowVars": "the activity's *result*, not an argument: it travels back from the " +
		"worker that already passed the tenant guard on the way in, and tenantArg reads arguments only.",

	"pkg/flowstate/v1/engine/tenant_internal_test.go#TestTenantArgReadsAScopeWithoutIdentityAsTheDefaultTenant": "the bare scope is the subject of the test: it asserts that an " +
		"identity-less scope is read as the default tenant rather than as an activity declining to say whose work it is. " +
		"Giving it an identity would delete the case.",

	"pkg/flowstate/v1/nodes.go#NewScope": "the general constructor, over outputs and a profile. It is what #737's " +
		"structural half would replace; every caller that builds a scope a run will dispatch assigns Identity after it " +
		"(engine/workflow.go varsScope, v1/call.go CallScope).",

	"pkg/flowstate/v1/signalpolicy.go#ResolveSignalPolicySubjects": "resolves `subject_from:` expressions in the server, at " +
		"submit, against the run's inputs — before the run exists and so before there is a run identity to carry. The scope " +
		"is never an activity argument, and the identity a signal policy decides on is the *sender's*, attested at delivery.",
}

// TestEveryScopeConstructionCarriesTheRunIdentity is the guard itself.
func TestEveryScopeConstructionCarriesTheRunIdentity(t *testing.T) {
	sites := scopeSitesInTree(t)

	seen := map[string]bool{}
	for _, site := range sites {
		if _, exempt := exemptScopeConstruction[site.key]; exempt {
			seen[site.key] = true
			continue
		}
		if site.carriesIdentity {
			continue
		}

		t.Errorf(`%s: this scope literal does not carry the run identity.

A scope that names no identity is read as the *default tenant* (tenantArg, in
pkg/flowstate/v1/engine/tenant.go), so if this one is dispatched to an activity a
worker started with "flow worker --tenant team-a" refuses it — including team-a's
own run's vars or its own run's compensation.

Either set Identity from the run, the way its neighbours do:

    Identity: e.identity            // executor, for a dispatch it makes
    Identity: st.GetIdentity()      // the run state, at the top of a segment
    Identity: e.scope.GetIdentity() // deriving from the scope a run already has

or, if this scope genuinely never travels to an activity, add

    %q: "…why it cannot…",

to exemptScopeConstruction in %s, with the reason. See #737.`,
			site.pos, site.key, "pkg/flowstate/v1/engine/scope_guard_test.go")
	}

	for key := range exemptScopeConstruction {
		if !seen[key] {
			t.Errorf("exemptScopeConstruction lists %s, but no scope literal is constructed there any more; delete the entry", key)
		}
	}
}

// TestTheScopeGuardWalksSomething. Every enumeration guard's own failure mode is
// finding nothing — a renamed type, a moved package, a walk rooted at the wrong
// directory — and reporting green because it had no site to object to. So the
// walk's yield is asserted: the four dispatch sites #676 enumerated are still
// found, still in this package, and still carrying an identity.
func TestTheScopeGuardWalksSomething(t *testing.T) {
	sites := scopeSitesInTree(t)
	if len(sites) < len(exemptScopeConstruction) {
		t.Fatalf("walked the tree and found %d scope literals, fewer than the %d exemptions listed: "+
			"the walk is not reaching the source it is supposed to guard", len(sites), len(exemptScopeConstruction))
	}

	dispatch := 0
	for _, site := range sites {
		if strings.HasPrefix(site.key, "pkg/flowstate/v1/engine/") &&
			!strings.Contains(site.key, "_test.go#") &&
			site.carriesIdentity {
			dispatch++
		}
	}
	if dispatch < 4 {
		t.Errorf("found %d identity-carrying scope literals in the durable driver, want at least the 4 dispatch sites #676 enumerated "+
			"(two WorkflowVars, the compacted TaskInScope scope, runUndoTask's compensation scope); "+
			"if one was legitimately removed, say so here", dispatch)
	}
}

// TestScopeSiteDetectionReadsTheShapesGoAllows is the discovery check the same
// argument as tools/fuzztargets' applies to: a guard is only as good as its idea of
// what it is looking for, and a text search for "&v1.Scope{" misses an import under
// another alias, a value (rather than pointer) literal, one nested inside another
// composite, and the shape that matters most — a literal that is bare on the line it
// is written and given its identity two statements later, which is how nodes.go's
// derivation helpers are written and is not a defect.
//
// The near-misses have to stay misses too: the flowfile package has four unrelated
// types whose names end in "Scope", and v1.UndoScope is a different type entirely.
func TestScopeSiteDetectionReadsTheShapesGoAllows(t *testing.T) {
	const src = `package p

import (
	"fmt"

	fs "github.com/picatz/flowstate/pkg/flowstate/v1"
)

func Bare() *fs.Scope { return &fs.Scope{Profile: "default"} }

func Carrying() *fs.Scope { return &fs.Scope{Identity: nil} }

func AssignedAfterwards() *fs.Scope {
	next := &fs.Scope{Profile: "default"}
	next.Identity = nil
	return next
}

func AssignedToSomethingElse() *fs.Scope {
	next := &fs.Scope{Profile: "default"}
	other := &fs.Scope{Identity: nil}
	other.Identity = nil
	fmt.Println(other)
	return next
}

func ValueLiteral() fs.Scope { return fs.Scope{Profile: "default"} }

// Not scope literals: an unrelated type whose name ends in Scope, a different
// message on the same package, and a slice whose elements are variables.
type refScope struct{ steps int }

func NearMisses(s *fs.Scope) any {
	return []any{refScope{}, fs.UndoScope(0), []*fs.Scope{s}}
}
`
	fset := token.NewFileSet()
	file, err := parser.ParseFile(fset, "scope_fixture.go", src, parser.SkipObjectResolution)
	if err != nil {
		t.Fatal(err)
	}

	var got []string
	for _, site := range scopeSitesInFile(fset, file, "scope_fixture.go") {
		got = append(got, fmt.Sprintf("%s=%t", site.key, site.carriesIdentity))
	}

	want := []string{
		"scope_fixture.go#Bare=false",
		"scope_fixture.go#Carrying=true",
		"scope_fixture.go#AssignedAfterwards=true",
		// Two literals in one function: the second is the one given an
		// identity, and the first must not inherit that from its neighbour.
		"scope_fixture.go#AssignedToSomethingElse=false",
		"scope_fixture.go#AssignedToSomethingElse=true",
		"scope_fixture.go#ValueLiteral=false",
	}
	if !slices.Equal(got, want) {
		t.Errorf("detected %v\nwant     %v", got, want)
	}
}

// scopeSite is one construction of a [v1.Scope] in the source.
type scopeSite struct {
	key             string // module-relative path + "#" + enclosing function
	pos             string // path:line:col, for a person to open
	carriesIdentity bool
}

// scopeSitesInTree returns every scope construction site this guard walks: every
// non-generated Go file in this module, plus this package's own tests.
//
// plugins/ is skipped for the reason CLAUDE.md already records for `make coverage`
// — they are separate modules outside this module's build graph — and nothing in
// them constructs a scope, because a plugin is handed one over the wire.
func scopeSitesInTree(t *testing.T) []scopeSite {
	t.Helper()

	const enginePkg = "pkg/flowstate/v1/engine"

	var sites []scopeSite
	fset := token.NewFileSet()
	err := filepath.WalkDir(repoRootFromEngine, func(path string, d os.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if d.IsDir() {
			switch d.Name() {
			case ".git", "plugins", "testdata", "node_modules":
				return filepath.SkipDir
			}
			return nil
		}
		name := d.Name()
		if !strings.HasSuffix(name, ".go") || strings.HasSuffix(name, ".pb.go") {
			return nil
		}

		rel, err := filepath.Rel(repoRootFromEngine, path)
		if err != nil {
			return err
		}
		rel = filepath.ToSlash(rel)

		if strings.HasSuffix(name, "_test.go") && filepath.ToSlash(filepath.Dir(rel)) != enginePkg {
			return nil
		}

		file, err := parser.ParseFile(fset, path, nil, parser.SkipObjectResolution)
		if err != nil {
			return fmt.Errorf("%s: %w", path, err)
		}
		sites = append(sites, scopeSitesInFile(fset, file, rel)...)
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	return sites
}

// scopeSitesInFile returns the scope literals one parsed file constructs.
//
// The type is recognised two ways, and both are needed: qualified through whatever
// name the file imports pkg/flowstate/v1 under, and bare in the package that
// declares it. Files that do neither are skipped outright, which is what keeps the
// flowfile package's own refScope, stepScope, taskScope and loopScope — four types
// whose names end the same way — out of this.
func scopeSitesInFile(fset *token.FileSet, file *ast.File, rel string) []scopeSite {
	const v1ImportPath = `"github.com/picatz/flowstate/pkg/flowstate/v1"`

	qualifier := ""
	for _, spec := range file.Imports {
		if spec.Path.Value != v1ImportPath {
			continue
		}
		qualifier = "v1"
		if spec.Name != nil {
			qualifier = spec.Name.Name
		}
	}
	// The v1 package's own files spell the type bare. Decided by directory
	// rather than by the package clause, because a `package v1` elsewhere in
	// the tree would declare a different Scope, and this is the one that is
	// dispatched.
	bare := filepath.ToSlash(filepath.Dir(rel)) == "pkg/flowstate/v1"

	if qualifier == "" && !bare {
		return nil
	}
	if qualifier == "_" || qualifier == "." {
		// Neither spelling reaches the type under a name this can match; a
		// dot-import would need a different check and nothing in the tree
		// does it.
		qualifier = ""
	}

	isScopeType := func(expr ast.Expr) bool {
		switch t := expr.(type) {
		case *ast.Ident:
			return bare && t.Name == "Scope"
		case *ast.SelectorExpr:
			pkg, ok := t.X.(*ast.Ident)
			return ok && qualifier != "" && pkg.Name == qualifier && t.Sel.Name == "Scope"
		}
		return false
	}

	var sites []scopeSite
	for _, decl := range file.Decls {
		fn, ok := decl.(*ast.FuncDecl)
		if !ok || fn.Body == nil {
			continue
		}

		// The names this function later assigns an identity to. Collected
		// over the whole body first, because the assignment legitimately
		// comes after the literal — nodes.go's WithLocal, WithLocals,
		// WithAmbientVars and WithOutputs each build the copy and then carry
		// the identity across, and each is a correct site.
		identified := map[string]bool{}
		ast.Inspect(fn.Body, func(n ast.Node) bool {
			assign, ok := n.(*ast.AssignStmt)
			if !ok {
				return true
			}
			for _, lhs := range assign.Lhs {
				sel, ok := lhs.(*ast.SelectorExpr)
				if !ok || sel.Sel.Name != "Identity" {
					continue
				}
				if target, ok := sel.X.(*ast.Ident); ok {
					identified[target.Name] = true
				}
			}
			return true
		})

		// The name each literal was bound to, so an identity assigned to one
		// scope in a function does not bless a second scope beside it.
		boundTo := map[*ast.CompositeLit]string{}
		ast.Inspect(fn.Body, func(n ast.Node) bool {
			assign, ok := n.(*ast.AssignStmt)
			if !ok {
				return true
			}
			for i, rhs := range assign.Rhs {
				if i >= len(assign.Lhs) {
					break
				}
				lit := compositeLitOf(rhs)
				if lit == nil {
					continue
				}
				if target, ok := assign.Lhs[i].(*ast.Ident); ok {
					boundTo[lit] = target.Name
				}
			}
			return true
		})

		ast.Inspect(fn.Body, func(n ast.Node) bool {
			lit, ok := n.(*ast.CompositeLit)
			if !ok || !isScopeType(lit.Type) {
				return true
			}

			carries := false
			for _, elt := range lit.Elts {
				kv, ok := elt.(*ast.KeyValueExpr)
				if !ok {
					continue
				}
				if key, ok := kv.Key.(*ast.Ident); ok && key.Name == "Identity" {
					carries = true
				}
			}
			if name, ok := boundTo[lit]; ok && identified[name] {
				carries = true
			}

			// Positioned against the module-relative path rather than the
			// walk's own `../../../..` prefix, so the failure names a file
			// the way an editor and a review comment do.
			at := fset.Position(lit.Pos())
			sites = append(sites, scopeSite{
				key:             rel + "#" + fn.Name.Name,
				pos:             fmt.Sprintf("%s:%d:%d", rel, at.Line, at.Column),
				carriesIdentity: carries,
			})
			return true
		})
	}
	return sites
}

// compositeLitOf unwraps the `&T{…}` a scope is nearly always built as, and
// accepts the bare `T{…}` too so that a value literal is not silently unwatched.
func compositeLitOf(expr ast.Expr) *ast.CompositeLit {
	if unary, ok := expr.(*ast.UnaryExpr); ok && unary.Op == token.AND {
		expr = unary.X
	}
	lit, ok := expr.(*ast.CompositeLit)
	if !ok {
		return nil
	}
	return lit
}
