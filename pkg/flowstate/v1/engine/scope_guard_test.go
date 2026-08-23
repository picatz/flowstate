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
// # What counts as a construction
//
// Two spellings, because [v1.NewScope] takes a profile and step outputs and *no
// identity* — so a scope it returns carries none until someone assigns one, and a
// guard watching only composite literals would be blind to the more likely way a
// future dispatch site gets written. Both are held to the same rule: carry an
// identity, or be listed below with the reason it cannot mis-route a tenant.
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

	// The [v1.NewScope] callers that build a scope for one in-process evaluation
	// and hand it straight to the evaluator. None of them is reachable from a
	// dispatch: each is either a convenience wrapper over an *InScope form the
	// engine calls instead, or an evaluation that happens before a run exists.
	"pkg/flowstate/v1/eval.go#EvalCondition": "a convenience wrapper that evaluates a condition in-process against the " +
		"first profile; the engine reaches EvalConditionInScope with the scope the run already built.",

	"pkg/flowstate/v1/eval.go#(*Task).Eval": "the same shape for a task: reached only for a task with nothing " +
		"profile-sensitive to evaluate, and anything that does evaluate an expression arrives through Task.EvalInScope " +
		"with the run's scope.",

	"pkg/flowstate/v1/nodes.go#EvalVars": "the local evaluation of a `vars:` block. Its durable counterpart is the " +
		"WorkflowVars *activity*, and that one's scope is assembled at its dispatch site — with the identity, which is " +
		"the pair of sites #676 was about.",

	"pkg/flowstate/v1/webhook.go#BindWebhookTriggerInputs": "a trigger is evaluated before there is a run to have an " +
		"identity — the scope holds `event` and nothing else, deliberately, and the site says so.",

	"pkg/flowstate/v1/flowtest/run.go#runCase": "the transcript's redaction set (#929) is built from the case's bound " +
		"inputs through the same sensitiveNativeValues the stub diagnostics use, which reads a scope's inputs and " +
		"nothing else; the scope exists for that one in-process read and is never an activity argument — `flow test` " +
		"runs the local driver only (#155) and dispatches nothing.",

	// The two in this package's own tests, which the walk reaches because
	// engine tests are in scope. Neither builds a scope that is dispatched: one
	// assembles an executor to drive a single method, the other hands a scope
	// straight to the function under test.
	"pkg/flowstate/v1/engine/execute_internal_test.go#runTaskProbe": "assembles an executor in-process to run one node " +
		"through executor.runTask; the activity it dispatches carries a *v1.Task and identity of its own, and this scope " +
		"stays in the test.",

	"pkg/flowstate/v1/engine/workflow_internal_test.go#TestResolveTaskInputs_PreResolveValueExprs": "the scope is an " +
		"argument to v1.ResolveTaskInputs, evaluated in-process; nothing dispatches it.",

	"pkg/flowstate/v1/engine/workflow_internal_test.go#TestResolveTaskInputs_MixedTypes_Table": "the same, for the table " +
		"half of that pair.",
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

		t.Errorf(`%s: this %s does not carry the run identity.

A scope that names no identity is read as the *default tenant* (tenantArg, in
pkg/flowstate/v1/engine/tenant.go), so if this one is dispatched to an activity a
worker started with "flow worker --tenant team-a" refuses it — including team-a's
own run's vars or its own run's compensation.

Either give it the run's identity, the way its neighbours do — in the literal, or
assigned to the variable it is bound to before it travels:

    Identity: e.identity            // executor, for a dispatch it makes
    Identity: st.GetIdentity()      // the run state, at the top of a segment
    Identity: e.scope.GetIdentity() // deriving from the scope a run already has

or, if this scope genuinely never travels to an activity, add

    %q: "…why it cannot…",

to exemptScopeConstruction in %s, with the reason. See #737.`,
			site.pos, site.kind, site.key, "pkg/flowstate/v1/engine/scope_guard_test.go")
	}

	for key := range exemptScopeConstruction {
		if !seen[key] {
			t.Errorf("exemptScopeConstruction lists %s, but no scope is constructed there any more; delete the entry", key)
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
		t.Fatalf("walked the tree and found %d scope constructions, fewer than the %d exemptions listed: "+
			"the walk is not reaching the source it is supposed to guard", len(sites), len(exemptScopeConstruction))
	}

	dispatch := 0
	for _, site := range sites {
		if site.kind == scopeLiteral &&
			strings.HasPrefix(site.key, "pkg/flowstate/v1/engine/") &&
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

	// And the constructor arm reaches something too, since it is the arm that
	// exists for the site nobody has written yet.
	constructed := 0
	for _, site := range sites {
		if site.kind == scopeConstructor {
			constructed++
		}
	}
	if constructed == 0 {
		t.Error("no v1.NewScope call was found anywhere in the walked set, so that half of the guard is watching nothing")
	}
}

// TestScopeSiteDetectionReadsTheShapesGoAllows is the discovery check the same
// argument as tools/fuzztargets' applies to: a guard is only as good as its idea of
// what it is looking for, and a text search for "&v1.Scope{" misses an import under
// another alias, a value (rather than pointer) literal, a scope built by
// [v1.NewScope] instead of written out, and the shape that matters most — a literal
// that is bare on the line it is written and given its identity two statements
// later, which is how nodes.go's derivation helpers are written and is not a defect.
//
// The `Shadowed` case is the hole this had on review: tracking "was an identity
// assigned to something spelled `scope` anywhere in this function" excuses a bare
// literal in one block because a *different* variable of the same name was given an
// identity in another. Bindings are therefore compared by identity, not by
// spelling, and this fixture fails without that.
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

// Two lexical scopes, one spelling. The second literal is bare and must be
// reported as bare, however identified the first block's variable is.
func Shadowed() {
	{
		scope := &fs.Scope{Profile: "default"}
		scope.Identity = nil
		fmt.Println(scope)
	}
	{
		scope := &fs.Scope{Profile: "default"}
		fmt.Println(scope)
	}
}

func ValueLiteral() fs.Scope { return fs.Scope{Profile: "default"} }

// Built by the constructor, which takes no identity: one given one afterwards,
// one handed straight to a caller.
func ViaConstructor() *fs.Scope {
	scope := fs.NewScope("default", nil)
	scope.Identity = nil
	return scope
}

func ViaConstructorBare() *fs.Scope { return fs.NewScope("default", nil) }

// Not scope constructions: an unrelated type whose name ends in Scope, a
// different message on the same package, and a slice whose elements are variables.
type refScope struct{ steps int }

func NearMisses(s *fs.Scope) any {
	return []any{refScope{}, fs.UndoScope(0), []*fs.Scope{s}}
}
`
	fset := token.NewFileSet()
	file, err := parser.ParseFile(fset, "scope_fixture.go", src, 0)
	if err != nil {
		t.Fatal(err)
	}

	var got []string
	for _, site := range scopeSitesInFile(fset, file, "scope_fixture.go") {
		got = append(got, fmt.Sprintf("%s %s=%t", site.key, site.kind, site.carriesIdentity))
	}

	want := []string{
		"scope_fixture.go#Bare scope literal=false",
		"scope_fixture.go#Carrying scope literal=true",
		"scope_fixture.go#AssignedAfterwards scope literal=true",
		// Two literals in one function: the second is the one given an
		// identity, and the first must not inherit that from its neighbour.
		"scope_fixture.go#AssignedToSomethingElse scope literal=false",
		"scope_fixture.go#AssignedToSomethingElse scope literal=true",
		// The shadowing shape: same spelling, different binding.
		"scope_fixture.go#Shadowed scope literal=true",
		"scope_fixture.go#Shadowed scope literal=false",
		"scope_fixture.go#ValueLiteral scope literal=false",
		"scope_fixture.go#ViaConstructor v1.NewScope call=true",
		"scope_fixture.go#ViaConstructorBare v1.NewScope call=false",
	}
	if !slices.Equal(got, want) {
		t.Errorf("detected:\n\t%s\nwant:\n\t%s", strings.Join(got, "\n\t"), strings.Join(want, "\n\t"))
	}
}

// The two ways a scope comes into being, named so a failure says which one it is
// looking at — the advice differs.
const (
	scopeLiteral     = "scope literal"
	scopeConstructor = "v1.NewScope call"
)

// scopeSite is one construction of a [v1.Scope] in the source.
type scopeSite struct {
	key             string // module-relative path + "#" + enclosing function
	kind            string // scopeLiteral or scopeConstructor
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

		// Parsed *with* object resolution, which is the whole of how a binding
		// is told from a spelling below; see scopeSitesInFile.
		file, err := parser.ParseFile(fset, path, nil, 0)
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

// scopeSitesInFile returns the scopes one parsed file constructs.
//
// The type is recognised two ways, and both are needed: qualified through whatever
// name the file imports pkg/flowstate/v1 under, and bare in the package that
// declares it. Files that do neither are skipped outright, which is what keeps the
// flowfile package's own refScope, stepScope, taskScope and loopScope — four types
// whose names end the same way — out of this.
//
// # Bindings, not spellings
//
// A scope is often built bare and given its identity a statement or two later —
// nodes.go's WithLocal, WithLocals, WithAmbientVars and WithOutputs are all written
// that way, and each is correct — so an assignment to `x.Identity` has to be able to
// vouch for the literal `x` was bound from. Matching on the *name* `x` is the
// obvious way to do that and is wrong: two blocks in one function may each declare
// their own `scope`, and the first one's assignment would then excuse the second
// one's bare literal. That is a hole a guard gets trusted about and then lied to
// through, so the vouching is by binding — go/parser's own resolution, where each
// lexical declaration is a distinct *ast.Object and a use points at the declaration
// it actually refers to.
//
// go/types would answer the same question with more ceremony: it needs the package
// type-checked, which means an importer and a build of everything this module
// imports — and x/tools, where the convenient loader lives, is not a dependency of
// this module — for a lint that only ever asks about locals inside one function.
// The parser's resolution is file-local, which is exactly the span this question
// lives in: a literal and the assignment that identifies it are in one function or
// the vouching does not happen at all.
//
// That is why the two maps below carry a `//lint:ignore SA1019`. ast.Object is
// deprecated, and the deprecation's own stated reason is that an Ident's meaning
// can require type information — its example is `T{K: 0}`, where `K` is a field or
// a value depending on what `T` is. The only Idents read here are the local a
// construction is assigned to and the `x` in `x.Identity = …`, both of which are
// plain variable references the parser resolves exactly. The caveat is real and
// does not reach this use; an unresolved binding is treated as un-vouched anyway,
// so the failure direction is to ask for a reason rather than to invent one.
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

	if qualifier == "_" || qualifier == "." {
		// Neither spelling reaches the type under a name this can match; a
		// dot-import would need a different check and nothing in the tree
		// does it.
		qualifier = ""
	}
	if qualifier == "" && !bare {
		return nil
	}

	// named reports whether an expression names `Scope` or `NewScope` — bare in
	// the declaring package, qualified anywhere else.
	named := func(expr ast.Expr, name string) bool {
		switch t := expr.(type) {
		case *ast.Ident:
			return bare && t.Name == name
		case *ast.SelectorExpr:
			pkg, ok := t.X.(*ast.Ident)
			return ok && qualifier != "" && pkg.Name == qualifier && t.Sel.Name == name
		}
		return false
	}

	var sites []scopeSite
	for _, decl := range file.Decls {
		fn, ok := decl.(*ast.FuncDecl)
		if !ok || fn.Body == nil {
			continue
		}

		// The bindings this function assigns an identity to, and the binding
		// each construction is bound to. Both collected over the whole body
		// first, because the assignment legitimately comes after the
		// construction.
		//
		//lint:ignore SA1019 deliberate: see "Bindings, not spellings" on this function for why the parser's own resolution is the right tool here and go/types the wrong one.
		identified := map[*ast.Object]bool{}

		//lint:ignore SA1019 the other half of the same decision.
		boundTo := map[ast.Node]*ast.Object{}
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
				if target, ok := sel.X.(*ast.Ident); ok && target.Obj != nil {
					identified[target.Obj] = true
				}
			}

			for i, rhs := range assign.Rhs {
				if i >= len(assign.Lhs) {
					break
				}
				target, ok := assign.Lhs[i].(*ast.Ident)
				if !ok || target.Obj == nil {
					continue
				}
				if lit := compositeLitOf(rhs); lit != nil {
					boundTo[lit] = target.Obj
					continue
				}
				if call, ok := rhs.(*ast.CallExpr); ok {
					boundTo[call] = target.Obj
				}
			}
			return true
		})

		// vouched reports whether the binding a construction was assigned to is
		// one this function gives an identity to. An unresolved binding counts
		// as un-vouched, which is the fail-closed direction: the guard asks for
		// a reason rather than assuming one.
		vouched := func(n ast.Node) bool {
			obj, ok := boundTo[n]
			return ok && identified[obj]
		}

		label := rel + "#" + funcLabel(fn)
		record := func(n ast.Node, kind string, carries bool) {
			// Positioned against the module-relative path rather than the
			// walk's own `../../../..` prefix, so the failure names a file
			// the way an editor and a review comment do.
			at := fset.Position(n.Pos())
			sites = append(sites, scopeSite{
				key:             label,
				kind:            kind,
				pos:             fmt.Sprintf("%s:%d:%d", rel, at.Line, at.Column),
				carriesIdentity: carries,
			})
		}

		ast.Inspect(fn.Body, func(n ast.Node) bool {
			switch node := n.(type) {
			case *ast.CompositeLit:
				if !named(node.Type, "Scope") {
					return true
				}
				carries := vouched(node)
				for _, elt := range node.Elts {
					kv, ok := elt.(*ast.KeyValueExpr)
					if !ok {
						continue
					}
					if key, ok := kv.Key.(*ast.Ident); ok && key.Name == "Identity" {
						carries = true
					}
				}
				record(node, scopeLiteral, carries)
			case *ast.CallExpr:
				if !named(node.Fun, "NewScope") {
					return true
				}
				// No arm for an identity in the argument list: NewScope's
				// parameters are a profile and step outputs, so a call can
				// only be vouched for by what happens to its result.
				record(node, scopeConstructor, vouched(node))
			}
			return true
		})
	}
	return sites
}

// funcLabel names a function the way a person would write it down, receiver
// included, so that two methods called Eval in one file are two keys.
func funcLabel(fn *ast.FuncDecl) string {
	if fn.Recv == nil || len(fn.Recv.List) == 0 {
		return fn.Name.Name
	}

	recv := ""
	switch t := fn.Recv.List[0].Type.(type) {
	case *ast.StarExpr:
		if id, ok := t.X.(*ast.Ident); ok {
			recv = "*" + id.Name
		}
	case *ast.Ident:
		recv = t.Name
	}
	if recv == "" {
		return fn.Name.Name
	}
	return "(" + recv + ")." + fn.Name.Name
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
