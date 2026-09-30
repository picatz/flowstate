package flowdebug_test

import (
	"go/ast"
	"go/parser"
	"go/token"
	"io/fs"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// The capability-construction guard.
//
// A snapshot's [v1.DebugCapabilities] is what a surface believes about the
// backend behind it, and `conformance.CapabilityCases` holds each driver to it.
// That proof is about the two constructors, so a third spelling of the message
// is a set of bits nothing holds to what any driver does: a front that wrote
// its own literal would advertise whatever its author remembered. Fronts read
// capabilities from the snapshot they were handed.
//
// Source-walking rather than reflective, like the scope guard beside the durable
// driver: the question is about the text someone will write next.

// repoRoot is this module's root: pkg/flowstate/v1/flowdebug is four levels down.
const repoRoot = "../../../.."

// capabilityConstructors are the only places outside tests and generated code
// that build a [v1.DebugCapabilities], keyed by module-relative path and
// enclosing function, not by line.
var capabilityConstructors = map[string]string{
	"pkg/flowstate/v1/flowdebug/contract.go#(*Session).capabilitiesLocked": "the local driver: what a session " +
		"does depends on whether it is controlled and has a source map, so it is decided from the session's own state.",

	"pkg/flowstate/v1/debugask.go#DurableDebugCapabilities": "the durable driver: what a run holding one position at " +
		"a time does, which the engine answers every snapshot with.",
}

// neverConstruct are the trees that must read capabilities and never write
// them, even with an entry above.
var neverConstruct = []string{"cmd/flow/", "pkg/flowstate/v1/flowdap/"}

// capabilitySite is one construction of [v1.DebugCapabilities].
type capabilitySite struct {
	pos token.Position
	key string
}

// TestOnlyTheTwoConstructorsBuildDebugCapabilities is the guard itself.
func TestOnlyTheTwoConstructorsBuildDebugCapabilities(t *testing.T) {
	sites := capabilitySitesInTree(t)

	seen := map[string]bool{}
	for _, site := range sites {
		path := strings.SplitN(site.key, "#", 2)[0]
		for _, tree := range neverConstruct {
			if strings.HasPrefix(path, tree) {
				t.Errorf("%s: %s builds a DebugCapabilities, and nothing under %s may: read them from the snapshot "+
					"or the session the front was given (Target's snapshots and Session.Capabilities)", site.pos, site.key, tree)
			}
		}
		if _, ok := capabilityConstructors[site.key]; ok {
			seen[site.key] = true

			continue
		}
		t.Errorf("%s: %s builds a DebugCapabilities. Only the local session's and DurableDebugCapabilities do, because "+
			"conformance.CapabilityCases proves each of those against its driver; read capabilities from a snapshot, or "+
			"if this is a third driver, add it to capabilityConstructors with a case that holds it to what it advertises", site.pos, site.key)
	}
	for key := range capabilityConstructors {
		if !seen[key] {
			t.Errorf("capabilityConstructors lists %s, but no DebugCapabilities is built there any more; delete the entry", key)
		}
	}
	for key, reason := range capabilityConstructors {
		assert.NotEmpty(t, strings.TrimSpace(reason), "%s has no reason; the reason is the record", key)
		for _, tree := range neverConstruct {
			assert.False(t, strings.HasPrefix(key, tree), "%s is exempt under %s, where none may be", key, tree)
		}
	}
}

// TestTheCapabilityGuardWalksSomething: a guard's own failure mode is finding
// nothing and reporting green, so the walk's yield is asserted.
func TestTheCapabilityGuardWalksSomething(t *testing.T) {
	sites := capabilitySitesInTree(t)
	assert.GreaterOrEqual(t, len(sites), len(capabilityConstructors),
		"the walk found fewer constructions than there are constructors, so it is not reaching the source it guards")
}

// TestCapabilitySiteDetectionReadsTheShapesGoAllows is the discovery check: a
// text search for `&v1.DebugCapabilities{` misses an import under another alias,
// a value literal, and new().
func TestCapabilitySiteDetectionReadsTheShapesGoAllows(t *testing.T) {
	const src = `package p

import (
	dbg "github.com/picatz/flowstate/pkg/flowstate/v1"
)

func Pointer() *dbg.DebugCapabilities { return &dbg.DebugCapabilities{Pause: true} }

func Value() dbg.DebugCapabilities { return dbg.DebugCapabilities{} }

func Empty() any { return new(dbg.DebugCapabilities) }

type Methods struct{}

func (*Methods) Built() any { return &dbg.DebugCapabilities{} }

func Twice() {
	_ = dbg.DebugCapabilities{}
	_ = dbg.DebugCapabilities{}
}

// Not constructions: reading one, a type that merely starts with the name, an
// unrelated set of capabilities, and a nil conversion of the type.
func NearMisses(caps *dbg.DebugCapabilities, other Capabilities) any {
	return []any{caps.GetPause(), dbg.DebugCapabilitiesX{}, other, (*dbg.DebugCapabilities)(nil)}
}
`
	fset := token.NewFileSet()
	file, err := parser.ParseFile(fset, "fixture.go", src, 0)
	require.NoError(t, err)

	var got []string
	for _, site := range capabilitySitesInFile(fset, file, "fixture.go") {
		got = append(got, site.key)
	}
	assert.Equal(t, []string{
		"fixture.go#Pointer", "fixture.go#Value", "fixture.go#Empty", "fixture.go#(*Methods).Built",
		"fixture.go#Twice", "fixture.go#Twice",
	}, got)
}

// capabilitySitesInTree walks every non-test, non-generated Go file in the
// module.
func capabilitySitesInTree(t *testing.T) []capabilitySite {
	t.Helper()

	var sites []capabilitySite
	fset := token.NewFileSet()
	require.NoError(t, filepath.WalkDir(repoRoot, func(path string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if d.IsDir() {
			// Dot directories hold other checkouts and tooling state, such as
			// `.claude/worktrees`, which are not this module's source.
			if name := d.Name(); path != repoRoot && (strings.HasPrefix(name, ".") || name == "testdata" || name == "node_modules") {
				return filepath.SkipDir
			}

			return nil
		}
		if !strings.HasSuffix(path, ".go") || strings.HasSuffix(path, "_test.go") {
			return nil
		}
		source, err := os.ReadFile(path)
		if err != nil {
			return err
		}
		if generated(source) {
			return nil
		}
		file, err := parser.ParseFile(fset, path, source, parser.SkipObjectResolution)
		if err != nil {
			return err
		}
		rel, err := filepath.Rel(repoRoot, path)
		if err != nil {
			return err
		}
		sites = append(sites, capabilitySitesInFile(fset, file, filepath.ToSlash(rel))...)

		return nil
	}))

	return sites
}

// generated reports whether source announces itself as generated, in the form
// `go generate` tools are asked to write.
func generated(source []byte) bool {
	for line := range strings.Lines(string(source)) {
		if strings.HasPrefix(line, "// Code generated") && strings.HasSuffix(strings.TrimSpace(line), "DO NOT EDIT.") {
			return true
		}
		if strings.HasPrefix(line, "package ") {
			return false
		}
	}

	return false
}

// capabilitySitesInFile finds each composite literal and new() of a type named
// DebugCapabilities, qualified by any import alias or not, keyed by path and
// enclosing function.
func capabilitySitesInFile(fset *token.FileSet, file *ast.File, path string) []capabilitySite {
	named := func(expr ast.Expr) bool {
		switch typ := expr.(type) {
		case *ast.SelectorExpr:
			return typ.Sel.Name == "DebugCapabilities"
		case *ast.Ident:
			return typ.Name == "DebugCapabilities"
		}

		return false
	}

	var sites []capabilitySite
	for _, decl := range file.Decls {
		fn, ok := decl.(*ast.FuncDecl)
		if !ok || fn.Body == nil {
			continue
		}
		name := fn.Name.Name
		if fn.Recv != nil && len(fn.Recv.List) == 1 {
			name = "(" + types(fn.Recv.List[0].Type) + ")." + name
		}
		ast.Inspect(fn.Body, func(n ast.Node) bool {
			var pos token.Pos
			switch node := n.(type) {
			case *ast.CompositeLit:
				if node.Type != nil && named(node.Type) {
					pos = node.Pos()
				}
			case *ast.CallExpr:
				if fun, ok := node.Fun.(*ast.Ident); ok && fun.Name == "new" && len(node.Args) == 1 && named(node.Args[0]) {
					pos = node.Pos()
				}
			}
			if pos.IsValid() {
				sites = append(sites, capabilitySite{pos: fset.Position(pos), key: path + "#" + name})
			}

			return true
		})
	}
	return sites
}

// types renders a receiver type as it is written: `*Session` or `Session`.
func types(expr ast.Expr) string {
	switch typ := expr.(type) {
	case *ast.StarExpr:
		return "*" + types(typ.X)
	case *ast.Ident:
		return typ.Name
	case *ast.IndexExpr:
		return types(typ.X)
	}

	return "?"
}
