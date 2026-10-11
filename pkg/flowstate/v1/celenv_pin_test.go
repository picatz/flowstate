package flowstatev1_test

import (
	"go/ast"
	"go/parser"
	"go/token"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/picatz/flowstate/internal/testkit"
)

// TestEveryStringsExtensionIsVersionPinned is the tripwire on a guarantee that
// cannot be tested by running anything.
//
// `ext.Strings()` with no version means MaxUint32 — every function, including
// whatever a future cel-go adds. An environment built that way changes meaning
// when a dependency is bumped, which is precisely the reinterpretation bug the
// profile mechanism exists to prevent for stored expressions. No evaluation test
// can catch it, because against today's cel-go the pinned and unpinned
// environments are identical; the difference only exists in a future nobody can
// import yet. What can be checked is the shape of the call, so the tree is
// walked and every `ext.Strings(...)` is required to carry an
// `ext.StringsVersion(...)` option.
//
// This found one real gap on the day it was written: all three policy surfaces
// pinned version 5 and the workflow core — whose expressions live longest and
// travel furthest — did not. The test exists so the fourth copy of that decision
// cannot quietly become a fifth surface's omission.
func TestEveryStringsExtensionIsVersionPinned(t *testing.T) {
	t.Parallel()

	root := testkit.RepoRoot(t)

	var found int
	require.NoError(t, filepath.WalkDir(root, func(path string, entry os.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if entry.IsDir() {
			name := entry.Name()
			if name == ".git" || name == ".claude" || name == "vendor" || name == "plugins" {
				// plugins are separate modules with their own go.sum and their
				// own cel-go if they take one; this test guards the engine's
				// environments, and a plugin's are its own module's to guard.
				return filepath.SkipDir
			}

			return nil
		}
		if !strings.HasSuffix(path, ".go") || strings.HasSuffix(path, ".pb.go") || strings.HasSuffix(path, "_test.go") {
			return nil
		}

		file, err := parser.ParseFile(token.NewFileSet(), path, nil, 0)
		if err != nil {
			return err
		}

		// The extension package's local name in THIS file, resolved from the
		// import path rather than assumed to be `ext`. Matching the spelling
		// alone would let an aliased import — `celext "…/cel-go/ext"` — build an
		// unpinned environment the walk never sees, while the existing call
		// sites keep the floor satisfied: a tripwire that checks the convention
		// instead of the fact. A dot import makes the calls bare and untraceable
		// by name, so it is refused outright rather than half-matched.
		extName, dotImported := celExtLocalName(file)
		require.False(t, dotImported,
			"%s dot-imports the cel-go ext package, which makes its calls impossible "+
				"to attribute by name; import it qualified so the version pin stays checkable", path)
		if extName == "" {
			return nil
		}

		ast.Inspect(file, func(node ast.Node) bool {
			call, ok := node.(*ast.CallExpr)
			if !ok {
				return true
			}
			selector, ok := call.Fun.(*ast.SelectorExpr)
			if !ok || selector.Sel.Name != "Strings" {
				return true
			}
			if pkg, ok := selector.X.(*ast.Ident); !ok || pkg.Name != extName {
				return true
			}

			found++
			require.True(t, hasStringsVersionOption(call.Args),
				"%s calls ext.Strings without ext.StringsVersion, so the environment it "+
					"builds gains whatever functions a future cel-go ships — an expression "+
					"stored today could mean something else after a dependency bump", path)

			return true
		})

		return nil
	}))

	// Four today: the profile map and the three policy surfaces. The floor is
	// what makes the walk honest — a corpus test that finds nothing passes every
	// assertion above.
	require.GreaterOrEqual(t, found, 4,
		"fewer ext.Strings call sites than expected were found, so this walked the wrong tree")
}

// TestEveryProfileLibraryIsVersionPinned holds the profile's own library table
// to the rule [stringsExtensionVersion] states for every version-gated cel-go
// library, not only strings: an unpinned constructor means "whatever a future
// cel-go adds", and no evaluation test can see that against today's cel-go.
//
// It reads the `extensionLibraries` literal in celenv.go and requires each
// cel-go constructor in it to carry its own `<Name>Version(...)` option whose
// argument is that library's own named `*ExtensionVersion` constant, so a pin cannot be a bare
// number that bypasses the one reviewed place the values live. The table of
// constructors below is the audit: a library cel-go gives a version knob
// appears here, or this fails when a new constructor shows up unpinned.
func TestEveryProfileLibraryIsVersionPinned(t *testing.T) {
	t.Parallel()

	// constructor -> its version option and the one constant that must feed it,
	// for every versioned library in the table.
	versioned := map[string]struct{ option, constant string }{
		"ext.Bindings":             {"ext.BindingsVersion", "bindingsExtensionVersion"},
		"ext.TwoVarComprehensions": {"ext.TwoVarComprehensionsVersion", "comprehensionsExtensionVersion"},
		"ext.Encoders":             {"ext.EncodersVersion", "encodersExtensionVersion"},
		"ext.Lists":                {"ext.ListsVersion", "listsExtensionVersion"},
		"ext.Math":                 {"ext.MathVersion", "mathExtensionVersion"},
		"cel.OptionalTypes":        {"cel.OptionalTypesVersion", "optionalExtensionVersion"},
		"ext.Protos":               {"ext.ProtosVersion", "protosExtensionVersion"},
		"ext.Regex":                {"ext.RegexVersion", "regexExtensionVersion"},
		"ext.Sets":                 {"ext.SetsVersion", "setsExtensionVersion"},
		"ext.Strings":              {"ext.StringsVersion", "stringsExtensionVersion"},
	}

	file, err := parser.ParseFile(token.NewFileSet(), "celenv.go", nil, 0)
	require.NoError(t, err)

	var table *ast.CompositeLit
	ast.Inspect(file, func(node ast.Node) bool {
		spec, ok := node.(*ast.ValueSpec)
		if !ok || len(spec.Names) != 1 || spec.Names[0].Name != "extensionLibraries" || len(spec.Values) != 1 {
			return true
		}
		table, _ = spec.Values[0].(*ast.CompositeLit)

		return false
	})
	require.NotNil(t, table, "extensionLibraries literal not found in celenv.go")

	seen := map[string]bool{}
	ast.Inspect(table, func(node ast.Node) bool {
		call, ok := node.(*ast.CallExpr)
		if !ok {
			return true
		}
		name := selectorName(call.Fun)
		pin, isVersioned := versioned[name]
		if !isVersioned {
			if strings.HasSuffix(name, "Version") {
				return true
			}
			if strings.HasPrefix(name, "ext.") || strings.HasPrefix(name, "cel.Optional") {
				require.Failf(t, "unaudited cel-go library",
					"%s is in extensionLibraries but not in this test's audit table; "+
						"find its version option in cel-go and pin it, or record that it has none", name)
			}

			return true
		}

		seen[name] = true
		require.True(t, isPinnedBy(call, pin.option, pin.constant),
			"%s is built without %s(%s), so an expression stored today could "+
				"gain functions after a cel-go bump, or is fed by the wrong constant", name, pin.option, pin.constant)

		return true
	})

	for name := range versioned {
		require.True(t, seen[name], "%s is not constructed in extensionLibraries; the audit table is stale", name)
	}
}

// TestEveryVersionedCelLibraryCallIsPinnedTree extends the table test to the
// whole tree: a versioned constructor called anywhere outside the profile
// table (a policy environment, a predicate checker) must carry its own
// `<Name>Version(...)` option too. This found `ext.Bindings()` bare in
// signalpredicate.go beside a pinned `ext.Strings`.
//
// Consciously left unpinned: ext.NativeTypes and ext.Network accept a version
// option, but cel-go v0.31.0 gates nothing on either, so a pin would freeze no
// behavior; they are not in the checked set. Add them, with a pin, when a
// cel-go release grows a gate. ext.Strings is covered by its own test above.
func TestEveryVersionedCelLibraryCallIsPinnedTree(t *testing.T) {
	t.Parallel()

	root := testkit.RepoRoot(t)
	var found int
	require.NoError(t, filepath.WalkDir(root, func(path string, entry os.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if entry.IsDir() {
			switch entry.Name() {
			case ".git", ".claude", "vendor", "plugins":
				return filepath.SkipDir
			}

			return nil
		}
		if !strings.HasSuffix(path, ".go") || strings.HasSuffix(path, ".pb.go") || strings.HasSuffix(path, "_test.go") {
			return nil
		}
		src, err := os.ReadFile(path)
		if err != nil {
			return err
		}
		bad, n := unpinnedVersionedCalls(t, path, src)
		found += n
		require.Empty(t, bad, "%s builds a versioned cel-go library without its Version option, so a "+
			"cel-go bump could change what its expressions mean", path)

		return nil
	}))
	require.GreaterOrEqual(t, found, 10, "walked the wrong tree: too few versioned constructor calls found")
}

// TestUnpinnedVersionedCallsAreDetected is the negative case for the tree walk.
func TestUnpinnedVersionedCallsAreDetected(t *testing.T) {
	t.Parallel()

	const src = `package p

import (
	"github.com/google/cel-go/cel"
	celext "github.com/google/cel-go/ext"
)

var _ = []cel.EnvOption{
	celext.Bindings(),
	celext.Math(celext.MathVersion(3)),
	cel.OptionalTypes(),
}
`
	bad, n := unpinnedVersionedCalls(t, "x.go", []byte(src))
	require.Equal(t, 3, n)
	require.Equal(t, []string{"Bindings", "OptionalTypes"}, bad)
}

// versionedConstructors are the cel-go constructors checked tree-wide, keyed
// by import path then constructor name, with the option each must carry.
var versionedConstructors = map[string]map[string]string{
	"github.com/google/cel-go/ext": {
		"Bindings": "BindingsVersion", "TwoVarComprehensions": "TwoVarComprehensionsVersion",
		"Encoders": "EncodersVersion", "Lists": "ListsVersion", "Math": "MathVersion",
		"Protos": "ProtosVersion", "Regex": "RegexVersion", "Sets": "SetsVersion",
	},
	"github.com/google/cel-go/cel": {"OptionalTypes": "OptionalTypesVersion"},
}

// unpinnedVersionedCalls returns the constructors called without their Version
// option in one source file, and how many versioned calls it saw.
func unpinnedVersionedCalls(t *testing.T, path string, src []byte) (bad []string, total int) {
	t.Helper()

	file, err := parser.ParseFile(token.NewFileSet(), path, src, 0)
	require.NoError(t, err)

	locals := map[string]map[string]string{}
	for _, imported := range file.Imports {
		importPath := strings.Trim(imported.Path.Value, `"`)
		ctors, ok := versionedConstructors[importPath]
		if !ok {
			continue
		}
		name := importPath[strings.LastIndex(importPath, "/")+1:]
		if imported.Name != nil {
			require.NotEqual(t, ".", imported.Name.Name, "%s dot-imports %s, which hides its calls", path, importPath)
			name = imported.Name.Name
		}
		locals[name] = ctors
	}
	if len(locals) == 0 {
		return nil, 0
	}

	ast.Inspect(file, func(node ast.Node) bool {
		call, ok := node.(*ast.CallExpr)
		if !ok {
			return true
		}
		selector, ok := call.Fun.(*ast.SelectorExpr)
		if !ok {
			return true
		}
		pkg, ok := selector.X.(*ast.Ident)
		if !ok {
			return true
		}
		option, ok := locals[pkg.Name][selector.Sel.Name]
		if !ok {
			return true
		}
		total++
		for _, arg := range call.Args {
			if optionCall, ok := arg.(*ast.CallExpr); ok {
				if sel, ok := optionCall.Fun.(*ast.SelectorExpr); ok && sel.Sel.Name == option {
					return true
				}
			}
		}
		bad = append(bad, selector.Sel.Name)

		return true
	})

	return bad, total
}

// selectorName renders a `pkg.Name` call target, or "" for any other shape.
func selectorName(expr ast.Expr) string {
	selector, ok := expr.(*ast.SelectorExpr)
	if !ok {
		return ""
	}
	pkg, ok := selector.X.(*ast.Ident)
	if !ok {
		return ""
	}

	return pkg.Name + "." + selector.Sel.Name
}

// isPinnedBy reports whether a constructor call carries `option(constant)` with
// exactly that identifier, so a pin fed by another library's constant, or by a
// bare number, does not count.
func isPinnedBy(call *ast.CallExpr, option, constant string) bool {
	for _, arg := range call.Args {
		optionCall, ok := arg.(*ast.CallExpr)
		if !ok || selectorName(optionCall.Fun) != option || len(optionCall.Args) != 1 {
			continue
		}
		if ident, ok := optionCall.Args[0].(*ast.Ident); ok && ident.Name == constant {
			return true
		}
	}

	return false
}

// TestIsPinnedByRejectsWrongConstant proves the pin check can fail: a version
// fed by another library's constant, a bare number, or no option at all is not
// a pin.
func TestIsPinnedByRejectsWrongConstant(t *testing.T) {
	t.Parallel()

	for src, want := range map[string]bool{
		"ext.Math(ext.MathVersion(mathExtensionVersion))":    true,
		"ext.Math(ext.MathVersion(stringsExtensionVersion))": false,
		"ext.Math(ext.MathVersion(3))":                       false,
		"ext.Math(ext.SetsVersion(mathExtensionVersion))":    false,
		"ext.Math()": false,
	} {
		expr, err := parser.ParseExpr(src)
		require.NoError(t, err)
		call, ok := expr.(*ast.CallExpr)
		require.True(t, ok)
		require.Equal(t, want, isPinnedBy(call, "ext.MathVersion", "mathExtensionVersion"), src)
	}
}

// celExtLocalName returns the name the cel-go ext package is imported under in
// a file, and whether it was dot-imported.
//
// Empty when the file does not import it at all, which is most files and the
// cheap early exit: a file that never imports the package cannot call it, so
// nothing in it needs walking.
func celExtLocalName(file *ast.File) (name string, dotImported bool) {
	for _, imported := range file.Imports {
		if imported.Path.Value != `"github.com/google/cel-go/ext"` {
			continue
		}
		if imported.Name == nil {
			return "ext", false
		}
		if imported.Name.Name == "." {
			return "", true
		}

		return imported.Name.Name, false
	}

	return "", false
}

// hasStringsVersionOption reports whether one of a call's arguments is an
// ext.StringsVersion(...) call.
func hasStringsVersionOption(args []ast.Expr) bool {
	for _, arg := range args {
		call, ok := arg.(*ast.CallExpr)
		if !ok {
			continue
		}
		if selector, ok := call.Fun.(*ast.SelectorExpr); ok && selector.Sel.Name == "StringsVersion" {
			return true
		}
	}

	return false
}
