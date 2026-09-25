package fuzztargets

import (
	"go/ast"
	"go/parser"
	"go/token"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// The boundary guard for #1721. targets.txt's own test holds every Fuzz
// function to the list; this holds the list to the parsers. Two directions:
// a parser listed in boundaries.txt must exist in the tree and its target
// must be a target targets.txt runs, and every exported `Parse*` function or
// `Unmarshal*` method in the directories boundaries.txt names must be listed,
// so a new boundary parser cannot land without a target or a decision.

// TestParsersUnderSeesGenericReceivers pins the receiver shapes the walk
// resolves: a method on a generic type has an index expression for its
// receiver, and a guard that missed it would let an exported Unmarshal on
// such a type land unlisted.
func TestParsersUnderSeesGenericReceivers(t *testing.T) {
	dir := t.TempDir()
	source := []byte(`package fixture

type Box[T any] struct{ v T }

func (b *Box[T]) UnmarshalText(data []byte) error { return nil }

type Pair[K comparable, V any] struct{}

func (p Pair[K, V]) UnmarshalJSON(data []byte) error { return nil }

func ParseThing(data []byte) (Box[int], error) { return Box[int]{}, nil }

type hidden struct{}

func (h *hidden) UnmarshalYAML(data []byte) error { return nil }
`)
	if err := os.WriteFile(filepath.Join(dir, "fixture.go"), source, 0o600); err != nil {
		t.Fatal(err)
	}

	found := parsersUnder(t, dir)
	for _, want := range []string{"Box.UnmarshalText", "Pair.UnmarshalJSON", "ParseThing"} {
		if !found[want] {
			t.Errorf("parsersUnder did not see %s; found %v", want, found)
		}
	}
	if found["hidden.UnmarshalYAML"] {
		t.Error("parsersUnder listed a method on an unexported type, which nothing outside the package can reach")
	}
}

func TestEveryBoundaryParserHasATarget(t *testing.T) {
	boundaries, err := Boundaries()
	if err != nil {
		t.Fatal(err)
	}
	if len(boundaries) == 0 {
		t.Fatal("boundaries.txt lists nothing, so this test cannot fail for the reason it exists")
	}

	targets := map[string]Target{}
	for _, target := range All() {
		targets[target.Name] = target
	}

	found := map[string]map[string]bool{}
	for _, b := range boundaries {
		if _, ok := found[b.Dir]; !ok {
			found[b.Dir] = parsersUnder(t, filepath.Join(repoRoot, b.Dir))
		}
	}

	listed := map[string]map[string]bool{}
	for _, b := range boundaries {
		if listed[b.Dir] == nil {
			listed[b.Dir] = map[string]bool{}
		}
		if listed[b.Dir][b.Parser] {
			t.Errorf("boundaries.txt lists %s in %s twice", b.Parser, b.Dir)
		}
		listed[b.Dir][b.Parser] = true

		if !found[b.Dir][b.Parser] {
			t.Errorf("boundaries.txt lists %s in %s, but no such exported function or method is declared there", b.Parser, b.Dir)
		}
		target, ok := targets[b.Target]
		switch {
		case !ok:
			t.Errorf("%s in %s names target %s, which targets.txt does not list", b.Parser, b.Dir, b.Target)
		case target.Dir != b.Dir:
			t.Errorf("%s in %s names target %s, which targets.txt places in %s; a boundary's target lives beside the parser", b.Parser, b.Dir, b.Target, target.Dir)
		}
	}

	for dir, parsers := range found {
		for parser := range parsers {
			if !isBoundaryShaped(parser) {
				continue
			}
			if !listed[dir][parser] {
				t.Errorf("%s in %s is an exported parser with no entry in tools/fuzztargets/boundaries.txt; give it a fuzz target and list it", parser, dir)
			}
		}
	}
}

// isBoundaryShaped reports whether an exported name is one this guard holds
// to the list: a `Parse*` function, an `Unmarshal*` method, or the one
// binder the list names by hand.
func isBoundaryShaped(name string) bool {
	_, method, isMethod := strings.Cut(name, ".")
	if isMethod {
		return strings.HasPrefix(method, "Unmarshal")
	}
	return strings.HasPrefix(name, "Parse") || name == "BindRunInputs"
}

// parsersUnder is every exported function and method declared in a
// directory's non-test Go files, functions by name and methods as
// Type.Method.
func parsersUnder(t *testing.T, dir string) map[string]bool {
	t.Helper()

	paths, err := filepath.Glob(filepath.Join(dir, "*.go"))
	if err != nil {
		t.Fatal(err)
	}
	if len(paths) == 0 {
		t.Fatalf("no Go files under %s; boundaries.txt names a directory that is not a package", dir)
	}

	names := map[string]bool{}
	fset := token.NewFileSet()
	for _, path := range paths {
		if strings.HasSuffix(path, "_test.go") {
			continue
		}
		file, err := parser.ParseFile(fset, path, nil, parser.SkipObjectResolution)
		if err != nil {
			t.Fatal(err)
		}
		for _, decl := range file.Decls {
			fn, ok := decl.(*ast.FuncDecl)
			if !ok || !fn.Name.IsExported() {
				continue
			}
			if fn.Recv == nil {
				names[fn.Name.Name] = true
				continue
			}
			// The receiver's type name, through a pointer and through the
			// type arguments of a generic receiver (`*Box[T]`, `Pair[K, V]`).
			recv := fn.Recv.List[0].Type
			if star, ok := recv.(*ast.StarExpr); ok {
				recv = star.X
			}
			switch generic := recv.(type) {
			case *ast.IndexExpr:
				recv = generic.X
			case *ast.IndexListExpr:
				recv = generic.X
			}
			if ident, ok := recv.(*ast.Ident); ok && ident.IsExported() {
				names[ident.Name+"."+fn.Name.Name] = true
			}
		}
	}
	return names
}
