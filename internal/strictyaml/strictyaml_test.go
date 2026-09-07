package strictyaml

import (
	"go/ast"
	"go/parser"
	"go/token"
	"io/fs"
	"path/filepath"
	"strconv"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

// The two documents the fuzzers minimized, one per boundary they were found
// at (#1721). flowtest's corpus pins a third shape beside that package.
var crashers = []string{
	"egress:\n  A: 0000\n  deny: ! ",
	"issuers:\n  - name: A\n    issuer: 00000000000000000000008\n    audiences: !0000000000\n000000000000\n      -000000000000000000000000000000000000",
}

func TestADecoderPanicIsARefusal(t *testing.T) {
	t.Parallel()

	type expect struct {
		Ran []string `yaml:"ran"`
	}
	type doc struct {
		Egress struct {
			A    string   `yaml:"A"`
			Deny []string `yaml:"deny"`
		} `yaml:"egress"`
		Issuers []struct {
			Name      string   `yaml:"name"`
			Issuer    string   `yaml:"issuer"`
			Audiences []string `yaml:"audiences"`
		} `yaml:"issuers"`
		Tests []struct {
			Expect expect `yaml:"expect"`
		} `yaml:"tests"`
	}

	for _, crasher := range crashers {
		var into doc
		err := UnmarshalStrict([]byte(crasher), &into)
		require.Error(t, err, "%q decoded", crasher)
		require.ErrorIs(t, err, ErrDecoderStopped, "%q", crasher)
		require.Contains(t, err.Error(), "drop the tag")
	}
}

func TestAnOrdinaryDocumentDecodesAndAStrictOneRefusesUnknownKeys(t *testing.T) {
	t.Parallel()

	var into struct {
		Deny []string `yaml:"deny"`
	}
	require.NoError(t, UnmarshalStrict([]byte("deny: [a, b]\n"), &into))
	require.Equal(t, []string{"a", "b"}, into.Deny)

	err := UnmarshalStrict([]byte("denny: [a]\n"), &into)
	require.Error(t, err)
	require.Contains(t, err.Error(), "denny")
	require.NoError(t, Unmarshal([]byte("denny: [a]\n"), &into), "the loose form accepts an unknown key")
}

// TestEveryYAMLDecodeInTheModuleIsContained refuses a bare decode outside this
// package: a parser added with yaml.Unmarshal would be the one decode a
// document could take the process down through.
func TestEveryYAMLDecodeInTheModuleIsContained(t *testing.T) {
	t.Parallel()

	const root = "../.."
	decoders := map[string]bool{"Unmarshal": true, "UnmarshalWithOptions": true, "UnmarshalContext": true, "NewDecoder": true}

	fset := token.NewFileSet()
	var walked int
	err := filepath.WalkDir(root, func(path string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if d.IsDir() {
			name := d.Name()
			if path != root && (strings.HasPrefix(name, ".") || name == "node_modules" || name == "testdata") {
				return filepath.SkipDir
			}
			return nil
		}
		if !strings.HasSuffix(path, ".go") || strings.HasSuffix(path, "_test.go") {
			return nil
		}
		rel, err := filepath.Rel(root, path)
		if err != nil {
			return err
		}
		if filepath.ToSlash(rel) == "internal/strictyaml/strictyaml.go" {
			return nil
		}
		file, err := parser.ParseFile(fset, path, nil, parser.SkipObjectResolution)
		if err != nil {
			return err
		}
		alias := ""
		for _, imp := range file.Imports {
			if p, _ := strconv.Unquote(imp.Path.Value); p == "github.com/goccy/go-yaml" {
				alias = "yaml"
				if imp.Name != nil {
					alias = imp.Name.Name
				}
			}
		}
		if alias == "" {
			return nil
		}
		walked++
		ast.Inspect(file, func(n ast.Node) bool {
			call, ok := n.(*ast.CallExpr)
			if !ok {
				return true
			}
			sel, ok := call.Fun.(*ast.SelectorExpr)
			if !ok {
				return true
			}
			if pkg, ok := sel.X.(*ast.Ident); ok && pkg.Name == alias && decoders[sel.Sel.Name] {
				t.Errorf("%s: yaml.%s decodes a document with no containment; use strictyaml.Unmarshal or UnmarshalStrict",
					fset.Position(call.Pos()), sel.Sel.Name)
			}
			return true
		})
		return nil
	})
	require.NoError(t, err)
	require.Positive(t, walked, "no file importing goccy/go-yaml was walked; the root is wrong, not the module")
}
