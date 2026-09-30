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
	"google.golang.org/protobuf/types/known/durationpb"
	"google.golang.org/protobuf/types/known/structpb"
	"google.golang.org/protobuf/types/known/wrapperspb"
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

// legacyTypedDecodes are the files allowed to decode YAML into a hand-written
// Go type, each with the reason and the issue that removes it. The list may
// only shrink: a file here that no longer makes a typed decode fails the test
// below, so a migrated site cannot linger as a standing exemption.
var legacyTypedDecodes = map[string]string{
	// Operator policy files, migrating to flowstate.policy.v1 (#1590).
	"pkg/flowstate/v1/auth/policy.go":        "trust policy, #1590",
	"pkg/flowstate/v1/auth/federation.go":    "federation policy, #1590",
	"pkg/flowstate/v1/netpolicy/config.go":   "egress policy, #1590",
	"pkg/flowstate/v1/taskpolicy_config.go":  "task-shape policy, #1590",
	"pkg/flowstate/v1/plugin/pins_config.go": "plugin pins file, #1590",
	"pkg/flowstate/v1/plugin/env_config.go":  "plugin environment file, #1590",
	"plugins/oidc/providers.go":              "plugin-owned grant file in its own module, #1590",
	"plugins/ssh/grants.go":                  "plugin-owned grant file in its own module, #1590",
	"plugins/docker/grants.go":               "plugin-owned grant file in its own module, #1590",
	"pkg/flowstate/v1/flowtest/bounds.go":    "the *.test.yaml format, migrating to a schema (#923 D9)",
	"pkg/flowstate/v1/flowfile/marshal.go":   "reads back YAML this package just wrote, to prove a scalar round-trips; not a document anyone authors",
}

// TestNewConfigurationIsDefinedInTheSchema refuses a new decode of a YAML
// document into a hand-written Go type.
//
// A configuration an operator or author writes is a shape several tools read,
// and the schema under proto/ is where a shape gets one definition, its
// validation rules, and its documentation together (AGENTS.md, invariant 1).
// A Go struct with yaml tags is a second, unvalidated, undocumented copy of a
// shape nobody wrote down. New formats decode with [UnmarshalProto] into a
// message and are checked with protovalidate; this test is what keeps that from
// being a convention someone has to remember.
func TestNewConfigurationIsDefinedInTheSchema(t *testing.T) {
	t.Parallel()

	const root = "../.."
	typed := map[string]bool{"Unmarshal": true, "UnmarshalStrict": true}
	found := map[string]bool{}

	fset := token.NewFileSet()
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
		rel = filepath.ToSlash(rel)
		if rel == "internal/strictyaml/strictyaml.go" {
			return nil
		}
		file, err := parser.ParseFile(fset, path, nil, parser.SkipObjectResolution)
		if err != nil {
			return err
		}
		alias := ""
		for _, imp := range file.Imports {
			if p, _ := strconv.Unquote(imp.Path.Value); p == "github.com/picatz/flowstate/internal/strictyaml" {
				alias = "strictyaml"
				if imp.Name != nil {
					alias = imp.Name.Name
				}
			}
		}
		if alias == "" {
			return nil
		}
		ast.Inspect(file, func(n ast.Node) bool {
			call, ok := n.(*ast.CallExpr)
			if !ok {
				return true
			}
			sel, ok := call.Fun.(*ast.SelectorExpr)
			if !ok {
				return true
			}
			if pkg, ok := sel.X.(*ast.Ident); ok && pkg.Name == alias && typed[sel.Sel.Name] {
				found[rel] = true
				if _, legacy := legacyTypedDecodes[rel]; !legacy {
					t.Errorf("%s: strictyaml.%s decodes a document into a hand-written Go type. Define the "+
						"format as a message under proto/ with protovalidate rules, decode it with "+
						"strictyaml.UnmarshalProto, and check it with v1.Validate",
						fset.Position(call.Pos()), sel.Sel.Name)
				}
			}
			return true
		})
		return nil
	})
	require.NoError(t, err)

	for rel, reason := range legacyTypedDecodes {
		require.True(t, found[rel],
			"%s is listed as a legacy typed decode (%s) but no longer makes one: remove it from the list", rel, reason)
	}
}

func TestUnmarshalProtoIsStrict(t *testing.T) {
	t.Parallel()

	var got wrapperspb.StringValue
	require.NoError(t, UnmarshalProto([]byte(`"hello"`), &got))
	require.Equal(t, "hello", got.GetValue())

	var st structpb.Struct
	require.NoError(t, UnmarshalProto([]byte("a:\n  b: [1, x]\n"), &st))
	require.Equal(t, "x", st.GetFields()["a"].GetStructValue().GetFields()["b"].GetListValue().GetValues()[1].GetStringValue())

	var dur durationpb.Duration
	require.Error(t, UnmarshalProto([]byte("seconds: 1\nextra: 2\n"), &dur), "an unknown field was accepted")
	require.Error(t, UnmarshalProto([]byte("seconds: x\n"), &dur), "a wrong-typed value was accepted")
	require.Error(t, UnmarshalProto([]byte("seconds: 1\nseconds: 2\n"), &dur), "a duplicate key was accepted")

	// The decoder's containment still applies.
	for _, doc := range crashers {
		require.NotPanics(t, func() { _ = UnmarshalProto([]byte(doc), &st) })
	}
}
