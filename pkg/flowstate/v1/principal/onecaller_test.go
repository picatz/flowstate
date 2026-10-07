package principal_test

import (
	"go/ast"
	"go/parser"
	"go/token"
	"io/fs"
	"path/filepath"
	"reflect"
	"strconv"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

// TestOneCallerType pins that who is calling has one CEL-typed spelling. A
// struct that tags a `subject` field for CEL and also one of the other fields
// that describe a caller (`issuer`, `kind`, `claims`, `principal`) is a second
// Caller, and it drifts: the sender of a signal once carried its own
// string-only claims and could not read a list. Only this package may declare
// one; every surface binds [principal.Caller].
//
// A struct that tags `subject` beside workload coordinates only (auth's
// `workload`, the assertion subject that would be minted) describes no caller
// and is not caught.
func TestOneCallerType(t *testing.T) {
	root := filepath.Join("..", "..", "..", "..")
	callerFields := map[string]bool{"issuer": true, "kind": true, "claims": true, "principal": true}
	fset := token.NewFileSet()

	err := filepath.WalkDir(root, func(path string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if d.IsDir() {
			switch d.Name() {
			case ".git", ".claude", "node_modules", "vendor":
				return filepath.SkipDir
			}

			return nil
		}
		slash := filepath.ToSlash(path)
		if !strings.HasSuffix(slash, ".go") || strings.HasSuffix(slash, "_test.go") ||
			strings.HasSuffix(slash, ".pb.go") || strings.Contains(slash, "/pkg/flowstate/v1/principal/") {
			return nil
		}

		file, err := parser.ParseFile(fset, path, nil, 0)
		require.NoError(t, err, path)
		ast.Inspect(file, func(n ast.Node) bool {
			st, ok := n.(*ast.StructType)
			if !ok {
				return true
			}
			var subject bool
			var also []string
			for _, field := range st.Fields.List {
				if field.Tag == nil {
					continue
				}
				raw, err := strconv.Unquote(field.Tag.Value)
				require.NoError(t, err)
				name, _ := reflect.StructTag(raw).Lookup("cel")
				switch {
				case name == "subject":
					subject = true
				case callerFields[name]:
					also = append(also, name)
				}
			}
			if subject && len(also) > 0 {
				t.Errorf("%s declares a CEL caller of its own (subject with %v); bind principal.Caller instead",
					fset.Position(st.Pos()), also)
			}

			return true
		})

		return nil
	})
	require.NoError(t, err)
}
