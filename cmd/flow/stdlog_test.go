package main

import (
	"go/parser"
	"go/token"
	"io/fs"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

// stdlibLogAllowed names the non-test files under cmd and pkg that may import
// the standard library's "log", and why each one may.
//
// The invariant this guards is one sentence: **the process has one logger, and
// it is slog**. Seven telemetry warnings used to go through `log.Printf` —
// their own timestamp layout, the level spelled inside the message, never JSON
// when the handler beside them was — and the next contributor reaching for
// `log.Printf` should get this failure rather than a review comment (#1716).
//
// An entry here is a debt with its reason written down, so it can be paid when
// the reason goes away, and a file that stops needing its entry fails the
// ratchet until the entry is removed.
var stdlibLogAllowed = map[string]string{
	// runLSP shapes the standard library's default logger, because slog.Default
	// writes through it and the language server logs through slog.Default when
	// given no logger of its own. It configures that logger; it writes nothing
	// through it.
	"cmd/flow/main.go": "configures the default logger slog.Default writes through, for the language server",
}

// TestNoNonTestFileImportsTheStandardLibraryLog is the guard.
//
// It reads the sources directly rather than shelling out to `go list`, so it
// holds in a sandbox with no module cache to warm, and it reports the file
// rather than the package, which is what a reader needs to fix it. Only the
// exact path "log" matches: "log/slog" is the logger this repository wants.
func TestNoNonTestFileImportsTheStandardLibraryLog(t *testing.T) {
	root := repoRoot(t)

	var importing []string
	fset := token.NewFileSet()
	for _, tree := range []string{"cmd", "pkg"} {
		err := filepath.WalkDir(filepath.Join(root, tree), func(path string, entry fs.DirEntry, err error) error {
			if err != nil {
				return err
			}
			if entry.IsDir() {
				if entry.Name() == "testdata" {
					return filepath.SkipDir
				}

				return nil
			}
			if !strings.HasSuffix(path, ".go") || strings.HasSuffix(path, "_test.go") {
				return nil
			}

			file, err := parser.ParseFile(fset, path, nil, parser.ImportsOnly)
			if err != nil {
				return err
			}

			for _, spec := range file.Imports {
				imported, err := strconv.Unquote(spec.Path.Value)
				if err != nil {
					return err
				}
				if imported == "log" {
					relative, err := filepath.Rel(root, path)
					if err != nil {
						return err
					}
					importing = append(importing, filepath.ToSlash(relative))
				}
			}

			return nil
		})
		require.NoError(t, err)
	}

	seen := map[string]bool{}
	for _, file := range importing {
		seen[file] = true
		if _, allowed := stdlibLogAllowed[file]; allowed {
			continue
		}

		t.Errorf("%s imports the standard library's \"log\".\n"+
			"The process logs through log/slog: a line written through log.Printf has its own timestamp layout, "+
			"carries its level inside the message, and is never JSON when the handler beside it is, so nothing "+
			"parsing the process's other lines can parse it (#1716). Log through the command's *slog.Logger "+
			"instead — infraLogger(), the run log handler, or telemetryLogger for telemetry's own warnings — "+
			"or, if the import is genuinely needed, add the file to stdlibLogAllowed with the reason.", file)
	}

	for file := range stdlibLogAllowed {
		if !seen[file] {
			t.Errorf("%s no longer imports \"log\"; remove it from stdlibLogAllowed so the ratchet tightens", file)
		}
	}
}

// repoRoot walks up from this package to the directory holding go.mod.
//
// The same helper, by the same name, as pkg/flowstate/v1's progress_test.go
// and tools/agentconfig's, because a test helper cannot be imported across
// package boundaries without exporting it; #1709 consolidates the copies.
func repoRoot(t *testing.T) string {
	t.Helper()

	dir, err := os.Getwd()
	require.NoError(t, err)

	for range 10 {
		if _, err := os.Stat(filepath.Join(dir, "go.mod")); err == nil {
			return dir
		}
		parent := filepath.Dir(dir)
		require.NotEqual(t, parent, dir, "walked to the filesystem root without finding go.mod")
		dir = parent
	}

	t.Fatal("go.mod not found within ten directories of the test")

	return ""
}
