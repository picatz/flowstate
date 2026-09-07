package flowstatev1_test

import (
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

// errorTextMatch is the shape of deciding something by an error's text:
// `strings.Contains(err.Error(), …)` and its spellings.
var errorTextMatch = regexp.MustCompile(`\bContains\(\s*[A-Za-z_][A-Za-z0-9_.]*\.Error\(\)`)

// errorTextMatchAllowed names the sites that may read an error's text, each
// with the reason the value it would match on does not exist.
var errorTextMatchAllowed = map[string]string{
	// The port is taken in the child process the SDK starts; its report
	// reaches this process as text, and the SDK's own error wraps a dial
	// failure rather than a syscall of this process's own. See devStartError.
	"cmd/flow/serverdev.go": "a child process's report, with no error value to match",

	// `expect.error_contains` and `expect.compensated` are a test file's own
	// assertions about the run's rendered failure; matching the text is the
	// feature, not a shortcut past a value.
	"pkg/flowstate/v1/flowtest/run.go": "a test case's declared assertion on the rendered error",

	// The denial is asserted on both drivers, and on the durable one it has
	// crossed Temporal's failure conversion, which carries the text and not
	// the Go value.
	"pkg/flowstate/v1/internal/conformance/undoidentity.go": "an error read back across the Temporal failure boundary",

	// os.Root's refusal has no exported sentinel (errPathEscapes is exported
	// for the standard library's own tests only).
	"pkg/flowstate/v1/secrets/file.go": "os.Root exports no sentinel for a path that escapes",

	// go-git builds the non-fast-forward refusal with fmt.Errorf and exports
	// no sentinel for it.
	"plugins/git/errors.go": "go-git exports no sentinel for a non-fast-forward update",
}

// TestNoDecisionIsMadeOnAnErrorsText keeps #1671 from returning: a walk once
// decided "the run is over" by `strings.Contains` on a sentinel's sentence, so
// an unrelated error quoting it ended the walk as a success. A sentinel is a
// value, and `errors.Is` is the exact test; every non-test file under the
// repository's Go trees is held to it, with an allowlist for the sites that
// have text and no value, each with its reason written beside it.
func TestNoDecisionIsMadeOnAnErrorsText(t *testing.T) {
	t.Parallel()

	root := repoRootDir(t)

	var offenders []string
	for _, tree := range []string{"cmd", "pkg", "tools", "plugins"} {
		err := filepath.WalkDir(filepath.Join(root, tree), func(path string, d os.DirEntry, err error) error {
			if err != nil {
				return err
			}
			if d.IsDir() || !strings.HasSuffix(path, ".go") || strings.HasSuffix(path, "_test.go") {
				return nil
			}
			data, err := os.ReadFile(path)
			if err != nil {
				return err
			}
			if !errorTextMatch.Match(data) {
				return nil
			}
			rel, err := filepath.Rel(root, path)
			require.NoError(t, err)
			rel = filepath.ToSlash(rel)
			if _, allowed := errorTextMatchAllowed[rel]; allowed {
				return nil
			}
			offenders = append(offenders, rel)
			return nil
		})
		require.NoError(t, err)
	}

	require.Empty(t, offenders,
		"these files decide something by an error's text; compare the value with errors.Is, or "+
			"add the file to errorTextMatchAllowed with the reason no value exists")
}
