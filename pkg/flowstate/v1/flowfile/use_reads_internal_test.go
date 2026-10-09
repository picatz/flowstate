package flowfile

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	v1 "github.com/picatz/flowstate/pkg/flowstate/v1"
)

// A file that names the same failing child as many times as a file may would read
// it once per use if a failure were forgotten. A
// failing module is read once, however it is named, and its report is not repeated
// more than once per use.
func TestAFailingModuleIsReadOnceHoweverOftenItIsNamed(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	write := func(name, content string) {
		require.NoError(t, os.WriteFile(filepath.Join(dir, name), []byte(content), 0o644))
	}
	head := "edition: " + CurrentEdition + "\nname: m\n"
	uses := func(target string) string {
		var b strings.Builder
		b.WriteString("use:\n")
		for i := range v1.MaxUsesPerFile {
			fmt.Fprintf(&b, "  u%c:\n    path: ./%s\n", 'a'+i, target)
		}

		return b.String()
	}
	write("bad.yaml", head+"types:\n  T:\n    type: nope\n")
	data := []byte("edition: " + CurrentEdition + "\nname: w\n" + uses("bad.yaml") + "steps:\n  - id: a\n    log:\n      message: hi\n")
	path := filepath.Join(dir, "w.yaml")

	session := newModuleSession()
	_, _, err := parse(data, path, nil, new(int), session)
	require.Error(t, err)
	assert.Equal(t, 1, *session.reads, "the failing child is read once, not once per use")
	assert.LessOrEqual(t, *session.reads, v1.MaxModules)
	assert.LessOrEqual(t, strings.Count(err.Error(), "failed to compile"), v1.MaxUsesPerFile,
		"one report per use, not one per path through the tree")
}

// The same, with the failing child at the bottom of a chain that is itself reached
// many times: every distinct module is read at most once.
func TestAChainOfModulesEndingInAFailureIsReadOncePerModule(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	head := "edition: " + CurrentEdition + "\nname: m\n"
	use := func(target string) string {
		var b strings.Builder
		b.WriteString("use:\n")
		for i := range v1.MaxUsesPerFile {
			fmt.Fprintf(&b, "  u%c:\n    path: ./%s\n", 'a'+i, target)
		}

		return b.String()
	}
	files := map[string]string{
		"bad.yaml": head + "types:\n  T:\n    type: nope\n",
		"l2.yaml":  head + use("bad.yaml"),
		"l1.yaml":  head + use("l2.yaml"),
		"w.yaml":   "edition: " + CurrentEdition + "\nname: w\n" + use("l1.yaml") + "steps:\n  - id: a\n    log:\n      message: hi\n",
	}
	for name, content := range files {
		require.NoError(t, os.WriteFile(filepath.Join(dir, name), []byte(content), 0o644))
	}

	session := newModuleSession()
	_, _, err := parse([]byte(files["w.yaml"]), filepath.Join(dir, "w.yaml"), nil, new(int), session)
	require.Error(t, err)
	assert.LessOrEqual(t, *session.reads, 3, "bad, l2 and l1 are each read once")
}
