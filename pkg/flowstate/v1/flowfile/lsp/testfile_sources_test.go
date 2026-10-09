package lsp

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
)

// TestTestSourceLinesReadsASiblingOncePerPass proves the memo by removing the
// file after the first lookup: a second read would find nothing, so getting the
// lines back can only mean the pass did not touch the disk again. The unreadable
// case is remembered too, so a missing sibling is not retried per problem.
func TestTestSourceLinesReadsASiblingOncePerPass(t *testing.T) {
	t.Parallel()

	path := filepath.Join(t.TempDir(), "testdefaults.yaml")
	require.NoError(t, os.WriteFile(path, []byte("a: 1\nb: 2\n"), 0o600))

	suite := &document{uri: fileURI(filepath.Join(filepath.Dir(path), "suite.test.yaml"))}
	sibling := fileURI(path)

	sources := testSourceLines{}
	first := sources.of(suite, nil, sibling, path)
	require.Equal(t, []string{"a: 1", "b: 2", ""}, first)

	require.NoError(t, os.Remove(path))
	require.Equal(t, first, sources.of(suite, nil, sibling, path),
		"a later problem in the same pass must reuse the lines, not read the file again")

	missing := filepath.Join(filepath.Dir(path), "gone.yaml")
	require.Empty(t, sources.of(suite, nil, fileURI(missing), missing))
	require.NoError(t, os.WriteFile(missing, []byte("x: 1\n"), 0o600))
	require.Empty(t, sources.of(suite, nil, fileURI(missing), missing),
		"a source that could not be read stays unread for the rest of the pass")

	require.Equal(t, []string{"a: 1", "b: 2", ""}, testSourceLines{}.of(
		&document{uri: suite.uri, text: "a: 1\nb: 2\n"}, nil, suite.uri, ""),
		"the suite's own text is used as is")
}
