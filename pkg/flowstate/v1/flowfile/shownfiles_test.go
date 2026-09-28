package flowfile_test

import (
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/picatz/flowstate/pkg/flowstate/v1/flowtest"
)

// TestShownTestFilesLoad holds the complete `*.test.yaml` documents a page
// shows to the loader `flow test` uses.
//
// A tutorial that teaches testing shows a test file beside its workflow, and a
// reader copies both. The workflow half is compiled by
// [TestREADMEWorkflowsCompile]; this is the other half, so a test file with a
// misspelled key or a retired field fails here rather than in the reader's
// terminal. Loading checks the file's own grammar and nothing that needs the
// workflow beside it, which is what a block on a page can promise.
func TestShownTestFilesLoad(t *testing.T) {
	t.Parallel()

	for _, doc := range shownDocs {
		data, err := os.ReadFile(filepath.Join("..", "..", "..", "..", doc))
		require.NoError(t, err, "%s moved and this test did not", doc)

		for i, source := range shownTestSources(string(data)) {
			_, err := flowtest.LoadSource([]byte(source))
			assert.NoError(t, err, "%s test file %d does not load:\n%s", doc, i+1, source)
		}
	}
}

// mirrorMarker pins the fenced block after it to a file in the repository:
//
//	<!-- mirrors: examples/release-approval/workflow.yaml -->
//	```yaml
//	...
//	```
//
// The block must be the file's bytes exactly. A page that walks a reader
// through an example shows the example itself, and the example is what CI
// runs, tests, formats and replays on both drivers; the marker is what makes
// the copy on the page the same claim rather than a second one that drifts.
var mirrorMarker = regexp.MustCompile("(?s)<!-- mirrors: ([^ ]+) -->\n```[a-z]*\n(.*?)```")

// TestShownBlocksMirrorTheirSources checks every mirrors marker in the shown
// documents against the file it names.
func TestShownBlocksMirrorTheirSources(t *testing.T) {
	t.Parallel()

	root := filepath.Join("..", "..", "..", "..")

	var found int
	for _, doc := range shownDocs {
		data, err := os.ReadFile(filepath.Join(root, doc))
		require.NoError(t, err, "%s moved and this test did not", doc)

		// A marker the pattern cannot pair with a block would make the check
		// silently vacuous for that block, so the two counts must agree.
		markers := strings.Count(string(data), "<!-- mirrors: ")
		matches := mirrorMarker.FindAllStringSubmatch(string(data), -1)
		assert.Len(t, matches, markers,
			"%s has a `<!-- mirrors: path -->` marker not followed directly by a fenced block", doc)

		for _, match := range matches {
			path, shown := match[1], match[2]
			found++

			source, err := os.ReadFile(filepath.Join(root, filepath.FromSlash(path)))
			if !assert.NoError(t, err, "%s mirrors %s, which cannot be read", doc, path) {
				continue
			}

			assert.Equal(t, string(source), shown,
				"%s shows %s, and the block no longer matches the file; copy the file into the page "+
					"(or change the file, and let its own tests say whether that was right)", doc, path)
		}
	}

	assert.Positive(t, found,
		"no shown document carries a mirrors marker; the tutorial lost them or the pattern stopped matching")
}
