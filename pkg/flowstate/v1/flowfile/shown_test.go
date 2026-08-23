package flowfile_test

import (
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/picatz/flowstate/pkg/flowstate/v1/flowfile"
)

// R8 is `shown ⊆ canonical ⊂ legal`, and its second clause is the one nothing
// checked: every Flowfile the documentation shows "produces zero tier-4
// findings" (docs/STYLE.md, R8).
//
// The snippets are the sharpest place to hold that line, and the one the charter
// singled out as invisible to every other check this repository runs. An example
// under `examples/` is walked by `flow fix --check`, run by `flow test` and
// compared by `flow breaking`; a fenced block in prose is read by people and by
// nothing else — until [TestREADMEWorkflowsCompile] made the compiler read it,
// and now this, which makes the style charter read it too. A reader copies the
// first complete workflow they meet, so a retired idiom in a README propagates
// further than one in a file nobody is teaching from.
//
// The corpus under `examples/` is deliberately *not* asserted clean here. It is
// not clean, docs/STYLE.md Part III records what it holds, and the leg over it
// lands advisory for exactly the reason the charter gives: a check has to be
// tried against the corpus before it is turned on. The snippets are the half of
// R8 that can be true today, and this is it.
//
// The same block pattern the compile test uses, deliberately: a "shown Flowfile"
// has to mean one thing across the two checks, or a snippet can be compiled and
// not linted by virtue of nothing but which regexp found it.
func TestShownWorkflowsAreLintClean(t *testing.T) {
	t.Parallel()

	for _, doc := range shownDocs {
		t.Run(doc, func(t *testing.T) {
			t.Parallel()

			path := filepath.Join("..", "..", "..", "..", doc)
			data, err := os.ReadFile(path)
			require.NoError(t, err, "%s moved and this test did not", doc)

			// Not required to be non-empty per document: docs/ARCHITECTURE.md
			// shows none today, and demanding one there would be this test
			// deciding what that document is for. The guard against a pattern
			// that quietly stops matching is TestREADMEHasAWorkflowToCheck,
			// over the document that would certainly notice.
			blocks := completeWorkflow.FindAllStringSubmatch(string(data), -1)

			for _, block := range blocks {
				source := block[1]

				name := "block " + strings.TrimSpace(strings.SplitN(source, "\n", 2)[0])
				t.Run(name, func(t *testing.T) {
					wf, positions, err := flowfile.Parse([]byte(source))
					require.NoError(t, err, "%s shows a workflow that does not compile:\n%s", doc, source)

					found := flowfile.Lint(wf, positions)

					rendered := make([]string, 0, len(found))
					for _, finding := range found {
						rendered = append(rendered, finding.String())
					}

					assert.Empty(t, found,
						"%s shows a workflow with tier-4 findings, which R8 forbids:\n%s\n\n%s",
						doc, strings.Join(rendered, "\n"), source)
				})
			}
		})
	}
}

// shownDocs are the documents whose complete Flowfiles are shown to a reader.
//
// One list, read by the compile check and by the lint check, because "what the
// documentation shows" is one set and a second copy of it is a set that loses a
// document the day somebody adds one.
var shownDocs = []string{
	"README.md",
	filepath.Join("docs", "DSL.md"),
	filepath.Join("docs", "ARCHITECTURE.md"),
	filepath.Join("docs", "STYLE.md"),
}
