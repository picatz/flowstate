package flowfile_test

import (
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/picatz/flowstate/pkg/flowstate/v1/flowfile"
)

// The README is the first Flowfile most people read, and until now nothing
// checked that it was one.
//
// It was not. `name: multi step hello world` has spaces, and a workflow name is
// used as an identifier, so the complete example a reader meets first was a file
// `flow validate` refuses. That is the same defect as an example that validates
// and does not run, one surface further out: documentation drifts from the
// language exactly the way code does, and only the parts something runs stay
// honest.
//
// So the README's Flowfiles are compiled. Not every snippet — most are fragments
// showing one key, and demanding a whole document for each would push the docs
// toward completeness over clarity, which is the wrong trade for a README. What
// is checked is every block that presents itself as a whole workflow.

// completeWorkflow matches a fenced yaml block that opens with `name:` at the
// margin, which is what a whole Flowfile looks like and what a fragment does not.
var completeWorkflow = regexp.MustCompile("(?s)```yaml\n(name:.*?)```")

// TestREADMEWorkflowsCompile checks the documented Flowfiles against the compiler.
func TestREADMEWorkflowsCompile(t *testing.T) {
	t.Parallel()

	for _, doc := range []string{"README.md", filepath.Join("docs", "DSL.md"), filepath.Join("docs", "ARCHITECTURE.md")} {
		t.Run(doc, func(t *testing.T) {
			t.Parallel()

			path := filepath.Join("..", "..", "..", "..", doc)
			data, err := os.ReadFile(path)
			require.NoError(t, err, "%s moved and this test did not", doc)

			blocks := completeWorkflow.FindAllStringSubmatch(string(data), -1)
			for i, block := range blocks {
				source := block[1]

				// Reported with the workflow's own name rather than an index, so a
				// failure says which example rather than which position.
				name := "block " + strings.TrimSpace(strings.SplitN(source, "\n", 2)[0])
				t.Run(name, func(t *testing.T) {
					// The edition is supplied here rather than written into every
					// fenced block, because a snippet in prose is a *fragment*: it is
					// shown to illustrate one thing, and the paragraph around it does
					// the job a file's header does. Requiring the marker in each would
					// put a ceremonial line at the top of every example in the
					// documentation, which is exactly where the language is judged.
					//
					// What the block still has to be is a workflow this build compiles
					// — the marker changes nothing about that, since it declares the
					// grammar the block is already written in.
					source := "edition: " + flowfile.CurrentEdition + "\n" + source

					ds, err := flowfile.ValidateSource([]byte(source))
					require.NoError(t, err, "%s example %d does not parse:\n%s", doc, i+1, source)
					assert.Empty(t, ds, "%s example %d does not validate:\n%s\n%s", doc, i+1, ds.Error(), source)
				})
			}
		})
	}
}

// TestREADMEHasAWorkflowToCheck guards the guard.
//
// A pattern that stops matching turns the test above into one that passes by
// checking nothing, and a coverage check that silently covers nothing is worse
// than none: it reads as evidence.
func TestREADMEHasAWorkflowToCheck(t *testing.T) {
	t.Parallel()

	data, err := os.ReadFile(filepath.Join("..", "..", "..", "..", "README.md"))
	require.NoError(t, err)

	blocks := completeWorkflow.FindAllStringSubmatch(string(data), -1)
	assert.NotEmpty(t, blocks,
		"no complete workflow found in the README; either it lost its examples or the pattern stopped matching them")
}
