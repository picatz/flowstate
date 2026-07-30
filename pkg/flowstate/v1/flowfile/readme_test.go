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

// completeWorkflow matches a fenced yaml block that opens with `edition:` at the
// margin, which is what a whole Flowfile looks like and what a fragment does not.
//
// It used to anchor on `name:`, and that stopped being the rule when `edition:`
// became required: a block opening with `name:` is now a *fragment* by the
// language's own definition, and one opening with `edition:` is the whole thing.
// Anchoring on the required key means the pattern says what a Flowfile is rather
// than approximating it — and it is why the block is compiled as written, with no
// edition supplied on its behalf.
//
// The info string must be exactly `yaml`. A block a reader should see but this
// build cannot compile — a design sketch for a phase that has not landed — is
// fenced ```yaml (proposed) instead: renderers highlight on the first word, so it
// still reads as YAML, and it is visibly marked for a human rather than silently
// skipped for a machine.
var completeWorkflow = regexp.MustCompile("(?s)```yaml\n(edition:.*?)```")

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
					// Compiled exactly as written. The edition used to be supplied
					// here, on the theory that a ceremonial first line in every
					// example costs more than it buys — which was true while the key
					// was optional, and stopped being true when it became required.
					// A reader who copies a block out of the README gets a file, and
					// a test that quietly adds the one line the compiler insists on
					// is a test that cannot tell them otherwise.
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
