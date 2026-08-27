package flowfile_test

import (
	"bytes"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"

	"github.com/picatz/flowstate/pkg/flowstate/v1/flowfile"
)

// R8's byte-identical clause, over the half of the shown corpus that is not
// `examples/`.
//
// [TestEveryExampleIsAlreadyWhatTheFormatterWrites] holds every workflow under
// `examples/` to the formatter's bytes. The workflows in prose — the one on the
// README's front page, the worked example in docs/DSL.md — were held to nothing,
// and docs/STYLE.md's Part III tabulated which of them were canonical by hand.
// A hand-kept table of what a tool says is a second declaration of the same
// facts, and the failure it has is the one #850 opened with: it was measured
// once, at `c4ead7c`, and every number in it was stale by the time anyone read
// it again.
//
// So this is that table derived rather than written down. The claims are the
// same two the corpus test makes and for the same reasons — bytes, because
// byte-identity is the whole content of R8, and [proto.Equal] over the compiled
// workflow, because bytes that never move say nothing about meaning and both
// `flow fix` corruptions on record produced files that validated perfectly.
// Neither claim is "it still validates", which is the check that let those
// through (CLAUDE.md, the rewriter section).
//
// Read out of [shownDocs], the same list [TestREADMEWorkflowsCompile] and
// [TestShownWorkflowsAreLintClean] read, because "the documents that show a
// Flowfile" is one set and a third copy of it is a set that loses a document the
// day somebody adds one.
func TestShownWorkflowsAreCanonical(t *testing.T) {
	t.Parallel()

	seen := map[string]bool{}

	for _, doc := range shownDocs {
		for _, block := range shownWorkflows(t, doc) {
			t.Run(block.id, func(t *testing.T) {
				t.Parallel()

				workflow, _, err := flowfile.Parse(block.source)
				require.NoError(t, err, "%s shows a workflow that does not compile:\n%s", doc, block.source)

				formatted, err := flowfile.Format(block.source, workflow)

				if holdout, ok := notYetCanonical[block.id]; ok {
					assertStillAHoldout(t, block, holdout, formatted, err)
					return
				}

				require.NoError(t, err,
					"the formatter refuses a workflow shown in %s. A refusal names the position, and the fix is "+
						"the one it names — or an entry in notYetCanonical saying in writing why not", doc)

				if !bytes.Equal(formatted, block.source) {
					assert.Equal(t, string(block.source), string(formatted),
						"the workflow shown in %s is not what `flow fmt` writes for it. Either the block was "+
							"written by hand into a shape the formatter does not write, or canon moved and the "+
							"snippet needs the same reformat `examples/` got in the same commit", doc)
				}

				after, _, err := flowfile.Parse(formatted)
				require.NoError(t, err,
					"formatting the workflow shown in %s wrote a document that no longer compiles", doc)

				if !proto.Equal(workflow, after) {
					assert.Equal(t, workflow.String(), after.String(),
						"formatting the workflow shown in %s changed the workflow it compiles to, which is the "+
							"formatter rewriting what the snippet means rather than how it is written", doc)
				}
			})

			seen[block.id] = true
		}
	}

	// A holdout naming a block that no longer exists is an exemption nothing can
	// ever retire: the snippet was renamed or deleted, and the entry stays
	// forever excusing nothing. Checked here rather than per-block, because the
	// question is about the whole set.
	for id := range notYetCanonical {
		assert.True(t, seen[id],
			"notYetCanonical names %q, and no shown workflow has that id any more — the snippet moved and this "+
				"exemption did not, so delete it", id)
	}
}

// TestTheCanonicalClaimFailsOnANonCanonicalSnippet is the falsifier for the test
// above, and it exists because that test now passes with nothing exempted.
//
// A green check over a corpus that happens to be canonical says nothing about
// whether the check *can* fail: had [shownWorkflows] silently stopped finding
// blocks, or had the comparison been made against the formatter's own output on
// both sides, every subtest would still be green. So both shapes the claim can
// fail in are driven here, over blocks written by hand rather than read out of a
// document, and each is put through the same [assertStillAHoldout] the map's
// entries were checked with — which is what keeps that machinery honest while
// the map is empty.
func TestTheCanonicalClaimFailsOnANonCanonicalSnippet(t *testing.T) {
	t.Parallel()

	// Non-canonical, not wrong: the keys under `log:` are in the order an author
	// typed them rather than the order Marshal writes them, and there is a blank
	// line the formatter does not keep. It compiles, and `flow fmt` rewrites it.
	differs := shownWorkflow{id: "hand-written#differs", source: []byte(`edition: v2026.3
name: differs

steps:
  - id: say
    log:
      message: hello
      level: info
`)}

	// Refused, and by the defect this change was about: an all-constant mapping
	// under a `${...}` fence is still folded into one expression, because the
	// mapping spelling of it would compile to a *literal* rather than to this
	// expression and so fails [unfoldedStructure]'s verification. The keys are
	// therefore not in the document Marshal writes, and the comment anchored to
	// one of them has nowhere to go.
	refused := shownWorkflow{id: "hand-written#refused", source: []byte(`edition: v2026.3
name: refused
steps:
  - id: notify
    log:
      message: hello
      fields:
        deployment: ${1}
        # a comment
        approved_by: ${2}
`)}

	for _, block := range []shownWorkflow{differs, refused} {
		workflow, _, err := flowfile.Parse(block.source)
		require.NoError(t, err, "%s does not compile, so it says nothing about the formatter", block.id)

		formatted, err := flowfile.Format(block.source, workflow)

		switch block.id {
		case differs.id:
			require.NoError(t, err)
			assert.NotEqual(t, string(block.source), string(formatted),
				"a snippet the formatter rewrites came back byte-identical, so the claim in "+
					"TestShownWorkflowsAreCanonical cannot fail and the shown corpus is unheld")
			assertStillAHoldout(t, block, "differs", formatted, err)
		case refused.id:
			require.Error(t, err,
				"a snippet whose comment cannot be carried was formatted rather than refused, so the "+
					"refusal half of the claim is unheld")
			assertStillAHoldout(t, block, "refused", formatted, err)
		}
	}
}

// notYetCanonical are the shown workflows `flow fmt` does not write today, each
// with the reason, so that a snippet outside this map is held to bytes from the
// day it is added.
//
// A known-failure list earns its place only by being falsifiable, so an entry
// here asserts what it claims: the block is checked to be *still* not canonical,
// in the shape the entry names. Fix one and this test fails until the entry is
// deleted, which is the opposite of a skip.
//
// It is empty, and that is the state R8 asks for: every complete workflow shown
// in prose is byte-identical to what `flow fmt` writes for it. The two entries
// it held were the same open decision on #850, and both closed with it. The
// worked example in docs/DSL.md was refused because `compiler.composite` folds a
// mapping holding a `${...}` into one expression, so the key its comment was
// anchored to was not in the document Marshal wrote; [unfoldedStructure] writes
// that mapping back and the comment has somewhere to land again. The README's
// `approval-gate` differed over one folded block scalar, and the answer to that
// was the README's rather than the formatter's — `flow fmt` does not fold, so
// the snippet was rewritten to the line the formatter writes.
//
// The map stays because it is the machinery a *future* holdout needs, and
// because it is what makes an exemption a decision in writing rather than a
// silence. [TestTheCanonicalClaimFailsOnANonCanonicalSnippet] is what keeps the
// claim itself falsifiable now that nothing is exempt from it.
var notYetCanonical = map[string]string{}

// assertStillAHoldout checks that a block in [notYetCanonical] fails in the way
// its entry claims, rather than merely failing.
//
// The distinction matters: "refused" and "differs" are two different defects
// with two different fixes, and an entry that says one while the block does the
// other is a stale exemption reading as a current one.
func assertStillAHoldout(t *testing.T, block shownWorkflow, holdout string, formatted []byte, err error) {
	t.Helper()

	switch holdout {
	case "refused":
		require.Error(t, err,
			"%s is listed as one the formatter refuses and it no longer refuses it — delete its "+
				"notYetCanonical entry and let this test hold it to bytes", block.id)
	case "differs":
		require.NoError(t, err,
			"%s is listed as merely non-canonical and the formatter now refuses it outright, which is a "+
				"worse failure than the entry claims", block.id)
		require.False(t, bytes.Equal(formatted, block.source),
			"%s is listed as not canonical and it now is — delete its notYetCanonical entry", block.id)
	default:
		t.Fatalf("notYetCanonical[%q] is %q, which is neither \"refused\" nor \"differs\"", block.id, holdout)
	}
}

// A shownWorkflow is one complete Flowfile a document shows, with the bytes it
// is written as.
type shownWorkflow struct {
	// id names the block as `document#workflow-name` — stable across an edit
	// that moves a block, unlike an index, and readable in a test name and in a
	// notYetCanonical entry alike.
	id string

	source []byte
}

// shownWorkflows reads the complete workflows out of one document.
//
// [completeWorkflow] is the same pattern the compile and lint checks use, for
// the reason readme_test.go gives: a "shown Flowfile" has to mean one thing
// across the checks over it, or a snippet can be compiled and not formatted by
// virtue of nothing but which regexp found it.
func shownWorkflows(t *testing.T, doc string) []shownWorkflow {
	t.Helper()

	data, err := os.ReadFile(filepath.Join("..", "..", "..", "..", doc))
	require.NoError(t, err, "%s moved and this test did not", doc)

	var out []shownWorkflow
	taken := map[string]bool{}
	for _, block := range completeWorkflow.FindAllStringSubmatch(string(data), -1) {
		source := block[1]

		name := "unnamed"
		for _, line := range strings.Split(source, "\n") {
			if rest, found := strings.CutPrefix(line, "name:"); found {
				name = strings.TrimSpace(rest)
				break
			}
		}

		// Two blocks in one document under one name would share an id, and a
		// holdout entry naming it would then excuse whichever the map was asked
		// about — an exemption covering a block nobody chose.
		id := filepath.ToSlash(doc) + "#" + name
		require.False(t, taken[id],
			"%s shows two complete workflows named %q, so neither can be addressed on its own; rename one", doc, name)
		taken[id] = true

		out = append(out, shownWorkflow{id: id, source: []byte(source)})
	}
	return out
}
