package reference

import (
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestDSLTOCHasNoDrift is TestTheMirrorMatchesTheRepository's counterpart for
// the contents list rather than the whole document.
//
// That test only compares the mirror against docs/DSL.md, so a heading added
// to the source without running `go generate` would still pass it — the
// mirror and the source would agree with each other while both disagreed
// with reality. This regenerates the list from docs/DSL.md's own headings
// and asserts the result is byte-identical to what is committed, the same
// "generate, then diff" discipline CLAUDE.md's gate applies to `buf
// generate` and `flow docs generate`.
//
// Skipped, not failed, outside a checkout — the same reason
// TestTheMirrorMatchesTheRepository is: this package is also built from a
// module cache where docs/ was never shipped.
func TestDSLTOCHasNoDrift(t *testing.T) {
	t.Parallel()

	dslPath := filepath.Join(repoRoot, "docs", "DSL.md")

	original, err := os.ReadFile(dslPath)
	if err != nil {
		t.Skip("not running from a checkout; nothing to compare the table of contents against")
	}

	const regenerate = "run `go generate ./cmd/flow/internal/reference` and commit the result"

	regenerated, err := SyncTOC(original)
	require.NoError(t, err, "docs/DSL.md is missing a %s or %s marker", tocStart, tocEnd)
	assert.Equal(t, string(original), string(regenerated),
		"docs/DSL.md's table of contents is stale: %s", regenerate)
}

// TestSlugifyMatchesGitHub pins the slugger against anchors this repository
// already depends on: DSL.md links to these headings by hand today
// (`grep -n '](#' docs/DSL.md`), so if GitHub resolved them any differently
// those links would already be dead. A slugger that disagreed with GitHub
// would make every entry in the generated table of contents a broken link —
// the opposite of what #702 asks for.
func TestSlugifyMatchesGitHub(t *testing.T) {
	t.Parallel()

	tests := []struct {
		heading string
		want    string
	}{
		{
			"The third round: versions in flight, and what the engine already knows",
			"the-third-round-versions-in-flight-and-what-the-engine-already-knows",
		},
		{
			"`manual:` narrows, and the body can read how a run started",
			"manual-narrows-and-the-body-can-read-how-a-run-started",
		},
		{
			"One edition, one sweep, and what the rewriter may not guess",
			"one-edition-one-sweep-and-what-the-rewriter-may-not-guess",
		},
		{
			"The type system is not a later phase",
			"the-type-system-is-not-a-later-phase",
		},
	}

	for _, tt := range tests {
		t.Run(tt.want, func(t *testing.T) {
			t.Parallel()

			got := slugify(stripMarkup(tt.heading))
			assert.Equal(t, tt.want, got)
		})
	}
}

// TestTOCHeadingsSkipsFencedYAMLComments is the regression #702's own fix
// would otherwise reintroduce: DSL.md's before/after examples open with a
// `# before` YAML comment inside a fenced block, which is not a markdown
// heading and must not appear in the contents list.
func TestTOCHeadingsSkipsFencedYAMLComments(t *testing.T) {
	t.Parallel()

	doc := "# Title\n\n" +
		"## Real heading\n\n" +
		"```yaml\n" +
		"# before\n" +
		"steps: []\n" +
		"```\n\n" +
		"### Also real\n"

	headings := tocHeadings([]byte(doc))

	var texts []string
	for _, h := range headings {
		texts = append(texts, h.text)
	}
	assert.Equal(t, []string{"Real heading", "Also real"}, texts,
		"a YAML comment inside a fence was read as a heading")
}

// TestTOCHeadingsDedupesRepeatedTitles matches GitHub's own anchor
// disambiguation (foo, foo-1, foo-2, ...), which DSL.md needs: "The spelling"
// and "What this round adds" each head more than one round's subsection by
// design, not by mistake — see the git history around #702 for the *other*
// kind of repeated title, which was a mistake and got renamed instead of
// deduplicated.
func TestTOCHeadingsDedupesRepeatedTitles(t *testing.T) {
	t.Parallel()

	doc := "# Title\n\n" +
		"## Round one\n\n" +
		"### The spelling\n\n" +
		"## Round two\n\n" +
		"### The spelling\n"

	headings := tocHeadings([]byte(doc))
	require.Len(t, headings, 4)

	assert.Equal(t, "the-spelling", headings[1].slug)
	assert.Equal(t, "the-spelling-1", headings[3].slug,
		"the second heading with identical text must not collide with the first")
}

// TestSyncTOCRoundTrips is the property [SyncTOC] exists to guarantee:
// running it again over its own output changes nothing, which is what lets
// TestDSLTOCHasNoDrift compare a single pass against the committed file
// instead of needing to fix a point first.
func TestSyncTOCRoundTrips(t *testing.T) {
	t.Parallel()

	doc := "# Title\n\nIntro.\n\n" +
		tocStart + "\n" + tocEnd + "\n\n" +
		"## First\n\n" +
		"### Nested\n\n" +
		"Body text.\n"

	once, err := SyncTOC([]byte(doc))
	require.NoError(t, err)
	assert.Contains(t, string(once), "- [First](#first)")
	assert.Contains(t, string(once), "  - [Nested](#nested)")

	twice, err := SyncTOC(once)
	require.NoError(t, err)
	assert.Equal(t, string(once), string(twice), "regenerating a synced document changed it")
}

// TestSyncTOCRefusesMissingMarkers fails closed: a document that lost a
// marker (or never had one) gets an error naming which, rather than a
// mangled rewrite or a silently unchanged file — the same "fail closed"
// discipline CLAUDE.md asks of every parser over content this package
// controls the shape of.
func TestSyncTOCRefusesMissingMarkers(t *testing.T) {
	t.Parallel()

	_, err := SyncTOC([]byte("# Title\n\n## Heading\n"))
	require.Error(t, err)
	assert.Contains(t, err.Error(), tocStart)

	_, err = SyncTOC([]byte("# Title\n\n" + tocStart + "\n\n## Heading\n"))
	require.Error(t, err)
	assert.Contains(t, err.Error(), tocEnd)
}

// TestSyncTOCExcludesItself guards against the list including its own
// bracketing comments or nesting inside itself if run twice in a row inside
// a larger, more realistic document — belt-and-braces alongside
// TestSyncTOCRoundTrips.
func TestSyncTOCExcludesItself(t *testing.T) {
	t.Parallel()

	doc := "# Title\n\n" + tocStart + "\n" + tocEnd + "\n\n## Only heading\n"

	out, err := SyncTOC([]byte(doc))
	require.NoError(t, err)

	assert.Equal(t, 1, strings.Count(string(out), tocStart))
	assert.Equal(t, 1, strings.Count(string(out), tocEnd))
	assert.Equal(t, 1, strings.Count(string(out), "- [Only heading]"))
}
