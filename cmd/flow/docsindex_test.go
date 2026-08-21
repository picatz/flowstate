package main

import (
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// docsDir is the documentation tree, from this package's directory. referenceDir
// (docs_test.go) is the generated part of it.
const docsDir = "../../docs"

// docsIndex is the hand-written map of that tree.
const docsIndexPath = docsDir + "/README.md"

// internalDocsDir holds agent-orchestration process rather than product
// documentation, and is indexed as a directory rather than file by file.
const internalDocsDir = docsDir + "/plans"

// markdownLink matches the target of an inline Markdown link. Reference-style
// links are not used in the index, and a target with a title (`](x "t")`) would
// be caught by the existence check below rather than passing silently.
var markdownLink = regexp.MustCompile(`\]\(([^)\s]+)\)`)

// indexLinkTargets returns the docs-relative paths docs/README.md links to,
// with anchors stripped and off-tree targets (http, mailto) dropped.
func indexLinkTargets(t *testing.T) map[string]bool {
	t.Helper()

	body, err := os.ReadFile(docsIndexPath)
	require.NoError(t, err, "the documentation index is missing; docs/README.md is what tells a reader the rest of docs/ exists")

	targets := map[string]bool{}

	for _, match := range markdownLink.FindAllStringSubmatch(string(body), -1) {
		target := match[1]

		if strings.Contains(target, "://") || strings.HasPrefix(target, "mailto:") {
			continue
		}

		target, _, _ = strings.Cut(target, "#")
		if target == "" {
			// A pure anchor: a link within this page.
			continue
		}

		targets[strings.TrimSuffix(target, "/")] = true
	}

	return targets
}

// The documentation set is only navigable if the index can be trusted, so the
// tests here are about the two ways it stops being true: a document that exists
// and is not listed, and a listing that points at a document that does not.
//
// Neither is hypothetical. Before this index there was no page anywhere that
// named docs/CLI_DESIGN.md or docs/VISION.md, so the only way to learn they
// existed was to list the directory (#708). An index that is allowed to drift
// reproduces exactly that state while looking like it does not.

// TestTheDocsIndexListsEveryDocument fails when a document is added, renamed or
// removed without docs/README.md moving with it.
//
// docs/plans/ is deliberately indexed as a directory rather than file by file:
// it is internal process, its contents turn over per wave, and a reader's whole
// business with it is knowing it is not for them.
func TestTheDocsIndexListsEveryDocument(t *testing.T) {
	targets := indexLinkTargets(t)

	documents, err := filepath.Glob(filepath.Join(docsDir, "*.md"))
	require.NoError(t, err)

	generated, err := filepath.Glob(filepath.Join(referenceDir, "*.md"))
	require.NoError(t, err)

	for _, document := range append(documents, generated...) {
		relative, err := filepath.Rel(docsDir, document)
		require.NoError(t, err)

		relative = filepath.ToSlash(relative)
		if relative == "README.md" {
			// The index does not index itself.
			continue
		}

		assert.True(t, targets[relative],
			"docs/%s is not listed in docs/README.md; add it there (with one line saying what it covers) so a reader can discover it without listing the directory",
			relative)
	}

	assert.True(t, targets["plans"],
		"docs/README.md no longer points at docs/plans/; it is internal process sitting at the same depth as the product documentation, and the index is what says so")
}

// TestTheDocsIndexPointsAtDocumentsThatExist is the same property from the other
// side: a rename that updates the tree and not the index leaves a link that
// 404s, which is worse than an omission because it looks answered.
func TestTheDocsIndexPointsAtDocumentsThatExist(t *testing.T) {
	for target := range indexLinkTargets(t) {
		t.Run(target, func(t *testing.T) {
			_, err := os.Stat(filepath.Join(docsDir, filepath.FromSlash(target)))
			assert.NoError(t, err,
				"docs/README.md links to %s, which is not in the tree; fix the link or restore the document", target)
		})
	}
}

// TestInternalDocumentsSayTheyAreInternal keeps docs/plans/ from reading as
// product documentation.
//
// It sits at the same path depth as ARCHITECTURE.md and DEPLOYMENT.md and is
// nothing like them — docs/plans/orchestration.md opens by saying agents do not
// read it — so every file there carries a banner naming itself internal. A new
// plan added without one is the whole failure this guards.
func TestInternalDocumentsSayTheyAreInternal(t *testing.T) {
	plans, err := filepath.Glob(filepath.Join(internalDocsDir, "*.md"))
	require.NoError(t, err)
	require.NotEmpty(t, plans, "docs/plans/ moved and this test did not")

	for _, plan := range plans {
		t.Run(filepath.Base(plan), func(t *testing.T) {
			body, err := os.ReadFile(plan)
			require.NoError(t, err)

			head := string(body)
			if len(head) > 1024 {
				head = head[:1024]
			}

			assert.Contains(t, head, "> [!NOTE]",
				"%s carries no internal-only banner in its first 1KB; copy the one the other files under docs/plans/ open with", plan)
			assert.Contains(t, head, "Internal",
				"%s's banner does not say it is internal", plan)
		})
	}
}
