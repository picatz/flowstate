package main

import (
	"io/fs"
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
// It walks docs/ rather than globbing its top level, because a glob only knows
// about the directory layout that exists on the day it is written: the first
// docs/guides/ or docs/adr/ anyone adds would be invisible to it, which is the
// original finding — a document nobody can discover — reproduced underneath a
// test that says it cannot happen (a Codex review of #820 caught exactly this).
//
// Two exclusions, both deliberate rather than incidental:
//
//   - docs/README.md is the index; it does not index itself.
//   - docs/plans/ is indexed as a *directory*. It is internal process, its
//     contents turn over every wave, and a reader's whole business with it is
//     knowing it is not for them — so what the index owes them is one line
//     saying that, not a listing that churns. The banner on each file is what
//     TestInternalDocumentsSayTheyAreInternal holds; the link to the directory
//     is asserted below.
//
// Any other directory added under docs/ is product documentation until someone
// decides otherwise, and is listed file by file, path and all.
func TestTheDocsIndexListsEveryDocument(t *testing.T) {
	targets := indexLinkTargets(t)

	internal := relativeTo(t, docsDir, internalDocsDir)

	for _, document := range documentPaths(t, docsDir, internal) {
		assert.True(t, targets[document],
			"docs/%s is not listed in docs/README.md; add it there (with one line saying what it covers) so a reader can discover it without listing the directory",
			document)
	}

	assert.True(t, targets[internal],
		"docs/README.md no longer points at docs/plans/; it is internal process sitting at the same depth as the product documentation, and the index is what says so")
}

// relativeTo is filepath.Rel with the slash normalization every path in this
// file wants, since an index target is written with forward slashes whatever
// the host does.
func relativeTo(t *testing.T, base, target string) string {
	t.Helper()

	relative, err := filepath.Rel(base, target)
	require.NoError(t, err)

	return filepath.ToSlash(relative)
}

// documentPaths walks root and returns every Markdown file under it as a
// slash-separated path relative to root, skipping root's own README.md (the
// index does not index itself) and everything under skipDir.
//
// Split out from the test above so the walk itself is testable against a tree
// this file controls — see TestTheWalkReachesADocumentInASubdirectory. A
// recursive walk that quietly stopped recursing would otherwise look exactly
// like a complete index.
func documentPaths(t *testing.T, root, skipDir string) []string {
	t.Helper()

	var documents []string

	err := filepath.WalkDir(root, func(path string, entry fs.DirEntry, err error) error {
		if err != nil {
			return err
		}

		relative := relativeTo(t, root, path)

		if entry.IsDir() {
			if relative == skipDir {
				return fs.SkipDir
			}

			return nil
		}

		if filepath.Ext(path) != ".md" || relative == "README.md" {
			return nil
		}

		documents = append(documents, relative)

		return nil
	})
	require.NoError(t, err)

	return documents
}

// TestTheWalkReachesADocumentInASubdirectory is the property the walk exists
// for, asserted somewhere it can fail: over a tree with a nested document, a
// nested index-named file, and a skipped directory. The tree under docs/ is
// flat today apart from reference/ and plans/, so nothing in the repository
// would notice a walk that stopped at the top level — which is how the glob
// this replaced looked correct for as long as nobody added a directory.
func TestTheWalkReachesADocumentInASubdirectory(t *testing.T) {
	root := t.TempDir()

	for _, path := range []string{
		"README.md",
		"TOP.md",
		"guides/nested.md",
		"guides/deeper/still.md",
		"guides/README.md",
		"guides/notes.txt",
		"plans/internal.md",
	} {
		full := filepath.Join(root, filepath.FromSlash(path))
		require.NoError(t, os.MkdirAll(filepath.Dir(full), 0o755))
		require.NoError(t, os.WriteFile(full, []byte("# heading\n"), 0o644))
	}

	assert.Equal(t, []string{
		"TOP.md",
		"guides/README.md",
		"guides/deeper/still.md",
		"guides/nested.md",
	}, documentPaths(t, root, "plans"))
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
	// Walked, not globbed, for the reason above: a plan filed under
	// docs/plans/2026-09/ is still internal, and a check that cannot see it
	// is the same gap in a smaller directory. Nothing is skipped except the
	// README.md documentPaths always exempts, which here would be the
	// directory's own index rather than a plan.
	plans := documentPaths(t, internalDocsDir, "")
	require.NotEmpty(t, plans, "docs/plans/ moved and this test did not")

	for _, plan := range plans {
		t.Run(plan, func(t *testing.T) {
			body, err := os.ReadFile(filepath.Join(internalDocsDir, filepath.FromSlash(plan)))
			require.NoError(t, err)

			head := string(body)
			if len(head) > 1024 {
				head = head[:1024]
			}

			assert.Contains(t, head, "> [!NOTE]",
				"docs/plans/%s carries no internal-only banner in its first 1KB; copy the one the other files under docs/plans/ open with", plan)
			assert.Contains(t, head, "Internal",
				"docs/plans/%s's banner does not say it is internal", plan)
		})
	}
}
