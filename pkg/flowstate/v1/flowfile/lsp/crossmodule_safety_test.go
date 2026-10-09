package lsp

import (
	"maps"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"

	"github.com/sourcegraph/go-lsp"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/picatz/flowstate/pkg/flowstate/v1/flowfile"
)

// TestRenameRefusesAnImporterItCannotRead: a file in the workspace that mentions the
// name but does not parse, or that is too large to read, is a reason to refuse and
// not an importer silently left behind. A file that cannot be an importer is not.
func TestRenameRefusesAnImporterItCannotRead(t *testing.T) {
	t.Parallel()

	dir := moduleTree(t)
	ws := workspace{roots: []string{dir}}
	doc := docAt(dir, "bill.yaml", useSource)
	at := positionOf(t, useSource, "ids.Uuid", 5)
	bad := filepath.Join(dir, "bad.yaml")

	require.NoError(t, os.WriteFile(bad, []byte("name: [unclosed\ntype: shared.Uuid\n"), 0o644))
	_, handled, err := renameQualified(doc, ws, at, "Id")
	assert.True(t, handled)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "does not parse")

	require.NoError(t, os.WriteFile(bad, []byte("name: [unclosed\n"), 0o644))
	edit, _, err := renameQualified(doc, ws, at, "Id")
	require.NoError(t, err, "an unparsable file that never mentions the name cannot be an importer")
	assert.Len(t, edit.Changes, 3)

	require.NoError(t, os.WriteFile(bad, []byte("name: big\n# "+strings.Repeat("x", maxDocumentBytes)+"\n"), 0o644))
	_, _, err = renameQualified(doc, ws, at, "Id")
	require.Error(t, err)
	assert.Contains(t, err.Error(), "cannot be read within")
}

// TestRenameReachesAnImporterInAHiddenDirectory: the rename walk enters hidden
// directories (apart from .git), so an importer there is edited rather than missed.
func TestRenameReachesAnImporterInAHiddenDirectory(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	hidden := filepath.Join(dir, ".team")
	require.NoError(t, os.MkdirAll(filepath.Join(hidden, "lib"), 0o755))
	require.NoError(t, os.WriteFile(filepath.Join(hidden, "lib", "ids.yaml"), []byte(usedModuleSource), 0o644))
	require.NoError(t, os.WriteFile(filepath.Join(hidden, "a.yaml"), []byte(useSource), 0o644))
	require.NoError(t, os.WriteFile(filepath.Join(hidden, "b.yaml"), []byte(otherImporterSource), 0o644))

	doc := docAt(hidden, "a.yaml", useSource)
	edit, handled, err := renameQualified(doc, workspace{roots: []string{dir}}, positionOf(t, useSource, "ids.Uuid", 5), "Id")
	require.NoError(t, err)
	require.True(t, handled)
	assert.Len(t, edit.Changes, 3, "the module and both importers, one of them found only by the walk")
	assert.Contains(t, edit.Changes, string(fileURI(filepath.Join(hidden, "b.yaml"))))
}

// TestRenameThroughASymlinkedWorkspaceEditsEachFileOnce: a workspace folder reached
// through a symlink yields each importer under one URI, and an open buffer is found
// by its real path, so the edit is built from what the editor holds.
func TestRenameThroughASymlinkedWorkspaceEditsEachFileOnce(t *testing.T) {
	t.Parallel()

	dir := moduleTree(t)
	link := filepath.Join(t.TempDir(), "link")
	require.NoError(t, os.Symlink(dir, link))

	store := &documentStore{}
	store.open(fileURI(filepath.Join(link, "other.yaml")), 1, otherImporterSource+"# unsaved\n", nil)
	ws := workspace{roots: []string{link}, open: store.getByFilesystemPath}

	doc := docAt(link, "bill.yaml", useSource)
	edit, handled, err := renameQualified(doc, ws, positionOf(t, useSource, "ids.Uuid", 5), "Id")
	require.NoError(t, err)
	require.True(t, handled)
	require.Len(t, edit.Changes, 3, "keys: %v", slices.Sorted(maps.Keys(edit.Changes)))
	for uri, edits := range edit.Changes {
		assert.Len(t, edits, 1, uri)
	}
	assert.Contains(t, edit.Changes, string(fileURI(filepath.Join(link, "other.yaml"))), "the open buffer's own URI")
}

// TestRenameNeverFallsBackToDiskForAnOpenBuffer: an importer the editor holds that
// does not parse is judged by the buffer, not by the file on disk, which is not what
// the editor will apply an edit to.
func TestRenameNeverFallsBackToDiskForAnOpenBuffer(t *testing.T) {
	t.Parallel()

	dir := moduleTree(t)
	other := filepath.Join(dir, "other.yaml")
	doc := docAt(dir, "bill.yaml", useSource)
	at := positionOf(t, useSource, "ids.Uuid", 5)
	rename := func(buffer string) (*lsp.WorkspaceEdit, error) {
		store := &documentStore{}
		store.open(fileURI(other), 1, buffer, nil)
		edit, _, err := renameQualified(doc, workspace{roots: []string{dir}, open: store.getByFilesystemPath}, at, "Id")
		return edit, err
	}

	// The disk copy parses and mentions the name; the buffer mentions it and does not parse.
	_, err := rename("name: [unclosed\ntype: shared.Uuid\n")
	require.Error(t, err)
	assert.Contains(t, err.Error(), "open buffer")

	// The disk copy lacks the name; only the unsaved buffer has it.
	require.NoError(t, os.WriteFile(other, []byte("edition: "+flowfile.CurrentEdition+"\nname: other\nsteps:\n  - id: a\n    log:\n      message: hi\n"), 0o644))
	_, err = rename("name: [unclosed\ntype: shared.Uuid\n")
	require.Error(t, err)

	// An unparsable buffer that never mentions the name is not an importer.
	edit, err := rename("name: [unclosed\n")
	require.NoError(t, err)
	assert.Len(t, edit.Changes, 2, "the module and the file the cursor is in")
}

// TestAddUseKeepsTheDocumentsLineEndings: a CRLF document gets a CRLF insertion.
func TestAddUseKeepsTheDocumentsLineEndings(t *testing.T) {
	t.Parallel()

	dir := moduleTree(t)
	src := strings.ReplaceAll("edition: "+flowfile.CurrentEdition+"\nname: bill\ninputs:\n  c:\n    type: ids.Uuid\nsteps:\n  - id: a\n    log:\n      message: hi\n", "\n", "\r\n")
	doc := docAt(dir, "bill.yaml", src)
	actions := codeActions(doc, codeActionParams{Range: lsp.Range{Start: positionOf(t, src, "ids.Uuid", 1), End: positionOf(t, src, "ids.Uuid", 3)}})
	var add *codeAction
	for i := range actions {
		if strings.HasPrefix(actions[i].Title, "Add `use:") {
			add = &actions[i]
		}
	}
	require.NotNil(t, add)
	assert.Equal(t, "use:\r\n  ids:\r\n    path: ./lib/ids.yaml\r\n", add.Edit.Changes[string(doc.uri)][0].NewText)
	fixed := applyAll(t, src, add.Edit.Changes[string(doc.uri)])
	assert.Equal(t, strings.Count(fixed, "\n"), strings.Count(fixed, "\r\n"), "no bare line feed")
}

// TestRenameEditsLandOnTheNameAfterWideCharacters: positions are UTF-16, so a name
// written after non-ASCII text on its line is replaced exactly.
func TestRenameEditsLandOnTheNameAfterWideCharacters(t *testing.T) {
	t.Parallel()

	dir := moduleTree(t)
	src := "edition: " + flowfile.CurrentEdition + "\nname: bill\nuse:\n  ids:\n    path: ./lib/ids.yaml\ninputs: {ref: {description: \"é€😀\", type: ids.Uuid}}\nsteps:\n  - id: a\n    log:\n      message: hi\n"
	doc := docAt(dir, "bill.yaml", src)
	edit, handled, err := renameQualified(doc, workspace{roots: []string{dir}}, positionOf(t, src, "ids.Uuid", 1), "Id")
	require.NoError(t, err)
	require.True(t, handled)
	assert.Contains(t, applyAll(t, src, edit.Changes[string(doc.uri)]), `description: "é€😀", type: ids.Id}}`)
}
