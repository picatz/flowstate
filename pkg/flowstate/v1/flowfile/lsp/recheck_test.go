package lsp

import (
	"fmt"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/sourcegraph/go-lsp"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	v1 "github.com/picatz/flowstate/pkg/flowstate/v1"
	"github.com/picatz/flowstate/pkg/flowstate/v1/flowfile"
)

// recheckServer is a server holding bill.yaml and other.yaml open, and lib/ids.yaml
// opened and so remembered, over a module tree on disk.
func recheckServer(t *testing.T) (s *FlowfileServer, dir string, module *document) {
	t.Helper()

	dir = moduleTree(t)
	s = &FlowfileServer{Logger: discardLogger()}
	for name, text := range map[string]string{"bill.yaml": useSource, "other.yaml": otherImporterSource} {
		s.docs.open(fileURI(filepath.Join(dir, name)), 1, text, nil)
	}
	module = s.docs.open(fileURI(filepath.Join(dir, "lib/ids.yaml")), 1, usedModuleSource, nil)
	s.rememberInterface(module)

	return s, dir, module
}

// save writes text over the module and returns the document the save is about.
func save(t *testing.T, s *FlowfileServer, dir, text string) *document {
	t.Helper()
	require.NoError(t, os.WriteFile(filepath.Join(dir, "lib/ids.yaml"), []byte(text), 0o644))

	return s.docs.open(fileURI(filepath.Join(dir, "lib/ids.yaml")), 2, text, nil)
}

func uris(docs []*document) []string {
	out := make([]string, 0, len(docs))
	for _, d := range docs {
		out = append(out, filepath.Base(string(d.uri)))
	}
	slices.Sort(out)

	return out
}

// A save that changes how a module is written and not what it declares reaches no
// one else; a save that changes what it declares reaches every open file that uses
// it, and only those.
func TestSavingAModuleRechecksOnlyTheFilesItsInterfaceChanged(t *testing.T) {
	t.Parallel()

	t.Run("comments and layout", func(t *testing.T) {
		t.Parallel()
		s, dir, _ := recheckServer(t)
		text := "# owned by identity\n" + strings.Replace(usedModuleSource, "    type: string\n", "    type:   string  # text\n", 1)
		assert.Empty(t, s.dependentsToRecheck(save(t, s, dir, text)))
	})
	t.Run("an interface edit", func(t *testing.T) {
		t.Parallel()
		s, dir, _ := recheckServer(t)
		text := strings.Replace(usedModuleSource, "must: isUuid(this)", `must: size(this) > 3`, 1)
		assert.Equal(t, []string{"bill.yaml", "other.yaml"}, uris(s.dependentsToRecheck(save(t, s, dir, text))))
	})
	t.Run("a module that stops compiling", func(t *testing.T) {
		t.Parallel()
		s, dir, _ := recheckServer(t)
		text := strings.Replace(usedModuleSource, "returns: bool", "returns: bogus", 1)
		assert.Equal(t, []string{"bill.yaml", "other.yaml"}, uris(s.dependentsToRecheck(save(t, s, dir, text))),
			"a module with no interface counts as changed")
	})
	t.Run("a module nobody remembered", func(t *testing.T) {
		t.Parallel()
		s, dir, _ := recheckServer(t)
		s.interfaces = interfaceMemo{}
		text := "# note\n" + usedModuleSource
		assert.Equal(t, []string{"bill.yaml", "other.yaml"}, uris(s.dependentsToRecheck(save(t, s, dir, text))),
			"no memory of the last interface is doubt, and doubt rechecks")
		assert.Empty(t, s.dependentsToRecheck(save(t, s, dir, "# another\n"+usedModuleSource)),
			"and once remembered a comment is a comment again")
	})
	t.Run("a file that does not use it", func(t *testing.T) {
		t.Parallel()
		s, dir, _ := recheckServer(t)
		s.docs.open(fileURI(filepath.Join(dir, "unrelated.yaml")), 1, workflowWithoutUse, nil)
		text := strings.Replace(usedModuleSource, "must: isUuid(this)", `must: size(this) > 3`, 1)
		assert.NotContains(t, uris(s.dependentsToRecheck(save(t, s, dir, text))), "unrelated.yaml")
	})
	t.Run("a workflow is not a module", func(t *testing.T) {
		t.Parallel()
		s, dir, _ := recheckServer(t)
		bill := s.docs.open(fileURI(filepath.Join(dir, "bill.yaml")), 2, useSource+"\n# edit\n", nil)
		assert.Empty(t, s.dependentsToRecheck(bill))
	})
}

const workflowWithoutUse = `edition: ` + flowfile.CurrentEdition + `
name: alone
steps:
  - id: noop
    log:
      message: hi
`

// Through a module the editor does not have open, an interface change still arrives,
// and a comment still does not.
func TestSavingAModuleReachesFilesThroughModulesThatAreNotOpen(t *testing.T) {
	t.Parallel()

	core := `edition: ` + flowfile.CurrentEdition + "\nname: core\ntypes:\n  Id:\n    type: string\n    must: this != \"\"\n"
	chain := `edition: ` + flowfile.CurrentEdition + "\nname: chain\nuse:\n  core:\n    path: ./core.yaml\ntypes:\n  Order:\n    fields:\n      id:\n        type: core.Id\n        required: true\n"
	root := `edition: ` + flowfile.CurrentEdition + "\nname: root\nuse:\n  chain:\n    path: ./lib/chain.yaml\ninputs:\n  o:\n    type: chain.Order\nsteps:\n  - id: noop\n    log:\n      message: hi\n"
	dir := t.TempDir()
	require.NoError(t, os.MkdirAll(filepath.Join(dir, "lib"), 0o755))
	for name, text := range map[string]string{"root.yaml": root, "lib/chain.yaml": chain, "lib/core.yaml": core} {
		require.NoError(t, os.WriteFile(filepath.Join(dir, name), []byte(text), 0o644))
	}
	s := &FlowfileServer{Logger: discardLogger()}
	s.docs.open(fileURI(filepath.Join(dir, "root.yaml")), 1, root, nil)
	leaf := s.docs.open(fileURI(filepath.Join(dir, "lib/core.yaml")), 1, core, nil)
	s.rememberInterface(leaf)
	// The middle module is checked once, as the first save would, so that it too has an interface to differ from.
	require.NotEmpty(t, moduleCache.Interface(filepath.Join(canonicalPath(dir), "lib/chain.yaml")))

	saveLeaf := func(text string) *document {
		require.NoError(t, os.WriteFile(filepath.Join(dir, "lib/core.yaml"), []byte(text), 0o644))
		return s.docs.open(fileURI(filepath.Join(dir, "lib/core.yaml")), 2, text, nil)
	}
	s.dependentsToRecheck(saveLeaf(core)) // the first walk learns the middle module's interface

	assert.Empty(t, s.dependentsToRecheck(saveLeaf("# note\n"+core)))
	assert.Equal(t, []string{"root.yaml"}, uris(s.dependentsToRecheck(saveLeaf(strings.Replace(core, `this != ""`, `size(this) > 3`, 1)))))
}

func TestRecheckIsBounded(t *testing.T) {
	t.Parallel()

	t.Run("republished files", func(t *testing.T) {
		t.Parallel()
		s, dir, _ := recheckServer(t)
		for i := range 2 * v1.MaxModules {
			s.docs.open(fileURI(filepath.Join(dir, fmt.Sprintf("extra%03d.yaml", i))), 1, useSource, nil)
		}
		text := strings.Replace(usedModuleSource, "must: isUuid(this)", `must: size(this) > 3`, 1)
		assert.Len(t, s.dependentsToRecheck(save(t, s, dir, text)), v1.MaxModules)
	})
	t.Run("remembered interfaces", func(t *testing.T) {
		t.Parallel()
		var memo interfaceMemo
		for i := range 3 * maxInterfaceMemo {
			memo.swap(fmt.Sprintf("/m%d.yaml", i), "sha256:x")
		}
		assert.LessOrEqual(t, len(memo.digests), maxInterfaceMemo)
		_, known := memo.swap("/m0.yaml", "sha256:y")
		assert.False(t, known, "what was forgotten is unknown, which is doubt")
	})
	t.Run("modules read in one walk", func(t *testing.T) {
		t.Parallel()
		// A chain longer than the walk may read: the answer is changed, not unknown.
		dir := t.TempDir()
		n := v1.MaxModules + 5
		for i := range n {
			text := `edition: ` + flowfile.CurrentEdition + fmt.Sprintf("\nname: m%d\ntypes:\n  T:\n    type: string\n", i)
			if i+1 < n {
				text += fmt.Sprintf("use:\n  next:\n    path: ./m%d.yaml\n", i+1)
			}
			require.NoError(t, os.WriteFile(filepath.Join(dir, fmt.Sprintf("m%d.yaml", i)), []byte(text), 0o644))
		}
		s := &FlowfileServer{Logger: discardLogger()}
		w := &interfaceWalk{memo: &s.interfaces, saved: filepath.Join(canonicalPath(dir), fmt.Sprintf("m%d.yaml", n-1)), loads: v1.MaxModules, verdicts: map[string]bool{}}
		assert.True(t, w.changed(filepath.Join(canonicalPath(dir), "m0.yaml"), 1))
	})
}

// Over the protocol: a save of the module republishes the importer with the one
// line the module's errors become there, and a save that repairs it clears it.
func TestSavingABrokenModulePublishesOneLineToTheImporter(t *testing.T) {
	t.Parallel()

	dir := moduleTree(t)
	module := filepath.Join(dir, "lib/ids.yaml")
	billURI := fileURI(filepath.Join(dir, "bill.yaml"))
	c := newClient(t)
	c.initialize()
	c.open(string(billURI), useSource)
	c.open(string(fileURI(module)), usedModuleSource)

	saved := func(text string) {
		require.NoError(t, os.WriteFile(module, []byte(text), 0o644))
		require.NoError(t, c.conn.Notify(t.Context(), "textDocument/didChange", lsp.DidChangeTextDocumentParams{
			TextDocument:   lsp.VersionedTextDocumentIdentifier{TextDocumentIdentifier: lsp.TextDocumentIdentifier{URI: fileURI(module)}, Version: 2},
			ContentChanges: []lsp.TextDocumentContentChangeEvent{{Text: text}},
		}))
		require.NoError(t, c.conn.Notify(t.Context(), "textDocument/didSave", lsp.DidSaveTextDocumentParams{
			TextDocument: lsp.TextDocumentIdentifier{URI: fileURI(module)},
		}))
	}
	importerSays := func(substring string) bool {
		got, ok := c.lastPublishedFor(billURI)
		if !ok {
			return false
		}
		joined := strings.Join(messages(got.Diagnostics), "\n")

		return strings.Contains(joined, substring) && strings.Count(joined, "which has") <= 1
	}

	saved(strings.Replace(usedModuleSource, "returns: bool", "returns: bogus", 1))
	require.Eventually(t, func() bool { return importerSays(`uses "./lib/ids.yaml", which has 1 error`) }, 10*time.Second, 10*time.Millisecond)

	saved(usedModuleSource)
	require.Eventually(t, func() bool {
		got, ok := c.lastPublishedFor(billURI)
		return ok && len(got.Diagnostics) == 0
	}, 10*time.Second, 10*time.Millisecond)
}
