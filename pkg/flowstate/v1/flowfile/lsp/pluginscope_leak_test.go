package lsp

import (
	"os"
	"path/filepath"
	"testing"

	lsp "github.com/sourcegraph/go-lsp"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	v1 "github.com/picatz/flowstate/pkg/flowstate/v1"
)

// A locked document's features compile through the shared module cache (its key
// includes the registry, so it may), leave the default registry as they found it,
// and an unlocked document on the same server neither sees the lock's task nor is
// served what the locked document's compile left in the cache. Not parallel: it
// reads the process-wide module cache's counters, which no other test may move
// meanwhile.
func TestFeaturesOnALockedDocumentLeakNothing(t *testing.T) {
	dir := moduleTree(t)
	// Bytes unique to this test, so the cache holds nothing for them yet.
	require.NoError(t, os.WriteFile(filepath.Join(dir, "lib", "ids.yaml"), []byte(usedModuleSource+"# "+dir+"\n"), 0o644))

	lock := &PluginLock{Path: filepath.Join(dir, "plugins.lock.json"), Version: "1", Tasks: []v1.TaskDef{lockTask(t, "alpha.run")}}
	server := &FlowfileServer{Logger: discardLogger(), PluginCatalog: func(path string) (*PluginLock, error) {
		if filepath.Base(path) == "bill.yaml" {
			return lock, nil
		}

		return nil, nil
	}}
	c := newClientFor(t, server)
	c.initialize()

	step := "  - id: g\n    alpha.run:\n      message: hi\n"
	locked := string(fileURI(filepath.Join(dir, "bill.yaml")))
	before := moduleCache.Stats()
	got := c.open(locked, useSource+step)
	assert.False(t, unknownTask(got, "alpha.run"), "%v", got.Diagnostics)
	c.hover(locked, 9, 6)
	c.complete(locked, 9, 6)
	c.format(locked)
	c.symbols(locked)
	c.definition(locked, 3, 10)
	c.codeAction(locked, lsp.Range{End: lsp.Position{Line: 100}}, nil, got.Diagnostics)
	after := moduleCache.Stats()
	assert.Greater(t, after.Compiles, before.Compiles, "a locked document did not compile through the shared cache")

	_, leaked := v1.DefaultRegistry().Lookup("alpha.run")
	assert.False(t, leaked, "a feature left the lock's task in the default registry")

	// The control: a document with no lock, on the same server, does not see the
	// task, and the module it shares with the locked document is compiled again
	// for its own registry rather than served from the locked document's entry.
	other := string(fileURI(filepath.Join(dir, "other.yaml")))
	plain := c.open(other, otherImporterSource+step)
	assert.True(t, unknownTask(plain, "alpha.run"), "an unlocked document saw a lock's task: %v", plain.Diagnostics)
	assert.Greater(t, moduleCache.Stats().Compiles, after.Compiles, "an unlocked document was served a module compiled against a lock's registry")
}

// A request that holds a document across the close of its last user still
// compiles it with the lock it was opened with, and the scope's retained copy is
// gone once nothing is open.
func TestARequestHoldingAClosedDocumentKeepsItsLockAndTheScopeEmpties(t *testing.T) {
	t.Parallel()

	l := newLocks(t)
	l.set("a", "1", "alpha.run")
	server := &FlowfileServer{Logger: discardLogger(), PluginCatalog: l.hook}
	c := newClientFor(t, server)
	c.initialize()

	uri := uriIn("a")
	require.False(t, unknownTask(c.open(uri, usesTask("alpha.run")), "alpha.run"))
	held, ok := c.rawServerDoc(uri)
	require.True(t, ok)
	registry := held.tasks
	_, ok = registry.Lookup("alpha.run")
	require.True(t, ok)
	require.Same(t, registry, held.env.Registry(), "the document's Env is not its own registry")

	closeAndWait(c, uri)

	// The scope's retained copy is gone, and the document still owns its registry.
	server.scopes.mu.Lock()
	assert.Empty(t, server.scopes.entries)
	assert.Empty(t, server.scopes.byDoc)
	server.scopes.mu.Unlock()

	_, ok = registry.Lookup("alpha.run")
	assert.True(t, ok, "a document held across its close lost its registry")
	for _, d := range diagnoseCarried(held) {
		assert.NotContains(t, d.published.Message, `"alpha.run"`, "a document held across its close lost its lock's tasks")
	}
}
