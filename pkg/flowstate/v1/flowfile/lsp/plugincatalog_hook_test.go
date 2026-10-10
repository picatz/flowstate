package lsp

import (
	"errors"
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// The PluginCatalog hook is how `flow lsp` reads the plugins.lock.json next to
// a document. This package owns only the seam: the hook is asked about a
// file-backed document before it is built, is not asked about a document with
// no file, and a failure is shown to the person rather than swallowed.
func TestThePluginCatalogHookIsAskedAboutFileDocumentsAndItsFailureIsShown(t *testing.T) {
	t.Parallel()

	var (
		mu    sync.Mutex
		asked []string
	)
	server := &FlowfileServer{Logger: discardLogger(), PluginCatalog: func(path string) error {
		mu.Lock()
		defer mu.Unlock()
		asked = append(asked, path)
		if len(asked) == 2 {
			return errors.New("lock is not a plugin catalog")
		}

		return nil
	}}
	c := newClientFor(t, server)
	c.initialize()

	c.open("file:///work/repo/a.yaml", "name: a\nsteps: []\n")
	c.open("untitled:Untitled-1", "name: b\nsteps: []\n")
	c.open("file:///work/repo/c.yaml", "name: c\nsteps: []\n")

	mu.Lock()
	assert.Equal(t, []string{"/work/repo/a.yaml", "/work/repo/c.yaml"}, asked,
		"the hook sees file documents only, once per open")
	mu.Unlock()

	// The warning is sent before the document's diagnostics on one ordered
	// stream, and open waits for those, so it has been received by now.
	c.mu.Lock()
	defer c.mu.Unlock()
	require.Equal(t, 1, c.notified["window/showMessage"], "a lock that could not be used was not shown to the person")
}

func TestWithoutAPluginCatalogHookNothingIsAsked(t *testing.T) {
	t.Parallel()

	c := newClient(t)
	c.initialize()
	c.open("file:///work/repo/a.yaml", "name: a\nsteps: []\n")

	c.mu.Lock()
	defer c.mu.Unlock()
	assert.Zero(t, c.notified["window/showMessage"])
}
