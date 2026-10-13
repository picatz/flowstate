//go:build unix

package lsp

import (
	"path/filepath"
	"syscall"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// TestLocatorViewRefusesAFIFO holds that a called file that is not a regular
// file is neither waited on nor read: a FIFO with no writer would otherwise hang
// the language server's diagnostics.
func TestLocatorViewRefusesAFIFO(t *testing.T) {
	dir := t.TempDir()
	fifo := filepath.Join(dir, "callee.yaml")
	require.NoError(t, syscall.Mkfifo(fifo, 0o600))

	l := newLocator(newDocument(fileURI(filepath.Join(dir, "workflow.yaml")), 1, "name: w\n", nil))

	done := make(chan *document, 1)
	go func() { done <- l.view("callee.yaml") }()
	select {
	case view := <-done:
		require.Nil(t, view, "a FIFO is not a Flowfile to read")
	case <-time.After(5 * time.Second):
		t.Fatal("view blocked opening a FIFO")
	}
}
