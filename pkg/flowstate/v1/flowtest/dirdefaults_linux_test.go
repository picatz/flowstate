package flowtest_test

import (
	"path/filepath"
	"syscall"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/picatz/flowstate/pkg/flowstate/v1/flowtest"
)

// TestSiblingScanRefusesAFIFOWithoutWaiting is Codex's finding on #2080's own
// review: the directory-wide taint scan discovers sibling suite files an
// author never named, and opening one blocks on a FIFO with nobody writing
// to it — or a symlink to one — before [readBounded]'s own "not a regular
// file" refusal is ever reached, since that refusal reads the *open*
// descriptor. The suite this load was asked to open has always accepted that
// risk on the one path an author named; a sibling this scan discovers on its
// own has not, so it is stat'd and skipped before Open is ever called on it.
func TestSiblingScanRefusesAFIFOWithoutWaiting(t *testing.T) {
	dir := t.TempDir()
	writeFile(t, filepath.Join(dir, "workflow.yaml"), `
edition: v2026.3
name: fine
steps:
  - id: s
    log:
      message: hello
outputs: {}
`)
	writeFile(t, filepath.Join(dir, "testdefaults.yaml"), "vars:\n  token: unrelated\n")
	require.NoError(t, syscall.Mkfifo(filepath.Join(dir, "sibling.test.yaml"), 0o600))

	targetPath := filepath.Join(dir, "target.test.yaml")
	writeFile(t, targetPath, "tests:\n  - name: it runs\n    workflow: ./workflow.yaml\n    expect: {failed: false}\n")

	done := make(chan error, 1)
	go func() {
		_, err := flowtest.Load(targetPath)
		done <- err
	}()

	select {
	case err := <-done:
		require.NoError(t, err)
	case <-time.After(5 * time.Second):
		t.Fatal("loading the target file blocked opening a sibling FIFO with nobody writing to it")
	}
}
