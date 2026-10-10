//go:build unix

package main

import (
	"path/filepath"
	"syscall"
	"testing"

	"github.com/stretchr/testify/require"
)

// A named pipe in a walked directory must be refused at once: opening a FIFO
// for reading waits for a writer forever, which hung `flow validate dir`. The
// walk runs in a goroutine so a regression fails against the test binary's own
// -timeout instead of stalling silently.
func TestValidateWalkRefusesANamedPipeWithoutBlocking(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	pipe := filepath.Join(dir, "pipe.yaml")
	require.NoError(t, syscall.Mkfifo(pipe, 0o600))

	done := make(chan error, 1)
	go func() {
		_, err := collectValidateTargets([]string{dir}, nil)
		done <- err
	}()

	select {
	case err := <-done:
		require.ErrorContains(t, err, pipe+" is not a regular file (named pipe); name a regular file instead")
	case <-t.Context().Done():
		t.Fatal("walking a directory with a FIFO blocked")
	}
}
