package main

import (
	"path/filepath"
	"syscall"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestReadBoundedFileRefusesAFIFOWithoutWaiting(t *testing.T) {
	path := filepath.Join(t.TempDir(), "fixture")
	require.NoError(t, syscall.Mkfifo(path, 0o600))

	done := make(chan error, 1)
	go func() {
		_, err := readBoundedFile(path, "a fixture", 64)
		done <- err
	}()

	select {
	case err := <-done:
		require.ErrorContains(t, err, "not a regular file")
	case <-time.After(time.Second):
		t.Fatal("opening a FIFO blocked waiting for a writer")
	}
}
