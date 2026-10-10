//go:build unix

package lsp

import (
	"context"
	"syscall"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// A FIFO callee must be refused at once: opening one for reading blocks until a
// writer appears, which would hang every completion, hover, and definition
// request that resolves the callee.
func TestReadCalleeSourceRefusesFIFOWithoutBlocking(t *testing.T) {
	t.Parallel()

	path := t.TempDir() + "/pipe.yaml"
	require.NoError(t, syscall.Mkfifo(path, 0o600))

	ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
	defer cancel()
	done := make(chan bool, 1)
	go func() {
		_, ok := readCalleeSource(path)
		done <- ok
	}()
	select {
	case ok := <-done:
		require.False(t, ok)
	case <-ctx.Done():
		t.Fatal("reading a FIFO callee blocked")
	}
}
