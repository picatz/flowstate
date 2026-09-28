package envelope

import (
	"path/filepath"
	"syscall"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// TestAKeyringFIFOIsRefusedWithoutWaiting: opening a FIFO for an ordinary
// read waits for a writer forever, and neither a context nor the startup
// budget interrupts an open. A keyring, key, escrow key or CA path that names
// one by mistake must be refused at once, not hang every command using it.
func TestAKeyringFIFOIsRefusedWithoutWaiting(t *testing.T) {
	t.Parallel()

	path := filepath.Join(t.TempDir(), "keyring.yaml")
	require.NoError(t, syscall.Mkfifo(path, 0o600))

	for name, read := range map[string]func() error{
		"keyring": func() error { _, err := LoadFile(t.Context(), path, OpenOptions{}); return err },
		// Every key, escrow key and CA file is read through readBounded too.
		"file": func() error { _, err := readBounded(path, MaxKeyFileBytes, nil); return err },
	} {
		done := make(chan error, 1)
		go func() { done <- read() }()
		select {
		case err := <-done:
			require.ErrorContains(t, err, "not a regular file", name)
		case <-time.After(time.Second):
			t.Fatalf("%s: opening a FIFO blocked waiting for a writer", name)
		}
	}
}
