package vault

import (
	"path/filepath"
	"syscall"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// TestAServiceAccountTokenFIFOIsRefusedWithoutWaiting: a FIFO named as the
// token path, or swapped in for it, is refused at the open rather than
// blocking every login waiting for a writer.
func TestAServiceAccountTokenFIFOIsRefusedWithoutWaiting(t *testing.T) {
	t.Parallel()

	path := filepath.Join(t.TempDir(), "token")
	require.NoError(t, syscall.Mkfifo(path, 0o600))

	done := make(chan error, 1)
	go func() { _, err := readBoundedRegular(path, maxJWTBytes); done <- err }()
	select {
	case err := <-done:
		require.ErrorContains(t, err, "not a regular file")
	case <-time.After(time.Second):
		t.Fatal("opening a token FIFO blocked waiting for a writer")
	}
}
