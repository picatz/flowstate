package vault

import (
	"path/filepath"
	"syscall"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// TestWithRootCAsFileFIFOIsRefusedWithoutWaiting: a FIFO named as the CA bundle
// is refused at the open rather than blocking provider construction.
func TestWithRootCAsFileFIFOIsRefusedWithoutWaiting(t *testing.T) {
	t.Parallel()

	path := filepath.Join(t.TempDir(), "ca.pem")
	require.NoError(t, syscall.Mkfifo(path, 0o600))

	done := make(chan error, 1)
	go func() { done <- WithRootCAsFile(path)(&Provider{}) }()
	select {
	case err := <-done:
		require.ErrorContains(t, err, "not a regular file")
	case <-time.After(time.Second):
		t.Fatal("opening a CA bundle FIFO blocked waiting for a writer")
	}
}
