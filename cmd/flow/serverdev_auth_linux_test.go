package main

import (
	"os"
	"path/filepath"
	"syscall"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestConfigureDevAuthenticationRefusesGeneratedDocumentFIFOsWithoutWaiting(t *testing.T) {
	for _, name := range []string{"signing-keys.jwks", "trust-policy.json"} {
		t.Run(name, func(t *testing.T) {
			database := filepath.Join(t.TempDir(), "flowstate.db")
			initial, err := configureDevAuthentication(devFlags{auth: true, db: database}, "127.0.0.1:9233")
			require.NoError(t, err)

			path := filepath.Join(filepath.Dir(initial.keyPath), name)
			require.NoError(t, os.Remove(path))
			require.NoError(t, syscall.Mkfifo(path, 0o600))

			done := make(chan error, 1)
			go func() {
				_, err := configureDevAuthentication(devFlags{auth: true, db: database}, "127.0.0.1:9233")
				done <- err
			}()

			select {
			case err := <-done:
				require.Error(t, err)
			case <-time.After(time.Second):
				t.Fatal("writing a generated authentication document blocked on a FIFO")
			}
		})
	}
}
