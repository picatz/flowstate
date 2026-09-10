package auth_test

import (
	"path/filepath"
	"syscall"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/picatz/flowstate/pkg/flowstate/v1/auth"
)

// TestOIDCVerifierRefusesAJWKSFIFOWithoutWaiting proves the byte limit is not
// mistaken for a time bound. Opening a FIFO for an ordinary blocking read waits
// for a writer forever; configuration must identify and reject it at startup.
func TestOIDCVerifierRefusesAJWKSFIFOWithoutWaiting(t *testing.T) {
	path := filepath.Join(t.TempDir(), "issuer.jwks")
	require.NoError(t, syscall.Mkfifo(path, 0o600))

	done := make(chan error, 1)
	go func() {
		_, err := auth.NewOIDCVerifier(auth.Policy{Issuers: []auth.TrustedIssuer{{
			Name:      "offline",
			Issuer:    "https://issuer.example.com",
			Audiences: []string{"flowstate"},
			JWKSFile:  path,
		}}})
		done <- err
	}()

	select {
	case err := <-done:
		require.ErrorContains(t, err, "not a regular file")
	case <-time.After(time.Second):
		t.Fatal("opening a JWKS FIFO blocked waiting for a writer")
	}
}
