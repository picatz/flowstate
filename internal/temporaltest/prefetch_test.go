package temporaltest

import (
	"context"
	"os"
	"runtime"
	"testing"

	"github.com/stretchr/testify/require"
	"go.temporal.io/sdk/log"
)

// TestPrefetchCLIRefetchesACachedFileThatDoesNotRun is the poisoned or
// truncated cache entry: a file at the cache path that cannot report its
// version is replaced, not trusted; one that runs is left alone.
func TestPrefetchCLIRefetchesACachedFileThatDoesNotRun(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("the fake CLI is a shell script")
	}
	dir := t.TempDir()
	old := cliCacheDir
	cliCacheDir = func() string { return dir }
	t.Cleanup(func() { cliCacheDir = old })
	require.NoError(t, os.WriteFile(cliCachePath(), []byte("not an executable"), 0o755))

	downloads := 0
	oldDownload := downloadCLI
	downloadCLI = func(context.Context, log.Logger) error {
		downloads++
		return os.WriteFile(cliCachePath(), []byte("#!/bin/sh\necho temporal version fake\n"), 0o755)
	}
	t.Cleanup(func() { downloadCLI = oldDownload })

	path, err := PrefetchCLI(t.Context(), discardLogger{})
	require.NoError(t, err)
	require.Equal(t, 1, downloads)
	require.Equal(t, cliCachePath(), path)

	downloads = 0
	_, err = PrefetchCLI(t.Context(), discardLogger{})
	require.NoError(t, err)
	require.Zero(t, downloads, "a CLI that runs is trusted and not fetched again")
}
