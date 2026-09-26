package temporaltest

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestLauncherArgsIgnoreOrdinaryTestInvocation(t *testing.T) {
	_, _, _, _, handled, err := launcherArgs([]string{"-test.run=TestSomething"})
	require.NoError(t, err)
	require.False(t, handled)
}

func TestLauncherArgsRequireCompleteOwnedInvocation(t *testing.T) {
	_, _, _, _, handled, err := launcherArgs([]string{
		"server", "start-dev", "--ip", "127.0.0.1", "--port", "7233", "--namespace", "default",
	})
	require.ErrorContains(t, err, "incomplete Temporal supervisor arguments")
	require.True(t, handled)
}

func TestLauncherArgsRecoverParentAndServerIdentity(t *testing.T) {
	parentPID, hostPort, namespace, cliPath, handled, err := launcherArgs([]string{
		"server", "start-dev",
		"--ip", "127.0.0.1",
		"--port", "8123",
		"--namespace", "isolated",
		"--headless",
		parentPIDFlag, "42",
	})
	require.NoError(t, err)
	require.True(t, handled)
	require.Equal(t, 42, parentPID)
	require.Equal(t, "127.0.0.1:8123", hostPort)
	require.Equal(t, "isolated", namespace)
	require.Empty(t, cliPath, "no cliPathFlag was given")
}

// TestLauncherArgsRecoverCLIPath pins the seam RunLauncher reads to avoid its
// own download (#2116): the path StartWith resolved and passed through
// cliPathFlag comes back unchanged, so RunLauncher can use it as
// ExistingPath.
func TestLauncherArgsRecoverCLIPath(t *testing.T) {
	_, _, _, cliPath, handled, err := launcherArgs([]string{
		"server", "start-dev",
		"--ip", "127.0.0.1",
		"--port", "8123",
		"--namespace", "isolated",
		parentPIDFlag, "42",
		cliPathFlag, "/tmp/temporal-cli-go-sdk-1.9.1",
	})
	require.NoError(t, err)
	require.True(t, handled)
	require.Equal(t, "/tmp/temporal-cli-go-sdk-1.9.1", cliPath)
}

// TestEnsureCLICachedSkipsDownloadWhenAlreadyCached proves the cheap, common
// path: when the SDK's cache formula already names a file on disk,
// ensureCLICached never calls the download seam.
func TestEnsureCLICachedSkipsDownloadWhenAlreadyCached(t *testing.T) {
	dir := t.TempDir()
	old := cliCacheDir
	cliCacheDir = func() string { return dir }
	t.Cleanup(func() { cliCacheDir = old })

	require.NoError(t, os.WriteFile(cliCachePath(), []byte("fake cli"), 0o755))

	oldDownload := downloadCLI
	downloadCLI = func(context.Context) error {
		t.Fatal("downloadCLI called although the cache path already existed")
		return nil
	}
	t.Cleanup(func() { downloadCLI = oldDownload })

	path, err := ensureCLICached(t.Context())
	require.NoError(t, err)
	require.Equal(t, cliCachePath(), path)
}

// TestEnsureCLICachedDownloadsWhenMissing proves the other direction: when
// the cache path does not exist, ensureCLICached calls the download seam and
// only returns once the seam has populated it — the ordering that keeps a
// slow download out of the SDK's connect wait (#2116).
func TestEnsureCLICachedDownloadsWhenMissing(t *testing.T) {
	dir := t.TempDir()
	old := cliCacheDir
	cliCacheDir = func() string { return dir }
	t.Cleanup(func() { cliCacheDir = old })

	called := false
	oldDownload := downloadCLI
	downloadCLI = func(context.Context) error {
		called = true
		return os.WriteFile(cliCachePath(), []byte("fake cli"), 0o755)
	}
	t.Cleanup(func() { downloadCLI = oldDownload })

	path, err := ensureCLICached(t.Context())
	require.NoError(t, err)
	require.True(t, called, "ensureCLICached did not call the download seam for a missing cache path")
	require.Equal(t, cliCachePath(), path)
	require.FileExists(t, path)
}

// TestEnsureCLICachedNamesTheDownloadOnFailure is the negative direction: a
// failing download must say so, not surface as an unrelated connect timeout
// (the acceptance criterion for #2116 that the fix must not regress).
func TestEnsureCLICachedNamesTheDownloadOnFailure(t *testing.T) {
	dir := t.TempDir()
	old := cliCacheDir
	cliCacheDir = func() string { return dir }
	t.Cleanup(func() { cliCacheDir = old })

	oldDownload := downloadCLI
	downloadCLI = func(context.Context) error {
		return errors.New("temporal.download unreachable")
	}
	t.Cleanup(func() { downloadCLI = oldDownload })

	_, err := ensureCLICached(t.Context())
	require.ErrorContains(t, err, "downloading the Temporal CLI")
	require.ErrorContains(t, err, "temporal.download unreachable")
}

func TestCLICachePathUsesSDKVersionFormula(t *testing.T) {
	dir := t.TempDir()
	old := cliCacheDir
	cliCacheDir = func() string { return dir }
	t.Cleanup(func() { cliCacheDir = old })

	got := cliCachePath()
	require.Equal(t, filepath.Dir(got), dir)
	require.Contains(t, filepath.Base(got), "temporal-cli-go-sdk-")
}
