package temporaltest

import (
	"context"
	"errors"
	"os"
	"testing"

	"github.com/stretchr/testify/require"
	"go.temporal.io/sdk/client"
	"go.temporal.io/sdk/log"
	"go.temporal.io/sdk/testsuite"
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
	downloadCLI = func(context.Context, log.Logger) error {
		t.Fatal("downloadCLI called although the cache path already existed")
		return nil
	}
	t.Cleanup(func() { downloadCLI = oldDownload })

	path, err := ensureCLICached(t.Context(), discardLogger{})
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
	downloadCLI = func(context.Context, log.Logger) error {
		called = true
		return os.WriteFile(cliCachePath(), []byte("fake cli"), 0o755)
	}
	t.Cleanup(func() { downloadCLI = oldDownload })

	path, err := ensureCLICached(t.Context(), discardLogger{})
	require.NoError(t, err)
	require.True(t, called, "ensureCLICached did not call the download seam for a missing cache path")
	require.Equal(t, cliCachePath(), path)
	require.FileExists(t, path)
}

// TestEnsureCLICachedTrustsTheCacheOverAReportedError pins the F2 fix:
// downloadCLI's deliberately invalid HostPort (cliDownloadOnlyHostPort) makes
// it return a non-nil error even when the download it triggered succeeded,
// so ensureCLICached must decide success or failure by statting the cache
// path, never by whether downloadCLI returned an error. This also covers a
// concurrent download racing on the SDK's own atomic rename: whichever
// process's error surfaces here, the file landing is what matters.
func TestEnsureCLICachedTrustsTheCacheOverAReportedError(t *testing.T) {
	dir := t.TempDir()
	old := cliCacheDir
	cliCacheDir = func() string { return dir }
	t.Cleanup(func() { cliCacheDir = old })

	oldDownload := downloadCLI
	downloadCLI = func(context.Context, log.Logger) error {
		// The real seam always errors on success too (invalid HostPort), but
		// the download itself landed the file.
		require.NoError(t, os.WriteFile(cliCachePath(), []byte("fake cli"), 0o755))
		return errors.New("invalid HostPort: address download-only: missing port in address")
	}
	t.Cleanup(func() { downloadCLI = oldDownload })

	path, err := ensureCLICached(t.Context(), discardLogger{})
	require.NoError(t, err, "a reported error was trusted over the cache path that was actually populated")
	require.Equal(t, cliCachePath(), path)
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
	downloadCLI = func(context.Context, log.Logger) error {
		return errors.New("temporal.download unreachable")
	}
	t.Cleanup(func() { downloadCLI = oldDownload })

	_, err := ensureCLICached(t.Context(), discardLogger{})
	require.ErrorContains(t, err, "downloading the Temporal CLI")
	require.ErrorContains(t, err, "temporal.download unreachable")
}

// TestCLICachePathMatchesTheSDKsOwnResolver replaces a circular check (this
// package's own formula reproduced back at itself) with one against the
// SDK's real cache lookup: a file seeded at exactly the path cliCachePath
// computes must read to the SDK as a cache hit. Only the version cannot
// drift silently (temporal.SDKVersion is exported and shared verbatim); the
// "temporal-cli-go-sdk-" prefix and the DestDir/os.TempDir() default are
// copied by hand, and this is what actually holds them to the SDK's own
// downloadIfNeeded (go.temporal.io/sdk@v1.48.0, testsuite/devserver.go)
// rather than to a description of it.
//
// A wrong path would make the SDK treat the seeded file as a miss and try to
// fetch instead, which the cancelled context turns into a distinguishable
// error — no network is reached either way.
func TestCLICachePathMatchesTheSDKsOwnResolver(t *testing.T) {
	dir := t.TempDir()
	old := cliCacheDir
	cliCacheDir = func() string { return dir }
	t.Cleanup(func() { cliCacheDir = old })

	require.NoError(t, os.WriteFile(cliCachePath(), []byte("fake cli"), 0o755))

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	_, err := testsuite.StartDevServer(ctx, testsuite.DevServerOptions{
		CachedDownload: testsuite.CachedDownload{DestDir: dir},
		ClientOptions:  &client.Options{HostPort: cliDownloadOnlyHostPort},
	})
	require.ErrorContains(t, err, "invalid HostPort",
		"the SDK did not read the seeded file as a cache hit at cliCachePath's location, so it "+
			"does not match the SDK's own resolver")
}
