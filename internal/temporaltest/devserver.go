// Package temporaltest owns the Temporal dev-server process used by tests.
package temporaltest

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"os"
	"os/signal"
	"path/filepath"
	"runtime"
	"strconv"
	"syscall"
	"time"

	"go.temporal.io/sdk/client"
	"go.temporal.io/sdk/temporal"
	"go.temporal.io/sdk/testsuite"
)

const parentPIDFlag = "--flowstate-test-parent-pid"
const cliPathFlag = "--flowstate-test-cli-path"

// Start starts a Temporal dev server supervised by a copy of the current test
// binary. The supervisor survives an abrupt death of the test process and owns
// the only handle used to stop the server it started.
//
// Every TestMain that calls Start must call RunLauncher before parsing flags.
func Start(ctx context.Context, clientOptions *client.Options) (*testsuite.DevServer, error) {
	return StartWith(ctx, StartOptions{ClientOptions: clientOptions})
}

// StartOptions is what [StartWith] takes beyond the client options.
type StartOptions struct {
	// ClientOptions are the SDK's, for the client the server hands back.
	ClientOptions *client.Options

	// Stdout and Stderr receive what the supervisor writes, which is the
	// SDK's start-up log and the CLI's banner. Nil discards it, which is what
	// a test binary wants; a command whose own stdout carries an answer sends
	// both to its stderr instead.
	Stdout io.Writer
	Stderr io.Writer
}

// StartWith is [Start] with a say over where the supervisor's output goes.
//
// The Temporal CLI the supervisor runs is cached before the supervisor
// starts, and its path is handed to the supervisor so it never downloads: see
// [ensureCLICached] for why.
func StartWith(ctx context.Context, options StartOptions) (*testsuite.DevServer, error) {
	executable, err := os.Executable()
	if err != nil {
		return nil, fmt.Errorf("locating the test binary for Temporal supervision: %w", err)
	}

	cliPath, err := ensureCLICached(ctx)
	if err != nil {
		return nil, err
	}

	return testsuite.StartDevServer(ctx, testsuite.DevServerOptions{
		ExistingPath:  executable,
		ClientOptions: options.ClientOptions,
		ExtraArgs:     []string{parentPIDFlag, strconv.Itoa(os.Getpid()), cliPathFlag, cliPath},
		Stdout:        options.Stdout,
		Stderr:        options.Stderr,
	})
}

// cliCacheDir is where the SDK's own download cache keeps the Temporal CLI:
// os.TempDir(), matching testsuite.StartDevServer's default when neither
// DevServerOptions.ExistingPath nor CachedDownload.DestDir is set. A test
// overrides it to keep a fake download off the machine's shared cache, which
// other processes may be reading or writing concurrently.
var cliCacheDir = os.TempDir

// cliCachePath reproduces the SDK's own cache-path formula for the "default"
// CLI version (go.temporal.io/sdk@v1.48.0, testsuite/devserver.go's
// downloadIfNeeded) so the parent can check for, and name, the file the
// supervisor's own StartDevServer call would otherwise download.
func cliCachePath() string {
	path := filepath.Join(cliCacheDir(), "temporal-cli-go-sdk-"+temporal.SDKVersion)
	if runtime.GOOS == "windows" {
		path += ".exe"
	}
	return path
}

// downloadCLI populates the SDK's download cache by starting and immediately
// stopping a throwaway dev server. The SDK does not expose a download-only
// entry point, so this is the only way to reach its (tested) download and
// extraction code rather than duplicating it here.
//
// A seam: a test replaces this with a fake that never touches the network.
var downloadCLI = func(ctx context.Context) error {
	server, err := testsuite.StartDevServer(ctx, testsuite.DevServerOptions{
		CachedDownload: testsuite.CachedDownload{DestDir: cliCacheDir()},
	})
	if err != nil {
		return err
	}
	return server.Stop()
}

// ensureCLICached makes sure the SDK's download cache already holds the
// Temporal CLI, downloading it first if it does not, and returns its path.
//
// Doing this in the parent, before the supervisor starts, keeps a slow
// download out of the SDK's wait for the supervisor's server to accept
// connections. That wait retries for a fixed ~600 attempts at 100ms
// (waitServerReady/retryFor in the pinned go.temporal.io/sdk@v1.48.0), about
// 60s, which DevServerOptions has no field to extend; a download that instead
// ran inside the supervisor could exhaust it before the supervisor's own
// server was ready, failing the start with a "connection refused" that never
// mentions the download that actually ran out of time (#2116). With the CLI
// already cached, the supervisor's own StartDevServer call (RunLauncher,
// below) uses ExistingPath and never downloads at all.
func ensureCLICached(ctx context.Context) (string, error) {
	path := cliCachePath()
	if _, err := os.Stat(path); err == nil {
		return path, nil
	}
	if err := downloadCLI(ctx); err != nil {
		return "", fmt.Errorf("downloading the Temporal CLI before starting the supervised dev server: %w", err)
	}
	if _, err := os.Stat(path); err != nil {
		return "", fmt.Errorf("downloading the Temporal CLI before starting the supervised dev server: %s missing after download: %w", path, err)
	}
	return path, nil
}

// RunLauncher runs the supervisor mode selected by Start. It returns handled=false
// for an ordinary test-binary invocation. A TestMain must exit immediately with
// the returned error status when handled is true.
//
// The SDK does not expose its exec.Cmd, child PID, or a command-construction
// hook. ExistingPath is its only launcher seam. Keeping supervision here avoids
// identifying or killing processes by executable name, command pattern, or a
// process group that could include another test run.
func RunLauncher() (handled bool, err error) {
	parentPID, hostPort, namespace, cliPath, handled, err := launcherArgs(os.Args[1:])
	if !handled || err != nil {
		return handled, err
	}
	parent, err := newParentWatch(parentPID)
	if err != nil {
		return true, fmt.Errorf("watching Temporal test parent %d: %w", parentPID, err)
	}
	defer parent.close()
	if parent.gone() {
		return true, fmt.Errorf("temporal test parent %d disappeared before its supervisor started", parentPID)
	}

	// No deadline is inherited across exec. Bound the same download-and-startup
	// work as the package harnesses; after startup, this context is no longer used.
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	interrupt := make(chan os.Signal, 1)
	signal.Notify(interrupt, os.Interrupt, syscall.SIGTERM)
	defer signal.Stop(interrupt)

	parentCheck := time.NewTicker(25 * time.Millisecond)
	defer parentCheck.Stop()

	type startResult struct {
		server *testsuite.DevServer
		err    error
	}
	started := make(chan startResult, 1)
	go func() {
		server, startErr := testsuite.StartDevServer(ctx, testsuite.DevServerOptions{
			ExistingPath: cliPath,
			ClientOptions: &client.Options{
				HostPort:  hostPort,
				Namespace: namespace,
			},
		})
		started <- startResult{server: server, err: startErr}
	}()

	parentDisappeared := false
	var server *testsuite.DevServer
	for server == nil {
		select {
		case result := <-started:
			if result.err != nil {
				return true, fmt.Errorf("starting the supervised Temporal dev server: %w", result.err)
			}
			server = result.server
		case <-interrupt:
			parentDisappeared = true
		case <-parentCheck.C:
			parentDisappeared = parentDisappeared || parent.gone()
		}
	}
	if parentDisappeared {
		return true, stop(server)
	}

	for {
		select {
		case <-interrupt:
			return true, stop(server)
		case <-parentCheck.C:
			if parent.gone() {
				return true, stop(server)
			}
		}
	}
}

// launcherArgs also returns cliPath, the Temporal CLI [StartWith] already
// cached and downloaded before launching the supervisor, empty when the
// invocation did not carry cliPathFlag. RunLauncher passes it on as
// ExistingPath so the supervisor's own StartDevServer call never downloads.
func launcherArgs(args []string) (parentPID int, hostPort, namespace, cliPath string, handled bool, err error) {
	if len(args) < 2 || args[0] != "server" || args[1] != "start-dev" {
		return 0, "", "", "", false, nil
	}

	var host, port string
	for i := 2; i < len(args); i++ {
		if i+1 >= len(args) {
			break
		}
		switch args[i] {
		case "--ip":
			host = args[i+1]
			i++
		case "--port":
			port = args[i+1]
			i++
		case "--namespace":
			namespace = args[i+1]
			i++
		case parentPIDFlag:
			parentPID, err = strconv.Atoi(args[i+1])
			if err != nil || parentPID <= 0 {
				return 0, "", "", "", true, fmt.Errorf("invalid %s value %q", parentPIDFlag, args[i+1])
			}
			i++
		case cliPathFlag:
			cliPath = args[i+1]
			i++
		}
	}
	if parentPID == 0 || host == "" || port == "" || namespace == "" {
		return 0, "", "", "", true, errors.New("incomplete Temporal supervisor arguments")
	}

	return parentPID, net.JoinHostPort(host, port), namespace, cliPath, true, nil
}

func stop(server *testsuite.DevServer) error {
	if err := server.Stop(); err != nil {
		return fmt.Errorf("stopping the supervised Temporal dev server: %w", err)
	}
	return nil
}
