// Package temporaltest owns the Temporal dev-server process used by tests.
package temporaltest

import (
	"context"
	"errors"
	"fmt"
	"net"
	"os"
	"os/signal"
	"strconv"
	"time"

	"go.temporal.io/sdk/client"
	"go.temporal.io/sdk/testsuite"
)

const parentPIDFlag = "--flowstate-test-parent-pid"

// Start starts a Temporal dev server supervised by a copy of the current test
// binary. The supervisor survives an abrupt death of the test process and owns
// the only handle used to stop the server it started.
//
// Every TestMain that calls Start must call RunLauncher before parsing flags.
func Start(ctx context.Context, clientOptions *client.Options) (*testsuite.DevServer, error) {
	executable, err := os.Executable()
	if err != nil {
		return nil, fmt.Errorf("locating the test binary for Temporal supervision: %w", err)
	}

	return testsuite.StartDevServer(ctx, testsuite.DevServerOptions{
		ExistingPath:  executable,
		ClientOptions: clientOptions,
		ExtraArgs:     []string{parentPIDFlag, strconv.Itoa(os.Getpid())},
	})
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
	parentPID, hostPort, namespace, handled, err := launcherArgs(os.Args[1:])
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
	signal.Notify(interrupt, os.Interrupt)
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

func launcherArgs(args []string) (parentPID int, hostPort, namespace string, handled bool, err error) {
	if len(args) < 2 || args[0] != "server" || args[1] != "start-dev" {
		return 0, "", "", false, nil
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
				return 0, "", "", true, fmt.Errorf("invalid %s value %q", parentPIDFlag, args[i+1])
			}
			i++
		}
	}
	if parentPID == 0 || host == "" || port == "" || namespace == "" {
		return 0, "", "", true, errors.New("incomplete Temporal supervisor arguments")
	}

	return parentPID, net.JoinHostPort(host, port), namespace, true, nil
}

func stop(server *testsuite.DevServer) error {
	if err := server.Stop(); err != nil {
		return fmt.Errorf("stopping the supervised Temporal dev server: %w", err)
	}
	return nil
}
