// Command devtemporal starts one Temporal dev server that stays up, for the
// packages whose tests share one to attach to instead of each booting its
// own (#1738).
//
//	make dev-temporal
//	export FLOWSTATE_TEST_TEMPORAL_ADDRESS=127.0.0.1:12345   # what it prints
//	go test ./pkg/flowstate/v1/engine/ -run TestOne
//
// The server is the same `temporal` CLI the test suite's supervisor starts,
// from the SDK's own download cache, so what a test sees is what it sees in
// CI; only who starts and stops it changes. It stops on SIGINT or SIGTERM.
// Every namespace a test registers carries its process id, so runs can come
// and go against it without colliding.
package main

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"syscall"
	"time"

	"go.temporal.io/sdk/client"
	"go.temporal.io/sdk/log"
	"go.temporal.io/sdk/testsuite"

	"github.com/picatz/flowstate/internal/temporaltest"
)

func main() {
	if err := run(); err != nil {
		fmt.Fprintf(os.Stderr, "devtemporal: %v\n", err)
		os.Exit(1)
	}
}

func run() error {
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	// Bounds the download and the wait for the server to answer; the process
	// outlives it and is stopped below.
	startCtx, cancel := context.WithTimeout(ctx, 2*time.Minute)
	defer cancel()

	// Stdout carries the export line and nothing else, so a shell can eval
	// what this prints; the SDK's start-up log and the CLI's banner go to
	// stderr, where they can still be read.
	server, err := testsuite.StartDevServer(startCtx, testsuite.DevServerOptions{
		ClientOptions: &client.Options{
			Logger: log.NewStructuredLogger(slog.New(slog.NewTextHandler(os.Stderr, nil))),
		},
		Stdout: os.Stderr,
		Stderr: os.Stderr,
	})
	if err != nil {
		return fmt.Errorf("starting the Temporal dev server: %w", err)
	}

	fmt.Printf("export %s=%s\n", temporaltest.AddressEnv, server.FrontendHostPort())
	fmt.Fprintf(os.Stderr, "devtemporal: serving at %s until interrupted\n", server.FrontendHostPort())

	<-ctx.Done()

	if err := server.Stop(); err != nil {
		return fmt.Errorf("stopping the Temporal dev server: %w", err)
	}
	return nil
}
