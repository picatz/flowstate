package temporaltest

import (
	"context"
	"fmt"
	"testing"
	"time"

	"go.temporal.io/sdk/client"
	"go.temporal.io/sdk/testsuite"
)

// RunPackage starts the dev server, stores it where the package's tests read
// it, runs the package's tests, and stops the server.
//
// This is the body every package sharing one dev server had written for its
// own TestMain (#1709): the server package, the engine, temporalclient and
// cmd/flow each started the server, ran, and stopped it in the same twenty
// lines. What differed was only the client options — a logger, or none — which
// is the parameter.
//
// Separate from TestMain because os.Exit does not run deferred functions: a
// TestMain that both defers the shutdown and exits leaves the server process
// behind on every run. The caller still owns its exit, and whatever it has to
// do before exiting (cmd/flow removes a binary it built) happens between this
// returning and that.
//
// into is the package's variable, assigned before the tests run so every test
// that reaches for the server finds it; the caller's `-short` branch is what
// keeps that variable nil and the tests skipping.
func RunPackage(m *testing.M, into **testsuite.DevServer, clientOptions *client.Options) (int, error) {
	if into == nil {
		// Said now rather than as a nil dereference after the server is up,
		// which would also leave the server running.
		return 0, fmt.Errorf("RunPackage needs the package's dev server variable to store the server in; into is nil")
	}

	// Bounds startup only. The SDK uses this context to download the executable
	// if it is not cached and to wait for the server to answer; the process it
	// starts outlives the context and is stopped below.
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()

	started, err := Start(ctx, clientOptions)
	if err != nil {
		return 0, fmt.Errorf("starting the Temporal dev server this package shares: %w", err)
	}
	defer func() { _ = started.Stop() }()

	*into = started

	return m.Run(), nil
}
