package main

import (
	"context"
	"flag"
	"fmt"
	"log/slog"
	"os"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.temporal.io/api/workflowservice/v1"
	"go.temporal.io/sdk/client"
	"go.temporal.io/sdk/log"
	"go.temporal.io/sdk/testsuite"
	"google.golang.org/protobuf/types/known/durationpb"

	"github.com/picatz/flowstate/internal/temporaltest"
	"github.com/picatz/flowstate/pkg/flowstate/v1/server"
)

// One Temporal server for the package, and a Temporal namespace per test.
//
// The same shape [server_test]'s TestMain uses, and for the same reason — see
// pkg/flowstate/v1/server/main_test.go:31 for the full rationale, which this file
// does not repeat. This package currently has one test that needs a real server
// (TestWatchFollowsARealRunningExecution); it used to start its own dev server,
// with a comment explaining that a *second* test needing one would be the point at
// which sharing pays for itself. #400 moves it here anyway, ahead of that second
// test, so the next dev-server test in this package lands on the shared shape
// instead of adding a fifth independent boot for #400 to find again.
//
// [server_test]: https://pkg.go.dev/github.com/picatz/flowstate/pkg/flowstate/v1/server

// devServer is the package's Temporal server, started once by TestMain.
var devServer *testsuite.DevServer

// namespaceOrdinal makes each registered namespace name unique, since two subtests
// of one parent share a sanitized name.
var namespaceOrdinal atomic.Int64

func TestMain(m *testing.M) {
	if handled, err := temporaltest.RunLauncher(); handled {
		if err != nil {
			fmt.Fprintf(os.Stderr, "%v\n", err)
			os.Exit(1)
		}
		os.Exit(0)
	}

	// testing.Short() reads a flag, and flags are only populated once parsed.
	// TestMain is the one entry point that runs before the testing package has
	// done that parsing itself, so it has to be done here first.
	flag.Parse()

	if testing.Short() {
		// Every test in this package that needs the server reaches it through
		// newTemporalNamespace, which skips before touching the nil devServer
		// left below. Skipping the download-and-boot here as well, rather than
		// only inside that helper, is what keeps `-short` from paying the dev
		// server's ~2 minutes of startup cost it exists to avoid.
		code := m.Run()
		removeFlowBinary()
		removeExamplePluginDir()
		os.Exit(code)
	}

	code, err := runPackageTests(m)

	// Here rather than deferred inside runPackageTests, for the reason that
	// function's own doc gives: os.Exit below runs no deferred function. See
	// buildFlowBinary, which compiles the binary once for the whole test binary
	// and so has no *testing.T whose Cleanup could remove it afterwards.
	removeFlowBinary()
	removeExamplePluginDir()

	if err != nil {
		fmt.Fprintf(os.Stderr, "%v\n", err)
		os.Exit(1)
	}

	os.Exit(code)
}

// runPackageTests starts the server, runs the package's tests, and stops it.
//
// Separate from TestMain because os.Exit does not run deferred functions: a
// TestMain that both defers the shutdown and exits leaves the server process
// behind on every run.
func runPackageTests(m *testing.M) (int, error) {
	// Bounds startup only. The SDK uses this context to download the executable
	// if it is not cached and to wait for the server to answer; the process it
	// starts outlives the context and is stopped below.
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()

	started, err := temporaltest.Start(ctx,
		// No *testing.T exists here to attach a log to, and the per-test clients
		// below carry one each. Warnings and errors still reach stderr, so a
		// server that comes up wrong says so.
		&client.Options{
			Logger: log.NewStructuredLogger(slog.New(slog.NewTextHandler(
				os.Stderr, &slog.HandlerOptions{Level: slog.LevelWarn}))),
		},
	)
	if err != nil {
		return 0, fmt.Errorf("starting the Temporal dev server this package shares: %w", err)
	}
	defer func() { _ = started.Stop() }()

	devServer = started

	return m.Run(), nil
}

// mustNewFlowstateServer is [server.New] for a test whose subject is not the
// construction.
//
// [server.New] reports an error because a [server.Option] can refuse — see
// [server.WithNamespace]. The tests in this package all build the
// zero-configuration or nil-Temporal-client server, so nothing here can refuse;
// the error is asserted rather than dropped so that stays a fact somebody
// checked instead of an assumption.
func mustNewFlowstateServer(t testing.TB, temporal client.Client, opts ...server.Option) *server.FlowstateServer {
	t.Helper()

	s, err := server.New(temporal, opts...)
	require.NoError(t, err)

	return s
}

// newTemporalNamespace registers a Temporal namespace for one test and returns a
// client bound to it.
func newTemporalNamespace(t *testing.T) client.Client {
	t.Helper()

	if testing.Short() {
		t.Skip("skipping: needs the shared Temporal dev server, not started under -short; CI runs the full suite")
	}

	namespace := namespaceNameFor(t)

	_, err := devServer.Client().WorkflowService().RegisterNamespace(t.Context(),
		&workflowservice.RegisterNamespaceRequest{
			Namespace: namespace,
			// The shortest retention Temporal accepts. Nothing registered here
			// outlives the test process, so the value only has to be legal.
			WorkflowExecutionRetentionPeriod: durationpb.New(24 * time.Hour),
		})
	require.NoError(t, err, "registering a Temporal namespace for this test")

	temporal, err := client.Dial(client.Options{
		HostPort:  devServer.FrontendHostPort(),
		Namespace: namespace,
	})
	require.NoError(t, err)
	t.Cleanup(temporal.Close)

	// Registration is accepted before the namespace is servable, so the first use
	// is retried rather than assumed. It settles in single-digit milliseconds on a
	// dev server; the budget is for a machine under load, and being wrong about
	// this would look like a flake in whichever test drew the short straw.
	require.Eventually(t, func() bool {
		_, err := temporal.ListWorkflow(t.Context(),
			&workflowservice.ListWorkflowExecutionsRequest{PageSize: 1})
		return err == nil
	}, 30*time.Second, 20*time.Millisecond,
		"the namespace registered for this test never became usable")

	return temporal
}

// namespaceNameFor derives a legal Temporal namespace name from a test's name.
//
// Named after the test so that a line in a server log, or a namespace left behind
// by a crash, says which test produced it. Numbered because two subtests of one
// parent sanitize to the same string, and because a name that collides would give
// one test another's runs — the exact isolation this is here to provide.
func namespaceNameFor(t *testing.T) string {
	t.Helper()

	safe := strings.Map(func(r rune) rune {
		switch {
		case r >= 'a' && r <= 'z', r >= 'A' && r <= 'Z', r >= '0' && r <= '9', r == '-':
			return r
		default:
			return '-'
		}
	}, t.Name())

	// Long enough to identify a test, short enough to stay readable in a log line.
	const maxNameLength = 48
	if len(safe) > maxNameLength {
		safe = safe[:maxNameLength]
	}

	return fmt.Sprintf("%s-%d", safe, namespaceOrdinal.Add(1))
}
