package main

import (
	"flag"
	"fmt"
	"log/slog"
	"os"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.temporal.io/api/workflowservice/v1"
	"go.temporal.io/sdk/client"
	"go.temporal.io/sdk/log"
	"google.golang.org/protobuf/types/known/durationpb"

	"github.com/picatz/flowstate/internal/temporaltest"
	"github.com/picatz/flowstate/pkg/flowstate/v1/server"

	"github.com/picatz/flowstate/internal/testkit"
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
var devServer temporaltest.Server

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

	code, err := temporaltest.RunPackage(m, &devServer, &client.Options{
		// No *testing.T exists here to attach a log to, and the per-test clients
		// carry one each, which is where a line is worth reading anyway.
		// Warnings and errors still reach stderr, so a server that comes up wrong
		// says so.
		Logger: log.NewStructuredLogger(slog.New(slog.NewTextHandler(
			os.Stderr, &slog.HandlerOptions{Level: slog.LevelWarn}))),
	})

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

	namespace := testkit.NamespaceNameFor(t)

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
