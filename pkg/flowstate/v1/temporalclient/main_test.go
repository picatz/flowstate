package temporalclient

import (
	"context"
	"flag"
	"fmt"
	"os"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.temporal.io/api/workflowservice/v1"
	"go.temporal.io/sdk/client"
	"go.temporal.io/sdk/testsuite"
	"google.golang.org/protobuf/types/known/durationpb"

	"github.com/picatz/flowstate/internal/temporaltest"
)

// One Temporal server for the package, and a Temporal namespace per test.
//
// This is the same shape [server_test]'s TestMain uses, and for the same reason —
// see pkg/flowstate/v1/server/main_test.go for the full argument. Replicated here
// rather than reinvented: NewPool's namespace-existence check is the one thing in
// this package that has to talk to a real cluster, since it is *asking the cluster*
// what makes the test meaningful, and a fake mapper cannot stand in for the answer
// Temporal itself gives about a namespace it does not have.
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
		os.Exit(m.Run())
	}

	code, err := runPackageTests(m)
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

	started, err := temporaltest.Start(ctx, &client.Options{})
	if err != nil {
		return 0, fmt.Errorf("starting the Temporal dev server this package shares: %w", err)
	}
	defer func() { _ = started.Stop() }()

	devServer = started

	return m.Run(), nil
}

// newTemporalNamespace registers a Temporal namespace for one test and returns its
// name, for a Config that points NewPool at the shared dev server.
func newTemporalNamespace(t *testing.T) string {
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

	return namespace
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
