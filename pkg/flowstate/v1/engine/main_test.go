package engine_test

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
	"go.temporal.io/sdk/worker"
	"google.golang.org/protobuf/types/known/durationpb"

	"github.com/picatz/flowstate/internal/temporaltest"
	"github.com/picatz/flowstate/pkg/flowstate/v1/engine"
)

// One Temporal server for the package, and a Temporal namespace per test.
//
// This is the same shape [server_test]'s TestMain uses, and for the same reason:
// this package used to start four independent dev servers — one each in
// examples_durable_test.go, versioning_test.go, workflow_e2e_test.go and
// replay_record_test.go — which is the exact contention `server`'s 65-line comment
// (pkg/flowstate/v1/server/main_test.go:31) already paid to learn about: four
// frontends, four history services, four matching services, all booting inside the
// same `make test` invocation. Replicated here rather than reinvented; see that
// file for the reasoning this one only summarizes.
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

	if withoutDevServer() {
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

// withoutDevServer reports that this process will not start the package's
// Temporal dev server, and so that every test needing one must skip.
//
// One predicate, read by [TestMain] to decide whether to boot and by
// [newTemporalNamespaceWithIdentity] to decide whether to skip. Two spellings
// of "no server here" is the shape CLAUDE.md's "one constant cannot disagree
// with itself" names: the half that skips and the half that boots would
// eventually answer differently, and the way that fails is a nil devServer
// dereference in whichever test the disagreement reaches first.
func withoutDevServer() bool { return testing.Short() || fuzzing() }

// fuzzing reports whether this process was started to fuzz — the coordinator
// `go test -fuzz` starts, or a worker it forks.
//
// The dev server is skipped for a fuzzing process for the reason `-short`
// skips it: nothing being fuzzed here needs one. [FuzzSignalDeliveryDecode]
// decodes bytes. What makes it worth detecting rather than leaving to whoever
// writes the command is that a fuzz run is *several* processes — the
// coordinator, plus a worker it may restart — and every one of them would pay
// the boot. The fuzz tiers run one command per target from
// tools/fuzztargets/targets.txt with no per-target flags (#857), by design, so
// there is nowhere to put a `-short` for this package alone; and the deep
// tier's 10m of fuzzing under a 900s test timeout leaves no room to spend two
// minutes per process on a server nothing asks a question of.
//
// The flags are read rather than declared: the testing package registers
// `test.fuzz` and `test.fuzzworker` itself, and [flag.Parse] above has already
// run, so this is the value the run was actually given. Lookup is guarded
// because a binary built without the testing flags registered would otherwise
// panic here rather than answer.
//
// The cost, stated: `go test -fuzz FuzzX ./engine/` with no `-run` filter now
// skips every server-backed test in this package rather than running the suite
// and then fuzzing. That is what `-short` already means here, and both fuzz
// tiers pass `-run=XXX`, so the case this changes is a developer fuzzing this
// package by hand — who gets the fuzzing they asked for, and skips saying so.
func fuzzing() bool {
	for _, name := range []string{"test.fuzz", "test.fuzzworker"} {
		f := flag.Lookup(name)
		if f == nil {
			continue
		}
		if value := f.Value.String(); value != "" && value != "false" {
			return true
		}
	}
	return false
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

// newTemporalNamespace registers a Temporal namespace for one test and returns a
// client bound to it.
func newTemporalNamespace(t *testing.T) client.Client {
	t.Helper()

	return newTemporalNamespaceWithIdentity(t, "")
}

// newTemporalNamespaceWithIdentity is [newTemporalNamespace] for the one caller that
// cannot take the SDK's default client identity: [TestRecordReplayCorpus] writes what
// the server recorded into this repository's committed testdata, and the default
// identity is `<pid>@<hostname>` — the recording machine's hostname, permanently in a
// file this repository then carries. An empty identity leaves the SDK default in
// place, which every other caller wants.
func newTemporalNamespaceWithIdentity(t *testing.T, identity string) client.Client {
	t.Helper()

	if withoutDevServer() {
		t.Skip("skipping: needs the shared Temporal dev server, which this process did not start (-short, or a fuzzing run); CI runs the full suite")
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
		Identity:  identity,
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

// startWorker runs the engine's workflow and activities against one namespace,
// stopping when the test does, with the SDK's own worker defaults.
func startWorker(t *testing.T, temporal client.Client) {
	t.Helper()

	w := worker.New(temporal, engine.RunTaskQueueName, worker.Options{})
	engine.Register(w)

	require.NoError(t, w.Start())
	t.Cleanup(w.Stop)
}
