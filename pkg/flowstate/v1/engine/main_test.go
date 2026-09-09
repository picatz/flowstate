package engine_test

import (
	"flag"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.temporal.io/api/workflowservice/v1"
	"go.temporal.io/sdk/client"
	"go.temporal.io/sdk/worker"
	"google.golang.org/protobuf/types/known/durationpb"

	"github.com/picatz/flowstate/internal/temporaltest"
	"github.com/picatz/flowstate/pkg/flowstate/v1/engine"

	"github.com/picatz/flowstate/internal/testkit"
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

	if withoutDevServer() {
		// Every test in this package that needs the server reaches it through
		// newTemporalNamespace, which skips before touching the nil devServer
		// left below. Skipping the download-and-boot here as well, rather than
		// only inside that helper, is what keeps `-short` from paying the dev
		// server's ~2 minutes of startup cost it exists to avoid.
		os.Exit(m.Run())
	}

	code, err := temporaltest.RunPackage(m, &devServer, &client.Options{})
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
func withoutDevServer() bool {
	return testing.Short() || fuzzing() || os.Getenv(workflowSliceReplayHelperEnv) != ""
}

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

	return newTemporalNamespaceWithOptions(t, client.Options{Identity: identity})
}

// newTemporalNamespaceWithOptions is the shared body of the constructors
// above, for the caller that has something else to say about the client — the
// poison-pill test hands its worker a logger it can read back, since what the
// SDK logs about a workflow task is the only evidence of whether one was
// retried. HostPort and Namespace are this harness's to set; the rest of
// options is the caller's.
func newTemporalNamespaceWithOptions(t *testing.T, options client.Options) client.Client {
	t.Helper()

	if withoutDevServer() {
		t.Skip("skipping: needs the shared Temporal dev server, which this process did not start (-short, or a fuzzing run); CI runs the full suite")
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

	options.HostPort = devServer.FrontendHostPort()
	options.Namespace = namespace
	temporal, err := client.Dial(options)
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

// startWorker runs the engine's workflow and activities against one namespace,
// stopping when the test does, with the SDK's own worker defaults and the one
// policy every deployed worker carries: a workflow task that panics fails the
// run rather than retrying forever ([engine.WorkerWorkflowPanicPolicy]).
func startWorker(t *testing.T, temporal client.Client) {
	t.Helper()

	w := worker.New(temporal, engine.RunTaskQueueName, worker.Options{
		WorkflowPanicPolicy: engine.WorkerWorkflowPanicPolicy,
	})
	engine.Register(w)

	require.NoError(t, w.Start())
	t.Cleanup(w.Stop)
}
