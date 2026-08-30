package server_test

import (
	"context"
	"flag"
	"fmt"
	"log/slog"
	"os"
	"slices"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	enumspb "go.temporal.io/api/enums/v1"
	historypb "go.temporal.io/api/history/v1"
	"go.temporal.io/api/workflowservice/v1"
	"go.temporal.io/sdk/client"
	"go.temporal.io/sdk/converter"
	"go.temporal.io/sdk/log"
	"go.temporal.io/sdk/testsuite"
	"go.temporal.io/sdk/worker"
	"google.golang.org/protobuf/types/known/durationpb"

	"github.com/picatz/flowstate/internal/temporaltest"
	v1 "github.com/picatz/flowstate/pkg/flowstate/v1"
	"github.com/picatz/flowstate/pkg/flowstate/v1/engine"
	"github.com/picatz/flowstate/pkg/flowstate/v1/server"
)

// One Temporal server for the package, and a Temporal namespace per test.
//
// Every test here that needs a real server used to start its own, which by the
// end was thirteen dev servers for one package — thirteen processes, each with a
// frontend, a history service, a matching service and its own store, all on the
// runner that is also compiling and running everything else. That is where the
// suite's reputation for being slow and unreliable came from. The most visible
// symptom was TestWaitSurvivesAWorkerRestart, which waits ninety seconds for the
// server to notice a worker is gone and redeliver its work to a replacement: on a
// machine sharing itself between several servers, ninety seconds of wall clock is
// not ninety seconds of progress, and the test failed for want of CPU rather than
// for want of correctness. Two speculative fixes to that test were tried and
// reverted, because the test was never the problem.
//
// So the isolation each test genuinely needs is separated from the process that
// provides it. What a test needs is a namespace nobody else is writing to: its
// runs, its visibility records, and its own workers polling a task queue that no
// other test is polling. A namespace gives all of that, costs a registration call
// and a few milliseconds, and is the boundary Temporal is built around. What no
// test needs is a private copy of Temporal.
//
// This matters most to listing. `List` scans a namespace and filters by tenant
// memo, so runs another test happened to start would be inside the scan whether
// they belong to the caller or not — a shared namespace would make a listing
// test's result depend on what else was running at the time. Registering a
// namespace per test keeps every such test asking about a namespace only it has
// written to.

// devServer is the package's Temporal server, started once by TestMain.
var devServer *testsuite.DevServer

// namespaceOrdinal makes each registered namespace name unique, since two
// subtests of one parent share a sanitized name.
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
		// only inside that helper, is what keeps `-short` from paying the
		// dev server's ~2 minutes of startup cost it exists to avoid.
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

	started, err := temporaltest.Start(ctx,
		// No *testing.T exists here to attach a log to, and the per-test clients
		// below carry one each, which is where a line is worth reading anyway.
		// Warnings and errors still reach stderr, so a server that comes up wrong
		// says so.
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

// mustNew is [server.New] for a test whose subject is not the construction.
//
// [server.New] reports an error because a [server.Option] can refuse — see
// [server.WithNamespace], which checks the namespace grammar every
// tenant-scoped derivation in that package assumes. A test that means "a server
// configured like this" should stop at the construction with the option's own
// message rather than nil-panic several lines later on something unrelated.
//
// A test whose subject *is* the refusal calls [server.New] directly and asserts
// on the error; see TestNewRefusesANamespaceOutsideTheGrammar.
func mustNew(t testing.TB, temporal client.Client, opts ...server.Option) *server.FlowstateServer {
	t.Helper()

	s, err := server.New(temporal, opts...)
	require.NoError(t, err)

	return s
}

// newTemporalNamespace registers a Temporal namespace for one test and returns a
// client bound to it, along with its name.
//
// The name is returned because a deployment that maps Flowstate namespaces onto
// Temporal ones has to be told which Temporal namespace to map onto, and that is
// now a per-test value rather than "default".
func newTemporalNamespace(t *testing.T) (client.Client, string) {
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
		Logger:    newTestingLogger(t),
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

	return temporal, namespace
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
//
// Those defaults are what almost every test here wants, because a worker
// configured for a test is a worker no deployment runs.
func startWorker(t *testing.T, temporal client.Client) {
	t.Helper()

	startWorkerWithOptions(t, temporal, worker.Options{})
}

// startWorkerWithOptions is startWorker for the tests that have something to say
// about the worker itself.
//
// One such test exists: the run that grows until it cannot be carried forward
// raises the deadlock budget, for the reason
// [conformance.BoundaryDeadlockDetectionTimeout] gives. Keeping the option at the call
// site rather than in the shared helper is the point, so that a worker with a
// budget nobody deploys is visibly the exception it is.
func startWorkerWithOptions(t *testing.T, temporal client.Client, options worker.Options) {
	t.Helper()

	w := worker.New(temporal, engine.RunTaskQueueName, options)
	engine.Register(w)

	require.NoError(t, w.Start())
	t.Cleanup(w.Stop)
}

// waitUntilParkedAtTheGate blocks until the run has actually reached its approval
// gate.
//
// Nothing on the RPC surface can answer this, which is why it reads history: a
// status says a run is going, not what it is doing. A gate with a timeout starts a
// durable timer and nothing else in this workload starts one, so a TimerStarted
// event is exactly the evidence wanted — and history is written when the workflow
// task completes, so unlike a listing there is no visibility lag to wait out.
func waitUntilParkedAtTheGate(t *testing.T, temporal client.Client, workflowID string) {
	t.Helper()

	require.Eventually(t, func() bool {
		events, err := historyOf(t.Context(), temporal, workflowID)
		if err != nil {
			return false
		}

		return slices.ContainsFunc(events, func(event *historypb.HistoryEvent) bool {
			return event.GetEventType() == enumspb.EVENT_TYPE_TIMER_STARTED
		})
	}, 30*time.Second, 100*time.Millisecond, "the run never reached its approval gate")
}

// stepsScheduled reports the message each step this run scheduled was given, in
// the order they were scheduled.
//
// The message identifies the step because every step in gatedWorkflow is an echo
// with its own, which is cheaper than correlating scheduled events back to step
// ids and says the same thing.
func stepsScheduled(ctx context.Context, temporal client.Client, workflowID string) ([]string, error) {
	events, err := historyOf(ctx, temporal, workflowID)
	if err != nil {
		return nil, err
	}

	var messages []string
	for _, event := range events {
		attributes := event.GetActivityTaskScheduledEventAttributes()
		if attributes == nil {
			continue
		}
		switch attributes.GetActivityType().GetName() {
		case "Task", "TaskWithPrev", "TaskInScope", "TaskAuthorized", "TaskInScopeAuthorized":
			// Step dispatch; decode its first argument below.
		default:
			// Admission and vars activities are not steps and carry different
			// input shapes.
			continue
		}

		payloads := attributes.GetInput().GetPayloads()
		if len(payloads) == 0 {
			return nil, fmt.Errorf("a step was scheduled with no input")
		}

		// The resolved task is the first argument of every step activity, whichever
		// of the three was scheduled.
		var task v1.Task
		if err := converter.GetDefaultDataConverter().FromPayload(payloads[0], &task); err != nil {
			return nil, fmt.Errorf("reading what a step was scheduled with: %w", err)
		}

		messages = append(messages, task.GetInputs()["message"].GetLiteral().GetStringValue())
	}

	return messages, nil
}

// historyOf collects a run's history.
//
// It reports an error rather than asserting on one, because its callers run inside
// require.Eventually and testify evaluates that condition on its own goroutine — a
// failed assertion there is runtime.Goexit off the test goroutine, so the condition
// never returns and Eventually reports its own message a timeout later. The same
// trap listRunIDs documents, in the same package.
func historyOf(ctx context.Context, temporal client.Client, workflowID string) ([]*historypb.HistoryEvent, error) {
	iterator := temporal.GetWorkflowHistory(
		ctx, workflowID, "", false, enumspb.HISTORY_EVENT_FILTER_TYPE_ALL_EVENT)

	var events []*historypb.HistoryEvent
	for iterator.HasNext() {
		event, err := iterator.Next()
		if err != nil {
			return nil, err
		}
		events = append(events, event)
	}

	return events, nil
}
