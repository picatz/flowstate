package main

import (
	"bufio"
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"strings"
	"sync"
	"syscall"
	"testing"
	"time"

	"connectrpc.com/connect"
	"github.com/stretchr/testify/require"
	"go.temporal.io/api/workflowservice/v1"
	"go.temporal.io/sdk/client"
	"google.golang.org/protobuf/types/known/durationpb"

	v1 "github.com/picatz/flowstate/pkg/flowstate/v1"
)

// registerTestNamespace registers a Temporal namespace directly against the
// package's shared dev server and returns the name it was registered under.
//
// Deliberately not [newTemporalNamespace]: that helper returns a bound client
// with no way to read the namespace name back out, and both tests in this file
// need the name itself to hand to the `flow worker` subprocess on its command
// line. namespaceNameFor is not idempotent — it increments a shared ordinal on
// every call — so it is called exactly once here and the result reused by the
// caller, rather than asked for again later.
func registerTestNamespace(t *testing.T) string {
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

	return namespace
}

// startedFlowWorker is a `flow worker` subprocess plus the running transcript of
// its stdout and stderr, collected concurrently so a test can assert on log
// lines without racing the process that produces them.
type startedFlowWorker struct {
	cmd      *exec.Cmd
	snapshot func() string
}

// startFlowWorker builds the flow binary, starts `flow worker` with the given
// extra arguments against the package's shared dev server, and waits for it to
// log that it has started polling before returning — every test in this file
// needs that ordering guarantee so a signal sent immediately after cannot race
// process start-up.
func startFlowWorker(t *testing.T, namespace string, env []string, extraArgs ...string) *startedFlowWorker {
	t.Helper()

	bin := buildFlowBinary(t)

	args := append([]string{"worker",
		"--allow-unversioned-interpreter",
		"--address", devServer.FrontendHostPort(),
		"--namespace", namespace,
	}, extraArgs...)

	cmd := exec.Command(bin, args...)
	if len(env) > 0 {
		cmd.Env = append(os.Environ(), env...)
	}

	stdout, err := cmd.StdoutPipe()
	require.NoError(t, err)
	stderr, err := cmd.StderrPipe()
	require.NoError(t, err)

	var mu sync.Mutex
	var output strings.Builder
	collect := func(r io.Reader) {
		scanner := bufio.NewScanner(r)
		for scanner.Scan() {
			mu.Lock()
			output.WriteString(scanner.Text())
			output.WriteString("\n")
			mu.Unlock()
		}
	}
	snapshot := func() string {
		mu.Lock()
		defer mu.Unlock()
		return output.String()
	}

	require.NoError(t, cmd.Start())
	go collect(stdout)
	go collect(stderr)
	t.Cleanup(func() { _ = cmd.Process.Kill() })

	// Wait for the worker to actually be polling before returning, so a signal
	// sent right after this cannot race process start-up and accidentally prove
	// that a process killed before it finished coming up "shut down gracefully."
	require.Eventually(t, func() bool {
		return strings.Contains(snapshot(), "starting worker")
	}, 30*time.Second, 50*time.Millisecond,
		"flow worker never logged that it started:\n%s", snapshot())

	return &startedFlowWorker{cmd: cmd, snapshot: snapshot}
}

// TestWorkerGracefulShutdownOnSIGTERM is the end-to-end proof for #751: the real
// plumbing, not workerStopTimeout's unit-level cousin above it.
//
// Everything else in this package that exercises the worker flags reads them back
// from a *cobra.Command — proof that the value is computed correctly, and nothing
// about whether the process that receives it actually reacts to a signal. The
// defect this issue reports lived one layer below that: main.go's
// signal.NotifyContext registered os.Kill (SIGKILL, which no process can ever
// catch — a no-op) and never registered SIGTERM at all, so no in-process test
// touching cmd.Context() directly could have found it; that context was never
// wired to the signal in the first place. Only an actual SIGTERM delivered to an
// actual `flow worker` process proves the registration is real, which is what this
// test does: build the binary, start it against the package's shared Temporal dev
// server, send SIGTERM once it is confirmed to be polling, and require that it
// exits cleanly — logging that it is shutting down and that it stopped — well
// inside its --worker-stop-timeout, rather than either ignoring the signal or
// being silently killed by the test's own deadline.
//
// Skipped under -short via newTemporalNamespace, like every other dev-server test
// in this package; buildFlowBinary skips there too, so this is a normal CI-only
// test rather than a special case.
func TestWorkerGracefulShutdownOnSIGTERM(t *testing.T) {
	namespace := registerTestNamespace(t)

	worker := startFlowWorker(t, namespace, nil,
		"--task-queue", "shutdown-test-"+namespace,
		// Comfortably above anything this test asks it to drain (nothing is
		// in flight) and comfortably below the test's own kill deadline, so a
		// timeout firing here — rather than a clean Stop() — is what a
		// regression to "ignores the signal" or "drains for 0s" would look
		// like.
		"--worker-stop-timeout", "10s",
	)

	require.NoError(t, worker.cmd.Process.Signal(syscall.SIGTERM),
		"sending SIGTERM to the worker process")

	waited := make(chan error, 1)
	go func() { waited <- worker.cmd.Wait() }()

	select {
	case err := <-waited:
		require.NoError(t, err,
			"flow worker exited non-zero after SIGTERM:\n%s", worker.snapshot())
	case <-time.After(20 * time.Second):
		_ = worker.cmd.Process.Kill()
		t.Fatalf("flow worker did not exit within 20s of SIGTERM — SIGTERM is "+
			"not being handled (this is the exact regression #751 reports):\n%s",
			worker.snapshot())
	}

	got := worker.snapshot()
	require.Contains(t, got, "shutting down worker",
		"no graceful-shutdown log line after SIGTERM:\n%s", got)
	require.Contains(t, got, "worker stopped",
		"worker did not report a clean stop after SIGTERM:\n%s", got)
}

// TestWorkerSecondSignalDuringADrainForcesExit is the fix for a review finding
// on #757's PR: signal.NotifyContext keeps intercepting SIGINT/SIGTERM until its
// stop function is called, and that used to happen only via a deferred call in
// main that runs after execute — and therefore after w.Stop()'s drain — returns.
// A worker draining a genuinely slow activity for the length of
// --worker-stop-timeout (up to 2 minutes by default) absorbed every signal an
// operator sent during that wait, leaving SIGKILL as the only way to force it
// down: exactly the hard-kill outcome this whole feature exists to give an
// alternative to.
//
// Proving it needs an activity that is actually executing Go code — blocked on
// something this test controls — when the signal arrives, which a durable
// `wait:` step cannot provide: waiting.go's own doc explains that a wait is
// workflow-side state with no worker, goroutine, or connection behind it, so a
// worker has nothing in flight to drain while one is outstanding. An `http:`
// step's activity does real, in-process, cancellable work, so this test runs one
// against a test server that blocks on a channel until released — genuine
// in-flight work with a lifetime this test, not a timer, controls.
func TestWorkerSecondSignalDuringADrainForcesExit(t *testing.T) {
	namespace := registerTestNamespace(t)

	temporal, err := client.Dial(client.Options{
		HostPort:  devServer.FrontendHostPort(),
		Namespace: namespace,
	})
	require.NoError(t, err)
	t.Cleanup(temporal.Close)
	require.Eventually(t, func() bool {
		_, err := temporal.ListWorkflow(t.Context(),
			&workflowservice.ListWorkflowExecutionsRequest{PageSize: 1})
		return err == nil
	}, 30*time.Second, 20*time.Millisecond,
		"the namespace registered for this test never became usable")

	flowstate := mustNewFlowstateServer(t, temporal)

	// The slow endpoint the run's one step calls: it announces that a request has
	// begun (proving the worker's activity is genuinely executing, not merely
	// dispatched) and then blocks until release is closed, deliberately holding
	// the activity's goroutine open for as long as this test needs it to.
	requestStarted := make(chan struct{})
	release := make(chan struct{})
	var closeReleaseOnce sync.Once
	closeRelease := func() { closeReleaseOnce.Do(func() { close(release) }) }
	slow := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		select {
		case <-requestStarted:
		default:
			close(requestStarted)
		}
		<-release
		w.WriteHeader(http.StatusOK)
	}))
	// Registered before closeRelease below, so cleanup runs closeRelease first
	// (t.Cleanup unwinds last-registered-first) — Close blocks until every
	// outstanding request completes, which the handler above never does on its
	// own if the worker process is killed mid-request.
	t.Cleanup(slow.Close)
	t.Cleanup(closeRelease)

	worker := startFlowWorker(t, namespace,
		// The default egress policy refuses loopback; this is the one lever
		// documented for turning it back on, exactly as egress.go describes.
		[]string{v1.AllowLoopbackEgressEnv + "=true"},
		// Generous: long enough that this test's own bounds below are what
		// decide the outcome, not a coincidence of drain length.
		"--worker-stop-timeout", "30s",
	)

	started, err := flowstate.Run(t.Context(), connect.NewRequest(&v1.RunRequest{
		Workflow: &v1.Workflow{
			Name: "slow-call",
			Steps: []*v1.Node{{
				Id: "call",
				Kind: &v1.Node_Task{Task: &v1.Task{Name: "http", Inputs: map[string]*v1.Value{
					"url": v1.NewLiteral(slow.URL),
				}}},
			}},
		},
	}))
	require.NoError(t, err)
	workflowID := started.Msg.GetWorkflowId()
	t.Cleanup(func() {
		_, _ = flowstate.Terminate(context.Background(), connect.NewRequest(&v1.TerminateRequest{
			WorkflowId: workflowID,
		}))
	})

	select {
	case <-requestStarted:
	case <-time.After(30 * time.Second):
		t.Fatalf("the slow http step never started executing on the worker:\n%s", worker.snapshot())
	}

	// First signal: cancels the worker's shutdown context, which starts the
	// drain — Stop() now waits (up to --worker-stop-timeout) for the in-flight
	// http activity above, which is deliberately parked mid-request until
	// release is closed.
	require.NoError(t, worker.cmd.Process.Signal(syscall.SIGTERM))

	// Confirm the process is still alive partway through the wait — proving the
	// first signal started a graceful drain rather than tearing the process
	// down immediately, which is the behavior the second signal below is
	// contrasted against.
	time.Sleep(1500 * time.Millisecond)
	require.NoError(t, worker.cmd.Process.Signal(syscall.Signal(0)),
		"flow worker exited before the second signal — the drain did not actually "+
			"wait for the in-flight activity:\n%s", worker.snapshot())

	// Second signal: with the fix, the moment the first signal canceled the
	// shutdown context, main's escalation goroutine called the NotifyContext
	// stop function, which unregisters the signal handler — so this one reaches
	// the process's default disposition and terminates it, well before the
	// still-blocked activity or --worker-stop-timeout (30s) would otherwise let
	// Stop() return. Without the fix this signal is absorbed by the same
	// signal.NotifyContext the first one was, and the process keeps waiting on
	// the still-blocked activity for the rest of the 30s — which is exactly what
	// the bound below distinguishes.
	require.NoError(t, worker.cmd.Process.Signal(syscall.SIGTERM))

	waited := make(chan error, 1)
	go func() { waited <- worker.cmd.Wait() }()

	select {
	case <-waited:
		// A forced process exits with a non-zero status (SIGTERM's default
		// disposition), which is expected and fine here — the claim under test
		// is *when* it exits, not the exit code.
	case <-time.After(10 * time.Second):
		closeRelease()
		_ = worker.cmd.Process.Kill()
		t.Fatalf("flow worker did not exit within 10s of the second SIGTERM — the "+
			"second signal is being absorbed by the same handler as the first, "+
			"instead of reaching the process's default disposition:\n%s",
			worker.snapshot())
	}
}
