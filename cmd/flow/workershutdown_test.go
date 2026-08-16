package main

import (
	"bufio"
	"io"
	"os/exec"
	"strings"
	"sync"
	"syscall"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.temporal.io/api/workflowservice/v1"
	"google.golang.org/protobuf/types/known/durationpb"
)

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
	// Registered directly, rather than through newTemporalNamespace: that helper
	// returns a *client.Client* bound to the namespace it registers, with no way
	// to read the namespace name back out, and namespaceNameFor is not
	// idempotent — it increments a shared ordinal on every call — so a second
	// call here would register a name the worker below is never told to use.
	// The subprocess dials the dev server itself, so no client is needed on
	// this side at all.
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

	bin := buildFlowBinary(t)

	cmd := exec.Command(bin, "worker",
		"--allow-unversioned-interpreter",
		"--address", devServer.FrontendHostPort(),
		"--namespace", namespace,
		"--task-queue", "shutdown-test-"+namespace,
		// Comfortably above anything this test asks it to drain (nothing is
		// in flight) and comfortably below the test's own kill deadline, so a
		// timeout firing here — rather than a clean Stop() — is what a
		// regression to "ignores the signal" or "drains for 0s" would look
		// like.
		"--worker-stop-timeout", "10s",
	)

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

	// Wait for the worker to actually be polling before sending the signal, so
	// this cannot race process start-up and accidentally prove that a process
	// killed before it finished coming up "shut down gracefully."
	require.Eventually(t, func() bool {
		return strings.Contains(snapshot(), "starting worker")
	}, 30*time.Second, 50*time.Millisecond,
		"flow worker never logged that it started:\n%s", snapshot())

	require.NoError(t, cmd.Process.Signal(syscall.SIGTERM),
		"sending SIGTERM to the worker process")

	waited := make(chan error, 1)
	go func() { waited <- cmd.Wait() }()

	select {
	case err := <-waited:
		require.NoError(t, err,
			"flow worker exited non-zero after SIGTERM:\n%s", snapshot())
	case <-time.After(20 * time.Second):
		_ = cmd.Process.Kill()
		t.Fatalf("flow worker did not exit within 20s of SIGTERM — SIGTERM is "+
			"not being handled (this is the exact regression #751 reports):\n%s",
			snapshot())
	}

	got := snapshot()
	require.Contains(t, got, "shutting down worker",
		"no graceful-shutdown log line after SIGTERM:\n%s", got)
	require.Contains(t, got, "worker stopped",
		"worker did not report a clean stop after SIGTERM:\n%s", got)
}
