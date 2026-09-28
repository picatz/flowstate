package main

import (
	"bufio"
	"fmt"
	"os/exec"
	"syscall"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// TestFlowDAPSurvivesAWriteToAnEditorThatHasGone is the adapter outliving its
// editor. An editor that dies takes the read end of the adapter's standard
// output with it, and a write to fd 1 after that is a SIGPIPE, which by
// default kills a Go process: under a run the session detached from, with its
// plugins never closed. The request here is answered into a pipe nobody reads,
// and the adapter must still exit on its own once its input ends.
func TestFlowDAPSurvivesAWriteToAnEditorThatHasGone(t *testing.T) {
	bin := buildFlowBinary(t)

	cmd := flowBinaryCommand(bin, "dap")
	stdin, err := cmd.StdinPipe()
	require.NoError(t, err)
	stdout, err := cmd.StdoutPipe()
	require.NoError(t, err)
	require.NoError(t, cmd.Start())

	// The editor goes away first on the reading side, then asks something.
	require.NoError(t, stdout.Close())
	body := `{"seq":1,"type":"request","command":"threads"}`
	_, err = fmt.Fprintf(stdin, "Content-Length: %d\r\n\r\n%s", len(body), body)
	require.NoError(t, err)
	require.NoError(t, stdin.Close())

	exited := make(chan error, 1)
	go func() { exited <- cmd.Wait() }()
	select {
	case err = <-exited:
	case <-time.After(30 * time.Second):
		_ = cmd.Process.Kill()
		t.Fatal("flow dap did not exit once its input ended")
	}

	var exit *exec.ExitError
	if err != nil {
		require.ErrorAs(t, err, &exit)
		status, _ := exit.Sys().(syscall.WaitStatus)
		require.False(t, status.Signaled() && status.Signal() == syscall.SIGPIPE,
			"flow dap was killed by SIGPIPE writing to an editor that had gone")
	}
	require.NoError(t, err)
}

// TestFlowDAPExitsCleanlyOnAnInterrupt is an editor stopping the adapter with
// SIGINT while it still holds the adapter's stdin open. A read blocked on that
// pipe is not interrupted by closing it, so the adapter must not wait on the
// read to detach and exit; and an interrupt is a clean stop, not a failure.
func TestFlowDAPExitsCleanlyOnAnInterrupt(t *testing.T) {
	bin := buildFlowBinary(t)

	cmd := flowBinaryCommand(bin, "dap")
	stdin, err := cmd.StdinPipe()
	require.NoError(t, err)
	t.Cleanup(func() { _ = stdin.Close() })
	stdout, err := cmd.StdoutPipe()
	require.NoError(t, err)
	require.NoError(t, cmd.Start())

	// Serving: an initialize is answered, so the adapter is past start-up and
	// blocked reading the next request.
	body := `{"seq":1,"type":"request","command":"initialize","arguments":{"adapterID":"flowstate"}}`
	_, err = fmt.Fprintf(stdin, "Content-Length: %d\r\n\r\n%s", len(body), body)
	require.NoError(t, err)
	header, err := bufio.NewReader(stdout).ReadString('\n')
	require.NoError(t, err)
	require.Contains(t, header, "Content-Length")

	require.NoError(t, cmd.Process.Signal(syscall.SIGINT))

	exited := make(chan error, 1)
	go func() { exited <- cmd.Wait() }()
	select {
	case err = <-exited:
	case <-time.After(30 * time.Second):
		_ = cmd.Process.Kill()
		t.Fatal("flow dap waited on its client's stdin after an interrupt")
	}
	require.NoError(t, err, "an interrupt made flow dap exit with a failure")
}
