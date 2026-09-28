package main

import (
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
