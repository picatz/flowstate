package main

import (
	"os"
	"path/filepath"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// This file is #2044's review finding #1, isolated to Linux the way
// readbounded_linux_test.go's own [syscall.Mkfifo] test already is: a named
// FIFO and /dev/stdin are both POSIX shapes a Windows build cannot exercise
// the same way, and the fix under test — reading the numeric-overflow
// refusal's own quoted text off a typed error instead of reopening
// --input-file — has nothing OS-specific about it; only these two
// reproductions do.

// TestInputFileFIFONumericOverflowDoesNotHang is the regression test for the
// review finding on an earlier version of this fix: redacting a
// numeric-overflow refusal by reopening --input-file a second time blocked
// forever on a named FIFO whose writer had already finished and gone away —
// exactly the shape process substitution (`--input-file <(vault read ...)`)
// produces, since the substituted process exits once it has written its
// output. The fix removed the second open entirely, so this has only the one
// read [runInputs] already did before #2044 to hang on, and this pins that it
// does not.
//
// Driven with the actual CLI invocation on a background goroutine rather than
// in this one, because a regression here is a real, unbounded block — os.Open
// on a FIFO with no reader waiting does not return on its own — and the test
// has to survive that rather than hang the whole run. Nothing that can call
// t.FailNow crosses the goroutine boundary: the invocation returns a plain
// [flowResult], and every assertion happens back in this goroutine once it is
// in hand, which is what testing.T requires of a helper spawned this way.
func TestInputFileFIFONumericOverflowDoesNotHang(t *testing.T) {
	fifo := filepath.Join(t.TempDir(), "inputs.fifo")
	require.NoError(t, syscall.Mkfifo(fifo, 0o600))

	workflowPath := writeWorkflowFile(t, overflowInputFileWorkflow)

	// The writer process substitution stands in for: it opens the FIFO,
	// writes the document, and closes its end — exactly what `<(vault read
	// ...)` does once the substituted command finishes.
	go func() {
		f, err := os.OpenFile(fifo, os.O_WRONLY, 0)
		if err != nil {
			return
		}
		defer f.Close()
		_, _ = f.WriteString(`{"pin": ` + overflowNumber + `}`)
	}()

	done := make(chan flowResult, 1)
	go func() {
		done <- flowRun{Args: []string{
			"run", "local", workflowPath, "--output", "json", "--input-file", fifo,
		}}.run(t)
	}()

	select {
	case res := <-done:
		require.Error(t, res.Err, "an out-of-range number is refused")
		assert.NotContains(t, res.Stdout, overflowNumber)
		assert.NotContains(t, res.Stderr, overflowNumber)
	case <-time.After(5 * time.Second):
		t.Fatal("redacting a --input-file refusal blocked reading a FIFO whose writer had already finished")
	}
}

// TestInputFileDevStdinNumericOverflowDoesNotLeak is the second shape the
// review found: --input-file /dev/stdin (or a pipe feeding it) is a stream a
// second os.Open cannot rewind — the first read already consumed it, so a
// second one gets EOF rather than the document again, and the old
// reopen-based fix printed the raw number because it read nothing back to
// redact against. This needs a real subprocess: /dev/stdin names *this
// process's* standard input, which only means something once there is a real
// process with one, not the in-process harness's captured buffers.
func TestInputFileDevStdinNumericOverflowDoesNotLeak(t *testing.T) {
	bin := buildFlowBinary(t)
	workflowPath := writeWorkflowFile(t, overflowInputFileWorkflow)

	cmd := flowBinaryCommand(bin, "run", "local", workflowPath, "--output", "json", "--input-file", "/dev/stdin")
	cmd.Stdin = strings.NewReader(`{"pin": ` + overflowNumber + `}`)

	res := runFlowBinaryWith(t, cmd)
	require.Error(t, res.Err, "an out-of-range number is refused")
	assert.NotContains(t, res.Output(), overflowNumber)
}
