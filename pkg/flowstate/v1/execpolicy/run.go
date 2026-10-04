package execpolicy

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"os/exec"
	"strings"
	"sync"
	"sync/atomic"
	"time"
	"unicode/utf8"

	"github.com/picatz/flowstate/pkg/flowstate/v1/internal/procgroup"
)

const (
	// terminateGrace is how long a program that was asked to stop (SIGTERM to
	// its process group) has before the group is killed.
	terminateGrace = 5 * time.Second

	// waitDelay bounds how long Wait lingers on a child that outlives its
	// cancellation, or on a descendant that holds an output pipe open after the
	// child exited. It sits past [terminateGrace] so the group kill comes first.
	waitDelay = terminateGrace + 2*time.Second

	// captureGrace is how long output capture may continue after the program
	// has been reaped and its group killed. Everything the program wrote is
	// already in the pipes by then, so this only ever waits on a descendant that
	// left the group and kept a pipe open; past it the read ends are closed and
	// the result says capture was incomplete.
	captureGrace = time.Second
)

// Result is what an admitted program did.
type Result struct {
	// ExitCode is the exit status, or -1 when a signal ended the program.
	ExitCode int

	// Stdout and Stderr hold at most the policy's byte bound each, the first
	// bytes written, as valid UTF-8 (invalid sequences become U+FFFD).
	Stdout string
	Stderr string

	// StdoutTruncated and StderrTruncated report that output past the bound was
	// read and discarded.
	StdoutTruncated bool
	StderrTruncated bool

	// CaptureIncomplete reports that reading the output stopped before the
	// streams ended: the program finished, but a descendant outside its process
	// group still held an output pipe, so Stdout and Stderr may lack bytes it
	// wrote. Distinct from the truncation flags, which mean the byte bound cut
	// output that was read.
	CaptureIncomplete bool

	// Signal names the signal that ended the program; empty when it exited.
	Signal string

	// Duration is wall time from start to reap.
	Duration time.Duration

	// Outcome is [OutcomeRan] for a result a successful Run returns.
	Outcome Outcome
}

// Run starts the program and waits for it, under the policy's bounds.
//
// A program that exits, with any status, returns a [Result] and a nil error:
// the exit code is data. A [*RunError] reports the other ways an admitted
// invocation can end — it could not be started, the time bound ended it, or ctx
// was cancelled — and carries whatever the program had printed by then.
//
// # How the program is stopped
//
// The program runs in a process group of its own. When the policy's timeout
// passes or ctx ends, the group receives SIGTERM, and SIGKILL once
// [terminateGrace] has elapsed. After the program is reaped the group is killed
// regardless, so a descendant it left behind in the group does not outlive the
// step; a descendant that deliberately left the group (setsid) is not reached,
// which is one more reason this is not a sandbox.
//
// # What is executed
//
// On Linux the verified file is opened once, without blocking (a FIFO swapped
// in for it is refused, not waited on), and executed through that descriptor
// (/proc/self/fd), so replacing the path between the check and the exec cannot
// change what runs. A pinned SHA-256 is verified against that same descriptor.
// Only an image the kernel certainly executes itself is run that way: a script,
// a foreign-architecture image, or anything a binfmt_misc registration would
// claim is handed to an interpreter that must reopen a path after the
// descriptor is gone, so those, and a system without /proc, fall back to
// executing the resolved path, leaving a small window in which a file replaced
// by someone with write access to its directory would run. Other platforms
// always execute the path. The pin is still verified immediately before in
// every case.
//
// # Output
//
// Output is read by this function, to the policy's byte bound per stream. If
// the program has finished but a descendant that left its process group still
// holds an output pipe, reading is cut off after a short grace and the result
// has CaptureIncomplete set; the outcome is still [OutcomeRan].
//
// # Platforms
//
// Where the platform cannot stop a program together with its descendants
// (anywhere but Unix) [Policy.Check] denies with [ReasonPlatform] rather than
// run with the guarantee silently weakened.
//
// Standard input is /dev/null.
func (c *Command) Run(ctx context.Context) (Result, error) {
	// The standard streams are made here, not by os/exec, so their descriptors
	// are known before the executable's is placed (see [Command.openExecutable])
	// and so output is read by this function, which can tell when capture ended
	// early.
	devnull, err := os.Open(os.DevNull)
	if err != nil {
		return Result{}, &RunError{Outcome: OutcomeDidNotStart, Err: startFailure(c.argv[0], err)}
	}
	defer devnull.Close()

	stdoutR, stdoutW, err := os.Pipe()
	if err != nil {
		return Result{}, &RunError{Outcome: OutcomeDidNotStart, Err: startFailure(c.argv[0], err)}
	}
	defer stdoutR.Close()
	defer stdoutW.Close()

	stderrR, stderrW, err := os.Pipe()
	if err != nil {
		return Result{}, &RunError{Outcome: OutcomeDidNotStart, Err: startFailure(c.argv[0], err)}
	}
	defer stderrR.Close()
	defer stderrW.Close()

	execPath, release, err := c.openExecutable([]*os.File{devnull, stdoutW, stderrW})
	if err != nil {
		return Result{}, err
	}
	defer release()

	runCtx, stop := context.WithTimeout(ctx, c.policy.timeout)
	defer stop()

	stdout := newBoundedBuffer(c.policy.maxOutput)
	stderr := newBoundedBuffer(c.policy.maxOutput)

	cmd := exec.CommandContext(runCtx, execPath, c.argv[1:]...)
	cmd.Path = execPath
	cmd.Args = slicesClone(c.argv)
	// The directory is the one Check authorized, which is the one the rules
	// saw. Re-resolve it immediately before the start and refuse any other
	// answer: a component swapped for a symlink since Check could otherwise
	// move the child to a different directory the rules never judged, even one
	// still under a root. This narrows the window rather than closing it; only
	// a handle held from the check to the exec would, which workspaces and
	// runners will provide.
	dir, err := c.policy.checkDir(c.dir)
	if err != nil {
		return Result{}, &RunError{Outcome: OutcomeDidNotStart, Err: err}
	}
	if dir != c.dir {
		return Result{}, &DeniedError{
			Reason: ReasonDir,
			Detail: fmt.Sprintf("the working directory now resolves to %q, not the %q the policy authorized", dir, c.dir),
		}
	}
	cmd.Dir = dir
	cmd.Env = append([]string{}, c.env...)
	cmd.Stdin = devnull
	cmd.Stdout = stdoutW
	cmd.Stderr = stderrW
	cmd.WaitDelay = waitDelay
	procgroup.Isolate(cmd)

	var (
		cancelled atomic.Bool
		killTimer *time.Timer
		timerMu   sync.Mutex
	)
	cmd.Cancel = func() error {
		cancelled.Store(true)
		err := procgroup.Terminate(cmd.Process, false)
		timerMu.Lock()
		killTimer = time.AfterFunc(terminateGrace, func() { _ = procgroup.Terminate(cmd.Process, true) })
		timerMu.Unlock()
		return err
	}

	var readers sync.WaitGroup
	for _, stream := range []struct {
		from *os.File
		into *boundedBuffer
	}{{stdoutR, stdout}, {stderrR, stderr}} {
		readers.Go(func() { _, _ = io.Copy(stream.into, stream.from) })
	}

	started := time.Now()
	if err := cmd.Start(); err != nil {
		stdoutW.Close()
		stderrW.Close()
		readers.Wait()
		// os/exec reports an expired or cancelled context as the context's
		// own error. That is the step's deadline or cancellation, not a
		// program that could not start, and must be classified as such: a
		// did-not-start failure is retryable and this one is not.
		if ctxErr := runCtx.Err(); ctxErr != nil && errors.Is(err, ctxErr) {
			return c.endedEarly(ctx, runCtx, Result{})
		}
		return Result{}, &RunError{Outcome: OutcomeDidNotStart, Err: startFailure(c.argv[0], err)}
	}
	// The child holds its own copies; ours would keep the pipes from ever
	// reaching end of file.
	stdoutW.Close()
	stderrW.Close()

	waitErr := cmd.Wait()
	elapsed := time.Since(started)

	timerMu.Lock()
	if killTimer != nil {
		killTimer.Stop()
	}
	timerMu.Unlock()
	// Whatever the program left in its group dies with the step. The group id
	// stays allocated while any member lives, so this reaches only this step's
	// leftovers; with no survivors it finds no group.
	_ = procgroup.Terminate(cmd.Process, true)

	// Drain what is in the pipes. With the group dead the streams end on their
	// own; a descendant that left the group may still hold them, and waiting on
	// it would hold this step too, so capture is cut off after a grace and said
	// to be incomplete.
	drained := make(chan struct{})
	go func() { readers.Wait(); close(drained) }()
	incomplete := false
	select {
	case <-drained:
	case <-time.After(captureGrace):
		incomplete = true
		stdoutR.Close()
		stderrR.Close()
		<-drained
	}

	result := Result{Duration: elapsed, CaptureIncomplete: incomplete}
	result.Stdout, result.StdoutTruncated = stdout.text()
	result.Stderr, result.StderrTruncated = stderr.text()

	if cancelled.Load() {
		return c.endedEarly(ctx, runCtx, result)
	}

	exitErr, exited := errors.AsType[*exec.ExitError](waitErr)
	switch {
	case waitErr == nil, errors.Is(waitErr, exec.ErrWaitDelay):
		result.ExitCode = cmd.ProcessState.ExitCode()
	case exited:
		result.ExitCode = exitErr.ExitCode()
		result.Signal = signalName(exitErr)
	case cmd.ProcessState != nil:
		// The program ran and was reaped; the error is the plumbing around it,
		// so what it did is still its result and is not retried as though it
		// never started.
		result.ExitCode = cmd.ProcessState.ExitCode()
	default:
		return result, &RunError{Outcome: OutcomeDidNotStart, Err: waitErr, Result: result}
	}
	result.Outcome = OutcomeRan

	return result, nil
}

// endedEarly classifies a program the context ended. The caller's context is
// consulted first so a step's own deadline or cancellation is reported as such;
// only when it is still live was it the policy's timeout.
func (c *Command) endedEarly(ctx, runCtx context.Context, result Result) (Result, error) {
	switch {
	case errors.Is(ctx.Err(), context.DeadlineExceeded):
		result.Outcome = OutcomeTimedOut
		return result, &RunError{Outcome: OutcomeTimedOut, Err: ctx.Err(), Result: result}
	case ctx.Err() != nil:
		result.Outcome = OutcomeCancelled
		return result, &RunError{Outcome: OutcomeCancelled, Err: ctx.Err(), Result: result}
	default:
		result.Outcome = OutcomeTimedOut
		return result, &RunError{Outcome: OutcomeTimedOut, PolicyTimeout: true, Err: runCtx.Err(), Result: result}
	}
}

// startFailure renders a start error without the descriptor path or the
// resolved path in it: the workflow named the program, so the message does too.
func startFailure(name string, err error) error {
	if pe, ok := errors.AsType[*os.PathError](err); ok {
		err = pe.Err
	}
	return fmt.Errorf("starting %q: %w", name, err)
}

func slicesClone(s []string) []string { return append([]string(nil), s...) }

// boundedBuffer keeps the first limit bytes written to it and discards the
// rest, always reporting the whole write as consumed so the program is never
// blocked on a full pipe.
type boundedBuffer struct {
	mu        sync.Mutex
	limit     int64
	buf       bytes.Buffer
	truncated bool
}

func newBoundedBuffer(limit int64) *boundedBuffer { return &boundedBuffer{limit: limit} }

func (b *boundedBuffer) Write(p []byte) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()

	room := b.limit - int64(b.buf.Len())
	switch {
	case room <= 0:
		b.truncated = true
	case int64(len(p)) > room:
		b.buf.Write(p[:room])
		b.truncated = true
	default:
		b.buf.Write(p)
	}

	return len(p), nil
}

// text returns the kept bytes as valid UTF-8 no longer than the limit.
//
// Replacing invalid sequences can lengthen the text (one bad byte becomes the
// three-byte U+FFFD), so the bound is applied again afterwards, on a rune
// boundary, and the stream reports truncated if that cut anything.
func (b *boundedBuffer) text() (string, bool) {
	b.mu.Lock()
	defer b.mu.Unlock()

	s := strings.ToValidUTF8(b.buf.String(), "�")
	truncated := b.truncated
	if int64(len(s)) > b.limit {
		cut := int(b.limit)
		for cut > 0 && !utf8.RuneStart(s[cut]) {
			cut--
		}
		s = s[:cut]
		truncated = true
	}

	return s, truncated
}
