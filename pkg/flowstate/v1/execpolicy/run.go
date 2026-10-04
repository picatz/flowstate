package execpolicy

import (
	"bytes"
	"context"
	"errors"
	"fmt"
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
// On Linux the verified file is opened once and executed through that
// descriptor (/proc/self/fd), so replacing the path between the check and the
// exec cannot change what runs. A pinned SHA-256 is verified against that same
// descriptor. Two cases fall back to executing the resolved path, leaving a
// small window in which a file replaced by someone with write access to its
// directory would run: a script (its interpreter cannot reopen a descriptor
// that closes on exec) and a system without /proc. Other platforms always
// execute the path. The pin is still verified immediately before in every case.
//
// Standard input is /dev/null.
func (c *Command) Run(ctx context.Context) (Result, error) {
	execPath, release, err := c.openExecutable()
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
	// Re-resolve and re-check immediately before the start: this narrows the
	// window in which a symlink swapped after Check could point the child
	// outside the roots. It does not close it; only a handle held from the
	// check to the exec would, which workspaces and runners will provide.
	dir, err := c.policy.checkDir(c.dir)
	if err != nil {
		return Result{}, &RunError{Outcome: OutcomeDidNotStart, Err: err}
	}
	cmd.Dir = dir
	cmd.Env = append([]string{}, c.env...)
	cmd.Stdin = nil
	cmd.Stdout = stdout
	cmd.Stderr = stderr
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

	started := time.Now()
	if err := cmd.Start(); err != nil {
		return Result{}, &RunError{Outcome: OutcomeDidNotStart, Err: startFailure(c.argv[0], err)}
	}
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

	result := Result{Duration: elapsed}
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
		// The program ran and was reaped; the error is the plumbing around it
		// (a pipe copy), so what it did is still its result and is not retried
		// as though it never started.
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
