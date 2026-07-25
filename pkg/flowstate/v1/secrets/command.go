package secrets

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"os/exec"
	"strings"
	"time"
)

// Defaults for providers that read a secret from a command's output.
const (
	// DefaultCommandTimeout bounds one invocation. A local secret tool answers in
	// milliseconds; a tool that has not answered in this long is prompting for a
	// password, waiting on a network, or wedged, and a workflow step should not be
	// held open for it.
	DefaultCommandTimeout = 10 * time.Second

	// DefaultCommandMaxBytes bounds how much output is read. A credential is small;
	// the limit is what stops a wrong or hostile command from feeding a worker an
	// unbounded stream.
	DefaultCommandMaxBytes int64 = 1 << 20 // 1 MiB
)

// commandRunner runs one subprocess and returns its standard output.
//
// It is an interface so a provider's logic can be tested without the tool it
// wraps: a test that needs a keychain to run is a test that does not run in CI.
// The real implementation is [execRunner].
type commandRunner interface {
	// run executes name with args and returns its standard output. Errors are
	// already classified against this package's sentinels.
	run(ctx context.Context, name string, args ...string) ([]byte, error)
}

// execRunner runs subprocesses with os/exec, under the constraints a secret tool
// invocation needs.
//
// There is no shell anywhere in this path. The executable and its arguments are
// passed separately, so a secret name containing a space, a quote, a semicolon, or
// a backtick is one argument rather than something a shell would re-interpret.
// That is the property that makes it safe to pass a workflow-supplied name to a
// local tool at all.
type execRunner struct {
	// timeout bounds one invocation, on top of any deadline the caller already has.
	timeout time.Duration

	// maxBytes bounds how much of the output is read.
	maxBytes int64

	// env is the child's environment. Nil inherits the parent's, which is the right
	// default here — a keychain or password-manager CLI needs HOME, the user's
	// session, and its own configuration to work at all.
	env []string
}

// run implements [commandRunner].
func (r execRunner) run(ctx context.Context, name string, args ...string) ([]byte, error) {
	// Resolve the executable explicitly so a missing tool is reported as a missing
	// tool, and so a binary sitting in the working directory is never picked up:
	// LookPath refuses that since Go 1.19, and this surfaces the refusal clearly.
	path, err := exec.LookPath(name)
	if err != nil {
		if errors.Is(err, exec.ErrDot) {
			return nil, fmt.Errorf("%w: refusing to run %q from the current directory", ErrUnavailable, name)
		}
		return nil, fmt.Errorf("%w: %q is not installed or not on PATH", ErrUnavailable, name)
	}

	if r.timeout > 0 {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, r.timeout)
		defer cancel()
	}

	cmd := exec.CommandContext(ctx, path, args...)
	cmd.Env = r.env

	// Nothing is written to the child, and leaving stdin attached to the worker's
	// would let a tool that decides to prompt block on a terminal that is not there.
	cmd.Stdin = nil

	// Bound the wait after cancellation, so a tool that ignores its kill signal
	// cannot hold the step open indefinitely.
	cmd.WaitDelay = 2 * time.Second

	var stdout, stderr bytes.Buffer
	cmd.Stdout = &limitedWriter{w: &stdout, limit: r.limit()}
	cmd.Stderr = &limitedWriter{w: &stderr, limit: 8 << 10}

	runErr := cmd.Run()

	// A deadline is reported as unavailable rather than as a failure of the tool:
	// it is the one condition here that another attempt might get past.
	if ctxErr := ctx.Err(); ctxErr != nil {
		if errors.Is(ctxErr, context.DeadlineExceeded) {
			return nil, fmt.Errorf("%w: %s did not answer within %s", ErrUnavailable, name, r.timeout)
		}
		return nil, ctxErr
	}

	if errors.Is(runErr, errOutputTooLarge) {
		return nil, fmt.Errorf("%w: %s produced more than %d bytes", ErrTooLarge, name, r.limit())
	}

	if runErr != nil {
		var exit *exec.ExitError
		if errors.As(runErr, &exit) {
			// The tool ran and refused. Its stderr says why, and a secret tool puts
			// the reason there rather than the secret, so it is safe to include —
			// bounded, and with any trailing newline trimmed.
			return nil, fmt.Errorf("%w: %s exited %d: %s",
				ErrNotFound, name, exit.ExitCode(), summarize(stderr.String()))
		}

		return nil, fmt.Errorf("%w: running %s: %v", ErrUnavailable, name, runErr)
	}

	return stdout.Bytes(), nil
}

// limit returns the output cap, substituting the default.
func (r execRunner) limit() int64 {
	if r.maxBytes > 0 {
		return r.maxBytes
	}

	return DefaultCommandMaxBytes
}

// errOutputTooLarge signals that a child wrote past its byte budget.
var errOutputTooLarge = errors.New("output exceeded limit")

// limitedWriter bounds what a child process can make the worker buffer.
//
// It stops the copy rather than truncating quietly, because a truncated credential
// is worse than no credential: it would be presented, refused, and diagnosed as an
// authentication problem rather than as a size problem.
type limitedWriter struct {
	w       io.Writer
	limit   int64
	written int64
}

// Write implements [io.Writer].
func (w *limitedWriter) Write(p []byte) (int, error) {
	if w.written+int64(len(p)) > w.limit {
		return 0, errOutputTooLarge
	}

	n, err := w.w.Write(p)
	w.written += int64(n)

	return n, err
}

// summarize renders a tool's diagnostic output for an error message: one line,
// bounded, and stripped of anything that could forge a log line.
func summarize(s string) string {
	s = strings.TrimSpace(s)
	if s == "" {
		return "no output"
	}

	s = strings.Map(func(r rune) rune {
		if isControl(r) {
			return ' '
		}
		return r
	}, s)

	if len(s) > 200 {
		s = s[:200] + "…"
	}

	return s
}

// hasCommand reports whether an executable is available, for a constructor that
// should fail on a machine without the tool rather than on the first workflow that
// needs a secret.
func hasCommand(name string) error {
	if _, err := exec.LookPath(name); err != nil {
		return fmt.Errorf("%q is not installed or not on PATH: %w", name, err)
	}

	return nil
}
