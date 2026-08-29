package secrets

import (
	"context"
	"fmt"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// fakeRunner stands in for a subprocess, so a provider's logic can be tested on a
// machine without the tool it wraps.
type fakeRunner struct {
	out []byte
	err error

	// calls records the argv of every invocation, so a test can assert what would
	// have been executed without executing it.
	calls [][]string
}

func (r *fakeRunner) run(_ context.Context, name string, args ...string) ([]byte, error) {
	r.calls = append(r.calls, append([]string{name}, args...))

	return r.out, r.err
}

// argv returns the argv of the only invocation.
func (r *fakeRunner) argv(t *testing.T) []string {
	t.Helper()

	require.Len(t, r.calls, 1, "expected exactly one invocation")

	return r.calls[0]
}

// TestHelperProcess is the subprocess the real-runner tests execute.
//
// Running the test binary as its own helper keeps these tests hermetic: they
// exercise the actual os/exec path without depending on any tool being installed
// on the machine, which is what would make them fail in CI.
//
//vacuity:ignore unasserted this is a subprocess entry point rather than a test — the assertions belong to whichever test launched it, and are made there against what this prints and exits with
func TestHelperProcess(t *testing.T) {
	if os.Getenv("FLOWSTATE_HELPER_MODE") == "" {
		t.Skip("not running as a helper process")
	}

	switch mode := os.Getenv("FLOWSTATE_HELPER_MODE"); mode {
	case "value":
		fmt.Print("the-secret-value\n")
	case "empty":
	case "fail":
		fmt.Fprintln(os.Stderr, "the item could not be found")
		os.Exit(44)
	case "noisy-fail":
		fmt.Fprint(os.Stderr, "line one\nline two\x00with a null\n")
		os.Exit(1)
	case "huge":
		fmt.Print(strings.Repeat("a", 64<<10))
	case "hang":
		time.Sleep(30 * time.Second)
	default:
		fmt.Fprintf(os.Stderr, "unknown helper mode %q\n", mode)
		os.Exit(2)
	}

	os.Exit(0)
}

// helperRunner returns a runner that executes this test binary in the given mode.
func helperRunner(t *testing.T, mode string, opts ...func(*execRunner)) (execRunner, string) {
	t.Helper()

	runner := execRunner{
		timeout:  5 * time.Second,
		maxBytes: DefaultCommandMaxBytes,
		env: append(os.Environ(),
			"FLOWSTATE_HELPER_MODE="+mode,
		),
	}

	for _, opt := range opts {
		opt(&runner)
	}

	return runner, os.Args[0]
}

func Test_execRunner(t *testing.T) {
	helperArgs := []string{"-test.run=^TestHelperProcess$", "-test.v=false"}

	t.Run("a missing tool is reported as unavailable", func(t *testing.T) {
		// A worker whose tool is absent should say so, and it is worth another
		// attempt only in the sense that the operator may install it — but it is
		// classified transient because the alternative, reporting a missing tool as
		// a missing secret, sends whoever reads it looking in the wrong place.
		runner := execRunner{timeout: time.Second}

		_, err := runner.run(t.Context(), "flowstate-definitely-not-a-real-tool")
		require.ErrorIs(t, err, ErrUnavailable)
		require.ErrorContains(t, err, "not installed or not on PATH")
	})

	t.Run("output is returned", func(t *testing.T) {
		runner, bin := helperRunner(t, "value")

		out, err := runner.run(t.Context(), bin, helperArgs...)
		require.NoError(t, err)
		require.Equal(t, "the-secret-value\n", string(out))
	})

	t.Run("empty output is returned as empty", func(t *testing.T) {
		// The runner does not decide what an empty result means; the provider does,
		// because "empty" is a secret-level concept rather than a process-level one.
		runner, bin := helperRunner(t, "empty")

		out, err := runner.run(t.Context(), bin, helperArgs...)
		require.NoError(t, err)
		require.Empty(t, out)
	})

	t.Run("a non-zero exit is reported with the tool's own diagnosis", func(t *testing.T) {
		runner, bin := helperRunner(t, "fail")

		_, err := runner.run(t.Context(), bin, helperArgs...)
		require.ErrorIs(t, err, ErrNotFound)
		require.ErrorContains(t, err, "exited 44")
		require.ErrorContains(t, err, "could not be found")
	})

	t.Run("stderr can be redacted for arbitrary commands", func(t *testing.T) {
		runner, bin := helperRunner(t, "fail", func(r *execRunner) { r.redactStderr = true })

		_, err := runner.run(t.Context(), bin, helperArgs...)
		require.ErrorIs(t, err, ErrNotFound)
		require.ErrorContains(t, err, "exited 44")
		require.ErrorContains(t, err, "stderr redacted")
		require.NotContains(t, err.Error(), "could not be found")
	})

	t.Run("a tool's diagnostic output cannot forge log lines", func(t *testing.T) {
		runner, bin := helperRunner(t, "noisy-fail")

		_, err := runner.run(t.Context(), bin, helperArgs...)
		require.Error(t, err)
		require.NotContains(t, err.Error(), "\n", "a newline would let the tool forge a log line")
		require.NotContains(t, err.Error(), "\x00")
	})

	t.Run("output past the cap is an error, not a truncated secret", func(t *testing.T) {
		runner, bin := helperRunner(t, "huge", func(r *execRunner) { r.maxBytes = 1024 })

		_, err := runner.run(t.Context(), bin, helperArgs...)
		require.ErrorIs(t, err, ErrTooLarge)
		require.ErrorContains(t, err, "more than 1024 bytes")
	})

	t.Run("a hanging tool hits the timeout", func(t *testing.T) {
		runner, bin := helperRunner(t, "hang", func(r *execRunner) { r.timeout = 100 * time.Millisecond })

		start := time.Now()
		_, err := runner.run(t.Context(), bin, helperArgs...)
		elapsed := time.Since(start)

		require.ErrorIs(t, err, ErrUnavailable)
		require.ErrorContains(t, err, "did not answer within")
		require.Less(t, elapsed, 10*time.Second, "the timeout was not enforced")
	})

	t.Run("a cancelled caller stops the tool", func(t *testing.T) {
		runner, bin := helperRunner(t, "hang")

		ctx, cancel := context.WithCancel(t.Context())
		cancel()

		start := time.Now()
		_, err := runner.run(ctx, bin, helperArgs...)

		require.ErrorIs(t, err, context.Canceled)
		require.Less(t, time.Since(start), 10*time.Second)
	})
}

func Test_summarize(t *testing.T) {
	tests := []struct {
		name string
		in   string
		want string
	}{
		{name: "empty", in: "", want: "no output"},
		{name: "whitespace only", in: "  \n\t ", want: "no output"},
		{name: "trimmed", in: "  message  \n", want: "message"},
		{name: "newlines become spaces", in: "one\ntwo", want: "one two"},
		{name: "carriage returns become spaces", in: "one\r\ntwo", want: "one  two"},
		{name: "null bytes become spaces", in: "a\x00b", want: "a b"},
		{name: "escapes become spaces", in: "a\x1b[31mb", want: "a [31mb"},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			require.Equal(t, test.want, summarize(test.in))
		})
	}

	t.Run("long output is bounded", func(t *testing.T) {
		got := summarize(strings.Repeat("x", 1000))
		require.LessOrEqual(t, len(got), 210)
		require.Contains(t, got, "…")
	})
}

func Test_limitedWriter(t *testing.T) {
	tests := []struct {
		name    string
		limit   int64
		writes  []string
		wantErr bool
	}{
		{name: "under the limit", limit: 16, writes: []string{"abc"}},
		{name: "exactly at the limit", limit: 3, writes: []string{"abc"}},
		{name: "one byte over", limit: 3, writes: []string{"abcd"}, wantErr: true},
		{name: "over across several writes", limit: 4, writes: []string{"ab", "cd", "ef"}, wantErr: true},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			var sink strings.Builder
			w := &limitedWriter{w: &sink, limit: test.limit}

			var err error
			for _, chunk := range test.writes {
				if _, err = w.Write([]byte(chunk)); err != nil {
					break
				}
			}

			if test.wantErr {
				require.ErrorIs(t, err, errOutputTooLarge)
				return
			}

			require.NoError(t, err)
			require.Equal(t, strings.Join(test.writes, ""), sink.String())
		})
	}
}
