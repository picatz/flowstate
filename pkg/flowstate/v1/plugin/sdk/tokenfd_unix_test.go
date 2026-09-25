//go:build unix

package sdk

import (
	"strconv"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/picatz/flowstate/pkg/flowstate/v1/plugin/internal/protocol"
)

// TestReadEnvironmentBoundsTheWaitForTheTokenLine checks that a launcher which
// never finishes the token line is refused rather than waited on.
//
// The byte bound cannot decide either of these: both carry fewer bytes than
// [protocol.MaxTokenBytes] and neither closes the write end, so a read to EOF
// has nothing to return and nothing to end it. Without a bound in time, that is
// where [Run] stops — before it can serve, before it can refuse, and past the
// point where cancelling it would be noticed.
func TestReadEnvironmentBoundsTheWaitForTheTokenLine(t *testing.T) {
	tests := []struct {
		name    string
		written string
	}{
		{name: "a writer that sends nothing at all", written: ""},
		{name: "a line that never gets its newline", written: "THE-PER-LAUNCH-TOKEN"},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			fd := heldOpenPipe(t, test.written)

			t.Setenv(protocol.MagicCookieEnv, protocol.MagicCookieValue)
			t.Setenv(protocol.VersionsEnv, protocol.FormatVersions(protocol.HostVersions()))
			t.Setenv(protocol.SocketEnv, "/tmp/s")
			t.Setenv(protocol.TokenFDEnv, strconv.Itoa(fd))

			start := time.Now()
			_, err := readEnvironment()
			elapsed := time.Since(start)

			if err == nil {
				t.Fatal("readEnvironment succeeded on a descriptor carrying no complete token line")
			}

			// The refusal has to be actionable by whoever launched this: which
			// descriptor was read, and how long it was given.
			for _, want := range []string{
				protocol.TokenFDEnv,
				strconv.Itoa(fd),
				tokenReadTimeout.String(),
			} {
				if !strings.Contains(err.Error(), want) {
					t.Errorf("error = %q, want it to name %q", err, want)
				}
			}

			// A refusal that arrives early is a refusal for some other reason —
			// a descriptor rejected before it was read, say — and would leave
			// the hang this bounds unproven.
			if elapsed < tokenReadTimeout {
				t.Errorf("refused after %s, sooner than the %s bound", elapsed, tokenReadTimeout)
			}
		})
	}
}

// heldOpenPipe returns the read end of a pipe holding written, whose write end
// stays open until the test ends.
//
// A raw pipe rather than [os.Pipe], because the shape is the point: a descriptor
// inherited through exec is a bare number this process has never wrapped, while
// os.Pipe's read end is already registered with the runtime poller, which
// changes what [os.NewFile] can make of it. See [tokenDescriptor], which hands
// over the other half of the same distinction.
//
// The read end is not closed here. Whoever reads it owns it, the way a plugin
// owns the descriptor it was launched with.
func heldOpenPipe(t *testing.T, written string) int {
	t.Helper()

	var fds [2]int
	if err := syscall.Pipe(fds[:]); err != nil {
		t.Fatalf("pipe: %v", err)
	}
	t.Cleanup(func() { syscall.Close(fds[1]) })

	if written != "" {
		if _, err := syscall.Write(fds[1], []byte(written)); err != nil {
			t.Fatalf("writing %q: %v", written, err)
		}
	}

	return fds[0]
}
