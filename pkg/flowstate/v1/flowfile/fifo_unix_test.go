//go:build unix

package flowfile_test

import (
	"context"
	"syscall"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/picatz/flowstate/pkg/flowstate/v1/flowfile"
)

// A `use:` module or `call:` target that is a FIFO must be refused at once: the
// read is bounded, and opening a FIFO for reading waits for a writer forever.
func TestNonRegularSourcesAreRefusedWithoutBlocking(t *testing.T) {
	t.Parallel()

	for name, step := range map[string]string{
		"use":  "use:\n  m:\n    path: ./pipe.yaml\nsteps:\n  - id: a\n    log:\n      message: hi\n",
		"call": "steps:\n  - id: a\n    call: ./pipe.yaml\n",
	} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()

			dir := t.TempDir()
			require.NoError(t, syscall.Mkfifo(dir+"/pipe.yaml", 0o600))
			caller := writeFile(t, dir, "caller.yaml", "edition: v2026.4\nname: caller\n"+step)

			ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
			defer cancel()
			done := make(chan error, 1)
			go func() {
				_, _, err := flowfile.ParseFile(caller)
				done <- err
			}()
			select {
			case err := <-done:
				require.ErrorContains(t, err, "is not a regular file")
			case <-ctx.Done():
				t.Fatal("reading a FIFO source blocked")
			}
		})
	}
}
