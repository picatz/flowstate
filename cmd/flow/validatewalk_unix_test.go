//go:build unix

package main

import (
	"os"
	"path/filepath"
	"syscall"
	"testing"

	"github.com/stretchr/testify/require"
)

// collectOrBlock runs collectValidateTargets in a goroutine and waits on a
// channel. A regression that blocks in open has no earlier deadline than the
// test binary's own -timeout, which is the failure signal here.
func collectOrBlock(paths ...string) error {
	done := make(chan error, 1)
	go func() {
		_, err := collectValidateTargets(paths, nil)
		done <- err
	}()
	return <-done
}

// A named pipe in a walked directory must be refused at once: opening a FIFO
// for reading waits for a writer forever, which hung `flow validate dir`.
func TestValidateWalkRefusesANamedPipeWithoutBlocking(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	pipe := filepath.Join(dir, "pipe.yaml")
	require.NoError(t, syscall.Mkfifo(pipe, 0o600))

	err := collectOrBlock(dir)
	require.ErrorContains(t, err, pipe+" is not a regular file (named pipe); name a regular file instead")
}

// A symlink named *.yaml that points at a FIFO is followed by the open, so it
// must be refused the same way.
func TestValidateWalkRefusesASymlinkToANamedPipe(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	pipe := filepath.Join(t.TempDir(), "pipe")
	require.NoError(t, syscall.Mkfifo(pipe, 0o600))
	link := filepath.Join(dir, "link.yaml")
	require.NoError(t, os.Symlink(pipe, link))

	err := collectOrBlock(dir)
	require.ErrorContains(t, err, link+" is not a regular file (named pipe); name a regular file instead")
}

// A named pipe given directly as the path must be refused, not opened.
func TestValidateNamedPathRefusesANamedPipeWithoutBlocking(t *testing.T) {
	t.Parallel()

	pipe := filepath.Join(t.TempDir(), "pipe.yaml")
	require.NoError(t, syscall.Mkfifo(pipe, 0o600))

	err := collectOrBlock(pipe)
	require.ErrorContains(t, err, pipe+" is not a regular file (named pipe); name a regular file instead")
}

// A named pipe named *.test.yaml is classified as a test by name alone and so
// reaches the test loader, which must refuse it rather than open it.
func TestValidateWalkRefusesATestFilePipeWithoutBlocking(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	require.NoError(t, syscall.Mkfifo(filepath.Join(dir, "suite.test.yaml"), 0o600))

	type result struct {
		out string
		err error
	}
	done := make(chan result, 1)
	go func() {
		out, err := validateOutput(t, dir)
		done <- result{out, err}
	}()
	res := <-done
	text := res.out
	if res.err != nil {
		text += res.err.Error()
	}
	require.Contains(t, text, "not a regular file")
}
