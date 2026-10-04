package main

import (
	"bytes"
	"context"
	"errors"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestPollWatcherSeesAnEditAndIgnoresQuiet: no change returns nothing until
// the context ends; an edit to a YAML file returns once it has settled; a
// non-YAML file is not watched.
func TestPollWatcherSeesAnEditAndIgnoresQuiet(t *testing.T) {
	dir := t.TempDir()
	file := filepath.Join(dir, "a.test.yaml")
	require.NoError(t, os.WriteFile(file, []byte("a: 1\n"), 0o600))

	w, err := newPollWatcher([]string{dir}, 5*time.Millisecond)
	require.NoError(t, err)

	quiet, cancel := context.WithTimeout(t.Context(), 60*time.Millisecond)
	defer cancel()
	require.ErrorIs(t, w.Wait(quiet), context.DeadlineExceeded, "nothing changed")

	require.NoError(t, os.WriteFile(filepath.Join(dir, "notes.txt"), []byte("x"), 0o600))
	quiet2, cancel2 := context.WithTimeout(t.Context(), 60*time.Millisecond)
	defer cancel2()
	require.ErrorIs(t, w.Wait(quiet2), context.DeadlineExceeded, "a non-YAML file is not watched")

	require.NoError(t, os.WriteFile(file, []byte("a: 22\n"), 0o600))
	edit, cancel3 := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel3()
	require.NoError(t, w.Wait(edit), "an edit must wake the watcher")
}

// TestPollWatcherSeesANewFileInANamedFilesDirectory: naming one test file
// watches its directory, where its workflow lives.
func TestPollWatcherSeesANewFileInANamedFilesDirectory(t *testing.T) {
	dir := t.TempDir()
	file := filepath.Join(dir, "a.test.yaml")
	require.NoError(t, os.WriteFile(file, []byte("a: 1\n"), 0o600))
	w, err := newPollWatcher([]string{file}, 5*time.Millisecond)
	require.NoError(t, err)

	require.NoError(t, os.WriteFile(filepath.Join(dir, "workflow.yaml"), []byte("b: 1\n"), 0o600))
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()
	require.NoError(t, w.Wait(ctx))
}

type fakeWatcher struct{ waits int }

func (f *fakeWatcher) Wait(ctx context.Context) error {
	f.waits++
	if f.waits > 2 {
		return context.Canceled
	}

	return nil
}

// TestWatchLoopRunsOncePerChangeAndSurvivesALaterError: one run up front and
// one per change, a failed later run does not end the loop, and the end of the
// context is a clean stop.
func TestWatchLoopRunsOncePerChangeAndSurvivesALaterError(t *testing.T) {
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	f := &fakeWatcher{}
	runs, between := 0, 0
	var status bytes.Buffer

	// The fake cancels the context on its third wait, as ^C would.
	err := watchLoop(ctx, cancelingWatcher{f, cancel},
		func(first bool) error {
			runs++
			if runs == 2 {
				return errors.New("half-written file")
			}
			if runs == 3 {
				return errTestsFailed
			}

			return nil
		},
		func() { between++ }, &status)
	require.NoError(t, err)
	assert.Equal(t, 3, runs)
	assert.Equal(t, 2, between)
	assert.Contains(t, status.String(), "half-written file")
}

type cancelingWatcher struct {
	*fakeWatcher
	cancel context.CancelFunc
}

func (c cancelingWatcher) Wait(ctx context.Context) error {
	err := c.fakeWatcher.Wait(ctx)
	if err != nil {
		c.cancel()
	}

	return err
}

// TestWatchLoopFirstRunUsageErrorEnds: an error an edit cannot fix ends the
// command instead of waiting forever.
func TestWatchLoopFirstRunUsageErrorEnds(t *testing.T) {
	err := watchLoop(t.Context(), &fakeWatcher{}, func(bool) error { return errors.New("no *.test.yaml files") }, func() {}, &bytes.Buffer{})
	require.ErrorContains(t, err, "no *.test.yaml files")
}

func TestWatchRefusedWithDebug(t *testing.T) {
	res := runFlow(t, "test", "--watch", "--debug", t.TempDir())
	require.Error(t, res.Err)
	assert.Contains(t, res.Err.Error(), "--watch cannot be combined with --debug")
}
