package main

import (
	"context"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"maps"
	"os"
	"path/filepath"
	"strings"
	"time"
)

// changeWatcher reports when the files an authoring loop reads have changed.
// It is a value behind an interface, as the local driver's scheduler is, so
// the loop is tested without a clock or a file system (#1472).
type changeWatcher interface {
	// Wait blocks until the watched set differs from what the previous Wait
	// (or construction) saw and has stopped changing, or ctx ends.
	Wait(ctx context.Context) error
}

const (
	// maxWatchedFiles bounds the snapshot work like the directory walks it
	// shares an argument with: a watch over a huge tree is refused, not slow.
	maxWatchedFiles = 10_000

	watchPollInterval = 250 * time.Millisecond
)

// fileStamp is what a poll compares: a change in either field is a change.
type fileStamp struct {
	modTime time.Time
	size    int64
}

// pollWatcher is the stdlib-only changeWatcher: it stats every YAML file
// under the roots on an interval. No event framework, and nothing to leak.
type pollWatcher struct {
	roots    []string
	interval time.Duration
	last     map[string]fileStamp
}

// watchRoots turns the paths given to `flow test` into what is watched: a
// directory is walked, and a named file contributes its own directory, since
// its workflow and `testdefaults.yaml` live beside it.
func watchRoots(paths []string) []string {
	var roots []string
	for _, p := range paths {
		if info, err := os.Stat(p); err == nil && !info.IsDir() {
			p = filepath.Dir(p)
		}
		roots = append(roots, p)
	}

	return roots
}

func newPollWatcher(paths []string, interval time.Duration) (*pollWatcher, error) {
	w := &pollWatcher{roots: watchRoots(paths), interval: interval}
	snap, err := w.snapshot()
	if err != nil {
		return nil, err
	}
	w.last = snap

	return w, nil
}

func (w *pollWatcher) snapshot() (map[string]fileStamp, error) {
	snap := map[string]fileStamp{}
	for _, root := range w.roots {
		err := filepath.WalkDir(root, func(p string, d fs.DirEntry, err error) error {
			if err != nil {
				// A file that vanishes between the walk and the stat is a
				// change the next poll reports, not a failure of the watch.
				if errors.Is(err, fs.ErrNotExist) {
					return nil
				}

				return err
			}
			if d.IsDir() || !(strings.HasSuffix(p, ".yaml") || strings.HasSuffix(p, ".yml")) {
				return nil
			}
			info, err := d.Info()
			if err != nil {
				return nil //nolint:nilerr // vanished mid-walk; see above
			}
			if len(snap) >= maxWatchedFiles {
				return fmt.Errorf("--watch: more than %d YAML files under %s; narrow the paths", maxWatchedFiles, root)
			}
			snap[p] = fileStamp{modTime: info.ModTime(), size: info.Size()}

			return nil
		})
		if err != nil {
			return nil, err
		}
	}

	return snap, nil
}

// Wait polls until a change appears and then until two consecutive polls
// agree, so an editor's write-then-rename lands as one change.
func (w *pollWatcher) Wait(ctx context.Context) error {
	ticker := time.NewTicker(w.interval)
	defer ticker.Stop()

	changed := false
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
		}
		snap, err := w.snapshot()
		if err != nil {
			return err
		}
		if maps.Equal(snap, w.last) {
			if changed {
				return nil
			}

			continue
		}
		changed = true
		w.last = snap
	}
}

// watchLoop runs once, then again after every change, until ctx ends. Whether
// the first run's error ends the loop is the caller's: a usage error is not
// something an edit can fix, but a later run failing may well be (a file saved
// half-written), so it is reported and the loop keeps watching.
func watchLoop(ctx context.Context, w changeWatcher, run func(first bool) error, between func(), status io.Writer) error {
	for first := true; ; first = false {
		if err := run(first); err != nil && !errors.Is(err, errTestsFailed) {
			if first {
				return err
			}
			fmt.Fprintln(status, "flow test --watch:", err)
		}
		fmt.Fprintln(status, "watching for changes; press Ctrl-C to stop")
		if err := w.Wait(ctx); err != nil {
			if ctx.Err() != nil {
				return nil
			}

			return err
		}
		between()
	}
}
