// Package covbuild is test-only support for measuring coverage inside a
// subprocess.
//
// At least seven test files in this repository drive the `flow` binary or a
// plugin as a real subprocess (exec.Command on a separately compiled binary)
// rather than calling into the package under test directly (#519):
// cmd/flow/execute_test.go, nocolor_test.go, mcp_plugin_test.go,
// cmd/flow/internal/appearance/appearance_test.go, and
// pkg/flowstate/v1/plugin/example_test.go among them. `go test -cover` only
// instruments the package it is compiling; it cannot see a line executed
// inside a process it merely launched. Go 1.20's GOCOVERDIR mechanism closes
// exactly that gap: a binary built with `-cover` writes coverage counters to
// the directory named by GOCOVERDIR every time it runs, and `go tool covdata`
// merges those counters — from as many separate processes as ran — with the
// ones `go test` collected for its own in-process code.
//
// Building every subprocess binary with -cover unconditionally would slow
// every ordinary `go test` run for a benefit nobody asked for on that run. So
// instrumentation here is opt-in, keyed off FLOWSTATE_COVERDIR: set it and a
// build made through this package adds -cover and every subprocess it runs
// is told, explicitly, to write into that directory; leave it unset — the
// default for every existing `go test` invocation, including `go run
// ./tools/gate` and `make check` — and nothing here changes their behavior
// at all. `make coverage` sets it; see CLAUDE.md's "Running tests" section
// for how to run it and read the result.
//
// FLOWSTATE_COVERDIR, not GOCOVERDIR, on purpose. `go test -cover ... -args
// -test.gocoverdir=X` does not simply pass X through as the GOCOVERDIR a
// running test process sees: it points GOCOVERDIR at a scratch directory of
// its own for the duration of the run and copies only the counters *it*
// collected into X afterward. A subprocess that inherited that scratch
// GOCOVERDIR (which is exactly what happens if this package used the same
// name and callers left Cmd.Env nil) writes real counters — verified with a
// standalone reproduction — into a directory `go test` then discards without
// ever merging into X, which is coverage that silently vanishes rather than
// coverage that is missing loudly. A distinct variable, explicitly threaded
// through Env into every subprocess this package's callers launch, is what
// makes the destination the one directory `make coverage` actually reads.
package covbuild

import "os"

// Dir reports the directory instrumented subprocess binaries should write
// their coverage counters to, and whether coverage instrumentation was
// requested at all. It is FLOWSTATE_COVERDIR — see the package doc for why
// this is not simply GOCOVERDIR.
func Dir() (dir string, enabled bool) {
	dir = os.Getenv("FLOWSTATE_COVERDIR")
	return dir, dir != ""
}

// BuildArgs returns the extra arguments a `go build` invocation should splice
// in to produce an instrumented binary — `-cover` when GOCOVERDIR is set, nil
// when it is not. Callers insert the result right after `build`:
//
//	args := append([]string{"build"}, covbuild.BuildArgs()...)
//	args = append(args, "-o", bin, ".")
//	exec.Command("go", args...)
func BuildArgs() []string {
	if _, enabled := Dir(); enabled {
		return []string{"-cover"}
	}
	return nil
}

// Env returns the extra environment entries a subprocess needs for its
// coverage counters to land in FLOWSTATE_COVERDIR: a real GOCOVERDIR set to
// that directory, when instrumentation was requested, and nothing otherwise
// — so appending the result is always safe.
//
// Callers must append this explicitly to every subprocess they launch,
// including one whose Cmd.Env is otherwise nil or built by appending to
// os.Environ(). Ordinary inheritance is not enough during a `go test -cover`
// run: see the package doc for why the GOCOVERDIR a running test process
// observes is not the directory a merge will ever read counters back out of.
func Env() []string {
	if dir, enabled := Dir(); enabled {
		return []string{"GOCOVERDIR=" + dir}
	}
	return nil
}
