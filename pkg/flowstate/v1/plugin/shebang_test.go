package plugin

import (
	"os"
	"path/filepath"
	"testing"
	"time"
)

// A plugin does not have to be a compiled binary. [Discover] admits any
// executable regular file, and a `#!` script is the ordinary way to ship one
// that wraps something else — so this is about a capability, not about the
// digest: pinning the image to its descriptor must not take scripts away.
//
// It nearly did. On Linux the pinned image is named `/proc/self/fd/N`, and a
// script is not executed the way a native binary is: the kernel starts the
// *interpreter* and hands it that path to reopen, by which time the
// close-on-exec descriptor is gone. The plugin then died before the handshake,
// with a message from `sh` about a path no operator ever wrote down.

// shebangPlugin builds a search directory whose only plugin is a `#!` script,
// and returns the directory and the script.
//
// The script execs a copy of this test binary, which is what every other fake
// plugin here is; the copy is named for the mode it should serve and kept
// outside the search directory, because [fakeMode] reads the mode from
// os.Args[0] and `exec` in a POSIX shell replaces argv[0] with the path it is
// given. Two files with one basename therefore need two directories.
func shebangPlugin(t *testing.T, mode string) (dir, script string) {
	t.Helper()

	self, err := os.Executable()
	if err != nil {
		t.Fatalf("finding the test binary: %v", err)
	}
	binary, err := os.ReadFile(self)
	if err != nil {
		t.Fatalf("reading the test binary: %v", err)
	}

	inner := t.TempDir()
	interpreted := filepath.Join(inner, BinaryPrefix+mode)
	if err := os.WriteFile(interpreted, binary, 0o755); err != nil {
		t.Fatalf("copying the test binary: %v", err)
	}

	dir = t.TempDir()
	script = filepath.Join(dir, BinaryPrefix+mode)
	if err := os.WriteFile(script, []byte("#!/bin/sh\nexec "+interpreted+"\n"), 0o755); err != nil {
		t.Fatalf("writing the script: %v", err)
	}

	return dir, script
}

// TestAShebangPluginStillLaunches is the regression itself, at the level the
// capability lives: a host opened over a search path holding a script gets a
// working plugin.
//
// It is an end-to-end launch rather than an assertion about [execImage],
// because what broke was not a field — it was a process that exited before the
// handshake, which only running one can show. It carries no build tag for the
// same reason: a script plugin has to launch on every platform this package
// supports, whatever each one does about pinning.
// The waiting bounds are generous here for launch_test.go's
// `timeoutIsTheBound` reason, and this test needs it more than most: what it
// launches is a shell that execs a *copy of this whole race-instrumented test
// binary*, written to disk by [shebangPlugin] moments earlier. Handshaking
// within testConfig's three seconds is comfortable on an idle machine and a
// coin flip on one running eleven packages' tests at once, and losing that
// flip reports "a `#!` script plugin did not launch" — the regression's own
// voice — for a machine that was merely busy. What is under test is whether a
// script launches at all, never how fast (issue #852, the same confusion the
// progress-starvation tests were carrying).
func TestAShebangPluginStillLaunches(t *testing.T) {
	t.Parallel()

	dir, _ := shebangPlugin(t, "ok")

	cfg := testConfig(t, dir)
	cfg.HandshakeTimeout = time.Minute
	cfg.DescribeTimeout = time.Minute

	host := openHost(t, cfg)

	if _, ok := host.Lookup("ok"); !ok {
		t.Fatal("a `#!` script plugin did not launch; scripts are a shape Discover accepts, " +
			"and pinning the image to its descriptor must not silently take them away")
	}
}
