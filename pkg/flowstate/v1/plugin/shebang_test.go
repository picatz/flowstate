package plugin

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// A plugin does not have to be a compiled binary. [Discover] admits any
// executable regular file, and a `#!` script is the ordinary way to ship one
// that wraps something else — so these are about a capability, not about the
// digest: pinning the image to its descriptor must not take scripts away.
//
// It nearly did. On Linux the pinned image is named `/proc/self/fd/N`, and a
// script is not executed the way a binary is: the kernel starts the
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
// handshake, which only running one can show.
func TestAShebangPluginStillLaunches(t *testing.T) {
	t.Parallel()

	dir, _ := shebangPlugin(t, "ok")

	host := openHost(t, testConfig(t, dir))

	if _, ok := host.Lookup("ok"); !ok {
		t.Fatal("a `#!` script plugin did not launch; scripts are a shape Discover accepts, " +
			"and pinning the image to its descriptor must not silently take them away")
	}
}

// TestAShebangPluginIsExecutedByPathAndSaysSo is the other half: the fallback
// is allowed to be weaker, and is not allowed to be quiet about it.
//
// The digest then describes what the path held at launch rather than proving
// what ran, which is exactly the claim the non-Linux fallback already makes —
// so it is made through the same log line, for a reason naming the limitation
// instead of naming the platform.
func TestAShebangPluginIsExecutedByPathAndSaysSo(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	path := filepath.Join(dir, BinaryPrefix+"scripted")
	if err := os.WriteFile(path, []byte("#!/bin/sh\nexit 0\n"), 0o755); err != nil {
		t.Fatalf("writing the script: %v", err)
	}

	var logged strings.Builder

	image, err := openExecImage(path, newCapturingLogger(t, &logged))
	if err != nil {
		t.Fatalf("openExecImage: %v", err)
	}
	t.Cleanup(image.close)

	if image.pinned {
		t.Error("a `#!` script was pinned to its descriptor; the interpreter cannot reopen that name")
	}
	if image.execPath != path {
		t.Errorf("exec path = %q, want the installed path %q", image.execPath, path)
	}

	// Honest about the weaker guarantee, and specific about why this image
	// rather than this platform is the reason.
	if !strings.Contains(logged.String(), "rather than proving what ran") {
		t.Errorf("nothing was logged about the weaker guarantee; log was:\n%s", logged.String())
	}
	if !strings.Contains(logged.String(), "script") {
		t.Errorf("the logged reason does not name the `#!` limitation; log was:\n%s", logged.String())
	}
}

// TestAnImageTooShortToHoldAShebangIsNotAScript pins the boundary the marker
// read has to get right: a file shorter than two bytes is a short read, not a
// failure, and not a script.
func TestAnImageTooShortToHoldAShebangIsNotAScript(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	for name, content := range map[string]string{"empty": "", "one-byte": "#"} {
		path := filepath.Join(dir, BinaryPrefix+name)
		if err := os.WriteFile(path, []byte(content), 0o755); err != nil {
			t.Fatalf("writing %s: %v", path, err)
		}

		f, err := os.Open(path)
		if err != nil {
			t.Fatalf("opening %s: %v", path, err)
		}

		script, err := startsWithShebang(f)
		f.Close()

		if err != nil {
			t.Errorf("startsWithShebang(%s): %v", name, err)
		}
		if script {
			t.Errorf("%s was read as a `#!` script", name)
		}
	}
}
