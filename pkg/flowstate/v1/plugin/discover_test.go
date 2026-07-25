package plugin

import (
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// TestPluginName checks which file names are plugins.
//
// The name is the host's identity for a plugin, so it is constrained to what the
// schema permits for a manifest name — which also means a file name cannot carry
// a path separator, a leading dash that would read as a flag, or anything else
// that would have to be quoted later.
func TestPluginName(t *testing.T) {
	t.Parallel()

	tests := []struct {
		fileName string
		want     string
		wantOK   bool
	}{
		{fileName: "flowstate-plugin-vault", want: "vault", wantOK: true},
		{fileName: "flowstate-plugin-aws-secrets", want: "aws-secrets", wantOK: true},
		{fileName: "flowstate-plugin-v2", want: "v2", wantOK: true},
		{fileName: "flowstate-plugin-", wantOK: false},
		{fileName: "flowstate-plugin--leading-dash", wantOK: false},
		{fileName: "flowstate-plugin-Vault", wantOK: false},
		{fileName: "flowstate-plugin-with_underscore", wantOK: false},
		{fileName: "flowstate-plugin-with.dot", wantOK: false},
		{fileName: "flowstate-plugin-with/slash", wantOK: false},
		{fileName: "vault", wantOK: false},
		{fileName: "flowstate-plugins-vault", wantOK: false},
		{fileName: "flowstate-plugin-" + strings.Repeat("x", MaxNameLen+1), wantOK: false},
	}

	for _, test := range tests {
		t.Run(test.fileName, func(t *testing.T) {
			t.Parallel()

			got, ok := pluginName(test.fileName)
			if ok != test.wantOK {
				t.Fatalf("pluginName(%q) ok = %v, want %v", test.fileName, ok, test.wantOK)
			}
			if ok && got != test.want {
				t.Errorf("pluginName(%q) = %q, want %q", test.fileName, got, test.want)
			}
		})
	}
}

// TestDiscoverRefusesUnsafeSearchPath checks the refusals that exist because a
// directory of plugin binaries is a list of programs this process will run.
func TestDiscoverRefusesUnsafeSearchPath(t *testing.T) {
	t.Parallel()

	t.Run("relative directory", func(t *testing.T) {
		t.Parallel()

		_, err := Discover(Config{SearchPath: []string{"./plugins"}})
		if !errors.Is(err, ErrSearchPath) {
			t.Fatalf("Discover error = %v, want one wrapping %v", err, ErrSearchPath)
		}
		if !strings.Contains(err.Error(), "must be absolute") {
			t.Errorf("error = %q, want it to say why", err.Error())
		}
	})

	t.Run("world-writable directory", func(t *testing.T) {
		t.Parallel()

		dir := t.TempDir()
		if err := os.Chmod(dir, 0o777); err != nil {
			t.Fatalf("chmod: %v", err)
		}

		_, err := Discover(Config{SearchPath: []string{dir}})
		if !errors.Is(err, ErrSearchPath) {
			t.Fatalf("Discover error = %v, want one wrapping %v", err, ErrSearchPath)
		}
		if !strings.Contains(err.Error(), "writable by any user") {
			t.Errorf("error = %q, want it to say why", err.Error())
		}

		// And the escape hatch works, for the single-user image it exists for.
		if _, err := Discover(Config{SearchPath: []string{dir}, AllowInsecureSearchPath: true}); err != nil {
			t.Errorf("Discover with AllowInsecureSearchPath: %v", err)
		}
	})

	t.Run("world-writable binary", func(t *testing.T) {
		t.Parallel()

		dir := t.TempDir()
		path := filepath.Join(dir, BinaryPrefix+"loose")
		if err := os.WriteFile(path, []byte("#!/bin/sh\n"), 0o755); err != nil {
			t.Fatalf("writing the binary: %v", err)
		}

		// Chmod rather than a mode on WriteFile: the umask filters the create
		// mode, so the file would come out 0755 and the test would pass without
		// testing anything.
		if err := os.Chmod(path, 0o777); err != nil {
			t.Fatalf("chmod: %v", err)
		}

		_, err := Discover(Config{SearchPath: []string{dir}})
		if !errors.Is(err, ErrSearchPath) {
			t.Fatalf("Discover error = %v, want one wrapping %v", err, ErrSearchPath)
		}
	})
}

// TestDiscoverSkipsWhatIsNotAPlugin checks that discovery is quiet about the
// ordinary contents of a directory, and refuses nothing for them.
func TestDiscoverSkipsWhatIsNotAPlugin(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()

	write := func(name string, mode os.FileMode) {
		t.Helper()
		if err := os.WriteFile(filepath.Join(dir, name), []byte("binary"), mode); err != nil {
			t.Fatalf("writing %q: %v", name, err)
		}
	}

	write(BinaryPrefix+"good", 0o755)
	write(BinaryPrefix+"not-executable", 0o644)
	write("README", 0o644)
	write("some-other-tool", 0o755)
	write(BinaryPrefix+"BadName", 0o755)

	if err := os.Mkdir(filepath.Join(dir, BinaryPrefix+"adirectory"), 0o755); err != nil {
		t.Fatalf("making a directory: %v", err)
	}

	found, err := Discover(Config{SearchPath: []string{dir}})
	if err != nil {
		t.Fatalf("Discover: %v", err)
	}

	if len(found) != 1 {
		var names []string
		for _, f := range found {
			names = append(names, f.Name)
		}
		t.Fatalf("discovered %v, want only [good]", names)
	}
	if found[0].Name != "good" {
		t.Errorf("discovered %q, want %q", found[0].Name, "good")
	}
	if found[0].Dir != dir {
		t.Errorf("Dir = %q, want %q", found[0].Dir, dir)
	}
}

// TestDiscoverPrecedence checks that a name in two directories resolves by
// configuration order rather than by whatever order the filesystem returns.
func TestDiscoverPrecedence(t *testing.T) {
	t.Parallel()

	first, second := t.TempDir(), t.TempDir()

	for _, dir := range []string{first, second} {
		if err := os.WriteFile(filepath.Join(dir, BinaryPrefix+"shared"), []byte("x"), 0o755); err != nil {
			t.Fatalf("writing: %v", err)
		}
	}
	if err := os.WriteFile(filepath.Join(second, BinaryPrefix+"only-second"), []byte("x"), 0o755); err != nil {
		t.Fatalf("writing: %v", err)
	}

	found, err := Discover(Config{SearchPath: []string{first, second}})
	if err != nil {
		t.Fatalf("Discover: %v", err)
	}

	if len(found) != 2 {
		t.Fatalf("discovered %d plugins, want 2", len(found))
	}

	// Sorted by name, so "only-second" then "shared".
	if found[1].Name != "shared" || found[1].Dir != first {
		t.Errorf("shared resolved to %q, want the copy in the first directory", found[1].Dir)
	}
}

// TestDiscoverSkipsMissingDirectories checks that a search path configured
// across hosts that do not all have plugins installed is not an error.
func TestDiscoverSkipsMissingDirectories(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	if err := os.WriteFile(filepath.Join(dir, BinaryPrefix+"here"), []byte("x"), 0o755); err != nil {
		t.Fatalf("writing: %v", err)
	}

	found, err := Discover(Config{SearchPath: []string{
		filepath.Join(t.TempDir(), "does-not-exist"),
		dir,
	}})
	if err != nil {
		t.Fatalf("Discover: %v", err)
	}
	if len(found) != 1 || found[0].Name != "here" {
		t.Errorf("discovered %v, want only [here]", found)
	}
}

// TestSocketPathLength checks the bound that turns a bare "invalid argument"
// from bind into a message naming the path and the fix.
func TestSocketPathLength(t *testing.T) {
	t.Parallel()

	if err := checkSocketPath("/tmp/short/s"); err != nil {
		t.Errorf("a short path was refused: %v", err)
	}

	long := "/tmp/" + strings.Repeat("d", maxSocketPathLen) + "/s"
	err := checkSocketPath(long)
	if err == nil {
		t.Fatal("an over-long socket path was accepted")
	}
	if !strings.Contains(err.Error(), "Config.SocketDir") {
		t.Errorf("error = %q, want it to name the fix", err.Error())
	}
}

// TestConfigValidation checks that a configuration that cannot work is refused
// when the host is built, rather than when the first workflow needs a plugin.
func TestConfigValidation(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		cfg  Config
	}{
		{name: "relative search path", cfg: Config{SearchPath: []string{"plugins"}}},
		{name: "relative socket directory", cfg: Config{SocketDir: "sockets"}},
		{name: "empty permitted scheme", cfg: Config{PermittedSchemes: []string{""}}},
		{name: "malformed environment entry", cfg: Config{Env: []string{"NOT_AN_ASSIGNMENT"}}},
		{name: "environment entry with no key", cfg: Config{Env: []string{"=value"}}},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()

			if _, err := NewHost(test.cfg); err == nil {
				t.Error("NewHost accepted a configuration that cannot work")
			}
		})
	}
}

// TestZeroConfigRunsNothing checks the default: a deployment that configures no
// plugin directories gets no plugins.
func TestZeroConfigRunsNothing(t *testing.T) {
	t.Parallel()

	host, err := NewHost(Config{})
	if err != nil {
		t.Fatalf("NewHost: %v", err)
	}
	defer host.Close(t.Context())

	if err := host.Open(t.Context()); err != nil {
		t.Fatalf("Open: %v", err)
	}

	if got := len(host.Plugins()); got != 0 {
		t.Errorf("a zero Config launched %d plugins, want 0", got)
	}
	if got := len(host.SecretProviders()); got != 0 {
		t.Errorf("a zero Config provides %d secret providers, want 0", got)
	}
	if got := len(host.TaskDefs()); got != 0 {
		t.Errorf("a zero Config provides %d tasks, want 0", got)
	}
}
