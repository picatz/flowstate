package plugin

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

// TestOpenRefusesBadPlugins is the heart of the host's fail-closed behavior: for
// each way a plugin can be wrong, the host must refuse it, say why, and leave no
// process behind.
//
// The last part is the one that would otherwise be missed. Every case here
// launches a real process, and a refusal that forgot to kill it would pass a
// test that only checked the error.
func TestOpenRefusesBadPlugins(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string

		// mode is the fake plugin to launch, which is also its plugin name.
		mode string

		// wantErr is the sentinel the refusal must wrap, so that a caller can
		// tell these apart without matching on message text.
		wantErr error

		// wantMessage is a fragment the operator-facing message must contain,
		// because a refusal nobody can act on is only half of failing closed.
		wantMessage string
	}{
		{
			name:        "binary that exits immediately",
			mode:        "exit-now",
			wantErr:     ErrExited,
			wantMessage: "handshake",
		},
		{
			name:        "binary that prints garbage",
			mode:        "garbage",
			wantErr:     ErrHandshake,
			wantMessage: "is this a Flowstate plugin?",
		},
		{
			name:        "binary that never handshakes",
			mode:        "silent",
			wantErr:     ErrHandshakeTimeout,
			wantMessage: "no handshake line within",
		},
		{
			name:        "binary that writes without a newline",
			mode:        "long-line",
			wantErr:     ErrHandshake,
			wantMessage: "without a newline",
		},
		{
			name:        "handshake with the wrong sentinel",
			mode:        "bad-sentinel",
			wantErr:     ErrHandshake,
			wantMessage: "is this a Flowstate plugin?",
		},
		{
			name:        "handshake naming a protocol version the host did not offer",
			mode:        "bad-version",
			wantErr:     ErrHandshake,
			wantMessage: "did not offer",
		},
		{
			name:        "handshake naming a socket other than the one assigned",
			mode:        "bad-address",
			wantErr:     ErrHandshake,
			wantMessage: "rather than the socket it was assigned",
		},
		{
			name:        "binary that handshakes then dies",
			mode:        "die-after",
			wantErr:     ErrLaunch,
			wantMessage: "describing",
		},
		{
			name:        "plugin whose Describe fails",
			mode:        "describe-fails",
			wantErr:     ErrLaunch,
			wantMessage: "describing",
		},
		{
			name:        "manifest that does not validate",
			mode:        "bad-manifest",
			wantErr:     ErrManifest,
			wantMessage: "invalid flowstate.v1.PluginManifest",
		},
		{
			// The schema's own min_items rule catches this one, which is the
			// right place for it: the manifest is refused before any of the
			// host's own reasoning runs.
			name:        "plugin advertising nothing",
			mode:        "no-caps",
			wantErr:     ErrManifest,
			wantMessage: "capabilities: value must contain at least 1 item",
		},
		{
			// The case the host's own check is for: capabilities that satisfy
			// the schema but that this host does not know. Each is ignored, per
			// the schema's additive rule, which leaves a plugin with nothing to
			// do rather than one to refuse a field of.
			name:        "plugin advertising only capabilities this host does not know",
			mode:        "unknown-caps",
			wantErr:     ErrManifest,
			wantMessage: "nothing for it to do",
		},
		{
			name:        "plugin advertising the unspecified capability",
			mode:        "unspecified-cap",
			wantErr:     ErrManifest,
			wantMessage: "CAPABILITY_UNSPECIFIED",
		},
		{
			name:        "secrets plugin claiming no schemes",
			mode:        "secrets-no-schemes",
			wantErr:     ErrManifest,
			wantMessage: "no reference would ever reach it",
		},
		{
			name:        "tasks plugin providing no tasks",
			mode:        "tasks-no-tasks",
			wantErr:     ErrManifest,
			wantMessage: "provides no tasks",
		},
		{
			name:        "task whose descriptor cannot be reconstructed",
			mode:        "bad-descriptor",
			wantErr:     ErrDescriptor,
			wantMessage: "could not reconstruct",
		},
		{
			name:        "task shadowing a built-in",
			mode:        "builtin-task",
			wantErr:     ErrManifest,
			wantMessage: "built-in task",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			dir := pluginDir(t, test.mode)
			cfg := testConfig(t, dir)

			host, err := NewHost(cfg)
			if err != nil {
				t.Fatalf("NewHost: %v", err)
			}

			openErr := host.Open(t.Context())
			if openErr == nil {
				t.Fatalf("Open succeeded, want a refusal")
			}

			if !errors.Is(openErr, test.wantErr) {
				t.Errorf("Open error = %v, want one wrapping %v", openErr, test.wantErr)
			}

			if !strings.Contains(openErr.Error(), test.wantMessage) {
				t.Errorf("Open error = %q, want it to mention %q", openErr.Error(), test.wantMessage)
			}

			// A refused plugin must be a plugin that is gone. Open has already
			// terminated what it started, so nothing should be left to close.
			if got := len(host.Plugins()); got != 0 {
				t.Errorf("host holds %d plugins after a refusal, want 0", got)
			}

			ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			defer cancel()
			if err := host.Close(ctx); err != nil {
				t.Errorf("Close: %v", err)
			}
		})
	}
}

// TestOpenRefusesMissingPinnedPlugin checks the case that has no process at all:
// a deployment that pinned a plugin which is not installed.
func TestOpenRefusesMissingPinnedPlugin(t *testing.T) {
	t.Parallel()

	cfg := testConfig(t, pluginDir(t))
	cfg.Only = []string{"absent"}

	host, err := NewHost(cfg)
	if err != nil {
		t.Fatalf("NewHost: %v", err)
	}
	defer host.Close(context.Background())

	err = host.Open(t.Context())
	if !errors.Is(err, ErrLaunch) {
		t.Fatalf("Open error = %v, want one wrapping %v", err, ErrLaunch)
	}
	if !strings.Contains(err.Error(), "flowstate-plugin-absent") {
		t.Errorf("Open error = %q, want it to name the missing binary", err.Error())
	}
}

// TestLaunchNoSuchBinary checks the launch path directly, for a binary that is
// not there at all.
func TestLaunchNoSuchBinary(t *testing.T) {
	t.Parallel()

	cfg := testConfig(t, t.TempDir()).withDefaults()

	_, err := launch(t.Context(), cfg, Found{
		Name: "ghost",
		Path: filepath.Join(t.TempDir(), "flowstate-plugin-ghost"),
	})
	if !errors.Is(err, ErrLaunch) {
		t.Fatalf("launch error = %v, want one wrapping %v", err, ErrLaunch)
	}

	var pluginErr *Error
	if !errors.As(err, &pluginErr) {
		t.Fatalf("launch error = %v, want a *plugin.Error naming the plugin", err)
	}
	if pluginErr.Plugin != "ghost" {
		t.Errorf("error names plugin %q, want %q", pluginErr.Plugin, "ghost")
	}
}

// TestOpenToleratesStdoutNoise checks that a plugin breaking the protocol's
// promise about stdout is tolerated rather than fatal.
//
// It matters because the failure it prevents is invisible: a plugin writing to a
// pipe nobody drains eventually blocks on a full buffer, and a blocked plugin
// looks like a hung one.
func TestOpenToleratesStdoutNoise(t *testing.T) {
	t.Parallel()

	host := openHost(t, testConfig(t, pluginDir(t, "stdout-noise")))

	p, ok := host.Lookup("stdout-noise")
	if !ok {
		t.Fatal("plugin was not launched")
	}

	// It keeps writing for a while; the plugin has to stay usable throughout.
	for range 3 {
		if health := p.CheckHealth(t.Context()); health.Status != HealthServing {
			t.Fatalf("health = %v, want serving: %v", health.Status, health.Err)
		}
		time.Sleep(50 * time.Millisecond)
	}
}

// TestHandshakeBoundsAreApplied checks that the launch environment is what the
// protocol says it is, since everything else depends on it.
func TestHandshakeBoundsAreApplied(t *testing.T) {
	t.Parallel()

	host := openHost(t, testConfig(t, pluginDir(t, "ok")))

	p, ok := host.Lookup("ok")
	if !ok {
		t.Fatal("plugin was not launched")
	}

	if p.PID() == 0 {
		t.Fatal("plugin has no process")
	}

	// The socket lives in a directory only this user can enter, which is what
	// authenticates the channel on every platform this runs on.
	p.mu.RLock()
	socketDir := p.inst.socketDir
	socketPath := p.inst.socketPath
	p.mu.RUnlock()

	info, err := os.Stat(socketDir)
	if err != nil {
		t.Fatalf("stat socket directory: %v", err)
	}
	if perm := info.Mode().Perm(); perm != 0o700 {
		t.Errorf("socket directory mode = %#o, want 0700", perm)
	}

	socketInfo, err := os.Stat(socketPath)
	if err != nil {
		t.Fatalf("stat socket: %v", err)
	}
	if perm := socketInfo.Mode().Perm(); perm != 0o600 {
		t.Errorf("socket mode = %#o, want 0600", perm)
	}
}

// TestPluginEnvironmentIsMinimal checks that a plugin does not inherit the
// worker's environment, which is where the worker's own credentials live.
// Not parallel: it calls t.Setenv, to prove a variable set in this process does
// not reach a plugin, and the testing package refuses that combination.
func TestPluginEnvironmentIsMinimal(t *testing.T) {
	cfg := testConfig(t, t.TempDir()).withDefaults()
	cfg.Env = []string{
		"SOMETHING=configured",
		// An operator cannot override what the protocol owns.
		"FLOWSTATE_PLUGIN_TOKEN=hijacked",
	}

	env := pluginEnv(cfg, "/tmp/s", "real-token")

	var found []string
	for _, entry := range env {
		found = append(found, entry)
	}
	joined := strings.Join(found, "\n")

	if !strings.Contains(joined, "SOMETHING=configured") {
		t.Errorf("configured environment was not passed: %v", env)
	}
	if !strings.Contains(joined, "FLOWSTATE_PLUGIN_TOKEN=real-token") {
		t.Errorf("the real token was not passed: %v", env)
	}
	if strings.Contains(joined, "hijacked") {
		t.Errorf("an operator-supplied entry overrode a protocol variable: %v", env)
	}

	// Nothing from this process leaked in.
	t.Setenv("A_WORKER_SECRET", "should-not-travel")
	for _, entry := range pluginEnv(cfg, "/tmp/s", "real-token") {
		if strings.HasPrefix(entry, "A_WORKER_SECRET") {
			t.Errorf("the worker's own environment reached the plugin: %q", entry)
		}
	}
}
