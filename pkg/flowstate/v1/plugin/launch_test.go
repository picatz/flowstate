package plugin

import (
	"context"
	"encoding/base64"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/picatz/flowstate/pkg/flowstate/v1/plugin/internal/protocol"
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

		// timeoutIsTheBound marks the one case whose refusal *is* a timeout
		// firing, so the timeout must stay short enough to fire.
		//
		// Every other case here refuses for a reason the plugin gives —
		// garbage on the handshake line, a manifest that does not validate, a
		// descriptor that cannot be reconstructed — and reaches that reason
		// only if the process is allowed to get far enough to say it. Sharing
		// one 3-second handshake timeout across all sixteen made that a race:
		// under CPU contention a perfectly healthy fake plugin does not always
		// get scheduled, exec'd and through its first write inside three
		// seconds, and when it does not, the host refuses it with
		// ErrHandshakeTimeout instead of the ErrHandshake or ErrManifest the
		// case exists to assert. That is this test's flake, and it is a bound
		// being applied where it is not the bound under test.
		//
		// So the timeout is generous everywhere it is incidental and short
		// exactly where it is the point. Generous does not weaken those cases:
		// each still has to produce its own specific sentinel and its own
		// operator-facing message, and a longer timeout only makes it more
		// certain that what it produced is what the case is about rather than
		// the clock running out first.
		timeoutIsTheBound bool
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
			name:              "binary that never handshakes",
			mode:              "silent",
			wantErr:           ErrHandshakeTimeout,
			wantMessage:       "no handshake line within",
			timeoutIsTheBound: true,
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
			wantMessage: "invalid flowstate.plugin.v1.PluginManifest",
		},
		{
			// The schema's own min_items rule catches this one, which is the
			// right place for it: the manifest is refused before any of the
			// host's own reasoning runs.
			name:    "plugin advertising nothing",
			mode:    "no-caps",
			wantErr: ErrManifest,
			// The min_items rule is what refuses this; the exact framing around
			// it is the validator's to phrase and has changed across releases
			// (the field prefix, an "(s)", a trailing rule id), so pin only the
			// clause that names the violated rule, not the sentence it sits in.
			wantMessage: "must contain at least 1 item",
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
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			dir := pluginDir(t, test.mode)
			cfg := testConfig(t, dir)

			// See timeoutIsTheBound. The waiting bounds are incidental
			// machinery for every case but "silent", and a busy machine must
			// not be able to turn one refusal into another.
			if !test.timeoutIsTheBound {
				cfg.HandshakeTimeout = time.Minute
				cfg.DescribeTimeout = time.Minute
			}

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

	// A nil image, which is the launch path for a caller with no digest to
	// protect: there is nothing to open, since the point of the case is a binary
	// that is not there.
	_, err := launch(t.Context(), cfg, Found{
		Name: "ghost",
		Path: filepath.Join(t.TempDir(), "flowstate-plugin-ghost"),
	}, nil)
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

// TestStderrFloodIsRateLimited checks the other half of issue #714's bound: a
// plugin that floods stderr with short lines — never tripping MaxStderrLine,
// which bounds one line's size and nothing about how many arrive — costs the
// host a bounded number of relayed log records rather than one per line.
//
// The pipe still has to be drained regardless, the same as the tolerated
// stdout noise above: a plugin blocked on a full pipe looks exactly like a
// hung one. This plugin writes 20,000 lines; the assertion is that far fewer
// than 20,000 made it into the host's log.
func TestStderrFloodIsRateLimited(t *testing.T) {
	t.Parallel()

	var logged capturedLogs
	cfg := testConfig(t, pluginDir(t, "stderr-flood"))
	cfg.MaxStderrLinesPerMinute = 5
	cfg.Logger = newCapturingLogger(t, &logged)

	host := openHost(t, cfg)

	p, ok := host.Lookup("stderr-flood")
	if !ok {
		t.Fatal("plugin was not launched")
	}

	if health := p.CheckHealth(t.Context()); health.Status != HealthServing {
		t.Fatalf("health = %v, want serving: %v", health.Status, health.Err)
	}

	// Give the flood time to run its course. The fake plugin's flood loop
	// finishes well under a second, but the plugin process then keeps
	// serving rather than exiting — see stderr-flood's source — so nothing
	// here bounds the pump, only the flood.
	time.Sleep(500 * time.Millisecond)

	// Stop the plugin before reading logged: the pump goroutine writes to it
	// concurrently for as long as the plugin's stderr pipe stays open, so any
	// read before this point (relayed count included) races the write. Close
	// waits for the pumps (i.pumps.Wait()), which is also what lets the
	// limiter's owed summary appear at all — the window this test runs in is
	// real-clock minutes long, so it never rolls over on its own, and only
	// closing the host stops the plugin, reaches EOF, and flushes the
	// pending count rather than stranding it (#714's flood-then-quiet
	// follow-up: a crash is a common reason a flooding plugin goes silent,
	// and it is exactly the count an operator most needs to see). Closed
	// explicitly here, ahead of openHost's own t.Cleanup, so every assertion
	// below observes the final log rather than racing it.
	host.Close(t.Context())

	relayed := strings.Count(logged.String(), `msg="plugin log"`)
	if relayed == 0 {
		t.Fatal("no stderr lines were relayed at all, so this proved nothing about the bound")
	}
	if relayed > cfg.MaxStderrLinesPerMinute {
		t.Errorf("relayed %d stderr lines, want at most the configured budget of %d", relayed, cfg.MaxStderrLinesPerMinute)
	}

	if !strings.Contains(logged.String(), "plugin log suppressed") {
		t.Error(`log does not contain "plugin log suppressed" after the plugin was stopped with lines still pending`)
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
		// An operator cannot override what the protocol owns, including the
		// retired name: the host no longer sets FLOWSTATE_PLUGIN_TOKEN, and an
		// operator entry must not be able to put a secret back in the
		// environment block under it.
		"FLOWSTATE_PLUGIN_TOKEN=hijacked",
		"FLOWSTATE_PLUGIN_TOKEN_FD=9",
	}

	env := pluginEnv(cfg, "/tmp/s")

	joined := strings.Join(env, "\n")

	if !strings.Contains(joined, "SOMETHING=configured") {
		t.Errorf("configured environment was not passed: %v", env)
	}
	if want := fmt.Sprintf("%s=%d", protocol.TokenFDEnv, tokenFD); !strings.Contains(joined, want) {
		t.Errorf("the token descriptor was not passed as %q: %v", want, env)
	}
	if strings.Contains(joined, "hijacked") {
		t.Errorf("an operator-supplied entry overrode a protocol variable: %v", env)
	}
	if strings.Contains(joined, protocol.TokenEnv+"=") {
		t.Errorf("the retired %s reached a plugin's environment: %v", protocol.TokenEnv, env)
	}

	// Nothing from this process leaked in.
	t.Setenv("A_WORKER_SECRET", "should-not-travel")
	for _, entry := range pluginEnv(cfg, "/tmp/s") {
		if strings.HasPrefix(entry, "A_WORKER_SECRET") {
			t.Errorf("the worker's own environment reached the plugin: %q", entry)
		}
	}
}

// TestTheEgressGrantReachesEveryPluginAndCannotBeOverriddenByEnv covers the half
// of the grant this package owns: that a policy the deployment configured is in
// the environment of every plugin, under one name, and that the Env list cannot
// quietly become a second place it is written.
//
// The name is not conditioned on which plugin is launching, which is the point —
// a third-party plugin the host has never heard of gets the same grant as the
// first-party ones (#1332). What the plugin does with it is
// [github.com/picatz/flowstate/pkg/flowstate/v1/plugin/sdk.EgressPolicy]'s half.
func TestTheEgressGrantReachesEveryPluginAndCannotBeOverriddenByEnv(t *testing.T) {
	t.Parallel()

	policy := []byte("egress:\n  schemes: [https]\n")

	cfg := testConfig(t, t.TempDir()).withDefaults()
	cfg.EgressPolicy = policy
	cfg.Env = []string{protocol.EgressPolicyEnv + "=" + base64.StdEncoding.EncodeToString([]byte("egress: {}"))}

	granted := grantsIn(t, pluginEnv(cfg, "/tmp/s"))
	if len(granted) != 1 {
		t.Fatalf("%s appears %d times in the launch environment, want exactly one: %v",
			protocol.EgressPolicyEnv, len(granted), granted)
	}

	decoded, err := base64.StdEncoding.DecodeString(granted[0])
	if err != nil {
		t.Fatalf("the grant is not base64: %v", err)
	}
	if string(decoded) != string(policy) {
		t.Errorf("the plugin was granted %q, want the operator's own policy %q", decoded, policy)
	}

	// A host with no policy grants none, rather than an empty one: an empty
	// policy read as "no restrictions" is the failure the SDK's refusal exists
	// to prevent, and it must not be manufactured here.
	cfg.EgressPolicy = nil
	cfg.Env = nil
	if granted := grantsIn(t, pluginEnv(cfg, "/tmp/s")); len(granted) != 0 {
		t.Errorf("a host with no egress policy granted one anyway: %q", granted)
	}
}

// grantsIn returns every value the launch environment carries under the grant's
// name, so a test can tell "absent" from "present and empty" — which is the
// distinction the grant is built on and the one a bare string search cannot see.
func grantsIn(t *testing.T, env []string) []string {
	t.Helper()

	var granted []string
	for _, entry := range env {
		if name, value, _ := strings.Cut(entry, "="); name == protocol.EgressPolicyEnv {
			granted = append(granted, value)
		}
	}

	return granted
}

// TestAnExplicitlyEmptyEgressPolicyIsStillGranted is the case a length check
// silently dropped.
//
// An operator whose --egress-policy names a zero-byte file has configured a
// policy: the worker parses that empty document and registers the http task
// under what it builds. Forwarding the grant only when it had bytes in it made
// every plugin on that worker read the deployment as ungranted, so plugins
// denied while the built-in task allowed — one file, one deployment, two
// answers. Nil is the only thing that means "nothing was configured".
func TestAnExplicitlyEmptyEgressPolicyIsStillGranted(t *testing.T) {
	t.Parallel()

	cfg := testConfig(t, t.TempDir()).withDefaults()
	cfg.EgressPolicy = []byte{}

	granted := grantsIn(t, pluginEnv(cfg, "/tmp/s"))
	if len(granted) != 1 {
		t.Fatalf("an explicitly configured empty policy produced %d grants, want exactly one: %q",
			len(granted), granted)
	}
	if granted[0] != "" {
		t.Errorf("the empty policy was granted as %q, want the empty string", granted[0])
	}

	// withDefaults clones every slice it carries, and the clone has to preserve
	// the distinction: a Config that came through it must still be able to say
	// "empty policy" rather than collapsing to "no policy".
	if cfg.withDefaults().EgressPolicy == nil {
		t.Error("withDefaults turned an explicitly empty policy into no policy at all")
	}
}

// TestAnOversizedEgressPolicyIsRefusedByConfig bounds the grant where it is
// configured rather than where it is spent.
//
// The grant becomes one environment string, and Linux bounds one of those at
// MAX_ARG_STRLEN. Past it, exec fails for every plugin at once with an errno
// naming neither this field nor the operator's file — so the refusal belongs
// here, in terms of the bound and the size, before anything is launched.
func TestAnOversizedEgressPolicyIsRefusedByConfig(t *testing.T) {
	t.Parallel()

	cfg := testConfig(t, t.TempDir())

	// Exactly at the bound is accepted. Without this half, a check that refused
	// everything would pass the half below.
	cfg.EgressPolicy = make([]byte, MaxEgressPolicyBytes)
	if err := cfg.validate(); err != nil {
		t.Fatalf("a policy exactly at the %d-byte bound was refused: %v", MaxEgressPolicyBytes, err)
	}

	cfg.EgressPolicy = make([]byte, MaxEgressPolicyBytes+1)
	err := cfg.validate()
	if err == nil {
		t.Fatalf("a policy one byte over the %d-byte bound was accepted", MaxEgressPolicyBytes)
	}
	if !strings.Contains(err.Error(), strconv.Itoa(MaxEgressPolicyBytes)) {
		t.Errorf("the refusal does not name the bound, so nobody can size a policy to fit it: %v", err)
	}

	// And it is refused by the constructor, not only by the unexported check:
	// an embedding host never calls validate itself.
	if _, err := NewHost(cfg); err == nil {
		t.Error("NewHost accepted a policy over the bound")
	}
}

// TestAPluginTaskNamedLikeABuiltinIsNamespacedNotRefused pins the shape that
// replaced a refusal.
//
// A plugin providing a task called `http` used to be refused as shadowing the
// built-in. The dotted registration makes the collision unrepresentable instead:
// the task registers as `<plugin>.http`, a different name from the built-in's
// bare one, so nothing an author already wrote can change meaning when the
// plugin is installed — which was the whole point of the refusal, held now by
// structure rather than by a check.
func TestAPluginTaskNamedLikeABuiltinIsNamespacedNotRefused(t *testing.T) {
	t.Parallel()

	host := openHost(t, testConfig(t, pluginDir(t, "builtin-task")))

	defs := host.TaskDefs()
	if len(defs) != 1 {
		t.Fatalf("host provides %d tasks, want 1", len(defs))
	}
	if want := "builtin-task.http"; defs[0].Name != want {
		t.Errorf("task name = %q, want %q", defs[0].Name, want)
	}
}
