package plugin

import (
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"

	flowstatev1 "github.com/picatz/flowstate/pkg/flowstate/v1"
)

// This file covers admission: whether a deployment's declared digest decides
// what may answer to a plugin name, and whether the refusal happens early
// enough to be worth anything.
//
// "Early enough" is the part a test has to work for. A pin checked after the
// process is running is a pin a compromised binary has already had a turn
// before — it has executed, it has whatever the exec gave it, and only then is
// it told it was not welcome. So the tests below do not merely assert that
// Open returns an error; [markerPlugin] gives the fixture a way to record that
// it ran at all, and a refusal that leaves no marker is the evidence that
// nothing was executed.

// digestOf hashes a file the way the host does, so a test can pin what it
// installed.
func digestOf(t *testing.T, path string) string {
	t.Helper()

	f, err := os.Open(path)
	if err != nil {
		t.Fatalf("opening %s: %v", path, err)
	}
	defer f.Close()

	digest, err := flowstatev1.ContentDigestOf(f)
	if err != nil {
		t.Fatalf("hashing %s: %v", path, err)
	}

	return digest
}

// markerPlugin installs a plugin binary that creates a file the moment it runs
// and then exits, and returns the search path directory and that marker's path.
//
// It is a shell script rather than a symlink to this test binary because what
// it has to prove is negative — that nothing ran — and the only way to prove
// that is to have something that would leave a trace if it had. It never hand
// shakes, so a launch that reaches exec fails; that failure is the *control*
// case, and it is a different failure from an admission refusal.
func markerPlugin(t *testing.T, name string) (dir, marker string) {
	t.Helper()

	dir = t.TempDir()
	marker = filepath.Join(t.TempDir(), "it-ran")

	script := "#!/bin/sh\n: > " + marker + "\nexit 0\n"
	path := filepath.Join(dir, BinaryPrefix+name)
	if err := os.WriteFile(path, []byte(script), 0o755); err != nil {
		t.Fatalf("writing the marker plugin: %v", err)
	}

	return dir, marker
}

// ran reports whether the marker plugin executed.
func ran(t *testing.T, marker string) bool {
	t.Helper()

	_, err := os.Stat(marker)
	if err == nil {
		return true
	}
	if !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("stat %s: %v", marker, err)
	}

	return false
}

// TestPinnedDigestAdmitsTheDeclaredBinary is the positive direction: a name
// pinned to the digest of the binary that is actually there launches exactly as
// an unpinned one does, and the digest the host then records is the one that was
// pinned — the pin and the measurement are the same value, not two.
func TestPinnedDigestAdmitsTheDeclaredBinary(t *testing.T) {
	t.Parallel()

	dir := pluginDir(t, "ok")
	digest := digestOf(t, filepath.Join(dir, BinaryPrefix+"ok"))

	cfg := testConfig(t, dir)
	cfg.PinnedDigests = map[string]string{"ok": digest}

	host := openHost(t, cfg)

	plugins := host.Plugins()
	if len(plugins) != 1 {
		t.Fatalf("host holds %d plugins, want 1", len(plugins))
	}
	if got := plugins[0].DistributionDigest(); got != digest {
		t.Errorf("recorded digest = %q, want the pinned %q", got, digest)
	}
}

// TestPinnedDigestMismatchIsRefusedBeforeAnythingRuns is the whole point of the
// feature. A name pinned to one digest, a different binary on the path: the
// launch is refused, the refusal names both digests so an operator can tell
// which of the two is wrong, and — the part that makes it admission rather than
// after-the-fact detection — the binary never executed.
func TestPinnedDigestMismatchIsRefusedBeforeAnythingRuns(t *testing.T) {
	t.Parallel()

	dir, marker := markerPlugin(t, "marker")
	actual := digestOf(t, filepath.Join(dir, BinaryPrefix+"marker"))

	// A syntactically perfect pin for some other bytes.
	const wanted = "sha256:" + "1111111111111111111111111111111111111111111111111111111111111111"

	cfg := testConfig(t, dir)
	cfg.PinnedDigests = map[string]string{"marker": wanted}

	host, err := NewHost(cfg)
	if err != nil {
		t.Fatalf("NewHost: %v", err)
	}
	t.Cleanup(func() { host.Close(t.Context()) })

	err = host.Open(t.Context())
	if !errors.Is(err, ErrDigestPin) {
		t.Fatalf("Open error = %v, want one wrapping %v", err, ErrDigestPin)
	}

	// Refused on the pin, and not incidentally on something later that would
	// have refused it anyway: this fixture cannot hand shake, so a check that
	// happened after exec would have produced a handshake failure instead.
	if errors.Is(err, ErrHandshake) || errors.Is(err, ErrHandshakeTimeout) {
		t.Errorf("Open failed at the handshake (%v), so the pin was checked too late", err)
	}

	if !strings.Contains(err.Error(), wanted) {
		t.Errorf("the refusal does not name the expected digest %q: %v", wanted, err)
	}
	if !strings.Contains(err.Error(), actual) {
		t.Errorf("the refusal does not name the digest found, %q: %v", actual, err)
	}

	if ran(t, marker) {
		t.Error("the refused binary executed; a pin checked after exec is not admission control")
	}
}

// TestTheMarkerPluginRunsWhenItIsAdmitted is the control the test above needs.
// Without it, a marker that is absent proves nothing: it would be equally
// absent if the fixture never worked at all. Pinned to its own digest, the same
// binary runs — and then fails, at the handshake, which is what a shell script
// pretending to be a plugin should do.
func TestTheMarkerPluginRunsWhenItIsAdmitted(t *testing.T) {
	t.Parallel()

	dir, marker := markerPlugin(t, "marker")

	cfg := testConfig(t, dir)
	cfg.PinnedDigests = map[string]string{
		"marker": digestOf(t, filepath.Join(dir, BinaryPrefix+"marker")),
	}

	host, err := NewHost(cfg)
	if err != nil {
		t.Fatalf("NewHost: %v", err)
	}
	t.Cleanup(func() { host.Close(t.Context()) })

	if err := host.Open(t.Context()); err == nil {
		t.Fatal("a shell script that never hand shakes was accepted as a plugin")
	} else if errors.Is(err, ErrDigestPin) {
		t.Fatalf("a binary pinned to its own digest was refused admission: %v", err)
	}

	if !ran(t, marker) {
		t.Error("the admitted binary never executed, so the negative test above proves nothing")
	}
}

// TestAnUnpinnedNameIsUnchanged states the cost the feature chooses: pinning is
// opt-in per name, so a deployment that pins one plugin has not thereby pinned
// the others, and a name with no pin is still trust-on-first-use.
func TestAnUnpinnedNameIsUnchanged(t *testing.T) {
	t.Parallel()

	cfg := testConfig(t, pluginDir(t, "ok"))
	cfg.PinnedDigests = map[string]string{
		// A pin for a name this deployment does not have, which must neither
		// refuse the plugin it does have nor be required to match anything.
		"some-other-plugin": "sha256:" + strings.Repeat("a", 64),
	}

	host := openHost(t, cfg)

	if len(host.Plugins()) != 1 {
		t.Fatalf("host holds %d plugins, want the unpinned one to have launched", len(host.Plugins()))
	}
}

// TestMalformedPinsAreRefusedAtConfigLoad checks that a pin that could never
// match is a startup failure rather than a launch-time refusal. A typo that
// surfaces the first time a workflow needs a plugin is an outage that looks
// exactly like a compromise, hours after the change that caused it.
func TestMalformedPinsAreRefusedAtConfigLoad(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		pins map[string]string
		want string
	}{
		{
			name: "no algorithm",
			pins: map[string]string{"ok": strings.Repeat("a", 64)},
			want: "sha256:",
		},
		{
			name: "another algorithm",
			pins: map[string]string{"ok": "sha512:" + strings.Repeat("a", 128)},
			want: "sha256:",
		},
		{
			name: "too short",
			pins: map[string]string{"ok": "sha256:abc"},
			want: "hex characters",
		},
		{
			name: "too long",
			pins: map[string]string{"ok": "sha256:" + strings.Repeat("a", 65)},
			want: "hex characters",
		},
		{
			name: "upper case",
			pins: map[string]string{"ok": "sha256:" + strings.Repeat("A", 64)},
			want: "lower-case",
		},
		{
			name: "not hexadecimal",
			pins: map[string]string{"ok": "sha256:" + strings.Repeat("z", 64)},
			want: "lower-case",
		},
		{
			name: "empty digest",
			pins: map[string]string{"ok": ""},
			want: "sha256:",
		},
		{
			name: "empty name",
			pins: map[string]string{"": "sha256:" + strings.Repeat("a", 64)},
			want: "empty plugin name",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()

			cfg := testConfig(t, pluginDir(t, "ok"))
			cfg.PinnedDigests = test.pins

			_, err := NewHost(cfg)
			if !errors.Is(err, ErrDigestPin) {
				t.Fatalf("NewHost error = %v, want one wrapping %v", err, ErrDigestPin)
			}
			if !strings.Contains(err.Error(), test.want) {
				t.Errorf("NewHost error = %q, want it to mention %q", err.Error(), test.want)
			}
		})
	}
}

// TestAPinnedNameWithNoMeasuredImageIsRefused is the fail-closed direction for
// the launch path that carries no image at all. Nothing outside this package
// takes it for a discovered plugin, but it exists, and a pinned name reaching it
// must refuse rather than admit a binary that nothing measured.
func TestAPinnedNameWithNoMeasuredImageIsRefused(t *testing.T) {
	t.Parallel()

	dir, marker := markerPlugin(t, "marker")

	cfg := testConfig(t, dir).withDefaults()
	cfg.PinnedDigests = map[string]string{
		// Even the *correct* digest for this file is refused, because a launch
		// with no image is a launch with nothing to compare it against.
		"marker": digestOf(t, filepath.Join(dir, BinaryPrefix+"marker")),
	}

	_, err := launch(t.Context(), cfg, Found{
		Name: "marker",
		Path: filepath.Join(dir, BinaryPrefix+"marker"),
	}, nil)
	if !errors.Is(err, ErrDigestPin) {
		t.Fatalf("launch error = %v, want one wrapping %v", err, ErrDigestPin)
	}
	if ran(t, marker) {
		t.Error("an unmeasured binary executed under a pinned name")
	}
}
