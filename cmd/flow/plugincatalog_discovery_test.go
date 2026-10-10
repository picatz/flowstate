package main

import (
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// Discovery of plugins.lock.json (item 9 of the DSL critique): the nearest
// lock above the files named is the catalog, the way go finds go.mod. These
// cases hold the walk to its contract, in both directions: what it finds, and
// every condition under which it must refuse, stand down, or stop.

func writeLock(t *testing.T, dir string) string {
	t.Helper()

	require.NoError(t, os.MkdirAll(dir, 0o755))
	path := filepath.Join(dir, pluginLockName)
	require.NoError(t, os.WriteFile(path, []byte(`{"claimsSchemaVersion": 5}`), 0o600))

	return path
}

func TestDiscoveryFindsTheNearestLockAboveTheFile(t *testing.T) {
	t.Parallel()

	root := t.TempDir()
	outer := writeLock(t, root)
	inner := writeLock(t, filepath.Join(root, "a", "b"))
	deep := filepath.Join(root, "a", "b", "c")
	require.NoError(t, os.MkdirAll(deep, 0o755))
	sibling := filepath.Join(root, "a", "x")
	require.NoError(t, os.MkdirAll(sibling, 0o755))

	for name, tc := range map[string]struct {
		anchor string
		want   string
	}{
		"a file whose directory is below the nearest lock": {filepath.Join(deep, "f.yaml"), inner},
		"a directory holding the lock":                     {filepath.Join(root, "a", "b"), inner},
		"a directory below the lock":                       {deep, inner},
		"a file that does not exist yet":                   {filepath.Join(deep, "unsaved.yaml"), inner},
		"a sibling tree reaches the outer lock":            {filepath.Join(sibling, "f.yaml"), outer},
	} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()

			got, err := discoverPluginLock([]string{tc.anchor})
			require.NoError(t, err)
			assert.Equal(t, tc.want, got)
		})
	}
}

func TestDiscoveryFindsNothingWhereThereIsNoLock(t *testing.T) {
	t.Parallel()

	root := t.TempDir()
	got, err := discoverPluginLock([]string{filepath.Join(root, "f.yaml"), "-", ""})
	require.NoError(t, err)
	assert.Empty(t, got, "no lock is today's behavior: nothing is read")

	got, err = discoverPluginLock(nil)
	require.NoError(t, err)
	assert.Empty(t, got)
}

// The walk is bounded: a lock past maxPluginLockDepth directories is not
// reached, and one within it is.
func TestDiscoveryStopsAtItsDepthBound(t *testing.T) {
	t.Parallel()

	root := t.TempDir()
	lock := writeLock(t, root)

	within := root
	for range maxPluginLockDepth - 1 {
		within = filepath.Join(within, "d")
	}
	require.NoError(t, os.MkdirAll(within, 0o755))
	got, err := discoverPluginLock([]string{filepath.Join(within, "f.yaml")})
	require.NoError(t, err)
	assert.Equal(t, lock, got, "a lock exactly at the bound is found")

	beyond := filepath.Join(within, "d")
	require.NoError(t, os.MkdirAll(beyond, 0o755))
	got, err = discoverPluginLock([]string{filepath.Join(beyond, "f.yaml")})
	require.NoError(t, err)
	assert.Empty(t, got, "a lock one directory past the bound is not searched for")
}

func TestDiscoveryRefusesALockItWouldHaveToFollowOrCannotRead(t *testing.T) {
	t.Parallel()

	t.Run("symbolic link", func(t *testing.T) {
		t.Parallel()

		outside := writeLock(t, t.TempDir())
		tree := t.TempDir()
		require.NoError(t, os.Symlink(outside, filepath.Join(tree, pluginLockName)))

		_, err := discoverPluginLock([]string{filepath.Join(tree, "f.yaml")})
		require.Error(t, err, "a symlinked lock was followed out of the tree")
		assert.Contains(t, err.Error(), "symbolic link")
		assert.Contains(t, err.Error(), filepath.Join(tree, pluginLockName))
	})

	t.Run("not a regular file", func(t *testing.T) {
		t.Parallel()

		tree := t.TempDir()
		require.NoError(t, os.Mkdir(filepath.Join(tree, pluginLockName), 0o755))
		// A valid lock above must not be reached past the broken one.
		writeLock(t, filepath.Dir(tree))

		_, err := discoverPluginLock([]string{filepath.Join(tree, "f.yaml")})
		require.Error(t, err)
		assert.Contains(t, err.Error(), "not a regular file")
	})
}

func TestDiscoveryRefusesFilesGovernedByDifferentLocks(t *testing.T) {
	t.Parallel()

	root := t.TempDir()
	one := writeLock(t, filepath.Join(root, "one"))
	two := writeLock(t, filepath.Join(root, "two"))
	bare := filepath.Join(root, "bare")
	require.NoError(t, os.MkdirAll(bare, 0o755))

	_, err := discoverPluginLock([]string{filepath.Join(root, "one", "f.yaml"), filepath.Join(root, "two", "g.yaml")})
	require.Error(t, err)
	assert.Contains(t, err.Error(), one)
	assert.Contains(t, err.Error(), two)

	_, err = discoverPluginLock([]string{filepath.Join(root, "one", "f.yaml"), filepath.Join(bare, "g.yaml")})
	require.Error(t, err, "some files under a lock and some under none were silently checked against the lock")

	got, err := discoverPluginLock([]string{filepath.Join(root, "one", "f.yaml"), filepath.Join(root, "one", "sub", "g.yaml"), "-"})
	require.NoError(t, err)
	assert.Equal(t, one, got, "files under one lock, and standard input, agree")
}

// A lock that is present but unusable fails the command, naming the lock, and
// is never searched past or treated as absent.
func TestLockDiscoveryReportsAnInvalidLockInsteadOfIgnoringIt(t *testing.T) {
	t.Parallel()

	tree := t.TempDir()
	lock := filepath.Join(tree, pluginLockName)
	require.NoError(t, os.WriteFile(lock, []byte(`{"nope": 1}`), 0o600))

	err := (&lockDiscovery{}).discover(filepath.Join(tree, "f.yaml"))
	require.Error(t, err)
	assert.Contains(t, err.Error(), lock)

	assert.NoError(t, (&lockDiscovery{}).discover(filepath.Join(t.TempDir(), "f.yaml")), "no lock is not an error")
}

// The CLI half, through the shipped binary: the shipped plugin examples are
// green with no flags (the critique's (a) and (b)), a flag beats the lock, an
// invalid lock fails the command, and with no lock the answer is unchanged.
func TestShippedPluginExamplesValidateAndTestWithNoFlags(t *testing.T) {
	bin := buildFlowBinary(t)

	output, err := runFlowCapturing(t, bin, "validate", "../../examples/plugins/slack/approval.yaml")
	require.NoError(t, err, "the shipped example is red by default:\n%s", output)
	assert.NotContains(t, output, "incomplete")
	assert.NotContains(t, output, "no plugin task")

	output, err = runFlowCapturing(t, bin, "test", "../../examples/plugins/slack/")
	require.NoError(t, err, "flow test needs an explicit --%s for a shipped example:\n%s", pluginCatalogFlag, output)
}

func TestValidateDiscoversAPluginLockNextToTheFile(t *testing.T) {
	bin := buildFlowBinary(t)
	catalog := pluginCatalogFor(t, bin)

	source, err := os.ReadFile(exampleGreetWorkflow)
	require.NoError(t, err)
	data, err := os.ReadFile(catalog)
	require.NoError(t, err)

	tree := t.TempDir()
	sub := filepath.Join(tree, "sub")
	require.NoError(t, os.MkdirAll(sub, 0o755))
	file := filepath.Join(sub, "workflow.yaml")
	require.NoError(t, os.WriteFile(file, source, 0o600))

	// No lock: today's answer, the installation question.
	output, err := runFlowCapturing(t, bin, "validate", file)
	require.Error(t, err, output)
	assert.Contains(t, output, `no plugin task "example.greet" is registered here`)

	// A lock in an ancestor: discovered.
	require.NoError(t, os.WriteFile(filepath.Join(tree, pluginLockName), data, 0o600))
	output, err = runFlowCapturing(t, bin, "validate", file)
	require.NoError(t, err, "the lock above the file was not discovered:\n%s", output)

	// An explicit flag wins, even over a good lock: a catalog that carries no
	// such task makes the same file unknown again.
	empty := filepath.Join(t.TempDir(), "empty.json")
	require.NoError(t, os.WriteFile(empty, []byte(`{"claimsSchemaVersion": 5}`), 0o600))
	output, err = runFlowCapturing(t, bin, "validate", "--"+pluginCatalogFlag, empty, file)
	require.Error(t, err, "the discovered lock beat the flag:\n%s", output)
	assert.Contains(t, output, `no plugin task "example.greet"`)

	// An invalid lock next to the file fails the command, naming the lock, and
	// is not reported as the file's tasks being unknown.
	require.NoError(t, os.WriteFile(filepath.Join(sub, pluginLockName), []byte(`{"nope": 1}`), 0o600))
	output, err = runFlowCapturing(t, bin, "validate", file)
	require.Error(t, err, "an invalid lock was ignored:\n%s", output)
	assert.Contains(t, output, filepath.Join(sub, pluginLockName))
	assert.False(t, strings.Contains(output, "no plugin task"),
		"an invalid lock was reported as unknown tasks:\n%s", output)

	// ...but the flag still wins over a broken lock, because nothing is read.
	output, err = runFlowCapturing(t, bin, "validate", "--"+pluginCatalogFlag, catalog, file)
	require.NoError(t, err, output)
}

func TestLockDiscoveryBoundsTheLocksItRegisters(t *testing.T) {
	d := &lockDiscovery{}
	for i := range maxDiscoveredLocks {
		lock := writeLock(t, filepath.Join(t.TempDir(), "w"))
		require.NoError(t, d.discover(filepath.Join(filepath.Dir(lock), "f.yaml")), "lock %d", i)
	}

	extra := writeLock(t, filepath.Join(t.TempDir(), "w"))
	err := d.discover(filepath.Join(filepath.Dir(extra), "f.yaml"))
	require.Error(t, err, "a lock past the cap was registered")
	assert.Contains(t, err.Error(), extra)
}
