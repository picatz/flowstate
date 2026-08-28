package main

import (
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	v1 "github.com/picatz/flowstate/pkg/flowstate/v1"
	"github.com/picatz/flowstate/pkg/flowstate/v1/plugin"
)

// #1010's operator surface: --plugin-pin and --plugin-pins feed
// plugin.Config.PinnedDigests, which until this file was complete, tested,
// checked pre-exec — and reachable from nothing a deployment could type.
//
// The flag-parsing and file-parsing cases below exercise pluginFlagsOf
// directly through `flow plugins`, which builds a host the same way every
// other verb does (pluginFlags.host is the one call site). The end-to-end
// cases at the bottom compile the repo's own example plugin and run it for
// real, because a refusal a test only asserts against a struct is a refusal
// nothing proves reaches the process boundary.

// hex64Digest returns a syntactically valid "sha256:" pin of the given byte,
// for a fixture that only needs to parse, never to match a real binary.
func hex64Digest(c byte) string {
	b := make([]byte, 64)
	for i := range b {
		b[i] = c
	}
	return v1.ContentDigestPrefix + string(b)
}

// TestPluginPinFlagIsWiredOnEveryVerb confirms --plugin-pin and --plugin-pins
// reach every command that builds a host through pluginFlags.host — the
// single call site #1010 names — the same way [TestTheWorkerTakesThePluginFlags]
// confirms the older plugin flags do.
func TestPluginPinFlagIsWiredOnEveryVerb(t *testing.T) {
	for _, path := range [][]string{{"worker"}, {"server"}, {"plugins"}, {"lsp"}, {"mcp"}, {"run", "local"}} {
		cmd := flowCommand(t, path...)

		for _, name := range []string{"plugin-pin", "plugin-pins"} {
			assert.NotNil(t, cmd.Flags().Lookup(name),
				"`flow %s` does not take --%s", strings.Join(path, " "), name)
		}
	}
}

// TestPluginPinFlagRefusesAMalformedEntry covers the flag's own shape: no "="
// at all is refused before anything about digests is even considered.
func TestPluginPinFlagRefusesAMalformedEntry(t *testing.T) {
	res := runFlow(t, "plugins", "--plugin-dir", t.TempDir(), "--plugin-pin", "not-a-pin")
	require.Error(t, res.Err)
	assert.Contains(t, res.Err.Error(), "not of the form name=sha256:hex")
}

// TestPluginPinFlagRefusesAMalformedDigestAtStartup is the fail-closed-on-config
// case CLAUDE.md asks for: a pin that reaches [plugin.Config] but can never
// match any binary is refused when the host is built, by the package's own
// [plugin.ErrDigestPin] — not redecided here, only surfaced. This is also the
// proof that a --plugin-pin flag actually reaches Config.PinnedDigests: the
// refusal cannot fire from a name-only parse.
func TestPluginPinFlagRefusesAMalformedDigestAtStartup(t *testing.T) {
	res := runFlow(t, "plugins", "--plugin-dir", t.TempDir(), "--plugin-pin", "example=sha256:tooshort")
	require.Error(t, res.Err)
	assert.Contains(t, res.Err.Error(), "hex characters",
		"the malformed pin was not refused by the package's own validation")
}

// TestPluginPinFlagRefusesADuplicateName is the ambiguity CLAUDE.md's "one
// value written down twice" pattern warns about, arriving through two
// instances of one flag instead of two code paths.
func TestPluginPinFlagRefusesADuplicateName(t *testing.T) {
	res := runFlow(t, "plugins", "--plugin-dir", t.TempDir(),
		"--plugin-pin", "example="+hex64Digest('a'),
		"--plugin-pin", "example="+hex64Digest('b'))
	require.Error(t, res.Err)
	assert.Contains(t, res.Err.Error(), "more than once")
}

// TestPluginPinFlagRefusesWithNoSearchPath mirrors the existing --plugin
// refusal ([TestTheLanguageServerKeepsThePluginPinRefusal]): a pin the host
// that would check it never opens is not silently skipped.
func TestPluginPinFlagRefusesWithNoSearchPath(t *testing.T) {
	res := runFlow(t, "plugins", "--plugin-pin", "example="+hex64Digest('a'))
	require.Error(t, res.Err)
	assert.Contains(t, res.Err.Error(), "nowhere to look")
}

// TestPluginPinFlagRefusesANameOnlyExcludes: a pin on a name --plugin does not
// admit can never be checked, since [plugin.Config.Only] refuses the name
// before [plugin.Config.PinnedDigests] is ever consulted.
func TestPluginPinFlagRefusesANameOnlyExcludes(t *testing.T) {
	res := runFlow(t, "plugins", "--plugin-dir", t.TempDir(),
		"--plugin", "other",
		"--plugin-pin", "example="+hex64Digest('a'))
	require.Error(t, res.Err)
	assert.Contains(t, res.Err.Error(), "does not admit")
}

// TestPluginPinsFileMergesWithTheFlag: the file is the base, --plugin-pin
// extends it, and each is independently readable through the host it built —
// asserted here by giving the file an unrelated valid pin, and the flag a
// deliberately malformed digest, so only [plugin.ErrDigestPin] on the flag's
// entry can be what fails.
func TestPluginPinsFileMergesWithTheFlag(t *testing.T) {
	dir := t.TempDir()
	pinsFile := filepath.Join(dir, "pins.yaml")
	require.NoError(t, os.WriteFile(pinsFile, []byte(
		"pins:\n  ghost: "+hex64Digest('a')+"\n"), 0o644))

	res := runFlow(t, "plugins", "--plugin-dir", t.TempDir(),
		"--plugin-pins", pinsFile,
		"--plugin-pin", "example=sha256:tooshort")
	require.Error(t, res.Err)
	assert.Contains(t, res.Err.Error(), "hex characters",
		"the file's own valid pin should have let parsing succeed up to the flag's malformed one")
}

// TestPluginPinsFileAndFlagConflictOnTheSameNameIsRefused: a name pinned by
// both the file and the flag is ambiguous even when nothing about either pin
// is individually malformed.
func TestPluginPinsFileAndFlagConflictOnTheSameNameIsRefused(t *testing.T) {
	dir := t.TempDir()
	pinsFile := filepath.Join(dir, "pins.yaml")
	require.NoError(t, os.WriteFile(pinsFile, []byte(
		"pins:\n  example: "+hex64Digest('a')+"\n"), 0o644))

	res := runFlow(t, "plugins", "--plugin-dir", t.TempDir(),
		"--plugin-pins", pinsFile,
		"--plugin-pin", "example="+hex64Digest('b'))
	require.Error(t, res.Err)
	assert.Contains(t, res.Err.Error(), "--plugin-pins and --plugin-pin both pin")
}

// TestPluginPinsFileRefusesAnUnknownKeyAtStartup: a misspelled top-level key
// in the pins file fails the command that loaded it, rather than pinning
// fewer plugins than the file's author wrote.
func TestPluginPinsFileRefusesAnUnknownKeyAtStartup(t *testing.T) {
	dir := t.TempDir()
	pinsFile := filepath.Join(dir, "pins.yaml")
	require.NoError(t, os.WriteFile(pinsFile, []byte(
		"pinns:\n  example: "+hex64Digest('a')+"\n"), 0o644))

	res := runFlow(t, "plugins", "--plugin-dir", t.TempDir(), "--plugin-pins", pinsFile)
	require.Error(t, res.Err)
	assert.Contains(t, res.Err.Error(), "parsing plugin pins")
}

// TestPluginPinsFileRefusesWithNoSearchPath is the file form of
// [TestPluginPinFlagRefusesWithNoSearchPath].
func TestPluginPinsFileRefusesWithNoSearchPath(t *testing.T) {
	dir := t.TempDir()
	pinsFile := filepath.Join(dir, "pins.yaml")
	require.NoError(t, os.WriteFile(pinsFile, []byte(
		"pins:\n  example: "+hex64Digest('a')+"\n"), 0o644))

	res := runFlow(t, "plugins", "--plugin-pins", pinsFile)
	require.Error(t, res.Err)
	assert.Contains(t, res.Err.Error(), "nowhere to look")
}

// The end-to-end cases. Slow — they compile the repo's own example plugin —
// and skipped under -short exactly as [buildExamplePluginDir] already is.

// digestOfFile hashes a binary the way an operator would with sha256sum,
// which docs/DEPLOYMENT.md and Config.PinnedDigests's own doc comment both
// point to as the recipe.
func digestOfFile(t *testing.T, path string) string {
	t.Helper()

	f, err := os.Open(path)
	require.NoError(t, err)
	defer f.Close()

	digest, err := v1.ContentDigestOf(f)
	require.NoError(t, err)

	return digest
}

// TestAPinnedMismatchedPluginRefusesAtDiscovery is decision 2's fail-closed
// promise, exercised through the real CLI rather than the plugin package's
// own unit tests: a pin that does not match refuses before the run's
// workflow gets to execute at all.
func TestAPinnedMismatchedPluginRefusesAtDiscovery(t *testing.T) {
	dir := buildExamplePluginDir(t)
	t.Setenv("FLOWSTATE_SECRET_GREET_TOKEN", "material")

	wrong := hex64Digest('0')

	stdout, stderr, err := runLocalFile(t, exampleGreetWorkflow,
		"--plugin-dir", dir,
		"--plugin-pin", "example="+wrong,
		"--secret-env", "GREET_TOKEN",
		"--auth-policy", localSecretPolicy(t),
		"--output", "json")
	require.Error(t, err, "a digest-mismatched plugin was admitted")
	assert.Contains(t, stderr, "refusing",
		"the refusal is not the package's own admission error")
	assert.NotContains(t, stdout, "Hello, world!",
		"the pinned-mismatched plugin ran before being refused")
}

// TestAPinnedMatchingPluginRunsExactlyAsAnUnpinnedOne is the positive half:
// the correct digest, computed the way an operator would, admits the plugin
// and the workflow completes identically to the unpinned case
// ([TestRunLocalExecutesAPluginTaskFromAnExample]).
func TestAPinnedMatchingPluginRunsExactlyAsAnUnpinnedOne(t *testing.T) {
	dir := buildExamplePluginDir(t)
	t.Setenv("FLOWSTATE_SECRET_GREET_TOKEN", "material")

	digest := digestOfFile(t, filepath.Join(dir, plugin.BinaryPrefix+"example"))

	stdout, stderr, err := runLocalFile(t, exampleGreetWorkflow,
		"--plugin-dir", dir,
		"--plugin-pin", "example="+digest,
		"--secret-env", "GREET_TOKEN",
		"--auth-policy", localSecretPolicy(t),
		"--output", "json")
	require.NoError(t, err, stderr)
	assert.Contains(t, stdout, "Hello, world!")
}

// TestAnUnpinnedPluginRunsUnaffectedWhenAnotherNameIsPinned is the CLI-level
// version of the plugin package's own TestAnUnpinnedNameIsUnchanged: pinning
// is opt-in per name, so a deployment that has pinned something else entirely
// must not find its other plugins refused.
func TestAnUnpinnedPluginRunsUnaffectedWhenAnotherNameIsPinned(t *testing.T) {
	dir := buildExamplePluginDir(t)
	t.Setenv("FLOWSTATE_SECRET_GREET_TOKEN", "material")

	stdout, stderr, err := runLocalFile(t, exampleGreetWorkflow,
		"--plugin-dir", dir,
		"--plugin-pin", "ghost="+hex64Digest('a'),
		"--secret-env", "GREET_TOKEN",
		"--auth-policy", localSecretPolicy(t),
		"--output", "json")
	require.NoError(t, err, stderr,
		"pinning an unrelated name refused a plugin that was never pinned")
	assert.Contains(t, stdout, "Hello, world!")
}
