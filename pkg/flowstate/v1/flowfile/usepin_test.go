package flowfile_test

import (
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"

	v1 "github.com/picatz/flowstate/pkg/flowstate/v1"
	"github.com/picatz/flowstate/pkg/flowstate/v1/flowfile"
)

// A `use:` entry's `digest:` is the same contract as a `call:` step's: the content
// hash of the file, verified against the bytes the compiler read, before it
// compiles them. These tests hold the module spelling to the call's, and to the
// properties a pin exists for: it fails closed, it is strict about what it accepts,
// and it is checked on the bytes that are then parsed.

// useWithPin spells the `use:` block of useIds with a pin; the digest's value sits
// on line 6, column 13 of workflowUsing's document.
func useWithPin(pin string) string {
	return "use:\n  ids:\n    path: ./lib/ids.yaml\n    digest: " + pin + "\n"
}

// pinProblems compiles dir/name and returns the diagnostics the compile refused it
// with, failing the test when it did not refuse.
func pinProblems(t *testing.T, dir, name string) flowfile.Diagnostics {
	t.Helper()

	_, err := compileAt(t, dir, name)
	require.Error(t, err)

	var ds flowfile.Diagnostics
	require.True(t, errors.As(err, &ds), "a refused pin is reported as diagnostics: %v", err)

	return ds
}

func TestAUsePinThatMatchesCompilesAndRecordsTheDigest(t *testing.T) {
	t.Parallel()

	pin := digestOf(t, idsModule)
	dir := tree(t, map[string]string{"w.yaml": workflowUsing(useWithPin(pin), ""), "lib/ids.yaml": idsModule})

	wf, err := compileAt(t, dir, "w.yaml")
	require.NoError(t, err)
	require.Len(t, wf.GetModules(), 1)
	assert.Equal(t, pin, wf.GetModules()[0].GetSourceDigest(), "the pin and the recorded digest are one string from one read")
	assert.Equal(t, v1.ContentDigest([]byte(idsModule)), wf.GetModules()[0].GetSourceDigest())

	// A pin is a property of the file and not of the run: the same workflow with no
	// pin compiles to the same specification.
	plain := tree(t, map[string]string{"w.yaml": workflowUsing(useIds, ""), "lib/ids.yaml": idsModule})
	unpinned, err := compileAt(t, plain, "w.yaml")
	require.NoError(t, err)
	// SourceDigest is the root file's own bytes, which differ by the pin's line.
	wf.SourceDigest, unpinned.SourceDigest = "", ""
	assert.True(t, proto.Equal(wf, unpinned), "a matching pin adds nothing to the compiled workflow")
}

func TestAnUnpinnedUseStillRecordsTheDigest(t *testing.T) {
	t.Parallel()

	dir := tree(t, map[string]string{"w.yaml": workflowUsing(useIds, ""), "lib/ids.yaml": idsModule})
	wf, err := compileAt(t, dir, "w.yaml")
	require.NoError(t, err)
	require.Len(t, wf.GetModules(), 1)
	assert.Equal(t, digestOf(t, idsModule), wf.GetModules()[0].GetSourceDigest())
}

func TestAMismatchedUsePinIsRefusedWithTheDigestToAdopt(t *testing.T) {
	t.Parallel()

	stale := digestOf(t, "something the module used to say\n")
	dir := tree(t, map[string]string{"w.yaml": workflowUsing(useWithPin(stale), ""), "lib/ids.yaml": idsModule})

	ds := pinProblems(t, dir, "w.yaml")
	require.Len(t, ds, 1, "one refusal, not a page of what the unauthorised module would have said")
	d := ds[0]
	assert.Equal(t, v1.DiagnosticCodeModulePinMismatch, d.Code)
	assert.Equal(t, 6, d.Line, "positioned at the digest")
	assert.Equal(t, 13, d.Column)
	for _, want := range []string{"ids", "./lib/ids.yaml", stale, digestOf(t, idsModule), "flow fix --repin"} {
		assert.Contains(t, d.Message, want, "the refusal names the alias, the path, both digests and the repin")
	}
}

func TestAnUpperCaseUsePinIsTheSamePin(t *testing.T) {
	t.Parallel()

	pin := "SHA256:" + strings.ToUpper(strings.TrimPrefix(digestOf(t, idsModule), "sha256:"))
	dir := tree(t, map[string]string{"w.yaml": workflowUsing(useWithPin(pin), ""), "lib/ids.yaml": idsModule})

	wf, err := compileAt(t, dir, "w.yaml")
	require.NoError(t, err)
	assert.Equal(t, digestOf(t, idsModule), wf.GetModules()[0].GetSourceDigest(), "recorded in the one form this tree writes")
}

// TestAUsePinIsStrict is the other direction of the case test above: nothing but
// the one spelling, folded for case, verifies. A pin that is a prefix of the digest,
// padded, from another algorithm or in another script must fail closed with the same
// code, because a comparison that forgives any of those is a comparison an attacker
// who controls the module can aim at.
func TestAUsePinIsStrict(t *testing.T) {
	t.Parallel()

	actual := digestOf(t, idsModule)
	hex := strings.TrimPrefix(actual, "sha256:")

	for name, pin := range map[string]string{
		"a prefix of the digest":    actual[:len(actual)-1],
		"a longer string":           actual + "0",
		"surrounded by spaces":      `" ` + actual + ` "`,
		"another algorithm":         "sha512:" + hex,
		"no algorithm":              hex,
		"a trailing newline":        `"` + actual + `\n"`,
		"empty":                     `""`,
		"a fullwidth colon":         "sha256：" + hex,
		"a lookalike letter":        "sha256:" + strings.Replace(hex, hex[:1], "а", 1),
		"the digest of other bytes": digestOf(t, idsModule+"\n"),
	} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()

			dir := tree(t, map[string]string{"w.yaml": workflowUsing(useWithPin(pin), ""), "lib/ids.yaml": idsModule})
			ds := pinProblems(t, dir, "w.yaml")
			require.NotEmpty(t, ds)
			assert.Equal(t, v1.DiagnosticCodeModulePinMismatch, ds[0].Code, ds[0].Message)
		})
	}
}

// TestAUsePinIsCheckedBeforeTheModuleIsCompiled pins fail-closed: a module whose
// bytes the author has not authorised is not compiled, so its own errors are not
// reported as though the author had asked for it, and nothing it declares is used.
func TestAUsePinThatIsNotTextIsRefused(t *testing.T) {
	t.Parallel()

	dir := tree(t, map[string]string{"w.yaml": workflowUsing(useWithPin("12345"), ""), "lib/ids.yaml": idsModule})
	ds := pinProblems(t, dir, "w.yaml")
	assert.Contains(t, ds[0].Message, "must be a string")
	assert.Equal(t, v1.DiagnosticCodeModulePinMismatch, ds[0].Code, "a pin that is not text is a pin that does not verify")
}

func TestAUsePinIsCheckedBeforeTheModuleIsCompiled(t *testing.T) {
	t.Parallel()

	broken := strings.Replace(idsModule, "returns: bool", "returns: bogus", 1)
	dir := tree(t, map[string]string{
		"w.yaml":       workflowUsing(useWithPin(digestOf(t, idsModule)), ""),
		"lib/ids.yaml": broken,
	})

	ds := pinProblems(t, dir, "w.yaml")
	require.Len(t, ds, 1)
	assert.Equal(t, v1.DiagnosticCodeModulePinMismatch, ds[0].Code)
	assert.NotContains(t, ds[0].Message, "failed to compile")
}

func TestEachUseEntryIsHeldToItsOwnPin(t *testing.T) {
	t.Parallel()

	good := digestOf(t, idsModule)
	bad := digestOf(t, "not the module\n")
	use := "use:\n  ids:\n    path: ./lib/ids.yaml\n    digest: " + good + "\n  same:\n    path: ./lib/ids.yaml\n    digest: " + bad + "\n"
	dir := tree(t, map[string]string{"w.yaml": workflowUsing(use, ""), "lib/ids.yaml": idsModule})

	ds := pinProblems(t, dir, "w.yaml")
	require.Len(t, ds, 1, "the entry that is right is not reported, and the entry that is wrong is, though the module is already loaded")
	assert.Contains(t, ds[0].Message, "module same")
	assert.Equal(t, 9, ds[0].Line)

	// And the reverse order: the first entry loads the module unpinned, so the
	// second one's pin is checked against a cached read and not skipped.
	use = "use:\n  ids:\n    path: ./lib/ids.yaml\n  same:\n    path: ./lib/ids.yaml\n    digest: " + bad + "\n"
	dir = tree(t, map[string]string{"w.yaml": workflowUsing(use, ""), "lib/ids.yaml": idsModule})
	ds = pinProblems(t, dir, "w.yaml")
	require.Len(t, ds, 1)
	assert.Equal(t, v1.DiagnosticCodeModulePinMismatch, ds[0].Code)
}

func TestAModuleUsedByAModuleCanBePinned(t *testing.T) {
	t.Parallel()

	inner := "edition: " + flowfile.CurrentEdition + "\nname: inner\nerrors:\n  Bad: {}\n"
	outer := func(pin string) string {
		return "edition: " + flowfile.CurrentEdition + "\nname: outer\nuse:\n  inner:\n    path: ./inner.yaml\n    digest: " + pin + "\ntypes:\n  Wrapper:\n    fields:\n      s: {type: string}\n"
	}
	w := workflowUsing("use:\n  outer:\n    path: ./outer.yaml\n", "")

	dir := tree(t, map[string]string{"w.yaml": w, "outer.yaml": outer(digestOf(t, inner)), "inner.yaml": inner})
	_, err := compileAt(t, dir, "w.yaml")
	require.NoError(t, err)

	dir = tree(t, map[string]string{"w.yaml": w, "outer.yaml": outer(digestOf(t, "other\n")), "inner.yaml": inner})
	_, err = compileAt(t, dir, "w.yaml")
	require.Error(t, err)
	assert.Contains(t, err.Error(), "module inner")
}

// TestTheDigestOfAUseEntryAloneIsStillRefused keeps the old rule for the shapes it
// still applies to: a `digest:` is a key of a `use:` entry and nowhere else in it.
func TestAUseEntryStillRefusesKeysItDoesNotKnow(t *testing.T) {
	t.Parallel()

	dir := tree(t, map[string]string{
		"w.yaml":       workflowUsing("use:\n  ids:\n    path: ./lib/ids.yaml\n    version: 2\n", ""),
		"lib/ids.yaml": idsModule,
	})
	_, err := compileAt(t, dir, "w.yaml")
	require.Error(t, err)
	assert.Contains(t, err.Error(), "version")
}

// TestAHandBuiltSpecRecordsOnlyCanonicalDigests is the server's door: a
// specification that never was a Flowfile carries no module bytes to hash, so the
// contract it can be held to is the shape of what it records, and that is strict.
func TestAHandBuiltSpecRecordsOnlyCanonicalDigests(t *testing.T) {
	t.Parallel()

	good := digestOf(t, idsModule)
	for name, digest := range map[string]string{
		"upper case":   strings.ToUpper(good),
		"a prefix":     good[:len(good)-2],
		"another hash": "sha512:" + strings.TrimPrefix(good, "sha256:"),
		"empty":        "",
		"padded":       " " + good,
	} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()

			wf := &v1.Workflow{Modules: []*v1.Module{{Alias: "ids", Source: "./lib/ids.yaml", SourceDigest: digest}}}
			require.Error(t, v1.CheckModules(wf))
			require.Error(t, v1.CheckDeclarationTypes(wf), "the door a submitted specification enters through")
			assert.NotEmpty(t, flowfile.Validate(wf), "and the validator a hand-built one is held to")
		})
	}

	ok := &v1.Workflow{Modules: []*v1.Module{{Alias: "ids", Source: "./lib/ids.yaml", SourceDigest: good}}}
	assert.NoError(t, v1.CheckModules(ok))
}

func TestCanonicalContentDigest(t *testing.T) {
	t.Parallel()

	good := digestOf(t, "x")
	got, err := v1.CanonicalContentDigest(strings.ToUpper(good))
	require.NoError(t, err)
	assert.Equal(t, good, got)

	for _, bad := range []string{"", good + " ", " " + good, good[:20], "sha256:" + strings.Repeat("g", 64), "sha256:" + strings.Repeat("é", 32), strings.Repeat("a", 1<<20)} {
		_, err := v1.CanonicalContentDigest(bad)
		assert.Error(t, err, "%.30q", bad)
	}
}

// TestFormatKeepsAUsePin: `flow fmt` rewrites a file from what it compiles to, and a
// pin is not in that, so it is read from the source and put back where it was
// written, beside `path:`, or the formatter turns a security check off in silence.
func TestFormatKeepsAUsePin(t *testing.T) {
	t.Parallel()

	pin := digestOf(t, idsModule)
	src := "edition: " + flowfile.CurrentEdition + "\nname: bill\nuse:\n  ids:\n    # reviewed by the platform team\n    path: ./lib/ids.yaml\n    digest: " + pin + " # 2026-10\nsteps:\n  - id: check\n    log:\n      message: hi\n"
	dir := tree(t, map[string]string{"w.yaml": src, "lib/ids.yaml": idsModule})

	wf, _, err := flowfile.ParseFile(filepath.Join(dir, "w.yaml"))
	require.NoError(t, err)
	got, err := flowfile.Format([]byte(src), wf)
	require.NoError(t, err)
	assert.Equal(t, src, string(got), "pin and comments survive, byte for byte")

	again, _, err := flowfile.ParseAt(got, filepath.Join(dir, "w.yaml"))
	require.NoError(t, err)
	twice, err := flowfile.Format(got, again)
	require.NoError(t, err)
	assert.Equal(t, string(got), string(twice), "a fixed point")
}

func TestCallPinsAndModuleUsesReadTheUseBlock(t *testing.T) {
	t.Parallel()

	pin := digestOf(t, idsModule)
	src := workflowUsing("use:\n  ids:\n    path: ./lib/ids.yaml\n    digest: "+pin+"\n  other:\n    path: ./lib/other.yaml\n", "")

	pins, err := flowfile.CallPins([]byte(src))
	require.NoError(t, err)
	require.Len(t, pins, 1, "an unpinned entry is not a pin")
	assert.Equal(t, flowfile.CallPin{Alias: "ids", Call: "./lib/ids.yaml", Digest: pin, Line: 6, Column: 5}, pins[0])

	uses, err := flowfile.ModuleUses([]byte(src))
	require.NoError(t, err)
	assert.Equal(t, []flowfile.ModuleUse{
		{Alias: "ids", Path: "./lib/ids.yaml", Line: 4, Column: 3},
		{Alias: "other", Path: "./lib/other.yaml", Line: 7, Column: 3},
	}, uses)

	// A variable that happens to be named like a pin is not one.
	vars := "edition: " + flowfile.CurrentEdition + "\nname: x\nvars:\n  use:\n    ids:\n      path: ./a\n      digest: sha256:00\nsteps:\n  - id: a\n    log:\n      message: hi\n"
	pins, err = flowfile.CallPins([]byte(vars))
	require.NoError(t, err)
	assert.Empty(t, pins)
}

// --- repin ---

func TestRepinRewritesAStaleUsePinInPlace(t *testing.T) {
	t.Parallel()

	stale := digestOf(t, "the module as it was\n")
	src := "edition: " + flowfile.CurrentEdition + "\nname: bill\nuse:\n  ids:\n    # reviewed by the platform team\n    path: ./lib/ids.yaml\n    digest: " + stale + " # 2026-10\n  free:\n    path: ./lib/ids.yaml\nsteps:\n  - id: check\n    log:\n      message: hi\n"
	dir := tree(t, map[string]string{"w.yaml": src, "lib/ids.yaml": idsModule})
	file := filepath.Join(dir, "w.yaml")

	result, err := flowfile.RepinUses(file, []byte(src))
	require.NoError(t, err)
	require.True(t, result.Complete())
	require.True(t, result.Changed())
	require.Len(t, result.Changes, 1)
	assert.Equal(t, 7, result.Changes[0].Line)
	assert.Contains(t, result.Changes[0].Message, "ids")

	want := strings.Replace(src, stale, digestOf(t, idsModule), 1)
	assert.Equal(t, want, string(result.Source), "only the digest's text moved: comments, quoting, the unpinned entry")

	// It compiles now, and is formatted as it was.
	_, err = compileAt(t, dir, "w.yaml")
	require.Error(t, err, "the file on disk is untouched: RepinUses returns bytes and writes nothing")
	require.NoError(t, os.WriteFile(file, result.Source, 0o644))
	wf, err := compileAt(t, dir, "w.yaml")
	require.NoError(t, err)
	formatted, err := flowfile.Format(result.Source, wf)
	require.NoError(t, err)
	assert.Equal(t, string(result.Source), string(formatted), "fmt-stable")

	// And idempotent.
	again, err := flowfile.RepinUses(file, result.Source)
	require.NoError(t, err)
	assert.False(t, again.Changed())
	assert.Equal(t, string(result.Source), string(again.Source))
}

func TestRepinLeavesACurrentPinAlone(t *testing.T) {
	t.Parallel()

	upper := "SHA256:" + strings.ToUpper(strings.TrimPrefix(digestOf(t, idsModule), "sha256:"))
	src := workflowUsing(useWithPin(upper), "")
	dir := tree(t, map[string]string{"w.yaml": src, "lib/ids.yaml": idsModule})

	result, err := flowfile.RepinUses(filepath.Join(dir, "w.yaml"), []byte(src))
	require.NoError(t, err)
	assert.False(t, result.Changed(), "a pin that already names the bytes is not rewritten, whatever its case")
	assert.Equal(t, src, string(result.Source))
}

func TestRepinDoesNotStartPinningAnUnpinnedUse(t *testing.T) {
	t.Parallel()

	src := workflowUsing(useIds, "")
	dir := tree(t, map[string]string{"w.yaml": src, "lib/ids.yaml": idsModule})

	result, err := flowfile.RepinUses(filepath.Join(dir, "w.yaml"), []byte(src))
	require.NoError(t, err)
	assert.False(t, result.Changed())
	assert.Equal(t, src, string(result.Source))
}

// TestRepinRefusesWhatItCannotResolve: a stale pin whose module cannot be read, a
// path a use may not name, a digest that is not a digest. Each leaves the whole file
// as it was, even beside a pin that could have been repinned, because a file converts
// in full or not at all.
func TestRepinRefusesWhatItCannotResolve(t *testing.T) {
	t.Parallel()

	stale := digestOf(t, "old\n")
	cases := map[string]struct {
		use   string
		files map[string]string
		want  string
	}{
		"a missing module": {
			use:  "use:\n  ids:\n    path: ./lib/gone.yaml\n    digest: " + stale + "\n",
			want: "could not be read",
		},
		"an absolute path": {
			use:  "use:\n  ids:\n    path: /etc/passwd\n    digest: " + stale + "\n",
			want: "absolute",
		},
		"a path that climbs": {
			use:  "use:\n  ids:\n    path: ../ids.yaml\n    digest: " + stale + "\n",
			want: "climbs",
		},
		"a pin that is not a digest": {
			use:  "use:\n  ids:\n    path: ./lib/ids.yaml\n    digest: sha256:00\n",
			want: "not the shape of a pin",
		},
		"a directory": {
			use:  "use:\n  ids:\n    path: ./lib\n    digest: " + stale + "\n",
			want: "could not be read",
		},
	}
	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			t.Parallel()

			// A second entry that could be repinned, so the refusal is shown to hold the
			// whole file and not only the entry.
			use := tc.use + "  fine:\n    path: ./lib/ids.yaml\n    digest: " + stale + "\n"
			src := workflowUsing(use, "")
			dir := tree(t, map[string]string{"w.yaml": src, "lib/ids.yaml": idsModule})

			result, err := flowfile.RepinUses(filepath.Join(dir, "w.yaml"), []byte(src))
			require.NoError(t, err)
			require.False(t, result.Complete())
			assert.False(t, result.Changed())
			assert.Equal(t, src, string(result.Source), "nothing is written")
			require.NotEmpty(t, result.Refusals)
			assert.Contains(t, result.Refusals[0].Message, tc.want)
			assert.Positive(t, result.Refusals[0].Line)
		})
	}
}

func TestRepinRefusesASymlinkThatLeavesTheDirectory(t *testing.T) {
	t.Parallel()

	outside := t.TempDir()
	require.NoError(t, os.WriteFile(filepath.Join(outside, "ids.yaml"), []byte(idsModule), 0o644))
	src := workflowUsing(useWithPin(digestOf(t, "old\n")), "")
	dir := tree(t, map[string]string{"w.yaml": src})
	require.NoError(t, os.MkdirAll(filepath.Join(dir, "lib"), 0o755))
	if err := os.Symlink(filepath.Join(outside, "ids.yaml"), filepath.Join(dir, "lib", "ids.yaml")); err != nil {
		t.Skipf("symlinks unavailable: %v", err)
	}

	result, err := flowfile.RepinUses(filepath.Join(dir, "w.yaml"), []byte(src))
	require.NoError(t, err)
	assert.False(t, result.Complete(), "the rule a compile resolves a use by is the rule a repin does")
	assert.Equal(t, src, string(result.Source))
}

func TestRepinFollowsAModulesOwnChange(t *testing.T) {
	t.Parallel()

	// The workflow pins the module at the module's bytes before an edit; the edit is
	// the thing being adopted.
	before := idsModule
	after := strings.Replace(idsModule, "No customer has this id.", "No customer has this id, or it was deleted.", 1)
	require.NotEqual(t, before, after)

	src := workflowUsing(useWithPin(digestOf(t, before)), "")
	dir := tree(t, map[string]string{"w.yaml": src, "lib/ids.yaml": after})
	require.Error(t, validateAt(t, dir, "w.yaml"), "the stale pin refuses the compile")

	result, err := flowfile.RepinUses(filepath.Join(dir, "w.yaml"), []byte(src))
	require.NoError(t, err)
	require.True(t, result.Changed())
	require.NoError(t, os.WriteFile(filepath.Join(dir, "w.yaml"), result.Source, 0o644))
	require.NoError(t, validateAt(t, dir, "w.yaml"))
}

func TestRepinStampsTwoEntriesForOneModuleFromOneRead(t *testing.T) {
	t.Parallel()

	old := digestOf(t, "old\n")
	use := "use:\n  ids:\n    path: ./lib/ids.yaml\n    digest: " + old + "\n  same:\n    path: ./lib/../lib/ids.yaml\n    digest: " + old + "\n"
	src := workflowUsing(use, "")
	dir := tree(t, map[string]string{"w.yaml": src, "lib/ids.yaml": idsModule})

	result, err := flowfile.RepinUses(filepath.Join(dir, "w.yaml"), []byte(src))
	require.NoError(t, err)
	require.Len(t, result.Changes, 2)
	assert.Equal(t, 2, strings.Count(string(result.Source), digestOf(t, idsModule)), "both entries carry the one digest")
}
