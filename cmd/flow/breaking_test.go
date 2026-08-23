package main

import (
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/picatz/flowstate/pkg/flowstate/v1/flowfile"
)

// diffFixtures compiles an old and a new Flowfile and returns the breaks the
// command would report for the new one against the old. The comparison is over
// the compiled protos, never the YAML text, so the fixtures run through the same
// parser a real invocation uses.
func diffFixtures(t *testing.T, oldSrc, newSrc string) flowfile.Diagnostics {
	t.Helper()
	oldWF, _, err := flowfile.Parse([]byte(oldSrc))
	require.NoError(t, err, "old fixture did not compile:\n%s", oldSrc)
	newWF, newPos, err := flowfile.Parse([]byte(newSrc))
	require.NoError(t, err, "new fixture did not compile:\n%s", newSrc)
	return breakingDiagnostics(oldWF, newWF, newPos)
}

// A step every fixture ends with, so the workflow is well-formed. It reads no
// input, so adding or removing an input never makes the file itself invalid,
// which is the point: a break is a shrunk contract, not a broken file.
const fixtureStep = "steps:\n  - id: noop\n    log:\n      message: done\n"

func fixtureHeader() string { return "edition: v2026.3\nname: demo\n" }

// TestBreakingRequiredInputAdded is the first break class: an input a caller must
// now supply. The negative direction is asserted beside it: an added *optional*
// input loosens the contract and says nothing.
func TestBreakingRequiredInputAdded(t *testing.T) {
	old := fixtureHeader() + fixtureStep
	added := fixtureHeader() +
		"inputs:\n  region:\n    type: string\n    required: true\n" +
		fixtureStep

	ds := diffFixtures(t, old, added)
	require.Len(t, ds, 1, "a required input added should report exactly one break")
	require.Contains(t, ds[0].Message, `input "region" now must be supplied`)
	require.Positive(t, ds[0].Line, "the break should be positioned at the new declaration")
	require.Positive(t, ds[0].Column)

	// Loosening: an optional input is silent.
	optional := fixtureHeader() +
		"inputs:\n  region:\n    type: string\n" +
		fixtureStep
	require.Empty(t, diffFixtures(t, old, optional),
		"adding an optional input must not be a break")

	// Loosening: a required input with a default is filled for a caller who
	// passes nothing, so it does not break them.
	withDefault := fixtureHeader() +
		"inputs:\n  region:\n    type: string\n    required: true\n    default: us-east-1\n" +
		fixtureStep
	require.Empty(t, diffFixtures(t, old, withDefault),
		"a required input with a default must not be a break")
}

// TestBreakingInputTypeChanged is the second break class: an existing input's
// type narrowed, so a caller passing the old type is refused. The negative
// direction is the same input unchanged.
func TestBreakingInputTypeChanged(t *testing.T) {
	old := fixtureHeader() +
		"inputs:\n  count:\n    type: string\n" +
		fixtureStep
	changed := fixtureHeader() +
		"inputs:\n  count:\n    type: int\n" +
		fixtureStep

	ds := diffFixtures(t, old, changed)
	require.Len(t, ds, 1)
	require.Contains(t, ds[0].Message, `input "count" changed type from string to int`)
	require.Positive(t, ds[0].Line, "the break should be positioned at the type")
	require.Positive(t, ds[0].Column)

	require.Empty(t, diffFixtures(t, old, old),
		"an unchanged input type must not be a break")
}

// TestBreakingDefaultRemovedRequiredFlips is the class the issue names directly:
// a default removed while required stays true. It resolves to the same
// must-supply predicate as an added required input.
func TestBreakingDefaultRemovedRequiredFlips(t *testing.T) {
	old := fixtureHeader() +
		"inputs:\n  region:\n    type: string\n    required: true\n    default: us-east-1\n" +
		fixtureStep
	flipped := fixtureHeader() +
		"inputs:\n  region:\n    type: string\n    required: true\n" +
		fixtureStep

	ds := diffFixtures(t, old, flipped)
	require.Len(t, ds, 1)
	require.Contains(t, ds[0].Message, `input "region" now must be supplied`)
	require.Positive(t, ds[0].Line)
}

// TestBreakingConstraintNarrowed is the constraint class: a bound raised or
// added refuses values the old contract accepted. The negative direction is a
// widened bound, which mirrors buf breaking's asymmetry.
func TestBreakingConstraintNarrowed(t *testing.T) {
	loose := fixtureHeader() +
		"inputs:\n  tag:\n    type: string\n    max_len: 100\n" +
		fixtureStep
	tight := fixtureHeader() +
		"inputs:\n  tag:\n    type: string\n    max_len: 10\n" +
		fixtureStep

	ds := diffFixtures(t, loose, tight)
	require.Len(t, ds, 1)
	require.Contains(t, ds[0].Message, `input "tag" narrowed its constraint`)
	require.Contains(t, ds[0].Message, "max_len lowered")
	require.Positive(t, ds[0].Line)

	// Loosening: widening the ceiling is silent.
	require.Empty(t, diffFixtures(t, tight, loose),
		"widening max_len must not be a break")

	// Loosening: dropping the bound entirely is silent.
	none := fixtureHeader() +
		"inputs:\n  tag:\n    type: string\n" +
		fixtureStep
	require.Empty(t, diffFixtures(t, tight, none),
		"dropping a bound must not be a break")

	// A must added where there was none is a narrowing (fail-closed).
	withMust := fixtureHeader() +
		"inputs:\n  tag:\n    type: string\n    must: size(this) > 3\n" +
		fixtureStep
	dsMust := diffFixtures(t, none, withMust)
	require.Len(t, dsMust, 1)
	require.Contains(t, dsMust[0].Message, "must tightened")

	// Removing a must is a widening.
	require.Empty(t, diffFixtures(t, withMust, none),
		"removing a must must not be a break")
}

// TestBreakingEnumValueRemoved is the enum-narrowing class: removing a member
// from an existing enum refuses an argument the old contract accepted, so it
// must be reported — the same direction a raised min_len is. The negative
// direction is an added member, which only admits more and must stay silent.
func TestBreakingEnumValueRemoved(t *testing.T) {
	old := fixtureHeader() +
		"inputs:\n  environment:\n    type: enum\n    values: [staging, production]\n" +
		fixtureStep
	narrowed := fixtureHeader() +
		"inputs:\n  environment:\n    type: enum\n    values: [production]\n" +
		fixtureStep

	ds := diffFixtures(t, old, narrowed)
	require.Len(t, ds, 1, "removing an enum member should report exactly one break")
	require.Contains(t, ds[0].Message, `input "environment" narrowed its constraint`)
	require.Contains(t, ds[0].Message, "values removed: staging")
	require.Positive(t, ds[0].Line)

	// Widening: adding a member is silent.
	require.Empty(t, diffFixtures(t, narrowed, old),
		"adding an enum member must not be a break")

	// No change: identical values is silent.
	require.Empty(t, diffFixtures(t, old, old),
		"unchanged enum values must not be a break")
}

// TestBreakingInputRemoved is the input-removed class: a caller passing it via
// `with:` breaks, because an unknown `with:` key is refused
// (flowfile/validate_call.go). It has no position in the new file, so it names
// the field. The negative direction is the input kept.
func TestBreakingInputRemoved(t *testing.T) {
	old := fixtureHeader() +
		"inputs:\n  region:\n    type: string\n" +
		fixtureStep
	removed := fixtureHeader() + fixtureStep

	ds := diffFixtures(t, old, removed)
	require.Len(t, ds, 1)
	require.Contains(t, ds[0].Message, `input "region" was removed`)
	require.Equal(t, "inputs", ds[0].Field)

	require.Empty(t, diffFixtures(t, old, old),
		"an input that is kept must not be a break")
}

// TestBreakingOutputRemoved is the output class: an output removed or renamed, so
// a caller reading it breaks. The negative direction is an *added* output, which
// grows the contract and passes.
func TestBreakingOutputRemoved(t *testing.T) {
	old := fixtureHeader() +
		"outputs:\n  where:\n    value: ${'here'}\n" +
		fixtureStep
	removed := fixtureHeader() + fixtureStep

	ds := diffFixtures(t, old, removed)
	require.Len(t, ds, 1)
	require.Contains(t, ds[0].Message, `output "where" was removed or renamed`)
	require.Equal(t, "outputs", ds[0].Field)

	// A rename is a removal of the old name.
	renamed := fixtureHeader() +
		"outputs:\n  location:\n    value: ${'here'}\n" +
		fixtureStep
	dsRename := diffFixtures(t, old, renamed)
	require.Len(t, dsRename, 1)
	require.Contains(t, dsRename[0].Message, `output "where" was removed or renamed`)

	// Loosening: adding an output alongside the existing one is silent.
	added := fixtureHeader() +
		"outputs:\n  where:\n    value: ${'here'}\n  extra:\n    value: ${'more'}\n" +
		fixtureStep
	require.Empty(t, diffFixtures(t, old, added),
		"adding an output must not be a break")
}

// TestBreakingOutputGuaranteeWeakened is the output-postcondition class: a
// declared output's `must:` is a guarantee a consumer may rely on, so dropping
// or changing it breaks callers, the mirror of an input precondition tightening.
// The negative direction is a `must:` *added* where there was none, which only
// strengthens the guarantee and passes.
func TestBreakingOutputGuaranteeWeakened(t *testing.T) {
	guaranteed := fixtureHeader() +
		"outputs:\n  code:\n    value: ${200}\n    must: this >= 200\n" +
		fixtureStep
	dropped := fixtureHeader() +
		"outputs:\n  code:\n    value: ${200}\n" +
		fixtureStep

	ds := diffFixtures(t, guaranteed, dropped)
	require.Len(t, ds, 1, "dropping an output must: should report exactly one break")
	require.Contains(t, ds[0].Message, `output "code" weakened its guarantee`)

	// A changed predicate is undecidable, so it is reported (fail-closed).
	changed := fixtureHeader() +
		"outputs:\n  code:\n    value: ${200}\n    must: this >= 100\n" +
		fixtureStep
	dsChanged := diffFixtures(t, guaranteed, changed)
	require.Len(t, dsChanged, 1)
	require.Contains(t, dsChanged[0].Message, `output "code" weakened its guarantee`)

	// Loosening: adding a must where there was none only strengthens the
	// guarantee, so it is silent.
	require.Empty(t, diffFixtures(t, dropped, guaranteed),
		"adding an output guarantee must not be a break")

	// An unchanged guarantee is silent.
	require.Empty(t, diffFixtures(t, guaranteed, guaranteed),
		"an unchanged output guarantee must not be a break")
}

// --- end-to-end: the git plumbing and the command wiring ---

// gitInitRepo builds a throwaway repository with one committed Flowfile, then
// leaves a possibly-different version in the working tree, and returns the
// directory. The command reads the committed version through `git show` and the
// working tree through the ordinary walk, exactly as it does against origin/main.
func gitInitRepo(t *testing.T, committed string) string {
	t.Helper()
	if _, err := exec.LookPath("git"); err != nil {
		t.Skip("git is not installed")
	}
	dir := t.TempDir()
	runGitTest(t, dir, "init", "-q")
	runGitTest(t, dir, "config", "user.email", "t@example.com")
	runGitTest(t, dir, "config", "user.name", "test")
	require.NoError(t, os.WriteFile(filepath.Join(dir, "workflow.yaml"), []byte(committed), 0o644))
	runGitTest(t, dir, "add", "workflow.yaml")
	runGitTest(t, dir, "commit", "-q", "-m", "initial")
	return dir
}

func runGitTest(t *testing.T, dir string, args ...string) {
	t.Helper()
	cmd := exec.Command("git", args...)
	cmd.Dir = dir
	out, err := cmd.CombinedOutput()
	require.NoError(t, err, "git %s: %s", strings.Join(args, " "), out)
}

// runBreakingCLI runs the real command, so the flag plumbing, the git reads, and
// the exit behavior are all exercised.
func runBreakingCLI(t *testing.T, dir string, args ...string) (string, error) {
	t.Helper()
	t.Chdir(dir)

	res := runFlow(t, append([]string{"breaking"}, args...)...)

	// Both streams, merged: this command's report and its findings are read
	// together here, and the split is pinned where it is the subject (see
	// runlocal_test.go).
	return res.Output(), res.Err
}

// TestBreakingCommandCleanAndBreaking drives the whole command end to end: a
// working tree that only grows its contract passes, and one that removes an
// output fails with a positioned finding.
func TestBreakingCommandCleanAndBreaking(t *testing.T) {
	committed := fixtureHeader() +
		"outputs:\n  where:\n    value: ${'here'}\n" +
		fixtureStep

	// Clean: an added output grows the contract.
	dir := gitInitRepo(t, committed)
	grown := fixtureHeader() +
		"outputs:\n  where:\n    value: ${'here'}\n  extra:\n    value: ${'more'}\n" +
		fixtureStep
	require.NoError(t, os.WriteFile(filepath.Join(dir, "workflow.yaml"), []byte(grown), 0o644))
	out, err := runBreakingCLI(t, dir, "--against", "HEAD", "workflow.yaml")
	require.NoError(t, err, "a grown contract should exit 0, got:\n%s", out)

	// Breaking: the output is removed.
	dir2 := gitInitRepo(t, committed)
	shrunk := fixtureHeader() + fixtureStep
	require.NoError(t, os.WriteFile(filepath.Join(dir2, "workflow.yaml"), []byte(shrunk), 0o644))
	out2, err2 := runBreakingCLI(t, dir2, "--against", "HEAD", "workflow.yaml")
	require.Error(t, err2, "a removed output should exit non-zero")
	require.Contains(t, out2, `output "where" was removed or renamed`)
}

// gitInitRepoFiles builds a throwaway repository with several committed
// Flowfiles at given repo-relative paths, and returns the directory. Nothing is
// changed in the working tree, so a run compares each file against itself.
func gitInitRepoFiles(t *testing.T, files map[string]string) string {
	t.Helper()
	if _, err := exec.LookPath("git"); err != nil {
		t.Skip("git is not installed")
	}
	dir := t.TempDir()
	runGitTest(t, dir, "init", "-q")
	runGitTest(t, dir, "config", "user.email", "t@example.com")
	runGitTest(t, dir, "config", "user.name", "test")
	for rel, src := range files {
		abs := filepath.Join(dir, rel)
		require.NoError(t, os.MkdirAll(filepath.Dir(abs), 0o755))
		require.NoError(t, os.WriteFile(abs, []byte(src), 0o644))
	}
	runGitTest(t, dir, "add", "-A")
	runGitTest(t, dir, "commit", "-q", "-m", "initial")
	return dir
}

// TestBreakingFromSubdirectory checks the pathspec is resolved against the
// repository root, not the caller's directory. Run from a subdirectory with `.`,
// the ref side must scope its listing to that subdirectory: an unrelated,
// unchanged workflow elsewhere in the tree must not read as removed. The names
// differ per directory, so this exercises only the path scoping, not the
// same-name refusal.
func TestBreakingFromSubdirectory(t *testing.T) {
	here := "edition: v2026.3\nname: here\n" + fixtureStep
	elsewhere := "edition: v2026.3\nname: elsewhere\n" + fixtureStep
	dir := gitInitRepoFiles(t, map[string]string{
		"svc/a/workflow.yaml": here,
		"svc/b/workflow.yaml": elsewhere,
	})

	// From svc/a, checking `.`, nothing changed anywhere, so the only workflow
	// in scope is unchanged and svc/b must not be seen at all.
	out, err := runBreakingCLI(t, filepath.Join(dir, "svc", "a"), "--against", "HEAD", ".")
	require.NoError(t, err, "an unchanged subdirectory must report no break, got:\n%s", out)
	require.NotContains(t, out, "elsewhere",
		"a workflow outside the checked subdirectory must not be listed")
}

// TestBreakingRefusesDuplicateNames checks that two files declaring one workflow
// name are refused rather than silently collapsed. Matching is by name, so a
// collision would compare one file and miss the other; the command names both
// files and fails instead.
func TestBreakingRefusesDuplicateNames(t *testing.T) {
	same := "edition: v2026.3\nname: shared\n" + fixtureStep
	dir := gitInitRepoFiles(t, map[string]string{
		"svc/a/workflow.yaml": same,
		"svc/b/workflow.yaml": same,
	})

	out, err := runBreakingCLI(t, dir, "--against", "HEAD", ".")
	require.Error(t, err, "duplicate names must fail the command, got:\n%s", out)
	require.Contains(t, out, `workflow name "shared" is declared by both`)
	require.Contains(t, out, "svc/a/workflow.yaml")
	require.Contains(t, out, "svc/b/workflow.yaml")
}

// TestBreakingCommandMissingRef reports the ref that is not in local history with
// the same guidance the buf breaking check carries: fetch the base branch first.
func TestBreakingCommandMissingRef(t *testing.T) {
	dir := gitInitRepo(t, fixtureHeader()+fixtureStep)
	_, err := runBreakingCLI(t, dir, "--against", "origin/nonesuch", "workflow.yaml")
	require.Error(t, err)
	require.Contains(t, err.Error(), "not in the local history")
}
