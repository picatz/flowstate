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

func fixtureHeader() string { return "edition: v2026.2\nname: demo\n" }

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
	root := newRootCommand()
	var out strings.Builder
	root.SetOut(&out)
	root.SetErr(&out)
	root.SetArgs(append([]string{"breaking"}, args...))
	err := root.Execute()
	return out.String(), err
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

// TestBreakingCommandMissingRef reports the ref that is not in local history with
// the same guidance the buf breaking check carries: fetch the base branch first.
func TestBreakingCommandMissingRef(t *testing.T) {
	dir := gitInitRepo(t, fixtureHeader()+fixtureStep)
	_, err := runBreakingCLI(t, dir, "--against", "origin/nonesuch", "workflow.yaml")
	require.Error(t, err)
	require.Contains(t, err.Error(), "not in the local history")
}
