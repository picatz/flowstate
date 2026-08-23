package main

import (
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/picatz/flowstate/pkg/flowstate/v1/flowfile"
)

// What `flow init` has to earn is narrower than what it writes, and the two
// tests that matter are the two properties its doc names.
//
// The first is that the scaffold cannot rot. A starter file is read by somebody
// with no way to tell whether it or the tool is wrong, so a template that has
// drifted past the grammar is worse than no template — and a template is exactly
// the kind of thing that drifts, because nothing compiles it. So the gate below
// runs the real `validate`, `test` and `fix --check` against what the command
// produced, in process, through the same command tree a person types into. That
// is the `buf generate` plus `git diff --exit-code` mechanism pointed at prose:
// an edition bump or a grammar change fails here, on the commit that makes it,
// rather than in a stranger's first five minutes with the tool.
//
// The second is that it never overwrites, in either direction and with nothing
// half-written when it refuses.

// TestInitProducesAFlowfileTheToolAccepts is the anti-rot gate.
//
// Not "the template parses" and not "the template contains an edition marker":
// the three commands an author runs next, run for real against the bytes on
// disk. `validate` says the file is well-formed, `test` says the case beside it
// passes — which also proves the workflow and its test file agree about the step
// ids and the task's inputs — and `fix --check` says the file is already in the
// current edition, which is the assertion that fails on the day the edition
// moves and this template does not.
func TestInitProducesAFlowfileTheToolAccepts(t *testing.T) {
	dir := t.TempDir()

	err := runFlow(t, "init", dir).Err
	require.NoError(t, err)

	workflow := filepath.Join(dir, scaffoldWorkflow)

	validated := runFlow(t, "validate", workflow)
	assert.NoError(t, validated.Err, "the scaffold does not validate:\n%s", validated.Output())

	tested := runFlow(t, "test", dir)
	assert.NoError(t, tested.Err, "the scaffold's own test does not pass:\n%s", tested.Output())

	fixed := runFlow(t, "fix", "--check", workflow)
	assert.NoError(t, fixed.Err, "the scaffold is not in the current edition:\n%s", fixed.Output())
}

// TestInitWritesTheCurrentEdition pins the mechanism the gate above depends on.
//
// `fix --check` passing is the behavioural half; this is the structural half,
// and it is worth stating separately because a hardcoded edition string that
// happens to equal the current one passes the gate right up until the bump —
// at which point the bump's own commit has two things to fix instead of one.
func TestInitWritesTheCurrentEdition(t *testing.T) {
	dir := t.TempDir()

	err := runFlow(t, "init", dir).Err
	require.NoError(t, err)

	for _, name := range []string{scaffoldWorkflow, scaffoldTest} {
		data, err := os.ReadFile(filepath.Join(dir, name))
		require.NoError(t, err)

		assert.Contains(t, string(data), "edition: "+flowfile.CurrentEdition,
			"%s does not declare the edition this build speaks", name)
	}
}

// TestInitRefusesRatherThanOverwrite is the fail-closed property, asserted
// independently for each file.
//
// Independently because a check that only looks at the workflow leaves the test
// file overwritable, and the failure that produces is silent: an author's own
// cases replaced by a greeting they did not write, reported as a success.
func TestInitRefusesRatherThanOverwrite(t *testing.T) {
	for _, existing := range []string{scaffoldWorkflow, scaffoldTest} {
		t.Run(existing, func(t *testing.T) {
			dir := t.TempDir()

			const mine = "# mine, and not to be replaced\n"
			path := filepath.Join(dir, existing)
			require.NoError(t, os.WriteFile(path, []byte(mine), 0o644))

			err := runFlow(t, "init", dir).Err
			require.Error(t, err, "init overwrote %s", existing)

			// The refusal names the file that stopped it, because the whole of
			// what the reader has to do next is decided by which file it was.
			assert.Contains(t, err.Error(), path)

			data, err := os.ReadFile(path)
			require.NoError(t, err)
			assert.Equal(t, mine, string(data), "%s was written over", existing)
		})
	}
}

// TestInitWritesNothingWhenItRefuses is the other half of failing closed, and
// the one a per-file check alone does not give.
//
// A command that wrote the workflow, then found the test file already there and
// stopped, would leave a directory holding somebody's test cases beside a
// workflow that has nothing to do with them — a state nothing else in this tool
// produces and no author asked for. Both paths are checked before either is
// written, and this is what says so.
func TestInitWritesNothingWhenItRefuses(t *testing.T) {
	dir := t.TempDir()

	require.NoError(t, os.WriteFile(filepath.Join(dir, scaffoldTest), []byte("# mine\n"), 0o644))

	err := runFlow(t, "init", dir).Err
	require.Error(t, err)

	_, statErr := os.Stat(filepath.Join(dir, scaffoldWorkflow))
	assert.True(t, os.IsNotExist(statErr),
		"init refused and still wrote %s, leaving a half-scaffolded directory", scaffoldWorkflow)
}

// TestReserveScaffoldFilesRefusesALateSymlink pins the atomic half of the
// no-overwrite promise. runInit checks both destinations before writing either,
// but a directory entry can appear after that check; the create itself must
// therefore refuse an existing symlink rather than follow it and truncate its
// target.
//
// The assertion is the negative direction — the victim's bytes are unchanged —
// because a test that only checks the error would pass just as happily against
// a create that followed the link and then failed for some other reason.
func TestReserveScaffoldFilesRefusesALateSymlink(t *testing.T) {
	dir := t.TempDir()
	victim := filepath.Join(dir, "victim")
	const mine = "mine, and not to be replaced\n"
	require.NoError(t, os.WriteFile(victim, []byte(mine), 0o644))

	require.NoError(t, os.Symlink(victim, filepath.Join(dir, scaffoldWorkflow)))

	_, err := reserveScaffoldFiles(dir, []scaffoldFile{
		{name: scaffoldWorkflow, contents: "replacement\n"},
	})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "never overwrites",
		"a late collision lost the refusal the preflight words for the same case")

	data, err := os.ReadFile(victim)
	require.NoError(t, err)
	assert.Equal(t, mine, string(data), "the scaffold write followed an existing symlink")
}

// TestReserveScaffoldFilesLeavesNothingWhenTheSecondPathCollides pins the other
// half of the same promise, and the half the exclusive create is what makes
// reachable: two files land or neither does.
//
// runInit's preflight cannot keep that on its own, because the collision this
// test plants arrives after it. Reserving both destinations before writing
// either is what keeps it, so the assertion is that the *first* path is gone
// again — not merely that the call failed.
func TestReserveScaffoldFilesLeavesNothingWhenTheSecondPathCollides(t *testing.T) {
	dir := t.TempDir()
	require.NoError(t, os.WriteFile(filepath.Join(dir, scaffoldTest), []byte("planted\n"), 0o644))

	_, err := reserveScaffoldFiles(dir, []scaffoldFile{
		{name: scaffoldWorkflow, contents: starterWorkflow("example")},
		{name: scaffoldTest, contents: starterTest()},
	})
	require.Error(t, err)

	_, err = os.Lstat(filepath.Join(dir, scaffoldWorkflow))
	assert.True(t, os.IsNotExist(err),
		"a collision on %s left %s behind, half-scaffolding the directory",
		scaffoldTest, scaffoldWorkflow)
}

// TestInitCreatesTheDirectory covers the invocation a newcomer actually types
// — `flow init my-thing` for a directory that does not exist yet — which is the
// one `cargo new` and `terraform init` set the expectation for.
func TestInitCreatesTheDirectory(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "deploy-frontend")

	err := runFlow(t, "init", dir).Err
	require.NoError(t, err)

	data, err := os.ReadFile(filepath.Join(dir, scaffoldWorkflow))
	require.NoError(t, err)
	assert.Contains(t, string(data), "name: deploy-frontend\n")
}

// TestInitNamesTheWorkflowAfterTheDirectory covers the derivation, including
// the two cases where the directory's name is not itself a legal one.
//
// The sanitized and fallback cases are asserted to *say so*, not merely to
// produce something legal: a workflow quietly called something the author never
// chose is a surprise found later, in a run's id, by somebody who has forgotten
// this command decided it.
func TestInitNamesTheWorkflowAfterTheDirectory(t *testing.T) {
	for _, tt := range []struct {
		name  string
		dir   string
		want  string
		noted bool
	}{
		{name: "already legal", dir: "nightly-report", want: "nightly-report"},
		{name: "spaces and dots", dir: "my report v1.2", want: "my-report-v1-2", noted: true},
		{name: "leading dot", dir: ".config", want: "config", noted: true},
		{name: "nothing legal in it", dir: "...", want: fallbackName, noted: true},
		// A plain scalar YAML would read as something other than a string. The
		// gate found this one — a temporary directory called `001` produced
		// `name: 001`, which the parser refuses as a number.
		{name: "all digits", dir: "001", want: "001"},
		{name: "a word YAML reads as a boolean", dir: "no", want: "no"},
	} {
		t.Run(tt.name, func(t *testing.T) {
			// Cleaned away by filepath.Abs before a base name is taken, so a
			// directory literally called "..." needs a parent to sit in.
			dir := filepath.Join(t.TempDir(), tt.dir)

			res := runFlow(t, "init", dir)
			require.NoError(t, res.Err)
			out := res.Stdout

			data, err := os.ReadFile(filepath.Join(dir, scaffoldWorkflow))
			require.NoError(t, err)

			// Parsed rather than matched as text, because what is under test is
			// the name the compiler ends up with — a quoted scalar and a plain
			// one are the same name and a `Contains` on the source cannot say so.
			wf, err := flowfile.Unmarshal(data)
			require.NoError(t, err)
			assert.Equal(t, tt.want, wf.GetName())

			if tt.noted {
				assert.Contains(t, out, tt.want,
					"the name was not the directory's and the report did not say so")
			}
		})
	}
}

// TestInitTakesTheNameItIsGiven covers `--name`, including the refusal.
//
// An illegal `--name` is an error rather than a silent sanitization, which is
// the opposite call from the derived case above and deliberately so: the author
// typed this one, so the answer is what a name may hold and what theirs would
// have to become — not a file naming a workflow they did not ask for.
func TestInitTakesTheNameItIsGiven(t *testing.T) {
	t.Run("legal", func(t *testing.T) {
		dir := t.TempDir()

		err := runFlow(t, "init", dir, "--name", "nightly_report-2").Err
		require.NoError(t, err)

		data, err := os.ReadFile(filepath.Join(dir, scaffoldWorkflow))
		require.NoError(t, err)
		assert.Contains(t, string(data), "name: nightly_report-2\n")
	})

	t.Run("illegal, refused with the remedy", func(t *testing.T) {
		dir := t.TempDir()

		err := runFlow(t, "init", dir, "--name", "my report").Err
		require.Error(t, err)
		assert.Contains(t, err.Error(), "spaces")
		assert.Contains(t, err.Error(), "my-report", "the refusal offers nothing to paste")

		// Refused before anything was written, on the same rule as the
		// no-clobber check: a directory is not half-scaffolded by a bad flag.
		_, statErr := os.Stat(filepath.Join(dir, scaffoldWorkflow))
		assert.True(t, os.IsNotExist(statErr))
	})

	t.Run("too long for the schema", func(t *testing.T) {
		err := runFlow(t, "init", t.TempDir(), "--name", strings.Repeat("a", maxWorkflowNameLen+1)).Err
		require.Error(t, err)
		assert.Contains(t, err.Error(), "at most")
	})
}

// TestInitReportsWhatToDoNext keeps the one thing a scaffold command exists for
// from being lost: the reader has just been handed two files and has to be told
// what to type at them. The commands named are the real ones, with the real
// path, so they can be pasted rather than adapted.
func TestInitReportsWhatToDoNext(t *testing.T) {
	dir := t.TempDir()

	res := runFlow(t, "init", dir)
	require.NoError(t, res.Err)
	out := res.Stdout

	assert.Contains(t, out, filepath.Join(dir, scaffoldWorkflow))
	assert.Contains(t, out, filepath.Join(dir, scaffoldTest))
	assert.Contains(t, out, "flow run local "+filepath.Join(dir, scaffoldWorkflow))
	assert.Contains(t, out, "flow test "+dir)

	// The durable half, which is the reason the tool exists and the half a
	// newcomer has no way to guess: `flow server dev` assembles the stack, and
	// the same `flow run` without `local` submits to it. Asserted here because
	// the whole claim of #377 is that this is two commands, and a scaffold that
	// stops at `run local` leaves the reader believing it is more.
	assert.Contains(t, out, "flow server dev")
	assert.Contains(t, out, "flow run "+filepath.Join(dir, scaffoldWorkflow))
}
