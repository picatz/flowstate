package main

import (
	"context"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
)

// realGitBinary finds a real git on the machine running this test, purely
// as a test fixture builder - the plugin's own runtime code never searches
// $PATH for anything (see doc.go and binary.go); only this test setup does,
// the same way plugins/vcs's own tests are free to use real git tooling to
// build fixtures without that meaning plugins/vcs execs git at runtime.
func realGitBinary(t *testing.T) string {
	t.Helper()
	path, err := exec.LookPath("git")
	if err != nil {
		t.Skip("no git binary on this system to build a test fixture with")
	}
	return path
}

func initRepoWithCommit(t *testing.T, gitBin, dir string) {
	t.Helper()
	run := func(args ...string) {
		cmd := exec.Command(gitBin, append([]string{"-C", dir}, args...)...)
		cmd.Env = append(os.Environ(),
			"GIT_AUTHOR_NAME=test", "GIT_AUTHOR_EMAIL=test@example.com",
			"GIT_COMMITTER_NAME=test", "GIT_COMMITTER_EMAIL=test@example.com",
		)
		if out, err := cmd.CombinedOutput(); err != nil {
			t.Fatalf("git %v: %v: %s", args, err, out)
		}
	}
	run("init", "-q")
	run("config", "user.email", "test@example.com")
	run("config", "user.name", "test")
	if err := os.WriteFile(filepath.Join(dir, "a.txt"), []byte("one\n"), 0o600); err != nil {
		t.Fatalf("WriteFile: %v", err)
	}
	run("add", "a.txt")
	run("commit", "-q", "-m", "initial")
}

// prepareForTest runs prepareHardenedGit the same way codexExec does, and
// registers its cleanup - callers get back what computePatch and
// observeWorkspace actually receive in production, rather than a fixture
// that only approximates the shape of those parameters.
func prepareForTest(t *testing.T, dir string) (gitBin string, hardened []string) {
	t.Helper()
	gitBin, hardened, cleanup, ok := prepareHardenedGit(context.Background(), dir)
	if cleanup != nil {
		t.Cleanup(cleanup)
	}
	if !ok {
		return "", nil
	}
	return gitBin, hardened
}

func TestComputePatchSkipsWhenNotMutating(t *testing.T) {
	patch, files, truncated := computePatch(context.Background(), "", nil, t.TempDir(), false, workspaceBaseline{observed: true},
		[]fileChange{{Path: "a.txt", ChangeType: "update"}})
	if patch != "" || truncated {
		t.Fatalf("computePatch with mutating=false = (%q, %v), want (\"\", false)", patch, truncated)
	}
	if len(files) != 1 {
		t.Fatalf("computePatch with mutating=false should still pass through the reported files unchanged")
	}
}

func TestComputePatchSkipsWithNoFilesChanged(t *testing.T) {
	patch, _, _ := computePatch(context.Background(), "", nil, t.TempDir(), true, workspaceBaseline{observed: true}, nil)
	if patch != "" {
		t.Fatalf("computePatch with no files_changed = %q, want empty", patch)
	}
}

func TestComputePatchSkipsWithNoGitBinaryConfigured(t *testing.T) {
	t.Setenv(gitBinaryEnv, "")
	dir := t.TempDir()
	gitBin, hardened := prepareForTest(t, dir)
	patch, _, _ := computePatch(context.Background(), gitBin, hardened, dir, true, workspaceBaseline{observed: true},
		[]fileChange{{Path: "a.txt", ChangeType: "update"}})
	if patch != "" {
		t.Fatalf("computePatch with no git binary configured = %q, want empty (best-effort, not an error)", patch)
	}
}

func TestComputePatchSkipsWhenDirIsNotAGitRepo(t *testing.T) {
	gitBin := realGitBinary(t)
	t.Setenv(gitBinaryEnv, gitBin)

	dir := t.TempDir()
	preparedBin, hardened := prepareForTest(t, dir)
	patch, _, _ := computePatch(context.Background(), preparedBin, hardened, dir, true, workspaceBaseline{observed: true},
		[]fileChange{{Path: "a.txt", ChangeType: "update"}})
	if patch != "" {
		t.Fatalf("computePatch against a plain directory = %q, want empty", patch)
	}
}

// TestComputePatchRendersAUnifiedDiff is the worked example: a real git
// checkout, a real file edit made after the "run" (standing in for what
// codex would have done in WORKSPACE_WRITE mode), and a real `git diff`
// subprocess this plugin's own code invokes.
func TestComputePatchRendersAUnifiedDiff(t *testing.T) {
	gitBin := realGitBinary(t)
	t.Setenv(gitBinaryEnv, gitBin)

	dir := t.TempDir()
	initRepoWithCommit(t, gitBin, dir)

	if err := os.WriteFile(filepath.Join(dir, "a.txt"), []byte("one\ntwo\n"), 0o600); err != nil {
		t.Fatalf("WriteFile: %v", err)
	}

	gitBin, hardened := prepareForTest(t, dir)
	patch, files, truncated := computePatch(context.Background(), gitBin, hardened, dir, true, workspaceBaseline{observed: true},
		[]fileChange{{Path: "a.txt", ChangeType: "update"}})
	if truncated {
		t.Fatal("computePatch reported truncated for a tiny diff")
	}
	if !strings.Contains(patch, "+two") {
		t.Fatalf("patch = %q, want it to contain the added line", patch)
	}
	if len(files) != 1 || files[0].Path != "a.txt" {
		t.Fatalf("files = %v, want the one file passed in, unchanged", files)
	}
}

func TestComputePatchDoesNotRunRepositoryHelpers(t *testing.T) {
	gitBin := realGitBinary(t)
	t.Setenv(gitBinaryEnv, gitBin)

	dir := t.TempDir()
	initRepoWithCommit(t, gitBin, dir)
	marker := filepath.Join(t.TempDir(), "external-diff-ran")
	helper := filepath.Join(dir, "git-helper.sh")
	if err := os.WriteFile(helper, []byte("#!/bin/sh\ntouch \""+marker+"\"\ncat\n"), 0o700); err != nil {
		t.Fatalf("WriteFile helper: %v", err)
	}
	hooks := filepath.Join(dir, "hooks")
	if err := os.MkdirAll(hooks, 0o700); err != nil {
		t.Fatalf("MkdirAll hooks: %v", err)
	}
	if err := os.WriteFile(filepath.Join(hooks, "post-index-change"),
		[]byte("#!/bin/sh\ntouch \""+marker+"\"\n"), 0o700); err != nil {
		t.Fatalf("WriteFile hook: %v", err)
	}
	for key, value := range map[string]string{
		"diff.external":     helper,
		"filter.evil.clean": helper,
		// The .process spelling is a second, independent way to name the same
		// program, and safeDiffArgs disables it separately - so a test that
		// only ever sets .clean leaves half of what the sweep claims untested.
		"filter.wicked.process": helper,
		// Not a content filter, and needing no gitattributes entry: Git runs
		// core.fsmonitor to ask what changed in the working tree, from any
		// command that inspects the index - the --intent-to-add call as much
		// as the diff itself. Sweeping filter.* does not reach it, so that
		// sweep alone leaves the one config key that is the whole attack.
		"core.fsmonitor": helper,
		// Writing the index fires post-index-change from wherever
		// core.hooksPath points, so `git add --intent-to-add` runs a hook the
		// repository supplies. There is no "no hooks" value, only a directory
		// with none in it.
		"core.hooksPath": hooks,
	} {
		cmd := exec.Command(gitBin, "-C", dir, "config", key, value)
		if out, err := cmd.CombinedOutput(); err != nil {
			t.Fatalf("configure repository %s: %v: %s", key, err, out)
		}
	}
	attributes := filepath.Join(dir, ".git", "info", "attributes")
	if err := os.WriteFile(attributes, []byte("a.txt filter=evil\nb.txt filter=wicked\nc.txt filter=evil\n"), 0o600); err != nil {
		t.Fatalf("WriteFile attributes: %v", err)
	}
	if err := os.WriteFile(filepath.Join(dir, "a.txt"), []byte("changed\n"), 0o600); err != nil {
		t.Fatalf("WriteFile change: %v", err)
	}
	// A second path, carrying the .process driver, so the diff has to convert
	// content under both filter spellings rather than only the .clean one.
	if err := os.WriteFile(filepath.Join(dir, "b.txt"), []byte("also changed\n"), 0o600); err != nil {
		t.Fatalf("WriteFile process-filtered change: %v", err)
	}
	// And an untracked file matching the clean filter, so the intent-to-add
	// that runs before the diff has content of its own to convert. That
	// command is the one the hardening reaches first and the one a diff-only
	// fix leaves running every helper here.
	if err := os.WriteFile(filepath.Join(dir, "c.txt"), []byte("new file\n"), 0o600); err != nil {
		t.Fatalf("WriteFile untracked: %v", err)
	}

	// Mirrors codexExec's own ordering: hardening is prepared once, the
	// baseline is read with it before anything else runs, and only then does
	// computePatch touch the repository again. observeWorkspace used to run
	// unhardened - see prepareHardenedGit's doc comment - so this checks the
	// marker after *that* call too, not only after computePatch, to prove the
	// baseline read no longer gets there first.
	gitBin, hardened := prepareForTest(t, dir)
	baseline := observeWorkspace(context.Background(), gitBin, hardened, dir, true)
	if !baseline.observed {
		t.Fatalf("observeWorkspace = %+v, want observed=true (a real repo, hardened successfully)", baseline)
	}
	if _, err := os.Stat(marker); !os.IsNotExist(err) {
		t.Fatalf("repository-controlled helper ran during observeWorkspace, before the run even started: Stat marker error = %v", err)
	}

	// The test files above are already dirty edits, standing in for the run's
	// own changes - computePatch is exercised the same way the other tests in
	// this file exercise it, with an observed-clean synthetic baseline, since
	// what this test cares about is that neither call ran a helper, not
	// whether this fixture's own pre-populated dirt would itself be honest to
	// commit.
	patch, _, truncated := computePatch(context.Background(), gitBin, hardened, dir, true, workspaceBaseline{observed: true},
		[]fileChange{{Path: "a.txt", ChangeType: "update"}})
	if truncated || !strings.Contains(patch, "+changed") {
		t.Fatalf("computePatch = (%q, truncated %v), want an ordinary unified diff", patch, truncated)
	}
	if _, err := os.Stat(marker); !os.IsNotExist(err) {
		t.Fatalf("repository-controlled diff or content-filter helper ran outside the Codex sandbox: Stat marker error = %v", err)
	}
}

// TestComputePatchOverridesFiltersInstalledDuringTheRun is the
// time-of-check gap TestComputePatchDoesNotRunRepositoryHelpers cannot see:
// there, every helper exists before hardening is computed, so a stale
// override list still names them all. Here the repository is clean when
// prepareHardenedGit enumerates its filter keys - the enumeration
// legitimately finds nothing to override - and the filter is installed only
// *after* the baseline observation, standing in for what a WORKSPACE_WRITE
// codex run can do to its own working_context mid-run (the repository's
// config is just another file under it). computePatch must still refuse to
// run that filter, which it can only do by re-enumerating the mutated
// config rather than reusing the pre-run list.
func TestComputePatchOverridesFiltersInstalledDuringTheRun(t *testing.T) {
	gitBin := realGitBinary(t)
	t.Setenv(gitBinaryEnv, gitBin)

	dir := t.TempDir()
	initRepoWithCommit(t, gitBin, dir)

	// Mirrors codexExec's ordering: hardening prepared and the baseline read
	// while the repository is still clean - no filters configured, so the
	// enumerated override list is legitimately empty of them.
	gitBin, hardened := prepareForTest(t, dir)
	baseline := observeWorkspace(context.Background(), gitBin, hardened, dir, true)
	if !baseline.observed || baseline.dirty {
		t.Fatalf("baseline = %+v, want observed and clean before the simulated run", baseline)
	}

	// "The run": install a filter the pre-run enumeration never saw, wire it
	// to files via gitattributes, and make the edits that give the post-run
	// git commands content to convert - a tracked modification for the diff,
	// an untracked file for the intent-to-add.
	marker := filepath.Join(t.TempDir(), "mid-run-filter-ran")
	helper := filepath.Join(dir, "git-helper.sh")
	if err := os.WriteFile(helper, []byte("#!/bin/sh\ntouch \""+marker+"\"\ncat\n"), 0o700); err != nil {
		t.Fatalf("WriteFile helper: %v", err)
	}
	for key, value := range map[string]string{
		"filter.midrun.clean":   helper,
		"filter.sneaky.process": helper,
	} {
		cmd := exec.Command(gitBin, "-C", dir, "config", key, value)
		if out, err := cmd.CombinedOutput(); err != nil {
			t.Fatalf("configure repository %s: %v: %s", key, err, out)
		}
	}
	attributes := filepath.Join(dir, ".git", "info", "attributes")
	if err := os.WriteFile(attributes, []byte("a.txt filter=midrun\nc.txt filter=sneaky\n"), 0o600); err != nil {
		t.Fatalf("WriteFile attributes: %v", err)
	}
	if err := os.WriteFile(filepath.Join(dir, "a.txt"), []byte("changed\n"), 0o600); err != nil {
		t.Fatalf("WriteFile change: %v", err)
	}
	if err := os.WriteFile(filepath.Join(dir, "c.txt"), []byte("new file\n"), 0o600); err != nil {
		t.Fatalf("WriteFile untracked: %v", err)
	}

	// computePatch gets exactly what codexExec would hand it: the pre-run
	// binary and override list, and a baseline that observed a clean tree.
	patch, _, truncated := computePatch(context.Background(), gitBin, hardened, dir, true, baseline,
		[]fileChange{{Path: "a.txt", ChangeType: "update"}})
	if truncated || !strings.Contains(patch, "+changed") {
		t.Fatalf("computePatch = (%q, truncated %v), want an ordinary unified diff", patch, truncated)
	}
	if _, err := os.Stat(marker); !os.IsNotExist(err) {
		t.Fatalf("filter installed mid-run executed outside the Codex sandbox: Stat marker error = %v", err)
	}
}

// TestHardenedGitConfigRejectsUnsafeFilterKeys proves the fail-closed
// response to a filter name that cannot be safely disabled via `-c
// NAME=VALUE`: Git parses that argument by splitting at the first `=`, and a
// quoted config subsection may itself legally contain one, so a key such as
// `filter.evil=driver.clean` does not mean what `key + "="` would assume.
// Appending `=` to it sets `filter.evil` (to a garbage value) rather than
// disabling the attacker's actual driver, `filter.evil=driver`. There is no
// override spelling that closes that gap, so hardenedGitConfig has to refuse
// the whole listing rather than silently leave that one filter enabled.
func TestHardenedGitConfigRejectsUnsafeFilterKeys(t *testing.T) {
	gitBin := realGitBinary(t)
	t.Setenv(gitBinaryEnv, gitBin)

	dir := t.TempDir()
	initRepoWithCommit(t, gitBin, dir)

	// A quoted subsection is the only way to put "=" or other unusual bytes
	// into a config subsection name.
	cmd := exec.Command(gitBin, "-C", dir, "config", `filter."evil=driver".clean`, "true")
	if out, err := cmd.CombinedOutput(); err != nil {
		t.Fatalf("configure repository filter.\"evil=driver\".clean: %v: %s", err, out)
	}

	_, _, ok := hardenedGitConfig(context.Background(), gitBin, dir)
	if ok {
		t.Fatal("hardenedGitConfig succeeded with a filter key containing \"=\" in its subsection, want fail-closed (ok=false)")
	}
}

func TestBoundedWriterCapsAtMaxBytes(t *testing.T) {
	w := &boundedWriter{max: 5}
	n, err := w.Write([]byte("hello world"))
	if err != nil {
		t.Fatalf("Write: unexpected error: %v", err)
	}
	if n != len("hello world") {
		t.Fatalf("Write returned n=%d, want %d (io.Writer contract: n < len(p) requires a non-nil "+
			"error, and this writer chooses to silently cap rather than error)", n, len("hello world"))
	}
	if len(w.buf) != 5 {
		t.Fatalf("buf = %q (%d bytes), want exactly 5 bytes retained", w.buf, len(w.buf))
	}
	if !w.truncated {
		t.Error("truncated = false, want true")
	}
}
