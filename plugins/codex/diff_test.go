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
func prepareForTest(t *testing.T, dir string) (gitBin string, hardened *gitHardening) {
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
		// program, and hardenedGitConfig disables it separately - so a test that
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

	_, _, cleanup, ok := prepareHardenedGit(context.Background(), dir)
	if cleanup != nil {
		t.Cleanup(cleanup)
	}
	if ok {
		t.Fatal("hardening succeeded with a filter key containing \"=\" in its subsection, want fail-closed (ok=false)")
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

// TestComputePatchIgnoresTheWorkersOwnGitEnvironment is the direction #700
// names that the repository sweep cannot reach: the config the *machine*
// supplies rather than the config the repository does. Every Git invocation
// here used to inherit os.Environ(), so GIT_EXTERNAL_DIFF named a program
// directly, GIT_CONFIG_GLOBAL named a file full of them, and $HOME named a
// directory where a DANGER_FULL_ACCESS run can write ~/.gitconfig itself -
// none of which the `-c` sweep over the repository's own keys ever looks at.
//
// The hook here sits in the checkout's default .git/hooks with no
// core.hooksPath configured at all, which is the case a sweep that only
// overrides configured keys cannot see: there is no key to override.
func TestComputePatchIgnoresTheWorkersOwnGitEnvironment(t *testing.T) {
	gitBin := realGitBinary(t)
	t.Setenv(gitBinaryEnv, gitBin)

	dir := t.TempDir()
	initRepoWithCommit(t, gitBin, dir)

	// GIT_DIR and GIT_WORK_TREE are the exfiltration half of the same
	// inheritance: they outrank the `-C <workDir>` every command here passes,
	// so an ambient pair pointed at a second repository has these commands
	// read *that* repository and return its uncommitted contents to the
	// caller as this run's patch. Verified against git 2.43: `git -C other
	// rev-parse --absolute-git-dir` with GIT_DIR set reports the GIT_DIR one.
	elsewhere := t.TempDir()
	initRepoWithCommit(t, gitBin, elsewhere)
	const secretElsewhere = "SECRET_FROM_ANOTHER_REPOSITORY"
	if err := os.WriteFile(filepath.Join(elsewhere, "a.txt"), []byte(secretElsewhere+"\n"), 0o600); err != nil {
		t.Fatalf("WriteFile in the other repository: %v", err)
	}
	t.Setenv("GIT_DIR", filepath.Join(elsewhere, ".git"))
	t.Setenv("GIT_WORK_TREE", elsewhere)

	markerDir := t.TempDir()
	marker := func(name string) string { return filepath.Join(markerDir, name) }
	helper := func(name string) string {
		path := filepath.Join(markerDir, name+".sh")
		if err := os.WriteFile(path, []byte("#!/bin/sh\ntouch \""+marker(name)+"\"\ncat\n"), 0o700); err != nil {
			t.Fatalf("WriteFile helper %s: %v", name, err)
		}
		return path
	}

	// A global config file naming two programs, reachable three different
	// ways: by GIT_CONFIG_GLOBAL directly, and - for a run that only got as
	// far as writing a dotfile - as $HOME/.gitconfig and
	// $XDG_CONFIG_HOME/git/config.
	fakeHome := t.TempDir()
	globalConfig := "[core]\n\tfsmonitor = " + helper("global-fsmonitor") +
		"\n[diff]\n\texternal = " + helper("global-external-diff") + "\n"
	if err := os.WriteFile(filepath.Join(fakeHome, ".gitconfig"), []byte(globalConfig), 0o600); err != nil {
		t.Fatalf("WriteFile ~/.gitconfig: %v", err)
	}
	t.Setenv("HOME", fakeHome)
	t.Setenv("XDG_CONFIG_HOME", fakeHome)
	t.Setenv("GIT_CONFIG_GLOBAL", filepath.Join(fakeHome, ".gitconfig"))
	t.Setenv("GIT_EXTERNAL_DIFF", helper("env-external-diff"))

	// No core.hooksPath is set: this is the repository's own default hooks
	// directory, which `git add` fires post-index-change from.
	hookPath := filepath.Join(dir, ".git", "hooks", "post-index-change")
	if err := os.WriteFile(hookPath, []byte("#!/bin/sh\ntouch \""+marker("default-hook")+"\"\n"), 0o700); err != nil {
		t.Fatalf("WriteFile default hook: %v", err)
	}

	if err := os.WriteFile(filepath.Join(dir, "a.txt"), []byte("changed\n"), 0o600); err != nil {
		t.Fatalf("WriteFile change: %v", err)
	}
	if err := os.WriteFile(filepath.Join(dir, "c.txt"), []byte("new file\n"), 0o600); err != nil {
		t.Fatalf("WriteFile untracked: %v", err)
	}

	gitBin, hardened := prepareForTest(t, dir)
	if hardened == nil {
		t.Fatal("prepareHardenedGit refused an ordinary checkout")
	}
	baseline := observeWorkspace(context.Background(), gitBin, hardened, dir, true)
	if !baseline.observed {
		t.Fatalf("observeWorkspace = %+v, want observed=true", baseline)
	}

	patch, _, truncated := computePatch(context.Background(), gitBin, hardened, dir, true, workspaceBaseline{observed: true},
		[]fileChange{{Path: "a.txt", ChangeType: "update"}})
	if truncated || !strings.Contains(patch, "+changed") {
		t.Fatalf("computePatch = (%q, truncated %v), want an ordinary unified diff", patch, truncated)
	}
	if strings.Contains(patch, secretElsewhere) {
		t.Fatalf("patch carries content from the repository GIT_DIR named, not from working_context: %q", patch)
	}

	for _, name := range []string{"global-fsmonitor", "global-external-diff", "env-external-diff", "default-hook"} {
		if _, err := os.Stat(marker(name)); !os.IsNotExist(err) {
			t.Errorf("%s ran: a program named by the worker's own environment or home directory executed during patch generation (Stat error = %v)", name, err)
		}
	}
}

// TestHardenedGitRefusesAnUnrecognizedConfigKey is the inversion #700 asks
// for. `remote.<name>.uploadpack` and `submodule.<name>.update` both name a
// program, and neither appears anywhere in this plugin's override list -
// they are exactly the shape of "a key nobody thought of". The recognizer
// does not have to know they are dangerous; it has to not recognize them,
// and refusing is what that costs.
func TestHardenedGitRefusesAnUnrecognizedConfigKey(t *testing.T) {
	gitBin := realGitBinary(t)
	t.Setenv(gitBinaryEnv, gitBin)

	for _, tc := range []struct{ key, value string }{
		{"remote.origin.uploadpack", "/bin/false"},
		{"submodule.libfoo.update", "!/bin/false"},
		{"nobody.thought.ofthis", "1"},
	} {
		t.Run(tc.key, func(t *testing.T) {
			dir := t.TempDir()
			initRepoWithCommit(t, gitBin, dir)
			cmd := exec.Command(gitBin, "-C", dir, "config", tc.key, tc.value)
			if out, err := cmd.CombinedOutput(); err != nil {
				t.Fatalf("configure repository %s: %v: %s", tc.key, err, out)
			}

			preparedBin, hardened := prepareForTest(t, dir)
			if hardened != nil {
				t.Fatalf("hardening accepted a repository configuring %s, want fail-closed", tc.key)
			}
			patch, files, _ := computePatch(context.Background(), preparedBin, hardened, dir, true,
				workspaceBaseline{observed: true}, []fileChange{{Path: "a.txt", ChangeType: "update"}})
			if patch != "" {
				t.Fatalf("computePatch = %q, want no patch when hardening refused", patch)
			}
			if len(files) != 1 {
				t.Fatal("files_changed should still pass through unchanged when the patch is refused")
			}
		})
	}
}

// TestHardenedGitRefusesARedirectedGitDir covers the arrangements where the
// workspace is not the plain checkout it appears to be: a `.git` file
// pointing at a repository elsewhere on the worker, a symlinked `.git`, and
// a workspace that is merely a subdirectory of a larger repository. In the
// first two the config, hooks and objects Git reads belong to a repository
// outside working_context; in the third `git diff` reports the whole
// repository, so the patch returned to the caller carries changes from
// outside the jail.
func TestHardenedGitRefusesARedirectedGitDir(t *testing.T) {
	gitBin := realGitBinary(t)
	t.Setenv(gitBinaryEnv, gitBin)

	t.Run("gitdir file", func(t *testing.T) {
		real := t.TempDir()
		initRepoWithCommit(t, gitBin, real)

		workspace := t.TempDir()
		if err := os.WriteFile(filepath.Join(workspace, ".git"),
			[]byte("gitdir: "+filepath.Join(real, ".git")+"\n"), 0o600); err != nil {
			t.Fatalf("WriteFile .git: %v", err)
		}

		if _, hardened := prepareForTest(t, workspace); hardened != nil {
			t.Fatal("hardening accepted a workspace whose .git is a gitdir: file pointing elsewhere, want fail-closed")
		}
	})

	t.Run("symlinked gitdir", func(t *testing.T) {
		real := t.TempDir()
		initRepoWithCommit(t, gitBin, real)

		workspace := t.TempDir()
		if err := os.Symlink(filepath.Join(real, ".git"), filepath.Join(workspace, ".git")); err != nil {
			t.Skipf("this platform cannot create the symlink this test is about: %v", err)
		}

		if _, hardened := prepareForTest(t, workspace); hardened != nil {
			t.Fatal("hardening accepted a workspace whose .git is a symlink, want fail-closed")
		}
	})

	t.Run("subdirectory of a repository", func(t *testing.T) {
		repo := t.TempDir()
		initRepoWithCommit(t, gitBin, repo)
		sub := filepath.Join(repo, "sub")
		if err := os.Mkdir(sub, 0o700); err != nil {
			t.Fatalf("Mkdir: %v", err)
		}

		if _, hardened := prepareForTest(t, sub); hardened != nil {
			t.Fatal("hardening accepted a workspace that is a subdirectory of a larger repository, want fail-closed")
		}
	})

	// The commondir case is the one --absolute-git-dir cannot see: the
	// workspace's gitdir genuinely *is* its own .git, so every check that
	// only compares the gitdir passes - but Git reads .git/commondir to set
	// GIT_COMMON_DIR, from which refs and objects are actually resolved, so a
	// commondir pointing at a second repository makes the add and diff read
	// that repository. Confirmed on git 2.43: a clean workspace produced a
	// patch carrying a deleted secret file from the pointed-to repo.
	t.Run("commondir pointing at another repository", func(t *testing.T) {
		// The pointed-to repository: a committed secret, then removed from the
		// working tree, so `git diff HEAD` renders it as a deletion carrying
		// its content - which is what would leak.
		const secret = "TOPSECRET_FROM_COMMON_DIR"
		other := t.TempDir()
		initRepoWithCommit(t, gitBin, other)
		if err := os.WriteFile(filepath.Join(other, "secret.txt"), []byte(secret+"\n"), 0o600); err != nil {
			t.Fatalf("WriteFile secret: %v", err)
		}
		add := exec.Command(gitBin, "-C", other, "add", "secret.txt")
		if out, err := add.CombinedOutput(); err != nil {
			t.Fatalf("git add secret: %v: %s", err, out)
		}
		commit := exec.Command(gitBin, "-C", other, "commit", "-q", "-m", "add secret")
		commit.Env = append(os.Environ(),
			"GIT_AUTHOR_NAME=t", "GIT_AUTHOR_EMAIL=t@e", "GIT_COMMITTER_NAME=t", "GIT_COMMITTER_EMAIL=t@e")
		if out, err := commit.CombinedOutput(); err != nil {
			t.Fatalf("git commit secret: %v: %s", err, out)
		}

		workspace := t.TempDir()
		initRepoWithCommit(t, gitBin, workspace)
		if err := os.WriteFile(filepath.Join(workspace, ".git", "commondir"),
			[]byte(filepath.Join(other, ".git")+"\n"), 0o600); err != nil {
			t.Fatalf("WriteFile commondir: %v", err)
		}

		preparedBin, hardened := prepareForTest(t, workspace)
		if hardened != nil {
			t.Fatal("hardening accepted a workspace whose .git/commondir points at another repository, want fail-closed")
		}

		// Belt-and-suspenders on the exfiltration itself: even handed the
		// refused hardening, computePatch must produce no patch, and certainly
		// not one carrying the other repository's secret.
		patch, _, _ := computePatch(context.Background(), preparedBin, hardened, workspace, true,
			workspaceBaseline{observed: true}, []fileChange{{Path: "a.txt", ChangeType: "update"}})
		if patch != "" {
			t.Fatalf("computePatch returned a patch for a commondir-redirected workspace: %q", patch)
		}
		if strings.Contains(patch, secret) {
			t.Fatalf("patch carries the secret from the repository commondir pointed at: %q", patch)
		}
	})
}

// TestClassifyConfigKey pins the three answers by name, including the
// capitalization case: Git's section and leaf names are case-insensitive,
// so a recognizer comparing raw spelling would let `Core.FSMonitor` through
// as unrecognized (harmless here, since unrecognized refuses) and, worse,
// would fail to sweep `Filter.Evil.Clean`.
func TestClassifyConfigKey(t *testing.T) {
	for _, tc := range []struct {
		key  string
		want configKeyClass
	}{
		{"core.repositoryformatversion", configKeyInert},
		{"Core.FSMonitor", configKeyInert},
		{"core.hooksPath", configKeyInert},
		{"user.email", configKeyInert},
		{"remote.origin.url", configKeyInert},
		{"branch.main.merge", configKeyInert},
		{"alias.st", configKeyInert},
		{"lfs.repositoryformatversion", configKeyInert},
		{"filter.lfs.clean", configKeySwept},
		{"Filter.Evil.Process", configKeySwept},
		{"filter.evil.smudge", configKeySwept},
		{"diff.lfs.textconv", configKeySwept},
		{"credential.https://example.com.helper", configKeySwept},
		{"gpg.x509.program", configKeySwept},
		{"remote.origin.uploadpack", configKeyUnrecognized},
		{"submodule.x.update", configKeyUnrecognized},
		{"merge.evil.driver", configKeyUnrecognized},
		{"core.worktree", configKeyUnrecognized},
		{"survey", configKeyUnrecognized},
	} {
		if got := classifyConfigKey(tc.key); got != tc.want {
			t.Errorf("classifyConfigKey(%q) = %d, want %d", tc.key, got, tc.want)
		}
	}
}

// TestResetWorkingContextFailsWithoutGitBinary proves resetWorkingContext
// itself fails rather than silently doing nothing when it has nothing to
// work with - see its own doc comment on why a requested reset that did not
// happen must be reported, unlike computePatch's best-effort fallback.
func TestResetWorkingContextFailsWithoutGitBinary(t *testing.T) {
	if err := resetWorkingContext(context.Background(), "", nil, t.TempDir()); err == nil {
		t.Fatal("resetWorkingContext with no git binary configured: got no error, want one")
	}
}

func TestResetWorkingContextFailsWhenDirIsNotAGitRepo(t *testing.T) {
	gitBin := realGitBinary(t)
	t.Setenv(gitBinaryEnv, gitBin)

	dir := t.TempDir()
	preparedBin, hardened := prepareForTest(t, dir)
	if err := resetWorkingContext(context.Background(), preparedBin, hardened, dir); err == nil {
		t.Fatal("resetWorkingContext against a plain directory: got no error, want one")
	}
}

// TestResetWorkingContextDiscardsTrackedAndUntrackedChanges is the worked
// example this input exists for: a checkout left dirty by a stand-in for a
// previous agentic turn - a modified tracked file and a new untracked one -
// restored to a clean baseline the same way computePatch's own baseline
// check requires (workspaceBaseline.dirty == false) before it will ever
// produce a patch.
func TestResetWorkingContextDiscardsTrackedAndUntrackedChanges(t *testing.T) {
	gitBin := realGitBinary(t)
	t.Setenv(gitBinaryEnv, gitBin)

	dir := t.TempDir()
	initRepoWithCommit(t, gitBin, dir)

	if err := os.WriteFile(filepath.Join(dir, "a.txt"), []byte("one\ntwo\n"), 0o600); err != nil {
		t.Fatalf("WriteFile a.txt: %v", err)
	}
	if err := os.WriteFile(filepath.Join(dir, "untracked.txt"), []byte("new\n"), 0o600); err != nil {
		t.Fatalf("WriteFile untracked.txt: %v", err)
	}

	preparedBin, hardened := prepareForTest(t, dir)
	if err := resetWorkingContext(context.Background(), preparedBin, hardened, dir); err != nil {
		t.Fatalf("resetWorkingContext: unexpected error: %v", err)
	}

	got, err := os.ReadFile(filepath.Join(dir, "a.txt"))
	if err != nil {
		t.Fatalf("ReadFile a.txt after reset: %v", err)
	}
	if string(got) != "one\n" {
		t.Errorf("a.txt after reset = %q, want the committed content %q", got, "one\n")
	}
	if _, err := os.Stat(filepath.Join(dir, "untracked.txt")); !os.IsNotExist(err) {
		t.Errorf("untracked.txt after reset: stat err = %v, want IsNotExist", err)
	}

	baseline := observeWorkspace(context.Background(), preparedBin, hardened, dir, true)
	if !baseline.observed || baseline.dirty {
		t.Errorf("baseline after reset = %+v, want observed and clean", baseline)
	}
}

// TestResetWorkingContextRefusesWhenWorkingContextIsASubdirectory proves
// resetWorkingContext inherits computePatch's own containment answer for
// this shape rather than a weaker one of its own: hardenedGitConfig's
// gitWorktreeIsPlain check requires workDir to be a checkout's own top
// level (see githarden.go), so prepareHardenedGit already returns ok=false
// for a working_context that is a subdirectory of a larger repository -
// the same "no patch" cost computePatch's own doc comment states for a
// linked worktree or a submodule checkout. codexExec's own resetRequested
// gate (exec.go) turns that ok=false into a hard failure rather than
// computePatch's silent best-effort skip - see
// TestCodexExecResetWorkingContextFailsClosedWithoutGitConfigured - which
// is what keeps this input from ever running a git command whose "."
// pathspec resolves somewhere the plugin has not already validated as a
// checkout's own root.
func TestResetWorkingContextRefusesWhenWorkingContextIsASubdirectory(t *testing.T) {
	gitBin := realGitBinary(t)
	t.Setenv(gitBinaryEnv, gitBin)

	repo := t.TempDir()
	sub := filepath.Join(repo, "sub")
	if err := os.Mkdir(sub, 0o755); err != nil {
		t.Fatalf("Mkdir sub: %v", err)
	}
	if err := os.WriteFile(filepath.Join(sub, "a.txt"), []byte("one\n"), 0o600); err != nil {
		t.Fatalf("WriteFile sub/a.txt: %v", err)
	}
	initRepoWithCommit(t, gitBin, repo)

	preparedBin, hardened := prepareForTest(t, sub)
	if err := resetWorkingContext(context.Background(), preparedBin, hardened, sub); err == nil {
		t.Fatal("resetWorkingContext against a subdirectory of a larger checkout: got no error, want one")
	}
}

// TestResetWorkingContextLeavesASiblingDirectoryUntouched is a narrower,
// still-worth-having containment check for the shape reset *does* run
// against - a plain checkout at working_context's own root: a completely
// separate directory under the same FLOWSTATE_CODEX_WORKDIR_ROOT, holding
// its own unrelated files and no git repository of its own, must survive a
// reset of its sibling untouched. Every git command resetWorkingContext
// runs is scoped with "-C workDir", so this is what proves that scoping
// actually holds rather than assuming it from reading the code.
func TestResetWorkingContextLeavesASiblingDirectoryUntouched(t *testing.T) {
	gitBin := realGitBinary(t)
	t.Setenv(gitBinaryEnv, gitBin)

	root := t.TempDir()
	repoDir := filepath.Join(root, "repo")
	if err := os.Mkdir(repoDir, 0o755); err != nil {
		t.Fatalf("Mkdir repo: %v", err)
	}
	initRepoWithCommit(t, gitBin, repoDir)
	if err := os.WriteFile(filepath.Join(repoDir, "a.txt"), []byte("one\ntwo\n"), 0o600); err != nil {
		t.Fatalf("WriteFile a.txt (dirty): %v", err)
	}

	sibling := filepath.Join(root, "sibling")
	if err := os.Mkdir(sibling, 0o755); err != nil {
		t.Fatalf("Mkdir sibling: %v", err)
	}
	if err := os.WriteFile(filepath.Join(sibling, "untouched.txt"), []byte("do not delete me\n"), 0o600); err != nil {
		t.Fatalf("WriteFile sibling/untouched.txt: %v", err)
	}

	preparedBin, hardened := prepareForTest(t, repoDir)
	if err := resetWorkingContext(context.Background(), preparedBin, hardened, repoDir); err != nil {
		t.Fatalf("resetWorkingContext: unexpected error: %v", err)
	}

	if got, err := os.ReadFile(filepath.Join(sibling, "untouched.txt")); err != nil || string(got) != "do not delete me\n" {
		t.Errorf("sibling/untouched.txt after a reset of repoDir = (%q, %v), want it left exactly as it was", got, err)
	}
}
