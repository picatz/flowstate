package main

import (
	"context"
	"os"
	"os/exec"
	"path/filepath"
	"slices"
	"strings"
	"time"
)

// gitBinaryEnv names the environment variable this plugin reads for a git
// binary, used only to render a unified diff of what a WORKSPACE_WRITE or
// DANGER_FULL_ACCESS run changed under working_context.
//
// Unset by default and never falls back to $PATH, for the same reason
// codexBinaryEnv does not: this plugin's own binary discovery rule
// (explicit configuration, never ambient) applies to every subprocess it
// runs, not only to codex itself. Unlike FLOWSTATE_CODEX_BIN, this one is
// optional - a deployment that never needs codex.exec's patch output (most
// uses of this task are read-only queries, where sandbox_mode's own
// fail-closed default means there is nothing to diff regardless) does not
// have to configure a second binary it will not use.
const gitBinaryEnv = "FLOWSTATE_CODEX_GIT_BIN"

// gitTimeout bounds a single git invocation this plugin makes on its own,
// independent of the step's own deadline (runCtx, already derived from it)
// - a diff computation that hangs should not consume the whole of a step's
// budget silently.
const gitTimeout = 30 * time.Second

// prepareHardenedGit resolves the git binary, checks that workDir is a
// checkout this plugin is willing to touch, creates the empty directory
// core.hooksPath will point at, and computes the `-c` overrides and the
// scrubbed environment the *pre-run* Git invocations over it must carry -
// once, before the first Git command touches the repository.
//
// Computing this before anything else matters for more than avoiding
// duplicate work: the baseline read (`git status`, in observeWorkspace)
// touches a task-controlled checkout and runs repository-configured
// programs if it runs unhardened. A version of this that computed hardening
// only inside computePatch left the baseline read - which runs *first*,
// before the codex subprocess even starts - exposed to exactly the
// fsmonitor hook and content filters the later hardening was built to
// stop. Calling this once, before the first Git command touches the
// repository, closes that gap.
//
// What this returns is *not* a prefix computePatch can reuse verbatim. The
// swept overrides in it are enumerated by name from the repository's config
// as it stands now, and a WORKSPACE_WRITE run mutates that config as freely
// as it mutates any other file under working_context - so computePatch,
// which runs after the subprocess finishes, re-enumerates against the
// mutated config rather than carrying this list across the run. See
// computePatch's own doc comment for the attack that reuse enables. The
// environment and the hooks directory are fixed and do carry across: neither
// is derived from anything the repository says.
//
// Fails closed on the empty return: a caller that gets ok=false must treat
// the workspace exactly as it would treat "not a git checkout" - see
// computePatch and observeWorkspace's own doc comments.
func prepareHardenedGit(ctx context.Context, workDir string) (gitBin string, hardened *gitHardening, cleanup func(), ok bool) {
	gitBin = os.Getenv(gitBinaryEnv)
	if gitBin == "" {
		return "", nil, nil, false
	}
	info, err := os.Stat(gitBin)
	if err != nil || info.IsDir() || info.Mode()&0o111 == 0 {
		return "", nil, nil, false
	}

	// One directory per run, holding two things a Git invocation must not
	// take from the machine it happens to run on: the (empty) hooks
	// directory, and the home directory Git resolves ~/.gitconfig against.
	home, err := os.MkdirTemp("", "codex-git-")
	if err != nil {
		return "", nil, nil, false
	}
	cleanup = func() { _ = os.RemoveAll(home) }
	hooks := filepath.Join(home, "hooks")
	if err := os.Mkdir(hooks, 0o700); err != nil {
		cleanup()
		return "", nil, nil, false
	}

	env := gitEnv(home)
	args, ok := hardenedGitConfig(ctx, gitBin, workDir, env, hooks)
	if !ok {
		cleanup()
		return "", nil, nil, false
	}
	return gitBin, &gitHardening{args: args, env: env, hooksDir: hooks}, cleanup, true
}

// computePatch renders a unified diff of what changed under workDir,
// working from a git checkout that was already there before this run
// started - this plugin creates no checkout of its own, on purpose, for the
// same no-shared-workspace reason plugins/vcs has no vcs.clone (see doc.go).
//
// gitBin and hardened come from prepareHardenedGit, computed once by the
// caller before the run started and shared with observeWorkspace. This
// function uses them only as its fail-closed gate: the Git commands it runs
// carry a *fresh* set of overrides, enumerated after the codex subprocess
// finished. The pre-run list cannot be reused, because its filter overrides
// name the drivers the repository configured *then* - and in
// WORKSPACE_WRITE mode the repository's config is the run's to mutate. A
// run that adds a new `filter.<name>.clean` or `.process` key (a config
// include under working_context plus a .gitattributes entry is enough)
// would have the `git add` and `git diff` below reload the mutated config
// while overriding only the stale key list, executing the newly named
// filter as this worker, outside the Codex sandbox. Re-running
// hardenedGitConfig also re-checks that the workspace is still a plain
// checkout whose gitdir is its own `.git` directory, which a run that
// replaced `.git` with a `gitdir:` file or a symlink would otherwise have
// left true only at the moment it was first asked (see
// gitWorktreeIsPlain). The fixed overrides and the scrubbed environment
// have no such time-of-check gap - only the parts read out of the
// repository do - but recomputing refreshes everything at once.
//
// It returns filesChanged unchanged, and no patch, whenever there is
// nothing to diff: a read-only run (mutating false) cannot have changed
// anything; an empty working_context has no directory to diff; no files
// reported changing means nothing for git to have tracked; and no git
// binary configured, hardening unavailable, or workDir not being a git
// checkout at all are all treated as "patch not available" rather than as
// an error, since a working_context is not required to be a git repository
// - only diffing one requires that.
func computePatch(ctx context.Context, gitBin string, hardened *gitHardening, workDir string, mutating bool, baseline workspaceBaseline, filesChanged []fileChange) (patch string, files []fileChange, truncated bool) {
	if !mutating || workDir == "" || len(filesChanged) == 0 {
		return "", filesChanged, false
	}

	// A patch is only honest when it is *this run's* delta. `git diff HEAD` in
	// a workspace that already had uncommitted edits reports those too, and
	// this output feeds git.commit_push directly - so a dirty start would
	// commit work this run never did. Pre-existing edits cannot be subtracted
	// after the fact, so this fails closed: no patch, and the caller still
	// learns what changed from files_changed. See workspaceBaseline.
	if !baseline.observed || baseline.dirty {
		return "", filesChanged, false
	}

	if gitBin == "" || hardened == nil {
		return "", filesChanged, false
	}

	// Re-enumerate the overrides against the config as the run left it, not
	// as it was found - see this function's doc comment. Fails closed the
	// same way the pre-run computation does: a config that can no longer be
	// safely swept (unreadable, over its bound, holding an unrecognized key,
	// or holding a key no `-c` override can disable), or a workspace whose
	// gitdir the run redirected, means no patch rather than a patch computed
	// with a repository-named program still live.
	fresh, ok := hardenedGitConfig(ctx, gitBin, workDir, hardened.env, hardened.hooksDir)
	if !ok {
		return "", filesChanged, false
	}

	// Files the run created are untracked, and `git diff` does not report an
	// untracked file at all - so without this the patch would silently omit
	// exactly the new files a downstream commit most needs. --intent-to-add
	// records their existence without staging content, which is what makes
	// them appear below as additions. Best-effort like the rest of this
	// function: if it fails, the tracked changes still diff.
	_, _, _ = runGitBounded(ctx, gitBin, workDir, hardened.env, maxPatchBytes,
		append(slices.Clone(fresh), "add", "--intent-to-add", "--", ".")...)

	// --no-ext-diff and --no-textconv cover the two diff drivers; the content
	// filters, the hooks, the fsmonitor hook, and the promisor/submodule
	// paths are covered by hardened - see hardenedGitConfig.
	// --ignore-submodules=all is belt-and-suspenders alongside
	// submodule.recurse=false in hardened: this command has no business
	// entering a nested checkout at all to render a patch of this one.
	out, ok, truncatedOutput := runGitBounded(ctx, gitBin, workDir, hardened.env, maxPatchBytes,
		append(slices.Clone(fresh), "diff", "--no-ext-diff", "--no-textconv", "--no-color", "--ignore-submodules=all", "-M", "HEAD")...)
	if !ok {
		return "", filesChanged, false
	}

	return out, filesChanged, truncatedOutput
}

// workspaceBaseline is what was true of working_context *before* a run
// started: whether this plugin managed to look, and whether it found
// uncommitted changes already there.
//
// Recorded before the subprocess starts, because afterwards the two are
// indistinguishable - an edit already present and an edit the run made look
// identical to `git diff`, and guessing wrong in the permissive direction is
// how a downstream commit picks up work nobody asked it to.
//
// observed is false when this plugin could not tell (no git binary
// configured, hardening unavailable, working_context is not a checkout),
// which is treated exactly like dirty: patch output is a claim about what a
// run did, and a claim that cannot be checked is not made.
type workspaceBaseline struct {
	observed bool
	dirty    bool
}

// observeWorkspace records whether workDir has uncommitted changes before a
// run begins. See [workspaceBaseline].
//
// gitBin and hardened come from prepareHardenedGit, called by codexExec
// before this and passed in rather than recomputed here - see that
// function's doc comment for why the baseline read has to carry the same
// hardening the later patch computation does, not its own.
func observeWorkspace(ctx context.Context, gitBin string, hardened *gitHardening, workDir string, mutating bool) workspaceBaseline {
	if !mutating || workDir == "" || gitBin == "" || hardened == nil {
		return workspaceBaseline{}
	}

	// --porcelain reports tracked modifications and untracked files alike,
	// which is the whole question here: any output at all means something was
	// already there. --ignore-submodules=all keeps this from wandering into a
	// submodule's own configuration merely to answer that question - see
	// hardenedGitConfig's doc comment on the submodule case.
	out, ok, _ := runGitBounded(ctx, gitBin, workDir, hardened.env, maxPatchBytes,
		append(slices.Clone(hardened.args), "status", "--porcelain", "--ignore-submodules=all")...)
	if !ok {
		return workspaceBaseline{}
	}

	return workspaceBaseline{observed: true, dirty: strings.TrimSpace(out) != ""}
}

// runGitBounded runs one git command with its combined output capped at
// maxBytes, the same boundedWriter shape plugins/vcs/diff.go's
// boundedPatchWriter uses for the identical reason: the cap has to be
// applied to the write path itself, not to a finished string, or a
// pathological diff has already cost the memory the cap exists to avoid
// before anything trims it.
//
// GIT_NO_LAZY_FETCH=1 is set on every invocation, not only the hardened
// ones: a partial clone's promisor remote can be configured to use an
// `ext::` transport, and that remote's transport helper is a
// repository-controlled program the moment any command here needs an object
// this checkout does not have locally - the environment variable disables
// the automatic fetch that would otherwise trigger it, and this plugin fails
// closed (missing-object errors surface as an ordinary command failure,
// covered above) rather than materializing the object by running the
// helper.
func runGitBounded(ctx context.Context, gitBin, dir string, env []string, maxBytes int, args ...string) (string, bool, bool) {
	runCtx, cancel := context.WithTimeout(ctx, gitTimeout)
	defer cancel()

	full := append([]string{"-C", dir}, args...)
	cmd := exec.CommandContext(runCtx, gitBin, full...)
	cmd.Env = env

	w := &boundedWriter{max: maxBytes}
	cmd.Stdout = w
	cmd.Stderr = nil // never surfaced: see computePatch, a failed diff is silently "not available"

	err := cmd.Run()
	if err != nil && !w.truncated {
		return "", false, false
	}

	return string(w.buf), true, w.truncated
}

// boundedWriter accepts writes up to max bytes total, cut on a rune
// boundary, and refuses every write past it - copied in shape from
// plugins/vcs/diff.go's boundedPatchWriter (a different module, so not
// imported) because the same reasoning applies: this is what turns maxBytes
// into an actual memory bound on the writer rather than a bound on a string
// that already finished being built.
type boundedWriter struct {
	buf       []byte
	max       int
	truncated bool
}

func (b *boundedWriter) Write(p []byte) (int, error) {
	if b.truncated {
		return len(p), nil
	}
	remaining := b.max - len(b.buf)
	if remaining <= 0 {
		b.truncated = true
		return len(p), nil
	}
	if len(p) > remaining {
		n := remaining
		for n > 0 && !isRuneStart(p[n]) {
			n--
		}
		b.buf = append(b.buf, p[:n]...)
		b.truncated = true
		return len(p), nil
	}
	b.buf = append(b.buf, p...)
	return len(p), nil
}
