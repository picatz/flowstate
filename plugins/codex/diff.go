package main

import (
	"context"
	"os"
	"os/exec"
	"sort"
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

// computePatch renders a unified diff of what changed under workDir,
// working from a git checkout that was already there before this run
// started - this plugin creates no checkout of its own, on purpose, for the
// same no-shared-workspace reason plugins/vcs has no vcs.clone (see doc.go).
//
// It returns filesChanged unchanged, and no patch, whenever there is
// nothing to diff: a read-only run (mutating false) cannot have changed
// anything; an empty working_context has no directory to diff; no files
// reported changing means nothing for git to have tracked; and no git
// binary configured (gitBinaryEnv unset) or workDir not being a git
// checkout at all are both treated as "patch not available" rather than as
// an error, since a working_context is not required to be a git repository
// - only diffing one requires that.
func computePatch(ctx context.Context, workDir string, mutating bool, baseline workspaceBaseline, filesChanged []fileChange) (patch string, files []fileChange, truncated bool) {
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

	gitBin := os.Getenv(gitBinaryEnv)
	if gitBin == "" {
		return "", filesChanged, false
	}
	info, err := os.Stat(gitBin)
	if err != nil || info.IsDir() || info.Mode()&0o111 == 0 {
		return "", filesChanged, false
	}

	if !isGitWorkTree(ctx, gitBin, workDir) {
		return "", filesChanged, false
	}

	// Files the run created are untracked, and `git diff` does not report an
	// untracked file at all - so without this the patch would silently omit
	// exactly the new files a downstream commit most needs. --intent-to-add
	// records their existence without staging content, which is what makes
	// them appear below as additions. Best-effort like the rest of this
	// function: if it fails, the tracked changes still diff.
	_, _, _ = runGitBounded(ctx, gitBin, workDir, maxPatchBytes,
		append(hardenedGitConfig(), "add", "--intent-to-add", "--", ".")...)

	// The repository is controlled by the task and may configure helpers that
	// execute as the worker, outside the Codex sandbox. --no-ext-diff and
	// --no-textconv cover diff drivers, but Git still applies clean/process
	// filters when it converts working-tree content for comparison. Discover
	// every configured filter command and override it with an empty value for
	// this invocation; an empty filter command means no filter. If the config
	// cannot be enumerated, fail closed rather than render with an unknown
	// command still enabled.
	diffArgs, ok := safeDiffArgs(ctx, gitBin, workDir)
	if !ok {
		return "", filesChanged, false
	}
	out, ok, truncatedOutput := runGitBounded(ctx, gitBin, workDir, maxPatchBytes, diffArgs...)
	if !ok {
		return "", filesChanged, false
	}

	return out, filesChanged, truncatedOutput
}

// hardenedGitConfig is the set of overrides every Git invocation over a
// task-controlled workspace carries, whatever subcommand it runs.
//
// core.fsmonitor names a program Git executes to ask what changed in the
// working tree, and it is consulted by any command that inspects the index -
// `git add` and `git diff` both. It is not a content filter, so enumerating
// filter.* does not reach it, and it needs no gitattributes entry to fire:
// setting the one config key is the whole attack. `false` is Git's own
// spelling for "there is no monitor," which is what an unconfigured
// repository looks like.
//
// Per-invocation rather than folded into safeDiffArgs, because the
// --intent-to-add call runs before any of that and is just as much a command
// over the same repository.
func hardenedGitConfig() []string {
	return []string{"-c", "core.fsmonitor=false"}
}

// safeDiffArgs builds a diff command with every configured content-filter
// command disabled. Git config keys cannot contain NUL, so --name-only --null
// gives an unambiguous list even when a task-controlled value contains newlines.
func safeDiffArgs(ctx context.Context, gitBin, workDir string) ([]string, bool) {
	out, ok, truncated := runGitBounded(ctx, gitBin, workDir, maxPatchBytes,
		append(hardenedGitConfig(), "config", "--list", "--name-only", "--null")...)
	if !ok || truncated {
		return nil, false
	}

	var keys []string
	for _, key := range strings.Split(strings.TrimSuffix(out, "\x00"), "\x00") {
		lower := strings.ToLower(key)
		if strings.HasPrefix(lower, "filter.") &&
			(strings.HasSuffix(lower, ".clean") || strings.HasSuffix(lower, ".process")) {
			keys = append(keys, key)
		}
	}
	sort.Strings(keys)
	overrides := append(hardenedGitConfig(), make([]string, 0, len(keys)*2)...)
	for _, key := range keys {
		overrides = append(overrides, "-c", key+"=")
	}
	return append(overrides, "diff", "--no-ext-diff", "--no-textconv", "--no-color", "-M", "HEAD"), true
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
// configured, working_context is not a checkout), which is treated exactly
// like dirty: patch output is a claim about what a run did, and a claim that
// cannot be checked is not made.
type workspaceBaseline struct {
	observed bool
	dirty    bool
}

// observeWorkspace records whether workDir has uncommitted changes before a
// run begins. See [workspaceBaseline].
func observeWorkspace(ctx context.Context, workDir string, mutating bool) workspaceBaseline {
	if !mutating || workDir == "" {
		return workspaceBaseline{}
	}

	gitBin := os.Getenv(gitBinaryEnv)
	if gitBin == "" {
		return workspaceBaseline{}
	}
	info, err := os.Stat(gitBin)
	if err != nil || info.IsDir() || info.Mode()&0o111 == 0 {
		return workspaceBaseline{}
	}
	if !isGitWorkTree(ctx, gitBin, workDir) {
		return workspaceBaseline{}
	}

	// --porcelain reports tracked modifications and untracked files alike,
	// which is the whole question here: any output at all means something was
	// already there.
	out, ok, _ := runGitBounded(ctx, gitBin, workDir, maxPatchBytes, "status", "--porcelain")
	if !ok {
		return workspaceBaseline{}
	}

	return workspaceBaseline{observed: true, dirty: strings.TrimSpace(out) != ""}
}

// isGitWorkTree reports whether dir is inside a git working tree, so
// computePatch can tell "working_context is not a repository" (not this
// task's business) apart from "diffing it failed" (also not surfaced as an
// error - see computePatch's own doc comment for why patch is best-effort).
func isGitWorkTree(ctx context.Context, gitBin, dir string) bool {
	runCtx, cancel := context.WithTimeout(ctx, gitTimeout)
	defer cancel()

	cmd := exec.CommandContext(runCtx, gitBin, "-C", dir, "rev-parse", "--is-inside-work-tree")
	cmd.Stdout = nil
	cmd.Stderr = nil
	return cmd.Run() == nil
}

// runGitBounded runs one git command with its combined output capped at
// maxBytes, the same boundedWriter shape plugins/vcs/diff.go's
// boundedPatchWriter uses for the identical reason: the cap has to be
// applied to the write path itself, not to a finished string, or a
// pathological diff has already cost the memory the cap exists to avoid
// before anything trims it.
func runGitBounded(ctx context.Context, gitBin, dir string, maxBytes int, args ...string) (string, bool, bool) {
	runCtx, cancel := context.WithTimeout(ctx, gitTimeout)
	defer cancel()

	full := append([]string{"-C", dir}, args...)
	cmd := exec.CommandContext(runCtx, gitBin, full...)

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
