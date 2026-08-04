package main

import (
	"context"
	"os"
	"os/exec"
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
func computePatch(ctx context.Context, workDir string, mutating bool, filesChanged []fileChange) (patch string, files []fileChange, truncated bool) {
	if !mutating || workDir == "" || len(filesChanged) == 0 {
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

	out, ok, truncatedOutput := runGitBounded(ctx, gitBin, workDir, maxPatchBytes, "diff", "--no-color", "-M", "HEAD")
	if !ok {
		return "", filesChanged, false
	}

	return out, filesChanged, truncatedOutput
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
