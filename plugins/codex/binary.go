package main

import (
	"os"
	"path/filepath"
	"strings"

	"github.com/picatz/flowstate/pkg/flowstate/v1/plugin/sdk"
)

// codexBinaryEnv names the environment variable this plugin reads for the
// codex binary's path. There is no fallback to $PATH: see doc.go, "Why this
// plugin execs a subprocess," for why that is deliberate rather than an
// oversight - a worker's binary must be told, once, by whoever configured
// it, exactly which codex it is allowed to run, not whichever one a shell's
// $PATH happens to resolve at the moment a task executes.
const codexBinaryEnv = "FLOWSTATE_CODEX_BIN"

// workdirRootEnv names the environment variable that bounds where
// working_context may point. Unset means codex.exec refuses every
// working_context input outright - fail closed, so that a worker an
// operator never configured for this cannot be pointed at an arbitrary
// directory on its own filesystem by a Flowfile's own input.
const workdirRootEnv = "FLOWSTATE_CODEX_WORKDIR_ROOT"

// resolveCodexBinary reads and validates the configured codex binary path.
//
// It is checked on every call rather than cached at process startup,
// deliberately: a plugin process is long-lived (the host launches one and
// reuses it across many task calls), and re-reading lets an operator's
// configuration mistake surface as this task's own permanent failure
// instead of a health check nobody is watching. The cost - a stat call per
// task invocation - is negligible next to the run this validates the
// binary for.
func resolveCodexBinary() (string, error) {
	path := os.Getenv(codexBinaryEnv)
	if path == "" {
		return "", sdk.Failed(
			"%s is not set; this plugin never searches $PATH for a codex binary "+
				"(see doc.go), so an operator must point it at one explicitly", codexBinaryEnv)
	}

	if !filepath.IsAbs(path) {
		return "", sdk.Failed("%s (%q) must be an absolute path", codexBinaryEnv, truncatePath(path))
	}

	info, err := os.Stat(path)
	if err != nil {
		return "", sdk.Failed("%s (%q): %v", codexBinaryEnv, truncatePath(path), err)
	}
	if info.IsDir() {
		return "", sdk.Failed("%s (%q) is a directory, not the codex binary", codexBinaryEnv, truncatePath(path))
	}
	if info.Mode()&0o111 == 0 {
		return "", sdk.Failed("%s (%q) is not executable", codexBinaryEnv, truncatePath(path))
	}

	return path, nil
}

// resolveWorkingContext validates working_context against the configured
// jail, refusing the request rather than the alternative of silently
// ignoring an input a Flowfile author actually set.
//
// An empty input is always allowed and means "no working directory" -
// this function is only reached when the caller passed something.
//
// # Why a jail, and why fail closed
//
// working_context is a path on the *worker's* filesystem, and a Flowfile's
// inputs are attacker-chosen by construction (CLAUDE.md's own framing for
// every parser and reader in this repository). A plugin that passed one
// straight to --cd would let any workflow ask codex to operate against any
// directory the worker process can read and, in WORKSPACE_WRITE or
// DANGER_FULL_ACCESS mode, write - `/etc`, another tenant's checkout,
// anything. FLOWSTATE_CODEX_WORKDIR_ROOT is the operator's explicit answer
// to "where is this worker willing to let codex touch the filesystem," the
// same shape codexBinaryEnv gives for "which binary this worker runs" -
// configuration, not a Flowfile's to choose, and refused outright rather
// than defaulted to something permissive (an unset root refuses every
// working_context, not the worker's cwd or its root) when it is not set.
func resolveWorkingContext(raw string) (string, error) {
	if raw == "" {
		return "", nil
	}

	if len(raw) > maxWorkingContextBytes {
		return "", sdk.InvalidInput(
			"working_context is %d bytes, over the %d byte limit", len(raw), maxWorkingContextBytes)
	}
	for _, r := range raw {
		if r == 0 {
			return "", sdk.InvalidInput("working_context contains a NUL byte")
		}
	}

	root := os.Getenv(workdirRootEnv)
	if root == "" {
		return "", sdk.Failed(
			"%s is not set, so this worker accepts no working_context at all; "+
				"an operator must configure a root directory before any workflow can use one", workdirRootEnv)
	}

	absRoot, err := filepath.Abs(root)
	if err != nil {
		return "", sdk.Failed("%s (%q) does not resolve: %v", workdirRootEnv, truncatePath(root), err)
	}

	var candidate string
	if filepath.IsAbs(raw) {
		candidate = filepath.Clean(raw)
	} else {
		candidate = filepath.Join(absRoot, raw)
	}

	// filepath.Rel refuses to reach outside the root with any amount of
	// "..", so this is the actual containment check - not a string prefix
	// test, which "/root-evil" passing a prefix check against "/root" is
	// the classic way to get wrong.
	// Lexical only, which is why it is not the last word: a directory that is
	// lexically inside the root can be a symlink whose target is not, and
	// every later use of this path - the Stat below, and codex's own --cd -
	// follows that link. Checked again on the resolved paths afterwards.
	rel, err := filepath.Rel(absRoot, candidate)
	if err != nil || rel == ".." || strings.HasPrefix(rel, ".."+string(filepath.Separator)) {
		return "", sdk.InvalidInput(
			"working_context must resolve inside the configured root; this worker's root is set, " +
				"but the given path does not stay within it")
	}

	info, err := os.Stat(candidate)
	if err != nil {
		return "", sdk.InvalidInput("working_context %q: %v", truncatePath(raw), err)
	}
	if !info.IsDir() {
		return "", sdk.InvalidInput("working_context %q is not a directory", truncatePath(raw))
	}

	// The containment check that actually holds: both sides canonicalized, so
	// a symlink pointing out of the root is refused rather than followed. An
	// ordinary checkout can contain a directory symlink without anyone
	// intending an escape, so the diagnostic names which property failed.
	realRoot, err := filepath.EvalSymlinks(absRoot)
	if err != nil {
		return "", sdk.Failed("%s (%q) does not resolve: %v", workdirRootEnv, truncatePath(root), err)
	}
	realCandidate, err := filepath.EvalSymlinks(candidate)
	if err != nil {
		return "", sdk.InvalidInput("working_context %q: %v", truncatePath(raw), err)
	}

	realRel, err := filepath.Rel(realRoot, realCandidate)
	if err != nil || realRel == ".." || strings.HasPrefix(realRel, ".."+string(filepath.Separator)) {
		return "", sdk.InvalidInput(
			"working_context resolves outside the configured root once symlinks are followed; " +
				"a path inside the root that links out of it is still outside it")
	}

	// The resolved path is what is handed onward, so what codex is given is
	// the directory that was actually checked.
	return realCandidate, nil
}

// truncatePath bounds a path before it enters an error message - it may
// have come from a Flowfile's own input.
func truncatePath(s string) string {
	return truncateRunes(s, 512)
}
