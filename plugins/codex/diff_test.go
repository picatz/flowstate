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

func TestComputePatchSkipsWhenNotMutating(t *testing.T) {
	patch, files, truncated := computePatch(context.Background(), t.TempDir(), false, workspaceBaseline{observed: true},
		[]fileChange{{Path: "a.txt", ChangeType: "update"}})
	if patch != "" || truncated {
		t.Fatalf("computePatch with mutating=false = (%q, %v), want (\"\", false)", patch, truncated)
	}
	if len(files) != 1 {
		t.Fatalf("computePatch with mutating=false should still pass through the reported files unchanged")
	}
}

func TestComputePatchSkipsWithNoFilesChanged(t *testing.T) {
	patch, _, _ := computePatch(context.Background(), t.TempDir(), true, workspaceBaseline{observed: true}, nil)
	if patch != "" {
		t.Fatalf("computePatch with no files_changed = %q, want empty", patch)
	}
}

func TestComputePatchSkipsWithNoGitBinaryConfigured(t *testing.T) {
	t.Setenv(gitBinaryEnv, "")
	patch, _, _ := computePatch(context.Background(), t.TempDir(), true, workspaceBaseline{observed: true},
		[]fileChange{{Path: "a.txt", ChangeType: "update"}})
	if patch != "" {
		t.Fatalf("computePatch with no git binary configured = %q, want empty (best-effort, not an error)", patch)
	}
}

func TestComputePatchSkipsWhenDirIsNotAGitRepo(t *testing.T) {
	gitBin := realGitBinary(t)
	t.Setenv(gitBinaryEnv, gitBin)

	patch, _, _ := computePatch(context.Background(), t.TempDir(), true, workspaceBaseline{observed: true},
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

	patch, files, truncated := computePatch(context.Background(), dir, true, workspaceBaseline{observed: true},
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
	for key, value := range map[string]string{
		"diff.external":     helper,
		"filter.evil.clean": helper,
		// Not a content filter, and needing no gitattributes entry: Git runs
		// core.fsmonitor to ask what changed in the working tree, from any
		// command that inspects the index - the --intent-to-add call as much
		// as the diff itself. Sweeping filter.* does not reach it, so that
		// sweep alone leaves the one config key that is the whole attack.
		"core.fsmonitor": helper,
	} {
		cmd := exec.Command(gitBin, "-C", dir, "config", key, value)
		if out, err := cmd.CombinedOutput(); err != nil {
			t.Fatalf("configure repository %s: %v: %s", key, err, out)
		}
	}
	attributes := filepath.Join(dir, ".git", "info", "attributes")
	if err := os.WriteFile(attributes, []byte("*.txt filter=evil\n"), 0o600); err != nil {
		t.Fatalf("WriteFile attributes: %v", err)
	}
	if err := os.WriteFile(filepath.Join(dir, "a.txt"), []byte("changed\n"), 0o600); err != nil {
		t.Fatalf("WriteFile change: %v", err)
	}

	patch, _, truncated := computePatch(context.Background(), dir, true, workspaceBaseline{observed: true},
		[]fileChange{{Path: "a.txt", ChangeType: "update"}})
	if truncated || !strings.Contains(patch, "+changed") {
		t.Fatalf("computePatch = (%q, truncated %v), want an ordinary unified diff", patch, truncated)
	}
	if _, err := os.Stat(marker); !os.IsNotExist(err) {
		t.Fatalf("repository-controlled diff or content-filter helper ran outside the Codex sandbox: Stat marker error = %v", err)
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
