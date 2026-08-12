package main

import (
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
)

// TestGeneratedCleanSeesNewArtifacts is the regression test for the hole a
// diff-only pin leaves open.
//
// `git diff --exit-code` answers a question about *tracked* files, so a
// generator that creates a new artifact — a mirror page for a newly added
// example, a .pb.go for a proto file this diff adds, a new page under
// docs/reference/ — leaves it untracked and a diff-only pin reports success
// while the artifact is missing from the commit. That is a gate passing when
// it should fail, which is worse than no gate, so both halves are asserted
// here: the drift the diff can see, and the creation it cannot.
func TestGeneratedCleanSeesNewArtifacts(t *testing.T) {
	repo := newTestRepo(t)

	write(t, repo, "docs/reference/tasks.md", "generated\n")
	write(t, repo, ".gitignore", "*.ignored\n")
	git(t, repo, "add", ".")
	git(t, repo, "-c", "user.email=t@example.com", "-c", "user.name=t", "commit", "-m", "initial")

	check := func() error {
		if err := checkTrackedClean([]string{"docs/reference/"}); err != nil {
			return err
		}
		return checkNoUntracked([]string{"docs/reference/"})
	}

	t.Chdir(repo)

	// A committed, unmodified artifact is clean.
	if err := check(); err != nil {
		t.Fatalf("a committed artifact must pass: %v", err)
	}

	// Drift in a tracked artifact fails, naming it.
	write(t, repo, "docs/reference/tasks.md", "regenerated differently\n")
	err := check()
	if err == nil {
		t.Fatal("drift in a tracked artifact must fail")
	}
	if !strings.Contains(err.Error(), "docs/reference/tasks.md") {
		t.Errorf("failure must name the drifted file, got: %v", err)
	}
	write(t, repo, "docs/reference/tasks.md", "generated\n")

	// The hole: a NEW artifact the generator created is untracked, so a
	// diff is blind to it. This must fail and name the file.
	write(t, repo, "docs/reference/newpage.md", "newly generated\n")
	if err := checkTrackedClean([]string{"docs/reference/"}); err != nil {
		t.Fatalf("a diff cannot see an untracked file; this is the premise of the test: %v", err)
	}
	err = check()
	if err == nil {
		t.Fatal("an untracked generated artifact must fail the pin, not pass it")
	}
	if !strings.Contains(err.Error(), "docs/reference/newpage.md") {
		t.Errorf("failure must name the untracked artifact, got: %v", err)
	}

	// Staging is what the failure asked for, so staging must clear it:
	// both halves take the index as their reference point, and an author
	// who stages what the gate regenerated passes on the next run rather
	// than being failed for not having committed yet.
	git(t, repo, "add", "docs/reference/newpage.md")
	if err := check(); err != nil {
		t.Errorf("staging the regenerated artifact must clear the pin: %v", err)
	}

	// An ignored file is build output, not a missing artifact.
	git(t, repo, "rm", "-f", "--cached", "docs/reference/newpage.md")
	if err := os.Remove(filepath.Join(repo, "docs/reference/newpage.md")); err != nil {
		t.Fatal(err)
	}
	write(t, repo, "docs/reference/scratch.ignored", "build output\n")
	if err := checkNoUntracked([]string{"docs/reference/"}); err != nil {
		t.Errorf("an ignored file must not be mistaken for a missing artifact: %v", err)
	}

	// A change outside the pathspec is somebody else's uncommitted work,
	// not this leg's business.
	write(t, repo, "unrelated.go", "package main\n")
	if err := checkNoUntracked([]string{"docs/reference/"}); err != nil {
		t.Errorf("the pin must stay scoped to its pathspecs: %v", err)
	}
}

func newTestRepo(t *testing.T) string {
	t.Helper()
	if _, err := exec.LookPath("git"); err != nil {
		t.Skip("git not on PATH")
	}
	dir := t.TempDir()
	git(t, dir, "init", "-q", "-b", "main")
	return dir
}

func git(t *testing.T, dir string, args ...string) {
	t.Helper()
	cmd := exec.Command("git", args...)
	cmd.Dir = dir
	if out, err := cmd.CombinedOutput(); err != nil {
		t.Fatalf("git %s: %v: %s", strings.Join(args, " "), err, out)
	}
}

func write(t *testing.T, dir, rel, contents string) {
	t.Helper()
	path := filepath.Join(dir, filepath.FromSlash(rel))
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path, []byte(contents), 0o644); err != nil {
		t.Fatal(err)
	}
}
