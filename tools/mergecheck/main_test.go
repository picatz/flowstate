package main

import (
	"bytes"
	"context"
	"errors"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
)

// run executes git in dir and fails the test on error.
func run(t *testing.T, dir string, args ...string) string {
	t.Helper()
	cmd := exec.Command("git", args...)
	cmd.Dir = dir
	cmd.Env = append(os.Environ(),
		"GIT_AUTHOR_NAME=t", "GIT_AUTHOR_EMAIL=t@example.com",
		"GIT_COMMITTER_NAME=t", "GIT_COMMITTER_EMAIL=t@example.com",
		"GIT_CONFIG_GLOBAL=/dev/null", "GIT_CONFIG_SYSTEM=/dev/null")
	out, err := cmd.CombinedOutput()
	if err != nil {
		t.Fatalf("git %s: %v\n%s", strings.Join(args, " "), err, out)
	}

	return strings.TrimSpace(string(out))
}

func write(t *testing.T, dir, name, text string) {
	t.Helper()
	if err := os.WriteFile(filepath.Join(dir, name), []byte(text), 0o644); err != nil {
		t.Fatal(err)
	}
}

// repo builds a clone whose origin/main holds file.txt = "base\n" and whose
// local branch has branched from it.
func repo(t *testing.T) (clone, origin string) {
	t.Helper()
	root := t.TempDir()
	origin = filepath.Join(root, "origin")
	clone = filepath.Join(root, "clone")
	run(t, root, "init", "-q", "-b", "main", origin)
	write(t, origin, "file.txt", "base\n")
	run(t, origin, "add", ".")
	run(t, origin, "commit", "-q", "-m", "base")
	run(t, root, "clone", "-q", origin, clone)
	run(t, clone, "checkout", "-q", "-b", "feature")

	return clone, origin
}

func advanceMain(t *testing.T, origin, text string) {
	t.Helper()
	write(t, origin, "file.txt", text)
	run(t, origin, "commit", "-q", "-am", "main moves")
}

func TestCleanBranchPrintsItsPositiveLine(t *testing.T) {
	clone, origin := repo(t)
	write(t, clone, "other.txt", "mine\n")
	run(t, clone, "add", ".")
	run(t, clone, "commit", "-q", "-m", "feature")
	advanceMain(t, origin, "main change\n")
	want := run(t, origin, "rev-parse", "HEAD")

	r, err := check(context.Background(), clone, false)
	if err != nil {
		t.Fatal(err)
	}
	var out bytes.Buffer
	if code, err := report(&out, r, false); code != 0 || err != nil {
		t.Fatalf("exit %d, output %q", code, out.String())
	}
	if got := out.String(); got != "mergecheck: clean against origin/main "+want+"\n" {
		t.Fatalf("output %q does not carry the fetched main revision %s", got, want)
	}
}

func TestConflictingBranchNamesThePath(t *testing.T) {
	clone, origin := repo(t)
	write(t, clone, "file.txt", "feature change\n")
	run(t, clone, "commit", "-q", "-am", "feature")
	advanceMain(t, origin, "main change\n")

	r, err := check(context.Background(), clone, false)
	if err != nil {
		t.Fatal(err)
	}
	var out bytes.Buffer
	if code, err := report(&out, r, false); code != 1 || err != nil {
		t.Fatalf("exit %d, output %q", code, out.String())
	}
	if !strings.Contains(out.String(), "conflicts against origin/main") || !strings.Contains(out.String(), "  file.txt\n") {
		t.Fatalf("output %q does not name the conflicting path", out.String())
	}
}

func TestStaleOriginMainIsRefreshedBeforeTheAnswer(t *testing.T) {
	clone, origin := repo(t)
	write(t, clone, "file.txt", "feature change\n")
	run(t, clone, "commit", "-q", "-am", "feature")
	advanceMain(t, origin, "main change\n")

	// Without the fetch the clone's origin/main is the old base and the
	// branch looks clean: the exact silence-read-as-success of #2245, which
	// the output must at least flag.
	stale, err := check(context.Background(), clone, true)
	if err != nil {
		t.Fatal(err)
	}
	var out bytes.Buffer
	if _, err := report(&out, stale, true); err != nil {
		t.Fatal(err)
	}
	if len(stale.conflicts) != 0 || !strings.Contains(out.String(), "not fetched") {
		t.Fatalf("a stale check must say it was not fetched: %q", out.String())
	}
	fresh, err := check(context.Background(), clone, false)
	if err != nil || len(fresh.conflicts) == 0 {
		t.Fatalf("a fetching check must see the conflict: %+v, %v", fresh, err)
	}
}

func TestMissingOriginMainFailsClosed(t *testing.T) {
	dir := t.TempDir()
	run(t, dir, "init", "-q", "-b", "main")
	write(t, dir, "f", "x\n")
	run(t, dir, "add", ".")
	run(t, dir, "commit", "-q", "-m", "x")

	if _, err := check(context.Background(), dir, true); err == nil {
		t.Fatal("no origin/main must be an error, not a pass")
	}
}

func TestFailedFetchFailsClosed(t *testing.T) {
	clone, origin := repo(t)
	if err := os.RemoveAll(origin); err != nil {
		t.Fatal(err)
	}

	if _, err := check(context.Background(), clone, false); err == nil {
		t.Fatal("a fetch that cannot reach origin must be an error, not a pass on the stale ref")
	}
}

func TestUnrelatedHistoriesFailClosed(t *testing.T) {
	clone, _ := repo(t)
	run(t, clone, "checkout", "-q", "--orphan", "island")
	run(t, clone, "rm", "-rqf", ".")
	write(t, clone, "island.txt", "x\n")
	run(t, clone, "add", ".")
	run(t, clone, "commit", "-q", "-m", "island")

	if _, err := check(context.Background(), clone, true); err == nil {
		t.Fatal("no merge base must be an error, not a pass")
	}
}

type failingWriter struct{}

func (failingWriter) Write([]byte) (int, error) { return 0, errors.New("disk full") }

func TestAnUnwritableVerdictIsAnErrorNotAPass(t *testing.T) {
	if code, err := report(failingWriter{}, result{sha: "abc"}, false); err == nil || code != 0 {
		t.Fatalf("a clean verdict that could not be written must fail: code %d, err %v", code, err)
	}
}

func TestAFetchRefspecThatSkipsMainStillRefreshesIt(t *testing.T) {
	clone, origin := repo(t)
	write(t, clone, "file.txt", "feature change\n")
	run(t, clone, "commit", "-q", "-am", "feature")
	// A remote.origin.fetch that does not map refs/heads/main: a plain
	// `git fetch origin main` would update FETCH_HEAD and leave origin/main
	// stale, and the check would call the branch clean.
	run(t, clone, "config", "remote.origin.fetch", "+refs/heads/other:refs/remotes/origin/other")
	advanceMain(t, origin, "main change\n")

	r, err := check(context.Background(), clone, false)
	if err != nil || len(r.conflicts) == 0 {
		t.Fatalf("the check must see main's new commit: %+v, %v", r, err)
	}
}
