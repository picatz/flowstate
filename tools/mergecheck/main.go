// Command mergecheck answers whether the current branch still merges cleanly
// into origin/main, and says so out loud (#2254).
//
//	go run ./tools/mergecheck             # fetch origin main, then check HEAD
//	go run ./tools/mergecheck -no-fetch   # check against the origin/main already here
//
// The answer is always a line: `clean against origin/main <sha>`, or
// `conflicts against origin/main <sha>` followed by one conflicting path per
// line. Silence is never a pass. Every way of not establishing the answer —
// a failed fetch, a missing origin/main, histories with no merge base, a git
// too old for `merge-tree --write-tree` — exits non-zero with a line saying
// which, because an empty output read as success is how a conflicting branch
// reached #2245.
//
// Read-only and local-tier: it writes no ref and no working-tree file, and it
// does not decide which CI jobs a diff reaches (docs/CI.md keeps that in one
// place). The fetch is the only network use; -no-fetch trades it for a
// checkout whose origin/main may be stale, which the output then says.
package main

import (
	"bytes"
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"os/exec"
	"strings"
)

// base is the revision a branch must merge into.
const base = "origin/main"

// result is what [check] established.
type result struct {
	sha       string   // the full revision of base that was used
	conflicts []string // paths that conflict; empty when clean
}

// git runs one git command in dir and returns its stdout. A non-zero exit is
// an *exec.ExitError carrying the status and stderr text.
func git(ctx context.Context, dir string, args ...string) (string, error) {
	cmd := exec.CommandContext(ctx, "git", args...)
	cmd.Dir = dir
	var stdout, stderr bytes.Buffer
	cmd.Stdout, cmd.Stderr = &stdout, &stderr
	if err := cmd.Run(); err != nil {
		return stdout.String(), fmt.Errorf("git %s: %w: %s", strings.Join(args, " "), err, strings.TrimSpace(stderr.String()))
	}

	return stdout.String(), nil
}

// check merges HEAD against [base] in memory and reports the conflicts. It
// fetches base first unless noFetch, and fails closed on every error.
func check(ctx context.Context, dir string, noFetch bool) (result, error) {
	if !noFetch {
		if _, err := git(ctx, dir, "fetch", "--quiet", "origin", "main"); err != nil {
			return result{}, fmt.Errorf("cannot refresh %s: %w", base, err)
		}
	}
	sha, err := git(ctx, dir, "rev-parse", "--verify", "--quiet", base+"^{commit}")
	if err != nil {
		return result{}, fmt.Errorf("%s does not exist here; fetch it first: %w", base, err)
	}
	sha = strings.TrimSpace(sha)

	// Exit 0 is a clean merge, 1 is a merge with conflicts, anything else —
	// no merge base, an old git — means no answer.
	out, err := git(ctx, dir, "merge-tree", "--write-tree", "--name-only", "--no-messages", "HEAD", sha)
	if err == nil {
		return result{sha: sha}, nil
	}
	var exit *exec.ExitError
	if !errors.As(err, &exit) || exit.ExitCode() != 1 {
		return result{}, fmt.Errorf("cannot establish whether HEAD merges into %s: %w", base, err)
	}

	// With --name-only the output is the merged tree's id, then the
	// conflicted paths, then (without --no-messages) a blank line and
	// informational messages.
	lines := strings.Split(strings.TrimSpace(out), "\n")
	var conflicts []string
	for _, line := range lines[min(1, len(lines)):] {
		if line == "" {
			break
		}
		conflicts = append(conflicts, line)
	}
	if len(conflicts) == 0 {
		return result{}, fmt.Errorf("git reported conflicts against %s but named no path: %q", base, out)
	}

	return result{sha: sha, conflicts: conflicts}, nil
}

// report prints the verdict and returns the exit status.
func report(w io.Writer, r result, noFetch bool) int {
	note := ""
	if noFetch {
		note = " (not fetched; origin/main may be stale)"
	}
	if len(r.conflicts) == 0 {
		fmt.Fprintf(w, "mergecheck: clean against %s %s%s\n", base, r.sha, note)

		return 0
	}
	fmt.Fprintf(w, "mergecheck: conflicts against %s %s%s\n", base, r.sha, note)
	for _, path := range r.conflicts {
		fmt.Fprintf(w, "  %s\n", path)
	}

	return 1
}

func main() {
	noFetch := flag.Bool("no-fetch", false, "check against the origin/main already present instead of fetching it")
	flag.Parse()

	r, err := check(context.Background(), ".", *noFetch)
	if err != nil {
		fmt.Fprintf(os.Stderr, "mergecheck: %v\n", err)
		os.Exit(2)
	}
	os.Exit(report(os.Stdout, r, *noFetch))
}
