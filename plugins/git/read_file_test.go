package main

import (
	"context"
	"fmt"
	"strings"
	"testing"
	"time"
)

// TestGitReadFileReturnsContentSizeMode proves every field this task
// promises round-trips: content bytes, size, and mode.
func TestGitReadFileReturnsContentSizeMode(t *testing.T) {
	remote := newBareRemote(t)
	seedRemote(t, remote, "main")
	work := newSeededWorkingClone(t, remote, "main")

	pushCommit(t, work, remote, "main", "auth/policy.rego", "allow if role == admin\n",
		"tighten the admin policy", "A", "a@example.com", "A", "a@example.com", time.Now())

	out, err := doReadFile(context.Background(), readFileParams{url: fileURL(t, remote), ref: "main", path: "auth/policy.rego"})
	if err != nil {
		t.Fatalf("doReadFile: %v", err)
	}
	if string(out.Content) != "allow if role == admin\n" {
		t.Fatalf("content = %q, want the file's exact bytes", out.Content)
	}
	if out.Size != int64(len("allow if role == admin\n")) {
		t.Fatalf("size = %d, want %d", out.Size, len("allow if role == admin\n"))
	}
	if out.Mode != "0100644" {
		t.Fatalf("mode = %q, want \"0100644\" (a plain file this task never marked executable)", out.Mode)
	}
	if out.Binary {
		t.Error("binary = true, want false for plain text content")
	}
}

// TestGitReadFileDetectsBinaryContent proves the NUL-byte heuristic actually
// fires, not merely that the field exists and defaults to false.
func TestGitReadFileDetectsBinaryContent(t *testing.T) {
	remote := newBareRemote(t)
	seedRemote(t, remote, "main")
	work := newSeededWorkingClone(t, remote, "main")

	binaryContent := string([]byte{0x89, 'P', 'N', 'G', 0x00, 0x01, 0x02})
	pushCommit(t, work, remote, "main", "logo.png", binaryContent,
		"add logo", "A", "a@example.com", "A", "a@example.com", time.Now())

	out, err := doReadFile(context.Background(), readFileParams{url: fileURL(t, remote), ref: "main", path: "logo.png"})
	if err != nil {
		t.Fatalf("doReadFile: %v", err)
	}
	if !out.Binary {
		t.Error("binary = false, want true - content has a NUL byte within the sniff window")
	}
	if string(out.Content) != binaryContent {
		t.Fatal("content does not match the exact bytes written - binary detection must never alter what Content carries")
	}
}

// TestGitReadFileDefaultsRefToHead mirrors git.log's own default: an empty
// ref means the remote's HEAD.
func TestGitReadFileDefaultsRefToHead(t *testing.T) {
	remote := newBareRemote(t)
	seedRemote(t, remote, "main")

	out, err := doReadFile(context.Background(), readFileParams{url: fileURL(t, remote), ref: "", path: "seed.txt"})
	if err != nil {
		t.Fatalf("doReadFile: %v", err)
	}
	if string(out.Content) != "seed\n" {
		t.Fatalf("content = %q, want %q", out.Content, "seed\n")
	}
}

// TestGitReadFileResolvesAShaOlderThanTheDefaultDepthOneWindow is the P2
// regression test for git.read_file's half of the "ref: cannot resolve what
// the task advertises" finding, and the audit chain
// examples/plugins/git/log-and-read-file.yaml is built to demonstrate: a
// commit several pushes older than HEAD - the kind of sha a previous git.log
// call itself would return - must still resolve, even though
// readFileCloneDepth (1) alone would only ever reach the tip. Before the
// fix, doReadFile cloned at depth 1 regardless of ref, so this sha - never
// the tip of any fetched branch - was absent from the store entirely and
// resolveOptionalRef reported it as a missing revision, not merely an
// oversized request refused on its own honest terms.
func TestGitReadFileResolvesAShaOlderThanTheDefaultDepthOneWindow(t *testing.T) {
	remote := newBareRemote(t)
	seedRemote(t, remote, "main")
	work := newSeededWorkingClone(t, remote, "main")

	when := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	oldSha := pushCommit(t, work, remote, "main", "policy.txt", "v1\n",
		"add the policy", "A", "a@example.com", "A", "a@example.com", when)
	for i := 0; i < 5; i++ {
		pushCommit(t, work, remote, "main", "other.txt", fmt.Sprintf("content %d", i), fmt.Sprintf("commit %d", i),
			"A", "a@example.com", "A", "a@example.com", when.Add(time.Duration(i+1)*time.Minute))
	}

	out, err := doReadFile(context.Background(), readFileParams{url: fileURL(t, remote), ref: oldSha.String(), path: "policy.txt"})
	if err != nil {
		t.Fatalf("doReadFile at a historical sha 5 commits behind HEAD: %v - the audit chain git.log -> git.read_file requires resolving a sha git.log itself could have returned", err)
	}
	if string(out.Content) != "v1\n" {
		t.Fatalf("content = %q, want %q (the file's content at oldSha, not HEAD's)", out.Content, "v1\n")
	}
}

// TestGitReadFileRefusesAnOversizedFile is the content-size bound reached,
// not merely respected: a file one byte over the configured ceiling is
// refused with a diagnostic naming the actual size, never silently
// truncated. doReadFileWithMax's small capBytes is what makes this fast - the
// real maxReadFileBytes (8 MiB) would need a multi-megabyte fixture to prove
// the same thing.
func TestGitReadFileRefusesAnOversizedFile(t *testing.T) {
	remote := newBareRemote(t)
	seedRemote(t, remote, "main")
	work := newSeededWorkingClone(t, remote, "main")

	const capBytes = 16
	content := strings.Repeat("x", capBytes+1) // exactly one byte over
	pushCommit(t, work, remote, "main", "big.txt", content,
		"add an oversized file", "A", "a@example.com", "A", "a@example.com", time.Now())

	_, err := doReadFileWithMax(context.Background(), readFileParams{url: fileURL(t, remote), ref: "main", path: "big.txt"}, capBytes)
	if err == nil {
		t.Fatal("doReadFileWithMax over the capBytes: got nil error, want a refusal")
	}
	if !strings.Contains(err.Error(), "over the") || !strings.Contains(err.Error(), "byte limit") {
		t.Fatalf("error = %q, want it to name the byte limit this task enforces", err)
	}
	if !strings.Contains(err.Error(), "17") { // the file's actual size
		t.Fatalf("error = %q, want it to name the file's actual size (%d bytes)", err, capBytes+1)
	}
}

// TestGitReadFileAcceptsAFileExactlyAtTheBound proves the boundary is
// inclusive in the right direction: a file exactly at the capBytes succeeds,
// only one byte over is refused - the same "reached, not merely avoided"
// standard applied from the other side.
func TestGitReadFileAcceptsAFileExactlyAtTheBound(t *testing.T) {
	remote := newBareRemote(t)
	seedRemote(t, remote, "main")
	work := newSeededWorkingClone(t, remote, "main")

	const capBytes = 16
	content := strings.Repeat("x", capBytes)
	pushCommit(t, work, remote, "main", "exact.txt", content,
		"add a file exactly at the bound", "A", "a@example.com", "A", "a@example.com", time.Now())

	out, err := doReadFileWithMax(context.Background(), readFileParams{url: fileURL(t, remote), ref: "main", path: "exact.txt"}, capBytes)
	if err != nil {
		t.Fatalf("doReadFileWithMax at the capBytes: unexpected error: %v", err)
	}
	if len(out.Content) != capBytes {
		t.Fatalf("len(content) = %d, want %d", len(out.Content), capBytes)
	}
}

// TestGitReadFileRefusesPathTraversal and
// TestGitReadFileRefusesAnAbsolutePath prove this task's own path checks -
// shared with commit_push's validateTreePath - actually run before any
// clone is attempted; both name a path that never reaches the network path
// in doReadFile, since gitReadFile validates before calling it. Exercised
// here at the validateTreePath layer directly, the same layer
// commit_push_test.go's TestValidateTreePathRefusesEscapesAndGitWrites
// exercises for the write task - see that test for why refusing here,
// before any clone, is the property that matters (an attacker-adjacent path
// never reaches go-git's tree lookup at all).
func TestGitReadFileRefusesPathTraversal(t *testing.T) {
	// The empty field is what gitReadFile itself passes: this task's only input
	// is the path, so the message must not double the word into "path: path ...".
	if _, err := validateTreePath("", "../outside"); err == nil {
		t.Fatal("validateTreePath(\"../outside\"): got nil error, want a refusal")
	}
}

func TestGitReadFileRefusesAnAbsolutePath(t *testing.T) {
	if _, err := validateTreePath("", "/etc/passwd"); err == nil {
		t.Fatal("validateTreePath(\"/etc/passwd\"): got nil error, want a refusal")
	}
}

// TestValidateTreePathMessageOmitsRedundantFieldPrefix pins the diagnostic
// text gitReadFile produces (empty field) against commit_push's (field
// "files"/"patch"): read_file's message reads "path is empty", never the
// doubled "path: path is empty" it produced when it passed "path" as the field,
// while a named field still prefixes exactly as before.
func TestValidateTreePathMessageOmitsRedundantFieldPrefix(t *testing.T) {
	_, err := validateTreePath("", "")
	if err == nil {
		t.Fatal("validateTreePath(\"\", \"\"): got nil error, want a refusal")
	}
	if got := err.Error(); got != "path is empty" {
		t.Fatalf("read_file message = %q, want %q (no doubled \"path\")", got, "path is empty")
	}

	_, err = validateTreePath("", "/etc/passwd")
	if err == nil {
		t.Fatal("validateTreePath(\"\", \"/etc/passwd\"): got nil error, want a refusal")
	}
	if got := err.Error(); strings.HasPrefix(got, "path: path") {
		t.Fatalf("read_file message %q still doubles \"path\"", got)
	}

	// A named field (commit_push's callers) still prefixes exactly as before.
	_, err = validateTreePath("files", "")
	if err == nil {
		t.Fatal("validateTreePath(\"files\", \"\"): got nil error, want a refusal")
	}
	if got := err.Error(); got != "files: path is empty" {
		t.Fatalf("files message = %q, want %q", got, "files: path is empty")
	}
}

// TestGitReadFileRefusesAMissingPath and
// TestGitReadFileClassifiesAnOversizedFileAsFailed, together with
// log_test.go's TestGitLogClassifiesAMissingRefAsInvalidInput and
// TestGitLogClassifiesAnUnreachableRemoteAsNotFound, are CLAUDE.md's
// diagnostics requirement: three different failures classified
// differently, not collapsed into one generic error. NotFound (a path that
// does not exist), InvalidInput (a ref go-git's revision parser rejects, or
// this plugin's own shallow window does not reach), and Failed (content
// this task's own configured ceiling refuses to serve) are each reached by
// a distinct scenario below and in log_test.go.
func TestGitReadFileRefusesAMissingPath(t *testing.T) {
	remote := newBareRemote(t)
	seedRemote(t, remote, "main")

	_, err := doReadFile(context.Background(), readFileParams{url: fileURL(t, remote), ref: "main", path: "does/not/exist.txt"})
	if err == nil {
		t.Fatal("doReadFile for a path that does not exist: got nil error")
	}
	if !strings.Contains(err.Error(), "does not exist") {
		t.Fatalf("error = %q, want it to say the path does not exist", err)
	}
}

func TestGitReadFileClassifiesAnOversizedFileAsFailed(t *testing.T) {
	remote := newBareRemote(t)
	seedRemote(t, remote, "main")
	work := newSeededWorkingClone(t, remote, "main")

	const capBytes = 4
	pushCommit(t, work, remote, "main", "big.txt", "way too big for the capBytes",
		"add an oversized file", "A", "a@example.com", "A", "a@example.com", time.Now())

	_, err := doReadFileWithMax(context.Background(), readFileParams{url: fileURL(t, remote), ref: "main", path: "big.txt"}, capBytes)
	if err == nil {
		t.Fatal("doReadFileWithMax over the capBytes: got nil error")
	}
	// Distinct wording from the missing-path (NotFound) and missing-ref
	// (InvalidInput) cases: this is a Failed classification (see
	// read_file.go's own comment on why), and its message names the byte
	// limit rather than "does not exist" or "no such revision."
	if strings.Contains(err.Error(), "does not exist") || strings.Contains(err.Error(), "no such revision") {
		t.Fatalf("error = %q, want the oversized-content wording, not the missing-path or missing-ref wording", err)
	}
	if !strings.Contains(err.Error(), "byte limit") {
		t.Fatalf("error = %q, want it to name the byte limit", err)
	}
}
