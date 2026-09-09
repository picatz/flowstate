package main

import (
	"encoding/json"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
	"time"
	"unicode/utf8"
)

const testHead = "0123456789abcdef0123456789abcdef01234567"

func passingPullRequest() pullRequest {
	return pullRequest{
		State:            "OPEN",
		BaseRefName:      "main",
		HeadRefOID:       testHead,
		AutoMergeRequest: presentJSON{Present: true, Value: json.RawMessage("null")},
		ChangedFiles:     0,
		StatusChecks: append(passingRequiredChecks(),
			statusCheck{Type: "CheckRun", Name: "test", Status: "COMPLETED", Conclusion: "SUCCESS"},
			statusCheck{Type: "CheckRun", Name: "not selected", Status: "COMPLETED", Conclusion: "SKIPPED"},
			statusCheck{Type: "StatusContext", Context: "external", State: "SUCCESS"},
		),
		Comments: []comment{{
			Author: actor{Login: "picatz"}, AuthorAssociation: "OWNER",
			Body: "Independent AI code/security review by amp-oracle for `" + testHead + "`: PASS with no actionable findings.\n\n" +
				`<!-- flowstate-independent-review:v1 {"headSha":"` + testHead + `","reviewer":"amp-oracle","scope":"code-security","status":"pass"} -->`,
		}},
	}
}

func passingRequiredChecks() []statusCheck {
	checks := make([]statusCheck, 0, len(requiredChecks))
	for _, required := range requiredChecks {
		checks = append(checks, statusCheck{
			Type: "CheckRun", Workflow: required.workflow, Name: required.name,
			Status: "COMPLETED", Conclusion: "SUCCESS",
		})
	}
	return checks
}

func TestEvaluateAcceptsCompleteFinalHeadEvidence(t *testing.T) {
	if problems := evaluate(passingPullRequest(), 0); len(problems) != 0 {
		t.Fatalf("evaluate returned problems: %v", problems)
	}
}

func TestEvaluateRejectsPrematureMergeState(t *testing.T) {
	pr := passingPullRequest()
	pr.State = "CLOSED"
	pr.BaseRefName = "release"
	pr.IsDraft = true
	pr.AutoMergeRequest = presentJSON{Present: true, Value: json.RawMessage(`{"enabledAt":"now"}`)}
	pr.StatusChecks[0].Status = "IN_PROGRESS"
	pr.StatusChecks[0].Conclusion = ""
	pr.Comments[0].Body = strings.ReplaceAll(pr.Comments[0].Body, "0123456789", "abcdef0123")

	problems := strings.Join(evaluate(pr, 2), "\n")
	for _, want := range []string{
		`pull request state is "CLOSED"`,
		`pull request base is "release"`,
		"pull request is still a draft",
		"auto-merge is enabled",
		`check "Analyze Go" latest result is IN_PROGRESS/`,
		"independent code/security review has not passed on the exact final head",
		"2 review thread(s) remain unresolved",
	} {
		if !strings.Contains(problems, want) {
			t.Errorf("problems do not contain %q:\n%s", want, problems)
		}
	}
}

func TestEvaluateRejectsMissingAutoMergeEvidenceAndBlockingReview(t *testing.T) {
	pr := passingPullRequest()
	pr.AutoMergeRequest = presentJSON{}
	pr.ReviewDecision = "CHANGES_REQUESTED"
	problems := strings.Join(evaluate(pr, 0), "\n")
	for _, want := range []string{"auto-merge state is missing", `review decision is "CHANGES_REQUESTED"`} {
		if !strings.Contains(problems, want) {
			t.Errorf("problems do not contain %q:\n%s", want, problems)
		}
	}
}

func TestEvaluateRejectsCancelledAndMissingChecks(t *testing.T) {
	pr := passingPullRequest()
	pr.StatusChecks = []statusCheck{{Type: "CheckRun", Name: "appearance", Status: "COMPLETED", Conclusion: "CANCELLED"}}
	if problems := strings.Join(evaluate(pr, 0), "\n"); !strings.Contains(problems, "CANCELLED") {
		t.Fatalf("cancelled check was accepted: %s", problems)
	}
	pr.StatusChecks = nil
	if problems := strings.Join(evaluate(pr, 0), "\n"); !strings.Contains(problems, "no check runs") {
		t.Fatalf("missing checks were accepted: %s", problems)
	}
}

func TestEvaluateRejectsTruncatedChangedFiles(t *testing.T) {
	pr := passingPullRequest()
	pr.ChangedFiles = 101
	pr.Files = make([]changedFile, 100)
	if problems := strings.Join(evaluate(pr, 0), "\n"); !strings.Contains(problems, "changed-file evidence is incomplete") {
		t.Fatalf("truncated changed files were accepted: %s", problems)
	}
}

func TestEvaluateAcceptsSuccessfulReplacementForCancelledDuplicate(t *testing.T) {
	pr := passingPullRequest()
	pr.StatusChecks = append(passingRequiredChecks(),
		statusCheck{Type: "CheckRun", Name: "replacement", Status: "COMPLETED", Conclusion: "CANCELLED", StartedAt: "2026-09-08T10:00:00Z"},
		statusCheck{Type: "CheckRun", Name: "replacement", Status: "COMPLETED", Conclusion: "SUCCESS", StartedAt: "2026-09-08T10:01:00Z"},
	)
	if problems := evaluate(pr, 0); len(problems) != 0 {
		t.Fatalf("successful replacement was rejected: %v", problems)
	}
}

func TestEvaluateRejectsFailedReplacementForSuccessfulDuplicate(t *testing.T) {
	pr := passingPullRequest()
	pr.StatusChecks = []statusCheck{
		{Type: "CheckRun", Name: "test", Status: "COMPLETED", Conclusion: "FAILURE", StartedAt: "2026-09-08T10:01:00Z"},
		{Type: "CheckRun", Name: "test", Status: "COMPLETED", Conclusion: "SUCCESS", StartedAt: "2026-09-08T10:00:00Z"},
	}
	if problems := strings.Join(evaluate(pr, 0), "\n"); !strings.Contains(problems, "latest result is COMPLETED/FAILURE") {
		t.Fatalf("failed replacement was accepted: %s", problems)
	}
}

func TestEvaluateRejectsQueuedReplacementWithoutTimestamps(t *testing.T) {
	pr := passingPullRequest()
	pr.StatusChecks = []statusCheck{
		{Type: "CheckRun", Name: "test", Status: "COMPLETED", Conclusion: "SUCCESS", StartedAt: "2026-09-08T10:00:00Z"},
		{Type: "CheckRun", Name: "test", Status: "QUEUED"},
	}
	if problems := strings.Join(evaluate(pr, 0), "\n"); !strings.Contains(problems, `check "test" is QUEUED/`) {
		t.Fatalf("queued replacement was accepted: %s", problems)
	}
}

func TestEvaluateRejectsUnorderedCompletedDuplicates(t *testing.T) {
	pr := passingPullRequest()
	pr.StatusChecks = []statusCheck{
		{Type: "CheckRun", Name: "test", Status: "COMPLETED", Conclusion: "SUCCESS", StartedAt: "2026-09-08T10:00:00Z"},
		{Type: "CheckRun", Name: "test", Status: "COMPLETED", Conclusion: "FAILURE"},
	}
	if problems := strings.Join(evaluate(pr, 0), "\n"); !strings.Contains(problems, "without enough timestamp evidence") {
		t.Fatalf("unordered failed duplicate was accepted: %s", problems)
	}
}

func TestEvaluateRejectsConflictingChecksAtSameTimestamp(t *testing.T) {
	for _, conclusions := range [][]string{{"FAILURE", "SUCCESS"}, {"SUCCESS", "FAILURE"}} {
		pr := passingPullRequest()
		pr.StatusChecks = append(pr.StatusChecks,
			statusCheck{Type: "CheckRun", Name: "test", Status: "COMPLETED", Conclusion: conclusions[0], StartedAt: "2026-09-09T16:00:00Z"},
			statusCheck{Type: "CheckRun", Name: "test", Status: "COMPLETED", Conclusion: conclusions[1], StartedAt: "2026-09-09T16:00:00Z"},
		)
		if problems := strings.Join(evaluate(pr, 0), "\n"); !strings.Contains(problems, "conflicting results at the latest timestamp") {
			t.Fatalf("equal-timestamp conflict %v was accepted: %s", conclusions, problems)
		}
	}
}

func TestIndependentReviewRejectsStaleOrIncompleteEvidence(t *testing.T) {
	pr := passingPullRequest()
	for name, mutate := range map[string]func(*comment){
		"stale head":  func(c *comment) { c.Body = strings.ReplaceAll(c.Body, testHead, strings.Repeat("f", 40)) },
		"no reviewer": func(c *comment) { c.Body = strings.ReplaceAll(c.Body, "amp-oracle", "") },
		"wrong scope": func(c *comment) { c.Body = strings.ReplaceAll(c.Body, "code-security", "code") },
		"no verdict":  func(c *comment) { c.Body = strings.ReplaceAll(c.Body, "no actionable", "reviewed") },
	} {
		t.Run(name, func(t *testing.T) {
			candidate := pr
			candidate.Comments = append([]comment(nil), pr.Comments...)
			mutate(&candidate.Comments[0])
			if hasIndependentReview(candidate) {
				t.Fatal("invalid independent-review evidence was accepted")
			}
		})
	}
}

func TestIndependentReviewMustFollowExactHeadReviewArtifacts(t *testing.T) {
	pr := passingPullRequest()
	pr.Comments[0].CreatedAt = "2026-09-09T16:00:00Z"
	pr.Reviews = []review{{SubmittedAt: "2026-09-09T16:01:00Z"}}
	if hasIndependentReview(pr) {
		t.Fatal("attestation older than an exact-head review artifact was accepted")
	}
	pr.Reviews[0].SubmittedAt = "2026-09-09T15:59:00Z"
	if !hasIndependentReview(pr) {
		t.Fatal("attestation newer than every exact-head review artifact was rejected")
	}
	pr.Reviews = []review{{SubmittedAt: pr.Comments[0].CreatedAt}}
	if hasIndependentReview(pr) {
		t.Fatal("equal-timestamp stale-head review artifact did not invalidate attestation")
	}
	pr.Reviews = []review{{SubmittedAt: "2026-09-09T15:00:00Z", UpdatedAt: "2026-09-09T16:01:00Z"}}
	if hasIndependentReview(pr) {
		t.Fatal("edited formal review did not invalidate attestation")
	}
}

func TestIndependentReviewMustFollowOtherComments(t *testing.T) {
	pr := passingPullRequest()
	pr.Comments[0].CreatedAt = "2026-09-09T16:00:00Z"
	pr.Comments[0].URL = "https://example.test/attestation"
	pr.Comments = append(pr.Comments, comment{URL: "https://example.test/finding", CreatedAt: "2026-09-09T16:01:00Z"})
	if hasIndependentReview(pr) {
		t.Fatal("attestation older than another PR comment was accepted")
	}
	pr.Comments[1].CreatedAt = "2026-09-09T15:00:00Z"
	pr.Comments[1].UpdatedAt = "2026-09-09T16:01:00Z"
	if hasIndependentReview(pr) {
		t.Fatal("attestation older than an edited PR comment was accepted")
	}
	pr.Comments = pr.Comments[:1]
	pr.Comments[0].UpdatedAt = "2026-09-09T16:01:00Z"
	if hasIndependentReview(pr) {
		t.Fatal("edited attestation was accepted")
	}
}

func TestIndependentReviewMustFollowInlineReviewCommentEdits(t *testing.T) {
	pr := passingPullRequest()
	pr.Comments[0].CreatedAt = "2026-09-09T16:00:00Z"
	pr.LatestReviewCommentUpdatedAt = "2026-09-09T16:00:00Z"
	if hasIndependentReview(pr) {
		t.Fatal("equal-timestamp inline review comment was accepted")
	}
	pr.LatestReviewCommentUpdatedAt = "2026-09-09T15:59:00Z"
	if !hasIndependentReview(pr) {
		t.Fatal("older inline review comment invalidated attestation")
	}
}

func TestRequiredCheckCannotDisappear(t *testing.T) {
	pr := passingPullRequest()
	pr.StatusChecks = pr.StatusChecks[1:]
	if problems := strings.Join(evaluate(pr, 0), "\n"); !strings.Contains(problems, `required check "Analyze Go"`) {
		t.Fatalf("missing CodeQL check was accepted: %s", problems)
	}
}

func TestRequiredCheckCannotBeSkipped(t *testing.T) {
	pr := passingPullRequest()
	pr.StatusChecks[0].Conclusion = "SKIPPED"
	if problems := strings.Join(evaluate(pr, 0), "\n"); !strings.Contains(problems, `required check "Analyze Go"`) {
		t.Fatalf("skipped CodeQL check was accepted: %s", problems)
	}
}

func TestEditorChangesRequireBothEditorChecks(t *testing.T) {
	pr := passingPullRequest()
	pr.Files = []changedFile{{Path: "cmd/flow/dap.go"}}
	pr.ChangedFiles = len(pr.Files)
	problems := strings.Join(evaluate(pr, 0), "\n")
	for _, name := range []string{"Neovim LSP smoke", "VS Code extension"} {
		if !strings.Contains(problems, name) {
			t.Errorf("missing Editors check %q was accepted: %s", name, problems)
		}
	}
	pr.StatusChecks = append(pr.StatusChecks,
		statusCheck{Type: "CheckRun", Workflow: "Editors", Name: "Neovim LSP smoke", Status: "COMPLETED", Conclusion: "SUCCESS"},
		statusCheck{Type: "CheckRun", Workflow: "Editors", Name: "VS Code extension", Status: "COMPLETED", Conclusion: "SUCCESS"},
	)
	if problems := evaluate(pr, 0); len(problems) != 0 {
		t.Fatalf("complete Editors evidence was rejected: %v", problems)
	}
}

func TestRunGHIsBounded(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("test helper is a POSIX shell script")
	}
	dir := t.TempDir()
	gh := filepath.Join(dir, "gh")
	if err := os.WriteFile(gh, []byte("#!/bin/sh\n/bin/sleep 1\n"), 0o755); err != nil {
		t.Fatal(err)
	}
	t.Setenv("PATH", dir)
	original := ghCommandTimeout
	originalWaitDelay := ghWaitDelay
	ghCommandTimeout = 10 * time.Millisecond
	ghWaitDelay = 10 * time.Millisecond
	t.Cleanup(func() {
		ghCommandTimeout = original
		ghWaitDelay = originalWaitDelay
	})

	started := time.Now()
	if _, err := runGH("pr", "view"); err == nil {
		t.Fatal("runGH did not time out")
	}
	if elapsed := time.Since(started); elapsed > 500*time.Millisecond {
		t.Fatalf("runGH returned after %s, want a bounded timeout", elapsed)
	}
}

func TestRunGHIgnoresStderrOnSuccess(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("test helper is a POSIX shell script")
	}
	dir := t.TempDir()
	gh := filepath.Join(dir, "gh")
	if err := os.WriteFile(gh, []byte("#!/bin/sh\necho warning >&2\nprintf '{\"state\":\"OPEN\"}'\n"), 0o755); err != nil {
		t.Fatal(err)
	}
	t.Setenv("PATH", dir)
	out, err := runGH("pr", "view")
	if err != nil {
		t.Fatalf("runGH: %v", err)
	}
	if got, want := string(out), `{"state":"OPEN"}`; got != want {
		t.Fatalf("runGH output = %q, want %q", got, want)
	}
}

func TestBoundedBufferCapsCollectedOutput(t *testing.T) {
	buffer := &boundedBuffer{limit: 4}
	input := []byte("abcdefgh")
	n, err := buffer.Write(input)
	if err != nil || n != len(input) {
		t.Fatalf("Write = %d, %v", n, err)
	}
	if got := buffer.String(); got != "abcd" {
		t.Fatalf("buffer = %q, want abcd", got)
	}
	if !buffer.exceeded {
		t.Fatal("buffer did not record overflow")
	}
}

func TestRunGHCapsSubprocessOutput(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("test helper is a POSIX shell script")
	}
	dir := t.TempDir()
	gh := filepath.Join(dir, "gh")
	if err := os.WriteFile(gh, []byte("#!/bin/sh\nprintf '1234567890'\nprintf 'abcdefghij' >&2\n"), 0o755); err != nil {
		t.Fatal(err)
	}
	t.Setenv("PATH", dir)
	originalLimit := ghOutputLimit
	ghOutputLimit = ghErrorOutputLimit + 4
	t.Cleanup(func() { ghOutputLimit = originalLimit })
	out, err := runGH("pr", "view")
	if err != errGHOutputLimit {
		t.Fatalf("runGH error = %v, want %v", err, errGHOutputLimit)
	}
	if len(out) > ghErrorOutputLimit+4 {
		t.Fatalf("runGH collected %d bytes beyond configured limit", len(out))
	}
}

func TestErrorSnippetIsSmallAndValidUTF8(t *testing.T) {
	out := append([]byte(strings.Repeat("x", errorSnippetLimit-1)), 0xe2, 0x82)
	got := errorSnippet(out)
	if len(got) > errorSnippetLimit+len("...") {
		t.Fatalf("snippet has %d bytes", len(got))
	}
	if !strings.HasSuffix(got, "...") || !utf8.ValidString(got) {
		t.Fatalf("snippet did not clean and mark truncation: %q", got[len(got)-32:])
	}
}
