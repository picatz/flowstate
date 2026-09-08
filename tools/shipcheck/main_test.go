package main

import (
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

const testHead = "0123456789abcdef0123456789abcdef01234567"

func passingPullRequest() pullRequest {
	return pullRequest{
		State:            "OPEN",
		BaseRefName:      "main",
		HeadRefOID:       testHead,
		AutoMergeRequest: presentJSON{Present: true, Value: json.RawMessage("null")},
		StatusChecks: []statusCheck{
			{Type: "CheckRun", Name: "test", Status: "COMPLETED", Conclusion: "SUCCESS"},
			{Type: "CheckRun", Name: "not selected", Status: "COMPLETED", Conclusion: "SKIPPED"},
			{Type: "StatusContext", Context: "external", State: "SUCCESS"},
		},
		Reviews: []review{
			{Author: actor{Login: "copilot-pull-request-reviewer"}, Commit: &commit{OID: testHead}},
			{Author: actor{Login: "chatgpt-codex-connector"}, Commit: &commit{OID: testHead}, Body: "### Codex Review"},
		},
		Comments: []comment{{
			Author: actor{Login: "chatgpt-codex-connector"},
			Body:   "Security review completed. No security issues were found.\n\n**Reviewed commit:** `" + testHead + "`",
		}},
	}
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
	pr.Reviews[0].Commit.OID = strings.Repeat("f", 40)
	pr.Reviews[1].Commit.OID = strings.Repeat("e", 40)
	pr.Comments[0].Body = strings.ReplaceAll(pr.Comments[0].Body, "0123456789", "abcdef0123")

	problems := strings.Join(evaluate(pr, 2), "\n")
	for _, want := range []string{
		`pull request state is "CLOSED"`,
		`pull request base is "release"`,
		"pull request is still a draft",
		"auto-merge is enabled",
		`check "test" latest result is IN_PROGRESS/`,
		"Copilot has not reviewed the exact final head",
		"Codex code review has not completed on the exact final head",
		"Codex security review has not completed on the exact final head",
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

func TestEvaluateRejectsCopilotFindingsOnTheFinalHead(t *testing.T) {
	pr := passingPullRequest()
	pr.Reviews[0].Body = "### Changes recommended\n\n### Suppressed comments (1)"
	if problems := strings.Join(evaluate(pr, 0), "\n"); !strings.Contains(problems, "Copilot exact-final-head review still contains findings") {
		t.Fatalf("Copilot finding was accepted: %s", problems)
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

func TestEvaluateAcceptsSuccessfulReplacementForCancelledDuplicate(t *testing.T) {
	pr := passingPullRequest()
	pr.StatusChecks = []statusCheck{
		{Type: "CheckRun", Name: "commitcheck", Status: "COMPLETED", Conclusion: "CANCELLED", StartedAt: "2026-09-08T10:00:00Z"},
		{Type: "CheckRun", Name: "commitcheck", Status: "COMPLETED", Conclusion: "SUCCESS", StartedAt: "2026-09-08T10:01:00Z"},
	}
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

func TestEvaluateUsesLatestExactHeadCopilotReview(t *testing.T) {
	pr := passingPullRequest()
	pr.Reviews[0].SubmittedAt = "2026-09-08T10:00:00Z"
	pr.Reviews = append(pr.Reviews, review{
		Author:      actor{Login: "copilot-pull-request-reviewer"},
		Commit:      &commit{OID: testHead},
		Body:        "### Suppressed comments (1)",
		SubmittedAt: "2026-09-08T10:01:00Z",
	})
	if problems := strings.Join(evaluate(pr, 0), "\n"); !strings.Contains(problems, "Copilot exact-final-head review still contains findings") {
		t.Fatalf("latest Copilot finding was accepted: %s", problems)
	}
}

func TestMentionsCommitRequiresTheQuotedFullHead(t *testing.T) {
	if !mentionsCommit("Reviewed commit: `"+testHead+"`", testHead) {
		t.Fatal("quoted full head was not recognized")
	}
	if mentionsCommit("unrelated 0123456789abcdef", testHead) {
		t.Fatal("unquoted commit text was recognized")
	}
	if mentionsCommit("Reviewed commit: `0123456789`", testHead) {
		t.Fatal("abbreviated commit was recognized")
	}
}

func TestSecuritySummaryRequiresCompletedExactHead(t *testing.T) {
	body := `<!-- codex-security-review:v1 {"headSha":"` + testHead + `","status":"completed"} -->`
	if !completedSecuritySummary(body, testHead) {
		t.Fatal("completed exact-head summary was not recognized")
	}
	if completedSecuritySummary(strings.Replace(body, "completed", "running", 1), testHead) {
		t.Fatal("running summary was recognized")
	}
	if completedSecuritySummary(strings.Replace(body, testHead, strings.Repeat("f", 40), 1), testHead) {
		t.Fatal("stale-head summary was recognized")
	}
}

func TestCodexSecurityReviewDoesNotCountAsCodeReview(t *testing.T) {
	pr := passingPullRequest()
	pr.Reviews[1].Body = "### Codex Security Review"
	pr.Comments = nil
	if hasCodexReview(pr, false) {
		t.Fatal("security review was recognized as a code review")
	}
}

func TestCodeSummaryRequiresFullHeadMarkerAndCompletedRow(t *testing.T) {
	body := `<!-- codex-pull-request-review-summary -->
<!-- codex-security-review:v1 {"headSha":"` + testHead + `","status":"completed"} -->
| 📝 **Code Review** | ✅ **Completed** <relative-time datetime="2026-09-08T10:01:00Z">now</relative-time> | ` + "`0123456`" + ` | Manual request |`
	request := []comment{{
		Body:      "@codex review\n\nReview exact head `" + testHead + "`.",
		CreatedAt: "2026-09-08T10:00:00Z",
	}}
	if !completedCodeSummary(body, testHead, request) {
		t.Fatal("completed summary following an exact-head request was not recognized")
	}
	if completedCodeSummary(strings.Replace(body, testHead, strings.Repeat("f", 40), 1), testHead, request) {
		t.Fatal("summary for a different full head was recognized")
	}
	if completedCodeSummary(strings.Replace(body, "**Completed**", "**Running**", 1), testHead, request) {
		t.Fatal("running code review was recognized")
	}
	if completedCodeSummary(body, testHead, nil) {
		t.Fatal("abbreviated row without a full-head code-review request was recognized")
	}
	lateRequest := append([]comment(nil), request...)
	lateRequest[0].CreatedAt = "2026-09-08T10:02:00Z"
	if completedCodeSummary(body, testHead, lateRequest) {
		t.Fatal("completion predating the exact-head request was recognized")
	}
}

func TestRunGHIsBounded(t *testing.T) {
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
