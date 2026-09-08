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
		AutoMergeRequest: json.RawMessage("null"),
		StatusChecks: []statusCheck{
			{Type: "CheckRun", Name: "test", Status: "COMPLETED", Conclusion: "SUCCESS"},
			{Type: "CheckRun", Name: "not selected", Status: "COMPLETED", Conclusion: "SKIPPED"},
			{Type: "StatusContext", Context: "external", State: "SUCCESS"},
		},
		Reviews: []review{
			{Author: actor{Login: "copilot-pull-request-reviewer"}, Commit: &commit{OID: testHead}},
			{Author: actor{Login: "chatgpt-codex-connector"}, Commit: &commit{OID: testHead}},
		},
		Comments: []comment{{
			Author: actor{Login: "chatgpt-codex-connector"},
			Body:   "Security review completed. No security issues were found.\n\n**Reviewed commit:** `0123456789`",
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
	pr.AutoMergeRequest = json.RawMessage(`{"enabledAt":"now"}`)
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
		`check "test" is IN_PROGRESS/`,
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
		{Type: "CheckRun", Name: "commitcheck", Status: "COMPLETED", Conclusion: "CANCELLED"},
		{Type: "CheckRun", Name: "commitcheck", Status: "COMPLETED", Conclusion: "SUCCESS"},
	}
	if problems := evaluate(pr, 0); len(problems) != 0 {
		t.Fatalf("successful replacement was rejected: %v", problems)
	}
}

func TestMentionsCommitRequiresAQuotedHeadPrefix(t *testing.T) {
	if !mentionsCommit("Reviewed commit: `0123456`", testHead) {
		t.Fatal("quoted seven-character prefix was not recognized")
	}
	if mentionsCommit("unrelated 0123456789abcdef", testHead) {
		t.Fatal("unquoted commit text was recognized")
	}
	if mentionsCommit("Reviewed commit: `0123450`", testHead) {
		t.Fatal("different commit was recognized")
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
