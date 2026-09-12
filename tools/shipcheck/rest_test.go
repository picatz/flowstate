package main

import (
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
)

// The REST fallback is exercised through the same seam as every other gh
// call here: a fake `gh` on PATH. This one refuses `pr view` and `api
// graphql` the way the Claude Code proxy does and serves canned REST
// documents from a fixture directory, so the test covers the endpoints the
// fallback asks for, the documents it decodes, and the evidence evaluate
// sees at the end; it does not cover GitHub itself.
const fakeGH = `#!/bin/sh
case "$*" in
  "pr view"*|"api graphql"*)
    echo "gh: GitHub GraphQL is not available from Claude Code sessions" >&2
    exit 1 ;;
  *"/pulls/7") cat "$SHIPCHECK_FIXTURES/pull.json" ;;
  *"/pulls/7/files?per_page=100&page=1") printf '[{"filename":"AGENTS.md"}]' ;;
  *"/actions/runs?head_sha=HEAD&per_page=100&page=1") cat "$SHIPCHECK_FIXTURES/runs.json" ;;
  *"/commits/HEAD/check-runs?filter=all&per_page=100&page=1") cat "$SHIPCHECK_FIXTURES/check-runs.json" ;;
  *"/commits/HEAD/status?per_page=100&page=1") printf '{"statuses":[{"context":"external","state":"success","created_at":"2026-09-12T00:00:00Z"}]}' ;;
  *"/pulls/7/reviews?per_page=100&page=1") cat "$SHIPCHECK_FIXTURES/reviews.json" ;;
  *"/rules/branches/main") cat "$SHIPCHECK_FIXTURES/rules.json" ;;
  *"/issues/7/comments?per_page=100&page=1") cat "$SHIPCHECK_FIXTURES/comments.json" ;;
  *"/pulls/7/comments -f sort=updated"*) printf '[]' ;;
  *"/pulls/7/ccr/review_threads") cat "$SHIPCHECK_FIXTURES/threads.json" ;;
  *) echo "unexpected gh $*" >&2; exit 3 ;;
esac
`

func installFakeGH(t *testing.T, script string) string {
	t.Helper()
	if runtime.GOOS == "windows" {
		t.Skip("the fake gh is a POSIX shell script")
	}
	dir := t.TempDir()
	script = strings.ReplaceAll(script, "HEAD", testHead)
	if err := os.WriteFile(filepath.Join(dir, "gh"), []byte(script), 0o755); err != nil {
		t.Fatal(err)
	}
	t.Setenv("PATH", dir+string(os.PathListSeparator)+os.Getenv("PATH"))
	t.Setenv("SHIPCHECK_FIXTURES", dir)
	fallbackNoted = false
	return dir
}

func writeFixture(t *testing.T, dir, name, body string) {
	t.Helper()
	if err := os.WriteFile(filepath.Join(dir, name), []byte(strings.ReplaceAll(body, "HEAD", testHead)), 0o644); err != nil {
		t.Fatal(err)
	}
}

func restFixtures(t *testing.T, dir string) {
	t.Helper()
	writeFixture(t, dir, "pull.json", `{"state":"open","draft":false,"base":{"ref":"main"},"head":{"sha":"HEAD"},"auto_merge":null,"changed_files":1}`)
	writeFixture(t, dir, "runs.json", `{"total_count":4,"workflow_runs":[
		{"name":"CI","check_suite_id":1},
		{"name":"CodeQL","check_suite_id":2},
		{"name":"Commit conventions","check_suite_id":3},
		{"name":"Dependency review","check_suite_id":4}]}`)
	writeFixture(t, dir, "check-runs.json", `{"total_count":7,"check_runs":[
		{"name":"plan","status":"completed","conclusion":"success","started_at":"2026-09-12T00:00:01Z","completed_at":"2026-09-12T00:00:02Z","check_suite":{"id":1}},
		{"name":"verdict","status":"completed","conclusion":"success","started_at":"2026-09-12T00:00:03Z","completed_at":"2026-09-12T00:00:04Z","check_suite":{"id":1}},
		{"name":"fuzz-smoke","status":"completed","conclusion":"skipped","started_at":"2026-09-12T00:00:01Z","completed_at":"2026-09-12T00:00:01Z","check_suite":{"id":1}},
		{"name":"Analyze Go","status":"completed","conclusion":"success","started_at":"2026-09-12T00:00:01Z","completed_at":"2026-09-12T00:00:02Z","check_suite":{"id":2}},
		{"name":"commitcheck","status":"completed","conclusion":"success","started_at":"2026-09-12T00:00:01Z","completed_at":"2026-09-12T00:00:02Z","check_suite":{"id":3}},
		{"name":"Review dependency changes","status":"completed","conclusion":"success","started_at":"2026-09-12T00:00:01Z","completed_at":"2026-09-12T00:00:02Z","check_suite":{"id":4}},
		{"name":"CodeQL","status":"completed","conclusion":"success","started_at":"2026-09-12T00:00:01Z","completed_at":"2026-09-12T00:00:02Z","check_suite":{"id":9}}]}`)
	writeFixture(t, dir, "reviews.json", `[{"user":{"login":"copilot-pull-request-reviewer[bot]"},"state":"COMMENTED","submitted_at":"2026-09-12T00:00:05Z"}]`)
	writeFixture(t, dir, "rules.json", `[{"type":"deletion"},{"type":"non_fast_forward"}]`)
	writeFixture(t, dir, "comments.json", `[{"user":{"login":"picatz"},"author_association":"OWNER","created_at":"2026-09-12T00:00:09Z","updated_at":"2026-09-12T00:00:09Z","html_url":"https://github.com/picatz/flowstate/pull/7#issuecomment-1",
		"body":"Independent AI code/security review by flowstate-reviewer for HEAD: PASS with no actionable findings.\n\n<!-- flowstate-independent-review:v1 {\"headSha\":\"HEAD\",\"reviewer\":\"flowstate-reviewer\",\"scope\":\"code-security\",\"status\":\"pass\"} -->"}]`)
	writeFixture(t, dir, "threads.json", `[{"resolved":true,"outdated":true,"path":"AGENTS.md","line":null,"comment_ids":[1,2]}]`)
}

// TestRESTFallbackAssemblesTheSameEvidence is the case the fallback exists
// for: GraphQL refused, every fact evaluate needs recovered over REST, and
// the verdict the GraphQL path would have reached.
func TestRESTFallbackAssemblesTheSameEvidence(t *testing.T) {
	dir := installFakeGH(t, fakeGH)
	restFixtures(t, dir)

	pr, err := loadPullRequest("picatz/flowstate", 7)
	if err != nil {
		t.Fatalf("loadPullRequest over REST: %v", err)
	}
	if pr.State != "OPEN" || pr.BaseRefName != "main" || pr.IsDraft || pr.HeadRefOID != testHead {
		t.Errorf("summary = %+v", pr)
	}
	if !pr.AutoMergeRequest.Present || strings.TrimSpace(string(pr.AutoMergeRequest.Value)) != "null" {
		t.Errorf("auto-merge evidence = %+v, want present and null", pr.AutoMergeRequest)
	}
	if len(pr.Files) != 1 || pr.Files[0].Path != "AGENTS.md" || pr.ChangedFiles != 1 {
		t.Errorf("files = %+v (changed %d)", pr.Files, pr.ChangedFiles)
	}
	byName := map[string]statusCheck{}
	for _, check := range pr.StatusChecks {
		byName[check.Workflow+"/"+check.Name+"/"+check.Context] = check
	}
	for _, want := range []string{"CI/plan/", "CI/verdict/", "CodeQL/Analyze Go/", "Commit conventions/commitcheck/", "Dependency review/Review dependency changes/", "/CodeQL/", "//external"} {
		if _, ok := byName[want]; !ok {
			t.Errorf("check %q missing from %v", want, byName)
		}
	}
	if got := byName["CI/plan/"]; got.Type != "CheckRun" || got.Status != "COMPLETED" || got.Conclusion != "SUCCESS" {
		t.Errorf("plan = %+v, want a completed successful CheckRun", got)
	}
	if got := byName["//external"]; got.Type != "StatusContext" || got.State != "SUCCESS" {
		t.Errorf("external = %+v, want a successful StatusContext", got)
	}
	if pr.ReviewDecision != "" {
		t.Errorf("review decision = %q from a COMMENTED review, want none", pr.ReviewDecision)
	}
	if len(pr.Reviews) != 1 || pr.Reviews[0].SubmittedAt == "" || len(pr.Comments) != 1 || pr.Comments[0].AuthorAssociation != "OWNER" {
		t.Errorf("reviews = %+v comments = %+v", pr.Reviews, pr.Comments)
	}
	unresolved, err := unresolvedReviewThreads("picatz/flowstate", 7)
	if err != nil {
		t.Fatalf("unresolvedReviewThreads over REST: %v", err)
	}
	if problems := evaluate(pr, unresolved); len(problems) != 0 {
		t.Fatalf("evaluate over REST evidence returned problems: %v", problems)
	}
}

// TestRESTFallbackStillReportsWhatBlocks: the fallback recovers the facts
// that block a merge as faithfully as the ones that permit it.
func TestRESTFallbackStillReportsWhatBlocks(t *testing.T) {
	dir := installFakeGH(t, fakeGH)
	restFixtures(t, dir)
	writeFixture(t, dir, "threads.json", `[{"resolved":false,"path":"AGENTS.md","line":3,"comment_ids":[1]}]`)
	writeFixture(t, dir, "reviews.json", `[
		{"user":{"login":"reviewer"},"state":"APPROVED","submitted_at":"2026-09-12T00:00:05Z"},
		{"user":{"login":"reviewer"},"state":"CHANGES_REQUESTED","submitted_at":"2026-09-12T00:00:06Z"}]`)

	pr, err := loadPullRequest("picatz/flowstate", 7)
	if err != nil {
		t.Fatalf("loadPullRequest over REST: %v", err)
	}
	if pr.ReviewDecision != "CHANGES_REQUESTED" {
		t.Errorf("review decision = %q, want the reviewer's latest decisive review", pr.ReviewDecision)
	}
	unresolved, err := unresolvedReviewThreads("picatz/flowstate", 7)
	if err != nil {
		t.Fatalf("unresolvedReviewThreads over REST: %v", err)
	}
	if unresolved != 1 {
		t.Errorf("unresolved = %d, want 1", unresolved)
	}
	problems := strings.Join(evaluate(pr, unresolved), "\n")
	for _, want := range []string{`review decision is "CHANGES_REQUESTED"`, "1 review thread(s) remain unresolved"} {
		if !strings.Contains(problems, want) {
			t.Errorf("problems %q lack %q", problems, want)
		}
	}
}

// TestRESTFallbackReadsARequiredReviewRule: GraphQL's reviewDecision says
// REVIEW_REQUIRED when a ruleset wants an approval nobody gave; the REST
// spelling derives the same word from the base branch's rules, and an
// approval clears it.
func TestRESTFallbackReadsARequiredReviewRule(t *testing.T) {
	dir := installFakeGH(t, fakeGH)
	restFixtures(t, dir)
	writeFixture(t, dir, "rules.json", `[{"type":"pull_request","parameters":{"required_approving_review_count":1}}]`)

	pr, err := loadPullRequest("picatz/flowstate", 7)
	if err != nil {
		t.Fatalf("loadPullRequest over REST: %v", err)
	}
	if pr.ReviewDecision != "REVIEW_REQUIRED" {
		t.Fatalf("review decision = %q, want REVIEW_REQUIRED from the ruleset", pr.ReviewDecision)
	}
	if problems := strings.Join(evaluate(pr, 0), "\n"); !strings.Contains(problems, `review decision is "REVIEW_REQUIRED"`) {
		t.Errorf("problems %q do not block on the required review", problems)
	}

	writeFixture(t, dir, "reviews.json", `[{"user":{"login":"reviewer"},"state":"APPROVED","submitted_at":"2026-09-12T00:00:05Z"}]`)
	pr, err = loadPullRequest("picatz/flowstate", 7)
	if err != nil {
		t.Fatalf("loadPullRequest over REST: %v", err)
	}
	if pr.ReviewDecision != "APPROVED" {
		t.Fatalf("review decision = %q after an approval, want APPROVED", pr.ReviewDecision)
	}

	// The count is honored, not just the presence of an approval: two
	// required, one given (twice, by the same reviewer) is still required;
	// a second reviewer clears it.
	writeFixture(t, dir, "rules.json", `[{"type":"pull_request","parameters":{"required_approving_review_count":2}}]`)
	writeFixture(t, dir, "reviews.json", `[
		{"user":{"login":"reviewer"},"state":"APPROVED","submitted_at":"2026-09-12T00:00:05Z"},
		{"user":{"login":"reviewer"},"state":"APPROVED","submitted_at":"2026-09-12T00:00:06Z"}]`)
	pr, err = loadPullRequest("picatz/flowstate", 7)
	if err != nil {
		t.Fatalf("loadPullRequest over REST: %v", err)
	}
	if pr.ReviewDecision != "REVIEW_REQUIRED" {
		t.Fatalf("review decision = %q with one of two required approvals, want REVIEW_REQUIRED", pr.ReviewDecision)
	}
	writeFixture(t, dir, "reviews.json", `[
		{"user":{"login":"reviewer"},"state":"APPROVED","submitted_at":"2026-09-12T00:00:05Z"},
		{"user":{"login":"second"},"state":"APPROVED","submitted_at":"2026-09-12T00:00:06Z"}]`)
	pr, err = loadPullRequest("picatz/flowstate", 7)
	if err != nil {
		t.Fatalf("loadPullRequest over REST: %v", err)
	}
	if pr.ReviewDecision != "APPROVED" {
		t.Fatalf("review decision = %q with two of two required approvals, want APPROVED", pr.ReviewDecision)
	}
}

// TestBothTransportsDownFailsClosed: with GraphQL refused and REST failing
// too, nothing loads and the error names both transports, so the caller
// exits non-zero rather than evaluating an empty pull request.
func TestBothTransportsDownFailsClosed(t *testing.T) {
	installFakeGH(t, "#!/bin/sh\necho 'gh: down' >&2\nexit 1\n")
	if _, err := loadPullRequest("picatz/flowstate", 7); err == nil {
		t.Fatal("loadPullRequest succeeded with every transport down")
	} else if !strings.Contains(err.Error(), "GraphQL:") || !strings.Contains(err.Error(), "REST:") {
		t.Errorf("error %q does not name both transports", err)
	}
	if _, err := unresolvedReviewThreads("picatz/flowstate", 7); err == nil {
		t.Fatal("unresolvedReviewThreads succeeded with every transport down")
	}
}

// TestRESTPagesStopAtTheBound: a list that never shortens is reported as
// running past the bound instead of returned as complete.
func TestRESTPagesStopAtTheBound(t *testing.T) {
	installFakeGH(t, "#!/bin/sh\nprintf '['\ni=0\nwhile [ $i -lt 100 ]; do [ $i -gt 0 ] && printf ','; printf '{}'; i=$((i+1)); done\nprintf ']'\n")
	if _, err := restPages("repos/picatz/flowstate/pulls/7/files", ""); err == nil || !strings.Contains(err.Error(), "ran past") {
		t.Fatalf("restPages on an endless list = %v, want the bound named", err)
	}
}
